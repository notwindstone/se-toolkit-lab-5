"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime

from sqlmodel.ext.asyncio.session import AsyncSession

from app.settings import settings


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


async def fetch_items() -> list[dict]:
    """Fetch the lab/task catalog from the autochecker API."""
    import httpx
    from app.settings import settings
    
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{settings.autochecker_api_url}/api/items",
            auth=(settings.autochecker_email, settings.autochecker_password),
        )
        response.raise_for_status()
        return response.json()


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API with pagination."""
    import httpx
    from app.settings import settings
    
    all_logs: list[dict] = []
    current_since = since
    
    async with httpx.AsyncClient() as client:
        while True:
            params: dict[str, str | int] = {"limit": 500}
            if current_since is not None:
                params["since"] = current_since.isoformat()
            
            response = await client.get(
                f"{settings.autochecker_api_url}/api/logs",
                auth=(settings.autochecker_email, settings.autochecker_password),
                params=params,
            )
            response.raise_for_status()
            data = response.json()
            
            logs = data.get("logs", [])
            all_logs.extend(logs)
            
            if not data.get("has_more", False):
                break
            
            # Use the last log's submitted_at as the new since value
            if logs:
                last_log = logs[-1]
                current_since = datetime.fromisoformat(
                    last_log["submitted_at"].replace("Z", "+00:00")
                )
    
    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database."""
    from sqlmodel import select
    from app.models.item import ItemRecord
    
    newly_created = 0
    
    # Build a mapping from lab short ID (e.g., "lab-01") to ItemRecord
    lab_id_to_item: dict[str, ItemRecord] = {}
    
    # Process labs first
    for item_data in items:
        if item_data.get("type") != "lab":
            continue
        
        lab_title = item_data["title"]
        lab_short_id = item_data["lab"]
        
        # Check if lab already exists
        existing = await session.exec(
            select(ItemRecord).where(
                ItemRecord.type == "lab",
                ItemRecord.title == lab_title,
            )
        )
        lab_item = existing.first()
        
        if lab_item is None:
            lab_item = ItemRecord(
                type="lab",
                title=lab_title,
                description="",
                parent_id=None,
            )
            session.add(lab_item)
            newly_created += 1
        
        lab_id_to_item[lab_short_id] = lab_item
    
    # Commit labs so they have IDs before processing tasks
    await session.commit()
    
    # Process tasks
    for item_data in items:
        if item_data.get("type") != "task":
            continue
        
        task_title = item_data["title"]
        lab_short_id = item_data["lab"]
        
        # Find parent lab
        parent_lab = lab_id_to_item.get(lab_short_id)
        if parent_lab is None:
            continue  # Skip task if parent lab not found
        
        # Check if task already exists
        existing = await session.exec(
            select(ItemRecord).where(
                ItemRecord.type == "task",
                ItemRecord.title == task_title,
                ItemRecord.parent_id == parent_lab.id,
            )
        )
        task_item = existing.first()
        
        if task_item is None:
            task_item = ItemRecord(
                type="task",
                title=task_title,
                description="",
                parent_id=parent_lab.id,
            )
            session.add(task_item)
            newly_created += 1
    
    await session.commit()
    return newly_created


async def load_logs(
    logs: list[dict],
    items_catalog: list[dict],
    session: AsyncSession,
) -> int:
    """Load interaction logs into the database."""
    from sqlmodel import select
    from app.models.learner import Learner
    from app.models.interaction import InteractionLog
    from app.models.item import ItemRecord
    
    newly_created = 0
    
    # Build lookup from (lab_short_id, task_short_id) to item title
    item_title_lookup: dict[tuple[str, str | None], str] = {}
    for item_data in items_catalog:
        lab_id = item_data["lab"]
        task_id = item_data.get("task")
        title = item_data["title"]
        item_type = item_data.get("type")
        
        if item_type == "lab":
            item_title_lookup[(lab_id, None)] = title
        elif item_type == "task":
            item_title_lookup[(lab_id, task_id)] = title
    
    for log in logs:
        # 1. Find or create learner
        learner = await session.exec(
            select(Learner).where(Learner.external_id == log["student_id"])
        )
        learner_item = learner.first()
        
        if learner_item is None:
            learner_item = Learner(
                external_id=log["student_id"],
                student_group=log.get("group", ""),
            )
            session.add(learner_item)
            await session.flush()  # Get the ID
        
        # 2. Find matching item in database
        lab_id = log["lab"]
        task_id = log.get("task")
        item_title = item_title_lookup.get((lab_id, task_id))
        
        if item_title is None:
            continue  # Skip if no matching item found
        
        db_item = await session.exec(
            select(ItemRecord).where(ItemRecord.title == item_title)
        )
        item_record = db_item.first()
        
        if item_record is None:
            continue  # Skip if item not in database
        
        # 3. Check if InteractionLog already exists (idempotent upsert)
        existing = await session.exec(
            select(InteractionLog).where(
                InteractionLog.external_id == log["id"]
            )
        )
        if existing.first() is not None:
            continue  # Skip if already exists
        
        # 4. Create InteractionLog
        interaction = InteractionLog(
            external_id=log["id"],
            learner_id=learner_item.id,
            item_id=item_record.id,
            kind="attempt",
            score=log.get("score"),
            checks_passed=log.get("passed"),
            checks_total=log.get("total"),
            created_at=datetime.fromisoformat(
                log["submitted_at"].replace("Z", "+00:00")
            ),
        )
        session.add(interaction)
        newly_created += 1
    
    await session.commit()
    return newly_created


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def sync(session: AsyncSession) -> dict:
    """Run the full ETL pipeline."""
    from sqlmodel import select
    from app.models.interaction import InteractionLog
    
    # Step 1: Fetch items and load them
    items_catalog = await fetch_items()
    await load_items(items_catalog, session)
    
    # Step 2: Determine the last synced timestamp
    last_interaction = await session.exec(
        select(InteractionLog)
        .order_by(InteractionLog.created_at.desc())
        .limit(1)
    )
    last_record = last_interaction.first()
    since = last_record.created_at if last_record else None
    
    # Step 3: Fetch logs since that timestamp and load them
    logs = await fetch_logs(since=since)
    new_interactions = await load_logs(logs, items_catalog, session)
    
    # Get total count
    total_result = await session.exec(select(InteractionLog))
    total_interactions = len(total_result.all())
    
    return {
        "new_records": new_interactions,
        "total_records": total_interactions,
    }
