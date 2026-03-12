"""Router for analytics endpoints.
Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""
from fastapi import APIRouter, Depends, Query
from sqlmodel import select, func, case, cast, Date
from sqlmodel.ext.asyncio.session import AsyncSession
from app.database import get_session
from app.models.item import ItemRecord
from app.models.learner import Learner
from app.models.interaction import InteractionLog

router = APIRouter()


def _get_lab_task_ids(session: AsyncSession, lab_param: str) -> list[int]:
    """Helper: find lab by title pattern, return IDs of child task items."""
    # Transform "lab-04" → "Lab 04" for title matching
    lab_title_part = lab_param.replace("-", " ").replace("lab", "Lab", 1)
    
    # Find the lab item whose title contains the lab identifier
    lab = session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.ilike(f"%{lab_title_part}%")
        )
    ).first()
    
    if not lab:
        return []
    
    # Find all task items that belong to this lab
    tasks = session.exec(
        select(ItemRecord.id).where(
            ItemRecord.parent_id == lab.id,
            ItemRecord.type == "task"
        )
    ).all()
    
    return list(tasks)


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab.
    
    Returns buckets: "0-25", "26-50", "51-75", "76-100"
    """
    task_ids = _get_lab_task_ids(session, lab)
    
    if not task_ids:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]
    
    # Group scores into buckets using CASE WHEN
    bucket_expr = case(
        (InteractionLog.score <= 25, "0-25"),
        (InteractionLog.score <= 50, "26-50"),
        (InteractionLog.score <= 75, "51-75"),
        (InteractionLog.score <= 100, "76-100"),
    ).label("bucket")
    
    query = select(
        bucket_expr,
        func.count(InteractionLog.id).label("count")
    ).where(
        InteractionLog.item_id.in_(task_ids),
        InteractionLog.score.isnot(None)
    ).group_by(bucket_expr)
    
    result = session.exec(query).all()
    
    # Build counts dict from query results
    counts = {row.bucket: row.count for row in result if row.bucket}
    
    # Always return all four buckets, even if count is 0
    return [
        {"bucket": "0-25", "count": counts.get("0-25", 0)},
        {"bucket": "26-50", "count": counts.get("26-50", 0)},
        {"bucket": "51-75", "count": counts.get("51-75", 0)},
        {"bucket": "76-100", "count": counts.get("76-100", 0)},
    ]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-task pass rates for a given lab."""
    task_ids = _get_lab_task_ids(session, lab)
    
    if not task_ids:
        return []
    
    # For each task: avg_score (rounded to 1 decimal) and total attempts
    query = select(
        ItemRecord.title.label("task"),
        func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
        func.count(InteractionLog.id).label("attempts")
    ).join(
        InteractionLog, InteractionLog.item_id == ItemRecord.id
    ).where(
        ItemRecord.id.in_(task_ids)
    ).group_by(ItemRecord.id, ItemRecord.title).order_by(ItemRecord.title)
    
    result = session.exec(query).all()
    
    return [
        {
            "task": row.task,
            "avg_score": float(row.avg_score) if row.avg_score is not None else 0.0,
            "attempts": row.attempts
        }
        for row in result
    ]


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Submissions per day for a given lab."""
    task_ids = _get_lab_task_ids(session, lab)
    
    if not task_ids:
        return []
    
    # Group interactions by date, count submissions per day
    query = select(
        cast(InteractionLog.created_at, Date).label("date"),
        func.count(InteractionLog.id).label("submissions")
    ).where(
        InteractionLog.item_id.in_(task_ids)
    ).group_by(cast(InteractionLog.created_at, Date)).order_by(cast(InteractionLog.created_at, Date))
    
    result = session.exec(query).all()
    
    return [
        {"date": row.date.isoformat(), "submissions": row.submissions}
        for row in result
    ]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-group performance for a given lab."""
    task_ids = _get_lab_task_ids(session, lab)
    
    if not task_ids:
        return []
    
    # Join interactions with learners to get student_group
    # Compute avg_score and count of distinct learners per group
    query = select(
        Learner.student_group.label("group"),
        func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
        func.count(func.distinct(Learner.id)).label("students")
    ).join(
        InteractionLog, InteractionLog.learner_id == Learner.id
    ).where(
        InteractionLog.item_id.in_(task_ids),
        InteractionLog.score.isnot(None)
    ).group_by(Learner.student_group).order_by(Learner.student_group)
    
    result = session.exec(query).all()
    
    return [
        {
            "group": row.group,
            "avg_score": float(row.avg_score) if row.avg_score is not None else 0.0,
            "students": row.students
        }
        for row in result
    ]
