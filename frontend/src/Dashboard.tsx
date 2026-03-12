import { Bar } from 'react-chartjs-2'
import { Chart as ChartJS, CategoryScale, LinearScale, BarElement, Title, Tooltip } from 'chart.js'

ChartJS.register(CategoryScale, LinearScale, BarElement, Title, Tooltip);

export default function Dashboard() {
  return (
    <div>
      Dashboard. Indeed. Truly fascinating.
    </div>
  );
}