import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { TrendingUp } from 'lucide-react';

const data = [
  { time: '00:00', rate: 92 },
  { time: '04:00', rate: 94 },
  { time: '08:00', rate: 96 },
  { time: '12:00', rate: 95 },
  { time: '16:00', rate: 97 },
  { time: '20:00', rate: 96 },
  { time: '23:59', rate: 95.8 },
];

export function PerformanceChart() {
  return (
    <div className="bg-white border border-slate-200 rounded-lg p-5 shadow-sm h-full">
      <h2 className="text-slate-800 mb-4 flex items-center gap-2">
        <TrendingUp className="w-5 h-5 text-blue-600" />
        📈 Success Rate Trend
      </h2>
      <ResponsiveContainer width="100%" height={250}>
        <LineChart data={data}>
          <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
          <XAxis
            dataKey="time"
            stroke="#64748b"
            style={{ fontSize: '12px' }}
          />
          <YAxis
            stroke="#64748b"
            style={{ fontSize: '12px' }}
            domain={[85, 100]}
            tickFormatter={(value) => `${value}%`}
          />
          <Tooltip
            contentStyle={{
              backgroundColor: '#fff',
              border: '1px solid #e2e8f0',
              borderRadius: '6px',
            }}
            formatter={(value: number) => [`${value}%`, 'Success Rate']}
          />
          <Line
            type="monotone"
            dataKey="rate"
            stroke="#3b82f6"
            strokeWidth={2}
            dot={{ fill: '#3b82f6', r: 4 }}
            activeDot={{ r: 6 }}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
