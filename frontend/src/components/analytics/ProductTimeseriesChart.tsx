import React from 'react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts';

export interface ProductTimeseriesItem {
  date: string;
  avg_price: number;
  min_price: number;
  max_price: number;
  total_reviews: number;
  avg_rating: number;
  revenue: number;
}

interface ProductTimeseriesChartProps {
  data: ProductTimeseriesItem[];
}

export function ProductTimeseriesChart({ data }: ProductTimeseriesChartProps) {
  const formatCurrency = (value: number) => {
    return new Intl.NumberFormat('vi-VN', {
      style: 'currency',
      currency: 'VND',
      minimumFractionDigits: 0,
    }).format(value);
  };

  const formatDate = (dateStr: string) => {
    const date = new Date(dateStr);
    return date.toLocaleDateString('vi-VN', {
      month: 'short',
      day: 'numeric',
    });
  };

  return (
    <div className="bg-white p-6 rounded-lg border border-gray-200 shadow-sm">
      <h4 className="text-lg font-semibold text-gray-900 mb-4">
        Xu Hướng Sản Phẩm Theo Thời Gian
      </h4>
      <div className="h-80">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={data} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
            <XAxis
              dataKey="date"
              tickFormatter={formatDate}
              stroke="#6b7280"
              fontSize={12}
            />
            <YAxis
              yAxisId="left"
              orientation="left"
              tickFormatter={formatCurrency}
              stroke="#3b82f6"
              fontSize={12}
            />
            <YAxis
              yAxisId="right"
              orientation="right"
              stroke="#10b981"
              fontSize={12}
            />
            <Tooltip
              formatter={(value: any, name: string) => {
                if (name === 'Doanh thu') return [formatCurrency(value), name];
                if (name === 'Giá TB') return [formatCurrency(value), name];
                if (name === 'Rating TB') return [value.toFixed(1), name];
                if (name === 'Tổng đánh giá') return [value.toLocaleString('vi-VN'), name];
                return [value, name];
              }}
              labelFormatter={(label) => `Ngày: ${formatDate(label)}`}
              contentStyle={{
                backgroundColor: 'white',
                border: '1px solid #e5e7eb',
                borderRadius: '8px',
                fontSize: '14px',
              }}
            />
            <Legend />
            <Line
              yAxisId="left"
              type="monotone"
              dataKey="revenue"
              stroke="#3b82f6"
              strokeWidth={2}
              name="Doanh thu"
              dot={{ fill: '#3b82f6', strokeWidth: 2, r: 4 }}
              activeDot={{ r: 6, stroke: '#3b82f6', strokeWidth: 2 }}
            />
            <Line
              yAxisId="right"
              type="monotone"
              dataKey="avg_price"
              stroke="#10b981"
              strokeWidth={2}
              name="Giá TB"
              dot={{ fill: '#10b981', strokeWidth: 2, r: 4 }}
              activeDot={{ r: 6, stroke: '#10b981', strokeWidth: 2 }}
            />
            <Line
              yAxisId="right"
              type="monotone"
              dataKey="avg_rating"
              stroke="#f59e0b"
              strokeWidth={2}
              name="Rating TB"
              dot={{ fill: '#f59e0b', strokeWidth: 2, r: 4 }}
              activeDot={{ r: 6, stroke: '#f59e0b', strokeWidth: 2 }}
            />
            <Line
              yAxisId="right"
              type="monotone"
              dataKey="total_reviews"
              stroke="#ef4444"
              strokeWidth={2}
              name="Tổng đánh giá"
              dot={{ fill: '#ef4444', strokeWidth: 2, r: 4 }}
              activeDot={{ r: 6, stroke: '#ef4444', strokeWidth: 2 }}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}