import React from 'react';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
} from 'recharts';

export interface ReviewSummary {
  product_key: string;
  platform_code: string;
  from_date: string;
  to_date: string;
  total_reviews: number;
  avg_rating: number;
  rating_breakdown: {
    by_rating: { [key: string]: number };
  };
  top_helpful_reviews: any[];
}

interface ReviewSummaryChartProps {
  data: ReviewSummary;
}

const RATING_COLORS = {
  1: '#ef4444',
  2: '#f97316',
  3: '#eab308',
  4: '#22c55e',
  5: '#10b981',
};

const SENTIMENT_COLORS = {
  positive: '#10b981',
  neutral: '#6b7280',
  negative: '#ef4444',
};

export function ReviewSummaryChart({ data }: ReviewSummaryChartProps) {
  // Convert rating_breakdown to rating distribution array
  const ratingData = data.rating_breakdown?.by_rating ?
    Object.entries(data.rating_breakdown.by_rating).map(([rating, count]) => ({
      rating: parseInt(rating),
      count: count as number,
      percentage: ((count as number / data.total_reviews) * 100).toFixed(1),
    })) || [] : [];

  // Sort rating data by rating
  ratingData.sort((a, b) => a.rating - b.rating);

  // For now, we'll skip sentiment data since it's not in the API response
  const sentimentData: any[] = [];

  return (
    <div className="space-y-6">
      {/* Rating Distribution */}
      <div className="bg-white p-6 rounded-lg border border-gray-200 shadow-sm">
        <h4 className="text-lg font-semibold text-gray-900 mb-4">
          Phân Phối Đánh Giá
        </h4>
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Bar Chart */}
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={ratingData} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                <XAxis
                  dataKey="rating"
                  stroke="#6b7280"
                  fontSize={12}
                  tickFormatter={(value) => `${value} sao`}
                />
                <YAxis
                  stroke="#6b7280"
                  fontSize={12}
                />
                <Tooltip
                  formatter={(value: any, name: string) => [
                    `${value} đánh giá`,
                    'Số lượng'
                  ]}
                  contentStyle={{
                    backgroundColor: 'white',
                    border: '1px solid #e5e7eb',
                    borderRadius: '8px',
                    fontSize: '14px',
                  }}
                />
                <Bar
                  dataKey="count"
                  fill="#3b82f6"
                  radius={[4, 4, 0, 0]}
                />
              </BarChart>
            </ResponsiveContainer>
          </div>

          {/* Pie Chart */}
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie
                  data={ratingData}
                  cx="50%"
                  cy="50%"
                  labelLine={false}
                  label={({ rating, percentage }) => `${rating}: ${percentage}%`}
                  outerRadius={80}
                  fill="#8884d8"
                  dataKey="count"
                >
                  {ratingData.map((entry, index) => (
                    <Cell
                      key={`cell-${index}`}
                      fill={RATING_COLORS[entry.rating as keyof typeof RATING_COLORS] || '#6b7280'}
                    />
                  ))}
                </Pie>
                <Tooltip
                  formatter={(value: any) => [`${value} đánh giá`]}
                  contentStyle={{
                    backgroundColor: 'white',
                    border: '1px solid #e5e7eb',
                    borderRadius: '8px',
                    fontSize: '14px',
                  }}
                />
              </PieChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>

      {/* Sentiment Distribution */}
      {sentimentData.length > 0 && (
        <div className="bg-white p-6 rounded-lg border border-gray-200 shadow-sm">
          <h4 className="text-lg font-semibold text-gray-900 mb-4">
            Phân Phối Tâm Trạng
          </h4>
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie
                  data={sentimentData}
                  cx="50%"
                  cy="50%"
                  labelLine={false}
                  label={({ sentiment, percentage }) => `${sentiment}: ${percentage}%`}
                  outerRadius={80}
                  fill="#8884d8"
                  dataKey="count"
                >
                  {sentimentData.map((entry, index) => (
                    <Cell
                      key={`cell-${index}`}
                      fill={SENTIMENT_COLORS[entry.sentiment.toLowerCase() as keyof typeof SENTIMENT_COLORS] || '#6b7280'}
                    />
                  ))}
                </Pie>
                <Tooltip
                  formatter={(value: any) => [`${value} đánh giá`]}
                  contentStyle={{
                    backgroundColor: 'white',
                    border: '1px solid #e5e7eb',
                    borderRadius: '8px',
                    fontSize: '14px',
                  }}
                />
              </PieChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {/* Summary Stats */}
      <div className="bg-white p-6 rounded-lg border border-gray-200 shadow-sm">
        <h4 className="text-lg font-semibold text-gray-900 mb-4">
          Thống Kê Tổng Quan
        </h4>
        <div className="grid grid-cols-2 gap-4">
          <div className="text-center p-4 bg-blue-50 rounded-lg">
            <div className="text-2xl font-bold text-blue-600">
              {data.avg_rating?.toFixed(1)}
            </div>
            <div className="text-sm text-gray-600">Rating trung bình</div>
          </div>
          <div className="text-center p-4 bg-green-50 rounded-lg">
            <div className="text-2xl font-bold text-green-600">
              {data.total_reviews?.toLocaleString('vi-VN')}
            </div>
            <div className="text-sm text-gray-600">Tổng đánh giá</div>
          </div>
        </div>
      </div>
    </div>
  );
}