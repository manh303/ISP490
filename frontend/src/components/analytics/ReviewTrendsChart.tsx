import { TrendingUp, Calendar, Star, MessageSquare } from 'lucide-react';
import { ReviewTrendData } from '../../services/analyticsApi';

interface ReviewTrendsChartProps {
  data: ReviewTrendData[];
  title?: string;
}

export function ReviewTrendsChart({ 
  data, 
  title = 'Xu Hướng Đánh Giá' 
}: ReviewTrendsChartProps) {
  if (!data || data.length === 0) {
    return (
      <div className="border border-gray-200 rounded-lg p-6 bg-white">
        <div className="flex items-center gap-2 mb-4">
          <TrendingUp className="h-5 w-5 text-green-500" />
          <h3 className="font-semibold text-gray-900">{title}</h3>
        </div>
        <div className="text-center py-8 text-gray-500">
          Không có dữ liệu
        </div>
      </div>
    );
  }

  const latestData = data[data.length - 1] || data[0];
  const avgRating = latestData?.avg_rating || 0;
  const totalReviews = latestData?.total_reviews || 0;
  const productsReviewed = latestData?.products_reviewed || 0;

  return (
    <div className="border border-gray-200 rounded-lg p-6 bg-white">
      <div className="flex items-center gap-2 mb-4">
        <TrendingUp className="h-5 w-5 text-green-500" />
        <h3 className="font-semibold text-gray-900">{title}</h3>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-3 gap-4 mb-6">
        <div className="bg-gradient-to-br from-blue-50 to-blue-100 rounded-lg p-4">
          <div className="flex items-center gap-2 mb-1">
            <Star className="h-4 w-4 text-blue-600" />
            <span className="text-xs text-blue-700 font-medium">Đánh giá TB</span>
          </div>
          <div className="text-2xl font-bold text-blue-900">
            {avgRating.toFixed(2)}
          </div>
        </div>

        <div className="bg-gradient-to-br from-purple-50 to-purple-100 rounded-lg p-4">
          <div className="flex items-center gap-2 mb-1">
            <MessageSquare className="h-4 w-4 text-purple-600" />
            <span className="text-xs text-purple-700 font-medium">Tổng đánh giá</span>
          </div>
          <div className="text-2xl font-bold text-purple-900">
            {totalReviews.toLocaleString('vi-VN')}
          </div>
        </div>

        <div className="bg-gradient-to-br from-green-50 to-green-100 rounded-lg p-4">
          <div className="flex items-center gap-2 mb-1">
            <Calendar className="h-4 w-4 text-green-600" />
            <span className="text-xs text-green-700 font-medium">Sản phẩm</span>
          </div>
          <div className="text-2xl font-bold text-green-900">
            {productsReviewed.toLocaleString('vi-VN')}
          </div>
        </div>
      </div>

      {/* Line Chart */}
      <div className="relative h-32 mb-4">
        <svg width="100%" height="100%" className="overflow-visible">
          {/* Grid lines */}
          {[0, 1, 2, 3, 4, 5].map((i) => (
            <line
              key={i}
              x1="0"
              y1={`${(i / 5) * 100}%`}
              x2="100%"
              y2={`${(i / 5) * 100}%`}
              stroke="#e5e7eb"
              strokeWidth="1"
              strokeDasharray="4 4"
            />
          ))}
          
          {/* Line path */}
          <polyline
            points={data.map((trend, index) => {
              const x = (index / (data.length - 1)) * 100;
              const y = 100 - (trend.avg_rating / 5) * 100;
              return `${x}%,${y}%`;
            }).join(' ')}
            fill="none"
            stroke="#3b82f6"
            strokeWidth="3"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
          
          {/* Data points */}
          {data.map((trend, index) => {
            const x = (index / (data.length - 1)) * 100;
            const y = 100 - (trend.avg_rating / 5) * 100;
            return (
              <g key={index}>
                <circle
                  cx={`${x}%`}
                  cy={`${y}%`}
                  r="4"
                  fill="#3b82f6"
                  stroke="white"
                  strokeWidth="2"
                />
              </g>
            );
          })}
        </svg>
      </div>

      {/* Timeline labels */}
      <div className="space-y-2">
        {data.map((trend, index) => (
          <div key={index} className="flex items-center justify-between text-xs">
            <div className="flex items-center gap-2">
              <div className="w-2 h-2 rounded-full bg-blue-500" />
              <span className="text-gray-600">
                {new Date(trend.date).toLocaleDateString('vi-VN', { month: 'short', day: 'numeric' })}
              </span>
            </div>
            <div className="flex items-center gap-3">
              <span className="text-gray-700">
                {trend.products_reviewed.toLocaleString('vi-VN')} SP
              </span>
              <div className="flex items-center gap-1">
                <Star className="h-3 w-3 text-yellow-500 fill-yellow-500" />
                <span className="font-semibold text-gray-900">{trend?.avg_rating?.toFixed(2)}</span>
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
