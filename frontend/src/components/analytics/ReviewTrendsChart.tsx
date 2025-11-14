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
  const avgRating = (latestData?.avg_rating ?? 0);
  const totalReviews = (latestData?.total_reviews ?? 0);
  const productsReviewed = (latestData?.products_reviewed ?? 0);

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
            {(totalReviews || 0).toLocaleString('vi-VN')}
          </div>
        </div>

        <div className="bg-gradient-to-br from-green-50 to-green-100 rounded-lg p-4">
          <div className="flex items-center gap-2 mb-1">
            <Calendar className="h-4 w-4 text-green-600" />
            <span className="text-xs text-green-700 font-medium">Sản phẩm</span>
          </div>
          <div className="text-2xl font-bold text-green-900">
            {(productsReviewed || 0).toLocaleString('vi-VN')}
          </div>
        </div>
      </div>

      {/* Timeline */}
      <div className="space-y-3">
        {data.map((trend, index) => (
          <div key={index} className="flex items-center gap-3">
            <div className="text-xs text-gray-500 w-24 flex-shrink-0">
              {new Date(trend.date).toLocaleDateString('vi-VN')}
            </div>
            <div className="flex-1 space-y-1">
              <div className="flex items-center justify-between text-sm">
                <span className="text-gray-700">
                  {((trend.products_reviewed || 0) || 0).toLocaleString('vi-VN')} sản phẩm
                </span>
                <div className="flex items-center gap-2">
                  <Star className="h-3 w-3 text-yellow-500 fill-yellow-500" />
                  <span className="font-semibold text-gray-900">
                    {(trend.avg_rating || 0).toFixed(2)}
                  </span>
                </div>
              </div>
              <div className="h-1 bg-gray-100 rounded-full overflow-hidden">
                <div
                  className="h-full bg-gradient-to-r from-green-500 to-green-600 rounded-full"
                  style={{ width: `${(trend.avg_rating / 5) * 100}%` }}
                />
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
