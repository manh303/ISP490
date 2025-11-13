import { GitCompare, BarChart3, Star, ShoppingBag, TrendingUp } from 'lucide-react';
import { PlatformComparisonData } from '../../services/analyticsApi';

interface PlatformComparisonChartProps {
  data: PlatformComparisonData[];
  title?: string;
}

export function PlatformComparisonChart({ 
  data, 
  title = 'So Sánh Nền Tảng' 
}: PlatformComparisonChartProps) {
  if (!data || data.length === 0) {
    return (
      <div className="border border-gray-200 rounded-lg p-6 bg-white">
        <div className="flex items-center gap-2 mb-4">
          <GitCompare className="h-5 w-5 text-cyan-500" />
          <h3 className="font-semibold text-gray-900">{title}</h3>
        </div>
        <div className="text-center py-8 text-gray-500">
          Không có dữ liệu
        </div>
      </div>
    );
  }

  const platformIcons: { [key: string]: string } = {
    'tiki': '🛒',
    'lazada': '🛍️',
    'shopee': '🏪',
    'sendo': '📦',
  };

  const maxProductCount = Math.max(...data.map(p => p.product_count));
  const maxReviews = Math.max(...data.map(p => p.total_reviews));

  return (
    <div className="border border-gray-200 rounded-lg p-6 bg-white">
      <div className="flex items-center gap-2 mb-4">
        <GitCompare className="h-5 w-5 text-cyan-500" />
        <h3 className="font-semibold text-gray-900">{title}</h3>
      </div>

      <div className="space-y-4">
        {data.map((platform, index) => {
          const productPercentage = (platform.product_count / maxProductCount) * 100;
          const reviewPercentage = (platform.total_reviews / maxReviews) * 100;

          return (
            <div key={platform.platform} className="space-y-2">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <span className="text-2xl">{platformIcons[platform.platform.toLowerCase()] || '🏬'}</span>
                  <div>
                    <h4 className="font-semibold text-gray-900 capitalize">
                      {platform.platform}
                    </h4>
                    <div className="flex items-center gap-1 text-xs text-gray-600">
                      <Star className="h-3 w-3 text-yellow-500 fill-yellow-500" />
                      <span>{platform.avg_rating.toFixed(2)}</span>
                    </div>
                  </div>
                </div>
                <div className="text-right">
                  <div className="text-sm font-bold text-gray-900">
                    {platform.product_count.toLocaleString('vi-VN')}
                  </div>
                  <div className="text-xs text-gray-500">sản phẩm</div>
                </div>
              </div>

              {/* Product Count Bar */}
              <div className="space-y-1">
                <div className="flex items-center justify-between text-xs text-gray-600">
                  <span className="flex items-center gap-1">
                    <ShoppingBag className="h-3 w-3" />
                    Số lượng SP
                  </span>
                  <span>{productPercentage.toFixed(0)}%</span>
                </div>
                <div className="relative h-2 bg-gray-100 rounded-full overflow-hidden">
                  <div
                    className="absolute h-full bg-gradient-to-r from-blue-500 to-blue-600 rounded-full transition-all duration-500"
                    style={{ width: `${productPercentage}%` }}
                  />
                </div>
              </div>

              {/* Reviews Bar */}
              <div className="space-y-1">
                <div className="flex items-center justify-between text-xs text-gray-600">
                  <span className="flex items-center gap-1">
                    <TrendingUp className="h-3 w-3" />
                    Đánh giá
                  </span>
                  <span>{(platform.total_reviews / 1000).toFixed(0)}K</span>
                </div>
                <div className="relative h-2 bg-gray-100 rounded-full overflow-hidden">
                  <div
                    className="absolute h-full bg-gradient-to-r from-purple-500 to-purple-600 rounded-full transition-all duration-500"
                    style={{ width: `${reviewPercentage}%` }}
                  />
                </div>
              </div>

              {/* Stats Grid */}
              <div className="grid grid-cols-3 gap-2 mt-2">
                <div className="bg-blue-50 rounded p-2 text-center">
                  <div className="text-xs text-blue-700 font-medium">Giá TB</div>
                  <div className="text-sm font-bold text-blue-900">
                    {(platform.avg_price / 1000000).toFixed(1)}M ₫
                  </div>
                </div>
                <div className="bg-green-50 rounded p-2 text-center">
                  <div className="text-xs text-green-700 font-medium">Chất lượng</div>
                  <div className="text-sm font-bold text-green-900">
                    {platform.high_rated_count}
                  </div>
                </div>
                <div className="bg-purple-50 rounded p-2 text-center">
                  <div className="text-xs text-purple-700 font-medium">Tổng ĐG</div>
                  <div className="text-sm font-bold text-purple-900">
                    {(platform.total_reviews / 1000).toFixed(0)}K
                  </div>
                </div>
              </div>

              {index < data.length - 1 && <div className="border-t border-gray-200 mt-3" />}
            </div>
          );
        })}
      </div>

      {/* Summary */}
      {data.length > 1 && (
        <div className="mt-4 pt-3 border-t border-gray-200">
          <div className="text-xs text-gray-600">
            <BarChart3 className="h-4 w-4 inline mr-1" />
            Tổng: {data.reduce((sum, p) => sum + p.product_count, 0).toLocaleString('vi-VN')} sản phẩm
          </div>
        </div>
      )}
    </div>
  );
}
