import { GitCompare, BarChart3, Star, ShoppingBag, TrendingUp } from 'lucide-react';
import { PlatformComparisonItem } from '../../services/analyticsApi';

interface PlatformComparisonChartProps {
  data: PlatformComparisonItem[];
  title?: string;
}

export function PlatformComparisonChart({ 
  data, 
  title = 'Platform Comparison' 
}: PlatformComparisonChartProps) {
  if (!data || data.length === 0) {
    return (
      <div className="border border-gray-200 rounded-lg p-6 bg-white">
        <div className="flex items-center gap-2 mb-4">
          <GitCompare className="h-5 w-5 text-cyan-500" />
          <h3 className="font-semibold text-gray-900">{title}</h3>
        </div>
        <div className="text-center py-8 text-gray-500">
          No data available
        </div>
      </div>
    );
  }

  const platformIcons: { [key: string]: string } = {
    'tiki': 'Tiki 🛒',
    'lazada': 'Lazada 🛍️',
    'shopee': '🏪',
    'sendo': '📦',
  };

  const maxProductCount = Math.max(...data.map(p => p?.total_products));
  const maxReviews = Math.max(...data.map(p => p?.total_reviews));

  return (
    <div className="border border-gray-200 rounded-lg p-6 bg-white">
      <div className="flex items-center gap-2 mb-4">
        <GitCompare className="h-5 w-5 text-cyan-500" />
        <h3 className="font-semibold text-gray-900">{title}</h3>
      </div>

      {/* Grouped Bar Chart */}
      <div className="space-y-4">
        {data.map((platform) => {
          const productPercentage = (platform?.total_products / maxProductCount) * 100;
          const reviewPercentage = (platform?.total_reviews / maxReviews) * 100;
          const ratingPercentage = ((platform?.avg_rating || 0) / 5) * 100;

          return (
            <div key={platform.platform_code} className="space-y-2">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <span className="text-xl">{platformIcons[platform.platform_code.toLowerCase()] || '🏬'}</span>
                  <span className="font-semibold text-gray-900 capitalize text-sm">
                    {platform.platform_name}
                  </span>
                </div>
                <div className="flex items-center gap-1">
                  <Star className="h-3 w-3 text-yellow-500 fill-yellow-500" />
                  <span className="font-bold text-gray-900 text-sm">{(platform.avg_rating || 0).toFixed(2)}</span>
                </div>
              </div>

              {/* Three grouped bars */}
              <div className="space-y-1.5">
                {/* Product Count */}
                <div className="flex items-center gap-2">
                  <div className="w-16 text-xs text-gray-600 flex items-center gap-1">
                    <ShoppingBag className="h-3 w-3" />
                    <span>Products</span>
                  </div>
                  <div className="flex-1 relative h-2.5 bg-gray-100 rounded-full overflow-hidden">
                    <div
                      className="absolute h-full bg-gradient-to-r from-blue-500 to-blue-600 rounded-full transition-all duration-700"
                      style={{ width: `${productPercentage}%` }}
                    />
                  </div>
                  <span className="w-16 text-xs font-medium text-gray-900 text-right">
                    {platform.total_products.toLocaleString('vi-VN')}
                  </span>
                </div>

                {/* Reviews */}
                <div className="flex items-center gap-2">
                  <div className="w-16 text-xs text-gray-600 flex items-center gap-1">
                    <TrendingUp className="h-3 w-3" />
                    <span>Reviews</span>
                  </div>
                  <div className="flex-1 relative h-2.5 bg-gray-100 rounded-full overflow-hidden">
                    <div
                      className="absolute h-full bg-gradient-to-r from-purple-500 to-purple-600 rounded-full transition-all duration-700"
                      style={{ width: `${reviewPercentage}%` }}
                    />
                  </div>
                  <span className="w-16 text-xs font-medium text-gray-900 text-right">
                    {(platform.total_reviews / 1000).toFixed(0)}K
                  </span>
                </div>

                {/* Rating */}
                <div className="flex items-center gap-2">
                  <div className="w-16 text-xs text-gray-600 flex items-center gap-1">
                    <Star className="h-3 w-3" />
                    <span>Rating</span>
                  </div>
                  <div className="flex-1 relative h-2.5 bg-gray-100 rounded-full overflow-hidden">
                    <div
                      className="absolute h-full bg-gradient-to-r from-yellow-400 to-yellow-500 rounded-full transition-all duration-700"
                      style={{ width: `${ratingPercentage}%` }}
                    />
                  </div>
                  <span className="w-16 text-xs font-medium text-gray-900 text-right">
                    {(platform.avg_rating || 0).toFixed(2)}
                  </span>
                </div>
              </div>

              <div className="flex justify-between text-xs text-gray-500 pt-1">
                <span>Avg Price: {((platform.avg_price || 0) / 1000000).toFixed(1)}M ₫</span>
                <span>Revenue: {((platform.total_revenue || 0) / 1000000000).toFixed(1)}B ₫</span>
              </div>
            </div>
          );
        })}
      </div>

      {/* Summary */}
      {data.length > 1 && (
        <div className="mt-4 pt-3 border-t border-gray-200">
          <div className="text-xs text-gray-600">
            <BarChart3 className="h-4 w-4 inline mr-1" />
            Total: {data.reduce((sum, p) => sum + p.total_products, 0).toLocaleString('en-US')} products
          </div>
        </div>
      )}
    </div>
  );
}
