import { DollarSign, TrendingDown, TrendingUp, BarChart4 } from 'lucide-react';
import { PlatformPriceComparisonData } from '../../services/analyticsApi';

interface PlatformPriceComparisonChartProps {
  data: PlatformPriceComparisonData[];
  title?: string;
}

export function PlatformPriceComparisonChart({
  data,
  title = 'So Sánh Giá Theo Nền Tảng'
}: PlatformPriceComparisonChartProps) {
  if (!data || data.length === 0) {
    return (
      <div className="border border-gray-200 rounded-lg p-6 bg-white">
        <div className="flex items-center gap-2 mb-4">
          <BarChart4 className="h-5 w-5 text-teal-500" />
          <h3 className="font-semibold text-gray-900">{title}</h3>
        </div>
        <div className="text-center py-8 text-gray-500">
          Không có dữ liệu
        </div>
      </div>
    );
  }

  // Group by category
  const categoriesMap = new Map<string, PlatformPriceComparisonData[]>();
  data.forEach(item => {
    if (!categoriesMap.has(item.category)) {
      categoriesMap.set(item.category, []);
    }
    categoriesMap.get(item.category)!.push(item);
  });

  const categories = Array.from(categoriesMap.entries());
  const maxPrice = Math.max(...data.map(d => d.max_price));

  const categoryIcons: { [key: string]: string } = {
    laptops: '💻',
    smartphones: '📱',
    tablets: '📱',
    headphones: '🎧',
    smartwatches: '⌚',
  };

  return (
    <div className="border border-gray-200 rounded-lg p-6 bg-white">
      <div className="flex items-center gap-2 mb-4">
        <BarChart4 className="h-5 w-5 text-teal-500" />
        <h3 className="font-semibold text-gray-900">{title}</h3>
      </div>

      <div className="space-y-4 max-h-[350px] overflow-y-auto">
        {categories.map(([category, platforms]) => {
          const avgPrices = platforms.map(p => p.avg_price);
          const minAvgPrice = Math.min(...avgPrices);
          const maxAvgPrice = Math.max(...avgPrices);
          const priceDiff = maxAvgPrice - minAvgPrice;
          const diffPercentage = ((priceDiff / minAvgPrice) * 100).toFixed(0);

          return (
            <div key={category} className="border border-gray-200 rounded-lg p-3">
              <div className="flex items-center justify-between mb-2">
                <div className="flex items-center gap-2">
                  <span className="text-xl">{categoryIcons[category] || '📦'}</span>
                  <h4 className="font-semibold text-gray-900 capitalize text-sm">
                    {category}
                  </h4>
                </div>
                {priceDiff > 0 && (
                  <div className="flex items-center gap-1 text-xs text-orange-600 bg-orange-50 px-2 py-1 rounded">
                    <TrendingUp className="h-3 w-3" />
                    <span>±{diffPercentage}%</span>
                  </div>
                )}
              </div>

              <div className="space-y-2">
                {platforms.map((item, index) => {
                  const barWidth = (item.avg_price / maxPrice) * 100;
                  const priceRange = item.max_price - item.min_price;
                  const rangePercentage = ((priceRange / item.avg_price) * 100).toFixed(0);

                  return (
                    <div key={index} className="space-y-1">
                      <div className="flex items-center justify-between text-xs">
                        <span className="text-gray-700 font-medium capitalize">
                          {item.platform}
                        </span>
                        <div className="flex items-center gap-2">
                          <span className="text-gray-600">
                            {item.product_count} SP
                          </span>
                          <span className="font-bold text-gray-900">
                            {item.avg_price < 1_000_000
                              ? `${Math.round(item.avg_price / 1000)}K ₫`
                              : `${(item.avg_price / 1_000_000).toFixed(1)}M ₫`
                            }
                          </span>
                        </div>
                      </div>

                      {/* Average Price Bar with value */}
                      <div className="relative h-3 bg-gray-100 rounded-lg overflow-hidden">
                        <div
                          className="absolute h-full bg-gradient-to-r from-teal-500 to-teal-600 rounded-lg transition-all duration-700 flex items-center justify-end pr-1"
                          style={{ width: `${barWidth}%` }}
                        >
                          <span className="text-[10px] text-white font-bold opacity-80">
                            {(item.avg_price / 1000000).toFixed(1)}M
                          </span>
                        </div>
                      </div>

                      {/* Price Range Indicator */}
                      <div className="flex justify-between text-[10px] text-gray-500">
                        <span className="flex items-center gap-1">
                          <TrendingDown className="h-2 w-2" />
                          Min: {(item.min_price / 1000000).toFixed(1)}M ₫
                        </span>
                        <span className="text-orange-600 font-medium">
                          Biên độ: {rangePercentage}%
                        </span>
                        <span className="flex items-center gap-1">
                          <TrendingUp className="h-2 w-2" />
                          Max: {(item.max_price / 1000000).toFixed(1)}M ₫
                        </span>
                      </div>
                    </div>
                  );
                })}
              </div>

              {/* Category Summary */}
              <div className="mt-2 pt-2 border-t border-gray-100 flex justify-between text-[10px] text-gray-600">
                <span>
                  Tổng: {platforms.reduce((sum, p) => sum + p.product_count, 0)} sản phẩm
                </span>
                {priceDiff > 0 && (
                  <span className="text-orange-600">
                    Chênh lệch: {(priceDiff / 1000000).toFixed(1)}M ₫
                  </span>
                )}
              </div>
            </div>
          );
        })}
      </div>

      {/* Legend */}
      <div className="mt-4 pt-3 border-t border-gray-200">
        <div className="grid grid-cols-2 gap-2 text-xs text-gray-600">
          <div className="flex items-center gap-1">
            <DollarSign className="h-3 w-3" />
            <span>Thanh màu = Giá trung bình</span>
          </div>
          <div className="flex items-center gap-1">
            <TrendingUp className="h-3 w-3 text-orange-500" />
            <span>Biên độ = Khoảng giá</span>
          </div>
        </div>
      </div>
    </div>
  );
}
