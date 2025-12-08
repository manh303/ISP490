import { Layers, TrendingUp, DollarSign, Star, ShoppingBag } from 'lucide-react';
import { CategoryPerformanceData } from '../../services/analyticsApi';

interface CategoryPerformanceChartProps {
  data: CategoryPerformanceData[];
  title?: string;
}

export function CategoryPerformanceChart({
  data,
  title = 'Category Performance'
}: CategoryPerformanceChartProps) {
  if (!data || data.length === 0) {
    return (
      <div className="border border-gray-200 rounded-lg p-6 bg-white">
        <div className="flex items-center gap-2 mb-4">
          <Layers className="h-5 w-5 text-indigo-500" />
          <h3 className="font-semibold text-gray-900">{title}</h3>
        </div>
        <div className="text-center py-8 text-gray-500">
          No data available
        </div>
      </div>
    );
  }

  const categoryIcons: { [key: string]: string } = {
    laptops: '💻',
    smartphones: '📱',
    tablets: '📱',
    headphones: '🎧',
    smartwatches: '⌚',
  };

  const maxCount = Math.max(...data.map(c => c.product_count));

  return (
    <div className="border border-gray-200 rounded-lg p-6 bg-white">
      <div className="flex items-center gap-2 mb-4">
        <Layers className="h-5 w-5 text-indigo-500" />
        <h3 className="font-semibold text-gray-900">{title}</h3>
      </div>

      {/* Bar Chart */}
      <div className="space-y-3">
        {data.map((category, index) => {
          const barWidth = (category.product_count / maxCount) * 100;
          const ratingPercentage = (category.avg_rating / 5) * 100;
          
          return (
            <div key={`${category.category}-${index}`} className="space-y-1">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <span className="text-xl">{categoryIcons[category.category] || '📦'}</span>
                  <span className="font-medium text-gray-900 capitalize text-sm">
                    {category.category}
                  </span>
                </div>
                <div className="flex items-center gap-1">
                  <Star className="h-3 w-3 text-yellow-500 fill-yellow-500" />
                  <span className="font-bold text-gray-900 text-sm">
                    {(category.avg_rating || 0).toFixed(2)}
                  </span>
                </div>
              </div>
              
              {/* Product count bar */}
              <div className="relative h-3 bg-gray-100 rounded-lg overflow-hidden">
                <div
                  className="absolute h-full bg-gradient-to-r from-indigo-500 to-indigo-600 rounded-lg transition-all duration-700"
                  style={{ width: `${barWidth}%` }}
                />
              </div>
              
              {/* Rating bar */}
              <div className="relative h-1.5 bg-gray-50 rounded-full overflow-hidden">
                <div
                  className="absolute h-full bg-yellow-400 rounded-full transition-all duration-700"
                  style={{ width: `${ratingPercentage}%` }}
                />
              </div>
              
              <div className="flex justify-between text-xs text-gray-600">
                <span>{(category.product_count || 0).toLocaleString('en-US')} products</span>
                <span>{category.high_rated_count} high-rated</span>
                <span>{(category.total_reviews / 1000).toFixed(0)}K reviews</span>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
