import { Layers, TrendingUp, DollarSign, Star, ShoppingBag } from 'lucide-react';
import { CategoryPerformanceData } from '../../services/analyticsApi';

interface CategoryPerformanceChartProps {
  data: CategoryPerformanceData[];
  title?: string;
}

export function CategoryPerformanceChart({ 
  data, 
  title = 'Hiệu Suất Theo Danh Mục' 
}: CategoryPerformanceChartProps) {
  if (!data || data.length === 0) {
    return (
      <div className="border border-gray-200 rounded-lg p-6 bg-white">
        <div className="flex items-center gap-2 mb-4">
          <Layers className="h-5 w-5 text-indigo-500" />
          <h3 className="font-semibold text-gray-900">{title}</h3>
        </div>
        <div className="text-center py-8 text-gray-500">
          Không có dữ liệu
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

  return (
    <div className="border border-gray-200 rounded-lg p-6 bg-white">
      <div className="flex items-center gap-2 mb-4">
        <Layers className="h-5 w-5 text-indigo-500" />
        <h3 className="font-semibold text-gray-900">{title}</h3>
      </div>

      <div className="grid grid-cols-1 gap-4">
        {data.map((category, index) => (
          <div 
            key={category.category}
            className="border border-gray-200 rounded-lg p-4 hover:shadow-md transition-shadow"
          >
            <div className="flex items-center justify-between mb-3">
              <div className="flex items-center gap-2">
                <span className="text-2xl">{categoryIcons[category.category] || '📦'}</span>
                <h4 className="font-semibold text-gray-900 capitalize">
                  {category.category}
                </h4>
              </div>
              <div className="flex items-center gap-1">
                <Star className="h-4 w-4 text-yellow-500 fill-yellow-500" />
                <span className="font-bold text-gray-900">
                  {category.avg_rating.toFixed(2)}
                </span>
              </div>
            </div>

            <div className="grid grid-cols-4 gap-3 text-xs">
              <div className="bg-blue-50 rounded p-2">
                <div className="flex items-center gap-1 mb-1">
                  <ShoppingBag className="h-3 w-3 text-blue-600" />
                  <span className="text-blue-700 font-medium">Sản phẩm</span>
                </div>
                <div className="font-bold text-blue-900">
                  {category.product_count.toLocaleString('vi-VN')}
                </div>
              </div>

              <div className="bg-green-50 rounded p-2">
                <div className="flex items-center gap-1 mb-1">
                  <DollarSign className="h-3 w-3 text-green-600" />
                  <span className="text-green-700 font-medium">Giá TB</span>
                </div>
                <div className="font-bold text-green-900 text-[10px]">
                  {(category.avg_price / 1000000).toFixed(1)}M ₫
                </div>
              </div>

              <div className="bg-purple-50 rounded p-2">
                <div className="flex items-center gap-1 mb-1">
                  <TrendingUp className="h-3 w-3 text-purple-600" />
                  <span className="text-purple-700 font-medium">Đánh giá</span>
                </div>
                <div className="font-bold text-purple-900 text-[10px]">
                  {(category.total_reviews / 1000).toFixed(0)}K
                </div>
              </div>

              <div className="bg-yellow-50 rounded p-2">
                <div className="flex items-center gap-1 mb-1">
                  <Star className="h-3 w-3 text-yellow-600" />
                  <span className="text-yellow-700 font-medium">Chất lượng</span>
                </div>
                <div className="font-bold text-yellow-900">
                  {category.high_rated_count}
                </div>
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
