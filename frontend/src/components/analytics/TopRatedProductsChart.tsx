import { BarChart3, Star, TrendingUp } from 'lucide-react';
import { TopRatedProduct } from '../../services/analyticsApi';

interface TopRatedProductsChartProps {
  data: TopRatedProduct[];
  title?: string;
}

export function TopRatedProductsChart({ data, title = 'Top Sản Phẩm Đánh Giá Cao' }: TopRatedProductsChartProps) {
  if (!data || data.length === 0) {
    return (
      <div className="border border-gray-200 rounded-lg p-6 bg-white">
        <div className="flex items-center gap-2 mb-4">
          <BarChart3 className="h-5 w-5 text-blue-500" />
          <h3 className="font-semibold text-gray-900">{title}</h3>
        </div>
        <div className="text-center py-8 text-gray-500">
          Không có dữ liệu
        </div>
      </div>
    );
  }

  const maxRating = Math.max(...data.map(p => p.rating_avg));

  return (
    <div className="border border-gray-200 rounded-lg p-6 bg-white">
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-2">
          <BarChart3 className="h-5 w-5 text-blue-500" />
          <h3 className="font-semibold text-gray-900">{title}</h3>
        </div>
        <div className="flex items-center gap-1 text-sm text-gray-600">
          <TrendingUp className="h-4 w-4" />
          <span>{data.length} sản phẩm</span>
        </div>
      </div>

      <div className="space-y-3">
        {data.map((product, index) => (
          <div key={index} className="space-y-1">
            <div className="flex items-center justify-between text-sm">
              <span className="text-gray-700 font-medium truncate max-w-[200px]" title={product.product_name}>
                {index + 1}. {product.product_name}
              </span>
              <div className="flex items-center gap-1">
                <Star className="h-3 w-3 text-yellow-500 fill-yellow-500" />
                <span className="font-semibold text-gray-900">{product.rating_avg.toFixed(1)}</span>
              </div>
            </div>
            <div className="relative h-2 bg-gray-100 rounded-full overflow-hidden">
              <div
                className="absolute h-full bg-gradient-to-r from-blue-500 to-blue-600 rounded-full transition-all duration-500"
                style={{ width: `${(product.rating_avg / maxRating) * 100}%` }}
              />
            </div>
            <div className="flex justify-between text-xs text-gray-500">
              <span>{(product.review_count || 0).toLocaleString('vi-VN')} đánh giá</span>
              <span>{(product.price || 0).toLocaleString('vi-VN')} ₫</span>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
