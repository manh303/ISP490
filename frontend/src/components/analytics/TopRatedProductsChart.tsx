import { BarChart3, Star, TrendingUp } from 'lucide-react';
import { TopProduct } from '../../services/analyticsApi';

interface TopRatedProductsChartProps {
  data: TopProduct[];
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

  const maxRating = 5; // Fixed scale for consistency

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

      {/* Horizontal Bar Chart */}
      <div className="space-y-3">
        {data.map((product, index) => (
          <div key={index} className="space-y-1">
            <div className="flex items-center justify-between text-sm">
              <span className="text-gray-700 font-medium truncate max-w-[180px]" title={product.product_name}>
                {index + 1}. {product.product_name}
              </span>
              <div className="flex items-center gap-1">
                <Star className="h-3 w-3 text-yellow-500 fill-yellow-500" />
                <span className="font-semibold text-gray-900">{product?.avg_rating?.toFixed(2)}</span>
              </div>
            </div>
            <div className="relative h-3 bg-gray-100 rounded-lg overflow-hidden">
              <div
                className="absolute h-full bg-gradient-to-r from-blue-500 to-blue-600 rounded-lg transition-all duration-700 flex items-center justify-end pr-2"
                style={{ width: `${(product.avg_rating / maxRating) * 100}%` }}
              >
                <span className="text-[10px] text-white font-bold opacity-80">
                  {product?.avg_rating?.toFixed(1)}
                </span>
              </div>
            </div>
            <div className="flex justify-between text-xs text-gray-500">
              <span>{product?.total_reviews?.toLocaleString('vi-VN')} đánh giá</span>
              <span>
                {product?.avg_price
                  ? product.avg_price < 1_000_000
                    ? `${Math.round(product.avg_price / 1000)}k ₫`
                    : `${(product.avg_price / 1_000_000).toFixed(1)}M ₫`
                  : 'N/A'}
              </span>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
