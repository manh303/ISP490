import { BarChart2, Star } from 'lucide-react';
import { RatingDistributionData } from '../../services/analyticsApi';

interface RatingDistributionChartProps {
  data: RatingDistributionData[];
  title?: string;
}

export function RatingDistributionChart({ 
  data, 
  title = 'Phân Bố Đánh Giá' 
}: RatingDistributionChartProps) {
  if (!data || data.length === 0) {
    return (
      <div className="border border-gray-200 rounded-lg p-6 bg-white">
        <div className="flex items-center gap-2 mb-4">
          <BarChart2 className="h-5 w-5 text-purple-500" />
          <h3 className="font-semibold text-gray-900">{title}</h3>
        </div>
        <div className="text-center py-8 text-gray-500">
          Không có dữ liệu
        </div>
      </div>
    );
  }

  const maxCount = Math.max(...data.map(d => d.product_count));
  const totalProducts = data.reduce((sum, d) => sum + d.product_count, 0);

  return (
    <div className="border border-gray-200 rounded-lg p-6 bg-white">
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-2">
          <BarChart2 className="h-5 w-5 text-purple-500" />
          <h3 className="font-semibold text-gray-900">{title}</h3>
        </div>
        <div className="text-sm text-gray-600">
          {totalProducts.toLocaleString('vi-VN')} sản phẩm
        </div>
      </div>

      <div className="space-y-4">
        {data.map((item) => {
          const percentage = ((item.product_count / totalProducts) * 100).toFixed(1);
          const barWidth = (item.product_count / maxCount) * 100;

          return (
            <div key={item.rating_bucket} className="space-y-1">
              <div className="flex items-center justify-between text-sm">
                <div className="flex items-center gap-2">
                  <div className="flex items-center gap-1 w-16">
                    {item.rating_bucket > 0 ? (
                      <>
                        <Star className="h-3 w-3 text-yellow-500 fill-yellow-500" />
                        <span className="font-medium text-gray-900">{item.rating_bucket}</span>
                      </>
                    ) : (
                      <span className="font-medium text-gray-500">Chưa đánh giá</span>
                    )}
                  </div>
                  <span className="text-gray-600">
                    {item.product_count.toLocaleString('vi-VN')} SP
                  </span>
                </div>
                <span className="font-semibold text-gray-900">{percentage}%</span>
              </div>
              <div className="relative h-2 bg-gray-100 rounded-full overflow-hidden">
                <div
                  className={`absolute h-full rounded-full transition-all duration-500 ${
                    item.rating_bucket >= 4
                      ? 'bg-gradient-to-r from-green-500 to-green-600'
                      : item.rating_bucket >= 3
                      ? 'bg-gradient-to-r from-blue-500 to-blue-600'
                      : item.rating_bucket >= 2
                      ? 'bg-gradient-to-r from-yellow-500 to-yellow-600'
                      : 'bg-gradient-to-r from-gray-400 to-gray-500'
                  }`}
                  style={{ width: `${barWidth}%` }}
                />
              </div>
              <div className="flex justify-between text-xs text-gray-500">
                <span>Giá TB: {item.avg_price ? item.avg_price.toLocaleString("vi-VN") : "N/A"} ₫</span>
                <span>{(item.total_reviews || 0).toLocaleString('vi-VN')} đánh giá</span>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
