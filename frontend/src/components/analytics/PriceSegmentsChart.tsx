import { DollarSign, TrendingUp, Star } from 'lucide-react';
import { PriceSegmentData } from '../../services/analyticsApi';

interface PriceSegmentsChartProps {
  data: PriceSegmentData[];
  title?: string;
}

export function PriceSegmentsChart({ 
  data, 
  title = 'Phân Khúc Giá' 
}: PriceSegmentsChartProps) {
  if (!data || data.length === 0) {
    return (
      <div className="border border-gray-200 rounded-lg p-6 bg-white">
        <div className="flex items-center gap-2 mb-4">
          <DollarSign className="h-5 w-5 text-emerald-500" />
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

  const segmentColors: { [key: string]: { gradient: string; badge: string } } = {
    'Budget (<100K)': { gradient: 'from-green-500 to-green-600', badge: 'bg-green-100 text-green-800' },
    'Mid-range (100K-500K)': { gradient: 'from-blue-500 to-blue-600', badge: 'bg-blue-100 text-blue-800' },
    'Premium (500K-1M)': { gradient: 'from-purple-500 to-purple-600', badge: 'bg-purple-100 text-purple-800' },
    'Luxury (>1M)': { gradient: 'from-amber-500 to-amber-600', badge: 'bg-amber-100 text-amber-800' },
  };

  return (
    <div className="border border-gray-200 rounded-lg p-6 bg-white">
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-2">
          <DollarSign className="h-5 w-5 text-emerald-500" />
          <h3 className="font-semibold text-gray-900">{title}</h3>
        </div>
        <div className="text-sm text-gray-600">
          {(totalProducts || 0).toLocaleString('vi-VN')} sản phẩm
        </div>
      </div>

      <div className="space-y-3">
        {data.map((segment) => {
          const percentage = ((segment.product_count / totalProducts) * 100).toFixed(1);
          const barWidth = (segment.product_count / maxCount) * 100;
          const highRatedPercentage = ((segment.high_rated / segment.product_count) * 100).toFixed(0);
          const colors = segmentColors[segment.price_segment] || segmentColors['Mid-range (100K-500K)'];

          return (
            <div key={segment.price_segment} className="space-y-1">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <span className={`px-2 py-1 rounded text-xs font-medium ${colors.badge}`}>
                    {segment.price_segment}
                  </span>
                  <div className="flex items-center gap-1 text-xs text-gray-600">
                    <Star className="h-3 w-3 text-yellow-500 fill-yellow-500" />
                    <span>{segment.avg_rating.toFixed(2)}</span>
                  </div>
                </div>
                <span className="text-sm font-semibold text-gray-900">{percentage}%</span>
              </div>

              {/* Main bar with value inside */}
              <div className="relative h-4 bg-gray-100 rounded-lg overflow-hidden">
                <div
                  className={`absolute h-full bg-gradient-to-r ${colors.gradient} rounded-lg transition-all duration-700 flex items-center justify-end pr-2`}
                  style={{ width: `${barWidth}%` }}
                >
                  <span className="text-xs text-white font-bold opacity-90">
                    {(segment.product_count || 0).toLocaleString('vi-VN')}
                  </span>
                </div>
              </div>

              {/* Secondary bar for high rated */}
              <div className="relative h-1.5 bg-gray-50 rounded-full overflow-hidden">
                <div
                  className="absolute h-full bg-yellow-400 rounded-full transition-all duration-700"
                  style={{ width: `${(segment.high_rated / maxCount) * 100}%` }}
                />
              </div>

              <div className="flex justify-between text-xs text-gray-500">
                <span className="text-yellow-600">
                  {segment.high_rated} chất lượng cao ({highRatedPercentage}%)
                </span>
                <span>{(segment.total_reviews || 0).toLocaleString('vi-VN')} đánh giá</span>
              </div>
            </div>
          );
        })}
      </div>

      {/* Summary */}
      <div className="mt-6 pt-4 border-t border-gray-200">
        <div className="grid grid-cols-2 gap-4 text-sm">
          <div className="flex items-center gap-2">
            <TrendingUp className="h-4 w-4 text-green-500" />
            <span className="text-gray-600">Phân khúc phổ biến:</span>
          </div>
          <div className="font-semibold text-gray-900">
            {data.reduce((max, segment) => 
              segment.product_count > max.product_count ? segment : max
            ).price_segment}
          </div>
        </div>
      </div>
    </div>
  );
}
