import { Sparkles, DollarSign, Star, TrendingUp } from 'lucide-react';
import { PriceVsRatingData } from '../../services/analyticsApi';

interface PriceVsRatingChartProps {
  data: PriceVsRatingData[];
  title?: string;
}

export function PriceVsRatingChart({ 
  data, 
  title = 'Giá vs Đánh Giá' 
}: PriceVsRatingChartProps) {
  if (!data || data.length === 0) {
    return (
      <div className="border border-gray-200 rounded-lg p-6 bg-white">
        <div className="flex items-center gap-2 mb-4">
          <Sparkles className="h-5 w-5 text-orange-500" />
          <h3 className="font-semibold text-gray-900">{title}</h3>
        </div>
        <div className="text-center py-8 text-gray-500">
          Không có dữ liệu
        </div>
      </div>
    );
  }

  // Calculate price ranges for visualization
  const maxPrice = Math.max(...data.map(p => p.price));
  const maxRating = 5;

  // Group data into price segments
  const priceSegments = [
    { label: 'Rẻ', min: 0, max: maxPrice * 0.25, color: 'bg-green-500' },
    { label: 'TB', min: maxPrice * 0.25, max: maxPrice * 0.5, color: 'bg-blue-500' },
    { label: 'Cao', min: maxPrice * 0.5, max: maxPrice * 0.75, color: 'bg-purple-500' },
    { label: 'Xa xỉ', min: maxPrice * 0.75, max: maxPrice, color: 'bg-amber-500' },
  ];

  // Sample top 10 products for display
  const displayProducts = data.slice(0, 10);

  // Calculate correlation insight
  const avgPriceHighRated = data
    .filter(p => p.rating_avg >= 4)
    .reduce((sum, p) => sum + p.price, 0) / data.filter(p => p.rating_avg >= 4).length || 0;

  const avgPriceLowRated = data
    .filter(p => p.rating_avg < 4)
    .reduce((sum, p) => sum + p.price, 0) / data.filter(p => p.rating_avg < 4).length || 0;

  const correlation = avgPriceHighRated > avgPriceLowRated ? 'positive' : 'negative';

  return (
    <div className="border border-gray-200 rounded-lg p-6 bg-white">
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-2">
          <Sparkles className="h-5 w-5 text-orange-500" />
          <h3 className="font-semibold text-gray-900">{title}</h3>
        </div>
        <div className="text-xs text-gray-500">
          {data.length} sản phẩm
        </div>
      </div>

      {/* Correlation Insight */}
      <div className={`mb-4 p-3 rounded-lg ${correlation === 'positive' ? 'bg-green-50' : 'bg-orange-50'}`}>
        <div className="flex items-center gap-2 mb-1">
          <TrendingUp className={`h-4 w-4 ${correlation === 'positive' ? 'text-green-600' : 'text-orange-600'}`} />
          <span className={`text-sm font-medium ${correlation === 'positive' ? 'text-green-900' : 'text-orange-900'}`}>
            Tương quan: {correlation === 'positive' ? 'Tích cực' : 'Tiêu cực'}
          </span>
        </div>
        <p className="text-xs text-gray-600">
          Sản phẩm giá cao (≥4⭐): {(avgPriceHighRated / 1000000).toFixed(1)}M ₫
        </p>
        <p className="text-xs text-gray-600">
          Sản phẩm giá thấp (&lt;4⭐): {(avgPriceLowRated / 1000000).toFixed(1)}M ₫
        </p>
      </div>

      {/* Scatter Plot Visualization */}
      <div className="relative h-56 bg-gray-50 rounded-lg p-4 mb-4">
        <svg width="100%" height="100%" className="overflow-visible">
          {/* Y-axis (Rating) grid lines */}
          {[0, 1, 2, 3, 4, 5].map((rating) => (
            <g key={rating}>
              <line
                x1="8%"
                y1={`${100 - (rating / 5) * 90}%`}
                x2="100%"
                y2={`${100 - (rating / 5) * 90}%`}
                stroke="#e5e7eb"
                strokeWidth="1"
                strokeDasharray="2 2"
              />
              <text
                x="2%"
                y={`${100 - (rating / 5) * 90}%`}
                fontSize="10"
                fill="#6b7280"
                alignmentBaseline="middle"
              >
                {rating}
              </text>
            </g>
          ))}
          
          {/* X-axis (Price) */}
          <line x1="8%" y1="95%" x2="100%" y2="95%" stroke="#9ca3af" strokeWidth="1" />
          <line x1="8%" y1="5%" x2="8%" y2="95%" stroke="#9ca3af" strokeWidth="1" />
          
          {/* Data points */}
          {displayProducts.map((product, index) => {
            const x = 8 + ((product.price / maxPrice) * 90);
            const y = 95 - ((product.rating_avg / 5) * 90);
            const segment = priceSegments.find(s => product.price >= s.min && product.price <= s.max);
            const color = segment?.label === 'Rẻ' ? '#22c55e' :
                         segment?.label === 'TB' ? '#3b82f6' :
                         segment?.label === 'Cao' ? '#a855f7' : '#f59e0b';
            
            return (
              <g key={index}>
                <circle
                  cx={`${x}%`}
                  cy={`${y}%`}
                  r="4"
                  fill={color}
                  opacity="0.8"
                  stroke="white"
                  strokeWidth="1.5"
                />
              </g>
            );
          })}
        </svg>
        
        {/* Axis labels */}
        <div className="absolute bottom-0 left-1/2 transform -translate-x-1/2 text-xs text-gray-600">
          Giá (VNĐ)
        </div>
        <div className="absolute left-0 top-1/2 transform -translate-y-1/2 -rotate-90 text-xs text-gray-600">
          Rating
        </div>
      </div>

      {/* Legend */}
      <div className="mt-4 pt-3 border-t border-gray-200">
        <div className="grid grid-cols-4 gap-2 text-xs">
          {priceSegments.map((segment) => (
            <div key={segment.label} className="flex items-center gap-1">
              <div className={`w-2 h-2 rounded-full ${segment.color}`} />
              <span className="text-gray-600">{segment.label}</span>
            </div>
          ))}
        </div>
        <p className="text-[10px] text-gray-500 mt-2">
          Thanh màu = Giá | Vạch vàng = Rating
        </p>
      </div>
    </div>
  );
}
