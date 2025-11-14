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
          Sản phẩm giá cao (≥4⭐): {((avgPriceHighRated || 0) / 1000000).toFixed(1)}M ₫
        </p>
        <p className="text-xs text-gray-600">
          Sản phẩm giá thấp (&lt;4⭐): {((avgPriceLowRated || 0) / 1000000).toFixed(1)}M ₫
        </p>
      </div>

      {/* Scatter Plot Visualization */}
      <div className="space-y-3 max-h-[300px] overflow-y-auto">
        {displayProducts.map((product, index) => {
          const pricePosition = (product.price / maxPrice) * 100;
          const ratingPosition = (product.rating_avg / maxRating) * 100;
          
          // Determine segment color
          const segment = priceSegments.find(s => product.price >= s.min && product.price <= s.max);
          
          return (
            <div key={index} className="relative">
              <div className="flex items-center justify-between mb-1">
                <span className="text-xs text-gray-700 truncate max-w-[150px]" title={product.product_name}>
                  {product.product_name}
                </span>
                <div className="flex items-center gap-2">
                  <div className="flex items-center gap-1">
                     <DollarSign className="h-3 w-3 text-gray-500" />
                     <span className="text-xs font-medium text-gray-700">
                       {((product.price || 0) / 1000000).toFixed(1)}M
                     </span>
                   </div>
                   <div className="flex items-center gap-1">
                     <Star className="h-3 w-3 text-yellow-500 fill-yellow-500" />
                     <span className="text-xs font-medium text-gray-900">
                       {(product.rating_avg || 0).toFixed(1)}
                     </span>
                   </div>
                </div>
              </div>
              
              {/* Visual representation */}
              <div className="relative h-2 bg-gray-100 rounded-full">
                <div
                  className={`absolute h-full rounded-full ${segment?.color || 'bg-gray-400'} opacity-70`}
                  style={{ width: `${pricePosition}%` }}
                />
                <div
                  className="absolute top-0 w-1 h-full bg-yellow-400 border border-yellow-600"
                  style={{ left: `${ratingPosition}%` }}
                />
              </div>
              
              <div className="flex justify-between text-[10px] text-gray-500 mt-1">
                <span>{(product.review_count || 0).toLocaleString('vi-VN')} đánh giá</span>
                <span className="capitalize">{product.category}</span>
              </div>
            </div>
          );
        })}
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
