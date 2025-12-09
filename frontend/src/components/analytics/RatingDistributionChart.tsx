import { BarChart2, Star } from 'lucide-react';
import { RatingDistributionData } from '../../services/analyticsApi';

interface RatingDistributionChartProps {
  data: RatingDistributionData[];
  title?: string;
}

export function RatingDistributionChart({ 
  data, 
  title = 'Rating Distribution' 
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
          {totalProducts.toLocaleString('vi-VN')} SP
        </div>
      </div>

      {/* Donut Chart */}
      <div className="flex justify-center mb-4">
        <div className="relative w-40 h-40">
          <svg viewBox="0 0 100 100" className="transform -rotate-90">
            {data.reduce((acc, item) => {
              const percentage = (item.product_count / totalProducts) * 100;
              const startAngle = acc.currentAngle;
              const angleSize = (percentage / 100) * 360;
              const endAngle = startAngle + angleSize;
              
              const x1 = 50 + 40 * Math.cos((Math.PI * startAngle) / 180);
              const y1 = 50 + 40 * Math.sin((Math.PI * startAngle) / 180);
              const x2 = 50 + 40 * Math.cos((Math.PI * endAngle) / 180);
              const y2 = 50 + 40 * Math.sin((Math.PI * endAngle) / 180);
              
              const largeArcFlag = angleSize > 180 ? 1 : 0;
              
              const pathData = [
                `M 50 50`,
                `L ${x1} ${y1}`,
                `A 40 40 0 ${largeArcFlag} 1 ${x2} ${y2}`,
                `Z`
              ].join(' ');
              
              const color = item.rating_bucket >= 4 ? '#22c55e' :
                           item.rating_bucket >= 3 ? '#3b82f6' :
                           item.rating_bucket >= 2 ? '#eab308' : '#9ca3af';
              
              acc.currentAngle = endAngle;
              acc.paths.push(
                <path key={item.rating_bucket} d={pathData} fill={color} opacity={0.9} stroke="white" strokeWidth="1" />
              );
              return acc;
            }, { currentAngle: 0, paths: [] as React.ReactElement[] }).paths}
            {/* Inner circle for donut */}
            <circle cx="50" cy="50" r="25" fill="white" />
          </svg>
          <div className="absolute inset-0 flex items-center justify-center">
            <div className="text-center">
              <div className="text-xl font-bold text-gray-900">{totalProducts}</div>
              <div className="text-[10px] text-gray-500">Sản phẩm</div>
            </div>
          </div>
        </div>
      </div>

      {/* Legend with bars */}
      <div className="space-y-2">
        {data.map((item) => {
          const percentage = ((item.product_count / totalProducts) * 100).toFixed(1);
          const barWidth = (item.product_count / maxCount) * 100;

          return (
            <div key={item.rating_bucket} className="space-y-1">
              <div className="flex items-center justify-between text-xs">
                <div className="flex items-center gap-2">
                  {item.rating_bucket > 0 ? (
                    <>
                      <Star className="h-3 w-3 text-yellow-500 fill-yellow-500" />
                      <span className="font-medium text-gray-900">{item.rating_bucket}⭐</span>
                    </>
                  ) : (
                    <span className="font-medium text-gray-500">Chưa ĐG</span>
                  )}
                  <span className="text-gray-600">{item.product_count.toLocaleString('vi-VN')}</span>
                </div>
                <span className="font-semibold text-gray-900">{percentage}%</span>
              </div>
              <div className="relative h-1.5 bg-gray-100 rounded-full overflow-hidden">
                <div
                  className={`absolute h-full rounded-full transition-all duration-500 ${
                    item.rating_bucket >= 4 ? 'bg-green-500' :
                    item.rating_bucket >= 3 ? 'bg-blue-500' :
                    item.rating_bucket >= 2 ? 'bg-yellow-500' : 'bg-gray-400'
                  }`}
                  style={{ width: `${barWidth}%` }}
                />
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
