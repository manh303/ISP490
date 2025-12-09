import { PieChart, Smile, Meh, Frown } from 'lucide-react';
import { SentimentDistributionData } from '../../services/analyticsApi';

interface SentimentDistributionChartProps {
  data: SentimentDistributionData[];
  title?: string;
}

export function SentimentDistributionChart({ 
  data, 
  title = 'Sentiment Distribution' 
}: SentimentDistributionChartProps) {
  if (!data || data.length === 0) {
    return (
      <div className="border border-gray-200 rounded-lg p-6 bg-white">
        <div className="flex items-center gap-2 mb-4">
          <PieChart className="h-5 w-5 text-pink-500" />
          <h3 className="font-semibold text-gray-900">{title}</h3>
        </div>
        <div className="text-center py-8 text-gray-500">
          No data available
        </div>
      </div>
    );
  }

  const total = data.reduce((sum, item) => sum + item.product_count, 0);
  
  const sentimentConfig: { [key: string]: { color: string; bgColor: string; fillColor: string; icon: any } } = {
    'Excellent': { color: 'text-green-700', bgColor: 'bg-green-500', fillColor: '#22c55e', icon: Smile },
    'Good': { color: 'text-blue-700', bgColor: 'bg-blue-500', fillColor: '#3b82f6', icon: Smile },
    'Average': { color: 'text-yellow-700', bgColor: 'bg-yellow-500', fillColor: '#eab308', icon: Meh },
    'Poor': { color: 'text-orange-700', bgColor: 'bg-orange-500', fillColor: '#f97316', icon: Frown },
    'Very Poor': { color: 'text-red-700', bgColor: 'bg-red-500', fillColor: '#ef4444', icon: Frown },
  };

  return (
    <div className="border border-gray-200 rounded-lg p-6 bg-white">
      <div className="flex items-center gap-2 mb-4">
        <PieChart className="h-5 w-5 text-pink-500" />
        <h3 className="font-semibold text-gray-900">{title}</h3>
      </div>

      <div className="space-y-4">
        {/* Pie Chart Visualization */}
        <div className="flex justify-center items-center py-4">
          <div className="relative w-48 h-48">
            <svg viewBox="0 0 100 100" className="transform -rotate-90">
              {data.reduce((acc, item, index) => {
                const percentage = (item.product_count / total) * 100;
                const config = sentimentConfig[item.sentiment] || sentimentConfig['Average'];
                
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
                
                acc.currentAngle = endAngle;
                acc.paths.push(
                  <path
                    key={index}
                    d={pathData}
                    fill={config.fillColor}
                    opacity={0.9}
                    stroke="white"
                    strokeWidth="0.5"
                  />
                );
                
                return acc;
              }, { currentAngle: 0, paths: [] as React.ReactElement[] }).paths}
            </svg>
            <div className="absolute inset-0 flex items-center justify-center">
              <div className="text-center">
                <div className="text-2xl font-bold text-gray-900">
                  {total.toLocaleString('vi-VN')}
                </div>
                <div className="text-xs text-gray-500">Products</div>
              </div>
            </div>
          </div>
        </div>

        {/* Legend */}
        <div className="space-y-2">
          {data.map((item) => {
            const percentage = ((item.product_count / total) * 100).toFixed(1);
            const config = sentimentConfig[item.sentiment] || sentimentConfig['Average'];
            const Icon = config.icon;

            return (
              <div key={item.sentiment} className="flex items-center justify-between">
                <div className="flex items-center gap-2 flex-1">
                  <div className={`w-3 h-3 rounded-full ${config.bgColor}`} />
                  <Icon className={`h-4 w-4 ${config.color}`} />
                  <span className="text-sm text-gray-700">{item.sentiment}</span>
                </div>
                <div className="flex items-center gap-3 text-sm">
                  <span className="text-gray-600">
                    {item.product_count.toLocaleString('vi-VN')}
                  </span>
                  <span className="font-semibold text-gray-900 w-12 text-right">
                    {percentage}%
                  </span>
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
