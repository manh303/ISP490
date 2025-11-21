import { PieChart, Pie, Cell, ResponsiveContainer, Tooltip, Legend } from 'recharts';
import { Layers, TrendingUp } from 'lucide-react';
import { CategoryShareItem } from '../../services/analyticsApi';

interface CategoryShareChartProps {
  data: CategoryShareItem[];
  title?: string;
}

export function CategoryShareChart({
  data,
  title = 'Tỷ Trọng Danh Mục'
}: CategoryShareChartProps) {
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

  const COLORS = [
    '#3B82F6', '#EF4444', '#10B981', '#F59E0B', '#8B5CF6',
    '#06B6D4', '#84CC16', '#F97316', '#EC4899', '#6B7280'
  ];

  const chartData = data.map((item, index) => ({
    name: item.category_name,
    value: item.revenue,
    percentage: item.revenue_share,
    color: COLORS[index % COLORS.length]
  }));

  const CustomTooltip = ({ active, payload }: any) => {
    if (active && payload && payload.length) {
      const data = payload[0].payload;
      return (
        <div className="bg-white border border-gray-200 rounded-lg p-3 shadow-lg">
          <p className="font-medium text-gray-900">{data.name}</p>
          <p className="text-sm text-gray-600">
            Doanh thu: {(data.value / 1000000).toFixed(1)}M ₫
          </p>
          <p className="text-sm text-gray-600">
            Tỷ trọng: {(data.percentage * 100).toFixed(1)}%
          </p>
        </div>
      );
    }
    return null;
  };

  const renderLabel = (props: any) => {
    const { percent } = props;
    return `${(percent * 100).toFixed(1)}%`;
  };

  return (
    <div className="border border-gray-200 rounded-lg p-6 bg-white">
      <div className="flex items-center gap-2 mb-6">
        <Layers className="h-5 w-5 text-indigo-500" />
        <h3 className="font-semibold text-gray-900">{title}</h3>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Pie Chart */}
        <div className="h-80">
          <ResponsiveContainer width="100%" height="100%">
            <PieChart>
              <Pie
                data={chartData}
                cx="50%"
                cy="50%"
                outerRadius={80}
                dataKey="value"
                label={renderLabel}
                labelLine={false}
              >
                {chartData.map((entry, index) => (
                  <Cell key={`cell-${index}`} fill={entry.color} />
                ))}
              </Pie>
              <Tooltip content={<CustomTooltip />} />
            </PieChart>
          </ResponsiveContainer>
        </div>

        {/* Legend */}
        <div className="space-y-2">
          <h4 className="font-medium text-gray-900 mb-3">Chi tiết danh mục</h4>
          {chartData.map((item, index) => (
            <div key={index} className="flex items-center justify-between p-2 rounded-lg bg-gray-50">
              <div className="flex items-center gap-2">
                <div
                  className="w-3 h-3 rounded-full"
                  style={{ backgroundColor: item.color }}
                />
                <span className="text-sm font-medium text-gray-900">{item.name}</span>
              </div>
              <div className="text-right">
                <div className="text-sm font-bold text-gray-900">
                  {(item.percentage * 100).toFixed(1)}%
                </div>
                <div className="text-xs text-gray-600">
                  {(item.value / 1000000).toFixed(1)}M ₫
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Summary */}
      <div className="mt-6 pt-4 border-t border-gray-200">
        <div className="grid grid-cols-2 gap-4 text-center">
          <div>
            <div className="text-2xl font-bold text-indigo-600">
              {data.length}
            </div>
            <div className="text-sm text-gray-600">Tổng danh mục</div>
          </div>
          <div>
            <div className="text-2xl font-bold text-green-600">
              {(data.reduce((sum, item) => sum + item.revenue, 0) / 1000000).toFixed(1)}M ₫
            </div>
            <div className="text-sm text-gray-600">Tổng doanh thu</div>
          </div>
        </div>
      </div>
    </div>
  );
}