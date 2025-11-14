import React from 'react';
import { SalesTrend as SalesTrendType } from '../../services/MLInsightsApi';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, Area, AreaChart } from 'recharts';

interface SalesTrendProps {
  data: SalesTrendType[];
  loading?: boolean;
}

const MONTH_NAMES = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

const SalesTrend: React.FC<SalesTrendProps> = ({ data, loading }) => {
  const chartData = data.map((item) => ({
    period: `${MONTH_NAMES[item.month - 1]} ${item.year}`,
    total_reviews: item.total_reviews,
    avg_rating: item.avg_rating,
    growth_rate: item.growth_rate,
    trend: item.trend,
  }));

  const getTrendIcon = (trend: string) => {
    if (trend.toLowerCase().includes('growth') || trend.toLowerCase().includes('increase')) {
      return '📈';
    } else if (trend.toLowerCase().includes('decline') || trend.toLowerCase().includes('decrease')) {
      return '📉';
    }
    return '➡️';
  };

  const getTrendColor = (trend: string) => {
    if (trend.toLowerCase().includes('growth') || trend.toLowerCase().includes('increase')) {
      return 'text-green-600';
    } else if (trend.toLowerCase().includes('decline') || trend.toLowerCase().includes('decrease')) {
      return 'text-red-600';
    }
    return 'text-gray-600';
  };

  if (loading) {
    return (
      <div className="bg-white rounded-lg shadow-md p-6">
        <div className="animate-pulse">
          <div className="h-6 bg-gray-200 rounded w-1/3 mb-6"></div>
          <div className="h-64 bg-gray-200 rounded"></div>
        </div>
      </div>
    );
  }

  const latestTrend = chartData[chartData.length - 1];

  return (
    <div className="bg-white rounded-lg shadow-md p-6 mb-8">
      <h2 className="text-2xl font-bold text-gray-800 mb-6">📊 Sales Trend Analysis</h2>

      {/* Current Trend Summary */}
      {latestTrend && (
        <div className="mb-6 p-4 bg-gradient-to-r from-blue-50 to-purple-50 rounded-lg">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-gray-600">Current Month Trend</p>
              <p className={`text-2xl font-bold ${getTrendColor(latestTrend.trend)}`}>
                {getTrendIcon(latestTrend.trend)} {latestTrend.trend}
              </p>
            </div>
            <div className="text-right">
              <p className="text-sm text-gray-600">Growth Rate</p>
              <p className={`text-2xl font-bold ${latestTrend.growth_rate >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                {latestTrend.growth_rate >= 0 ? '+' : ''}{latestTrend.growth_rate.toFixed(2)}%
              </p>
            </div>
          </div>
        </div>
      )}

      {/* Line Chart - Total Reviews */}
      <div className="mb-6">
        <h3 className="text-lg font-semibold text-gray-700 mb-4">Monthly Review Volume</h3>
        <ResponsiveContainer width="100%" height={300}>
          <AreaChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="period" />
            <YAxis />
            <Tooltip />
            <Legend />
            <Area
              type="monotone"
              dataKey="total_reviews"
              stroke="#3b82f6"
              fill="#93c5fd"
              name="Total Reviews"
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>

      {/* Line Chart - Growth Rate & Rating */}
      <div className="mb-6">
        <h3 className="text-lg font-semibold text-gray-700 mb-4">Growth Rate & Average Rating</h3>
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="period" />
            <YAxis yAxisId="left" />
            <YAxis yAxisId="right" orientation="right" />
            <Tooltip />
            <Legend />
            <Line
              yAxisId="left"
              type="monotone"
              dataKey="growth_rate"
              stroke="#8b5cf6"
              strokeWidth={2}
              name="Growth Rate (%)"
              dot={{ r: 5 }}
            />
            <Line
              yAxisId="right"
              type="monotone"
              dataKey="avg_rating"
              stroke="#10b981"
              strokeWidth={2}
              name="Avg Rating"
              dot={{ r: 5 }}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Summary Stats */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
        <div className="bg-blue-50 p-4 rounded-lg">
          <div className="text-sm text-blue-600 font-medium mb-1">Total Reviews</div>
          <div className="text-2xl font-bold text-blue-700">
            {(latestTrend?.total_reviews || 0).toLocaleString()}
          </div>
          <div className="text-xs text-blue-600 mt-1">Current month</div>
        </div>

        <div className="bg-green-50 p-4 rounded-lg">
          <div className="text-sm text-green-600 font-medium mb-1">Avg Rating</div>
          <div className="text-2xl font-bold text-green-700">
            ⭐ {latestTrend?.avg_rating.toFixed(2)}
          </div>
          <div className="text-xs text-green-600 mt-1">Current month</div>
        </div>

        <div className={`${latestTrend?.growth_rate >= 0 ? 'bg-green-50' : 'bg-red-50'} p-4 rounded-lg`}>
          <div className={`text-sm ${latestTrend?.growth_rate >= 0 ? 'text-green-600' : 'text-red-600'} font-medium mb-1`}>
            Growth Rate
          </div>
          <div className={`text-2xl font-bold ${latestTrend?.growth_rate >= 0 ? 'text-green-700' : 'text-red-700'}`}>
            {latestTrend?.growth_rate >= 0 ? '+' : ''}{latestTrend?.growth_rate.toFixed(2)}%
          </div>
          <div className={`text-xs ${latestTrend?.growth_rate >= 0 ? 'text-green-600' : 'text-red-600'} mt-1`}>
            vs. previous month
          </div>
        </div>

        <div className="bg-purple-50 p-4 rounded-lg">
          <div className="text-sm text-purple-600 font-medium mb-1">Trend Status</div>
          <div className="text-lg font-bold text-purple-700">
            {getTrendIcon(latestTrend?.trend || '')} {latestTrend?.trend}
          </div>
          <div className="text-xs text-purple-600 mt-1">Current analysis</div>
        </div>
      </div>

      {/* Data Table */}
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Period
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Total Reviews
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Avg Rating
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Growth Rate
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Trend
              </th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {chartData.map((item, index) => (
              <tr key={index} className="hover:bg-gray-50">
                <td className="px-4 py-4 text-sm font-medium text-gray-900">
                  {item.period}
                </td>
                <td className="px-4 py-4 text-sm text-gray-900">
                  {(item.total_reviews || 0).toLocaleString()}
                </td>
                <td className="px-4 py-4 text-sm text-gray-900">
                  <span className="flex items-center">
                    <span className="text-yellow-500 mr-1">⭐</span>
                    {item.avg_rating.toFixed(2)}
                  </span>
                </td>
                <td className="px-4 py-4 text-sm">
                  <span className={item.growth_rate >= 0 ? 'text-green-600 font-medium' : 'text-red-600 font-medium'}>
                    {item.growth_rate >= 0 ? '+' : ''}{item.growth_rate.toFixed(2)}%
                  </span>
                </td>
                <td className="px-4 py-4 text-sm">
                  <span className={getTrendColor(item.trend)}>
                    {getTrendIcon(item.trend)} {item.trend}
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default SalesTrend;
