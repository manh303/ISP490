import React, { useState, useMemo } from 'react';
import { DemandForecast as DemandForecastType } from '../../services/MLInsightsApi';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, PieChart, Pie, Cell } from 'recharts';

interface DemandForecastProps {
  data: DemandForecastType[];
  loading?: boolean;
}

const TREND_COLORS = {
  'Growing': '#10b981',
  'Declining': '#ef4444',
  'Stable': '#6b7280',
};

const DemandForecast: React.FC<DemandForecastProps> = ({ data, loading }) => {
  const [filter, setFilter] = useState<string>('All');
  const [currentPage, setCurrentPage] = useState(1);
  const itemsPerPage = 10;

  const trendChartData = useMemo(() => {
    const growing = data.filter((d) => d.demand_trend === 'Growing').length;
    const declining = data.filter((d) => d.demand_trend === 'Declining').length;
    const stable = data.filter((d) => d.demand_trend === 'Stable').length;

    return [
      { name: 'Growing', value: growing, color: TREND_COLORS['Growing'] },
      { name: 'Declining', value: declining, color: TREND_COLORS['Declining'] },
      { name: 'Stable', value: stable, color: TREND_COLORS['Stable'] },
    ].filter(item => item.value > 0);
  }, [data]);

  const lineChartData = useMemo(() => {
    return data.slice(0, 10).map((item) => ({
      name: item.product_name.substring(0, 20) + '...',
      recent: item.recent_demand,
      forecast_7d: item.forecast_7d,
      forecast_30d: item.forecast_30d,
    }));
  }, [data]);

  const filteredData = useMemo(() => {
    if (filter === 'All') return data;
    return data.filter((item) => item.demand_trend === filter);
  }, [data, filter]);

  const paginatedData = useMemo(() => {
    const startIndex = (currentPage - 1) * itemsPerPage;
    return filteredData.slice(startIndex, startIndex + itemsPerPage);
  }, [filteredData, currentPage]);

  const totalPages = Math.ceil(filteredData.length / itemsPerPage);

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

  return (
    <div className="bg-white rounded-lg shadow-md p-6 mb-8">
      <h2 className="text-2xl font-bold text-gray-800 mb-6">📈 Demand Forecast</h2>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
        {/* Pie Chart - Trend Distribution */}
        <div>
          <h3 className="text-lg font-semibold text-gray-700 mb-4">Demand Trend Distribution</h3>
          <ResponsiveContainer width="100%" height={300}>
            <PieChart>
              <Pie
                data={trendChartData}
                cx="50%"
                cy="50%"
                labelLine={false}
                label={(entry: any) => `${entry.name}: ${(entry.percent * 100).toFixed(1)}%`}
                outerRadius={80}
                fill="#8884d8"
                dataKey="value"
              >
                {trendChartData.map((entry, index) => (
                  <Cell key={`cell-${index}`} fill={entry.color} />
                ))}
              </Pie>
              <Tooltip />
              <Legend />
            </PieChart>
          </ResponsiveContainer>
        </div>

        {/* Line Chart - Top Products Forecast */}
        <div>
          <h3 className="text-lg font-semibold text-gray-700 mb-4">Top 10 Products Forecast</h3>
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={lineChartData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="name" angle={-45} textAnchor="end" height={100} fontSize={10} />
              <YAxis />
              <Tooltip />
              <Legend />
              <Line type="monotone" dataKey="recent" stroke="#8b5cf6" name="Recent Demand" />
              <Line type="monotone" dataKey="forecast_7d" stroke="#3b82f6" name="7-Day Forecast" />
              <Line type="monotone" dataKey="forecast_30d" stroke="#10b981" name="30-Day Forecast" />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Filter Buttons */}
      <div className="flex gap-2 mb-4 flex-wrap">
        {['All', 'Growing', 'Declining', 'Stable'].map((option) => (
          <button
            key={option}
            onClick={() => {
              setFilter(option);
              setCurrentPage(1);
            }}
            className={`px-4 py-2 rounded-lg font-medium transition-colors ${
              filter === option
                ? 'bg-purple-600 text-white'
                : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
            }`}
          >
            {option}
          </button>
        ))}
      </div>

      {/* Table */}
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Product
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Recent Demand
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                7-Day Forecast
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                30-Day Forecast
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Trend
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Quality
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Stock Recommendation
              </th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {paginatedData.map((item) => (
              <tr key={item.product_sk} className="hover:bg-gray-50">
                <td className="px-4 py-4 text-sm text-gray-900 max-w-xs truncate" title={item.product_name}>
                  {item.product_name}
                </td>
                <td className="px-4 py-4 text-sm text-gray-900">
                  {item.recent_demand.toLocaleString()}
                </td>
                <td className="px-4 py-4 text-sm text-gray-900">
                  {item.forecast_7d.toLocaleString()}
                </td>
                <td className="px-4 py-4 text-sm text-gray-900">
                  {item.forecast_30d.toLocaleString()}
                </td>
                <td className="px-4 py-4 text-sm">
                  <span
                    className={`px-2 py-1 rounded-full text-xs font-medium ${
                      item.demand_trend === 'Growing'
                        ? 'bg-green-100 text-green-800'
                        : item.demand_trend === 'Declining'
                        ? 'bg-red-100 text-red-800'
                        : 'bg-gray-100 text-gray-800'
                    }`}
                  >
                    {item.demand_trend === 'Growing' ? '📈' : item.demand_trend === 'Declining' ? '📉' : '➡️'} {item.demand_trend}
                  </span>
                </td>
                <td className="px-4 py-4 text-sm text-gray-900">
                  <div className="flex items-center">
                    <span className="text-yellow-500">{'⭐'.repeat(item.quality_score)}</span>
                    <span className="ml-1">({item.quality_score}/5)</span>
                  </div>
                </td>
                <td className="px-4 py-4 text-sm">
                  <span className={`px-2 py-1 rounded-full text-xs font-medium ${
                    item.stock_recommendation.includes('Increase')
                      ? 'bg-blue-100 text-blue-800'
                      : item.stock_recommendation.includes('Reduce')
                      ? 'bg-orange-100 text-orange-800'
                      : 'bg-gray-100 text-gray-800'
                  }`}>
                    {item.stock_recommendation}
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="flex items-center justify-between mt-4">
          <div className="text-sm text-gray-700">
            Showing {((currentPage - 1) * itemsPerPage) + 1} to {Math.min(currentPage * itemsPerPage, filteredData.length)} of {filteredData.length} results
          </div>
          <div className="flex gap-2">
            <button
              onClick={() => setCurrentPage(Math.max(1, currentPage - 1))}
              disabled={currentPage === 1}
              className="px-3 py-1 rounded bg-gray-200 text-gray-700 disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-300"
            >
              Previous
            </button>
            <span className="px-3 py-1 text-gray-700">
              Page {currentPage} of {totalPages}
            </span>
            <button
              onClick={() => setCurrentPage(Math.min(totalPages, currentPage + 1))}
              disabled={currentPage === totalPages}
              className="px-3 py-1 rounded bg-gray-200 text-gray-700 disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-300"
            >
              Next
            </button>
          </div>
        </div>
      )}
    </div>
  );
};

export default DemandForecast;
