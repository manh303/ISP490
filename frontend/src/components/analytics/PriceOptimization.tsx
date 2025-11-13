import React, { useState, useMemo } from 'react';
import { PriceOptimization as PriceOptimizationType } from '../../services/MLInsightsApi';
import { PieChart, Pie, Cell, ResponsiveContainer, Legend, Tooltip } from 'recharts';

interface PriceOptimizationProps {
  data: PriceOptimizationType[];
  loading?: boolean;
}

const COLORS = {
  'Increase Price': '#10b981',
  'Decrease Price': '#ef4444',
  'Maintain Price': '#3b82f6',
};

const PriceOptimization: React.FC<PriceOptimizationProps> = ({ data, loading }) => {
  const [filter, setFilter] = useState<string>('All');
  const [currentPage, setCurrentPage] = useState(1);
  const itemsPerPage = 10;

  const chartData = useMemo(() => {
    const increase = data.filter((d) => d.recommendation === 'Increase Price').length;
    const decrease = data.filter((d) => d.recommendation === 'Decrease Price').length;
    const maintain = data.filter((d) => d.recommendation === 'Maintain Price').length;

    return [
      { name: 'Increase Price', value: increase, color: COLORS['Increase Price'] },
      { name: 'Decrease Price', value: decrease, color: COLORS['Decrease Price'] },
      { name: 'Maintain Price', value: maintain, color: COLORS['Maintain Price'] },
    ].filter(item => item.value > 0);
  }, [data]);

  const filteredData = useMemo(() => {
    if (filter === 'All') return data;
    return data.filter((item) => item.recommendation === filter);
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
      <h2 className="text-2xl font-bold text-gray-800 mb-6">🏷️ Price Optimization</h2>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
        {/* Pie Chart */}
        <div>
          <h3 className="text-lg font-semibold text-gray-700 mb-4">Recommendation Distribution</h3>
          <ResponsiveContainer width="100%" height={300}>
            <PieChart>
              <Pie
                data={chartData}
                cx="50%"
                cy="50%"
                labelLine={false}
                label={(entry: any) => `${entry.name}: ${(entry.percent * 100).toFixed(1)}%`}
                outerRadius={80}
                fill="#8884d8"
                dataKey="value"
              >
                {chartData.map((entry, index) => (
                  <Cell key={`cell-${index}`} fill={entry.color} />
                ))}
              </Pie>
              <Tooltip />
              <Legend />
            </PieChart>
          </ResponsiveContainer>
        </div>

        {/* Summary Stats */}
        <div className="flex flex-col justify-center space-y-4">
          <div className="bg-green-50 p-4 rounded-lg">
            <div className="flex items-center justify-between">
              <span className="text-green-700 font-medium">🟢 Increase Price</span>
              <span className="text-2xl font-bold text-green-600">
                {chartData.find(d => d.name === 'Increase Price')?.value || 0}
              </span>
            </div>
          </div>
          <div className="bg-red-50 p-4 rounded-lg">
            <div className="flex items-center justify-between">
              <span className="text-red-700 font-medium">🔴 Decrease Price</span>
              <span className="text-2xl font-bold text-red-600">
                {chartData.find(d => d.name === 'Decrease Price')?.value || 0}
              </span>
            </div>
          </div>
          <div className="bg-blue-50 p-4 rounded-lg">
            <div className="flex items-center justify-between">
              <span className="text-blue-700 font-medium">🔵 Maintain Price</span>
              <span className="text-2xl font-bold text-blue-600">
                {chartData.find(d => d.name === 'Maintain Price')?.value || 0}
              </span>
            </div>
          </div>
        </div>
      </div>

      {/* Filter Buttons */}
      <div className="flex gap-2 mb-4 flex-wrap">
        {['All', 'Increase Price', 'Decrease Price', 'Maintain Price'].map((option) => (
          <button
            key={option}
            onClick={() => {
              setFilter(option);
              setCurrentPage(1);
            }}
            className={`px-4 py-2 rounded-lg font-medium transition-colors ${
              filter === option
                ? 'bg-blue-600 text-white'
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
                Current Price
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Optimal Price
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Margin Change
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Recommendation
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Position
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
                  {item.current_price.toLocaleString()}đ
                </td>
                <td className="px-4 py-4 text-sm text-gray-900">
                  {item.optimal_price.toLocaleString()}đ
                </td>
                <td className="px-4 py-4 text-sm">
                  <span className={item.expected_margin_change > 0 ? 'text-green-600' : item.expected_margin_change < 0 ? 'text-red-600' : 'text-gray-600'}>
                    {item.expected_margin_change > 0 ? '+' : ''}{item.expected_margin_change}%
                  </span>
                </td>
                <td className="px-4 py-4 text-sm">
                  <span
                    className={`px-2 py-1 rounded-full text-xs font-medium ${
                      item.recommendation === 'Increase Price'
                        ? 'bg-green-100 text-green-800'
                        : item.recommendation === 'Decrease Price'
                        ? 'bg-red-100 text-red-800'
                        : 'bg-blue-100 text-blue-800'
                    }`}
                  >
                    {item.recommendation}
                  </span>
                </td>
                <td className="px-4 py-4 text-sm text-gray-600">
                  {item.price_position}
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

export default PriceOptimization;
