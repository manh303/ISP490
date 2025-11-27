import React, { useState, useEffect } from 'react';
import { TrendingUp, Calendar, Database, BarChart3, RefreshCw } from 'lucide-react';
import { getTableGrowth } from '../../services/dataEngineerApi';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, BarChart, Bar } from 'recharts';

interface TableGrowthData {
  date: string;
  row_count: number;
  size_mb: number;
}

const TableGrowthPage: React.FC = () => {
  const [growthData, setGrowthData] = useState<TableGrowthData[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [schemaName, setSchemaName] = useState('ecommerce');
  const [tableName, setTableName] = useState('products');
  const [days, setDays] = useState(30);

  const fetchTableGrowth = async () => {
    try {
      setLoading(true);
      setError(null);
      const data = await getTableGrowth(schemaName, tableName, days);
      setGrowthData(data);
    } catch (err) {
      console.error('Error fetching table growth:', err);
      setError('Failed to load table growth data');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchTableGrowth();
  }, [schemaName, tableName, days]);

  const formatBytes = (bytes: number) => {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold text-gray-900 dark:text-white">
            Table Growth History
          </h1>
          <p className="text-gray-600 dark:text-gray-300 mt-1">
            Monitor table size and row count growth over time
          </p>
        </div>
        <button
          onClick={fetchTableGrowth}
          className="flex items-center gap-2 bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg transition-colors"
        >
          <RefreshCw className="w-4 h-4" />
          Refresh
        </button>
      </div>

      {/* Filters */}
      <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
              Schema Name
            </label>
            <input
              type="text"
              value={schemaName}
              onChange={(e) => setSchemaName(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent dark:bg-gray-700 dark:text-white"
              placeholder="e.g., ecommerce"
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
              Table Name
            </label>
            <input
              type="text"
              value={tableName}
              onChange={(e) => setTableName(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent dark:bg-gray-700 dark:text-white"
              placeholder="e.g., products"
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
              Days
            </label>
            <select
              value={days}
              onChange={(e) => setDays(Number(e.target.value))}
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent dark:bg-gray-700 dark:text-white"
            >
              <option value={7}>Last 7 days</option>
              <option value={30}>Last 30 days</option>
              <option value={90}>Last 90 days</option>
              <option value={180}>Last 180 days</option>
            </select>
          </div>
          <div className="flex items-end">
            <button
              onClick={fetchTableGrowth}
              className="w-full bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg transition-colors"
            >
              Load Data
            </button>
          </div>
        </div>
      </div>

      {/* Error State */}
      {error && (
        <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded">
          {error}
        </div>
      )}

      {/* Loading State */}
      {loading && (
        <div className="flex items-center justify-center py-12">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
        </div>
      )}

      {/* Charts */}
      {!loading && !error && growthData.length > 0 && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Row Count Growth */}
          <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
            <div className="flex items-center mb-4">
              <Database className="w-5 h-5 mr-2 text-blue-600" />
              <h2 className="text-xl font-semibold text-gray-900 dark:text-white">
                Row Count Growth
              </h2>
            </div>
            <ResponsiveContainer width="100%" height={300}>
              <LineChart data={growthData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis
                  dataKey="date"
                  tick={{ fontSize: 12 }}
                  tickFormatter={(value) => new Date(value).toLocaleDateString()}
                />
                <YAxis tick={{ fontSize: 12 }} />
                <Tooltip
                  labelFormatter={(value) => new Date(value).toLocaleDateString()}
                  formatter={(value: number) => [value.toLocaleString(), 'Row Count']}
                />
                <Line
                  type="monotone"
                  dataKey="row_count"
                  stroke="#3B82F6"
                  strokeWidth={2}
                  dot={{ fill: '#3B82F6', strokeWidth: 2, r: 4 }}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>

          {/* Size Growth */}
          <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
            <div className="flex items-center mb-4">
              <BarChart3 className="w-5 h-5 mr-2 text-green-600" />
              <h2 className="text-xl font-semibold text-gray-900 dark:text-white">
                Size Growth (MB)
              </h2>
            </div>
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={growthData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis
                  dataKey="date"
                  tick={{ fontSize: 12 }}
                  tickFormatter={(value) => new Date(value).toLocaleDateString()}
                />
                <YAxis tick={{ fontSize: 12 }} />
                <Tooltip
                  labelFormatter={(value) => new Date(value).toLocaleDateString()}
                  formatter={(value: number) => [`${value.toFixed(2)} MB`, 'Size']}
                />
                <Bar dataKey="size_mb" fill="#10B981" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {/* Data Table */}
      {!loading && !error && growthData.length > 0 && (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow border">
          <div className="p-6 border-b">
            <h2 className="text-xl font-semibold text-gray-900 dark:text-white flex items-center">
              <TrendingUp className="w-5 h-5 mr-2" />
              Growth Data Table
            </h2>
          </div>
          <div className="p-6">
            <div className="overflow-x-auto">
              <table className="min-w-full">
                <thead>
                  <tr className="border-b">
                    <th className="text-left py-2 px-4 font-medium text-gray-700 dark:text-gray-300">Date</th>
                    <th className="text-left py-2 px-4 font-medium text-gray-700 dark:text-gray-300">Row Count</th>
                    <th className="text-left py-2 px-4 font-medium text-gray-700 dark:text-gray-300">Size (MB)</th>
                    <th className="text-left py-2 px-4 font-medium text-gray-700 dark:text-gray-300">Growth Rate</th>
                  </tr>
                </thead>
                <tbody>
                  {growthData.map((item, index) => {
                    const prevItem = growthData[index - 1];
                    const rowGrowth = prevItem ? ((item.row_count - prevItem.row_count) / prevItem.row_count * 100) : 0;
                    const sizeGrowth = prevItem ? ((item.size_mb - prevItem.size_mb) / prevItem.size_mb * 100) : 0;

                    return (
                      <tr key={index} className="border-b hover:bg-gray-50 dark:hover:bg-gray-700">
                        <td className="py-3 px-4 text-gray-600 dark:text-gray-300">
                          {new Date(item.date).toLocaleDateString()}
                        </td>
                        <td className="py-3 px-4 text-gray-900 dark:text-white font-medium">
                          {item.row_count.toLocaleString()}
                        </td>
                        <td className="py-3 px-4 text-gray-900 dark:text-white font-medium">
                          {item.size_mb.toFixed(2)} MB
                        </td>
                        <td className="py-3 px-4">
                          <div className="text-sm">
                            <span className={rowGrowth >= 0 ? 'text-green-600' : 'text-red-600'}>
                              Rows: {rowGrowth >= 0 ? '+' : ''}{rowGrowth.toFixed(2)}%
                            </span>
                            <br />
                            <span className={sizeGrowth >= 0 ? 'text-green-600' : 'text-red-600'}>
                              Size: {sizeGrowth >= 0 ? '+' : ''}{sizeGrowth.toFixed(2)}%
                            </span>
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}

      {/* Empty State */}
      {!loading && !error && growthData.length === 0 && (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow border p-12">
          <div className="text-center">
            <BarChart3 className="w-16 h-16 mx-auto mb-4 text-gray-400" />
            <h3 className="text-xl font-medium text-gray-900 dark:text-white mb-2">
              No Growth Data Available
            </h3>
            <p className="text-gray-600 dark:text-gray-300">
              No growth data found for the selected table and time period.
            </p>
          </div>
        </div>
      )}
    </div>
  );
};

export default TableGrowthPage;