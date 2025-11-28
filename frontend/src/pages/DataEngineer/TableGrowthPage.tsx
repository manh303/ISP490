import React, { useState, useEffect } from 'react';
import { TrendingUp, Calendar, Database, BarChart3, RefreshCw } from 'lucide-react';
import { getTableGrowth } from '../../services/dataEngineerApi';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, BarChart, Bar } from 'recharts';

interface TableGrowthData {
  snapshot_date: string;
  row_count: number;
  size_mb: number;
  avg_row_size_kb: number;
}

const TableGrowthPage: React.FC = () => {
  const [growthData, setGrowthData] = useState<TableGrowthData[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [schemaName, setSchemaName] = useState('dwh');
  const [tableName, setTableName] = useState('');
  const [days, setDays] = useState(30);

  const schemas = ['dwh', 'ml'];
  const tablesBySchema: { [key: string]: string[] } = {
    dwh: [
      'dim_brand',
      'dim_category',
      'dim_date',
      'dim_platform',
      'dim_product',
      'dim_reviewer',
      'fact_product_daily',
      'fact_product_daily_agg',
      'fact_review',
      'fact_review_daily',
      'fact_review_daily_agg',
      'fact_reviews_detail'
    ],
    ml: [
      'dim_ml_model',
      'fact_price_prediction',
      'fact_product_recommen',
      'fact_review_sentiment'
    ]
  };

  const availableTables = tablesBySchema[schemaName] || [];

  const fetchTableGrowth = async () => {
    try {
      setLoading(true);
      setError(null);
      const data = await getTableGrowth(schemaName, tableName, days);
      console.log('Fetched growth data:', data);
      // Sort data by date ascending (oldest first)
      const sortedData = data.sort((a: TableGrowthData, b: TableGrowthData) => new Date(a.snapshot_date).getTime() - new Date(b.snapshot_date).getTime());
      setGrowthData(sortedData);
    } catch (err) {
      console.error('Error fetching table growth:', err);
      setError('Không thể tải dữ liệu tăng trưởng bảng');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (schemaName && tableName) {
      fetchTableGrowth();
    }
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
            Lịch sử Tăng trưởng Bảng
          </h1>
          <p className="text-gray-600 dark:text-gray-300 mt-1">
            Giám sát kích thước bảng và tăng trưởng số lượng hàng theo thời gian
          </p>
        </div>
        <button
          onClick={fetchTableGrowth}
          className="flex items-center gap-2 bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg transition-colors"
        >
          <RefreshCw className="w-4 h-4" />
          Làm mới
        </button>
      </div>

      {/* Filters */}
      <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
              Tên Lược đồ
            </label>
            <select
              value={schemaName}
              onChange={(e) => setSchemaName(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent dark:bg-gray-700 dark:text-white"
            >
              {schemas.map(schema => (
                <option key={schema} value={schema}>{schema}</option>
              ))}
            </select>
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
              Tên Bảng
            </label>
            <input
              type="text"
              value={tableName}
              onChange={(e) => setTableName(e.target.value)}
              list="table-options"
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent dark:bg-gray-700 dark:text-white"
              placeholder="Chọn hoặc nhập tên bảng"
            />
            <datalist id="table-options">
              {availableTables.map(table => (
                <option key={table} value={table} />
              ))}
            </datalist>
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
              Ngày
            </label>
            <select
              value={days}
              onChange={(e) => setDays(Number(e.target.value))}
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent dark:bg-gray-700 dark:text-white"
            >
              <option value={7}>7 ngày qua</option>
              <option value={30}>30 ngày qua</option>
              <option value={90}>90 ngày qua</option>
              <option value={180}>180 ngày qua</option>
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
                Tăng trưởng Số lượng Hàng
              </h2>
            </div>
            <ResponsiveContainer width="100%" height={300}>
              <LineChart data={growthData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis
                  dataKey="snapshot_date"
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
                  stroke="#EF4444"
                  strokeWidth={2}
                  dot={{ fill: '#EF4444', strokeWidth: 2, r: 4 }}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>

          {/* Size Growth */}
          <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
            <div className="flex items-center mb-4">
              <BarChart3 className="w-5 h-5 mr-2 text-green-600" />
              <h2 className="text-xl font-semibold text-gray-900 dark:text-white">
                Tăng trưởng Kích thước (MB)
              </h2>
            </div>
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={growthData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis
                  dataKey="snapshot_date"
                  tick={{ fontSize: 12 }}
                  tickFormatter={(value) => new Date(value).toLocaleDateString()}
                />
                <YAxis tick={{ fontSize: 12 }} />
                <Tooltip
                  labelFormatter={(value) => new Date(value).toLocaleDateString()}
                  formatter={(value: number) => [`${value.toFixed(2)} MB`, 'Size']}
                />
                <Bar dataKey="size_mb" fill="#F59E0B" />
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
                    <th className="text-left py-2 px-4 font-medium text-gray-700 dark:text-gray-300">Ngày</th>
                    <th className="text-left py-2 px-4 font-medium text-gray-700 dark:text-gray-300">Số lượng Hàng</th>
                    <th className="text-left py-2 px-4 font-medium text-gray-700 dark:text-gray-300">Kích thước (MB)</th>
                    <th className="text-left py-2 px-4 font-medium text-gray-700 dark:text-gray-300">Tỷ lệ Tăng trưởng</th>
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
                          {new Date(item.snapshot_date).toLocaleDateString()}
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
                              Hàng: {rowGrowth >= 0 ? '+' : ''}{rowGrowth.toFixed(2)}%
                            </span>
                            <br />
                            <span className={sizeGrowth >= 0 ? 'text-green-600' : 'text-red-600'}>
                              Kích thước: {sizeGrowth >= 0 ? '+' : ''}{sizeGrowth.toFixed(2)}%
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
              Không có Dữ liệu Tăng trưởng nào
            </h3>
            <p className="text-gray-600 dark:text-gray-300">
              Không tìm thấy dữ liệu tăng trưởng cho bảng và khoảng thời gian đã chọn.
            </p>
          </div>
        </div>
      )}
    </div>
  );
};

export default TableGrowthPage;