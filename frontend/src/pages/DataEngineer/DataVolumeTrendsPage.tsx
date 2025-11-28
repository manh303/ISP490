import React, { useState, useEffect } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, BarChart, Bar, AreaChart, Area } from 'recharts';
import { Database, TrendingUp, TrendingDown, RefreshCw, Calendar, BarChart3 } from 'lucide-react';
import { getDataVolumeTrends } from '../../services/dataEngineerApi';

interface VolumeTrend {
  schema_name: string;
  snapshot_date: string;
  total_rows: number;
  total_size_gb: number;
}

interface VolumeStats {
  total_schemas: number;
  total_tables: number;
  total_rows: number;
  total_size_gb: number;
  avg_growth_rate: number;
}

const DataVolumeTrendsPage: React.FC = () => {
  const [trends, setTrends] = useState<VolumeTrend[]>([]);
  const [stats, setStats] = useState<VolumeStats | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [timeRange, setTimeRange] = useState<string>('30d');
  const [selectedSchema, setSelectedSchema] = useState<string>('ALL');
  const [chartType, setChartType] = useState<'line' | 'area' | 'bar'>('line');

  const fetchDataVolumeTrends = async () => {
    try {
      setLoading(true);
      setError(null);
      // Convert timeRange to days
      const days = timeRange === '7d' ? 7 : timeRange === '30d' ? 30 : timeRange === '90d' ? 90 : 180;
      const data = await getDataVolumeTrends(days);
      setTrends(data);
      setStats(null);
    } catch (err) {
      console.error('Error fetching data volume trends:', err);
      setError('Failed to load data volume trends');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchDataVolumeTrends();
  }, [timeRange]);

  const filteredTrends = selectedSchema === 'ALL'
    ? trends
    : trends.filter(trend => trend.schema_name === selectedSchema);

  const uniqueSchemas = [...new Set(trends.map(trend => trend.schema_name))];

  // Prepare chart data
  const rowCountData = filteredTrends.reduce((acc, trend) => {
    const date = new Date(trend.snapshot_date).toLocaleDateString();
    if (!acc[date]) {
      acc[date] = { date, totalRows: 0, tables: {} };
    }
    acc[date].totalRows += trend.total_rows;
    acc[date].tables[trend.schema_name] = trend.total_rows;
    return acc;
  }, {} as Record<string, any>);

  const sizeData = filteredTrends.reduce((acc, trend) => {
    const date = new Date(trend.snapshot_date).toLocaleDateString();
    if (!acc[date]) {
      acc[date] = { date, totalSize: 0, tables: {} };
    }
    acc[date].totalSize += trend.total_size_gb;
    acc[date].tables[trend.schema_name] = trend.total_size_gb;
    return acc;
  }, {} as Record<string, any>);

  const rowCountChartData = Object.values(rowCountData).sort((a: any, b: any) =>
    new Date(a.date).getTime() - new Date(b.date).getTime()
  );

  const sizeChartData = Object.values(sizeData).sort((a: any, b: any) =>
    new Date(a.date).getTime() - new Date(b.date).getTime()
  );

  const formatNumber = (num: number) => {
    if (num >= 1000000) return `${(num / 1000000).toFixed(1)}M`;
    if (num >= 1000) return `${(num / 1000).toFixed(1)}K`;
    return num.toString();
  };

  const formatSize = (gb: number) => {
    return `${gb.toFixed(2)}GB`;
  };

  const renderChart = (data: any[], dataKey: string, title: string, yAxisLabel: string, formatter: (value: number) => string) => {
    const ChartComponent = chartType === 'line' ? LineChart : chartType === 'area' ? AreaChart : BarChart;
    const DataComponent = chartType === 'line' ? Line : chartType === 'area' ? Area : Bar;

    return (
      <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
        <div className="flex justify-between items-center mb-4">
          <h3 className="text-lg font-semibold text-gray-900 dark:text-white">{title}</h3>
          <div className="flex items-center gap-2">
            <button
              onClick={() => setChartType('line')}
              className={`p-2 rounded ${chartType === 'line' ? 'bg-blue-100 text-blue-600' : 'text-gray-400 hover:text-gray-600'}`}
            >
              📈
            </button>
            <button
              onClick={() => setChartType('area')}
              className={`p-2 rounded ${chartType === 'area' ? 'bg-blue-100 text-blue-600' : 'text-gray-400 hover:text-gray-600'}`}
            >
              📊
            </button>
            <button
              onClick={() => setChartType('bar')}
              className={`p-2 rounded ${chartType === 'bar' ? 'bg-blue-100 text-blue-600' : 'text-gray-400 hover:text-gray-600'}`}
            >
              📊
            </button>
          </div>
        </div>
        <ResponsiveContainer width="100%" height={300}>
          <ChartComponent data={data}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis
              dataKey="date"
              fontSize={12}
              tick={{ fontSize: 12 }}
            />
            <YAxis
              label={{ value: yAxisLabel, angle: -90, position: 'insideLeft' }}
              fontSize={12}
              tickFormatter={formatter}
            />
            <Tooltip
              formatter={(value: number) => [formatter(value), yAxisLabel]}
              labelFormatter={(label) => `Date: ${label}`}
            />
            <DataComponent
              type="monotone"
              dataKey={dataKey}
              stroke="#3B82F6"
              fill={chartType === 'area' ? "#3B82F6" : chartType === 'bar' ? "#3B82F6" : undefined}
              fillOpacity={chartType === 'area' ? 0.3 : undefined}
            />
          </ChartComponent>
        </ResponsiveContainer>
      </div>
    );
  };

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold text-gray-900 dark:text-white">
            Data Volume Trends
          </h1>
          <p className="text-gray-600 dark:text-gray-300 mt-1">
            Monitor database growth patterns and volume trends over time
          </p>
        </div>
        <button
          onClick={fetchDataVolumeTrends}
          className="flex items-center gap-2 bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg transition-colors"
        >
          <RefreshCw className="w-4 h-4" />
          Refresh
        </button>
      </div>

      {/* Filters */}
      <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
              Time Range
            </label>
            <select
              value={timeRange}
              onChange={(e) => setTimeRange(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent dark:bg-gray-700 dark:text-white"
            >
              <option value="7d">Last 7 days</option>
              <option value="30d">Last 30 days</option>
              <option value="90d">Last 90 days</option>
              <option value="180d">Last 6 months</option>
            </select>
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
              Schema Filter
            </label>
            <select
              value={selectedSchema}
              onChange={(e) => setSelectedSchema(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent dark:bg-gray-700 dark:text-white"
            >
              <option value="ALL">All Schemas</option>
              {uniqueSchemas.map(schema => (
                <option key={schema} value={schema}>{schema}</option>
              ))}
            </select>
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
              Chart Type
            </label>
            <select
              value={chartType}
              onChange={(e) => setChartType(e.target.value as 'line' | 'area' | 'bar')}
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent dark:bg-gray-700 dark:text-white"
            >
              <option value="line">Line Chart</option>
              <option value="area">Area Chart</option>
              <option value="bar">Bar Chart</option>
            </select>
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
      {!loading && !error && trends.length > 0 && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {renderChart(
            rowCountChartData,
            'totalRows',
            'Row Count Trends',
            'Row Count',
            formatNumber
          )}

          {renderChart(
            sizeChartData,
            'totalSize',
            'Data Size Trends',
            'Size (GB)',
            (value) => formatSize(value)
          )}
        </div>
      )}

      {/* Detailed Trends Table */}
      {!loading && !error && trends.length > 0 && (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow border">
          <div className="p-6 border-b">
            <h2 className="text-xl font-semibold text-gray-900 dark:text-white">
              Detailed Volume Trends
            </h2>
          </div>
          <div className="p-6">
            <div className="overflow-x-auto">
              <table className="min-w-full">
                <thead>
                  <tr className="border-b">
                    <th className="text-left py-3 px-4 font-medium text-gray-700 dark:text-gray-300">Schema</th>
                    <th className="text-left py-3 px-4 font-medium text-gray-700 dark:text-gray-300">Date</th>
                    <th className="text-left py-3 px-4 font-medium text-gray-700 dark:text-gray-300">Total Rows</th>
                    <th className="text-left py-3 px-4 font-medium text-gray-700 dark:text-gray-300">Total Size</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredTrends
                    .sort((a, b) => new Date(a.snapshot_date).getTime() - new Date(b.snapshot_date).getTime())
                    .map((trend, index) => (
                    <tr key={index} className="border-b hover:bg-gray-50 dark:hover:bg-gray-700">
                      <td className="py-3 px-4 font-medium text-gray-900 dark:text-white">
                        {trend.schema_name}
                      </td>
                      <td className="py-3 px-4 text-gray-600 dark:text-gray-300">
                        {new Date(trend.snapshot_date).toLocaleDateString()}
                      </td>
                      <td className="py-3 px-4 text-gray-600 dark:text-gray-300">
                        {formatNumber(trend.total_rows)}
                      </td>
                      <td className="py-3 px-4 text-gray-600 dark:text-gray-300">
                        {formatSize(trend.total_size_gb)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}

      {/* Empty State */}
      {!loading && !error && trends.length === 0 && (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow border p-12">
          <div className="text-center">
            <Database className="w-16 h-16 mx-auto mb-4 text-gray-400" />
            <h3 className="text-xl font-medium text-gray-900 dark:text-white mb-2">
              No Volume Data Available
            </h3>
            <p className="text-gray-600 dark:text-gray-300">
              No data volume trends found for the selected time range.
            </p>
          </div>
        </div>
      )}
    </div>
  );
};

export default DataVolumeTrendsPage;