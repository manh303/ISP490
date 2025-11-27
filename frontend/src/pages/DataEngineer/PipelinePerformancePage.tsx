import React, { useState, useEffect } from 'react';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, LineChart, Line, PieChart, Pie, Cell } from 'recharts';
import { Activity, Clock, CheckCircle, AlertTriangle, RefreshCw, TrendingUp, TrendingDown } from 'lucide-react';
import { getPipelinePerformanceStats } from '../../services/dataEngineerApi';

interface PipelineMetric {
  pipeline_name: string;
  job_code: string;
  status: 'SUCCESS' | 'FAILED' | 'RUNNING' | 'PENDING';
  start_time: string;
  end_time?: string;
  duration_seconds: number;
  records_processed: number;
  error_message?: string;
  schema_name: string;
  table_name: string;
}

interface PerformanceStats {
  total_pipelines: number;
  successful_runs: number;
  failed_runs: number;
  avg_duration: number;
  total_records_processed: number;
}

const PipelinePerformancePage: React.FC = () => {
  const [metrics, setMetrics] = useState<PipelineMetric[]>([]);
  const [stats, setStats] = useState<PerformanceStats | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [timeRange, setTimeRange] = useState<string>('24h');

  const fetchPipelinePerformance = async () => {
    try {
      setLoading(true);
      setError(null);
      // Convert timeRange to days
      const days = timeRange === '1h' ? 1 : timeRange === '24h' ? 1 : timeRange === '7d' ? 7 : 30;
      const data = await getPipelinePerformanceStats(days);
      setMetrics(data.metrics || []);
      setStats(data.stats || null);
    } catch (err) {
      console.error('Error fetching pipeline performance:', err);
      setError('Failed to load pipeline performance data');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchPipelinePerformance();
  }, [timeRange]);

  const getStatusColor = (status: string) => {
    switch (status.toUpperCase()) {
      case 'SUCCESS': return '#10B981';
      case 'FAILED': return '#EF4444';
      case 'RUNNING': return '#F59E0B';
      case 'PENDING': return '#6B7280';
      default: return '#6B7280';
    }
  };

  const getStatusIcon = (status: string) => {
    switch (status.toUpperCase()) {
      case 'SUCCESS': return <CheckCircle className="w-4 h-4" />;
      case 'FAILED': return <AlertTriangle className="w-4 h-4" />;
      case 'RUNNING': return <Activity className="w-4 h-4" />;
      case 'PENDING': return <Clock className="w-4 h-4" />;
      default: return <Clock className="w-4 h-4" />;
    }
  };

  const formatDuration = (seconds: number) => {
    if (seconds < 60) return `${seconds}s`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
    return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
  };

  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleString();
  };

  // Prepare chart data
  const statusDistribution = metrics.reduce((acc, metric) => {
    acc[metric.status] = (acc[metric.status] || 0) + 1;
    return acc;
  }, {} as Record<string, number>);

  const statusChartData = Object.entries(statusDistribution).map(([status, count]) => ({
    name: status,
    value: count,
    color: getStatusColor(status)
  }));

  const durationChartData = metrics
    .filter(m => m.status === 'SUCCESS')
    .sort((a, b) => b.duration_seconds - a.duration_seconds)
    .slice(0, 10)
    .map(m => ({
      name: m.pipeline_name,
      duration: m.duration_seconds,
      jobCode: m.job_code
    }));

  const recordsProcessedData = metrics
    .filter(m => m.records_processed > 0)
    .sort((a, b) => b.records_processed - a.records_processed)
    .slice(0, 10)
    .map(m => ({
      name: m.pipeline_name,
      records: m.records_processed,
      jobCode: m.job_code
    }));

  const successRate = stats ? ((stats.successful_runs / stats.total_pipelines) * 100).toFixed(1) : '0';

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold text-gray-900 dark:text-white">
            Pipeline Performance
          </h1>
          <p className="text-gray-600 dark:text-gray-300 mt-1">
            Monitor ETL pipeline execution metrics and performance trends
          </p>
        </div>
        <div className="flex items-center gap-4">
          <select
            value={timeRange}
            onChange={(e) => setTimeRange(e.target.value)}
            className="px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent dark:bg-gray-700 dark:text-white"
          >
            <option value="1h">Last Hour</option>
            <option value="24h">Last 24 Hours</option>
            <option value="7d">Last 7 Days</option>
            <option value="30d">Last 30 Days</option>
          </select>
          <button
            onClick={fetchPipelinePerformance}
            className="flex items-center gap-2 bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg transition-colors"
          >
            <RefreshCw className="w-4 h-4" />
            Refresh
          </button>
        </div>
      </div>

      {/* Stats Cards */}
      {stats && (
        <div className="grid grid-cols-1 md:grid-cols-5 gap-6">
          <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
            <div className="flex items-center">
              <div className="p-2 bg-blue-100 rounded-lg">
                <Activity className="w-6 h-6 text-blue-600" />
              </div>
              <div className="ml-4">
                <p className="text-sm font-medium text-gray-600 dark:text-gray-300">Total Pipelines</p>
                <p className="text-2xl font-bold text-gray-900 dark:text-white">{stats.total_pipelines}</p>
              </div>
            </div>
          </div>

          <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
            <div className="flex items-center">
              <div className="p-2 bg-green-100 rounded-lg">
                <CheckCircle className="w-6 h-6 text-green-600" />
              </div>
              <div className="ml-4">
                <p className="text-sm font-medium text-gray-600 dark:text-gray-300">Success Rate</p>
                <p className="text-2xl font-bold text-gray-900 dark:text-white">{successRate}%</p>
              </div>
            </div>
          </div>

          <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
            <div className="flex items-center">
              <div className="p-2 bg-red-100 rounded-lg">
                <AlertTriangle className="w-6 h-6 text-red-600" />
              </div>
              <div className="ml-4">
                <p className="text-sm font-medium text-gray-600 dark:text-gray-300">Failed Runs</p>
                <p className="text-2xl font-bold text-gray-900 dark:text-white">{stats.failed_runs}</p>
              </div>
            </div>
          </div>

          <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
            <div className="flex items-center">
              <div className="p-2 bg-purple-100 rounded-lg">
                <Clock className="w-6 h-6 text-purple-600" />
              </div>
              <div className="ml-4">
                <p className="text-sm font-medium text-gray-600 dark:text-gray-300">Avg Duration</p>
                <p className="text-2xl font-bold text-gray-900 dark:text-white">{formatDuration(stats.avg_duration)}</p>
              </div>
            </div>
          </div>

          <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
            <div className="flex items-center">
              <div className="p-2 bg-indigo-100 rounded-lg">
                <TrendingUp className="w-6 h-6 text-indigo-600" />
              </div>
              <div className="ml-4">
                <p className="text-sm font-medium text-gray-600 dark:text-gray-300">Records Processed</p>
                <p className="text-2xl font-bold text-gray-900 dark:text-white">{stats.total_records_processed.toLocaleString()}</p>
              </div>
            </div>
          </div>
        </div>
      )}

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
      {!loading && !error && metrics.length > 0 && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Status Distribution */}
          <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
              Pipeline Status Distribution
            </h3>
            <ResponsiveContainer width="100%" height={300}>
              <PieChart>
                <Pie
                  data={statusChartData}
                  cx="50%"
                  cy="50%"
                  labelLine={false}
                  label={({ name, percent }) => `${name} ${percent ? (percent * 100).toFixed(0) : 0}%`}
                  outerRadius={80}
                  fill="#8884d8"
                  dataKey="value"
                >
                  {statusChartData.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={entry.color} />
                  ))}
                </Pie>
                <Tooltip />
              </PieChart>
            </ResponsiveContainer>
          </div>

          {/* Top Duration Pipelines */}
          <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
              Longest Running Pipelines
            </h3>
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={durationChartData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis
                  dataKey="name"
                  angle={-45}
                  textAnchor="end"
                  height={80}
                  fontSize={12}
                />
                <YAxis
                  label={{ value: 'Duration (seconds)', angle: -90, position: 'insideLeft' }}
                  fontSize={12}
                />
                <Tooltip
                  formatter={(value: number) => [formatDuration(value), 'Duration']}
                  labelFormatter={(label) => `Pipeline: ${label}`}
                />
                <Bar dataKey="duration" fill="#3B82F6" />
              </BarChart>
            </ResponsiveContainer>
          </div>

          {/* Records Processed */}
          <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border lg:col-span-2">
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
              Records Processed by Pipeline
            </h3>
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={recordsProcessedData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis
                  dataKey="name"
                  angle={-45}
                  textAnchor="end"
                  height={80}
                  fontSize={12}
                />
                <YAxis
                  label={{ value: 'Records', angle: -90, position: 'insideLeft' }}
                  fontSize={12}
                />
                <Tooltip
                  formatter={(value: number) => [value.toLocaleString(), 'Records']}
                  labelFormatter={(label) => `Pipeline: ${label}`}
                />
                <Bar dataKey="records" fill="#10B981" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {/* Detailed Table */}
      {!loading && !error && metrics.length > 0 && (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow border">
          <div className="p-6 border-b">
            <h2 className="text-xl font-semibold text-gray-900 dark:text-white">
              Pipeline Execution Details
            </h2>
          </div>
          <div className="p-6">
            <div className="overflow-x-auto">
              <table className="min-w-full">
                <thead>
                  <tr className="border-b">
                    <th className="text-left py-3 px-4 font-medium text-gray-700 dark:text-gray-300">Pipeline</th>
                    <th className="text-left py-3 px-4 font-medium text-gray-700 dark:text-gray-300">Status</th>
                    <th className="text-left py-3 px-4 font-medium text-gray-700 dark:text-gray-300">Duration</th>
                    <th className="text-left py-3 px-4 font-medium text-gray-700 dark:text-gray-300">Records</th>
                    <th className="text-left py-3 px-4 font-medium text-gray-700 dark:text-gray-300">Table</th>
                    <th className="text-left py-3 px-4 font-medium text-gray-700 dark:text-gray-300">Start Time</th>
                    <th className="text-left py-3 px-4 font-medium text-gray-700 dark:text-gray-300">End Time</th>
                  </tr>
                </thead>
                <tbody>
                  {metrics.map((metric, index) => (
                    <tr key={index} className="border-b hover:bg-gray-50 dark:hover:bg-gray-700">
                      <td className="py-3 px-4">
                        <div>
                          <p className="font-medium text-gray-900 dark:text-white">
                            {metric.pipeline_name}
                          </p>
                          <p className="text-sm text-gray-500">{metric.job_code}</p>
                        </div>
                      </td>
                      <td className="py-3 px-4">
                        <div className="flex items-center">
                          {getStatusIcon(metric.status)}
                          <span
                            className="ml-2 inline-flex px-2 py-1 text-xs rounded-full text-white"
                            style={{ backgroundColor: getStatusColor(metric.status) }}
                          >
                            {metric.status}
                          </span>
                        </div>
                      </td>
                      <td className="py-3 px-4 text-gray-600 dark:text-gray-300">
                        {formatDuration(metric.duration_seconds)}
                      </td>
                      <td className="py-3 px-4 text-gray-600 dark:text-gray-300">
                        {metric.records_processed.toLocaleString()}
                      </td>
                      <td className="py-3 px-4">
                        <div>
                          <p className="font-medium text-gray-900 dark:text-white">
                            {metric.table_name}
                          </p>
                          <p className="text-sm text-gray-500">{metric.schema_name}</p>
                        </div>
                      </td>
                      <td className="py-3 px-4 text-sm text-gray-600 dark:text-gray-300">
                        {formatDate(metric.start_time)}
                      </td>
                      <td className="py-3 px-4 text-sm text-gray-600 dark:text-gray-300">
                        {metric.end_time ? formatDate(metric.end_time) : '-'}
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
      {!loading && !error && metrics.length === 0 && (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow border p-12">
          <div className="text-center">
            <Activity className="w-16 h-16 mx-auto mb-4 text-gray-400" />
            <h3 className="text-xl font-medium text-gray-900 dark:text-white mb-2">
              No Pipeline Data Available
            </h3>
            <p className="text-gray-600 dark:text-gray-300">
              No pipeline performance data found for the selected time range.
            </p>
          </div>
        </div>
      )}
    </div>
  );
};

export default PipelinePerformancePage;