import React, { useState, useEffect } from 'react';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { Activity, RefreshCw } from 'lucide-react';
import { getPipelinePerformanceStats } from '../../services/dataEngineerApi';

interface PipelineMetric {
  job_code: string;
  job_name: string;
  run_date: string;
  runs_count: number;
  success_count: number;
  failed_count: number;
  avg_duration_minutes: number;
  min_duration_minutes: number;
  max_duration_minutes: number;
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
      setMetrics(data);
      setStats(null);
    } catch (err) {
      console.error('Error fetching pipeline performance:', err);
      setError('Không thể tải dữ liệu hiệu suất đường ống');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchPipelinePerformance();
  }, [timeRange]);

  const formatDuration = (minutes: number) => {
    if (minutes < 1) return `${(minutes * 60).toFixed(0)}s`;
    if (minutes < 60) return `${minutes.toFixed(1)}m`;
    return `${Math.floor(minutes / 60)}h ${Math.floor(minutes % 60)}m`;
  };

  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleDateString();
  };

  // Prepare chart data
  const successRateData = metrics.map(metric => ({
    date: formatDate(metric.run_date),
    job: metric.job_name,
    successRate: metric.runs_count > 0 ? (metric.success_count / metric.runs_count) * 100 : 0,
    avgDuration: metric.avg_duration_minutes
  }));

  const durationChartData = metrics
    .sort((a, b) => b.avg_duration_minutes - a.avg_duration_minutes)
    .slice(0, 10)
    .map(m => ({
      name: m.job_name,
      duration: m.avg_duration_minutes,
      jobCode: m.job_code
    }));

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold text-gray-900 dark:text-white">
            Hiệu suất Đường ống
          </h1>
          <p className="text-gray-600 dark:text-gray-300 mt-1">
            Giám sát các chỉ số thực thi đường ống ETL và xu hướng hiệu suất
          </p>
        </div>
        <div className="flex items-center gap-4">
          <select
            value={timeRange}
            onChange={(e) => setTimeRange(e.target.value)}
            className="px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent dark:bg-gray-700 dark:text-white"
          >
            <option value="1h">Giờ qua</option>
            <option value="24h">24 giờ qua</option>
            <option value="7d">7 ngày qua</option>
            <option value="30d">30 ngày qua</option>
          </select>
          <button
            onClick={fetchPipelinePerformance}
            className="flex items-center gap-2 bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg transition-colors"
          >
            <RefreshCw className="w-4 h-4" />
            Làm mới
          </button>
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
      {!loading && !error && metrics.length > 0 && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Success Rate */}
          <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
              Tỷ lệ Thành công theo Công việc
            </h3>
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={successRateData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis
                  dataKey="date"
                  angle={-45}
                  textAnchor="end"
                  height={80}
                  fontSize={12}
                />
                <YAxis
                  label={{ value: 'Success Rate (%)', angle: -90, position: 'insideLeft' }}
                  fontSize={12}
                />
                <Tooltip
                  formatter={(value: number) => [`${value.toFixed(1)}%`, 'Success Rate']}
                  labelFormatter={(label) => `Date: ${label}`}
                />
                <Bar dataKey="successRate" fill="#10B981" />
              </BarChart>
            </ResponsiveContainer>
          </div>

          {/* Average Duration */}
          <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
              Thời lượng Trung bình theo Công việc
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
                  label={{ value: 'Duration (minutes)', angle: -90, position: 'insideLeft' }}
                  fontSize={12}
                />
                <Tooltip
                  formatter={(value: number) => [formatDuration(value), 'Duration']}
                  labelFormatter={(label) => `Job: ${label}`}
                />
                <Bar dataKey="duration" fill="#3B82F6" />
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
              Chi tiết Thực thi Đường ống
            </h2>
          </div>
          <div className="p-6">
            <div className="overflow-x-auto">
              <table className="min-w-full">
                <thead>
                  <tr className="border-b">
                    <th className="text-left py-3 px-4 font-medium text-gray-700 dark:text-gray-300">Tên Công việc</th>
                    <th className="text-left py-3 px-4 font-medium text-gray-700 dark:text-gray-300">Ngày Chạy</th>
                    <th className="text-left py-3 px-4 font-medium text-gray-700 dark:text-gray-300">Số lượng Chạy</th>
                    <th className="text-left py-3 px-4 font-medium text-gray-700 dark:text-gray-300">Số lượng Thành công</th>
                    <th className="text-left py-3 px-4 font-medium text-gray-700 dark:text-gray-300">Số lượng Thất bại</th>
                    <th className="text-left py-3 px-4 font-medium text-gray-700 dark:text-gray-300">Thời lượng Trung bình</th>
                    <th className="text-left py-3 px-4 font-medium text-gray-700 dark:text-gray-300">Thời lượng Tối thiểu</th>
                    <th className="text-left py-3 px-4 font-medium text-gray-700 dark:text-gray-300">Thời lượng Tối đa</th>
                  </tr>
                </thead>
                <tbody>
                  {metrics.map((metric, index) => (
                    <tr key={index} className="border-b hover:bg-gray-50 dark:hover:bg-gray-700">
                      <td className="py-3 px-4">
                        <div>
                          <p className="font-medium text-gray-900 dark:text-white">
                            {metric.job_name}
                          </p>
                          <p className="text-sm text-gray-500">{metric.job_code}</p>
                        </div>
                      </td>
                      <td className="py-3 px-4 text-gray-600 dark:text-gray-300">
                        {formatDate(metric.run_date)}
                      </td>
                      <td className="py-3 px-4 text-gray-600 dark:text-gray-300">
                        {metric.runs_count}
                      </td>
                      <td className="py-3 px-4 text-green-600">
                        {metric.success_count}
                      </td>
                      <td className="py-3 px-4 text-red-600">
                        {metric.failed_count}
                      </td>
                      <td className="py-3 px-4 text-gray-600 dark:text-gray-300">
                        {formatDuration(metric.avg_duration_minutes)}
                      </td>
                      <td className="py-3 px-4 text-gray-600 dark:text-gray-300">
                        {formatDuration(metric.min_duration_minutes)}
                      </td>
                      <td className="py-3 px-4 text-gray-600 dark:text-gray-300">
                        {formatDuration(metric.max_duration_minutes)}
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
              Không có Dữ liệu Đường ống nào
            </h3>
            <p className="text-gray-600 dark:text-gray-300">
              Không tìm thấy dữ liệu hiệu suất đường ống cho khoảng thời gian đã chọn.
            </p>
          </div>
        </div>
      )}
    </div>
  );
};

export default PipelinePerformancePage;