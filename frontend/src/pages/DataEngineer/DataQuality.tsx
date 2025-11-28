import React, { useState, useEffect } from 'react';
import {
  AlertTriangle,
  CheckCircle,
  XCircle,
  TrendingUp,
  BarChart3,
  RefreshCw,
  Filter
} from 'lucide-react';
import {
  getDataQualityIssues,
  getDataQualitySummary,
  DataQualityIssue,
  DataQualitySummaryItem
} from '../../services/dataEngineerApi';

const DataQuality: React.FC = () => {
  const [issues, setIssues] = useState<DataQualityIssue[]>([]);
  const [summary, setSummary] = useState<DataQualitySummaryItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [filters, setFilters] = useState({
    status: 'OPEN',
    severity: '',
    schemaName: ''
  });

  useEffect(() => {
    fetchData();
  }, [filters]);

  const fetchData = async () => {
    try {
      setLoading(true);
      const [issuesData, summaryData] = await Promise.all([
        getDataQualityIssues(filters.status, filters.severity, filters.schemaName),
        getDataQualitySummary()
      ]);
      setIssues(issuesData);
      setSummary(summaryData);
    } catch (error) {
      console.error('Error fetching data quality data:', error);
    } finally {
      setLoading(false);
    }
  };

  const getSeverityColor = (severity: string) => {
    switch (severity?.toLowerCase()) {
      case 'critical':
      case 'high':
        return 'text-red-600 bg-red-100';
      case 'warning':
      case 'medium':
        return 'text-yellow-600 bg-yellow-100';
      case 'low':
        return 'text-blue-600 bg-blue-100';
      default:
        return 'text-gray-600 bg-gray-100';
    }
  };

  const getStatusColor = (status: string) => {
    switch (status?.toLowerCase()) {
      case 'open':
        return 'text-orange-600 bg-orange-100';
      case 'in_progress':
        return 'text-blue-600 bg-blue-100';
      case 'resolved':
        return 'text-green-600 bg-green-100';
      case 'ignored':
        return 'text-gray-600 bg-gray-100';
      default:
        return 'text-gray-600 bg-gray-100';
    }
  };

  const getSeverityIcon = (severity: string) => {
    switch (severity?.toLowerCase()) {
      case 'critical':
      case 'high':
        return <XCircle className="w-5 h-5 text-red-600" />;
      case 'warning':
      case 'medium':
        return <AlertTriangle className="w-5 h-5 text-yellow-600" />;
      case 'low':
        return <CheckCircle className="w-5 h-5 text-blue-600" />;
      default:
        return <AlertTriangle className="w-5 h-5 text-gray-600" />;
    }
  };

  const handleFilterChange = (key: string, value: string) => {
    setFilters(prev => ({ ...prev, [key]: value }));
  };

  const getTotalIssues = () => summary.reduce((sum, item) => sum + item.issue_count, 0);
  const getOpenIssues = () => summary.filter(item => item.status === 'OPEN').reduce((sum, item) => sum + item.issue_count, 0);
  const getResolvedIssues = () => summary.filter(item => item.status === 'RESOLVED').reduce((sum, item) => sum + item.issue_count, 0);
  const getAvgResolutionHours = () => 'N/A';

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
      </div>
    );
  }

  return (
    <div className="p-6">
      <div className="mb-6">
        <h1 className="text-3xl font-bold text-gray-900 dark:text-white mb-2">
          Quản lý Chất lượng Dữ liệu
        </h1>
        <p className="text-gray-600 dark:text-gray-300">
          Giám sát và giải quyết vấn đề chất lượng dữ liệu trên tất cả các bảng
        </p>
      </div>

      {/* Summary Cards */}
      {summary.length > 0 && (
        <div className="grid grid-cols-1 md:grid-cols-4 gap-6 mb-6">
          <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-gray-500 dark:text-gray-400">Tổng số Vấn đề</p>
                <p className="text-2xl font-bold text-gray-900 dark:text-white">
                  {getTotalIssues()}
                </p>
              </div>
              <AlertTriangle className="w-8 h-8 text-orange-600" />
            </div>
          </div>

          <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-gray-500 dark:text-gray-400">Vấn đề Mở</p>
                <p className="text-2xl font-bold text-orange-600">
                  {getOpenIssues()}
                </p>
              </div>
              <XCircle className="w-8 h-8 text-orange-600" />
            </div>
          </div>

          <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-gray-500 dark:text-gray-400">Vấn đề Đã giải quyết</p>
                <p className="text-2xl font-bold text-green-600">
                  {getResolvedIssues()}
                </p>
              </div>
              <CheckCircle className="w-8 h-8 text-green-600" />
            </div>
          </div>

          <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-gray-500 dark:text-gray-400">Thời gian Giải quyết Trung bình</p>
                <p className="text-2xl font-bold text-blue-600">
                  {getAvgResolutionHours()}
                </p>
              </div>
              <TrendingUp className="w-8 h-8 text-blue-600" />
            </div>
          </div>
        </div>
      )}

      {/* Filters */}
      <div className="bg-white dark:bg-gray-800 p-4 rounded-lg shadow border mb-6">
        <div className="flex items-center gap-4">
          <Filter className="w-5 h-5 text-gray-500" />
          <span className="font-medium text-gray-900 dark:text-white">Bộ lọc</span>
          <div className="flex gap-4">
            <select
              value={filters.status}
              onChange={(e) => handleFilterChange('status', e.target.value)}
              className="px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-white"
            >
              <option value="OPEN">Mở</option>
              <option value="IN_PROGRESS">Đang tiến hành</option>
              <option value="RESOLVED">Đã giải quyết</option>
              <option value="IGNORED">Bỏ qua</option>
            </select>

            <select
              value={filters.severity}
              onChange={(e) => handleFilterChange('severity', e.target.value)}
              className="px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-white"
            >
              <option value="">Tất cả Mức độ nghiêm trọng</option>
              <option value="CRITICAL">Critical</option>
              <option value="HIGH">High</option>
              <option value="MEDIUM">Medium</option>
              <option value="LOW">Low</option>
            </select>

            <input
              type="text"
              placeholder="Tên lược đồ..."
              value={filters.schemaName}
              onChange={(e) => handleFilterChange('schemaName', e.target.value)}
              className="px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-white"
            />

            <button
              onClick={fetchData}
              className="flex items-center gap-2 bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg transition-colors"
            >
              <RefreshCw className="w-4 h-4" />
              Làm mới
            </button>
          </div>
        </div>
      </div>

      {/* Issues List */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow border">
        <div className="p-6 border-b">
          <h2 className="text-xl font-semibold text-gray-900 dark:text-white flex items-center">
            <AlertTriangle className="w-5 h-5 mr-2" />
            Data Quality Issues ({issues.length})
          </h2>
        </div>
        <div className="p-6">
          {issues.length === 0 ? (
            <div className="text-center py-12">
              <CheckCircle className="w-16 h-16 mx-auto mb-4 text-green-500" />
              <h3 className="text-lg font-medium text-gray-900 dark:text-white mb-2">
                Không tìm thấy Vấn đề
              </h3>
              <p className="text-gray-600 dark:text-gray-300">
                Tất cả kiểm tra chất lượng dữ liệu đều đang vượt qua
              </p>
            </div>
          ) : (
            <div className="space-y-4">
              {issues.map((issue) => (
                <div key={issue.issue_id} className="border border-gray-200 dark:border-gray-700 rounded-lg p-4">
                  <div className="flex items-start justify-between mb-3">
                    <div className="flex items-center gap-3">
                      {getSeverityIcon(issue.severity)}
                      <div>
                        <h3 className="font-medium text-gray-900 dark:text-white">
                          {issue.issue_type}
                        </h3>
                        <p className="text-sm text-gray-600 dark:text-gray-300">
                          {issue.schema_name}.{issue.table_name}
                        </p>
                      </div>
                    </div>
                    <div className="flex gap-2">
                      <span className={`inline-flex px-2 py-1 text-xs rounded-full ${getSeverityColor(issue.severity)}`}>
                        {issue.severity}
                      </span>
                      <span className={`inline-flex px-2 py-1 text-xs rounded-full ${getStatusColor(issue.status)}`}>
                        {issue.status}
                      </span>
                    </div>
                  </div>

                  <p className="text-gray-700 dark:text-gray-300 mb-3">
                    {issue.issue_description}
                  </p>

                  <div className="flex items-center justify-between text-sm text-gray-500">
                    <div className="flex gap-4">
                      <span>Hàng bị ảnh hưởng: {issue.affected_rows.toLocaleString()}</span>
                      <span>Đã phát hiện: {new Date(issue.detected_at).toLocaleString()}</span>
                    </div>
                    <div className="flex gap-2">
                      <button className="text-blue-600 hover:text-blue-800">
                        Xem Chi tiết
                      </button>
                      <button className="text-green-600 hover:text-green-800">
                        Đánh dấu Đã giải quyết
                      </button>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default DataQuality;