import React, { useState, useEffect } from 'react';
import { AlertTriangle, Clock, CheckCircle, XCircle, RefreshCw, Filter } from 'lucide-react';
import { getAlertHistory } from '../../services/dataEngineerApi';

interface AlertItem {
  alert_id: number;
  alert_type: string;
  severity: 'LOW' | 'MEDIUM' | 'HIGH' | 'CRITICAL';
  message: string;
  status: 'ACTIVE' | 'RESOLVED' | 'ACKNOWLEDGED';
  created_at: string;
  resolved_at?: string;
  schema_name: string;
  table_name: string;
  job_code?: string;
}

const AlertHistoryPage: React.FC = () => {
  const [alerts, setAlerts] = useState<AlertItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [statusFilter, setStatusFilter] = useState<string>('ALL');
  const [severityFilter, setSeverityFilter] = useState<string>('ALL');
  const [schemaFilter, setSchemaFilter] = useState<string>('ALL');
  const [dateRange, setDateRange] = useState<string>('7d');

  const fetchAlertHistory = async () => {
    try {
      setLoading(true);
      setError(null);
      const data = await getAlertHistory();
      setAlerts(data);
    } catch (err) {
      console.error('Error fetching alert history:', err);
      setError('Không thể tải lịch sử cảnh báo');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchAlertHistory();
  }, []);

  const getSeverityColor = (severity: string) => {
    switch (severity.toUpperCase()) {
      case 'CRITICAL': return 'bg-red-100 text-red-800 border-red-200';
      case 'HIGH': return 'bg-orange-100 text-orange-800 border-orange-200';
      case 'MEDIUM': return 'bg-yellow-100 text-yellow-800 border-yellow-200';
      case 'LOW': return 'bg-blue-100 text-blue-800 border-blue-200';
      default: return 'bg-gray-100 text-gray-800 border-gray-200';
    }
  };

  const getStatusColor = (status: string) => {
    switch (status.toUpperCase()) {
      case 'ACTIVE': return 'bg-red-100 text-red-800';
      case 'RESOLVED': return 'bg-green-100 text-green-800';
      case 'ACKNOWLEDGED': return 'bg-blue-100 text-blue-800';
      default: return 'bg-gray-100 text-gray-800';
    }
  };

  const getStatusIcon = (status: string) => {
    switch (status.toUpperCase()) {
      case 'ACTIVE': return <AlertTriangle className="w-4 h-4" />;
      case 'RESOLVED': return <CheckCircle className="w-4 h-4" />;
      case 'ACKNOWLEDGED': return <Clock className="w-4 h-4" />;
      default: return <XCircle className="w-4 h-4" />;
    }
  };

  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleString();
  };

  const filteredAlerts = alerts.filter(alert => {
    const statusMatch = statusFilter === 'ALL' || alert.status === statusFilter;
    const severityMatch = severityFilter === 'ALL' || alert.severity === severityFilter;
    const schemaMatch = schemaFilter === 'ALL' || alert.schema_name === schemaFilter;
    return statusMatch && severityMatch && schemaMatch;
  });

  const uniqueSchemas = [...new Set(alerts.map(alert => alert.schema_name))];

  const getAlertStats = () => {
    const total = alerts.length;
    const active = alerts.filter(a => a.status === 'ACTIVE').length;
    const resolved = alerts.filter(a => a.status === 'RESOLVED').length;
    const critical = alerts.filter(a => a.severity === 'CRITICAL').length;

    return { total, active, resolved, critical };
  };

  const stats = getAlertStats();

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold text-gray-900 dark:text-white">
            Lịch sử Cảnh báo
          </h1>
          <p className="text-gray-600 dark:text-gray-300 mt-1">
            Giám sát và theo dõi cảnh báo và thông báo hệ thống
          </p>
        </div>
        <button
          onClick={fetchAlertHistory}
          className="flex items-center gap-2 bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg transition-colors"
        >
          <RefreshCw className="w-4 h-4" />
          Làm mới
        </button>
      </div>

      {/* Stats Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
        <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
          <div className="flex items-center">
            <div className="p-2 bg-blue-100 rounded-lg">
              <AlertTriangle className="w-6 h-6 text-blue-600" />
            </div>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-600 dark:text-gray-300">Tổng số Cảnh báo</p>
              <p className="text-2xl font-bold text-gray-900 dark:text-white">{stats.total}</p>
            </div>
          </div>
        </div>

        <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
          <div className="flex items-center">
            <div className="p-2 bg-red-100 rounded-lg">
              <AlertTriangle className="w-6 h-6 text-red-600" />
            </div>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-600 dark:text-gray-300">Cảnh báo Đang hoạt động</p>
              <p className="text-2xl font-bold text-gray-900 dark:text-white">{stats.active}</p>
            </div>
          </div>
        </div>

        <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
          <div className="flex items-center">
            <div className="p-2 bg-green-100 rounded-lg">
              <CheckCircle className="w-6 h-6 text-green-600" />
            </div>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-600 dark:text-gray-300">Đã giải quyết</p>
              <p className="text-2xl font-bold text-gray-900 dark:text-white">{stats.resolved}</p>
            </div>
          </div>
        </div>

        <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
          <div className="flex items-center">
            <div className="p-2 bg-red-100 rounded-lg">
              <XCircle className="w-6 h-6 text-red-600" />
            </div>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-600 dark:text-gray-300">Nghiêm trọng</p>
              <p className="text-2xl font-bold text-gray-900 dark:text-white">{stats.critical}</p>
            </div>
          </div>
        </div>
      </div>

      {/* Filters */}
      <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
        <div className="flex items-center mb-4">
          <Filter className="w-5 h-5 mr-2 text-gray-600 dark:text-gray-300" />
          <h3 className="text-lg font-semibold text-gray-900 dark:text-white">Bộ lọc</h3>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
              Trạng thái
            </label>
            <select
              value={statusFilter}
              onChange={(e) => setStatusFilter(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent dark:bg-gray-700 dark:text-white"
            >
              <option value="ALL">Tất cả Trạng thái</option>
              <option value="ACTIVE">Đang hoạt động</option>
              <option value="RESOLVED">Đã giải quyết</option>
              <option value="ACKNOWLEDGED">Đã xác nhận</option>
            </select>
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
              Mức độ nghiêm trọng
            </label>
            <select
              value={severityFilter}
              onChange={(e) => setSeverityFilter(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent dark:bg-gray-700 dark:text-white"
            >
              <option value="ALL">Tất cả Mức độ</option>
              <option value="CRITICAL">Nghiêm trọng</option>
              <option value="HIGH">Cao</option>
              <option value="MEDIUM">Trung bình</option>
              <option value="LOW">Thấp</option>
            </select>
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
              Lược đồ
            </label>
            <select
              value={schemaFilter}
              onChange={(e) => setSchemaFilter(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent dark:bg-gray-700 dark:text-white"
            >
              <option value="ALL">Tất cả Lược đồ</option>
              {uniqueSchemas.map(schema => (
                <option key={schema} value={schema}>{schema}</option>
              ))}
            </select>
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
              Khoảng thời gian
            </label>
            <select
              value={dateRange}
              onChange={(e) => setDateRange(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent dark:bg-gray-700 dark:text-white"
            >
              <option value="1d">24 giờ qua</option>
              <option value="7d">7 ngày qua</option>
              <option value="30d">30 ngày qua</option>
              <option value="90d">90 ngày qua</option>
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

      {/* Alerts Table */}
      {!loading && !error && (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow border">
          <div className="p-6 border-b">
            <h2 className="text-xl font-semibold text-gray-900 dark:text-white">
              Lịch sử Cảnh báo ({filteredAlerts.length} cảnh báo)
            </h2>
          </div>
          <div className="p-6">
            <div className="overflow-x-auto">
              <table className="min-w-full">
                <thead>
                  <tr className="border-b">
                    <th className="text-left py-3 px-4 font-medium text-gray-700 dark:text-gray-300">Trạng thái</th>
                    <th className="text-left py-3 px-4 font-medium text-gray-700 dark:text-gray-300">Mức độ nghiêm trọng</th>
                    <th className="text-left py-3 px-4 font-medium text-gray-700 dark:text-gray-300">Thông điệp</th>
                    <th className="text-left py-3 px-4 font-medium text-gray-700 dark:text-gray-300">Bảng</th>
                    <th className="text-left py-3 px-4 font-medium text-gray-700 dark:text-gray-300">Mã Công việc</th>
                    <th className="text-left py-3 px-4 font-medium text-gray-700 dark:text-gray-300">Đã tạo</th>
                    <th className="text-left py-3 px-4 font-medium text-gray-700 dark:text-gray-300">Đã giải quyết</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredAlerts.map((alert) => (
                    <tr key={alert.alert_id} className="border-b hover:bg-gray-50 dark:hover:bg-gray-700">
                      <td className="py-3 px-4">
                        <div className="flex items-center">
                          {getStatusIcon(alert.status)}
                          <span className={`ml-2 inline-flex px-2 py-1 text-xs rounded-full ${getStatusColor(alert.status)}`}>
                            {alert.status}
                          </span>
                        </div>
                      </td>
                      <td className="py-3 px-4">
                        <span className={`inline-flex px-2 py-1 text-xs rounded-full border ${getSeverityColor(alert.severity)}`}>
                          {alert.severity}
                        </span>
                      </td>
                      <td className="py-3 px-4">
                        <div className="max-w-xs">
                          <p className="text-sm text-gray-900 dark:text-white truncate" title={alert.message}>
                            {alert.message}
                          </p>
                        </div>
                      </td>
                      <td className="py-3 px-4">
                        <div>
                          <p className="font-medium text-gray-900 dark:text-white">
                            {alert.table_name}
                          </p>
                          <p className="text-sm text-gray-500">{alert.schema_name}</p>
                        </div>
                      </td>
                      <td className="py-3 px-4 text-gray-600 dark:text-gray-300">
                        {alert.job_code || '-'}
                      </td>
                      <td className="py-3 px-4 text-sm text-gray-600 dark:text-gray-300">
                        {formatDate(alert.created_at)}
                      </td>
                      <td className="py-3 px-4 text-sm text-gray-600 dark:text-gray-300">
                        {alert.resolved_at ? formatDate(alert.resolved_at) : '-'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            {filteredAlerts.length === 0 && (
              <div className="text-center py-12">
                <AlertTriangle className="w-16 h-16 mx-auto mb-4 text-gray-400" />
                <h3 className="text-xl font-medium text-gray-900 dark:text-white mb-2">
                  Không tìm thấy Cảnh báo
                </h3>
                <p className="text-gray-600 dark:text-gray-300">
                  Không có cảnh báo nào khớp với tiêu chí bộ lọc hiện tại.
                </p>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
};

export default AlertHistoryPage;