import React, { useState, useEffect } from 'react';
import {
  Activity,
  Database,
  AlertTriangle,
  TrendingUp,
  CheckCircle,
  XCircle,
  Clock,
  Server,
  BarChart3,
  RefreshCw
} from 'lucide-react';
import {
  getHealth,
  getETLJobs,
  getTableHealth,
  getDataQualityIssues,
  getDatabaseHealth,
  getAlertSummary,
  HealthResponse,
  ETLJob,
  TableHealth,
  DataQualityIssue,
  DatabaseHealth,
  AlertSummary
} from '../../services/dataEngineerApi';

const DataEngineerDashboard: React.FC = () => {
  const [health, setHealth] = useState<HealthResponse | null>(null);
  const [etlJobs, setEtlJobs] = useState<ETLJob[]>([]);
  const [tableHealth, setTableHealth] = useState<TableHealth[]>([]);
  const [dataQualityIssues, setDataQualityIssues] = useState<DataQualityIssue[]>([]);
  const [databaseHealth, setDatabaseHealth] = useState<DatabaseHealth | null>(null);
  const [alerts, setAlerts] = useState<AlertSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchData = async () => {
    try {
      setLoading(true);
      setError(null);

      const [
        healthData,
        etlJobsData,
        tableHealthData,
        dataQualityData,
        dbHealthData,
        alertsData
      ] = await Promise.all([
        getHealth(),
        getETLJobs(),
        getTableHealth(),
        getDataQualityIssues(),
        getDatabaseHealth(),
        getAlertSummary()
      ]);

      setHealth(healthData);
      setEtlJobs(etlJobsData);
      setTableHealth(tableHealthData);
      setDataQualityIssues(dataQualityData);
      setDatabaseHealth(dbHealthData);
      setAlerts(alertsData);
    } catch (err) {
      console.error('Error fetching data:', err);
      setError('Failed to load dashboard data');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, []);

  const getStatusColor = (status: string) => {
    switch (status?.toLowerCase()) {
      case 'healthy':
      case 'success':
      case 'active':
        return 'text-green-600 bg-green-100';
      case 'warning':
      case 'running':
        return 'text-yellow-600 bg-yellow-100';
      case 'error':
      case 'failed':
      case 'critical':
        return 'text-red-600 bg-red-100';
      default:
        return 'text-gray-600 bg-gray-100';
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

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="p-6">
        <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded">
          {error}
        </div>
      </div>
    );
  }

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold text-gray-900 dark:text-white">
            Data Engineer Dashboard
          </h1>
          <p className="text-gray-600 dark:text-gray-300 mt-1">
            Monitor ETL pipelines, data quality, and system health
          </p>
        </div>
        <button
          onClick={fetchData}
          className="flex items-center gap-2 bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg transition-colors"
        >
          <RefreshCw className="w-4 h-4" />
          Refresh
        </button>
      </div>

      {/* System Health Overview */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-gray-500 dark:text-gray-400">System Health</p>
              <p className={`text-2xl font-bold ${health?.status === 'healthy' ? 'text-green-600' : 'text-red-600'}`}>
                {health?.status === 'healthy' ? 'Healthy' : 'Unhealthy'}
              </p>
            </div>
            <Activity className={`w-8 h-8 ${health?.status === 'healthy' ? 'text-green-600' : 'text-red-600'}`} />
          </div>
        </div>

        <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-gray-500 dark:text-gray-400">Database Status</p>
              <p className={`text-2xl font-bold ${databaseHealth?.status?.toLowerCase() === 'healthy' ? 'text-green-600' : 'text-red-600'}`}>
                {databaseHealth?.status ? databaseHealth.status.charAt(0) + databaseHealth.status.slice(1).toLowerCase() : 'Unknown'}
              </p>
            </div>
            <Database className={`w-8 h-8 ${databaseHealth?.status?.toLowerCase() === 'healthy' ? 'text-green-600' : 'text-red-600'}`} />
          </div>
        </div>

        <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-gray-500 dark:text-gray-400">Active ETL Jobs</p>
              <p className="text-2xl font-bold text-blue-600">
                {etlJobs.filter(job => job.is_active).length}
              </p>
            </div>
            <Server className="w-8 h-8 text-blue-600" />
          </div>
        </div>

        <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-gray-500 dark:text-gray-400">Data Quality Issues</p>
              <p className="text-2xl font-bold text-orange-600">
                {dataQualityIssues.length}
              </p>
            </div>
            <AlertTriangle className="w-8 h-8 text-orange-600" />
          </div>
        </div>
      </div>

      {/* ETL Jobs Status */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow border">
        <div className="p-6 border-b">
          <h2 className="text-xl font-semibold text-gray-900 dark:text-white flex items-center">
            <Server className="w-5 h-5 mr-2" />
            ETL Pipeline Status
          </h2>
        </div>
        <div className="p-6">
          <div className="overflow-x-auto">
            <table className="min-w-full">
              <thead>
                <tr className="border-b">
                  <th className="text-left py-2 px-4 font-medium text-gray-700 dark:text-gray-300">Job Name</th>
                  <th className="text-left py-2 px-4 font-medium text-gray-700 dark:text-gray-300">Status</th>
                  <th className="text-left py-2 px-4 font-medium text-gray-700 dark:text-gray-300">Last Run</th>
                  <th className="text-left py-2 px-4 font-medium text-gray-700 dark:text-gray-300">Success Rate</th>
                  <th className="text-left py-2 px-4 font-medium text-gray-700 dark:text-gray-300">Duration</th>
                </tr>
              </thead>
              <tbody>
                {etlJobs.map((job) => (
                  <tr key={job.job_code} className="border-b hover:bg-gray-50 dark:hover:bg-gray-700">
                    <td className="py-3 px-4">
                      <div>
                        <p className="font-medium text-gray-900 dark:text-white">{job.job_name}</p>
                        <p className="text-sm text-gray-500">{job.job_code}</p>
                      </div>
                    </td>
                    <td className="py-3 px-4">
                      <span className={`inline-flex px-2 py-1 text-xs rounded-full ${getStatusColor(job.last_run_status)}`}>
                        {job.last_run_status}
                      </span>
                    </td>
                    <td className="py-3 px-4 text-sm text-gray-600 dark:text-gray-300">
                      {job.last_run_date}
                    </td>
                    <td className="py-3 px-4">
                      <span className={`font-medium ${job.success_rate >= 80 ? 'text-green-600' : job.success_rate >= 60 ? 'text-yellow-600' : 'text-red-600'}`}>
                        {job.success_rate.toFixed(1)}%
                      </span>
                    </td>
                    <td className="py-3 px-4 text-sm text-gray-600 dark:text-gray-300">
                      {job.last_run_duration_minutes ? `${job.last_run_duration_minutes.toFixed(1)}m` : 'N/A'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      {/* Data Quality Issues */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow border">
        <div className="p-6 border-b">
          <h2 className="text-xl font-semibold text-gray-900 dark:text-white flex items-center">
            <AlertTriangle className="w-5 h-5 mr-2" />
            Data Quality Issues
          </h2>
        </div>
        <div className="p-6">
          {dataQualityIssues.length === 0 ? (
            <div className="text-center py-8 text-gray-500">
              <CheckCircle className="w-12 h-12 mx-auto mb-4 text-green-500" />
              <p>No data quality issues found</p>
            </div>
          ) : (
            <div className="space-y-4">
              {dataQualityIssues.slice(0, 5).map((issue) => (
                <div key={issue.issue_id} className="flex items-start justify-between p-4 border rounded-lg">
                  <div className="flex-1">
                    <div className="flex items-center gap-2 mb-2">
                      <span className={`inline-flex px-2 py-1 text-xs rounded-full ${getSeverityColor(issue.severity)}`}>
                        {issue.severity}
                      </span>
                      <span className={`inline-flex px-2 py-1 text-xs rounded-full ${getStatusColor(issue.status)}`}>
                        {issue.status}
                      </span>
                    </div>
                    <p className="font-medium text-gray-900 dark:text-white">{issue.issue_description}</p>
                    <p className="text-sm text-gray-600 dark:text-gray-300">
                      {issue.schema_name}.{issue.table_name} • {issue.affected_rows} rows affected
                    </p>
                  </div>
                  <div className="text-right text-sm text-gray-500">
                    {new Date(issue.detected_at).toLocaleDateString()}
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Database Health & Alerts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Database Health */}
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow border">
          <div className="p-6 border-b">
            <h2 className="text-xl font-semibold text-gray-900 dark:text-white flex items-center">
              <Database className="w-5 h-5 mr-2" />
              Database Health
            </h2>
          </div>
          <div className="p-6 space-y-4">
            <div className="grid grid-cols-2 gap-4">
              <div>
                <p className="text-sm text-gray-500 dark:text-gray-400">Active Connections</p>
                <p className="text-xl font-bold text-gray-900 dark:text-white">
                  {databaseHealth?.active_connections || 0}
                </p>
              </div>
              <div>
                <p className="text-sm text-gray-500 dark:text-gray-400">Connection Usage</p>
                <p className="text-xl font-bold text-gray-900 dark:text-white">
                  {databaseHealth?.connection_usage_pct?.toFixed(1) || 0}%
                </p>
              </div>
              <div>
                <p className="text-sm text-gray-500 dark:text-gray-400">Avg Query Time</p>
                <p className="text-xl font-bold text-gray-900 dark:text-white">
                  {databaseHealth?.avg_query_time_ms?.toFixed(1) || 0}ms
                </p>
              </div>
              <div>
                <p className="text-sm text-gray-500 dark:text-gray-400">Slow Queries</p>
                <p className="text-xl font-bold text-gray-900 dark:text-white">
                  {databaseHealth?.slow_queries_count || 0}
                </p>
              </div>
            </div>
          </div>
        </div>

        {/* Recent Alerts */}
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow border">
          <div className="p-6 border-b">
            <h2 className="text-xl font-semibold text-gray-900 dark:text-white flex items-center">
              <AlertTriangle className="w-5 h-5 mr-2" />
              Recent Alerts (24h)
            </h2>
          </div>
          <div className="p-6">
            {alerts.length === 0 ? (
              <div className="text-center py-8 text-gray-500">
                <CheckCircle className="w-8 h-8 mx-auto mb-2 text-green-500" />
                <p>No alerts in the last 24 hours</p>
              </div>
            ) : (
              <div className="space-y-3">
                {alerts.slice(0, 5).map((alert, index) => (
                  <div key={index} className="flex items-center justify-between p-3 border rounded">
                    <div className="flex-1">
                      <p className="font-medium text-gray-900 dark:text-white">{alert.alert_name}</p>
                      <p className="text-sm text-gray-600 dark:text-gray-300">{alert.target_name}</p>
                    </div>
                    <div className="text-right">
                      <span className={`inline-flex px-2 py-1 text-xs rounded-full ${getSeverityColor(alert.severity)}`}>
                        {alert.severity}
                      </span>
                      <p className="text-xs text-gray-500 mt-1">
                        {alert.triggered_count_24h} triggers
                      </p>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Table Health Summary */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow border">
        <div className="p-6 border-b">
          <h2 className="text-xl font-semibold text-gray-900 dark:text-white flex items-center">
            <BarChart3 className="w-5 h-5 mr-2" />
            Table Health Summary
          </h2>
        </div>
        <div className="p-6">
          <div className="overflow-x-auto">
            <table className="min-w-full">
              <thead>
                <tr className="border-b">
                  <th className="text-left py-2 px-4 font-medium text-gray-700 dark:text-gray-300">Table</th>
                  <th className="text-left py-2 px-4 font-medium text-gray-700 dark:text-gray-300">Rows</th>
                  <th className="text-left py-2 px-4 font-medium text-gray-700 dark:text-gray-300">Size</th>
                  <th className="text-left py-2 px-4 font-medium text-gray-700 dark:text-gray-300">Freshness</th>
                  <th className="text-left py-2 px-4 font-medium text-gray-700 dark:text-gray-300">Status</th>
                </tr>
              </thead>
              <tbody>
                {tableHealth.slice(0, 10).map((table, index) => (
                  <tr key={index} className="border-b hover:bg-gray-50 dark:hover:bg-gray-700">
                    <td className="py-3 px-4">
                      <div>
                        <p className="font-medium text-gray-900 dark:text-white">{table.table_name}</p>
                        <p className="text-sm text-gray-500">{table.schema_name}</p>
                      </div>
                    </td>
                    <td className="py-3 px-4 text-gray-600 dark:text-gray-300">
                      {table.row_count.toLocaleString()}
                    </td>
                    <td className="py-3 px-4 text-gray-600 dark:text-gray-300">
                      {table.size_mb.toFixed(1)} MB
                    </td>
                    <td className="py-3 px-4 text-gray-600 dark:text-gray-300">
                      {table.freshness_hours}h ago
                    </td>
                    <td className="py-3 px-4">
                      <span className={`inline-flex px-2 py-1 text-xs rounded-full ${getStatusColor(table.health_status)}`}>
                        {table.health_status}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
};

export default DataEngineerDashboard;
