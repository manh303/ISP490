import React, { useState, useEffect } from 'react';
import { AlertTriangle, CheckCircle, XCircle, BarChart3, RefreshCw, TrendingUp } from 'lucide-react';
import { getDataQualitySummary } from '../../services/dataEngineerApi';
import { PieChart, Pie, Cell, ResponsiveContainer, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip } from 'recharts';

interface DataQualitySummaryItem {
  status: string;
  severity: string;
  issue_count: number;
  total_affected_rows: number;
}

type DataQualitySummary = DataQualitySummaryItem[];

const DataQualitySummaryPage: React.FC = () => {
  const [summary, setSummary] = useState<DataQualitySummary | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchSummary = async () => {
    try {
      setLoading(true);
      setError(null);
      const data = await getDataQualitySummary();
      setSummary(data);
    } catch (err) {
      console.error('Error fetching data quality summary:', err);
      setError('Failed to load data quality summary');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchSummary();
  }, []);

  // Calculate stats from the array
  const getStats = () => {
    if (!summary) return null;

    const totalIssues = summary.reduce((sum, item) => sum + item.issue_count, 0);
    const openIssues = summary.filter(item => item.status === 'OPEN').reduce((sum, item) => sum + item.issue_count, 0);
    const resolvedIssues = summary.filter(item => item.status === 'RESOLVED').reduce((sum, item) => sum + item.issue_count, 0);
    const criticalIssues = summary.filter(item => item.severity === 'CRITICAL').reduce((sum, item) => sum + item.issue_count, 0);
    const highIssues = summary.filter(item => item.severity === 'HIGH').reduce((sum, item) => sum + item.issue_count, 0);
    const mediumIssues = summary.filter(item => item.severity === 'MEDIUM').reduce((sum, item) => sum + item.issue_count, 0);
    const lowIssues = summary.filter(item => item.severity === 'LOW').reduce((sum, item) => sum + item.issue_count, 0);

    return {
      totalIssues,
      openIssues,
      resolvedIssues,
      criticalIssues,
      highIssues,
      mediumIssues,
      lowIssues
    };
  };

  const stats = getStats();

  const getSeverityColor = (severity: string) => {
    switch (severity.toLowerCase()) {
      case 'critical': return '#EF4444';
      case 'high': return '#F59E0B';
      case 'medium': return '#3B82F6';
      case 'low': return '#10B981';
      default: return '#6B7280';
    }
  };

  const severityData = stats ? [
    { name: 'Critical', value: stats.criticalIssues, color: '#EF4444' },
    { name: 'High', value: stats.highIssues, color: '#F59E0B' },
    { name: 'Medium', value: stats.mediumIssues, color: '#3B82F6' },
    { name: 'Low', value: stats.lowIssues, color: '#10B981' }
  ].filter(item => item.value > 0) : [];

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
            Data Quality Summary
          </h1>
          <p className="text-gray-600 dark:text-gray-300 mt-1">
            Overview of data quality issues across all schemas and tables
          </p>
        </div>
        <button
          onClick={fetchSummary}
          className="flex items-center gap-2 bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg transition-colors"
        >
          <RefreshCw className="w-4 h-4" />
          Refresh
        </button>
      </div>

      {/* Summary Cards */}
      {stats && (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-gray-500 dark:text-gray-400">Total Issues</p>
                <p className="text-2xl font-bold text-gray-900 dark:text-white">
                  {stats.totalIssues}
                </p>
              </div>
              <AlertTriangle className="w-8 h-8 text-orange-600" />
            </div>
          </div>

          <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-gray-500 dark:text-gray-400">Open Issues</p>
                <p className="text-2xl font-bold text-red-600">
                  {stats.openIssues}
                </p>
              </div>
              <XCircle className="w-8 h-8 text-red-600" />
            </div>
          </div>

          <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-gray-500 dark:text-gray-400">Resolved Issues</p>
                <p className="text-2xl font-bold text-green-600">
                  {stats.resolvedIssues}
                </p>
              </div>
              <CheckCircle className="w-8 h-8 text-green-600" />
            </div>
          </div>

          <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-gray-500 dark:text-gray-400">Critical Issues</p>
                <p className="text-2xl font-bold text-red-600">
                  {stats.criticalIssues}
                </p>
              </div>
              <AlertTriangle className="w-8 h-8 text-red-600" />
            </div>
          </div>
        </div>
      )}

      {/* Charts */}
      {stats && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Severity Distribution */}
          <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
            <div className="flex items-center mb-4">
              <BarChart3 className="w-5 h-5 mr-2 text-blue-600" />
              <h2 className="text-xl font-semibold text-gray-900 dark:text-white">
                Issues by Severity
              </h2>
            </div>
            {severityData.length > 0 ? (
              <ResponsiveContainer width="100%" height={300}>
                <PieChart>
                  <Pie
                    data={severityData}
                    cx="50%"
                    cy="50%"
                    labelLine={false}
                    label={({ name, percent }) => `${name}: ${percent ? (percent * 100).toFixed(0) : 0}%`}
                    outerRadius={80}
                    fill="#8884d8"
                    dataKey="value"
                  >
                    {severityData.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={entry.color} />
                    ))}
                  </Pie>
                  <Tooltip />
                </PieChart>
              </ResponsiveContainer>
            ) : (
              <div className="flex items-center justify-center h-64 text-gray-500">
                No severity data available
              </div>
            )}
          </div>

          {/* Status Distribution */}
          <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
            <div className="flex items-center mb-4">
              <TrendingUp className="w-5 h-5 mr-2 text-green-600" />
              <h2 className="text-xl font-semibold text-gray-900 dark:text-white">
                Issues by Status
              </h2>
            </div>
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={[
                { name: 'Open', value: stats.openIssues, color: '#EF4444' },
                { name: 'Resolved', value: stats.resolvedIssues, color: '#10B981' }
              ]}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="name" tick={{ fontSize: 12 }} />
                <YAxis tick={{ fontSize: 12 }} />
                <Tooltip />
                <Bar dataKey="value" fill="#3B82F6" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {/* Issues by Type Table */}
      {summary && summary.length > 0 && (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow border">
          <div className="p-6 border-b">
            <h2 className="text-xl font-semibold text-gray-900 dark:text-white flex items-center">
              <AlertTriangle className="w-5 h-5 mr-2" />
              Issues Summary
            </h2>
          </div>
          <div className="p-6">
            <div className="overflow-x-auto">
              <table className="min-w-full">
                <thead>
                  <tr className="border-b">
                    <th className="text-left py-2 px-4 font-medium text-gray-700 dark:text-gray-300">Status</th>
                    <th className="text-left py-2 px-4 font-medium text-gray-700 dark:text-gray-300">Severity</th>
                    <th className="text-left py-2 px-4 font-medium text-gray-700 dark:text-gray-300">Issue Count</th>
                    <th className="text-left py-2 px-4 font-medium text-gray-700 dark:text-gray-300">Affected Rows</th>
                    <th className="text-left py-2 px-4 font-medium text-gray-700 dark:text-gray-300">Percentage</th>
                  </tr>
                </thead>
                <tbody>
                  {summary.map((item, index) => (
                    <tr key={index} className="border-b hover:bg-gray-50 dark:hover:bg-gray-700">
                      <td className="py-3 px-4">
                        <span className={`inline-flex px-2 py-1 text-xs rounded-full ${
                          item.status === 'OPEN' ? 'bg-red-100 text-red-800' : 'bg-green-100 text-green-800'
                        }`}>
                          {item.status}
                        </span>
                      </td>
                      <td className="py-3 px-4">
                        <span className={`inline-flex px-2 py-1 text-xs rounded-full ${
                          item.severity === 'CRITICAL' ? 'bg-red-100 text-red-800' :
                          item.severity === 'HIGH' ? 'bg-orange-100 text-orange-800' :
                          item.severity === 'MEDIUM' ? 'bg-yellow-100 text-yellow-800' :
                          'bg-blue-100 text-blue-800'
                        }`}>
                          {item.severity}
                        </span>
                      </td>
                      <td className="py-3 px-4 text-gray-900 dark:text-white font-medium">
                        {item.issue_count}
                      </td>
                      <td className="py-3 px-4 text-gray-600 dark:text-gray-300">
                        {item.total_affected_rows.toLocaleString()}
                      </td>
                      <td className="py-3 px-4">
                        <div className="flex items-center">
                          <div className="w-full bg-gray-200 rounded-full h-2 mr-2">
                            <div
                              className="bg-blue-600 h-2 rounded-full"
                              style={{ width: `${stats && stats.totalIssues > 0 ? (item.issue_count / stats.totalIssues) * 100 : 0}%` }}
                            ></div>
                          </div>
                          <span className="text-sm text-gray-600 dark:text-gray-300">
                            {stats && stats.totalIssues > 0 ? ((item.issue_count / stats.totalIssues) * 100).toFixed(1) : 0}%
                          </span>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}


    </div>
  );
};

export default DataQualitySummaryPage;