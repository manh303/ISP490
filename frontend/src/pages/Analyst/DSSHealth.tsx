import React, { useState, useEffect } from 'react';
import { Activity, Database, Brain, CheckCircle, XCircle, AlertTriangle, RefreshCw } from 'lucide-react';
import { getDSSHealth, getDataStatus, DSSHealthResponse, DataStatusResponse } from '../../services/DSSApi';

const DSSHealth: React.FC = () => {
  const [healthData, setHealthData] = useState<DSSHealthResponse | null>(null);
  const [dataStatus, setDataStatus] = useState<DataStatusResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);

  useEffect(() => {
    fetchHealthData();
  }, []);

  const fetchHealthData = async () => {
    try {
      setRefreshing(true);
      const [healthResponse, dataResponse] = await Promise.all([
        getDSSHealth(),
        getDataStatus()
      ]);
      setHealthData(healthResponse);
      setDataStatus(dataResponse);
    } catch (error) {
      console.error('Error fetching DSS health data:', error);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  const getStatusColor = (status: string) => {
    switch (status?.toLowerCase()) {
      case 'healthy':
      case 'ok':
        return 'text-green-600 bg-green-100';
      case 'warning':
      case 'degraded':
        return 'text-yellow-600 bg-yellow-100';
      case 'error':
      case 'unhealthy':
        return 'text-red-600 bg-red-100';
      default:
        return 'text-gray-600 bg-gray-100';
    }
  };

  const getStatusIcon = (status: string) => {
    switch (status?.toLowerCase()) {
      case 'healthy':
      case 'ok':
        return <CheckCircle className="w-5 h-5 text-green-600" />;
      case 'warning':
      case 'degraded':
        return <AlertTriangle className="w-5 h-5 text-yellow-600" />;
      case 'error':
      case 'unhealthy':
        return <XCircle className="w-5 h-5 text-red-600" />;
      default:
        return <Activity className="w-5 h-5 text-gray-600" />;
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
      </div>
    );
  }

  return (
    <div className="p-6">
      <div className="mb-8">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold text-gray-900 dark:text-white mb-2">
              DSS System Health
            </h1>
            <p className="text-gray-600 dark:text-gray-300">
              Monitor the health and status of Decision Support System components
            </p>
          </div>
          <button
            onClick={fetchHealthData}
            disabled={refreshing}
            className="flex items-center gap-2 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400 text-white px-4 py-2 rounded-lg transition-colors"
          >
            <RefreshCw className={`w-4 h-4 ${refreshing ? 'animate-spin' : ''}`} />
            Refresh
          </button>
        </div>
      </div>

      {/* Overall System Status */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
        <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-gray-500 dark:text-gray-400">System Status</p>
              <p className="text-2xl font-bold text-gray-900 dark:text-white">
                {healthData?.status || 'Unknown'}
              </p>
            </div>
            {getStatusIcon(healthData?.status || 'unknown')}
          </div>
        </div>

        <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-gray-500 dark:text-gray-400">Database</p>
              <p className="text-2xl font-bold text-gray-900 dark:text-white">
                {healthData?.components?.database || 'Unknown'}
              </p>
            </div>
            {getStatusIcon(healthData?.components?.database || 'unknown')}
          </div>
        </div>

        <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-gray-500 dark:text-gray-400">AI Service</p>
              <p className="text-2xl font-bold text-gray-900 dark:text-white">
                {healthData?.components?.ai?.status || 'Unknown'}
              </p>
            </div>
            {getStatusIcon(healthData?.components?.ai?.status || 'unknown')}
          </div>
        </div>

        <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-gray-500 dark:text-gray-400">ML Tables</p>
              <p className="text-2xl font-bold text-gray-900 dark:text-white">
                {healthData?.components?.ml_tables?.count || 0}
              </p>
            </div>
            {getStatusIcon(healthData?.components?.ml_tables?.status || 'unknown')}
          </div>
        </div>
      </div>

      {/* Component Details */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 mb-8">
        {/* DSS Components */}
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow border">
          <div className="p-6 border-b">
            <h2 className="text-xl font-semibold text-gray-900 dark:text-white flex items-center">
              <Activity className="w-5 h-5 mr-2" />
              DSS Components
            </h2>
          </div>
          <div className="p-6 space-y-4">
            <div className="flex items-center justify-between p-4 bg-gray-50 dark:bg-gray-700 rounded-lg">
              <div className="flex items-center gap-3">
                <Database className="w-5 h-5 text-blue-600" />
                <div>
                  <h3 className="font-medium text-gray-900 dark:text-white">Database</h3>
                  <p className="text-sm text-gray-600 dark:text-gray-300">Data storage and retrieval</p>
                </div>
              </div>
              <span className={`px-2 py-1 text-xs rounded-full ${getStatusColor(healthData?.components?.database || 'unknown')}`}>
                {healthData?.components?.database || 'Unknown'}
              </span>
            </div>

            <div className="flex items-center justify-between p-4 bg-gray-50 dark:bg-gray-700 rounded-lg">
              <div className="flex items-center gap-3">
                <Brain className="w-5 h-5 text-purple-600" />
                <div>
                  <h3 className="font-medium text-gray-900 dark:text-white">AI Service</h3>
                  <p className="text-sm text-gray-600 dark:text-gray-300">
                    Model: {healthData?.components?.ai?.model || 'N/A'}
                  </p>
                </div>
              </div>
              <span className={`px-2 py-1 text-xs rounded-full ${getStatusColor(healthData?.components?.ai?.status || 'unknown')}`}>
                {healthData?.components?.ai?.status || 'Unknown'}
              </span>
            </div>

            <div className="flex items-center justify-between p-4 bg-gray-50 dark:bg-gray-700 rounded-lg">
              <div className="flex items-center gap-3">
                <Activity className="w-5 h-5 text-green-600" />
                <div>
                  <h3 className="font-medium text-gray-900 dark:text-white">ML Tables</h3>
                  <p className="text-sm text-gray-600 dark:text-gray-300">
                    {healthData?.components?.ml_tables?.count || 0} tables available
                  </p>
                </div>
              </div>
              <span className={`px-2 py-1 text-xs rounded-full ${getStatusColor(healthData?.components?.ml_tables?.status || 'unknown')}`}>
                {healthData?.components?.ml_tables?.status || 'Unknown'}
              </span>
            </div>
          </div>
        </div>

        {/* Data Status */}
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow border">
          <div className="p-6 border-b">
            <h2 className="text-xl font-semibold text-gray-900 dark:text-white flex items-center">
              <Database className="w-5 h-5 mr-2" />
              Data Pipeline Status
            </h2>
          </div>
          <div className="p-6 space-y-4">
            <div className="grid grid-cols-2 gap-4">
              <div className="text-center p-4 bg-blue-50 dark:bg-blue-900/20 rounded-lg">
                <p className="text-sm text-gray-500 dark:text-gray-400">Latest Fact Data</p>
                <p className="text-lg font-semibold text-blue-600">
                  {dataStatus?.latest_fact_date ? new Date(dataStatus.latest_fact_date).toLocaleDateString() : 'N/A'}
                </p>
                <p className="text-xs text-gray-600 dark:text-gray-300">
                  {dataStatus?.days_since_last_fact || 0} days ago
                </p>
              </div>
              <div className="text-center p-4 bg-green-50 dark:bg-green-900/20 rounded-lg">
                <p className="text-sm text-gray-500 dark:text-gray-400">Latest ML Data</p>
                <p className="text-lg font-semibold text-green-600">
                  {dataStatus?.latest_ml_date ? new Date(dataStatus.latest_ml_date).toLocaleDateString() : 'N/A'}
                </p>
                <p className="text-xs text-gray-600 dark:text-gray-300">
                  {dataStatus?.days_since_last_ml || 0} days ago
                </p>
              </div>
            </div>

            {dataStatus?.warnings && dataStatus.warnings.length > 0 && (
              <div className="mt-4">
                <h4 className="font-medium text-orange-900 dark:text-orange-100 mb-2 flex items-center">
                  <AlertTriangle className="w-4 h-4 mr-1" />
                  Warnings
                </h4>
                <ul className="list-disc list-inside space-y-1 text-orange-800 dark:text-orange-200 text-sm">
                  {dataStatus.warnings.map((warning, index) => (
                    <li key={index}>{warning}</li>
                  ))}
                </ul>
              </div>
            )}

            {dataStatus?.recommendations && dataStatus.recommendations.length > 0 && (
              <div className="mt-4">
                <h4 className="font-medium text-blue-900 dark:text-blue-100 mb-2">Recommendations</h4>
                <ul className="list-disc list-inside space-y-1 text-blue-800 dark:text-blue-200 text-sm">
                  {dataStatus.recommendations.map((rec, index) => (
                    <li key={index}>{rec}</li>
                  ))}
                </ul>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* System Information */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow border">
        <div className="p-6 border-b">
          <h2 className="text-xl font-semibold text-gray-900 dark:text-white">
            System Information
          </h2>
        </div>
        <div className="p-6">
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            <div>
              <h3 className="font-medium text-gray-900 dark:text-white mb-2">API Endpoints</h3>
              <ul className="text-sm text-gray-600 dark:text-gray-300 space-y-1">
                <li>• Price Prediction: /api/v1/dss/price/run</li>
                <li>• Product Recommendation: /api/v1/dss/reco/run</li>
                <li>• Review Sentiment: /api/v1/dss/review/run</li>
                <li>• AI Summary: /api/v1/ai/summarize</li>
              </ul>
            </div>
            <div>
              <h3 className="font-medium text-gray-900 dark:text-white mb-2">Supported Models</h3>
              <ul className="text-sm text-gray-600 dark:text-gray-300 space-y-1">
                <li>• Price Prediction Model</li>
                <li>• Product Recommendation Engine</li>
                <li>• Review Sentiment Analyzer</li>
              </ul>
            </div>
            <div>
              <h3 className="font-medium text-gray-900 dark:text-white mb-2">Data Sources</h3>
              <ul className="text-sm text-gray-600 dark:text-gray-300 space-y-1">
                <li>• Tiki E-commerce Platform</li>
                <li>• Lazada E-commerce Platform</li>
                <li>• Shopee E-commerce Platform</li>
              </ul>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default DSSHealth;