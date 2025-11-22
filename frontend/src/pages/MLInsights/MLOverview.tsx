import React, { useEffect, useState } from 'react';
import {
  getMLSummary,
  MLSummary as MLSummaryType,
} from '../../services/MLInsightsApi';
import MLSummaryCards from '../../components/analytics/MLSummaryCards';

const MLOverview: React.FC = () => {
  const [summary, setSummary] = useState<MLSummaryType | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchData();
  }, []);

  const fetchData = async () => {
    setLoading(true);
    setError(null);

    try {
      const summaryData = await getMLSummary();
      setSummary(summaryData);
    } catch (err: any) {
      console.error('Error fetching ML overview:', err);
      setError(err.response?.data?.message || err.message || 'Failed to load ML overview');
    } finally {
      setLoading(false);
    }
  };

  if (error) {
    return (
      <div className="min-h-screen bg-gray-50 p-6">
        <div className="max-w-7xl mx-auto">
          <div className="bg-red-50 border border-red-200 rounded-lg p-6 text-center">
            <div className="text-4xl mb-4">❌</div>
            <h2 className="text-2xl font-bold text-red-800 mb-2">Error Loading ML Overview</h2>
            <p className="text-red-600 mb-4">{error}</p>
            <button
              onClick={fetchData}
              className="px-6 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 transition-colors"
            >
              Retry
            </button>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50 p-6">
      <div className="max-w-7xl mx-auto">
        {/* Header */}
        <div className="mb-8">
          <h1 className="text-4xl font-bold text-gray-900 mb-2">🤖 ML Overview</h1>
          <p className="text-gray-600">
            Tổng quan về các mô hình AI và dự đoán của hệ thống
          </p>
          <button
            onClick={fetchData}
            disabled={loading}
            className="mt-4 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {loading ? '🔄 Refreshing...' : '🔄 Refresh Data'}
          </button>
        </div>

        {/* Summary Cards */}
        <MLSummaryCards data={summary} loading={loading} />

        {/* Loading Overlay */}
        {loading && (
          <div className="fixed inset-0 bg-black bg-opacity-20 flex items-center justify-center z-50">
            <div className="bg-white rounded-lg p-8 shadow-xl">
              <div className="flex flex-col items-center">
                <div className="animate-spin rounded-full h-16 w-16 border-b-2 border-blue-600 mb-4"></div>
                <p className="text-lg font-semibold text-gray-700">Loading ML Overview...</p>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default MLOverview;