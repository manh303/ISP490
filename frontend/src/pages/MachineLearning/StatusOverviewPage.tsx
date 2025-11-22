import React, { useState, useEffect } from 'react';
import { getStatusSummary, StatusSummary } from '../../services/machineLearningApi';

const StatusOverviewPage: React.FC = () => {
  const [status, setStatus] = useState<StatusSummary | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchStatus();
  }, []);

  const fetchStatus = async () => {
    try {
      setLoading(true);
      const data = await getStatusSummary();
      setStatus(data);
    } catch (error) {
      console.error('Error fetching status summary:', error);
    } finally {
      setLoading(false);
    }
  };

  const StatCard: React.FC<{ title: string; value: number; icon: string; color: string }> = ({ title, value, icon, color }) => (
    <div className={`bg-white rounded-lg shadow p-6 border-l-4 ${color}`}>
      <div className="flex items-center justify-between">
        <div>
          <p className="text-sm font-medium text-gray-600">{title}</p>
          <p className="text-3xl font-bold text-gray-900">{value.toLocaleString()}</p>
        </div>
        <div className="text-4xl">{icon}</div>
      </div>
    </div>
  );

  if (loading) {
    return (
      <div className="p-6">
        <h1 className="text-2xl font-bold mb-6">ML System Status Overview</h1>
        <div className="flex justify-center items-center h-64">
          <div className="text-lg">Loading...</div>
        </div>
      </div>
    );
  }

  if (!status) {
    return (
      <div className="p-6">
        <h1 className="text-2xl font-bold mb-6">ML System Status Overview</h1>
        <div className="flex justify-center items-center h-64">
          <div className="text-lg text-red-600">Failed to load status</div>
        </div>
      </div>
    );
  }

  return (
    <div className="p-6">
      <div className="flex justify-between items-center mb-6">
        <h1 className="text-2xl font-bold">ML System Status Overview</h1>
        <button
          onClick={fetchStatus}
          className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 transition-colors"
        >
          Refresh
        </button>
      </div>

      {/* Stats Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-8">
        <StatCard
          title="Total Models"
          value={status.models_total}
          icon="🤖"
          color="border-blue-500"
        />
        <StatCard
          title="Active Models"
          value={status.models_active}
          icon="✅"
          color="border-green-500"
        />
        <StatCard
          title="Deprecated Models"
          value={status.models_deprecated}
          icon="⚠️"
          color="border-yellow-500"
        />
        <StatCard
          title="Training Models"
          value={status.models_training}
          icon="🔄"
          color="border-purple-500"
        />
        <StatCard
          title="Predictions (7 days)"
          value={status.predictions_last_7_days}
          icon="📈"
          color="border-indigo-500"
        />
        <StatCard
          title="Recommendations (7 days)"
          value={status.recommendations_last_7_days}
          icon="🎯"
          color="border-pink-500"
        />
      </div>

      {/* System Health Overview */}
      <div className="bg-white rounded-lg shadow p-6">
        <h2 className="text-xl font-semibold mb-4">System Health</h2>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          {/* Model Status Breakdown */}
          <div>
            <h3 className="text-lg font-medium mb-3">Model Status Distribution</h3>
            <div className="space-y-2">
              <div className="flex justify-between">
                <span className="text-green-600">Active</span>
                <span className="font-medium">{status.models_active}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-yellow-600">Deprecated</span>
                <span className="font-medium">{status.models_deprecated}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-purple-600">Training</span>
                <span className="font-medium">{status.models_training}</span>
              </div>
              <div className="flex justify-between border-t pt-2">
                <span className="font-medium">Total</span>
                <span className="font-bold">{status.models_total}</span>
              </div>
            </div>
          </div>

          {/* Activity Metrics */}
          <div>
            <h3 className="text-lg font-medium mb-3">Recent Activity (Last 7 days)</h3>
            <div className="space-y-2">
              <div className="flex justify-between">
                <span>Price Predictions</span>
                <span className="font-medium">{status.predictions_last_7_days.toLocaleString()}</span>
              </div>
              <div className="flex justify-between">
                <span>Product Recommendations</span>
                <span className="font-medium">{status.recommendations_last_7_days.toLocaleString()}</span>
              </div>
            </div>

            {/* Activity Rate */}
            <div className="mt-4 p-3 bg-gray-50 rounded">
              <div className="text-sm text-gray-600">Daily Average Activity</div>
              <div className="text-lg font-semibold">
                {Math.round((status.predictions_last_7_days + status.recommendations_last_7_days) / 7)} requests/day
              </div>
            </div>
          </div>
        </div>

        {/* Health Indicators */}
        <div className="mt-6 pt-6 border-t">
          <h3 className="text-lg font-medium mb-3">Health Indicators</h3>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div className={`p-3 rounded ${status.models_active > 0 ? 'bg-green-50 text-green-700' : 'bg-red-50 text-red-700'}`}>
              <div className="font-medium">Model Availability</div>
              <div className="text-sm">{status.models_active > 0 ? '✅ Active models available' : '❌ No active models'}</div>
            </div>

            <div className={`p-3 rounded ${status.predictions_last_7_days > 0 ? 'bg-green-50 text-green-700' : 'bg-yellow-50 text-yellow-700'}`}>
              <div className="font-medium">Prediction Service</div>
              <div className="text-sm">{status.predictions_last_7_days > 0 ? '✅ Active usage' : '⚠️ No recent activity'}</div>
            </div>

            <div className={`p-3 rounded ${status.recommendations_last_7_days > 0 ? 'bg-green-50 text-green-700' : 'bg-yellow-50 text-yellow-700'}`}>
              <div className="font-medium">Recommendation Service</div>
              <div className="text-sm">{status.recommendations_last_7_days > 0 ? '✅ Active usage' : '⚠️ No recent activity'}</div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default StatusOverviewPage;