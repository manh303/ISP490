import React, { useState, useEffect } from 'react';

const DSSPage = () => {
  const [activeTab, setActiveTab] = useState('dashboard');
  const [loading, setLoading] = useState(false);
  const [dssData, setDssData] = useState(null);
  const [error, setError] = useState('');
  const [startedActions, setStartedActions] = useState(new Set());
  const [actionModal, setActionModal] = useState(null);

  // Mock login token - in real app, get from auth context
  const getAuthToken = () => {
    return localStorage.getItem('auth_token') || 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhZG1pbiIsInJvbGUiOiJhZG1pbiIsImV4cCI6MTc1OTk0ODEzMH0.K7hlVKE3L_ILzz3iOeUtKFdVudHochFS64sF5TKBrLo';
  };

  const fetchDSSData = async (endpoint) => {
    setLoading(true);
    setError('');
    try {
      const response = await fetch(`http://localhost:8000/api/v1/dss/${endpoint}`, {
        headers: {
          'Authorization': `Bearer ${getAuthToken()}`,
          'Content-Type': 'application/json',
        },
      });

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const data = await response.json();
      setDssData(data);
    } catch (err) {
      setError(`Error fetching ${endpoint}: ${err.message}`);
      console.error('DSS fetch error:', err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchDSSData('dashboard');
  }, []);

  const handleTabChange = (tab) => {
    setActiveTab(tab);
    fetchDSSData(tab);
  };

  const formatCurrency = (amount) => {
    return new Intl.NumberFormat('vi-VN', {
      style: 'currency',
      currency: 'VND',
    }).format(amount);
  };

  const getPriorityBadge = (priority) => {
    const colors = {
      high: 'bg-red-100 text-red-800',
      medium: 'bg-yellow-100 text-yellow-800',
      low: 'bg-green-100 text-green-800',
    };
    return colors[priority] || 'bg-gray-100 text-gray-800';
  };

  const handleStartAction = (action) => {
    setActionModal(action);
  };

  const confirmStartAction = (actionId) => {
    setStartedActions(prev => new Set([...prev, actionId]));
    setActionModal(null);

    // Show success notification
    const notification = document.createElement('div');
    notification.className = 'fixed top-4 right-4 bg-green-500 text-white px-6 py-3 rounded-lg shadow-lg z-50';
    notification.textContent = '✅ Action started successfully!';
    document.body.appendChild(notification);

    setTimeout(() => {
      document.body.removeChild(notification);
    }, 3000);
  };

  const isActionStarted = (actionId) => {
    return startedActions.has(actionId);
  };

  const renderDashboard = () => {
    if (!dssData?.dashboard) return null;

    const { summary_metrics, recommendations, action_plans } = dssData.dashboard;

    return (
      <div className="space-y-6">
        {/* Summary Metrics */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow">
            <h3 className="text-sm font-medium text-gray-500 dark:text-gray-400">Total Customers</h3>
            <p className="text-2xl font-bold text-gray-900 dark:text-white">
              {summary_metrics?.total_customers?.toLocaleString()}
            </p>
          </div>
          <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow">
            <h3 className="text-sm font-medium text-gray-500 dark:text-gray-400">Total Orders</h3>
            <p className="text-2xl font-bold text-gray-900 dark:text-white">
              {summary_metrics?.total_orders?.toLocaleString()}
            </p>
          </div>
          <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow">
            <h3 className="text-sm font-medium text-gray-500 dark:text-gray-400">Total Revenue</h3>
            <p className="text-2xl font-bold text-gray-900 dark:text-white">
              {formatCurrency(summary_metrics?.total_revenue)}
            </p>
          </div>
          <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow">
            <h3 className="text-sm font-medium text-gray-500 dark:text-gray-400">Avg Order Value</h3>
            <p className="text-2xl font-bold text-gray-900 dark:text-white">
              {formatCurrency(summary_metrics?.avg_order_value)}
            </p>
          </div>
        </div>

        {/* Top Recommendations */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow">
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
              💡 Top Recommendations
            </h3>
            <div className="space-y-4">
              {recommendations?.slice(0, 3).map((rec, index) => (
                <div key={index} className={`p-4 rounded-lg ${rec.priority === 'high' ? 'bg-red-50 border-red-200' : 'bg-yellow-50 border-yellow-200'} border`}>
                  <div className="flex justify-between items-start mb-2">
                    <h4 className="font-medium text-gray-900">{rec.title}</h4>
                    <span className={`px-2 py-1 text-xs rounded-full ${getPriorityBadge(rec.priority)}`}>
                      {rec.priority}
                    </span>
                  </div>
                  <p className="text-sm text-gray-600">{rec.description}</p>
                </div>
              ))}
            </div>
          </div>

          <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow">
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
              🎯 Priority Actions
            </h3>
            <div className="space-y-4">
              {action_plans?.slice(0, 3).map((action, index) => (
                <div key={index} className="p-4 bg-blue-50 dark:bg-blue-900/20 rounded-lg border border-blue-200">
                  <h4 className="font-medium text-gray-900 dark:text-white">{action.title}</h4>
                  <p className="text-sm text-gray-600 dark:text-gray-300 mt-1">
                    {action.estimated_impact} • {action.timeline}
                  </p>
                  <span className={`inline-block mt-2 px-2 py-1 text-xs rounded ${action.effort_level === 'high' ? 'bg-red-100 text-red-800' : 'bg-green-100 text-green-800'}`}>
                    {action.effort_level} effort
                  </span>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    );
  };

  const renderRecommendations = () => {
    if (!dssData?.recommendations) return null;

    return (
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {dssData.recommendations.map((rec, index) => (
          <div key={index} className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow">
            <div className="flex justify-between items-start mb-4">
              <h3 className="text-lg font-semibold text-gray-900 dark:text-white">{rec.title}</h3>
              <span className={`px-3 py-1 text-sm rounded-full ${getPriorityBadge(rec.priority)}`}>
                {rec.priority}
              </span>
            </div>
            <p className="text-gray-600 dark:text-gray-300 mb-4">{rec.description}</p>

            {rec.metric_value && (
              <div className="mb-4 p-3 bg-gray-50 dark:bg-gray-700 rounded">
                <p className="text-sm">
                  Current: <span className="font-semibold">{rec.metric_value.toLocaleString()}</span>
                  {rec.target_value && (
                    <> → Target: <span className="font-semibold text-green-600">{rec.target_value.toLocaleString()}</span></>
                  )}
                </p>
              </div>
            )}

            <div>
              <h4 className="font-medium text-gray-900 dark:text-white mb-2">Action Items:</h4>
              <ul className="space-y-1">
                {rec.action_items.map((item, idx) => (
                  <li key={idx} className="text-sm text-gray-600 dark:text-gray-300">• {item}</li>
                ))}
              </ul>
            </div>
          </div>
        ))}
      </div>
    );
  };

  const renderActions = () => {
    if (!dssData?.action_plans) return null;

    return (
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {dssData.action_plans.map((action, index) => (
          <div key={index} className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow">
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-2">{action.title}</h3>
            <p className="text-gray-600 dark:text-gray-300 mb-4">{action.description}</p>

            <div className="flex gap-2 mb-4">
              <span className="px-2 py-1 text-xs bg-blue-100 text-blue-800 rounded">{action.category}</span>
              <span className={`px-2 py-1 text-xs rounded ${action.effort_level === 'high' ? 'bg-red-100 text-red-800' : 'bg-green-100 text-green-800'}`}>
                {action.effort_level} effort
              </span>
            </div>

            <div className="space-y-2 mb-4">
              <p className="text-sm"><span className="font-medium">Impact:</span> {action.estimated_impact}</p>
              <p className="text-sm"><span className="font-medium">Timeline:</span> {action.timeline}</p>
              <p className="text-sm"><span className="font-medium">KPIs:</span> {action.kpis.join(', ')}</p>
            </div>

            <button
              onClick={() => handleStartAction(action)}
              disabled={isActionStarted(action.action_id)}
              className={`w-full py-2 px-4 rounded transition-colors ${
                isActionStarted(action.action_id)
                  ? 'bg-green-600 text-white cursor-not-allowed'
                  : 'bg-blue-600 text-white hover:bg-blue-700'
              }`}
            >
              {isActionStarted(action.action_id) ? '✅ Action Started' : '🚀 Start Action'}
            </button>
          </div>
        ))}
      </div>
    );
  };

  const renderAlerts = () => {
    if (!dssData?.alerts) return null;

    const getAlertColor = (type) => {
      switch (type) {
        case 'critical': return 'bg-red-50 border-red-500 text-red-700';
        case 'warning': return 'bg-yellow-50 border-yellow-500 text-yellow-700';
        case 'opportunity': return 'bg-green-50 border-green-500 text-green-700';
        default: return 'bg-blue-50 border-blue-500 text-blue-700';
      }
    };

    return (
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {dssData.alerts.map((alert, index) => (
          <div key={index} className={`p-6 rounded-lg border-l-4 ${getAlertColor(alert.type)}`}>
            <h3 className="text-lg font-semibold mb-2">{alert.title}</h3>
            <p className="mb-2">{alert.message}</p>
            <p className="text-sm font-medium">Action Required: {alert.action_required}</p>
          </div>
        ))}
      </div>
    );
  };

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900">
      <div className="container mx-auto px-4 py-8">
        {/* Header */}
        <div className="text-center mb-8">
          <h1 className="text-4xl font-bold text-gray-900 dark:text-white mb-2">
            🎯 Decision Support System (DSS)
          </h1>
          <p className="text-xl text-gray-600 dark:text-gray-300">
            AI-Powered Business Intelligence & Strategic Insights
          </p>
        </div>

        {/* Error Alert */}
        {error && (
          <div className="mb-6 p-4 bg-red-50 border border-red-200 text-red-700 rounded-lg">
            {error}
          </div>
        )}

        {/* Tabs */}
        <div className="mb-8">
          <div className="flex flex-wrap justify-center gap-2 bg-white dark:bg-gray-800 p-2 rounded-lg shadow">
            {[
              { id: 'dashboard', label: '📊 Dashboard', endpoint: 'dashboard' },
              { id: 'recommendations', label: '💡 Recommendations', endpoint: 'recommendations' },
              { id: 'actions', label: '🎯 Actions', endpoint: 'actions' },
              { id: 'alerts', label: '⚠️ Alerts', endpoint: 'alerts' },
              { id: 'performance', label: '📈 Performance', endpoint: 'performance' },
            ].map((tab) => (
              <button
                key={tab.id}
                onClick={() => handleTabChange(tab.endpoint)}
                className={`px-4 py-2 rounded-md transition-colors ${
                  activeTab === tab.endpoint
                    ? 'bg-blue-600 text-white'
                    : 'text-gray-600 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-700'
                }`}
              >
                {tab.label}
              </button>
            ))}
          </div>
        </div>

        {/* Content */}
        {loading ? (
          <div className="flex justify-center items-center py-12">
            <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
          </div>
        ) : (
          <div>
            {activeTab === 'dashboard' && renderDashboard()}
            {activeTab === 'recommendations' && renderRecommendations()}
            {activeTab === 'actions' && renderActions()}
            {activeTab === 'alerts' && renderAlerts()}
            {activeTab === 'performance' && (
              <div className="text-center py-12">
                <p className="text-gray-600 dark:text-gray-300">Performance analytics coming soon...</p>
              </div>
            )}
          </div>
        )}
      </div>

      {/* Action Confirmation Modal */}
      {actionModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow-xl max-w-md w-full mx-4">
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
              Confirm Action Start
            </h3>
            <p className="text-gray-600 dark:text-gray-300 mb-2">
              <strong>{actionModal.title}</strong>
            </p>
            <p className="text-sm text-gray-500 dark:text-gray-400 mb-6">
              {actionModal.description}
            </p>
            <div className="flex gap-3 justify-end">
              <button
                onClick={() => setActionModal(null)}
                className="px-4 py-2 text-gray-600 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-700 rounded transition-colors"
              >
                Cancel
              </button>
              <button
                onClick={() => confirmStartAction(actionModal.action_id)}
                className="px-4 py-2 bg-blue-600 text-white hover:bg-blue-700 rounded transition-colors"
              >
                Start Action
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default DSSPage;