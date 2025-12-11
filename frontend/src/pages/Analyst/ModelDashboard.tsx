import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { TrendingUp, Users, MessageSquare, Play, Eye, BarChart3, Clock, Activity } from 'lucide-react';

interface Model {
  id: string;
  name: string;
  description: string;
  icon: React.ReactNode;
  accuracy: number;
  version: string;
  status: 'active' | 'training' | 'inactive';
  lastUsed: string | null;
  usageCount: number;
}

interface ModelStats {
  totalRuns: number;
  avgConfidence: number;
  lastRunDate: string | null;
}

const ModelDashboard: React.FC = () => {
  const navigate = useNavigate();
  const [modelStats, setModelStats] = useState<Record<string, ModelStats>>({});

  // Model definitions - these are the DSS models available in the system
  const models: Model[] = [
    {
      id: 'price_prediction',
      name: 'Price Prediction',
      description: 'Predict optimal pricing for products based on market data and trends',
      icon: <TrendingUp className="w-8 h-8" />,
      accuracy: 92.5, // ML model accuracy from training
      version: 'v2.1',
      status: 'active',
      lastUsed: modelStats['price_prediction']?.lastRunDate || null,
      usageCount: modelStats['price_prediction']?.totalRuns || 0
    },
    {
      id: 'product_recommendation',
      name: 'Product Recommendation',
      description: 'Recommend products to customers based on their preferences and behavior',
      icon: <Users className="w-8 h-8" />,
      accuracy: 88.7,
      version: 'v1.5',
      status: 'active',
      lastUsed: modelStats['product_recommendation']?.lastRunDate || null,
      usageCount: modelStats['product_recommendation']?.totalRuns || 0
    },
    {
      id: 'review_sentiment',
      name: 'Review Sentiment Analysis',
      description: 'Analyze customer reviews to understand sentiment and feedback',
      icon: <MessageSquare className="w-8 h-8" />,
      accuracy: 94.2,
      version: 'v3.0',
      status: 'active',
      lastUsed: modelStats['review_sentiment']?.lastRunDate || null,
      usageCount: modelStats['review_sentiment']?.totalRuns || 0
    }
  ];

  // Load usage stats from localStorage (can be extended to use API)
  useEffect(() => {
    const loadStats = () => {
      const stats: Record<string, ModelStats> = {};

      models.forEach(model => {
        const storedStats = localStorage.getItem(`dss_stats_${model.id}`);
        if (storedStats) {
          try {
            stats[model.id] = JSON.parse(storedStats);
          } catch {
            stats[model.id] = { totalRuns: 0, avgConfidence: 0, lastRunDate: null };
          }
        } else {
          stats[model.id] = { totalRuns: 0, avgConfidence: 0, lastRunDate: null };
        }
      });

      setModelStats(stats);
    };

    loadStats();
  }, []);

  // Calculate dynamic stats
  const totalModels = models.length;
  const activeModels = models.filter(m => m.status === 'active').length;
  const avgAccuracy = models.reduce((sum, m) => sum + m.accuracy, 0) / totalModels;
  const totalRuns = Object.values(modelStats).reduce((sum, s) => sum + s.totalRuns, 0);

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'active': return 'bg-green-100 text-green-800';
      case 'training': return 'bg-yellow-100 text-yellow-800';
      case 'inactive': return 'bg-gray-100 text-gray-800';
      default: return 'bg-gray-100 text-gray-800';
    }
  };

  const formatLastUsed = (dateStr: string | null) => {
    if (!dateStr) return 'Never';

    const date = new Date(dateStr);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffHours = Math.floor(diffMs / (1000 * 60 * 60));
    const diffDays = Math.floor(diffHours / 24);

    if (diffHours < 1) return 'Just now';
    if (diffHours < 24) return `${diffHours}h ago`;
    if (diffDays < 7) return `${diffDays}d ago`;
    return date.toLocaleDateString();
  };

  const handleOpenDSS = (modelId: string) => {
    navigate(`/analyst/dss/${modelId}`);
  };

  const handleViewDetails = (modelId: string) => {
    // Navigate to a details page or show modal
    navigate(`/analyst/dss/${modelId}`);
  };

  return (
    <div className="p-6">
      <div className="mb-8">
        <h1 className="text-3xl font-bold text-gray-900 dark:text-white mb-2">
          Model Dashboard
        </h1>
        <p className="text-gray-600 dark:text-gray-300">
          Manage and access Decision Support System models for business intelligence
        </p>
      </div>

      {/* Quick Stats */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-8">
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-4 border border-gray-200 dark:border-gray-700">
          <div className="flex items-center">
            <BarChart3 className="w-8 h-8 text-blue-600 mr-3" />
            <div>
              <p className="text-sm text-gray-500 dark:text-gray-400">Total Models</p>
              <p className="text-2xl font-bold text-gray-900 dark:text-white">{totalModels}</p>
            </div>
          </div>
        </div>
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-4 border border-gray-200 dark:border-gray-700">
          <div className="flex items-center">
            <Activity className="w-8 h-8 text-green-600 mr-3" />
            <div>
              <p className="text-sm text-gray-500 dark:text-gray-400">Active Models</p>
              <p className="text-2xl font-bold text-gray-900 dark:text-white">{activeModels}</p>
            </div>
          </div>
        </div>
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-4 border border-gray-200 dark:border-gray-700">
          <div className="flex items-center">
            <TrendingUp className="w-8 h-8 text-purple-600 mr-3" />
            <div>
              <p className="text-sm text-gray-500 dark:text-gray-400">Avg Accuracy</p>
              <p className="text-2xl font-bold text-gray-900 dark:text-white">{avgAccuracy.toFixed(1)}%</p>
            </div>
          </div>
        </div>
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-4 border border-gray-200 dark:border-gray-700">
          <div className="flex items-center">
            <Play className="w-8 h-8 text-orange-600 mr-3" />
            <div>
              <p className="text-sm text-gray-500 dark:text-gray-400">Total Runs</p>
              <p className="text-2xl font-bold text-gray-900 dark:text-white">{totalRuns}</p>
            </div>
          </div>
        </div>
      </div>

      {/* Model Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {models.map((model) => (
          <div key={model.id} className="bg-white dark:bg-gray-800 rounded-lg shadow-lg p-6 border border-gray-200 dark:border-gray-700 hover:shadow-xl transition-shadow duration-200">
            <div className="flex items-center mb-4">
              <div className="p-3 bg-blue-100 dark:bg-blue-900/20 rounded-lg mr-4">
                {model.icon}
              </div>
              <div className="flex-1">
                <h3 className="text-xl font-semibold text-gray-900 dark:text-white">
                  {model.name}
                </h3>
                <div className="flex items-center gap-2 mt-1">
                  <span className={`inline-block px-2 py-0.5 text-xs rounded-full ${getStatusColor(model.status)}`}>
                    {model.status}
                  </span>
                  <span className="text-xs text-gray-500">{model.version}</span>
                </div>
              </div>
            </div>

            <p className="text-gray-600 dark:text-gray-300 mb-4 text-sm">
              {model.description}
            </p>

            <div className="space-y-2 mb-6">
              <div className="flex justify-between items-center">
                <span className="text-sm text-gray-500 dark:text-gray-400">Accuracy:</span>
                <div className="flex items-center">
                  <div className="w-24 h-2 bg-gray-200 rounded-full mr-2">
                    <div
                      className="h-full bg-green-500 rounded-full"
                      style={{ width: `${model.accuracy}%` }}
                    />
                  </div>
                  <span className="text-sm font-medium text-gray-900 dark:text-white">
                    {model.accuracy}%
                  </span>
                </div>
              </div>
              <div className="flex justify-between">
                <span className="text-sm text-gray-500 dark:text-gray-400">Usage:</span>
                <span className="text-sm font-medium text-gray-900 dark:text-white">
                  {model.usageCount} runs
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-sm text-gray-500 dark:text-gray-400">Last Used:</span>
                <span className="text-sm font-medium text-gray-900 dark:text-white flex items-center">
                  <Clock className="w-3 h-3 mr-1 text-gray-400" />
                  {formatLastUsed(model.lastUsed)}
                </span>
              </div>
            </div>

            <div className="flex gap-2">
              <button
                onClick={() => handleOpenDSS(model.id)}
                className="flex-1 bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg transition-colors duration-200 flex items-center justify-center gap-2"
              >
                <Play className="w-4 h-4" />
                Run DSS
              </button>
              <button
                onClick={() => handleViewDetails(model.id)}
                className="px-4 py-2 border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-300 rounded-lg hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors duration-200 flex items-center gap-2"
              >
                <Eye className="w-4 h-4" />
              </button>
            </div>
          </div>
        ))}
      </div>

      {/* Info Note */}
      <div className="mt-8 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg p-4">
        <p className="text-sm text-blue-800 dark:text-blue-200">
          <strong>Note:</strong> Model accuracy values are based on validation metrics from training.
          Usage statistics are tracked locally. For detailed model performance analytics,
          visit the Analytics Dashboard.
        </p>
      </div>
    </div>
  );
};

export default ModelDashboard;