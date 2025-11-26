import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { TrendingUp, Users, MessageSquare, Play, Eye, BarChart3 } from 'lucide-react';

interface Model {
  id: string;
  name: string;
  description: string;
  icon: React.ReactNode;
  accuracy: number;
  trainingDate: string;
  status: 'active' | 'training' | 'inactive';
  lastUpdated: string;
}

const ModelDashboard: React.FC = () => {
  const navigate = useNavigate();

  const models: Model[] = [
    {
      id: 'price_prediction',
      name: 'Price Prediction',
      description: 'Predict optimal pricing for products based on market data and trends',
      icon: <TrendingUp className="w-8 h-8" />,
      accuracy: 92.5,
      trainingDate: '2025-11-20',
      status: 'active',
      lastUpdated: '2025-11-25'
    },
    {
      id: 'product_recommendation',
      name: 'Product Recommendation',
      description: 'Recommend products to customers based on their preferences and behavior',
      icon: <Users className="w-8 h-8" />,
      accuracy: 88.7,
      trainingDate: '2025-11-18',
      status: 'active',
      lastUpdated: '2025-11-24'
    },
    {
      id: 'review_sentiment',
      name: 'Review Sentiment Analysis',
      description: 'Analyze customer reviews to understand sentiment and feedback',
      icon: <MessageSquare className="w-8 h-8" />,
      accuracy: 94.2,
      trainingDate: '2025-11-22',
      status: 'active',
      lastUpdated: '2025-11-26'
    }
  ];

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'active': return 'bg-green-100 text-green-800';
      case 'training': return 'bg-yellow-100 text-yellow-800';
      case 'inactive': return 'bg-gray-100 text-gray-800';
      default: return 'bg-gray-100 text-gray-800';
    }
  };

  const handleOpenDSS = (modelId: string) => {
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

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {models.map((model) => (
          <div key={model.id} className="bg-white dark:bg-gray-800 rounded-lg shadow-lg p-6 border border-gray-200 dark:border-gray-700">
            <div className="flex items-center mb-4">
              <div className="p-3 bg-blue-100 dark:bg-blue-900/20 rounded-lg mr-4">
                {model.icon}
              </div>
              <div>
                <h3 className="text-xl font-semibold text-gray-900 dark:text-white">
                  {model.name}
                </h3>
                <span className={`inline-block px-2 py-1 text-xs rounded-full ${getStatusColor(model.status)}`}>
                  {model.status}
                </span>
              </div>
            </div>

            <p className="text-gray-600 dark:text-gray-300 mb-4">
              {model.description}
            </p>

            <div className="space-y-2 mb-6">
              <div className="flex justify-between">
                <span className="text-sm text-gray-500 dark:text-gray-400">Accuracy:</span>
                <span className="text-sm font-medium text-gray-900 dark:text-white">
                  {model.accuracy}%
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-sm text-gray-500 dark:text-gray-400">Training Date:</span>
                <span className="text-sm font-medium text-gray-900 dark:text-white">
                  {new Date(model.trainingDate).toLocaleDateString()}
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-sm text-gray-500 dark:text-gray-400">Last Updated:</span>
                <span className="text-sm font-medium text-gray-900 dark:text-white">
                  {new Date(model.lastUpdated).toLocaleDateString()}
                </span>
              </div>
            </div>

            <div className="flex gap-2">
              <button
                onClick={() => handleOpenDSS(model.id)}
                className="flex-1 bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg transition-colors duration-200 flex items-center justify-center gap-2"
              >
                <Play className="w-4 h-4" />
                Open DSS
              </button>
              <button className="px-4 py-2 border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-300 rounded-lg hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors duration-200 flex items-center gap-2">
                <Eye className="w-4 h-4" />
                View Details
              </button>
            </div>
          </div>
        ))}
      </div>

      {/* Quick Stats */}
      <div className="mt-8 grid grid-cols-1 md:grid-cols-3 gap-6">
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6 border border-gray-200 dark:border-gray-700">
          <div className="flex items-center">
            <BarChart3 className="w-8 h-8 text-blue-600 mr-3" />
            <div>
              <p className="text-sm text-gray-500 dark:text-gray-400">Total Models</p>
              <p className="text-2xl font-bold text-gray-900 dark:text-white">3</p>
            </div>
          </div>
        </div>
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6 border border-gray-200 dark:border-gray-700">
          <div className="flex items-center">
            <TrendingUp className="w-8 h-8 text-green-600 mr-3" />
            <div>
              <p className="text-sm text-gray-500 dark:text-gray-400">Avg Accuracy</p>
              <p className="text-2xl font-bold text-gray-900 dark:text-white">91.8%</p>
            </div>
          </div>
        </div>
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6 border border-gray-200 dark:border-gray-700">
          <div className="flex items-center">
            <Play className="w-8 h-8 text-purple-600 mr-3" />
            <div>
              <p className="text-sm text-gray-500 dark:text-gray-400">Active Models</p>
              <p className="text-2xl font-bold text-gray-900 dark:text-white">3</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default ModelDashboard;