import React from 'react';
import { Link } from 'react-router-dom';

const MLWireframe: React.FC = () => {
  const mlPages = [
    {
      title: 'Models Management',
      description: 'CRUD operations for ML models',
      pages: [
        { name: 'Models List', path: '/ml/models', description: 'View and manage all models' },
        { name: 'Create Model', path: '/ml/models/create', description: 'Create a new ML model' },
        { name: 'Model Details', path: '/ml/models/1', description: 'View and edit model details' }
      ]
    },
    {
      title: 'Price Prediction',
      description: 'Price forecasting and historical data',
      pages: [
        { name: 'Price Prediction', path: '/ml/price-prediction', description: 'Online prediction and history' }
      ]
    },
    {
      title: 'Recommendations',
      description: 'Product recommendation system',
      pages: [
        { name: 'Recommendations', path: '/ml/recommendations', description: 'Get product recommendations' }
      ]
    },
    {
      title: 'Sentiment Analysis',
      description: 'Review sentiment analysis',
      pages: [
        { name: 'Sentiment Analysis', path: '/ml/sentiment', description: 'Analyze review sentiments' }
      ]
    },
    {
      title: 'Status Overview',
      description: 'ML system status and metrics',
      pages: [
        { name: 'Status Overview', path: '/ml/status', description: 'System health and statistics' }
      ]
    }
  ];

  return (
    <div className="p-6">
      <h1 className="text-3xl font-bold mb-8">Machine Learning Dashboard</h1>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {mlPages.map((section, sectionIndex) => (
          <div key={sectionIndex} className="bg-white rounded-lg shadow p-6">
            <h2 className="text-xl font-semibold mb-2">{section.title}</h2>
            <p className="text-gray-600 mb-4">{section.description}</p>

            <div className="space-y-3">
              {section.pages.map((page, pageIndex) => (
                <Link
                  key={pageIndex}
                  to={page.path}
                  className="block p-3 border rounded hover:bg-gray-50 transition-colors"
                >
                  <div className="font-medium text-blue-600">{page.name}</div>
                  <div className="text-sm text-gray-500">{page.description}</div>
                </Link>
              ))}
            </div>
          </div>
        ))}
      </div>

      <div className="mt-8 p-4 bg-blue-50 rounded-lg">
        <h3 className="font-semibold text-blue-800 mb-2">Quick Actions</h3>
        <div className="flex flex-wrap gap-2">
          <Link
            to="/ml/models"
            className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 transition-colors"
          >
            Manage Models
          </Link>
          <Link
            to="/ml/price-prediction"
            className="px-4 py-2 bg-green-600 text-white rounded hover:bg-green-700 transition-colors"
          >
            Price Prediction
          </Link>
          <Link
            to="/ml/recommendations"
            className="px-4 py-2 bg-purple-600 text-white rounded hover:bg-purple-700 transition-colors"
          >
            Get Recommendations
          </Link>
          <Link
            to="/ml/sentiment"
            className="px-4 py-2 bg-orange-600 text-white rounded hover:bg-orange-700 transition-colors"
          >
            Sentiment Analysis
          </Link>
          <Link
            to="/ml/status"
            className="px-4 py-2 bg-gray-600 text-white rounded hover:bg-gray-700 transition-colors"
          >
            System Status
          </Link>
        </div>
      </div>
    </div>
  );
};

export default MLWireframe;