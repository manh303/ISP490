/**
 * ML Insights API Test/Demo Page
 * 
 * This component demonstrates all ML Insights API calls
 * Use this to test if your API integration is working correctly
 */

import React, { useState } from 'react';
import {
  getMLSummary,
  getPriceOptimization,
  getDemandForecast,
  getWeeklySalesForecast,
  getSalesTrend,
  getSeasonality,
} from '../../services/MLInsightsApi';

const MLInsightsTest: React.FC = () => {
  const [results, setResults] = useState<{ [key: string]: any }>({});
  const [loading, setLoading] = useState<{ [key: string]: boolean }>({});
  const [errors, setErrors] = useState<{ [key: string]: string }>({});

  const testAPI = async (name: string, apiCall: () => Promise<any>) => {
    setLoading(prev => ({ ...prev, [name]: true }));
    setErrors(prev => ({ ...prev, [name]: '' }));

    try {
      const result = await apiCall();
      setResults(prev => ({ ...prev, [name]: result }));
      console.log(`✅ ${name}:`, result);
    } catch (error: any) {
      const errorMsg = error.response?.data?.message || error.message || 'Unknown error';
      setErrors(prev => ({ ...prev, [name]: errorMsg }));
      console.error(`❌ ${name}:`, error);
    } finally {
      setLoading(prev => ({ ...prev, [name]: false }));
    }
  };

  const tests = [
    {
      name: 'ML Summary',
      description: 'Get aggregated ML insights summary',
      call: () => getMLSummary(),
    },
    {
      name: 'Price Optimization (All)',
      description: 'Get all price optimization recommendations',
      call: () => getPriceOptimization(10),
    },
    {
      name: 'Price Optimization (Increase)',
      description: 'Get products that should increase price',
      call: () => getPriceOptimization(10, 'Increase Price'),
    },
    {
      name: 'Demand Forecast (All)',
      description: 'Get all demand forecasts',
      call: () => getDemandForecast(10),
    },
    {
      name: 'Demand Forecast (Growing)',
      description: 'Get products with growing demand',
      call: () => getDemandForecast(10, 'Growing'),
    },
    {
      name: 'Weekly Sales Forecast',
      description: 'Get weekly sales forecast by day of week',
      call: () => getWeeklySalesForecast(),
    },
    {
      name: 'Sales Trend',
      description: 'Get monthly sales trend',
      call: () => getSalesTrend(),
    },
    {
      name: 'Seasonality',
      description: 'Get seasonality analysis',
      call: () => getSeasonality(),
    },
  ];

  const testAll = async () => {
    for (const test of tests) {
      await testAPI(test.name, test.call);
      // Add small delay between requests
      await new Promise(resolve => setTimeout(resolve, 500));
    }
  };

  return (
    <div className="min-h-screen bg-gray-50 p-6">
      <div className="max-w-7xl mx-auto">
        <div className="bg-white rounded-lg shadow-md p-6 mb-6">
          <h1 className="text-3xl font-bold text-gray-900 mb-2">
            🧪 ML Insights API Test
          </h1>
          <p className="text-gray-600 mb-4">
            Test all ML Insights API endpoints to verify integration
          </p>
          <button
            onClick={testAll}
            disabled={Object.values(loading).some(l => l)}
            className="px-6 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors disabled:opacity-50 disabled:cursor-not-allowed font-medium"
          >
            {Object.values(loading).some(l => l) ? '⏳ Testing...' : '▶️ Run All Tests'}
          </button>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {tests.map((test) => (
            <div key={test.name} className="bg-white rounded-lg shadow-md p-6">
              <div className="flex items-start justify-between mb-4">
                <div className="flex-1">
                  <h3 className="text-lg font-semibold text-gray-800 mb-1">
                    {test.name}
                  </h3>
                  <p className="text-sm text-gray-600">{test.description}</p>
                </div>
                <button
                  onClick={() => testAPI(test.name, test.call)}
                  disabled={loading[test.name]}
                  className="ml-4 px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 transition-colors disabled:opacity-50 disabled:cursor-not-allowed text-sm"
                >
                  {loading[test.name] ? '⏳' : '▶️'} Test
                </button>
              </div>

              {loading[test.name] && (
                <div className="flex items-center justify-center py-8">
                  <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
                </div>
              )}

              {errors[test.name] && (
                <div className="bg-red-50 border border-red-200 rounded-lg p-4">
                  <div className="flex items-start">
                    <span className="text-red-500 text-lg mr-2">❌</span>
                    <div>
                      <p className="text-sm font-medium text-red-800">Error</p>
                      <p className="text-sm text-red-600 mt-1">{errors[test.name]}</p>
                    </div>
                  </div>
                </div>
              )}

              {results[test.name] && !loading[test.name] && !errors[test.name] && (
                <div className="bg-green-50 border border-green-200 rounded-lg p-4">
                  <div className="flex items-start mb-2">
                    <span className="text-green-500 text-lg mr-2">✅</span>
                    <div className="flex-1">
                      <p className="text-sm font-medium text-green-800">Success</p>
                      <p className="text-xs text-green-600 mt-1">
                        {Array.isArray(results[test.name]) 
                          ? `${results[test.name].length} items returned`
                          : 'Data received'}
                      </p>
                    </div>
                  </div>
                  <details className="mt-2">
                    <summary className="text-sm text-blue-600 cursor-pointer hover:text-blue-700">
                      View Response
                    </summary>
                    <pre className="mt-2 p-3 bg-gray-900 text-green-400 rounded text-xs overflow-x-auto max-h-64 overflow-y-auto">
                      {JSON.stringify(results[test.name], null, 2)}
                    </pre>
                  </details>
                </div>
              )}

              {!results[test.name] && !loading[test.name] && !errors[test.name] && (
                <div className="bg-gray-50 border border-gray-200 rounded-lg p-4 text-center">
                  <p className="text-sm text-gray-500">Click "Test" to run this API call</p>
                </div>
              )}
            </div>
          ))}
        </div>

        {/* Summary */}
        {Object.keys(results).length > 0 && (
          <div className="mt-6 bg-white rounded-lg shadow-md p-6">
            <h2 className="text-xl font-bold text-gray-800 mb-4">📊 Test Summary</h2>
            <div className="grid grid-cols-3 gap-4">
              <div className="bg-green-50 p-4 rounded-lg text-center">
                <div className="text-3xl font-bold text-green-600 mb-1">
                  {Object.keys(results).length}
                </div>
                <div className="text-sm text-green-700">Successful</div>
              </div>
              <div className="bg-red-50 p-4 rounded-lg text-center">
                <div className="text-3xl font-bold text-red-600 mb-1">
                  {Object.keys(errors).filter(k => errors[k]).length}
                </div>
                <div className="text-sm text-red-700">Failed</div>
              </div>
              <div className="bg-blue-50 p-4 rounded-lg text-center">
                <div className="text-3xl font-bold text-blue-600 mb-1">
                  {tests.length - Object.keys(results).length - Object.keys(errors).filter(k => errors[k]).length}
                </div>
                <div className="text-sm text-blue-700">Pending</div>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default MLInsightsTest;
