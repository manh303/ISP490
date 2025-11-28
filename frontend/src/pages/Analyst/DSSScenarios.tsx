import React, { useState, useEffect } from 'react';
import { BookOpen, Target, Settings, Play, Info, CheckCircle, Clock, Users } from 'lucide-react';
import { getDSSScenarios, DSSScenariosResponse, DSSScenario } from '../../services/DSSApi';

const DSSScenarios: React.FC = () => {
  const [scenarios, setScenarios] = useState<DSSScenario[]>([]);
  const [loading, setLoading] = useState(true);
  const [selectedScenario, setSelectedScenario] = useState<DSSScenario | null>(null);

  useEffect(() => {
    fetchScenarios();
  }, []);

  const fetchScenarios = async () => {
    try {
      setLoading(true);
      const response: DSSScenariosResponse = await getDSSScenarios();
      setScenarios(response.scenarios);
    } catch (error) {
      console.error('Error fetching DSS scenarios:', error);
      // Fallback mock data
      setScenarios([
        {
          key: 'price_prediction',
          name: 'Price Prediction & Optimization',
          description: 'Predict optimal pricing strategies based on market data, competitor analysis, and demand patterns',
          endpoint: '/api/v1/dss/price/run',
          use_cases: [
            'Dynamic pricing for e-commerce products',
            'Competitor price monitoring and adjustment',
            'Revenue optimization strategies',
            'Promotional pricing recommendations'
          ],
          required_inputs: ['product_key', 'platform_code', 'time_range'],
          optional_inputs: ['category', 'min_confidence', 'max_discount_pct']
        },
        {
          key: 'product_recommendation',
          name: 'Product Recommendation Engine',
          description: 'Generate personalized product recommendations using collaborative filtering and content-based algorithms',
          endpoint: '/api/v1/dss/reco/run',
          use_cases: [
            'Cross-selling recommendations',
            'Up-selling opportunities',
            'Customer retention through personalization',
            'Basket analysis and complementary products'
          ],
          required_inputs: ['source_product_key', 'scope_mode'],
          optional_inputs: ['platforms', 'categories', 'top_k', 'min_similarity']
        },
        {
          key: 'review_sentiment',
          name: 'Review Sentiment Analysis',
          description: 'Analyze customer reviews and feedback to understand sentiment patterns and identify improvement opportunities',
          endpoint: '/api/v1/dss/review/run',
          use_cases: [
            'Customer satisfaction monitoring',
            'Product quality assessment',
            'Brand reputation management',
            'Competitive intelligence from reviews'
          ],
          required_inputs: ['from_date', 'to_date'],
          optional_inputs: ['platforms', 'categories', 'sentiment_focus', 'min_reviews_per_product']
        }
      ]);
    } finally {
      setLoading(false);
    }
  };

  const getScenarioIcon = (key: string) => {
    switch (key) {
      case 'price_prediction':
        return <Target className="w-6 h-6" />;
      case 'product_recommendation':
        return <Users className="w-6 h-6" />;
      case 'review_sentiment':
        return <BookOpen className="w-6 h-6" />;
      default:
        return <Settings className="w-6 h-6" />;
    }
  };

  const getScenarioColor = (key: string) => {
    switch (key) {
      case 'price_prediction':
        return 'bg-blue-100 text-blue-800 border-blue-200';
      case 'product_recommendation':
        return 'bg-green-100 text-green-800 border-green-200';
      case 'review_sentiment':
        return 'bg-purple-100 text-purple-800 border-purple-200';
      default:
        return 'bg-gray-100 text-gray-800 border-gray-200';
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
        <h1 className="text-3xl font-bold text-gray-900 dark:text-white mb-2">
          DSS Scenarios
        </h1>
        <p className="text-gray-600 dark:text-gray-300">
          Explore available Decision Support System scenarios and their capabilities
        </p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
        {/* Scenarios List */}
        <div className="space-y-4">
          <h2 className="text-xl font-semibold text-gray-900 dark:text-white mb-4">
            Available Scenarios ({scenarios.length})
          </h2>

          {scenarios.map((scenario) => (
            <div
              key={scenario.key}
              className={`border rounded-lg p-6 cursor-pointer transition-all duration-200 hover:shadow-lg ${
                selectedScenario?.key === scenario.key
                  ? 'border-blue-500 bg-blue-50 dark:bg-blue-900/20'
                  : 'border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800'
              }`}
              onClick={() => setSelectedScenario(scenario)}
            >
              <div className="flex items-start gap-4">
                <div className={`p-3 rounded-lg ${getScenarioColor(scenario.key)}`}>
                  {getScenarioIcon(scenario.key)}
                </div>
                <div className="flex-1">
                  <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-2">
                    {scenario.name}
                  </h3>
                  <p className="text-gray-600 dark:text-gray-300 text-sm mb-3">
                    {scenario.description}
                  </p>
                  <div className="flex items-center gap-4 text-xs text-gray-500">
                    <span className="flex items-center gap-1">
                      <CheckCircle className="w-3 h-3" />
                      {scenario.required_inputs.length} required inputs
                    </span>
                    <span className="flex items-center gap-1">
                      <Clock className="w-3 h-3" />
                      {scenario.optional_inputs.length} optional inputs
                    </span>
                  </div>
                </div>
              </div>
            </div>
          ))}
        </div>

        {/* Scenario Details */}
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow border border-gray-200 dark:border-gray-700">
          {selectedScenario ? (
            <div className="p-6">
              <div className="flex items-center gap-3 mb-6">
                <div className={`p-3 rounded-lg ${getScenarioColor(selectedScenario.key)}`}>
                  {getScenarioIcon(selectedScenario.key)}
                </div>
                <div>
                  <h2 className="text-xl font-semibold text-gray-900 dark:text-white">
                    {selectedScenario.name}
                  </h2>
                  <p className="text-sm text-gray-600 dark:text-gray-300">
                    {selectedScenario.description}
                  </p>
                </div>
              </div>

              <div className="space-y-6">
                {/* Use Cases */}
                <div>
                  <h3 className="font-semibold text-gray-900 dark:text-white mb-3 flex items-center">
                    <Target className="w-4 h-4 mr-2" />
                    Use Cases
                  </h3>
                  <ul className="list-disc list-inside space-y-1 text-gray-600 dark:text-gray-300 text-sm">
                    {selectedScenario.use_cases.map((useCase, index) => (
                      <li key={index}>{useCase}</li>
                    ))}
                  </ul>
                </div>

                {/* Required Inputs */}
                <div>
                  <h3 className="font-semibold text-gray-900 dark:text-white mb-3 flex items-center">
                    <CheckCircle className="w-4 h-4 mr-2" />
                    Required Inputs
                  </h3>
                  <div className="flex flex-wrap gap-2">
                    {selectedScenario.required_inputs.map((input, index) => (
                      <span
                        key={index}
                        className="px-3 py-1 bg-red-100 text-red-800 text-xs rounded-full"
                      >
                        {input}
                      </span>
                    ))}
                  </div>
                </div>

                {/* Optional Inputs */}
                <div>
                  <h3 className="font-semibold text-gray-900 dark:text-white mb-3 flex items-center">
                    <Info className="w-4 h-4 mr-2" />
                    Optional Inputs
                  </h3>
                  <div className="flex flex-wrap gap-2">
                    {selectedScenario.optional_inputs.map((input, index) => (
                      <span
                        key={index}
                        className="px-3 py-1 bg-blue-100 text-blue-800 text-xs rounded-full"
                      >
                        {input}
                      </span>
                    ))}
                  </div>
                </div>

                {/* API Endpoint */}
                <div>
                  <h3 className="font-semibold text-gray-900 dark:text-white mb-3">API Endpoint</h3>
                  <code className="block p-3 bg-gray-100 dark:bg-gray-700 rounded text-sm font-mono text-gray-800 dark:text-gray-200">
                    {selectedScenario.endpoint}
                  </code>
                </div>

                {/* Action Button */}
                <button
                  className="w-full bg-blue-600 hover:bg-blue-700 text-white px-4 py-3 rounded-lg transition-colors duration-200 flex items-center justify-center gap-2"
                  onClick={() => window.open(`/analyst/dss/${selectedScenario.key}`, '_self')}
                >
                  <Play className="w-4 h-4" />
                  Run This Scenario
                </button>
              </div>
            </div>
          ) : (
            <div className="p-12 text-center">
              <BookOpen className="w-16 h-16 mx-auto mb-4 text-gray-400" />
              <h3 className="text-lg font-medium text-gray-900 dark:text-white mb-2">
                Select a Scenario
              </h3>
              <p className="text-gray-600 dark:text-gray-300">
                Choose a DSS scenario from the list to view its details and capabilities.
              </p>
            </div>
          )}
        </div>
      </div>

      {/* Quick Stats */}
      <div className="mt-8 grid grid-cols-1 md:grid-cols-3 gap-6">
        <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border border-gray-200 dark:border-gray-700">
          <div className="flex items-center">
            <BookOpen className="w-8 h-8 text-blue-600 mr-3" />
            <div>
              <p className="text-sm text-gray-500 dark:text-gray-400">Total Scenarios</p>
              <p className="text-2xl font-bold text-gray-900 dark:text-white">{scenarios.length}</p>
            </div>
          </div>
        </div>
        <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border border-gray-200 dark:border-gray-700">
          <div className="flex items-center">
            <Settings className="w-8 h-8 text-green-600 mr-3" />
            <div>
              <p className="text-sm text-gray-500 dark:text-gray-400">Active Models</p>
              <p className="text-2xl font-bold text-gray-900 dark:text-white">
                {scenarios.filter(s => ['price_prediction', 'product_recommendation', 'review_sentiment'].includes(s.key)).length}
              </p>
            </div>
          </div>
        </div>
        <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow border border-gray-200 dark:border-gray-700">
          <div className="flex items-center">
            <Target className="w-8 h-8 text-purple-600 mr-3" />
            <div>
              <p className="text-sm text-gray-500 dark:text-gray-400">Use Cases</p>
              <p className="text-2xl font-bold text-gray-900 dark:text-white">
                {scenarios.reduce((sum, s) => sum + s.use_cases.length, 0)}
              </p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default DSSScenarios;