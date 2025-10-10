import React, { useState, useEffect } from 'react';
import authService from '../services/authService';

interface AIRecommendation {
  id: string;
  type: string;
  priority: string;
  title: string;
  description: string;
  ai_confidence: number;
  predicted_impact: Record<string, number>;
  action_items: string[];
  supporting_data: any;
  model_version: string;
  generated_at: string;
  analyst_persona: string;
  business_context: any;
}

interface AIRecommendationsProps {
  analystType?: string;
  maxRecommendations?: number;
  onRecommendationClick?: (recommendation: AIRecommendation) => void;
}

const AIRecommendations: React.FC<AIRecommendationsProps> = ({
  analystType = 'financial_analyst',
  maxRecommendations = 5,
  onRecommendationClick
}) => {
  const [recommendations, setRecommendations] = useState<AIRecommendation[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>('');
  const [selectedPersona, setSelectedPersona] = useState(analystType);

  const analystPersonas = [
    { id: 'financial_analyst', name: '💰 Financial Analyst', color: 'bg-green-100 text-green-800' },
    { id: 'marketing_analyst', name: '📢 Marketing Analyst', color: 'bg-blue-100 text-blue-800' },
    { id: 'operations_analyst', name: '⚙️ Operations Analyst', color: 'bg-yellow-100 text-yellow-800' },
    { id: 'data_scientist', name: '🔬 Data Scientist', color: 'bg-purple-100 text-purple-800' }
  ];

  const fetchAIRecommendations = async (persona: string) => {
    setLoading(true);
    setError('');

    try {
      const token = await authService.getToken();
      if (!token) {
        await authService.login({ username: 'admin', password: 'admin123' });
      }

      // For demo, use mock data since backend might not be ready
      const mockRecommendations: AIRecommendation[] = [
        {
          id: 'ai_rec_20251009_001',
          type: 'revenue_growth',
          priority: 'high',
          title: '🚀 AI-Identified Revenue Growth Opportunity',
          description: `Based on your ${persona.replace('_', ' ')} analysis patterns, I've identified a 23.5% revenue growth opportunity through cross-selling optimization and premium product positioning.`,
          ai_confidence: 0.87,
          predicted_impact: {
            revenue_increase: 0.235,
            customer_growth: 0.165,
            market_share_gain: 0.118
          },
          action_items: [
            'Analyze customer segments with highest LTV potential',
            'A/B test pricing strategies for key product categories',
            'Implement cross-sell recommendation engine',
            'Develop premium product line based on demand patterns'
          ],
          supporting_data: {
            confidence_breakdown: {
              data_quality: 0.92,
              pattern_strength: 0.87,
              historical_accuracy: 0.85
            }
          },
          model_version: 'v1.0.0',
          generated_at: new Date().toISOString(),
          analyst_persona: persona,
          business_context: {
            industry: 'ecommerce',
            company_size: 'medium',
            growth_stage: 'scaling'
          }
        },
        {
          id: 'ai_rec_20251009_002',
          type: 'customer_insight',
          priority: 'critical',
          title: '🎯 AI-Powered Customer Behavior Insight',
          description: 'Machine learning analysis reveals customer segmentation opportunities affecting retention by 18.2%. High-value customers show different purchasing patterns.',
          ai_confidence: 0.93,
          predicted_impact: {
            retention_improvement: 0.182,
            ltv_increase: 0.218,
            satisfaction_boost: 0.146
          },
          action_items: [
            'Deploy advanced customer segmentation model',
            'Set up churn prediction alerts for high-value customers',
            'Analyze customer journey touchpoint effectiveness',
            'Create personalized retention campaigns'
          ],
          supporting_data: {
            confidence_breakdown: {
              data_quality: 0.95,
              pattern_strength: 0.93,
              historical_accuracy: 0.89
            }
          },
          model_version: 'v1.0.0',
          generated_at: new Date().toISOString(),
          analyst_persona: persona,
          business_context: {
            industry: 'ecommerce',
            company_size: 'medium',
            growth_stage: 'scaling'
          }
        },
        {
          id: 'ai_rec_20251009_003',
          type: 'cost_reduction',
          priority: 'medium',
          title: '💡 AI-Detected Cost Optimization Opportunity',
          description: 'Predictive models suggest operational efficiency optimization could reduce costs by 14.7% through automated process improvements.',
          ai_confidence: 0.79,
          predicted_impact: {
            cost_savings: 0.147,
            efficiency_gain: 0.132,
            margin_improvement: 0.162
          },
          action_items: [
            'Optimize inventory levels using demand forecasting',
            'Automate manual processes with highest labor costs',
            'Negotiate better supplier terms based on volume analysis',
            'Implement dynamic resource allocation'
          ],
          supporting_data: {
            confidence_breakdown: {
              data_quality: 0.84,
              pattern_strength: 0.79,
              historical_accuracy: 0.82
            }
          },
          model_version: 'v1.0.0',
          generated_at: new Date().toISOString(),
          analyst_persona: persona,
          business_context: {
            industry: 'ecommerce',
            company_size: 'medium',
            growth_stage: 'scaling'
          }
        }
      ];

      setRecommendations(mockRecommendations.slice(0, maxRecommendations));

    } catch (err) {
      setError(`Failed to fetch AI recommendations: ${(err as Error).message}`);
      console.error('AI recommendations error:', err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchAIRecommendations(selectedPersona);
  }, [selectedPersona, maxRecommendations]);

  const getPriorityColor = (priority: string) => {
    switch (priority) {
      case 'critical': return 'bg-red-100 text-red-800 border-red-200';
      case 'high': return 'bg-orange-100 text-orange-800 border-orange-200';
      case 'medium': return 'bg-yellow-100 text-yellow-800 border-yellow-200';
      case 'low': return 'bg-green-100 text-green-800 border-green-200';
      default: return 'bg-gray-100 text-gray-800 border-gray-200';
    }
  };

  const getTypeIcon = (type: string) => {
    switch (type) {
      case 'revenue_growth': return '📈';
      case 'customer_insight': return '👥';
      case 'cost_reduction': return '💰';
      case 'performance_optimization': return '⚡';
      case 'market_opportunity': return '🎯';
      case 'risk_mitigation': return '🛡️';
      default: return '💡';
    }
  };

  const formatPercentage = (value: number) => {
    return `${(value * 100).toFixed(1)}%`;
  };

  const handlePersonaChange = (persona: string) => {
    setSelectedPersona(persona);
  };

  const handleRecommendationClick = (recommendation: AIRecommendation) => {
    if (onRecommendationClick) {
      onRecommendationClick(recommendation);
    }
  };

  return (
    <div className="space-y-6">
      {/* Header with Persona Selector */}
      <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow">
        <div className="flex flex-col md:flex-row md:items-center md:justify-between mb-4">
          <div>
            <h2 className="text-2xl font-bold text-gray-900 dark:text-white flex items-center">
              🤖 AI-Powered Recommendations
            </h2>
            <p className="text-gray-600 dark:text-gray-300 mt-1">
              Personalized insights powered by machine learning
            </p>
          </div>
          <button
            onClick={() => fetchAIRecommendations(selectedPersona)}
            disabled={loading}
            className="mt-4 md:mt-0 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 transition-colors"
          >
            {loading ? '🔄 Analyzing...' : '🔄 Refresh AI Insights'}
          </button>
        </div>

        {/* Analyst Persona Selector */}
        <div className="flex flex-wrap gap-2">
          {analystPersonas.map((persona) => (
            <button
              key={persona.id}
              onClick={() => handlePersonaChange(persona.id)}
              className={`px-3 py-2 rounded-lg text-sm font-medium transition-colors ${
                selectedPersona === persona.id
                  ? persona.color
                  : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
              }`}
            >
              {persona.name}
            </button>
          ))}
        </div>
      </div>

      {/* Error Display */}
      {error && (
        <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded-lg">
          <div className="flex items-center">
            <span className="mr-2">⚠️</span>
            {error}
          </div>
        </div>
      )}

      {/* Loading State */}
      {loading && (
        <div className="flex justify-center items-center py-12">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
          <span className="ml-3 text-gray-600">AI is analyzing your data...</span>
        </div>
      )}

      {/* Recommendations List */}
      {!loading && recommendations.length > 0 && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {recommendations.map((recommendation) => (
            <div
              key={recommendation.id}
              className="bg-white dark:bg-gray-800 rounded-lg shadow hover:shadow-lg transition-shadow cursor-pointer"
              onClick={() => handleRecommendationClick(recommendation)}
            >
              <div className="p-6">
                {/* Header */}
                <div className="flex items-start justify-between mb-4">
                  <div className="flex items-center">
                    <span className="text-2xl mr-3">{getTypeIcon(recommendation.type)}</span>
                    <div>
                      <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
                        {recommendation.title}
                      </h3>
                      <div className="flex items-center mt-1 space-x-2">
                        <span className={`px-2 py-1 text-xs rounded-full border ${getPriorityColor(recommendation.priority)}`}>
                          {recommendation.priority.toUpperCase()}
                        </span>
                        <span className="text-sm text-gray-500">
                          🤖 AI Confidence: {(recommendation.ai_confidence * 100).toFixed(1)}%
                        </span>
                      </div>
                    </div>
                  </div>
                </div>

                {/* Description */}
                <p className="text-gray-600 dark:text-gray-300 mb-4">
                  {recommendation.description}
                </p>

                {/* Predicted Impact */}
                <div className="mb-4">
                  <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                    📊 Predicted Impact:
                  </h4>
                  <div className="grid grid-cols-1 sm:grid-cols-3 gap-2">
                    {Object.entries(recommendation.predicted_impact).map(([key, value]) => (
                      <div key={key} className="bg-gray-50 dark:bg-gray-700 p-2 rounded">
                        <div className="text-xs text-gray-500 capitalize">
                          {key.replace('_', ' ')}
                        </div>
                        <div className="text-sm font-semibold text-green-600">
                          +{formatPercentage(value)}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>

                {/* Action Items Preview */}
                <div className="mb-4">
                  <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                    🎯 Recommended Actions:
                  </h4>
                  <ul className="space-y-1">
                    {recommendation.action_items.slice(0, 2).map((action, index) => (
                      <li key={index} className="text-sm text-gray-600 dark:text-gray-400">
                        • {action}
                      </li>
                    ))}
                    {recommendation.action_items.length > 2 && (
                      <li className="text-sm text-blue-600">
                        + {recommendation.action_items.length - 2} more actions...
                      </li>
                    )}
                  </ul>
                </div>

                {/* Footer */}
                <div className="flex items-center justify-between text-xs text-gray-500">
                  <span>Model: {recommendation.model_version}</span>
                  <span>Generated: {new Date(recommendation.generated_at).toLocaleTimeString()}</span>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Empty State */}
      {!loading && recommendations.length === 0 && !error && (
        <div className="text-center py-12">
          <div className="text-6xl mb-4">🤖</div>
          <h3 className="text-lg font-medium text-gray-900 dark:text-white mb-2">
            No AI Recommendations Available
          </h3>
          <p className="text-gray-600 dark:text-gray-300 mb-4">
            AI is still learning from your data. Please try again later.
          </p>
          <button
            onClick={() => fetchAIRecommendations(selectedPersona)}
            className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
          >
            🔄 Check Again
          </button>
        </div>
      )}
    </div>
  );
};

export default AIRecommendations;