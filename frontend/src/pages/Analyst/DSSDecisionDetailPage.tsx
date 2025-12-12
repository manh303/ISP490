import React, { useState, useEffect } from 'react';
import { useParams, Link } from 'react-router-dom';
import { getDSSDecisionDetail, DSSDecisionDetailResponse } from '../../services/DSSApi';

const DSSDecisionDetailPage: React.FC = () => {
  const { decisionId } = useParams<{ decisionId: string }>();
  const [decision, setDecision] = useState<DSSDecisionDetailResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (decisionId) {
      fetchDecisionDetail(Number(decisionId));
    }
  }, [decisionId]);

  const fetchDecisionDetail = async (id: number) => {
    try {
      setLoading(true);
      const data = await getDSSDecisionDetail(id);
      setDecision(data);
    } catch (err) {
      setError('Failed to load decision details');
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'DRAFT': return 'bg-yellow-100 text-yellow-800';
      case 'APPROVED': return 'bg-green-100 text-green-800';
      case 'REJECTED': return 'bg-red-100 text-red-800';
      case 'IMPLEMENTED': return 'bg-blue-100 text-blue-800';
      default: return 'bg-gray-100 text-gray-800';
    }
  };

  const getScenarioName = (key: string) => {
    switch (key) {
      case 'price_prediction': return 'Price Prediction';
      case 'product_recommendation': return 'Product Recommendation';
      case 'review_sentiment': return 'Review Sentiment';
      default: return key;
    }
  };

  const formatCurrency = (value: number) => {
    return new Intl.NumberFormat('vi-VN', {
      style: 'currency',
      currency: 'VND'
    }).format(value);
  };

  if (loading) {
    return (
      <div className="p-6">
        <div className="flex justify-center items-center h-64">
          <div className="text-lg">Loading decision details...</div>
        </div>
      </div>
    );
  }

  if (error || !decision) {
    return (
      <div className="p-6">
        <div className="text-center text-red-600">{error || 'Decision not found'}</div>
      </div>
    );
  }

  return (
    <div className="p-6 max-w-6xl mx-auto">
      <div className="mb-6">
        <Link
          to="/analyst/dss-decisions"
          className="text-blue-600 hover:text-blue-800 mb-4 inline-block"
        >
          ← Back to Decisions
        </Link>
        <h1 className="text-3xl font-bold text-gray-900">{decision.title}</h1>
        <div className="mt-2 flex items-center space-x-4">
          <span className={`inline-flex px-3 py-1 text-sm font-semibold rounded-full ${getStatusColor(decision.status)}`}>
            {decision.status}
          </span>
          <span className="text-sm text-gray-600">
            {getScenarioName(decision.scenario_key)}
          </span>
          <span className="text-sm text-gray-600">
            Created by {decision.created_by_email || `User ${decision.created_by}`}
          </span>
          <span className="text-sm text-gray-600">
            {new Date(decision.created_at).toLocaleString()}
          </span>
        </div>
      </div>

      {/* Description */}
      {decision.description && (
        <div className="bg-white rounded-lg shadow p-6 mb-6">
          <h2 className="text-xl font-semibold mb-4 flex items-center">
            <svg className="w-5 h-5 mr-2 text-gray-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6h16M4 12h16M4 18h7" />
            </svg>
            Description
          </h2>
          <p className="text-gray-700 whitespace-pre-line">{decision.description}</p>
        </div>
      )}

      {/* Analysis Filters Used */}
      {decision.filters && Object.keys(decision.filters).length > 0 && (
        <div className="bg-gray-50 rounded-lg border border-gray-200 p-4 mb-6">
          <h3 className="text-sm font-semibold text-gray-700 mb-3 flex items-center">
            <svg className="w-4 h-4 mr-2 text-gray-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3 4a1 1 0 011-1h16a1 1 0 011 1v2.586a1 1 0 01-.293.707l-6.414 6.414a1 1 0 00-.293.707V17l-4 4v-6.586a1 1 0 00-.293-.707L3.293 7.293A1 1 0 013 6.586V4z" />
            </svg>
            Analysis Filters Used
          </h3>
          <div className="flex flex-wrap gap-3 text-sm">
            {decision.filters.from_date && (
              <span className="px-2 py-1 bg-white rounded border">
                📅 {decision.filters.from_date} → {decision.filters.to_date}
              </span>
            )}
            {decision.filters.platforms && decision.filters.platforms.length > 0 && (
              <span className="px-2 py-1 bg-white rounded border">
                🏪 {decision.filters.platforms.join(', ')}
              </span>
            )}
            {decision.filters.categories && decision.filters.categories.length > 0 && (
              <span className="px-2 py-1 bg-white rounded border">
                📁 {decision.filters.categories.join(', ')}
              </span>
            )}
            {decision.filters.scope_mode && (
              <span className="px-2 py-1 bg-white rounded border capitalize">
                🎯 {decision.filters.scope_mode.replace('_', ' ')}
              </span>
            )}
          </div>
        </div>
      )}

      {/* KPI Summary */}
      <div className="bg-white rounded-lg shadow p-6 mb-6">
        <h2 className="text-xl font-semibold mb-4 flex items-center">
          <svg className="w-5 h-5 mr-2 text-purple-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
          </svg>
          KPI Summary
        </h2>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {Object.entries(decision.kpi_summary).map(([key, value]) => (
            <div key={key} className="bg-gray-50 p-4 rounded">
              <div className="text-sm font-medium text-gray-600 capitalize">
                {key.replace(/_/g, ' ')}
              </div>
              <div className="text-2xl font-bold text-gray-900">
                {typeof value === 'number' ? value.toLocaleString() : String(value)}
              </div>
            </div>
          ))}
        </div>
      </div>

      <div className="bg-white rounded-lg shadow p-6 mb-6">
        <h2 className="text-xl font-semibold mb-4 flex items-center">
          <svg className="w-5 h-5 mr-2 text-blue-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" />
          </svg>
          AI Insights
        </h2>
        <div className="space-y-3">
          {decision.ai_summary_insights.map((insight, index) => (
            <div key={index} className="flex items-start">
              <span className="inline-flex items-center justify-center w-6 h-6 rounded-full bg-blue-100 text-blue-800 text-xs font-medium mr-3">
                {index + 1}
              </span>
              <p className="text-gray-700">{insight}</p>
            </div>
          ))}
        </div>
      </div>

      {/* AI Recommended Actions */}
      <div className="bg-white rounded-lg shadow p-6 mb-6">
        <h2 className="text-xl font-semibold mb-4">AI Recommended Actions</h2>
        <div className="space-y-3">
          {decision.ai_recommended_actions.map((action, index) => (
            <div key={index} className="flex items-start">
              <span className="inline-flex items-center justify-center w-6 h-6 rounded-full bg-green-100 text-green-800 text-xs font-medium mr-3">
                {index + 1}
              </span>
              <p className="text-gray-700">{action}</p>
            </div>
          ))}
        </div>
      </div>

      {/* Actions */}
      <div className="bg-white rounded-lg shadow p-6">
        <h2 className="text-xl font-semibold mb-4">Action Plan ({decision.actions.length} actions)</h2>
        <div className="space-y-4">
          {decision.actions.map((action) => (
            <div key={action.action_id} className="border border-gray-200 rounded-lg p-4">
              <div className="flex justify-between items-start mb-3">
                <div>
                  <h3 className="font-medium text-gray-900">{action.action_type}</h3>
                  <p className="text-sm text-gray-600">Target: {action.target_level}</p>
                </div>
                <span className={`inline-flex px-2 py-1 text-xs font-semibold rounded-full ${getStatusColor(action.status)}`}>
                  {action.status}
                </span>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
                {action.product_name && (
                  <div>
                    <span className="font-medium">Product:</span> {action.product_name}
                  </div>
                )}
                {action.category_name && (
                  <div>
                    <span className="font-medium">Category:</span> {action.category_name}
                  </div>
                )}
                {action.platform_name && (
                  <div>
                    <span className="font-medium">Platform:</span> {action.platform_name}
                  </div>
                )}
                {action.current_value !== undefined && (
                  <div>
                    <span className="font-medium">Current Value:</span>{' '}
                    {action.unit === 'VND' ? formatCurrency(action.current_value) : action.current_value}
                  </div>
                )}
                {action.recommended_value !== undefined && (
                  <div>
                    <span className="font-medium">Recommended Value:</span>{' '}
                    {action.unit === 'VND' ? formatCurrency(action.recommended_value) : action.recommended_value}
                  </div>
                )}
                {action.chosen_value !== undefined && (
                  <div>
                    <span className="font-medium">Chosen Value:</span>{' '}
                    {action.unit === 'VND' ? formatCurrency(action.chosen_value) : action.chosen_value}
                  </div>
                )}
                {action.planned_start_date && (
                  <div>
                    <span className="font-medium">Start Date:</span>{' '}
                    {new Date(action.planned_start_date).toLocaleDateString()}
                  </div>
                )}
                {action.planned_end_date && (
                  <div>
                    <span className="font-medium">End Date:</span>{' '}
                    {new Date(action.planned_end_date).toLocaleDateString()}
                  </div>
                )}
              </div>

              {action.note && (
                <div className="mt-3 text-sm">
                  <span className="font-medium">Note:</span> {action.note}
                </div>
              )}
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};

export default DSSDecisionDetailPage;