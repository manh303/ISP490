import React, { useState, useEffect } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { saveDSSDecision, SaveDSSDecisionRequest, DSSActionItem } from '../../services/DSSApi';

// Interface for product data from DSS Results
interface ProductData {
  product_key: string;
  product_name: string;
  platform?: string;
  category_name?: string;
  current_price?: number;
  recommended_price?: number;
  predicted_price?: number;
  price_change_pct?: number;
  confidence?: number;
}

// Interface for prefill state from DSS Results
interface PrefillState {
  scenario_key?: string;
  kpi_summary?: Record<string, any>;
  filters?: Record<string, any>;
  ai_summary_insights?: string[];
  ai_recommended_actions?: string[];
  title?: string;
  description?: string;
  table_data?: ProductData[];
}

const DSSDecisionCreatePage: React.FC = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const prefillState = location.state as PrefillState | undefined;

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Form state
  const [formData, setFormData] = useState<SaveDSSDecisionRequest>({
    scenario_key: 'price_prediction',
    title: '',
    description: '',
    status: 'DRAFT',
    actions: []
  });

  const [currentAction, setCurrentAction] = useState<Partial<DSSActionItem>>({
    action_type: 'change_price',
    target_level: 'product',
    status: 'PLANNED'
  });

  // Generate auto-description based on scenario and prefill data
  const generateDescription = (state: PrefillState): string => {
    const scenarioNames: Record<string, string> = {
      'price_prediction': 'Price Optimization & Revenue Impact',
      'product_recommendation': 'Cross-sell / Upsell Recommendations',
      'review_sentiment': 'Review Sentiment Analysis'
    };

    const scenarioName = scenarioNames[state.scenario_key || 'price_prediction'] || state.scenario_key;
    const filters = state.filters || {};
    const fromDate = filters.from_date || 'N/A';
    const toDate = filters.to_date || 'N/A';
    const platforms = (filters.platforms || []).join(', ') || 'All platforms';
    const categories = (filters.categories || []).join(', ') || 'All categories';

    // Extract top 2-3 insights
    const insights = (state.ai_summary_insights || []).slice(0, 3);
    const insightsText = insights.length > 0
      ? insights.join('; ')
      : 'Analysis results are available for review.';

    // Build description based on scenario
    let businessGoal = '';
    if (state.scenario_key === 'price_prediction') {
      businessGoal = 'optimize pricing strategy and maximize revenue while maintaining healthy profit margins';
    } else if (state.scenario_key === 'product_recommendation') {
      businessGoal = 'implement cross-sell/upsell bundles for high-potential product pairs to increase basket value';
    } else if (state.scenario_key === 'review_sentiment') {
      businessGoal = 'reduce negative review rate and improve average rating through targeted quality improvements';
    }

    return `This decision was created from the ${scenarioName} scenario for the period ${fromDate} to ${toDate} on ${platforms}, categories: ${categories}.

Analysis findings: ${insightsText}

The objective of this decision is to ${businessGoal} through the action items listed below.`;
  };

  // Prefill form from DSS Results state
  useEffect(() => {
    if (prefillState) {
      // Auto-generate description if not provided
      const autoDescription = prefillState.description || generateDescription(prefillState);

      setFormData(prev => ({
        ...prev,
        scenario_key: prefillState.scenario_key || prev.scenario_key,
        title: prefillState.title || prev.title,
        description: autoDescription,
        kpi_summary: prefillState.kpi_summary,
        filters: prefillState.filters,
        ai_summary_insights: prefillState.ai_summary_insights,
        ai_recommended_actions: prefillState.ai_recommended_actions
      }));
    }
  }, [prefillState]);

  const handleInputChange = (field: keyof SaveDSSDecisionRequest, value: any) => {
    setFormData(prev => ({ ...prev, [field]: value }));
  };

  const handleActionChange = (field: keyof DSSActionItem, value: any) => {
    setCurrentAction(prev => ({ ...prev, [field]: value }));
  };

  // Handle product selection - auto-fill current and recommended values
  const handleProductSelect = (productKey: string) => {
    const product = products.find(p => p.product_key === productKey);
    if (product) {
      setCurrentAction(prev => ({
        ...prev,
        product_key: product.product_key,
        current_value: product.current_price,
        recommended_value: product.recommended_price || product.predicted_price,
        unit: 'VND'
      }));
    } else {
      setCurrentAction(prev => ({
        ...prev,
        product_key: productKey
      }));
    }
  };

  // Get products from prefill state
  const products = prefillState?.table_data || [];

  const addAction = () => {
    if (!currentAction.action_type || !currentAction.target_level) {
      alert('Please fill in action type and target level');
      return;
    }

    const newAction: DSSActionItem = {
      action_type: currentAction.action_type,
      target_level: currentAction.target_level,
      product_key: currentAction.product_key,
      product_sk: currentAction.product_sk,
      platform_sk: currentAction.platform_sk,
      category_sk: currentAction.category_sk,
      current_value: currentAction.current_value,
      recommended_value: currentAction.recommended_value,
      chosen_value: currentAction.chosen_value,
      unit: currentAction.unit,
      planned_start_date: currentAction.planned_start_date,
      planned_end_date: currentAction.planned_end_date,
      status: currentAction.status || 'PLANNED',
      note: currentAction.note
    };

    setFormData(prev => ({
      ...prev,
      actions: [...prev.actions, newAction]
    }));

    // Reset current action
    setCurrentAction({
      action_type: 'change_price',
      target_level: 'product',
      status: 'PLANNED'
    });
  };

  const removeAction = (index: number) => {
    setFormData(prev => ({
      ...prev,
      actions: prev.actions.filter((_, i) => i !== index)
    }));
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();

    if (!formData.title.trim()) {
      setError('Title is required');
      return;
    }

    if (formData.actions.length === 0) {
      setError('At least one action is required');
      return;
    }

    try {
      setLoading(true);
      setError(null);

      const result = await saveDSSDecision(formData);
      navigate(`/analyst/dss-decisions/${result.decision_id}`);
    } catch (err) {
      setError('Failed to save decision');
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="p-6 max-w-4xl mx-auto">
      <div className="mb-6">
        <button
          onClick={() => navigate('/analyst/dss-decisions')}
          className="text-blue-600 hover:text-blue-800 mb-4"
        >
          ← Back to Decisions
        </button>
        <h1 className="text-3xl font-bold text-gray-900">Create New DSS Decision</h1>
        {prefillState && (
          <p className="text-sm text-gray-500 mt-1">Pre-filled with data from DSS analysis</p>
        )}
      </div>

      {/* Prefilled Data Summary */}
      {prefillState && (prefillState.kpi_summary || prefillState.ai_summary_insights) && (
        <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 space-y-4">
          <h3 className="font-semibold text-blue-900 flex items-center">
            <svg className="w-5 h-5 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
            Analysis Summary (Read-only)
          </h3>

          {/* KPI Summary */}
          {prefillState.kpi_summary && (
            <div>
              <h4 className="text-sm font-medium text-blue-800 mb-2">KPI Summary</h4>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                {Object.entries(prefillState.kpi_summary).map(([key, value]) => (
                  <div key={key} className="bg-white rounded p-2 text-center">
                    <p className="text-xs text-gray-500 capitalize">{key.replace(/_/g, ' ')}</p>
                    <p className="font-semibold text-gray-900">
                      {typeof value === 'number' ? value.toLocaleString('vi-VN') : String(value)}
                    </p>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* AI Insights */}
          {prefillState.ai_summary_insights && prefillState.ai_summary_insights.length > 0 && (
            <div>
              <h4 className="text-sm font-medium text-blue-800 mb-2">AI Insights</h4>
              <ul className="text-sm text-gray-700 space-y-1">
                {prefillState.ai_summary_insights.slice(0, 3).map((insight, idx) => (
                  <li key={idx} className="flex items-start">
                    <span className="text-blue-500 mr-2">•</span>
                    {insight}
                  </li>
                ))}
              </ul>
            </div>
          )}

          {/* AI Recommended Actions */}
          {prefillState.ai_recommended_actions && prefillState.ai_recommended_actions.length > 0 && (
            <div>
              <h4 className="text-sm font-medium text-blue-800 mb-2">AI Recommended Actions</h4>
              <ul className="text-sm text-gray-700 space-y-1">
                {prefillState.ai_recommended_actions.slice(0, 3).map((action, idx) => (
                  <li key={idx} className="flex items-start">
                    <span className="text-green-500 mr-2">✓</span>
                    {action}
                  </li>
                ))}
              </ul>
            </div>
          )}

          {/* Products available */}
          {products.length > 0 && (
            <p className="text-xs text-blue-600">
              {products.length} products available from analysis for action selection
            </p>
          )}
        </div>
      )}

      <form onSubmit={handleSubmit} className="space-y-6">
        {/* Basic Information */}
        <div className="bg-white rounded-lg shadow p-6">
          <h2 className="text-xl font-semibold mb-4">Basic Information</h2>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Scenario *
              </label>
              <select
                value={formData.scenario_key}
                onChange={(e) => handleInputChange('scenario_key', e.target.value)}
                className="w-full border border-gray-300 rounded px-3 py-2"
                required
              >
                <option value="price_prediction">Price Prediction</option>
                <option value="product_recommendation">Product Recommendation</option>
                <option value="review_sentiment">Review Sentiment</option>
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Status
              </label>
              <select
                value={formData.status}
                onChange={(e) => handleInputChange('status', e.target.value)}
                className="w-full border border-gray-300 rounded px-3 py-2"
              >
                <option value="DRAFT">Draft</option>
                <option value="APPROVED">Approved</option>
                <option value="REJECTED">Rejected</option>
                <option value="IMPLEMENTED">Implemented</option>
              </select>
            </div>
          </div>

          <div className="mt-4">
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Title *
            </label>
            <input
              type="text"
              value={formData.title}
              onChange={(e) => handleInputChange('title', e.target.value)}
              className="w-full border border-gray-300 rounded px-3 py-2"
              placeholder="Enter decision title"
              required
            />
          </div>

          <div className="mt-4">
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Description
            </label>
            <textarea
              value={formData.description || ''}
              onChange={(e) => handleInputChange('description', e.target.value)}
              className="w-full border border-gray-300 rounded px-3 py-2"
              rows={3}
              placeholder="Enter decision description"
            />
          </div>
        </div>

        {/* Actions */}
        <div className="bg-white rounded-lg shadow p-6">
          <h2 className="text-xl font-semibold mb-4">Action Plan</h2>

          {/* Add Action Form */}
          <div className="border border-gray-200 rounded-lg p-4 mb-4">
            <h3 className="font-medium mb-3">Add New Action</h3>

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Action Type *
                </label>
                <select
                  value={currentAction.action_type}
                  onChange={(e) => handleActionChange('action_type', e.target.value)}
                  className="w-full border border-gray-300 rounded px-3 py-2"
                >
                  <option value="change_price">Change Price</option>
                  <option value="marketing_campaign">Marketing Campaign</option>
                  <option value="fix_quality">Fix Quality</option>
                  <option value="add_product">Add Product</option>
                  <option value="remove_product">Remove Product</option>
                </select>
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Target Level *
                </label>
                <select
                  value={currentAction.target_level}
                  onChange={(e) => handleActionChange('target_level', e.target.value)}
                  className="w-full border border-gray-300 rounded px-3 py-2"
                >
                  <option value="product">Product</option>
                  <option value="category">Category</option>
                  <option value="platform">Platform</option>
                </select>
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Product {products.length > 0 ? `(${products.length} available)` : ''}
                </label>
                {products.length > 0 ? (
                  <select
                    value={currentAction.product_key || ''}
                    onChange={(e) => handleProductSelect(e.target.value)}
                    className="w-full border border-gray-300 rounded px-3 py-2"
                  >
                    <option value="">-- Select Product --</option>
                    {products.map((product) => (
                      <option key={product.product_key} value={product.product_key}>
                        {product.product_name} ({product.platform || 'N/A'}) - {product.current_price?.toLocaleString('vi-VN')} VND
                      </option>
                    ))}
                  </select>
                ) : (
                  <input
                    type="text"
                    value={currentAction.product_key || ''}
                    onChange={(e) => handleActionChange('product_key', e.target.value)}
                    className="w-full border border-gray-300 rounded px-3 py-2"
                    placeholder="e.g., tiki_123456"
                  />
                )}
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Current Value
                </label>
                <input
                  type="number"
                  value={currentAction.current_value || ''}
                  onChange={(e) => handleActionChange('current_value', parseFloat(e.target.value))}
                  className="w-full border border-gray-300 rounded px-3 py-2"
                  step="0.01"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Recommended Value
                </label>
                <input
                  type="number"
                  value={currentAction.recommended_value || ''}
                  onChange={(e) => handleActionChange('recommended_value', parseFloat(e.target.value))}
                  className="w-full border border-gray-300 rounded px-3 py-2"
                  step="0.01"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Chosen Value
                </label>
                <input
                  type="number"
                  value={currentAction.chosen_value || ''}
                  onChange={(e) => handleActionChange('chosen_value', parseFloat(e.target.value))}
                  className="w-full border border-gray-300 rounded px-3 py-2"
                  step="0.01"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Unit
                </label>
                <input
                  type="text"
                  value={currentAction.unit || ''}
                  onChange={(e) => handleActionChange('unit', e.target.value)}
                  className="w-full border border-gray-300 rounded px-3 py-2"
                  placeholder="VND, %, score"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Start Date
                </label>
                <input
                  type="date"
                  value={currentAction.planned_start_date || ''}
                  onChange={(e) => handleActionChange('planned_start_date', e.target.value)}
                  className="w-full border border-gray-300 rounded px-3 py-2"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  End Date
                </label>
                <input
                  type="date"
                  value={currentAction.planned_end_date || ''}
                  onChange={(e) => handleActionChange('planned_end_date', e.target.value)}
                  className="w-full border border-gray-300 rounded px-3 py-2"
                />
              </div>
            </div>

            <div className="mt-4">
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Note
              </label>
              <textarea
                value={currentAction.note || ''}
                onChange={(e) => handleActionChange('note', e.target.value)}
                className="w-full border border-gray-300 rounded px-3 py-2"
                rows={2}
                placeholder="Additional notes"
              />
            </div>

            <button
              type="button"
              onClick={addAction}
              className="mt-4 bg-green-600 text-white px-4 py-2 rounded hover:bg-green-700"
            >
              Add Action
            </button>
          </div>

          {/* Actions List */}
          <div className="space-y-3">
            <h3 className="font-medium">Actions ({formData.actions.length})</h3>
            {formData.actions.map((action, index) => (
              <div key={index} className="flex justify-between items-center bg-gray-50 p-3 rounded">
                <div>
                  <span className="font-medium">{action.action_type}</span>
                  <span className="text-gray-600 ml-2">({action.target_level})</span>
                  {action.product_key && (
                    <span className="text-gray-600 ml-2">Product: {action.product_key}</span>
                  )}
                  {action.chosen_value && (
                    <span className="text-gray-600 ml-2">
                      Value: {action.chosen_value} {action.unit}
                    </span>
                  )}
                </div>
                <button
                  type="button"
                  onClick={() => removeAction(index)}
                  className="text-red-600 hover:text-red-800"
                >
                  Remove
                </button>
              </div>
            ))}
          </div>
        </div>

        {error && (
          <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded">
            {error}
          </div>
        )}

        <div className="flex justify-end space-x-4">
          <button
            type="button"
            onClick={() => navigate('/analyst/dss-decisions')}
            className="px-4 py-2 border border-gray-300 rounded text-gray-700 hover:bg-gray-50"
          >
            Cancel
          </button>
          <button
            type="submit"
            disabled={loading}
            className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50"
          >
            {loading ? 'Saving...' : 'Save Decision'}
          </button>
        </div>
      </form>
    </div>
  );
};

export default DSSDecisionCreatePage;