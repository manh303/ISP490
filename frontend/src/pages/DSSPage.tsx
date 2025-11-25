import React, { useState, useEffect } from 'react';
import { useAuth } from '../contexts/AuthContext';
import Cookies from 'js-cookie';
import AIRecommendations from '../components/AIRecommendations';

interface EcommerceOverview {
  total_products: number;
  platforms: number;
  avg_price: number;
  rated_products: number;
  platform_breakdown?: Array<{
    source_platform: string;
    total_products: number;
    avg_price: number;
    rated_products: number;
  }>;
}

interface ProductData {
  source_platform: string;
  product_name: string;
  brand: string;
  price: number;
  discount?: number;
  rating?: number;
  review_count?: number;
  sold_count?: number;
}

interface BrandData {
  brand: string;
  source_platform: string;
  product_count: number;
  avg_price: number;
  avg_rating: number;
}

interface DSSData {
  overview?: EcommerceOverview;
  price_comparison?: ProductData[];
  brand_analysis?: BrandData[];
  trending_products?: ProductData[];
  [key: string]: any;
}

const DSSPage: React.FC = () => {
  const { user, isAuthenticated } = useAuth();
  const [activeTab, setActiveTab] = useState<string>('dashboard');
  const [loading, setLoading] = useState<boolean>(false);
  const [dssData, setDssData] = useState<DSSData | null>(null);
  const [error, setError] = useState<string>('');
  const [startedActions, setStartedActions] = useState<Set<string>>(new Set());
  const [actionModal, setActionModal] = useState<any>(null);

  // Get auth token from AuthContext
  const getAuthToken = (): string | null => {
    if (!isAuthenticated) {
      setError('Please log in to access DSS data.');
      return null;
    }
    return Cookies.get('access_token');
  };

  const fetchDSSData = async (tab: string) => {
    setLoading(true);
    setError('');
    try {
      const token = getAuthToken();

      // Map tabs to specific endpoints
      const endpointMap: {[key: string]: string} = {
        'dashboard': 'ecommerce/overview',
        'price-analysis': 'ecommerce/price-comparison',
        'brand-analysis': 'ecommerce/brand-analysis',
        'trending': 'ecommerce/trending-products',
        'ai-recommendations': 'dashboard', // Keep original for AI recommendations
      };

      const endpoint = endpointMap[tab] || 'ecommerce/overview';
      const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:8000';

      const response = await fetch(`${API_BASE}/api/v1/dss/${endpoint}`, {
        headers: {
          'Authorization': token ? `Bearer ${token}` : '',
          'Content-Type': 'application/json',
        },
      });

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const result = await response.json();

      // Update dssData based on the endpoint
      if (tab === 'dashboard') {
        setDssData({ overview: result.data });
      } else if (tab === 'price-analysis') {
        setDssData({ price_comparison: result.data });
      } else if (tab === 'brand-analysis') {
        setDssData({ brand_analysis: result.data });
      } else if (tab === 'trending') {
        setDssData({ trending_products: result.data });
      } else {
        setDssData(result);
      }

    } catch (err) {
      setError(`Error fetching ${tab}: ${(err as Error).message}`);
      console.error('DSS fetch error:', err);
      // Set some mock data on error to keep demo working
      if (tab === 'dashboard') {
        setDssData({
          overview: {
            total_products: 1250,
            platforms: 2,
            avg_price: 15000000,
            rated_products: 980
          }
        });
      }
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchDSSData('dashboard');
  }, []);

  const handleTabChange = (tab: string) => {
    setActiveTab(tab);
    fetchDSSData(tab);
  };

  const formatCurrency = (amount: number) => {
    return new Intl.NumberFormat('vi-VN', {
      style: 'currency',
      currency: 'VND',
    }).format(amount);
  };

  const getPriorityBadge = (priority: string) => {
    const colors = {
      high: 'bg-red-100 text-red-800',
      medium: 'bg-yellow-100 text-yellow-800',
      low: 'bg-green-100 text-green-800',
    };
    return colors[priority] || 'bg-gray-100 text-gray-800';
  };

  const handleStartAction = (action: any) => {
    setActionModal(action);
  };

  const confirmStartAction = (actionId: string) => {
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

  const isActionStarted = (actionId: string) => {
    return startedActions.has(actionId);
  };

  const renderDashboard = () => {
    if (!dssData?.overview) return null;

    const overview = dssData.overview;

    return (
      <div className="space-y-6">
        {/* Summary Metrics */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow">
            <h3 className="text-sm font-medium text-gray-500 dark:text-gray-400">Total Products</h3>
            <p className="text-2xl font-bold text-gray-900 dark:text-white">
              {overview?.total_products?.toLocaleString()}
            </p>
          </div>
          <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow">
            <h3 className="text-sm font-medium text-gray-500 dark:text-gray-400">Platforms</h3>
            <p className="text-2xl font-bold text-gray-900 dark:text-white">
              {overview?.platforms}
            </p>
          </div>
          <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow">
            <h3 className="text-sm font-medium text-gray-500 dark:text-gray-400">Average Price</h3>
            <p className="text-2xl font-bold text-gray-900 dark:text-white">
              {formatCurrency(overview?.avg_price || 0)}
            </p>
          </div>
          <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow">
            <h3 className="text-sm font-medium text-gray-500 dark:text-gray-400">Products with Ratings</h3>
            <p className="text-2xl font-bold text-gray-900 dark:text-white">
              {overview?.rated_products?.toLocaleString()}
            </p>
          </div>
        </div>

        {/* Platform Breakdown */}
        {overview.platform_breakdown && (
          <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow">
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
              🏪 Platform Breakdown
            </h3>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {overview.platform_breakdown.map((platform, index) => (
                <div key={index} className="p-4 bg-gray-50 dark:bg-gray-700 rounded-lg">
                  <div className="flex items-center justify-between mb-2">
                    <h4 className="font-medium text-gray-900 dark:text-white capitalize">
                      {platform.source_platform}
                    </h4>
                    <span className={`px-2 py-1 text-xs rounded ${
                      platform.source_platform === 'tiki' ? 'bg-blue-100 text-blue-800' : 'bg-orange-100 text-orange-800'
                    }`}>
                      {platform.total_products} products
                    </span>
                  </div>
                  <div className="space-y-1 text-sm text-gray-600 dark:text-gray-300">
                    <div>Avg Price: {formatCurrency(platform.avg_price || 0)}</div>
                    <div>Rated Products: {platform.rated_products}</div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* DSS Insights */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow">
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
              💡 Key Insights
            </h3>
            <div className="space-y-4">
              <div className="p-4 bg-blue-50 dark:bg-blue-900/20 rounded-lg border border-blue-200">
                <h4 className="font-medium text-gray-900 dark:text-white">Market Coverage</h4>
                <p className="text-sm text-gray-600 dark:text-gray-300 mt-1">
                  Tracking {overview.total_products.toLocaleString()} products across {overview.platforms} major platforms
                </p>
              </div>
              <div className="p-4 bg-green-50 dark:bg-green-900/20 rounded-lg border border-green-200">
                <h4 className="font-medium text-gray-900 dark:text-white">Rating Coverage</h4>
                <p className="text-sm text-gray-600 dark:text-gray-300 mt-1">
                  {Math.round((overview.rated_products / overview.total_products) * 100)}% of products have customer ratings
                </p>
              </div>
            </div>
          </div>

          <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow">
            <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
              🎯 DSS Recommendations
            </h3>
            <div className="space-y-4">
              <div className="p-4 bg-yellow-50 dark:bg-yellow-900/20 rounded-lg border border-yellow-200">
                <h4 className="font-medium text-gray-900 dark:text-white">Price Analysis</h4>
                <p className="text-sm text-gray-600 dark:text-gray-300 mt-1">
                  Analyze price trends across platforms for competitive insights
                </p>
              </div>
              <div className="p-4 bg-purple-50 dark:bg-purple-900/20 rounded-lg border border-purple-200">
                <h4 className="font-medium text-gray-900 dark:text-white">Brand Performance</h4>
                <p className="text-sm text-gray-600 dark:text-gray-300 mt-1">
                  Monitor brand market share and customer satisfaction metrics
                </p>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  };

  const renderPriceAnalysis = () => {
    if (!dssData?.price_comparison) return null;

    const priceData = dssData.price_comparison;

    return (
      <div className="space-y-6">
        <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow">
          <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
            💰 Price Comparison Analysis
          </h3>
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
              <thead className="bg-gray-50 dark:bg-gray-700">
                <tr>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">
                    Product
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">
                    Brand
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">
                    Platform
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">
                    Price
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">
                    Discount
                  </th>
                </tr>
              </thead>
              <tbody className="bg-white dark:bg-gray-800 divide-y divide-gray-200 dark:divide-gray-700">
                {priceData.slice(0, 20).map((product, index) => (
                  <tr key={index}>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900 dark:text-white">
                      {product.product_name?.slice(0, 50)}...
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-600 dark:text-gray-300">
                      {product.brand || 'N/A'}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      <span className={`px-2 py-1 text-xs rounded ${\n                        product.source_platform === 'tiki' ? 'bg-blue-100 text-blue-800' : 'bg-orange-100 text-orange-800'\n                      }`}>
                        {product.source_platform}
                      </span>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900 dark:text-white">
                      {formatCurrency(product.price || 0)}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-600 dark:text-gray-300">
                      {product.discount ? `${product.discount}%` : 'N/A'}
                    </td>
                  </tr>
                ))}\n              </tbody>
            </table>
          </div>
        </div>
      </div>
    );
  };

  const renderBrandAnalysis = () => {
    if (!dssData?.brand_analysis) return null;

    const brandData = dssData.brand_analysis;

    return (
      <div className="space-y-6">
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {brandData.slice(0, 10).map((brand, index) => (
            <div key={index} className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow">
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
                  {brand.brand}
                </h3>
                <span className={`px-2 py-1 text-xs rounded ${\n                  brand.source_platform === 'tiki' ? 'bg-blue-100 text-blue-800' : 'bg-orange-100 text-orange-800'\n                }`}>
                  {brand.source_platform}
                </span>
              </div>

              <div className="space-y-3">
                <div className="flex justify-between">
                  <span className="text-sm text-gray-600 dark:text-gray-300">Product Count</span>
                  <span className="text-sm font-medium text-gray-900 dark:text-white">
                    {brand.product_count}
                  </span>
                </div>

                <div className="flex justify-between">
                  <span className="text-sm text-gray-600 dark:text-gray-300">Average Price</span>
                  <span className="text-sm font-medium text-gray-900 dark:text-white">
                    {formatCurrency(brand.avg_price || 0)}
                  </span>
                </div>

                <div className="flex justify-between">
                  <span className="text-sm text-gray-600 dark:text-gray-300">Average Rating</span>
                  <span className="text-sm font-medium text-gray-900 dark:text-white">
                    ⭐ {brand.avg_rating?.toFixed(1) || 'N/A'}
                  </span>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>
    );
  };

  const renderTrendingProducts = () => {
    if (!dssData?.trending_products) return null;

    const trendingData = dssData.trending_products;

    return (
      <div className="space-y-6">
        <div className="bg-white dark:bg-gray-800 p-6 rounded-lg shadow">
          <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
            🔥 Trending Products
          </h3>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {trendingData.slice(0, 12).map((product, index) => (
              <div key={index} className="p-4 border border-gray-200 dark:border-gray-700 rounded-lg">
                <div className="flex items-start justify-between mb-2">
                  <h4 className="text-sm font-medium text-gray-900 dark:text-white line-clamp-2">
                    {product.product_name}
                  </h4>
                  <span className={`px-2 py-1 text-xs rounded ml-2 ${\n                    product.source_platform === 'tiki' ? 'bg-blue-100 text-blue-800' : 'bg-orange-100 text-orange-800'\n                  }`}>
                    {product.source_platform}
                  </span>
                </div>

                <div className="space-y-1 text-xs text-gray-600 dark:text-gray-300">
                  <div>Brand: {product.brand || 'N/A'}</div>
                  <div>Price: {formatCurrency(product.price || 0)}</div>
                  <div>Rating: ⭐ {product.rating?.toFixed(1) || 'N/A'}</div>
                  <div>Reviews: {product.review_count || 0}</div>
                  {product.sold_count && <div>Sold: {product.sold_count}</div>}
                </div>
              </div>
            ))}
          </div>
        </div>
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

    const getAlertColor = (type: string) => {
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
            🛒 E-commerce DSS Dashboard
          </h1>
          <p className="text-xl text-gray-600 dark:text-gray-300">
            Tiki & Lazada Market Intelligence Platform
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
              { id: 'dashboard', label: '📊 Overview', endpoint: 'dashboard' },
              { id: 'price-analysis', label: '💰 Price Analysis', endpoint: 'price-analysis' },
              { id: 'brand-analysis', label: '🏷️ Brand Analysis', endpoint: 'brand-analysis' },
              { id: 'trending', label: '🔥 Trending Products', endpoint: 'trending' },
              { id: 'ai-recommendations', label: '🤖 AI Insights', endpoint: 'ai-recommendations' },
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
            {activeTab === 'price-analysis' && renderPriceAnalysis()}
            {activeTab === 'brand-analysis' && renderBrandAnalysis()}
            {activeTab === 'trending' && renderTrendingProducts()}
            {activeTab === 'ai-recommendations' && (
              <AIRecommendations
                analystType="financial_analyst"
                maxRecommendations={6}
                onRecommendationClick={(rec) => {
                  console.log('AI Recommendation clicked:', rec);
                  // Could open a detailed modal or navigate to detail view
                }}
              />
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