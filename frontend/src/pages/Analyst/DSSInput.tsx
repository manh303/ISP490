import React, { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { ArrowLeft, Send, TrendingUp, Users, MessageSquare, ChevronDown, ChevronUp, Settings } from 'lucide-react';
import Button from '../../components/ui/button/Button';
import Form from '../../components/form/Form';
import Input from '../../components/form/input/InputField';
import Select from '../../components/form/Select';
import { PlatformSelect } from '../../components/analytics/PlatformSelect';
import { CategorySelect } from '../../components/analytics/CategorySelect';
import { ProductSearch } from '../../components/analytics/ProductSearch';
import DatePicker from 'react-datepicker';
import { FaRegCalendarAlt } from 'react-icons/fa';
import { vi } from 'date-fns/locale';
import {
  runPricePredictionDSS,
  runProductRecommendationDSS,
  runReviewSentimentDSS,
  PricePredictionRequest,
  ProductRecommendationRequest,
  ReviewSentimentRequest,
} from '../../services/DSSApi';
import { getCategories, type Category } from '../../services/analyticsApi';
interface DSSInputData {
  product_key?: string;
  platform_code?: string;
  category?: string;
  time_range?: string;
  customer_id?: string;
  review_text?: string;
  from_date?: string;
  to_date?: string;
  scope_mode?: 'by_product' | 'by_category';
}

interface AdvancedOptions {
  // Price Prediction
  max_discount_pct: number;
  min_margin_pct: number;
  min_confidence: number;
  min_price_change_pct: number;
  top_n: number;
  // Product Recommendation
  top_k: number;
  min_similarity: number;
  min_co_purchase_rate: number;
  // Review Sentiment
  min_reviews_per_product: number;
  sentiment_focus: 'all' | 'only_positive' | 'only_negative';
  negative_threshold: number;
}

const DSSInput: React.FC = () => {
  const { modelId } = useParams<{ modelId: string }>();
  const navigate = useNavigate();
  const [loading, setLoading] = useState(false);
  const [formData, setFormData] = useState<DSSInputData>({
    platform_code: 'tiki'
  });
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [categories, setCategories] = useState<Category[]>([]);
  const [fromDate, setFromDate] = useState<Date | null>(null);
  const [toDate, setToDate] = useState<Date | null>(null);

  // Select states
  const [platformCode, setPlatformCode] = useState<string>('tiki');
  const [categoryKey, setCategoryKey] = useState<string>('');
  const [productId, setProductId] = useState<string>('');
  const [productName, setProductName] = useState<string>('');
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [advancedOptions, setAdvancedOptions] = useState<AdvancedOptions>({
    // Price defaults
    max_discount_pct: 20,
    min_margin_pct: 10,
    min_confidence: 80,
    min_price_change_pct: 0,
    top_n: 50,
    // Reco defaults
    top_k: 10,
    min_similarity: 50,
    min_co_purchase_rate: 5,
    // Sentiment defaults
    min_reviews_per_product: 1,
    sentiment_focus: 'all',
    negative_threshold: 25
  });

  // Load categories on component mount
  useEffect(() => {
    const loadCategories = async () => {
      try {
        const categoriesData = await getCategories();
        setCategories(categoriesData);
      } catch (error) {
        console.error('Error loading categories:', error);
        // Fallback to hardcoded categories if API fails
        setCategories([
          { category_key: '1', category_name: 'Smartphones', level: 3, parent_key: null, platform_code: null },
          { category_key: '2', category_name: 'Accessories', level: 3, parent_key: null, platform_code: null },
          { category_key: '3', category_name: 'Printers', level: 3, parent_key: null, platform_code: null },
          { category_key: '4', category_name: 'Accessories', level: 3, parent_key: null, platform_code: null },
          { category_key: '5', category_name: 'Desktop', level: 3, parent_key: null, platform_code: null },
          { category_key: '6', category_name: 'Headphones', level: 3, parent_key: null, platform_code: null },
          { category_key: '7', category_name: 'Smart TVs', level: 3, parent_key: null, platform_code: null },
          { category_key: '8', category_name: 'Laptops', level: 3, parent_key: null, platform_code: null },
          { category_key: '9', category_name: 'Smartwatches', level: 3, parent_key: null, platform_code: null },
          { category_key: '10', category_name: 'Monitors', level: 3, parent_key: null, platform_code: null },
          { category_key: '11', category_name: 'Earphones', level: 3, parent_key: null, platform_code: null },
          { category_key: '12', category_name: 'Speakers', level: 3, parent_key: null, platform_code: null },
          { category_key: '13', category_name: 'Cameras', level: 2, parent_key: null, platform_code: null },
          { category_key: '14', category_name: 'OTHER', level: 1, parent_key: null, platform_code: null },
          { category_key: '15', category_name: 'Tablets', level: 2, parent_key: null, platform_code: null }
        ]);
      }
    };
    loadCategories();
  }, []);

  const validateForm = () => {
    const newErrors: Record<string, string> = {};

    // Validate required fields based on model
    if (modelId === 'price_prediction') {
      if (!formData.scope_mode) {
        newErrors.scope_mode = 'Scope mode is required';
      }
      if (!formData.platform_code) {
        newErrors.platform_code = 'Platform is required';
      }
      if (formData.scope_mode === 'by_product') {
        if (!formData.product_key?.trim()) {
          newErrors.product_key = 'Product is required';
        }
      } else if (formData.scope_mode === 'by_category') {
        if (!formData.category) {
          newErrors.category = 'Category is required';
        }
      }
      if (!formData.from_date) {
        newErrors.from_date = 'Start date is required';
      }
      if (!formData.to_date) {
        newErrors.to_date = 'End date is required';
      }
    } else if (modelId === 'product_recommendation') {
      if (!formData.scope_mode) {
        newErrors.scope_mode = 'Scope mode is required';
      }
      if (formData.scope_mode === 'by_product') {
        if (!formData.product_key?.trim()) {
          newErrors.product_key = 'Source product code is required';
        }
      } else if (formData.scope_mode === 'by_category') {
        if (!formData.platform_code) {
          newErrors.platform_code = 'Platform is required';
        }
        if (!formData.category) {
          newErrors.category = 'Category is required';
        }
      }
    } else if (modelId === 'review_sentiment') {
      if (!formData.scope_mode) {
        newErrors.scope_mode = 'Scope mode is required';
      }
      if (formData.scope_mode === 'by_product') {
        if (!formData.product_key?.trim()) {
          newErrors.product_key = 'Product code is required';
        }
        if (!formData.platform_code) {
          newErrors.platform_code = 'Platform is required';
        }
      } else if (formData.scope_mode === 'by_category') {
        if (!formData.platform_code) {
          newErrors.platform_code = 'Platform is required';
        }
        if (!formData.category) {
          newErrors.category = 'Category is required';
        }
      }
      if (!formData.category) {
        newErrors.category = 'Category is required';
      }
      if (!formData.from_date) {
        newErrors.from_date = 'Start date is required';
      }
      if (!formData.to_date) {
        newErrors.to_date = 'End date is required';
      }
    }

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const models = {
    price_prediction: {
      name: 'Price Prediction',
      icon: <TrendingUp className="w-6 h-6" />,
      description: 'Predict optimal product prices based on market data',
      fields: ['scope_mode', 'platform_code', 'product_key', 'category', 'from_date', 'to_date']
    },
    product_recommendation: {
      name: 'Product Recommendations',
      icon: <Users className="w-6 h-6" />,
      description: 'Personalized product recommendations for customers',
      fields: ['scope_mode', 'product_key', 'platform_code', 'category']
    },
    review_sentiment: {
      name: 'Review Sentiment Analysis',
      icon: <MessageSquare className="w-6 h-6" />,
      description: 'Analyze sentiment in customer reviews',
      fields: ['scope_mode', 'product_key', 'platform_code', 'category', 'from_date', 'to_date']
    },
  };

  const currentModel = models[modelId as keyof typeof models];

  if (!currentModel) {
    return <div>Model not found</div>;
  }

  const categoryOptions = categories.map(category => ({
    value: category.category_key,
    label: category.category_name
  }));

  const scopeModeOptions = [
    { value: 'by_product', label: 'By product (based on source product)' },
    { value: 'by_category', label: 'By category (top products in category)' }
  ];

  const timeRangeOptions = [
    { value: '7d', label: 'Last 7 days' },
    { value: '30d', label: 'Last 30 days' },
    { value: '90d', label: 'Last 90 days' },
    { value: '1y', label: 'Last year' }
  ];

  const handleChange = (field: string, value: any) => {
    setFormData(prev => ({ ...prev, [field]: value }));
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();

    // Validate form before submitting
    if (!validateForm()) {
      return;
    }

    setLoading(true);

    try {
      let dssResponse: any;

      // Step 4: Call DSS API based on model type
      switch (modelId) {
        case 'price_prediction':
          // Determine scope_mode based on user input
          const priceScopeMode = formData.product_key
            ? 'by_product' as const
            : (formData.category ? 'by_category' as const : 'by_category' as const);

          const priceRequest: PricePredictionRequest = {
            from_date: formData.from_date || new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0],
            to_date: formData.to_date || new Date().toISOString().split('T')[0],
            platforms: formData.platform_code ? [formData.platform_code] : undefined,
            categories: formData.category ? [formData.category] : undefined,
            scope_mode: priceScopeMode,
            product_keys: formData.product_key ? [formData.product_key] : undefined,
            page: 1,
            page_size: advancedOptions.top_n,
            top_n: advancedOptions.top_n,
            max_discount_pct: advancedOptions.max_discount_pct / 100, // Convert % to decimal
            min_margin_pct: advancedOptions.min_margin_pct / 100,
            min_confidence: advancedOptions.min_confidence / 100,
            min_price_change_pct: advancedOptions.min_price_change_pct / 100
          };
          dssResponse = await runPricePredictionDSS(priceRequest);
          break;

        case 'product_recommendation':
          const recoRequest: ProductRecommendationRequest = {
            from_date: formData.from_date || new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0],
            to_date: formData.to_date || new Date().toISOString().split('T')[0],
            platforms: formData.platform_code ? [formData.platform_code] : undefined,
            categories: formData.category ? [formData.category] : undefined,
            scope_mode: formData.scope_mode || 'by_product',
            source_product_key: formData.scope_mode === 'by_product' ? formData.product_key : undefined,
            top_k: advancedOptions.top_k,
            min_similarity: advancedOptions.min_similarity / 100, // Convert % to decimal
            min_co_purchase_rate: advancedOptions.min_co_purchase_rate / 100
          };
          dssResponse = await runProductRecommendationDSS(recoRequest);
          break;

        case 'review_sentiment':
          const sentimentRequest: ReviewSentimentRequest = {
            from_date: formData.from_date || new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0],
            to_date: formData.to_date || new Date().toISOString().split('T')[0],
            platforms: formData.platform_code ? [formData.platform_code] : undefined,
            categories: formData.category ? [formData.category] : undefined,
            min_reviews_per_product: advancedOptions.min_reviews_per_product,
            sentiment_focus: advancedOptions.sentiment_focus,
            negative_threshold: advancedOptions.negative_threshold / 100 // Convert % to decimal
          };
          dssResponse = await runReviewSentimentDSS(sentimentRequest);
          break;

        default:
          throw new Error('Unknown model type');
      }

      // Navigate to results with DSS response
      navigate(`/analyst/dss/${modelId}/results`, {
        state: {
          inputData: formData,
          dssResults: dssResponse
        }
      });
    } catch (error) {
      console.error('Error running DSS analysis:', error);
      // For demo purposes, navigate with mock data if API fails
      navigate(`/analyst/dss/${modelId}/results`, { state: { inputData: formData } });
    } finally {
      setLoading(false);
    }
  };

  const CustomDateInput = React.forwardRef<HTMLButtonElement, any>(({ value, onClick, placeholder }, ref) => (
    <button
      type="button"
      onClick={onClick}
      ref={ref}
      className="w-full flex items-center border border-gray-300 rounded-lg px-3 py-2 bg-white hover:border-blue-400 focus:border-blue-500 focus:outline-none transition-colors duration-150 shadow-sm"
      style={{ minHeight: 40 }}
    >
      <span className={`flex-1 text-left ${!value ? 'text-gray-400' : 'text-gray-900'}`}>{value || placeholder}</span>
      <FaRegCalendarAlt className="ml-2 text-blue-500 text-lg" />
    </button>
  ));
  CustomDateInput.displayName = 'CustomDateInput';

  const renderField = (field: string) => {
    switch (field) {
      case 'scope_mode':
        return (
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Scope mode *</label>
            <p className="text-xs text-gray-500 mb-2">Choose how to analyze: by specific product or by category-wide data</p>
            <Select
              options={scopeModeOptions}
              defaultValue={formData.scope_mode || ''}
              onChange={(value) => handleChange('scope_mode', value)}
              placeholder="Select scope mode"
            />
          </div>
        );
      case 'product_key':
        // Only show product_key when scope_mode is by_product for all DSS models that use scope_mode
        if (
          (modelId === 'price_prediction' && formData.scope_mode !== 'by_product') ||
          (modelId === 'product_recommendation' && formData.scope_mode !== 'by_product') ||
          (modelId === 'review_sentiment' && formData.scope_mode !== 'by_product')
        ) {
          return null;
        }
        const isProductKeyRequired = formData.scope_mode === 'by_product';
        return (
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              {modelId === 'product_recommendation' ? 'Source product' : 'Product'}{isProductKeyRequired ? ' *' : ''}
            </label>
            <p className="text-xs text-gray-500 mb-2">
              {modelId === 'product_recommendation'
                ? 'Select the product to find similar or complementary items'
                : 'Search and select a specific product to analyze'}
            </p>
            <ProductSearch
              value={productName}
              onProductSelect={(productKey, productName) => {
                setProductId(productKey);
                setProductName(productName);
                handleChange('product_key', productKey);
              }}
              platformCode={platformCode}
              categoryKey={categoryKey}
              placeholder="Search products..."
            />
          </div>
        );
      case 'platform_code':
        return (
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Platform *</label>
            <p className="text-xs text-gray-500 mb-2">Select e-commerce platform to analyze (Tiki, Lazada, Shopee)</p>
            <PlatformSelect
              value={platformCode}
              onValueChange={(value) => {
                setPlatformCode(value || 'tiki');
                handleChange('platform_code', value || 'tiki');
              }}
            />
          </div>
        );
      case 'category':
        // For models with scope_mode, only show category when by_category is selected
        if (
          (modelId === 'price_prediction' && formData.scope_mode !== 'by_category') ||
          (modelId === 'product_recommendation' && formData.scope_mode !== 'by_category')
        ) {
          return null;
        }
        const isCategoryRequired = formData.scope_mode === 'by_category' || modelId === 'review_sentiment';
        return (
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Category{isCategoryRequired ? ' *' : ''}
            </label>
            <p className="text-xs text-gray-500 mb-2">Select product category to filter analysis scope</p>
            <CategorySelect
              value={categoryKey}
              onValueChange={(value) => {
                setCategoryKey(value || '');
                handleChange('category', value || '');
              }}
              platformCode={platformCode}
            />
          </div>
        );
      case 'time_range':
        return (
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">Time range</label>
            <Select
              options={timeRangeOptions}
              defaultValue={formData.time_range || ''}
              onChange={(value) => handleChange('time_range', value)}
              placeholder="Select time range"
            />
          </div>
        );
      case 'customer_id':
        return (
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">Customer code *</label>
            <Input
              type="text"
              value={formData.customer_id || ''}
              onChange={(e) => handleChange('customer_id', e.target.value)}
              placeholder="vd: KH_001"
              required
            />
          </div>
        );
      case 'review_text':
        return (
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">Review content *</label>
            <textarea
              className="w-full p-2 border border-gray-300 rounded-lg focus:border-blue-500 focus:outline-none"
              rows={4}
              value={formData.review_text || ''}
              onChange={(e) => handleChange('review_text', e.target.value)}
              placeholder="Enter review content to analyze sentiment..."
              required
            />
          </div>
        );
      case 'from_date':
        return (
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">From date *</label>
            <p className="text-xs text-gray-500 mb-2">Start date for historical data analysis</p>
            <DatePicker
              selected={fromDate}
              onChange={(date: Date | null) => {
                setFromDate(date);
                handleChange('from_date', date ? date.toISOString().split('T')[0] : '');
              }}
              dateFormat="dd/MM/yyyy"
              placeholderText="Select start date"
              maxDate={toDate || undefined}
              showMonthDropdown
              showYearDropdown
              dropdownMode="select"
              locale={vi}
              customInput={<CustomDateInput />}
              popperClassName="z-50"
              calendarClassName="rounded-lg shadow-lg border border-gray-200"
              dayClassName={date =>
                'text-sm rounded-full transition-colors duration-100 ' +
                (fromDate && date.toDateString() === fromDate.toDateString() ? 'bg-blue-500 text-white' : 'hover:bg-blue-100')
              }
            />
          </div>
        );
      case 'to_date':
        return (
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">To date *</label>
            <p className="text-xs text-gray-500 mb-2">End date for historical data analysis</p>
            <DatePicker
              selected={toDate}
              onChange={(date: Date | null) => {
                setToDate(date);
                handleChange('to_date', date ? date.toISOString().split('T')[0] : '');
              }}
              dateFormat="dd/MM/yyyy"
              placeholderText="Select end date"
              minDate={fromDate || undefined}
              showMonthDropdown
              showYearDropdown
              dropdownMode="select"
              locale={vi}
              customInput={<CustomDateInput />}
              popperClassName="z-50"
              calendarClassName="rounded-lg shadow-lg border border-gray-200"
              dayClassName={date =>
                'text-sm rounded-full transition-colors duration-100 ' +
                (toDate && date.toDateString() === toDate.toDateString() ? 'bg-blue-500 text-white' : 'hover:bg-blue-100')
              }
            />
          </div>
        );
      default:
        return null;
    }
  };

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
          {/* Main Content */}
          <div className="lg:col-span-2">
            <div className="bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700 p-6">
              <div className="mb-6">
                <button
                  onClick={() => navigate('/analyst/model-dashboard')}
                  className="flex items-center text-blue-600 hover:text-blue-800 mb-4"
                >
                  <ArrowLeft className="w-4 h-4 mr-2" />
                  Back to Model Dashboard
                </button>
                <div className="flex items-center mb-4">
                  <div className="p-3 bg-blue-100 dark:bg-blue-900/20 rounded-lg mr-4">
                    {currentModel.icon}
                  </div>
                  <div>
                    <h1 className="text-3xl font-bold text-gray-900 dark:text-white">
                      {currentModel.name} - DSS Input
                    </h1>
                    <p className="text-gray-600 dark:text-gray-300">
                      {currentModel.description}
                    </p>
                  </div>
                </div>
              </div>

              <Form onSubmit={handleSubmit}>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
                  {currentModel.fields.map(field => (
                    <div key={field} className={field === 'review_text' ? 'md:col-span-2' : ''}>
                      {renderField(field)}
                    </div>
                  ))}
                </div>

                {/* Advanced Options - Collapsible */}
                <div className="mb-6 border border-gray-200 dark:border-gray-700 rounded-lg">
                  <button
                    type="button"
                    onClick={() => setShowAdvanced(!showAdvanced)}
                    className="w-full flex items-center justify-between p-4 text-left bg-gray-50 dark:bg-gray-700/50 rounded-lg hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors"
                  >
                    <div className="flex items-center gap-2">
                      <Settings className="w-4 h-4 text-gray-500" />
                      <span className="font-medium text-gray-700 dark:text-gray-300">Advanced Options</span>
                      <span className="text-xs text-gray-500">(Optional)</span>
                    </div>
                    {showAdvanced ? <ChevronUp className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}
                  </button>

                  {showAdvanced && (
                    <div className="p-4 border-t border-gray-200 dark:border-gray-700">
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        {/* Price Prediction Advanced Options */}
                        {modelId === 'price_prediction' && (
                          <>
                            <div>
                              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                                Max Discount % <span className="text-gray-400">({advancedOptions.max_discount_pct}%)</span>
                              </label>
                              <p className="text-xs text-gray-500 mb-2">Maximum price reduction allowed in recommendations</p>
                              <input
                                type="range"
                                min="0"
                                max="50"
                                value={advancedOptions.max_discount_pct}
                                onChange={(e) => setAdvancedOptions(prev => ({ ...prev, max_discount_pct: Number(e.target.value) }))}
                                className="w-full h-2 bg-gray-200 rounded-lg appearance-none cursor-pointer accent-blue-600"
                              />
                            </div>
                            <div>
                              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                                Min Margin % <span className="text-gray-400">({advancedOptions.min_margin_pct}%)</span>
                              </label>
                              <p className="text-xs text-gray-500 mb-2">Minimum profit margin to maintain</p>
                              <input
                                type="range"
                                min="0"
                                max="50"
                                value={advancedOptions.min_margin_pct}
                                onChange={(e) => setAdvancedOptions(prev => ({ ...prev, min_margin_pct: Number(e.target.value) }))}
                                className="w-full h-2 bg-gray-200 rounded-lg appearance-none cursor-pointer accent-blue-600"
                              />
                            </div>
                            <div>
                              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                                Min Confidence <span className="text-gray-400">({advancedOptions.min_confidence}%)</span>
                              </label>
                              <p className="text-xs text-gray-500 mb-2">Only show predictions with this confidence or higher</p>
                              <input
                                type="range"
                                min="0"
                                max="100"
                                value={advancedOptions.min_confidence}
                                onChange={(e) => setAdvancedOptions(prev => ({ ...prev, min_confidence: Number(e.target.value) }))}
                                className="w-full h-2 bg-gray-200 rounded-lg appearance-none cursor-pointer accent-blue-600"
                              />
                            </div>
                            <div>
                              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                                Top N Products
                              </label>
                              <p className="text-xs text-gray-500 mb-2">Maximum number of products to analyze</p>
                              <input
                                type="number"
                                min="1"
                                max="100"
                                value={advancedOptions.top_n}
                                onChange={(e) => setAdvancedOptions(prev => ({ ...prev, top_n: Number(e.target.value) }))}
                                className="w-full border border-gray-300 rounded-lg px-3 py-2 focus:border-blue-500 focus:outline-none"
                              />
                            </div>
                          </>
                        )}

                        {/* Product Recommendation Advanced Options */}
                        {modelId === 'product_recommendation' && (
                          <>
                            <div>
                              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                                Top K Recommendations
                              </label>
                              <p className="text-xs text-gray-500 mb-2">Number of similar products to return</p>
                              <input
                                type="number"
                                min="1"
                                max="50"
                                value={advancedOptions.top_k}
                                onChange={(e) => setAdvancedOptions(prev => ({ ...prev, top_k: Number(e.target.value) }))}
                                className="w-full border border-gray-300 rounded-lg px-3 py-2 focus:border-blue-500 focus:outline-none"
                              />
                            </div>
                            <div>
                              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                                Min Similarity <span className="text-gray-400">({advancedOptions.min_similarity}%)</span>
                              </label>
                              <p className="text-xs text-gray-500 mb-2">Minimum product similarity score to include</p>
                              <input
                                type="range"
                                min="0"
                                max="100"
                                value={advancedOptions.min_similarity}
                                onChange={(e) => setAdvancedOptions(prev => ({ ...prev, min_similarity: Number(e.target.value) }))}
                                className="w-full h-2 bg-gray-200 rounded-lg appearance-none cursor-pointer accent-green-600"
                              />
                            </div>
                            <div>
                              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                                Min Co-purchase Rate <span className="text-gray-400">({advancedOptions.min_co_purchase_rate}%)</span>
                              </label>
                              <p className="text-xs text-gray-500 mb-2">Minimum rate of customers buying both products</p>
                              <input
                                type="range"
                                min="0"
                                max="50"
                                value={advancedOptions.min_co_purchase_rate}
                                onChange={(e) => setAdvancedOptions(prev => ({ ...prev, min_co_purchase_rate: Number(e.target.value) }))}
                                className="w-full h-2 bg-gray-200 rounded-lg appearance-none cursor-pointer accent-green-600"
                              />
                            </div>
                          </>
                        )}

                        {/* Review Sentiment Advanced Options */}
                        {modelId === 'review_sentiment' && (
                          <>
                            <div>
                              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                                Min Reviews per Product
                              </label>
                              <p className="text-xs text-gray-500 mb-2">Products with fewer reviews will be excluded</p>
                              <input
                                type="number"
                                min="1"
                                max="100"
                                value={advancedOptions.min_reviews_per_product}
                                onChange={(e) => setAdvancedOptions(prev => ({ ...prev, min_reviews_per_product: Number(e.target.value) }))}
                                className="w-full border border-gray-300 rounded-lg px-3 py-2 focus:border-blue-500 focus:outline-none"
                              />
                            </div>
                            <div>
                              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                                Sentiment Focus
                              </label>
                              <p className="text-xs text-gray-500 mb-2">Filter to show only specific sentiment types</p>
                              <select
                                value={advancedOptions.sentiment_focus}
                                onChange={(e) => setAdvancedOptions(prev => ({ ...prev, sentiment_focus: e.target.value as any }))}
                                className="w-full border border-gray-300 rounded-lg px-3 py-2 focus:border-blue-500 focus:outline-none"
                              >
                                <option value="all">All Sentiments</option>
                                <option value="only_positive">Only Positive</option>
                                <option value="only_negative">Only Negative</option>
                              </select>
                            </div>
                            <div>
                              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                                Negative Threshold <span className="text-gray-400">({advancedOptions.negative_threshold}%)</span>
                              </label>
                              <input
                                type="range"
                                min="0"
                                max="100"
                                value={advancedOptions.negative_threshold}
                                onChange={(e) => setAdvancedOptions(prev => ({ ...prev, negative_threshold: Number(e.target.value) }))}
                                className="w-full h-2 bg-gray-200 rounded-lg appearance-none cursor-pointer accent-purple-600"
                              />
                              <p className="text-xs text-gray-500 mt-1">Products above this % negative reviews are flagged as critical</p>
                            </div>
                          </>
                        )}
                      </div>
                    </div>
                  )}
                </div>

                <Button disabled={loading} className="w-full">
                  {loading ? (
                    <>
                      <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white mr-2"></div>
                      Running DSS...
                    </>
                  ) : (
                    <>
                      <Send className="w-4 h-4 mr-2" />
                      Run DSS Analysis
                    </>
                  )}
                </Button>
              </Form>
            </div>
          </div>

          {/* Sidebar */}
          <div className="lg:col-span-1">
            <div className="bg-white dark:bg-gray-800 rounded-lg shadow-sm border border-gray-200 dark:border-gray-700 p-6">
              <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
                Model Information
              </h3>
              <div className="space-y-4">
                <div>
                  <h4 className="font-medium text-gray-700 dark:text-gray-300">Model Type</h4>
                  <p className="text-sm text-gray-600 dark:text-gray-400">{currentModel.name}</p>
                </div>
                <div>
                  <h4 className="font-medium text-gray-700 dark:text-gray-300">Description</h4>
                  <p className="text-sm text-gray-600 dark:text-gray-400">{currentModel.description}</p>
                </div>
                <div>
                  <h4 className="font-medium text-gray-700 dark:text-gray-300">Required Fields</h4>
                  <ul className="text-sm text-gray-600 dark:text-gray-400 space-y-1">
                    {(modelId === 'price_prediction' || modelId === 'product_recommendation' || modelId === 'review_sentiment') ? (
                      <>
                        <li className="flex items-center">
                          <span className="w-2 h-2 bg-blue-500 rounded-full mr-2"></span>
                          Scope Mode
                        </li>
                        {formData.scope_mode === 'by_product' && (
                          <>
                            <li className="flex items-center">
                              <span className="w-2 h-2 bg-blue-500 rounded-full mr-2"></span>
                              Product Key (Source)
                            </li>
                            <li className="flex items-center">
                              <span className="w-2 h-2 bg-blue-500 rounded-full mr-2"></span>
                              Platform
                            </li>
                          </>
                        )}
                        {formData.scope_mode === 'by_category' && (
                          <>
                            <li className="flex items-center">
                              <span className="w-2 h-2 bg-blue-500 rounded-full mr-2"></span>
                              Platform
                            </li>
                            <li className="flex items-center">
                              <span className="w-2 h-2 bg-blue-500 rounded-full mr-2"></span>
                              Category
                            </li>
                          </>
                        )}
                        {modelId === 'review_sentiment' && (
                          <li className="flex items-center">
                            <span className="w-2 h-2 bg-blue-500 rounded-full mr-2"></span>
                            Category
                          </li>
                        )}
                        <li className="flex items-center">
                          <span className="w-2 h-2 bg-blue-500 rounded-full mr-2"></span>
                          From Date
                        </li>
                        <li className="flex items-center">
                          <span className="w-2 h-2 bg-blue-500 rounded-full mr-2"></span>
                          To Date
                        </li>
                      </>
                    ) : (
                      currentModel.fields.filter(field => field !== 'category' && field !== 'time_range').map(field => (
                        <li key={field} className="flex items-center">
                          <span className="w-2 h-2 bg-blue-500 rounded-full mr-2"></span>
                          {field.replace('_', ' ').replace(/\b\w/g, l => l.toUpperCase())}
                        </li>
                      ))
                    )}
                  </ul>
                </div>
                <div>
                  <h4 className="font-medium text-gray-700 dark:text-gray-300">Optional Fields</h4>
                  <ul className="text-sm text-gray-600 dark:text-gray-400 space-y-1">
                    {currentModel.fields.filter(field => field === 'category' || field === 'time_range').map(field => (
                      <li key={field} className="flex items-center">
                        <span className="w-2 h-2 bg-gray-400 rounded-full mr-2"></span>
                        {field.replace('_', ' ').replace(/\b\w/g, l => l.toUpperCase())}
                      </li>
                    ))}
                  </ul>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default DSSInput;