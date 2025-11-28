import React, { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { ArrowLeft, Send, TrendingUp, Users, MessageSquare } from 'lucide-react';
import Button from '../../components/ui/button/Button';
import Form from '../../components/form/Form';
import Input from '../../components/form/input/InputField';
import Select from '../../components/form/Select';
import DatePicker from 'react-datepicker';
import { FaRegCalendarAlt } from 'react-icons/fa';
import { vi } from 'date-fns/locale';
import {
  runPricePredictionDSS,
  runProductRecommendationDSS,
  runReviewSentimentDSS,
  getAISummary,
  PricePredictionRequest,
  ProductRecommendationRequest,
  ReviewSentimentRequest,
  AISummarizeRequest
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

const DSSInput: React.FC = () => {
  const { modelId } = useParams<{ modelId: string }>();
  const navigate = useNavigate();
  const [loading, setLoading] = useState(false);
  const [formData, setFormData] = useState<DSSInputData>({});
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [categories, setCategories] = useState<Category[]>([]);
  const [fromDate, setFromDate] = useState<Date | null>(null);
  const [toDate, setToDate] = useState<Date | null>(null);

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
      if (!formData.product_key?.trim()) {
        newErrors.product_key = 'Mã sản phẩm là bắt buộc';
      }
      if (!formData.platform_code) {
        newErrors.platform_code = 'Nền tảng là bắt buộc';
      }
    } else if (modelId === 'product_recommendation') {
      if (!formData.scope_mode) {
        newErrors.scope_mode = 'Chế độ phạm vi là bắt buộc';
      }
      if (formData.scope_mode === 'by_product') {
        if (!formData.product_key?.trim()) {
          newErrors.product_key = 'Mã sản phẩm nguồn là bắt buộc';
        }
      } else if (formData.scope_mode === 'by_category') {
        if (!formData.platform_code) {
          newErrors.platform_code = 'Nền tảng là bắt buộc';
        }
        if (!formData.category) {
          newErrors.category = 'Danh mục là bắt buộc';
        }
      }
    } else if (modelId === 'review_sentiment') {
      if (!formData.scope_mode) {
        newErrors.scope_mode = 'Chế độ phạm vi là bắt buộc';
      }
      if (formData.scope_mode === 'by_product') {
        if (!formData.product_key?.trim()) {
          newErrors.product_key = 'Mã sản phẩm là bắt buộc';
        }
        if (!formData.platform_code) {
          newErrors.platform_code = 'Nền tảng là bắt buộc';
        }
      } else if (formData.scope_mode === 'by_category') {
        if (!formData.platform_code) {
          newErrors.platform_code = 'Nền tảng là bắt buộc';
        }
        if (!formData.category) {
          newErrors.category = 'Danh mục là bắt buộc';
        }
      }
      if (!formData.from_date) {
        newErrors.from_date = 'Ngày bắt đầu là bắt buộc';
      }
      if (!formData.to_date) {
        newErrors.to_date = 'Ngày kết thúc là bắt buộc';
      }
    }

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const models = {
    price_prediction: {
      name: 'Dự đoán Giá',
      icon: <TrendingUp className="w-6 h-6" />,
      description: 'Dự đoán giá tối ưu cho sản phẩm dựa trên dữ liệu thị trường',
      fields: ['product_key', 'platform_code', 'category', 'time_range']
    },
    product_recommendation: {
      name: 'Gợi ý Sản phẩm',
      icon: <Users className="w-6 h-6" />,
      description: 'Gợi ý sản phẩm cá nhân hóa cho khách hàng',
      fields: ['scope_mode', 'product_key', 'platform_code', 'category']
    },
    review_sentiment: {
      name: 'Phân tích Cảm xúc Đánh giá',
      icon: <MessageSquare className="w-6 h-6" />,
      description: 'Phân tích cảm xúc trong đánh giá của khách hàng',
      fields: ['scope_mode', 'product_key', 'platform_code', 'category', 'from_date', 'to_date']
    },
  };

  const currentModel = models[modelId as keyof typeof models];

  if (!currentModel) {
    return <div>Model not found</div>;
  }

  const platformOptions = [
    { value: 'tiki', label: 'Tiki' },
    { value: 'lazada', label: 'Lazada' },
    { value: 'shopee', label: 'Shopee' }
  ];

  const categoryOptions = categories.map(category => ({
    value: category.category_key,
    label: category.category_name
  }));

  const scopeModeOptions = [
    { value: 'by_product', label: 'Theo sản phẩm (dựa trên sản phẩm nguồn)' },
    { value: 'by_category', label: 'Theo danh mục (top sản phẩm trong danh mục)' }
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
      let aiRequest: AISummarizeRequest;

      // Step 4: Call DSS API based on model type
      switch (modelId) {
        case 'price_prediction':
          const priceRequest: PricePredictionRequest = {
            from_date: formData.from_date || '2025-01-01',
            to_date: formData.to_date || '2025-12-31',
            platforms: formData.platform_code ? [formData.platform_code] : undefined,
            categories: formData.category ? [formData.category] : undefined,
            scope_mode: 'specific_products',
            product_keys: formData.product_key ? [formData.product_key] : undefined,
            max_discount_pct: 0.2, // 20% as decimal
            min_margin_pct: 0.1,  // 10% as decimal
            min_confidence: 0.8,
            min_price_change_pct: 0.05 // 5% as decimal
          };
          dssResponse = await runPricePredictionDSS(priceRequest);
          aiRequest = {
            model_type: 'price_prediction',
            ml_results: dssResponse,
            business_context: {
              platform: formData.platform_code,
              product_key: formData.product_key
            }
          };
          break;

        case 'product_recommendation':
          const recoRequest: ProductRecommendationRequest = {
            from_date: formData.from_date || '2025-11-28',
            to_date: formData.to_date || '2025-11-28',
            platforms: formData.platform_code ? [formData.platform_code] : undefined,
            categories: formData.category ? [formData.category] : undefined,
            scope_mode: formData.scope_mode || 'by_product',
            source_product_key: formData.scope_mode === 'by_product' ? formData.product_key : undefined,
            top_k: 10,
            min_similarity: 0.5,
            min_co_purchase_rate: 0.05
          };
          dssResponse = await runProductRecommendationDSS(recoRequest);
          aiRequest = {
            model_type: 'product_recommendation',
            ml_results: dssResponse,
            business_context: {
              platform: formData.platform_code,
              product_key: formData.product_key
            }
          };
          break;

        case 'review_sentiment':
          const sentimentRequest: ReviewSentimentRequest = {
            from_date: formData.from_date || '2025-01-01',
            to_date: formData.to_date || '2025-12-31',
            platforms: formData.platform_code ? [formData.platform_code] : undefined,
            categories: formData.scope_mode === 'by_category' ? (formData.category ? [formData.category] : undefined) : undefined,
            min_reviews_per_product: 10,
            sentiment_focus: formData.scope_mode === 'by_category' ? 'only_negative' : 'all',
            negative_threshold: 0.3
          };
          dssResponse = await runReviewSentimentDSS(sentimentRequest);
          aiRequest = {
            model_type: 'review_sentiment',
            ml_results: dssResponse,
            business_context: {
              platform: formData.platform_code,
              product_key: formData.product_key,
              scope_mode: formData.scope_mode
            }
          };
          break;

        default:
          throw new Error('Unknown model type');
      }

      // Step 5: Call AI Summary API
      const aiResponse = await getAISummary(aiRequest);

      // Navigate to results with both responses
      navigate(`/analyst/dss/${modelId}/results`, {
        state: {
          inputData: formData,
          dssResults: dssResponse,
          aiSummary: aiResponse
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
            <label className="block text-sm font-medium text-gray-700 mb-2">Chế độ phạm vi *</label>
            <Select
              options={scopeModeOptions}
              defaultValue={formData.scope_mode || ''}
              onChange={(value) => handleChange('scope_mode', value)}
              placeholder="Chọn chế độ phạm vi"
            />
          </div>
        );
      case 'product_key':
        // Only show product_key field for product_recommendation when scope_mode is by_product
        // or for review_sentiment when scope_mode is by_product
        if ((modelId === 'product_recommendation' && formData.scope_mode !== 'by_product') ||
            (modelId === 'review_sentiment' && formData.scope_mode !== 'by_product')) {
          return null;
        }
        return (
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              {modelId === 'product_recommendation' ? 'Mã sản phẩm nguồn *' : 'Mã sản phẩm *'}
            </label>
            <Input
              type="text"
              value={formData.product_key || ''}
              onChange={(e) => handleChange('product_key', e.target.value)}
              placeholder={modelId === 'product_recommendation' ? "vd: tiki_123456" : "vd: tiki_123456"}
              required
            />
          </div>
        );
      case 'platform_code':
        return (
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">Nền tảng *</label>
            <Select
              options={platformOptions}
              defaultValue={formData.platform_code || ''}
              onChange={(value) => handleChange('platform_code', value)}
              placeholder="Chọn nền tảng"
            />
          </div>
        );
      case 'category':
        return (
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Danh mục{((modelId === 'product_recommendation' || modelId === 'review_sentiment') && formData.scope_mode === 'by_category') ? ' *' : ''}
            </label>
            <Select
              options={categoryOptions}
              defaultValue={formData.category || ''}
              onChange={(value) => handleChange('category', value)}
              placeholder="Chọn danh mục"
            />
          </div>
        );
      case 'time_range':
        return (
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">Khoảng thời gian</label>
            <Select
              options={timeRangeOptions}
              defaultValue={formData.time_range || ''}
              onChange={(value) => handleChange('time_range', value)}
              placeholder="Chọn khoảng thời gian"
            />
          </div>
        );
      case 'customer_id':
        return (
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">Mã khách hàng *</label>
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
            <label className="block text-sm font-medium text-gray-700 mb-2">Nội dung đánh giá *</label>
            <textarea
              className="w-full p-2 border border-gray-300 rounded-lg focus:border-blue-500 focus:outline-none"
              rows={4}
              value={formData.review_text || ''}
              onChange={(e) => handleChange('review_text', e.target.value)}
              placeholder="Nhập nội dung đánh giá để phân tích cảm xúc..."
              required
            />
          </div>
        );
      case 'from_date':
        return (
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">Từ ngày *</label>
            <DatePicker
              selected={fromDate}
              onChange={(date: Date | null) => {
                setFromDate(date);
                handleChange('from_date', date ? date.toISOString().split('T')[0] : '');
              }}
              dateFormat="dd/MM/yyyy"
              placeholderText="Chọn ngày bắt đầu"
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
            <label className="block text-sm font-medium text-gray-700 mb-2">Đến ngày *</label>
            <DatePicker
              selected={toDate}
              onChange={(date: Date | null) => {
                setToDate(date);
                handleChange('to_date', date ? date.toISOString().split('T')[0] : '');
              }}
              dateFormat="dd/MM/yyyy"
              placeholderText="Chọn ngày kết thúc"
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
                    {(modelId === 'product_recommendation' || modelId === 'review_sentiment') ? (
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