import React, { useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { ArrowLeft, Send, TrendingUp, Users, MessageSquare } from 'lucide-react';
import Button from '../../components/ui/button/Button';
import Form from '../../components/form/Form';
import Input from '../../components/form/input/InputField';
import Select from '../../components/form/Select';
import DatePicker from 'react-datepicker';
import { runDSSAnalysis, getAISummary, DSSRunRequest, AISummarizeRequest } from '../../services/mockDSSApi';

interface DSSInputData {
  product_key?: string;
  platform_code?: string;
  category?: string;
  time_range?: string;
  customer_id?: string;
  review_text?: string;
  from_date?: string;
  to_date?: string;
}

const DSSInput: React.FC = () => {
  const { modelId } = useParams<{ modelId: string }>();
  const navigate = useNavigate();
  const [loading, setLoading] = useState(false);
  const [formData, setFormData] = useState<DSSInputData>({});
  const [fromDate, setFromDate] = useState<Date | null>(null);
  const [toDate, setToDate] = useState<Date | null>(null);

  const models = {
    price_prediction: {
      name: 'Price Prediction',
      icon: <TrendingUp className="w-6 h-6" />,
      description: 'Predict optimal pricing for products',
      fields: ['product_key', 'platform_code', 'category', 'time_range']
    },
    product_recommendation: {
      name: 'Product Recommendation',
      icon: <Users className="w-6 h-6" />,
      description: 'Recommend products to customers',
      fields: ['customer_id', 'platform_code', 'category']
    },
    review_sentiment: {
      name: 'Review Sentiment Analysis',
      icon: <MessageSquare className="w-6 h-6" />,
      description: 'Analyze customer review sentiment',
      fields: ['product_key', 'platform_code', 'from_date', 'to_date']
    }
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

  const categoryOptions = [
    { value: 'electronics', label: 'Electronics' },
    { value: 'fashion', label: 'Fashion' },
    { value: 'home', label: 'Home & Garden' },
    { value: 'sports', label: 'Sports' }
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
    setLoading(true);

    try {
      // Step 4: Call DSS API
      const dssRequest: DSSRunRequest = {
        model_type: modelId as any,
        input_data: formData
      };

      const dssResponse = await runDSSAnalysis(dssRequest);

      // Step 5: Call AI Summary API
      const aiRequest: AISummarizeRequest = {
        model_type: modelId,
        ml_results: dssResponse,
        business_context: {
          platform: formData.platform_code,
          product_key: formData.product_key || formData.source_product_key
        }
      };

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
      case 'product_key':
        return (
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">Product Key *</label>
            <Input
              type="text"
              value={formData.product_key || ''}
              onChange={(e) => handleChange('product_key', e.target.value)}
              placeholder="e.g., tiki_123456"
              required
            />
          </div>
        );
      case 'platform_code':
        return (
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">Platform *</label>
            <Select
              options={platformOptions}
              defaultValue={formData.platform_code || ''}
              onChange={(value) => handleChange('platform_code', value)}
              placeholder="Select platform"
            />
          </div>
        );
      case 'category':
        return (
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">Category</label>
            <Select
              options={categoryOptions}
              defaultValue={formData.category || ''}
              onChange={(value) => handleChange('category', value)}
              placeholder="Select category"
            />
          </div>
        );
      case 'time_range':
        return (
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">Time Range</label>
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
            <label className="block text-sm font-medium text-gray-700 mb-2">Customer ID *</label>
            <Input
              type="text"
              value={formData.customer_id || ''}
              onChange={(e) => handleChange('customer_id', e.target.value)}
              placeholder="e.g., CUST_001"
              required
            />
          </div>
        );
      case 'review_text':
        return (
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">Review Text *</label>
            <textarea
              className="w-full p-2 border border-gray-300 rounded-lg focus:border-blue-500 focus:outline-none"
              rows={4}
              value={formData.review_text || ''}
              onChange={(e) => handleChange('review_text', e.target.value)}
              placeholder="Enter review text for sentiment analysis..."
              required
            />
          </div>
        );
      case 'from_date':
        return (
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">From Date *</label>
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
            <label className="block text-sm font-medium text-gray-700 mb-2">To Date *</label>
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
    <div className="p-6">
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

      <div className="max-w-2xl">
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
  );
};

export default DSSInput;