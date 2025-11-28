import React, { useState } from 'react';
import { Download, FileText, BarChart3, MessageSquare, Package, Layers } from 'lucide-react';
import Button from '../../components/ui/button/Button';
import DatePicker from 'react-datepicker';
import { FaRegCalendarAlt } from 'react-icons/fa';
import { vi } from 'date-fns/locale';
import Select from '../../components/form/Select';
import {
  exportOverviewReport,
  exportProductsReport,
  exportReviewsReport,
  exportReviewsDetailsReport,
  exportProductReviewsDetails,
  exportProductsByCategory,
  exportProductsByCategoryAllPlatforms
} from '../../services/reportApi';

interface ReportFormData {
  from_date: Date | null;
  to_date: Date | null;
  platform_code?: string;
  metric?: string;
  min_reviews?: number;
  limit?: number;
  product_id?: string;
  category_id?: string;
}

const ReportsPage: React.FC = () => {
  const [activeTab, setActiveTab] = useState('overview');
  const [loading, setLoading] = useState(false);
  const [formData, setFormData] = useState<ReportFormData>({
    from_date: null,
    to_date: null,
    limit: 100
  });

  const platformOptions = [
    { value: 'tiki', label: 'Tiki' },
    { value: 'lazada', label: 'Lazada' },
    { value: 'shopee', label: 'Shopee' }
  ];

  const metricOptions = [
    { value: 'revenue', label: 'Revenue' },
    { value: 'reviews', label: 'Reviews' },
    { value: 'rating', label: 'Rating' },
    { value: 'price', label: 'Price' }
  ];

  const tabs = [
    { id: 'overview', name: 'Overview Report', icon: <BarChart3 className="w-5 h-5" />, description: 'General business overview and KPIs' },
    { id: 'products', name: 'Products Report', icon: <Package className="w-5 h-5" />, description: 'Top products analysis and metrics' },
    { id: 'reviews', name: 'Reviews Report', icon: <MessageSquare className="w-5 h-5" />, description: 'Reviews and sentiment analysis' },
    { id: 'reviews-details', name: 'Reviews Details', icon: <FileText className="w-5 h-5" />, description: 'Detailed reviews data' },
    { id: 'product-reviews', name: 'Product Reviews', icon: <MessageSquare className="w-5 h-5" />, description: 'Reviews for specific product' },
    { id: 'products-category', name: 'Products by Category', icon: <Layers className="w-5 h-5" />, description: 'Products grouped by category' }
  ];

  const handleInputChange = (field: string, value: any) => {
    setFormData(prev => ({ ...prev, [field]: value }));
  };

  const downloadFile = (blob: Blob, filename: string) => {
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    window.URL.revokeObjectURL(url);
    document.body.removeChild(a);
  };

  const handleExport = async () => {
    if (!formData.from_date || !formData.to_date) {
      alert('Please select both from and to dates');
      return;
    }

    setLoading(true);
    try {
      let blob: Blob;
      let filename: string;

      const baseParams = {
        from_date: formData.from_date.toISOString().split('T')[0],
        to_date: formData.to_date.toISOString().split('T')[0],
        platform_code: formData.platform_code,
        limit: formData.limit
      };

      switch (activeTab) {
        case 'overview':
          blob = await exportOverviewReport(baseParams);
          filename = `overview-report-${baseParams.from_date}-to-${baseParams.to_date}.csv`;
          break;

        case 'products':
          blob = await exportProductsReport({
            ...baseParams,
            metric: formData.metric as any
          });
          filename = `products-report-${baseParams.from_date}-to-${baseParams.to_date}.csv`;
          break;

        case 'reviews':
          blob = await exportReviewsReport({
            ...baseParams,
            min_reviews: formData.min_reviews
          });
          filename = `reviews-report-${baseParams.from_date}-to-${baseParams.to_date}.csv`;
          break;

        case 'reviews-details':
          blob = await exportReviewsDetailsReport(baseParams);
          filename = `reviews-details-${baseParams.from_date}-to-${baseParams.to_date}.csv`;
          break;

        case 'product-reviews':
          if (!formData.product_id) {
            alert('Please enter Product ID');
            return;
          }
          blob = await exportProductReviewsDetails({
            product_id: formData.product_id,
            ...baseParams
          });
          filename = `product-reviews-${formData.product_id}-${baseParams.from_date}-to-${baseParams.to_date}.csv`;
          break;

        case 'products-category':
          if (!formData.platform_code) {
            alert('Please select a platform');
            return;
          }
          blob = await exportProductsByCategory({
            platform_code: formData.platform_code,
            from_date: baseParams.from_date,
            to_date: baseParams.to_date,
            category_id: formData.category_id,
            limit: formData.limit
          });
          filename = `products-by-category-${formData.platform_code}-${baseParams.from_date}-to-${baseParams.to_date}.csv`;
          break;

        default:
          throw new Error('Unknown report type');
      }

      downloadFile(blob, filename);
    } catch (error) {
      console.error('Export failed:', error);
      alert('Export failed. Please try again.');
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

  const renderFormFields = () => {
    const commonFields = (
      <>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">From Date *</label>
            <DatePicker
              selected={formData.from_date}
              onChange={(date: Date | null) => handleInputChange('from_date', date)}
              dateFormat="dd/MM/yyyy"
              placeholderText="Select start date"
              maxDate={formData.to_date || undefined}
              showMonthDropdown
              showYearDropdown
              dropdownMode="select"
              locale={vi}
              customInput={<CustomDateInput />}
              popperClassName="z-50"
              calendarClassName="rounded-lg shadow-lg border border-gray-200"
              dayClassName={date =>
                'text-sm rounded-full transition-colors duration-100 ' +
                (formData.from_date && date.toDateString() === formData.from_date.toDateString() ? 'bg-blue-500 text-white' : 'hover:bg-blue-100')
              }
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">To Date *</label>
            <DatePicker
              selected={formData.to_date}
              onChange={(date: Date | null) => handleInputChange('to_date', date)}
              dateFormat="dd/MM/yyyy"
              placeholderText="Select end date"
              minDate={formData.from_date || undefined}
              showMonthDropdown
              showYearDropdown
              dropdownMode="select"
              locale={vi}
              customInput={<CustomDateInput />}
              popperClassName="z-50"
              calendarClassName="rounded-lg shadow-lg border border-gray-200"
              dayClassName={date =>
                'text-sm rounded-full transition-colors duration-100 ' +
                (formData.to_date && date.toDateString() === formData.to_date.toDateString() ? 'bg-blue-500 text-white' : 'hover:bg-blue-100')
              }
            />
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mt-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">Platform</label>
            <Select
              options={platformOptions}
              defaultValue={formData.platform_code || ''}
              onChange={(value) => handleInputChange('platform_code', value)}
              placeholder="All Platforms"
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">Limit</label>
            <input
              type="number"
              value={formData.limit || 100}
              onChange={(e) => handleInputChange('limit', parseInt(e.target.value))}
              className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:border-blue-500 focus:outline-none"
              min="1"
              max="50000"
            />
          </div>
          <div></div>
        </div>
      </>
    );

    switch (activeTab) {
      case 'products':
        return (
          <>
            {commonFields}
            <div className="mt-4">
              <label className="block text-sm font-medium text-gray-700 mb-2">Metric</label>
              <Select
                options={metricOptions}
                defaultValue={formData.metric || ''}
                onChange={(value) => handleInputChange('metric', value)}
                placeholder="Select metric"
              />
            </div>
          </>
        );

      case 'reviews':
        return (
          <>
            {commonFields}
            <div className="mt-4">
              <label className="block text-sm font-medium text-gray-700 mb-2">Min Reviews</label>
              <input
                type="number"
                value={formData.min_reviews || 0}
                onChange={(e) => handleInputChange('min_reviews', parseInt(e.target.value))}
                className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:border-blue-500 focus:outline-none"
                min="0"
              />
            </div>
          </>
        );

      case 'product-reviews':
        return (
          <>
            {commonFields}
            <div className="mt-4">
              <label className="block text-sm font-medium text-gray-700 mb-2">Product ID *</label>
              <input
                type="text"
                value={formData.product_id || ''}
                onChange={(e) => handleInputChange('product_id', e.target.value)}
                className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:border-blue-500 focus:outline-none"
                placeholder="e.g., tiki_123456"
              />
            </div>
          </>
        );

      case 'products-category':
        return (
          <>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">From Date *</label>
                <DatePicker
                  selected={formData.from_date}
                  onChange={(date: Date | null) => handleInputChange('from_date', date)}
                  dateFormat="dd/MM/yyyy"
                  placeholderText="Select start date"
                  maxDate={formData.to_date || undefined}
                  showMonthDropdown
                  showYearDropdown
                  dropdownMode="select"
                  locale={vi}
                  customInput={<CustomDateInput />}
                  popperClassName="z-50"
                  calendarClassName="rounded-lg shadow-lg border border-gray-200"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">To Date *</label>
                <DatePicker
                  selected={formData.to_date}
                  onChange={(date: Date | null) => handleInputChange('to_date', date)}
                  dateFormat="dd/MM/yyyy"
                  placeholderText="Select end date"
                  minDate={formData.from_date || undefined}
                  showMonthDropdown
                  showYearDropdown
                  dropdownMode="select"
                  locale={vi}
                  customInput={<CustomDateInput />}
                  popperClassName="z-50"
                  calendarClassName="rounded-lg shadow-lg border border-gray-200"
                />
              </div>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mt-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Platform *</label>
                <Select
                  options={platformOptions}
                  defaultValue={formData.platform_code || ''}
                  onChange={(value) => handleInputChange('platform_code', value)}
                  placeholder="Select platform"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Category ID</label>
                <input
                  type="text"
                  value={formData.category_id || ''}
                  onChange={(e) => handleInputChange('category_id', e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:border-blue-500 focus:outline-none"
                  placeholder="Optional category filter"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Limit</label>
                <input
                  type="number"
                  value={formData.limit || 100}
                  onChange={(e) => handleInputChange('limit', parseInt(e.target.value))}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:border-blue-500 focus:outline-none"
                  min="1"
                  max="10000"
                />
              </div>
            </div>
          </>
        );

      default:
        return commonFields;
    }
  };

  return (
    <div className="p-6">
      <div className="mb-8">
        <h1 className="text-3xl font-bold text-gray-900 dark:text-white mb-2">
          Reports & Exports
        </h1>
        <p className="text-gray-600 dark:text-gray-300">
          Export comprehensive business reports and analytics data
        </p>
      </div>

      {/* Report Type Tabs */}
      <div className="mb-6">
        <div className="border-b border-gray-200 dark:border-gray-700">
          <nav className="-mb-px flex space-x-8">
            {tabs.map((tab) => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`py-4 px-1 border-b-2 font-medium text-sm flex items-center gap-2 ${
                  activeTab === tab.id
                    ? 'border-blue-500 text-blue-600 dark:text-blue-400'
                    : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                }`}
              >
                {tab.icon}
                {tab.name}
              </button>
            ))}
          </nav>
        </div>
        <div className="mt-4 p-4 bg-blue-50 dark:bg-blue-900/20 rounded-lg">
          <div className="flex items-center gap-2">
            {tabs.find(tab => tab.id === activeTab)?.icon}
            <div>
              <h3 className="font-medium text-blue-900 dark:text-blue-100">
                {tabs.find(tab => tab.id === activeTab)?.name}
              </h3>
              <p className="text-sm text-blue-700 dark:text-blue-200">
                {tabs.find(tab => tab.id === activeTab)?.description}
              </p>
            </div>
          </div>
        </div>
      </div>

      {/* Export Form */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow-lg p-6 border border-gray-200 dark:border-gray-700">
        <h2 className="text-xl font-semibold text-gray-900 dark:text-white mb-6">
          Export Parameters
        </h2>

        <div className="space-y-6">
          {renderFormFields()}

          <div className="flex justify-end pt-4 border-t">
            <Button
              onClick={handleExport}
              disabled={loading}
              className="flex items-center gap-2"
            >
              {loading ? (
                <>
                  <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white"></div>
                  Exporting...
                </>
              ) : (
                <>
                  <Download className="w-4 h-4" />
                  Export CSV
                </>
              )}
            </Button>
          </div>
        </div>
      </div>

      {/* Export History/Info */}
      <div className="mt-6 bg-gray-50 dark:bg-gray-800 rounded-lg p-4">
        <h3 className="font-medium text-gray-900 dark:text-white mb-2">Export Information</h3>
        <ul className="text-sm text-gray-600 dark:text-gray-300 space-y-1">
          <li>• All reports are exported in CSV format for easy analysis in Excel or other tools</li>
          <li>• Date ranges are inclusive and based on business dates</li>
          <li>• Large exports may take several minutes to process</li>
          <li>• Default limit is 100 rows, maximum varies by report type</li>
        </ul>
      </div>
    </div>
  );
};

export default ReportsPage;