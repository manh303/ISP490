import { useState, useEffect } from 'react';
import {
  Download,
  FileDown,
  AlertCircle,
  Loader2,
  RefreshCw,
  Search,
  TrendingUp,
  Star,
  MessageSquare,
  ShoppingCart
} from 'lucide-react';
import { Button } from '../../components/ui/figma/button';
import {
  getProductReport,
  getPlatforms,
  getCategories,
  type ProductReport,
  type Platform,
  type Category,
  type GetProductReportParams,
} from '../../services/analyticsApi';
import { ProductTimeseriesChart } from '../../components/analytics/ProductTimeseriesChart';
import { ReviewSummaryChart } from '../../components/analytics/ReviewSummaryChart';
import { PlatformSelect } from '../../components/analytics/PlatformSelect';
import { CategorySelect } from '../../components/analytics/CategorySelect';
import { DateRangePicker } from '../../components/analytics/DateRangePicker';
import { ProductSearch } from '../../components/analytics/ProductSearch';

export function ProductDetailAnalytics() {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Filter states
  const [fromDate, setFromDate] = useState<Date>();
  const [toDate, setToDate] = useState<Date>();
  const [platformCode, setPlatformCode] = useState<string>('tiki'); // Default to tiki
  const [categoryKey, setCategoryKey] = useState<string>();
  const [productId, setProductId] = useState<string>('');
  const [productName, setProductName] = useState<string>('');

  // Analytics data state
  const [productReport, setProductReport] = useState<ProductReport | null>(null);

  // Load analytics data
  const loadAnalyticsData = async () => {
    if (!productId.trim()) {
      setProductReport(null);
      setLoading(false);
      return;
    }

    try {
      setLoading(true);
      setError(null);

      const fromDateStr = fromDate ? fromDate.toISOString().split('T')[0] : undefined;
      const toDateStr = toDate ? toDate.toISOString().split('T')[0] : undefined;

      const reportParams: GetProductReportParams = {
        product_key: productId,
        platform_code: platformCode || 'tiki',
        from_date: fromDateStr || '2025-10-22',
        to_date: toDateStr || '2025-11-21',
      };

      const reportData = await getProductReport(reportParams);
      setProductReport(reportData);
    } catch (err) {
      console.error('Error loading product detail analytics data:', err);
      setError('Không thể tải dữ liệu chi tiết sản phẩm. Vui lòng thử lại.');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    // Set default date range (last 30 days)
    const now = new Date();
    const thirtyDaysAgo = new Date();
    thirtyDaysAgo.setDate(now.getDate() - 30);
    setFromDate(thirtyDaysAgo);
    setToDate(now);
  }, []);

  useEffect(() => {
    if (fromDate && toDate && platformCode && productId) {
      loadAnalyticsData();
    } else if (!productId) {
      setProductReport(null);
      setLoading(false);
    }
  }, [fromDate, toDate, platformCode, categoryKey, productId]);

  const handleRefresh = () => {
    window.location.reload();
  };

  if (loading) {
    return (
      <div className="border border-gray-200 bg-white rounded-lg overflow-hidden shadow-sm flex items-center justify-center" style={{ height: '800px' }}>
        <div className="text-center">
          <Loader2 className="h-12 w-12 text-blue-500 animate-spin mx-auto mb-4" />
          <p className="text-gray-600">Đang tải dữ liệu chi tiết sản phẩm...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="border border-red-200 bg-white rounded-lg overflow-hidden shadow-sm flex items-center justify-center" style={{ height: '800px' }}>
        <div className="text-center p-8">
          <AlertCircle className="h-12 w-12 text-red-500 mx-auto mb-4" />
          <p className="text-red-600 mb-4">{error}</p>
          <Button onClick={handleRefresh}>
            <RefreshCw className="h-4 w-4 mr-2" />
            Thử lại
          </Button>
        </div>
      </div>
    );
  }

  return (
    <div className="border border-gray-200 bg-white rounded-lg overflow-hidden shadow-sm" style={{ minHeight: '800px' }}>
      <div className="flex h-full flex-col">
        {/* Main Content */}
        <div className="flex-1 flex flex-col bg-white overflow-hidden">

          {/* Export Controls */}
          <div className="px-6 py-4 border-b border-gray-200 bg-gray-50">
            <div className="flex items-center gap-4 justify-between">
              <div className="flex items-center gap-3">
                <Button variant="outline" size="sm" onClick={loadAnalyticsData}>
                  <RefreshCw className="h-4 w-4 mr-2" />
                  Làm mới
                </Button>
                <Button variant="outline" size="sm">
                  <Download className="h-4 w-4 mr-2" />
                  Export Dashboard
                </Button>
                <Button variant="outline" size="sm">
                  <FileDown className="h-4 w-4 mr-2" />
                  Export Data
                </Button>
              </div>
            </div>
          </div>

          {/* Filters */}
          <div className="px-6 py-4 border-b border-gray-200 bg-white">
            <div className="flex items-center gap-4 flex-wrap">
              <div className="flex items-center gap-2">
                <label className="text-sm font-medium">Thời gian:</label>
                <DateRangePicker
                  fromDate={fromDate}
                  toDate={toDate}
                  onFromDateChange={setFromDate}
                  onToDateChange={setToDate}
                />
              </div>
              <div className="flex items-center gap-2">
                <label className="text-sm font-medium">Nền tảng:</label>
                <PlatformSelect
                  value={platformCode}
                  onValueChange={(value) => setPlatformCode(value || 'tiki')}
                />
              </div>
              <div className="flex items-center gap-2">
                <label className="text-sm font-medium">Danh mục:</label>
                <CategorySelect
                  value={categoryKey}
                  onValueChange={(value) => setCategoryKey(value || '')}
                  platformCode={platformCode}
                />
              </div>
              <div className="flex items-center gap-2">
                <label className="text-sm font-medium">Sản phẩm:</label>
                <ProductSearch
                  value={productName}
                  onProductSelect={(productKey, productName) => {
                    setProductId(productKey);
                    setProductName(productName);
                  }}
                  platformCode={platformCode}
                  categoryKey={categoryKey}
                  placeholder="Tìm kiếm sản phẩm..."
                  className="w-64"
                />
              </div>
            </div>
          </div>

          {/* Product Summary */}
          {productReport?.review_summary && (
            <div className="px-6 py-4 border-b border-gray-200 bg-gradient-to-r from-blue-50 to-purple-50">
              <h3 className="text-gray-900 font-semibold mb-3">Tóm Tắt Sản Phẩm</h3>
              <div className="grid grid-cols-4 gap-4">
                <div className="bg-white rounded-lg p-4 shadow-sm border border-gray-200">
                  <div className="flex items-center gap-2 mb-2">
                    <Star className="h-5 w-5 text-yellow-500" />
                    <div className="text-sm text-gray-600">Đánh giá trung bình</div>
                  </div>
                  <div className="text-xl font-bold text-gray-900">
                    {productReport.review_summary.avg_rating?.toFixed(1)}/5
                  </div>
                </div>
                <div className="bg-white rounded-lg p-4 shadow-sm border border-gray-200">
                  <div className="flex items-center gap-2 mb-2">
                    <MessageSquare className="h-5 w-5 text-blue-500" />
                    <div className="text-sm text-gray-600">Tổng đánh giá</div>
                  </div>
                  <div className="text-xl font-bold text-blue-600">
                    {productReport.review_summary.total_reviews?.toLocaleString('vi-VN')}
                  </div>
                </div>
                <div className="bg-white rounded-lg p-4 shadow-sm border border-gray-200">
                  <div className="flex items-center gap-2 mb-2">
                    <TrendingUp className="h-5 w-5 text-purple-500" />
                    <div className="text-sm text-gray-600">Đánh giá trung bình</div>
                  </div>
                  <div className="text-xl font-bold text-purple-600">
                    {productReport.review_summary.avg_rating?.toFixed(2)}
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* Product Charts */}
          <div className="px-6 py-4 border-b border-gray-200 bg-white overflow-auto">
            <h3 className="text-gray-900 font-semibold mb-4">Biểu Đồ Chi Tiết Sản Phẩm</h3>

            <div className="grid grid-cols-1 gap-6">
              {productReport?.timeseries && productReport.timeseries.points && productReport.timeseries.points.length > 0 && (
                <ProductTimeseriesChart data={productReport.timeseries.points} />
              )}

              {productReport?.review_summary && (
                <ReviewSummaryChart data={productReport.review_summary} />
              )}

              {!productId && (
                <div className="text-center py-12 text-gray-500">
                  <Search className="h-12 w-12 mx-auto mb-4 text-gray-300" />
                  <p>Vui lòng chọn sản phẩm để xem chi tiết phân tích</p>
                </div>
              )}

              {productId && !productReport?.timeseries && !productReport?.review_summary && (
                <div className="text-center py-12 text-gray-500">
                  <AlertCircle className="h-12 w-12 mx-auto mb-4 text-gray-300" />
                  <p>Không tìm thấy dữ liệu cho sản phẩm này</p>
                </div>
              )}
            </div>
          </div>

          {/* Footer */}
          <div className="px-6 py-4 border-t border-gray-200 bg-gray-50 flex justify-between items-center">
            <div className="text-gray-600 text-sm">
              Product Detail Analytics - Phân tích chi tiết sản phẩm
            </div>
            <Button>
              <FileDown className="h-4 w-4 mr-2" />
              Xuất báo cáo
            </Button>
          </div>
        </div>
      </div>
    </div>
  );
}