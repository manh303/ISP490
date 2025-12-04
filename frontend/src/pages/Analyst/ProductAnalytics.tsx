import { useState, useEffect } from 'react';
import {
  Download,
  FileDown,
  AlertCircle,
  Loader2,
  RefreshCw
} from 'lucide-react';
import { Button } from '../../components/ui/figma/button';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '../../components/ui/figma/table';
import {
  getTopProducts,
  getPriceVsRevenue,
  getOverviewKPIs,
  getProductTimeseries,
  getProductReviewSummary,
  type TopProduct,
  type PriceVsRevenueItem,
  type OverviewKPIs,
  type ProductTimeseries,
  type ProductReviewSummary,
  type GetTopProductsParams,
  type GetPriceVsRevenueParams,
  type GetOverviewKPIsParams,
  type GetProductTimeseriesParams,
  type GetProductReviewSummaryParams,
} from '../../services/analyticsApi';
import { PriceVsRatingChart } from '../../components/analytics/PriceVsRatingChart';
import { ProductTimeseriesChart } from '../../components/analytics/ProductTimeseriesChart';
import { ReviewSummaryChart } from '../../components/analytics/ReviewSummaryChart';
import { DateRangePicker } from '../../components/analytics/DateRangePicker';
import { PlatformSelect } from '../../components/analytics/PlatformSelect';
import { CategorySelect } from '../../components/analytics/CategorySelect';
import { MetricSelect } from '../../components/analytics/MetricSelect';

export function ProductAnalytics() {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Analytics data state
  const [topProducts, setTopProducts] = useState<TopProduct[] | null>(null);
  const [priceVsRevenue, setPriceVsRevenue] = useState<PriceVsRevenueItem[] | null>(null);
  const [overviewKPIs, setOverviewKPIs] = useState<OverviewKPIs | null>(null);

  // Selected product detail state
  const [selectedProduct, setSelectedProduct] = useState<TopProduct | null>(null);
  const [productTimeseries, setProductTimeseries] = useState<ProductTimeseries | null>(null);
  const [productReviewSummary, setProductReviewSummary] = useState<ProductReviewSummary | null>(null);

  // Filter state
  const [fromDate, setFromDate] = useState<Date | undefined>();
  const [toDate, setToDate] = useState<Date | undefined>();
  const [platformCode, setPlatformCode] = useState<string>('');
  const [categoryKey, setCategoryKey] = useState<string>('');
  const [metric, setMetric] = useState<string>('revenue');

  // Load analytics data
  const loadAnalyticsData = async () => {
    try {
      setLoading(true);
      setError(null);

      const fromDateStr = fromDate ? fromDate.toISOString().split('T')[0] : undefined;
      const toDateStr = toDate ? toDate.toISOString().split('T')[0] : undefined;

      const topProductsParams: GetTopProductsParams = {
        from_date: fromDateStr || '2025-10-22',
        to_date: toDateStr || '2025-11-21',
        platform_code: platformCode,
        category_key: categoryKey,
        limit: 10,
      };

      const priceVsRevenueParams: GetPriceVsRevenueParams = {
        from_date: fromDateStr || '2025-10-22',
        to_date: toDateStr || '2025-11-21',
        platform_code: platformCode || 'tiki',
        limit: 100,
      };

      const kpisParams: GetOverviewKPIsParams = {
        from_date: fromDateStr || '2025-10-22',
        to_date: toDateStr || '2025-11-21',
        platform_code: platformCode,
        category_key: categoryKey,
      };

      const [
        topProductsData,
        priceVsRevenueData,
        kpisData,
      ] = await Promise.all([
        getTopProducts(topProductsParams),
        getPriceVsRevenue(priceVsRevenueParams),
        getOverviewKPIs(kpisParams),
      ]);

      setTopProducts(topProductsData);
      setPriceVsRevenue(priceVsRevenueData);
      setOverviewKPIs(kpisData);
    } catch (err) {
      console.error('Error loading analytics data:', err);
      setError('Không thể tải dữ liệu phân tích. Vui lòng thử lại.');
  } finally {
    setLoading(false);
  }
};

  useEffect(() => {
    // Set default date range (last 7 days)
    const now = new Date();
    const sevenDaysAgo = new Date();
    sevenDaysAgo.setDate(now.getDate() - 7);
    setFromDate(sevenDaysAgo);
    setToDate(now);
  }, []);

  useEffect(() => {
    if (fromDate && toDate) {
      loadAnalyticsData();
    }
  }, [fromDate, toDate, platformCode, categoryKey, metric]);  const handleRefresh = () => {
    loadAnalyticsData();
  };

  // Load product detail when selected
  const loadProductDetail = async (product: TopProduct) => {
    try {
      setSelectedProduct(product);

      const fromDateStr = fromDate ? fromDate.toISOString().split('T')[0] : '2025-10-22';
      const toDateStr = toDate ? toDate.toISOString().split('T')[0] : '2025-11-21';

      const timeseriesParams: GetProductTimeseriesParams = {
        product_key: product.product_key,
        platform_code: product.platform_code,
        from_date: fromDateStr,
        to_date: toDateStr,
      };

      const reviewParams: GetProductReviewSummaryParams = {
        product_key: product.product_key,
        platform_code: product.platform_code,
        from_date: fromDateStr,
        to_date: toDateStr,
      };

      const [timeseriesData, reviewData] = await Promise.all([
        getProductTimeseries(timeseriesParams),
        getProductReviewSummary(reviewParams),
      ]);

      setProductTimeseries(timeseriesData);
      setProductReviewSummary(reviewData);
    } catch (error) {
      console.error('Error loading product detail:', error);
    }
  };

  if (loading) {
    return (
      <div className="border border-gray-200 bg-white rounded-lg overflow-hidden shadow-sm flex items-center justify-center" style={{ height: '800px' }}>
        <div className="text-center">
          <Loader2 className="h-12 w-12 text-blue-500 animate-spin mx-auto mb-4" />
          <p className="text-gray-600">Đang tải dữ liệu phân tích...</p>
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
                <Button variant="outline" size="sm" onClick={handleRefresh}>
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
                  onValueChange={(value) => setPlatformCode(value || '')}
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
                <label className="text-sm font-medium">Chỉ số:</label>
                <MetricSelect
                  value={metric}
                  onValueChange={setMetric}
                />
              </div>
            </div>
          </div>

          {/* Overview KPIs */}
          {overviewKPIs && (
            <div className="px-6 py-4 border-b border-gray-200 bg-gradient-to-r from-blue-50 to-purple-50">
              <h3 className="text-gray-900 font-semibold mb-3">Tổng Quan</h3>
              <div className="grid grid-cols-4 gap-4">
                <div className="bg-white rounded-lg p-4 shadow-sm border border-gray-200">
                  <div className="text-sm text-gray-600 mb-1">Tổng sản phẩm</div>
                  <div className="text-2xl font-bold text-gray-900">
                    {overviewKPIs?.total_products?.toLocaleString('vi-VN')}
                  </div>
                </div>
                <div className="bg-white rounded-lg p-4 shadow-sm border border-gray-200">
                  <div className="text-sm text-gray-600 mb-1">Đánh giá trung bình</div>
                  <div className="text-2xl font-bold text-blue-600">
                    {overviewKPIs?.avg_rating?.toFixed(2)} ⭐
                  </div>
                </div>
                <div className="bg-white rounded-lg p-4 shadow-sm border border-gray-200">
                  <div className="text-sm text-gray-600 mb-1">Tổng đánh giá</div>
                  <div className="text-2xl font-bold text-purple-600">
                    {(overviewKPIs?.total_reviews / 1000)?.toFixed(0)}K
                  </div>
                </div>
                <div className="bg-white rounded-lg p-4 shadow-sm border border-gray-200">
                  <div className="text-sm text-gray-600 mb-1">Giá trung bình</div>
                  <div className="text-2xl font-bold text-green-600">
                    {overviewKPIs?.avg_price?.toLocaleString('vi-VN')} VND
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* Charts Section */}
          <div className="px-6 py-4 border-b border-gray-200 bg-white overflow-auto">
            <h3 className="text-gray-900 font-semibold mb-4">Biểu Đồ Phân Tích Sản Phẩm</h3>

            {/* Row 1: Price vs Revenue */}
            <div className="grid grid-cols-1 gap-4">
              {priceVsRevenue && (
                <PriceVsRatingChart data={priceVsRevenue} />
              )}
            </div>
          </div>

          {/* Top Products Table */}
          <div className="flex-1 overflow-auto px-6 py-4 bg-white">
            <h3 className="text-gray-900 font-semibold mb-4">Top Products</h3>
            <Table>
              <TableHeader>
                <TableRow className="border-gray-200 hover:bg-gray-50">
                  <TableHead className="text-gray-600">Tên sản phẩm</TableHead>
                  <TableHead className="text-gray-600">Rating</TableHead>
                  <TableHead className="text-gray-600">Số review</TableHead>
                  <TableHead className="text-gray-600">Giá trung bình</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {topProducts?.map((product, index) => (
                  <TableRow key={index} className="border-gray-200 hover:bg-gray-50">
                    <TableCell className="text-gray-900">
                      <button
                        onClick={() => loadProductDetail(product)}
                        className="text-blue-600 hover:text-blue-800 hover:underline text-left"
                      >
                        {product?.product_name}
                      </button>
                    </TableCell>
                    <TableCell className="text-gray-700">{product?.avg_rating?.toFixed(2)} ⭐</TableCell>
                    <TableCell className="text-gray-700">{product?.total_reviews}</TableCell>
                    <TableCell className="text-gray-700">{product?.avg_price?.toLocaleString('vi-VN')} VND</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>

          {/* Selected Product Detail */}
          {selectedProduct && (
            <div className="px-6 py-4 border-b border-gray-200 bg-white">
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-gray-900 font-semibold">
                  Chi tiết sản phẩm: {selectedProduct.product_name}
                </h3>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => {
                    setSelectedProduct(null);
                    setProductTimeseries(null);
                    setProductReviewSummary(null);
                  }}
                >
                  Đóng
                </Button>
              </div>

              {/* Product Detail Charts */}
              <div className="grid grid-cols-1 gap-6">
                {productTimeseries && productTimeseries.points && productTimeseries.points.length > 0 && (
                  <ProductTimeseriesChart data={productTimeseries.points} />
                )}

                {productReviewSummary && (
                  <ReviewSummaryChart data={productReviewSummary} />
                )}
              </div>
            </div>
          )}

          {/* Footer */}
          <div className="px-6 py-4 border-t border-gray-200 bg-gray-50 flex justify-between items-center">
            <div className="text-gray-600 text-sm">
              Product Analytics - Phân tích sản phẩm
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