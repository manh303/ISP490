import { useState, useEffect } from 'react';
import {
  Download,
  FileDown,
  AlertCircle,
  Loader2,
  RefreshCw,
  // Calendar,
  // Filter
} from 'lucide-react';
import { Button } from '../../components/ui/figma/button';
// import { Calendar as CalendarComponent } from '../../components/ui/figma/calendar';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '../../components/ui/figma/select';
// import {
//   Popover,
//   PopoverContent,
//   PopoverTrigger,
// } from '../../components/ui/figma/popover';
// import { format } from 'date-fns';
import {
  getAllOverviewData,
  getTopProducts,
  // getPlatforms,
  // getCategories,
  type OverviewReport,
  type TopProduct,
  // type Platform,
  // type Category,
  type GetOverviewReportParams,
  type GetTopProductsParams,
} from '../../services/analyticsApi';
import { TopRatedProductsChart } from '../../components/analytics/TopRatedProductsChart';
import { CategoryPerformanceChart } from '../../components/analytics/CategoryPerformanceChart';
// import { PriceSegmentsChart } from '../../components/analytics/PriceSegmentsChart';
import { DateRangePicker } from '../../components/analytics/DateRangePicker';
import { PlatformSelect } from '../../components/analytics/PlatformSelect';
import { CategorySelect } from '../../components/analytics/CategorySelect';

// Mock data for loading states and fallbacks
/*
const mockOverviewReport: OverviewReport = {
  from_date: '2025-10-22',
  to_date: '2025-11-21',
  kpis: {
    from_date: '2025-10-22',
    to_date: '2025-11-21',
    total_revenue: 1250000000,
    total_products: 15420,
    total_reviews: 89250,
    avg_price: 85000,
    avg_rating: 4.2
  },
  trends: {
    from_date: '2025-10-22',
    to_date: '2025-11-21',
    points: [
      { date: '2025-10-22', revenue: 45000000, total_orders: 520, avg_price: 82000, avg_rating: 4.1, total_reviews: 1250 },
      { date: '2025-10-29', revenue: 48000000, total_orders: 550, avg_price: 83000, avg_rating: 4.2, total_reviews: 1300 },
      { date: '2025-11-05', revenue: 52000000, total_orders: 580, avg_price: 85000, avg_rating: 4.3, total_reviews: 1400 },
      { date: '2025-11-12', revenue: 51000000, total_orders: 570, avg_price: 84000, avg_rating: 4.2, total_reviews: 1350 },
      { date: '2025-11-19', revenue: 53000000, total_orders: 590, avg_price: 86000, avg_rating: 4.3, total_reviews: 1450 }
    ]
  },
  platform_comparison: [
    { platform_code: 'tiki', platform_name: 'Tiki', total_revenue: 650000000, total_products: 8200, avg_price: 78000, avg_rating: 4.1, total_reviews: 45200 },
    { platform_code: 'shopee', platform_name: 'Shopee', total_revenue: 480000000, total_products: 5800, avg_price: 92000, avg_rating: 4.3, total_reviews: 38100 },
    { platform_code: 'lazada', platform_name: 'Lazada', total_revenue: 120000000, total_products: 1420, avg_price: 105000, avg_rating: 4.0, total_reviews: 5950 }
  ],
  category_share: [
    { category_key: 'dien-thoai', category_name: 'Điện thoại', platform_code: 'tiki', revenue: 250000000, revenue_share: 0.2 },
    { category_key: 'laptop', category_name: 'Laptop', platform_code: 'tiki', revenue: 180000000, revenue_share: 0.14 },
    { category_key: 'phu-kien', category_name: 'Phụ kiện', platform_code: 'shopee', revenue: 150000000, revenue_share: 0.12 },
    { category_key: 'dien-tu', category_name: 'Điện tử', platform_code: 'lazada', revenue: 80000000, revenue_share: 0.065 }
  ]
};
*/

/*
const mockTopProducts: TopProduct[] = [
  { product_key: 'iphone-15-pro', product_name: 'iPhone 15 Pro 128GB', platform_code: 'tiki', category_key: 'dien-thoai', total_revenue: 45000000, total_reviews: 1250, avg_rating: 4.5, avg_price: 28500000 },
  { product_key: 'macbook-air-m2', product_name: 'MacBook Air M2 13 inch', platform_code: 'tiki', category_key: 'laptop', total_revenue: 38000000, total_reviews: 890, avg_rating: 4.7, avg_price: 32000000 },
  { product_key: 'samsung-galaxy-s24', product_name: 'Samsung Galaxy S24 Ultra', platform_code: 'shopee', category_key: 'dien-thoai', total_revenue: 32000000, total_reviews: 980, avg_rating: 4.3, avg_price: 26500000 },
  { product_key: 'airpods-pro', product_name: 'AirPods Pro 2', platform_code: 'shopee', category_key: 'phu-kien', total_revenue: 25000000, total_reviews: 750, avg_rating: 4.4, avg_price: 5500000 },
  { product_key: 'dell-xps-13', product_name: 'Dell XPS 13 9340', platform_code: 'lazada', category_key: 'laptop', total_revenue: 22000000, total_reviews: 420, avg_rating: 4.2, avg_price: 35000000 }
];
*/

export function AnalyticsDashboard() {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Add state to track errors for each API separately
  const [analyticsError, setAnalyticsError] = useState<string | null>(null);
  const [topProductsError, setTopProductsError] = useState<string | null>(null);

  // Filter states
  const [fromDate, setFromDate] = useState<Date>();
  const [toDate, setToDate] = useState<Date>();
  const [platformCode, setPlatformCode] = useState<string>();
  const [categoryKey, setCategoryKey] = useState<string>();
  const [metric, setMetric] = useState<'revenue' | 'review_count' | 'avg_rating' | 'price_growth'>('revenue');

  // Analytics data state
  const [overviewReport, setOverviewReport] = useState<OverviewReport | null>(null);
  const [topProducts, setTopProducts] = useState<TopProduct[] | null>(null);

  // Load all overview data (only call when initializing or changing general filters)
  const loadAnalyticsData = async () => {
    try {
      setLoading(true);
      setAnalyticsError(null); // Reset lỗi cục bộ

      const fromDateStr = fromDate ? fromDate.toISOString().split('T')[0] : undefined;
      const toDateStr = toDate ? toDate.toISOString().split('T')[0] : undefined;

      const overviewParams: GetOverviewReportParams = {
        from_date: fromDateStr || '2025-10-22',
        to_date: toDateStr || '2025-11-21',
        platform_code: platformCode,
        category_key: categoryKey,
      };

      // console.log('API Params:', {
      //   overviewParams,
      //   categoryKey,
      //   platformCode
      // });

      const overviewData = await getAllOverviewData(overviewParams);
      setOverviewReport(overviewData);
    } catch (err) {
      console.error('Error loading analytics data:', err);
      setAnalyticsError('Unable to load analytics data. Please try again.');
      setOverviewReport(null);
    } finally {
      setLoading(false);
    }
  };

  // Only refilter getTopProducts when changing filters
  const loadTopProducts = async () => {
    try {
      setLoading(true);
      setTopProductsError(null); // Reset lỗi cục bộ

      const fromDateStr = fromDate ? fromDate.toISOString().split('T')[0] : undefined;
      const toDateStr = toDate ? toDate.toISOString().split('T')[0] : undefined;

      const topProductsParams: GetTopProductsParams = {
        from_date: fromDateStr || '2025-10-22',
        to_date: toDateStr || '2025-11-21',
        platform_code: platformCode,
        category_key: categoryKey,
        metric,
        limit: 10,
      };

      const topProductsData = await getTopProducts(topProductsParams);
      setTopProducts(topProductsData);
    } catch (err) {
      console.error('Error loading top products:', err);
      setTopProductsError('Unable to load top products data. Please try again.');
      setTopProducts(null);
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

  // Only load overview when changing general filters
  useEffect(() => {
    if (fromDate && toDate) {
      console.log('Loading analytics data with:', { platformCode, categoryKey });
      loadAnalyticsData();
    }
  }, [fromDate, toDate, platformCode, categoryKey]);

  // Only refilter top products when changing filters or metric
  useEffect(() => {
    if (fromDate && toDate) {
      loadTopProducts();
    }
  }, [fromDate, toDate, platformCode, categoryKey, metric]);

  const handleRefresh = () => {
    setAnalyticsError(null);
    setTopProductsError(null);
    window.location.reload();
  };

  if (loading && !overviewReport && !topProducts) {
    return (
      <div className="border border-gray-200 bg-white rounded-lg overflow-hidden shadow-sm flex items-center justify-center" style={{ height: '800px' }}>
        <div className="text-center">
          <Loader2 className="h-12 w-12 text-blue-500 animate-spin mx-auto mb-4" />
          <p className="text-gray-600">Loading analytics data...</p>
        </div>
      </div>
    );
  }

  // Không còn hiển thị error component global nữa
  // Thay vào đó, hiển thị dashboard với thông báo lỗi cục bộ

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
                  Refresh
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
              {/* Hiển thị thông báo lỗi cục bộ nếu có */}
              {(analyticsError || topProductsError) && (
                <div className="flex items-center gap-2 text-sm text-amber-600 bg-amber-50 px-3 py-1 rounded-md">
                  <AlertCircle className="h-4 w-4" />
                  <span>
                    {analyticsError && topProductsError 
                      ? 'Some data could not be loaded. Please try again.' 
                      : analyticsError || topProductsError}
                  </span>
                </div>
              )}
            </div>
          </div>

          {/* Filters */}
          <div className="px-6 py-4 border-b border-gray-200 bg-white">
            <div className="flex items-center gap-4 flex-wrap">
              <div className="flex items-center gap-2">
                <label className="text-sm font-medium">Time:</label>
                <DateRangePicker
                  fromDate={fromDate}
                  toDate={toDate}
                  onFromDateChange={setFromDate}
                  onToDateChange={setToDate}
                />
              </div>
              <div className="flex items-center gap-2">
                <label className="text-sm font-medium">Platform:</label>
                <PlatformSelect
                  value={platformCode}
                  onValueChange={setPlatformCode}
                />
              </div>
              <div className="flex items-center gap-2">
                <label className="text-sm font-medium">Category:</label>
                <CategorySelect
                  value={categoryKey}
                  onValueChange={setCategoryKey}
                />
              </div>
              <div className="flex items-center gap-2">
                <label className="text-sm font-medium">Criteria:</label>
                <Select value={metric} onValueChange={v => setMetric(v as 'revenue' | 'review_count' | 'avg_rating' | 'price_growth')}>
                  <SelectTrigger className="w-[150px]">
                    <SelectValue placeholder="Select criteria" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="revenue">Revenue</SelectItem>
                    <SelectItem value="review_count">Review count</SelectItem>
                    <SelectItem value="avg_rating">Avg rating</SelectItem>
                    <SelectItem value="price_growth">Price growth</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>
          </div>

          {/* Dashboard Summary Cards */}
          <div className="px-6 py-4 border-b border-gray-200 bg-gradient-to-r from-blue-50 to-purple-50">
<<<<<<< Updated upstream
            <h3 className="text-gray-900 font-semibold mb-3">Tổng Quan Hệ Thống</h3>
            <div className="grid grid-cols-2 sm:grid-cols-2 md:grid-cols-4 gap-4">
              <div className="font-bold text-purple-600 break-words text-xl md:text-2xl max-w-[120px] md:max-w-full" style={{wordBreak: 'break-word'}}>
                <div className="text-sm text-gray-600 mb-1">Tổng sản phẩm</div>
                <div className="font-bold text-gray-900 break-words text-xl md:text-2xl max-w-[120px] md:max-w-full" style={{wordBreak: 'break-word'}}>
                  {(overviewReport || mockOverviewReport)?.kpis?.total_products?.toLocaleString('vi-VN')}
                </div>
              </div>
              <div className="font-bold text-purple-600 break-words text-xl md:text-2xl max-w-[120px] md:max-w-full" style={{wordBreak: 'break-word'}}>
                <div className="text-sm text-gray-600 mb-1">Đánh giá trung bình</div>
                <div className="font-bold text-blue-600 break-words text-xl md:text-2xl max-w-[120px] md:max-w-full" style={{wordBreak: 'break-word'}}>
                  {(overviewReport || mockOverviewReport)?.kpis?.avg_rating?.toFixed(2)} ⭐
                </div>
              </div>
              <div className="font-bold text-purple-600 break-words text-xl md:text-2xl max-w-[120px] md:max-w-full" style={{wordBreak: 'break-word'}}>
                <div className="text-sm text-gray-600 mb-1">Tổng đánh giá</div>
                <div className="font-bold text-purple-600 break-words text-xl md:text-2xl max-w-[120px] md:max-w-full" style={{wordBreak: 'break-word'}}>
                  {((overviewReport || mockOverviewReport)?.kpis?.total_reviews / 1000)?.toFixed(0)}K
                </div>
              </div>
              <div className="font-bold text-purple-600 break-words text-xl md:text-2xl max-w-[120px] md:max-w-full" style={{wordBreak: 'break-word'}}>
                <div className="text-sm text-gray-600 mb-1">Số lượng nền tảng</div>
                <div className="font-bold text-green-600 break-words text-xl md:text-2xl max-w-[120px] md:max-w-full" style={{wordBreak: 'break-word'}}>
                  {(overviewReport || mockOverviewReport)?.platform_comparison?.length?.toLocaleString('vi-VN') || 0}
=======
            <h3 className="text-gray-900 font-semibold mb-3">System Overview</h3>
            <div className="grid grid-cols-4 gap-4">
              <div className="bg-white rounded-lg p-4 shadow-sm border border-gray-200">
                <div className="text-sm text-gray-600 mb-1">Total products</div>
                <div className="text-2xl font-bold text-gray-900">
                  {overviewReport?.kpis?.total_products?.toLocaleString('vi-VN') || 'N/A'}
                </div>
              </div>
              <div className="bg-white rounded-lg p-4 shadow-sm border border-gray-200">
                <div className="text-sm text-gray-600 mb-1">Average rating</div>
                <div className="text-2xl font-bold text-blue-600">
                  {overviewReport?.kpis?.avg_rating?.toFixed(2) || 'N/A'} ⭐
                </div>
              </div>
              <div className="bg-white rounded-lg p-4 shadow-sm border border-gray-200">
                <div className="text-sm text-gray-600 mb-1">Total reviews</div>
                <div className="text-2xl font-bold text-purple-600">
                  {overviewReport?.kpis?.total_reviews ? ((overviewReport.kpis.total_reviews / 1000).toFixed(0)) + 'K' : 'N/A'}
                </div>
              </div>
              <div className="bg-white rounded-lg p-4 shadow-sm border border-gray-200">
                <div className="text-sm text-gray-600 mb-1">Number of platforms</div>
                <div className="text-2xl font-bold text-green-600">
                  {overviewReport?.platform_comparison?.length?.toLocaleString('vi-VN') || 'N/A'}
>>>>>>> Stashed changes
                </div>
              </div>
            </div>
          </div>

          {/* Dashboard Charts */}
          <div className="px-6 py-4 border-b border-gray-200 bg-white overflow-auto">
            <h3 className="text-gray-900 font-semibold mb-4">Analysis Charts</h3>

            {/* Row 1: Top Products, Category Share */}
            <div className="grid grid-cols-2 gap-4 mb-4">
              {topProducts && (
                <TopRatedProductsChart data={topProducts} />
              )}
              {overviewReport?.category_share && (
                <CategoryPerformanceChart 
                  data={overviewReport.category_share.map(item => ({
                    category: item.category_name,
                    product_count: Math.floor(Math.random() * 100) + 10,
                    avg_rating: parseFloat((Math.random() * 2 + 3).toFixed(2)),
                    high_rated_count: Math.floor(Math.random() * 20) + 5,
                    total_reviews: Math.floor(Math.random() * 500) + 50,
                  }))} 
                />
              )}
            </div>
          </div>

          {/* Footer */}
          <div className="px-6 py-4 border-t border-gray-200 bg-gray-50 flex justify-between items-center">
            <div className="text-gray-600 text-sm">
              Analytics Dashboard - Overview
            </div>
            <Button>
              <FileDown className="h-4 w-4 mr-2" />
              Export report
            </Button>
          </div>
        </div>
      </div>
    </div>
  );
}