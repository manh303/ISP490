import { useState, useEffect } from 'react';
import {
  Download,
  FileDown,
  AlertCircle,
  Loader2,
  RefreshCw,
  Calendar,
  Filter
} from 'lucide-react';
import { Button } from '../../components/ui/figma/button';
import { Calendar as CalendarComponent } from '../../components/ui/figma/calendar';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '../../components/ui/figma/select';
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from '../../components/ui/figma/popover';
import { format } from 'date-fns';
import {
  getOverviewReport,
  getTopProducts,
  getPlatforms,
  getCategories,
  type OverviewReport,
  type TopProduct,
  type Platform,
  type Category,
  type GetOverviewReportParams,
  type GetTopProductsParams,
} from '../../services/analyticsApi';
import { TopRatedProductsChart } from '../../components/analytics/TopRatedProductsChart';
import { CategoryPerformanceChart } from '../../components/analytics/CategoryPerformanceChart';
import { PriceSegmentsChart } from '../../components/analytics/PriceSegmentsChart';
import { DateRangePicker } from '../../components/analytics/DateRangePicker';
import { PlatformSelect } from '../../components/analytics/PlatformSelect';
import { CategoryHierarchySelector } from '../../components/analytics/CategoryHierarchySelector';

export function AnalyticsDashboard() {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Filter states
  const [fromDate, setFromDate] = useState<Date>();
  const [toDate, setToDate] = useState<Date>();
  const [platformCode, setPlatformCode] = useState<string>();
  const [categoryKey, setCategoryKey] = useState<string>();
  const [parentCategoryKey, setParentCategoryKey] = useState<string>();

  // Analytics data state
  const [overviewReport, setOverviewReport] = useState<OverviewReport | null>(null);
  const [topProducts, setTopProducts] = useState<TopProduct[] | null>(null);

  // Load analytics data
  const loadAnalyticsData = async () => {
    try {
      setLoading(true);
      setError(null);

      const fromDateStr = fromDate ? fromDate.toISOString().split('T')[0] : undefined;
      const toDateStr = toDate ? toDate.toISOString().split('T')[0] : undefined;

      const overviewParams: GetOverviewReportParams = {
        from_date: fromDateStr || '2025-10-22',
        to_date: toDateStr || '2025-11-21',
        platform_code: platformCode,
        category_key: categoryKey,
      };

      console.log('API Params:', {
        overviewParams,
        categoryKey,
        parentCategoryKey,
        platformCode
      });

      const topProductsParams: GetTopProductsParams = {
        from_date: fromDateStr || '2025-10-22',
        to_date: toDateStr || '2025-11-21',
        platform_code: platformCode,
        category_key: categoryKey,
        limit: 10,
      };

      const [
        overviewData,
        topProductsData,
      ] = await Promise.all([
        getOverviewReport(overviewParams),
        getTopProducts(topProductsParams),
      ]);

      setOverviewReport(overviewData);
      setTopProducts(topProductsData);
    } catch (err) {
      console.error('Error loading analytics data:', err);
      setError('Không thể tải dữ liệu phân tích. Vui lòng thử lại.');
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
    if (fromDate && toDate) {
      console.log('Loading analytics data with:', { platformCode, categoryKey, parentCategoryKey });
      loadAnalyticsData();
    }
  }, [fromDate, toDate, platformCode, categoryKey]);

  const handleRefresh = () => {
    window.location.reload();
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
                  onValueChange={setPlatformCode}
                />
              </div>
              <div className="flex items-center gap-2">
                <label className="text-sm font-medium">Danh mục:</label>
                <CategoryHierarchySelector
                  platformCode={platformCode}
                  selectedCategoryKey={categoryKey}
                  selectedParentKey={parentCategoryKey}
                  onCategoryChange={(categoryKey, parentKey) => {
                    console.log('CategoryHierarchySelector onCategoryChange:', { categoryKey, parentKey });
                    setCategoryKey(categoryKey);
                    setParentCategoryKey(parentKey);
                  }}
                />
              </div>
            </div>
          </div>

          {/* Dashboard Summary Cards */}
          {overviewReport && (
            <div className="px-6 py-4 border-b border-gray-200 bg-gradient-to-r from-blue-50 to-purple-50">
              <h3 className="text-gray-900 font-semibold mb-3">Tổng Quan Hệ Thống</h3>
              <div className="grid grid-cols-4 gap-4">
                <div className="bg-white rounded-lg p-4 shadow-sm border border-gray-200">
                  <div className="text-sm text-gray-600 mb-1">Tổng sản phẩm</div>
                  <div className="text-2xl font-bold text-gray-900">
                    {overviewReport?.kpis?.total_products?.toLocaleString('vi-VN')}
                  </div>
                </div>
                <div className="bg-white rounded-lg p-4 shadow-sm border border-gray-200">
                  <div className="text-sm text-gray-600 mb-1">Đánh giá trung bình</div>
                  <div className="text-2xl font-bold text-blue-600">
                    {overviewReport?.kpis?.avg_rating?.toFixed(2)} ⭐
                  </div>
                </div>
                <div className="bg-white rounded-lg p-4 shadow-sm border border-gray-200">
                  <div className="text-sm text-gray-600 mb-1">Tổng đánh giá</div>
                  <div className="text-2xl font-bold text-purple-600">
                    {(overviewReport?.kpis?.total_reviews / 1000)?.toFixed(0)}K
                  </div>
                </div>
                <div className="bg-white rounded-lg p-4 shadow-sm border border-gray-200">
                  <div className="text-sm text-gray-600 mb-1">Số lượng nền tảng</div>
                  <div className="text-2xl font-bold text-green-600">
                    {overviewReport?.platform_comparison?.length?.toLocaleString('vi-VN') || 0}
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* Dashboard Charts */}
          <div className="px-6 py-4 border-b border-gray-200 bg-white overflow-auto">
            <h3 className="text-gray-900 font-semibold mb-4">Biểu Đồ Phân Tích</h3>

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
              Analytics Dashboard - Tổng quan
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