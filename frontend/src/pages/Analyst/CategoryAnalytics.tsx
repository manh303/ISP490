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
  getCategoryShare,
  getCategoryAnalytics,
  getPlatforms,
  getCategories,
  type CategoryShareItem,
  type OverviewKPIs,
  type OverviewTrends,
  type Platform,
  type Category,
  type GetCategoryShareParams,
} from '../../services/analyticsApi';
import { CategoryShareChart } from '../../components/analytics/CategoryShareChart';
import { PlatformSelect } from '../../components/analytics/PlatformSelect';
import { CategoryHierarchySelector } from '../../components/analytics/CategoryHierarchySelector';
import { DateRangePicker } from '../../components/analytics/DateRangePicker';

export function CategoryAnalytics() {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Filter states
  const [fromDate, setFromDate] = useState<Date>();
  const [toDate, setToDate] = useState<Date>();
  const [platformCode, setPlatformCode] = useState<string>('tiki'); // Default to tiki
  const [selectedCategory, setSelectedCategory] = useState<string>();
  const [selectedParentCategory, setSelectedParentCategory] = useState<string>();

  // Analytics data state
  const [categoryShare, setCategoryShare] = useState<CategoryShareItem[] | null>(null);
  const [categoryAnalytics, setCategoryAnalytics] = useState<{
    kpis: OverviewKPIs;
    trends: OverviewTrends;
  } | null>(null);

  // Load analytics data
  const loadAnalyticsData = async () => {
    try {
      setLoading(true);
      setError(null);

      const fromDateStr = fromDate ? fromDate.toISOString().split('T')[0] : undefined;
      const toDateStr = toDate ? toDate.toISOString().split('T')[0] : undefined;

      const params: GetCategoryShareParams = {
        from_date: fromDateStr || '2025-10-22',
        to_date: toDateStr || '2025-11-21',
        platform_code: platformCode || 'tiki',
      };

      // Load category share data
      const categoryShareData = await getCategoryShare(params);
      setCategoryShare(categoryShareData);

      // Load specific category analytics if category is selected
      if (selectedCategory) {
        const fromDateStr = fromDate ? fromDate.toISOString().split('T')[0] : '2025-10-22';
        const toDateStr = toDate ? toDate.toISOString().split('T')[0] : '2025-11-21';
        
        const categoryData = await getCategoryAnalytics(selectedCategory, fromDateStr, toDateStr);
        setCategoryAnalytics(categoryData);
      } else {
        setCategoryAnalytics(null);
      }
    } catch (err) {
      console.error('Error loading category analytics data:', err);
      setError('Không thể tải dữ liệu phân tích danh mục. Vui lòng thử lại.');
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
    if (fromDate && toDate && platformCode) {
      console.log('Filter changed:', {
        platformCode,
        selectedCategory,
        selectedParentCategory,
        fromDate: fromDate.toISOString().split('T')[0],
        toDate: toDate.toISOString().split('T')[0]
      });
      loadAnalyticsData();
    }
  }, [fromDate, toDate, platformCode, selectedCategory, selectedParentCategory]);  const handleRefresh = () => {
    window.location.reload();
  };

  if (loading) {
    return (
      <div className="border border-gray-200 bg-white rounded-lg overflow-hidden shadow-sm flex items-center justify-center" style={{ height: '800px' }}>
        <div className="text-center">
          <Loader2 className="h-12 w-12 text-blue-500 animate-spin mx-auto mb-4" />
          <p className="text-gray-600">Đang tải dữ liệu phân tích danh mục...</p>
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
                <CategoryHierarchySelector
                  platformCode={platformCode}
                  onCategoryChange={(categoryKey, parentKey) => {
                    setSelectedCategory(categoryKey);
                    setSelectedParentCategory(parentKey);
                  }}
                />
              </div>
            </div>
          </div>

          {/* Category Share Chart */}
          <div className="px-6 py-4 border-b border-gray-200 bg-white overflow-auto">
            <h3 className="text-gray-900 font-semibold mb-4">Tỷ Trọng Danh Mục Theo Nền Tảng</h3>

            <div className="grid grid-cols-1 gap-4">
              {categoryShare && (
                <CategoryShareChart data={categoryShare} />
              )}
            </div>
          </div>

          {/* Selected Category Analytics */}
          {categoryAnalytics && selectedCategory && (
            <div className="px-6 py-4 border-b border-gray-200 bg-gradient-to-r from-blue-50 to-purple-50">
              <h3 className="text-gray-900 font-semibold mb-3">Phân Tích Danh Mục Được Chọn</h3>
              <div className="grid grid-cols-4 gap-4">
                <div className="bg-white rounded-lg p-4 shadow-sm border border-gray-200">
                  <div className="text-sm text-gray-600 mb-1">Tổng sản phẩm</div>
                  <div className="text-2xl font-bold text-gray-900">
                    {categoryAnalytics.kpis?.total_products?.toLocaleString('vi-VN')}
                  </div>
                </div>
                <div className="bg-white rounded-lg p-4 shadow-sm border border-gray-200">
                  <div className="text-sm text-gray-600 mb-1">Đánh giá trung bình</div>
                  <div className="text-2xl font-bold text-blue-600">
                    {categoryAnalytics.kpis?.avg_rating?.toFixed(2)} ⭐
                  </div>
                </div>
                <div className="bg-white rounded-lg p-4 shadow-sm border border-gray-200">
                  <div className="text-sm text-gray-600 mb-1">Tổng đánh giá</div>
                  <div className="text-2xl font-bold text-purple-600">
                    {(categoryAnalytics.kpis?.total_reviews / 1000)?.toFixed(0)}K
                  </div>
                </div>
                <div className="bg-white rounded-lg p-4 shadow-sm border border-gray-200">
                  <div className="text-sm text-gray-600 mb-1">Giá trung bình</div>
                  <div className="text-2xl font-bold text-green-600">
                    {categoryAnalytics.kpis?.avg_price?.toLocaleString('vi-VN')} VND
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* Footer */}
          <div className="px-6 py-4 border-t border-gray-200 bg-gray-50 flex justify-between items-center">
            <div className="text-gray-600 text-sm">
              Category Analytics - Phân tích danh mục
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