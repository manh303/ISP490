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
  getTopRatedProducts,
  getCategoryPerformance,
  getPriceSegments,
  getDashboardSummary,
  type TopRatedProductsResponse,
  type CategoryPerformanceResponse,
  type PriceSegmentsResponse,
  type DashboardSummaryResponse,
} from '../../services/analyticsApi';
import { TopRatedProductsChart } from '../../components/analytics/TopRatedProductsChart';
import { CategoryPerformanceChart } from '../../components/analytics/CategoryPerformanceChart';
import { PriceSegmentsChart } from '../../components/analytics/PriceSegmentsChart';

export function AnalyticsDashboard() {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Analytics data state
  const [dashboardSummary, setDashboardSummary] = useState<DashboardSummaryResponse | null>(null);
  const [topRatedProducts, setTopRatedProducts] = useState<TopRatedProductsResponse | null>(null);
  const [categoryPerformance, setCategoryPerformance] = useState<CategoryPerformanceResponse | null>(null);
  const [priceSegments, setPriceSegments] = useState<PriceSegmentsResponse | null>(null);

  // Load analytics data
  useEffect(() => {
    const loadAnalyticsData = async () => {
      try {
        setLoading(true);
        setError(null);

        const [
          summaryData,
          topRatedData,
          categoryData,
          priceSegData,
        ] = await Promise.all([
          getDashboardSummary(),
          getTopRatedProducts({ limit: 10 }),
          getCategoryPerformance(),
          getPriceSegments(),
        ]);

        setDashboardSummary(summaryData);
        setTopRatedProducts(topRatedData);
        setCategoryPerformance(categoryData);
        setPriceSegments(priceSegData);
      } catch (err) {
        console.error('Error loading analytics data:', err);
        setError('Không thể tải dữ liệu phân tích. Vui lòng thử lại.');
      } finally {
        setLoading(false);
      }
    };

    loadAnalyticsData();
  }, []);

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

          {/* Dashboard Summary Cards */}
          {dashboardSummary && (
            <div className="px-6 py-4 border-b border-gray-200 bg-gradient-to-r from-blue-50 to-purple-50">
              <h3 className="text-gray-900 font-semibold mb-3">Tổng Quan Hệ Thống</h3>
              <div className="grid grid-cols-4 gap-4">
                <div className="bg-white rounded-lg p-4 shadow-sm border border-gray-200">
                  <div className="text-sm text-gray-600 mb-1">Tổng sản phẩm</div>
                  <div className="text-2xl font-bold text-gray-900">
                    {dashboardSummary?.summary?.total_products?.toLocaleString('vi-VN')}
                  </div>
                </div>
                <div className="bg-white rounded-lg p-4 shadow-sm border border-gray-200">
                  <div className="text-sm text-gray-600 mb-1">Đánh giá trung bình</div>
                  <div className="text-2xl font-bold text-blue-600">
                    {dashboardSummary?.summary?.overall_avg_rating?.toFixed(2)} ⭐
                  </div>
                </div>
                <div className="bg-white rounded-lg p-4 shadow-sm border border-gray-200">
                  <div className="text-sm text-gray-600 mb-1">Tổng đánh giá</div>
                  <div className="text-2xl font-bold text-purple-600">
                    {(dashboardSummary?.summary?.total_reviews / 1000)?.toFixed(0)}K
                  </div>
                </div>
                <div className="bg-white rounded-lg p-4 shadow-sm border border-gray-200">
                  <div className="text-sm text-gray-600 mb-1">Số lượng nền tảng</div>
                  <div className="text-2xl font-bold text-green-600">
                    {dashboardSummary?.summary?.total_platforms?.toLocaleString('vi-VN') || 0}
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* Dashboard Charts */}
          <div className="px-6 py-4 border-b border-gray-200 bg-white overflow-auto">
            <h3 className="text-gray-900 font-semibold mb-4">Biểu Đồ Phân Tích</h3>

            {/* Row 1: Top Products, Category Performance */}
            <div className="grid grid-cols-2 gap-4 mb-4">
              {topRatedProducts && (
                <TopRatedProductsChart data={topRatedProducts.data} />
              )}
              {categoryPerformance && (
                <CategoryPerformanceChart data={categoryPerformance.data} />
              )}
            </div>

            {/* Row 2: Price Segment Distribution */}
            <div className="grid grid-cols-1 gap-4">
              {priceSegments && (
                <PriceSegmentsChart data={priceSegments.data} />
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