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
  getTopRatedProducts,
  getRatingDistribution,
  getCategoryPerformance,
  getPriceSegments,
  getPriceVsRating,
  type TopRatedProductsResponse,
  type RatingDistributionResponse,
  type CategoryPerformanceResponse,
  type PriceSegmentsResponse,
  type PriceVsRatingResponse,
} from '../../services/analyticsApi';
import { RatingDistributionChart } from '../../components/analytics/RatingDistributionChart';
import { PriceVsRatingChart } from '../../components/analytics/PriceVsRatingChart';
import { CategoryPerformanceChart } from '../../components/analytics/CategoryPerformanceChart';
import { PriceSegmentsChart } from '../../components/analytics/PriceSegmentsChart';

export function ProductAnalytics() {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Analytics data state
  const [topRatedProducts, setTopRatedProducts] = useState<TopRatedProductsResponse | null>(null);
  const [ratingDistribution, setRatingDistribution] = useState<RatingDistributionResponse | null>(null);
  const [categoryPerformance, setCategoryPerformance] = useState<CategoryPerformanceResponse | null>(null);
  const [priceSegments, setPriceSegments] = useState<PriceSegmentsResponse | null>(null);
  const [priceVsRating, setPriceVsRating] = useState<PriceVsRatingResponse | null>(null);

  // Load analytics data
  useEffect(() => {
    const loadAnalyticsData = async () => {
      try {
        setLoading(true);
        setError(null);

        const [
          topRatedData,
          ratingDistData,
          categoryData,
          priceSegData,
          priceVsRatingData,
        ] = await Promise.all([
          getTopRatedProducts({ limit: 10 }),
          getRatingDistribution(),
          getCategoryPerformance(),
          getPriceSegments(),
          getPriceVsRating(),
        ]);

        setTopRatedProducts(topRatedData);
        setRatingDistribution(ratingDistData);
        setCategoryPerformance(categoryData);
        setPriceSegments(priceSegData);
        setPriceVsRating(priceVsRatingData);
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

          {/* Charts Section */}
          <div className="px-6 py-4 border-b border-gray-200 bg-white overflow-auto">
            <h3 className="text-gray-900 font-semibold mb-4">Biểu Đồ Phân Tích Sản Phẩm</h3>

            {/* Row 1: Rating Distribution, Price vs Rating */}
            <div className="grid grid-cols-2 gap-4 mb-4">
              {ratingDistribution && (
                <RatingDistributionChart data={ratingDistribution.data} />
              )}
              {priceVsRating && (
                <PriceVsRatingChart data={priceVsRating.data} />
              )}
            </div>

            {/* Row 2: Category Performance, Price Segments */}
            <div className="grid grid-cols-2 gap-4">
              {categoryPerformance && (
                <CategoryPerformanceChart data={categoryPerformance.data} />
              )}
              {priceSegments && (
                <PriceSegmentsChart data={priceSegments.data} />
              )}
            </div>
          </div>

          {/* Top Rated Products Table */}
          <div className="flex-1 overflow-auto px-6 py-4 bg-white">
            <h3 className="text-gray-900 font-semibold mb-4">Top Rated Products</h3>
            <Table>
              <TableHeader>
                <TableRow className="border-gray-200 hover:bg-gray-50">
                  <TableHead className="text-gray-600">Tên sản phẩm</TableHead>
                  <TableHead className="text-gray-600">Rating</TableHead>
                  <TableHead className="text-gray-600">Số review</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {topRatedProducts?.data.map((product, index) => (
                  <TableRow key={index} className="border-gray-200 hover:bg-gray-50">
                    <TableCell className="text-gray-900">{product.product_name}</TableCell>
                    <TableCell className="text-gray-700">{product.rating_avg?.toFixed(2)} ⭐</TableCell>
                    <TableCell className="text-gray-700">{product.review_count}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>

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