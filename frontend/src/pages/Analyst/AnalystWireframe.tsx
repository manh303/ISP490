import { useState, useEffect } from 'react';
import { 
  Download, 
  FileDown, 
  Lightbulb,
  AlertCircle,
  CheckCircle,
  FileText,
  Loader2,
  RefreshCw
} from 'lucide-react';
import { Button } from '../../components/ui/figma/button';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '../../components/ui/figma/select';
import { Badge } from '../../components/ui/figma/badge';
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
  getReviewTrends,
  getCategoryPerformance,
  getSentimentDistribution,
  getPriceSegments,
  getPriceVsRating,
  getPlatformComparison,
  getPlatformPriceComparison,
  getDashboardSummary,
  type TopRatedProductsResponse,
  type RatingDistributionResponse,
  type ReviewTrendsResponse,
  type CategoryPerformanceResponse,
  type SentimentDistributionResponse,
  type PriceSegmentsResponse,
  type PriceVsRatingResponse,
  type PlatformComparisonResponse,
  type PlatformPriceComparisonResponse,
  type DashboardSummaryResponse,
} from '../../services/analyticsApi';
import { TopRatedProductsChart } from '../../components/analytics/TopRatedProductsChart';
import { RatingDistributionChart } from '../../components/analytics/RatingDistributionChart';
import { ReviewTrendsChart } from '../../components/analytics/ReviewTrendsChart';
import { CategoryPerformanceChart } from '../../components/analytics/CategoryPerformanceChart';
import { SentimentDistributionChart } from '../../components/analytics/SentimentDistributionChart';
import { PriceSegmentsChart } from '../../components/analytics/PriceSegmentsChart';
import { PriceVsRatingChart } from '../../components/analytics/PriceVsRatingChart';
import { PlatformComparisonChart } from '../../components/analytics/PlatformComparisonChart';
import { PlatformPriceComparisonChart } from '../../components/analytics/PlatformPriceComparisonChart';

export function AnalystWireframe() {
  const [itemsPerPage, setItemsPerPage] = useState('5');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  
  // Analytics data state
  const [dashboardSummary, setDashboardSummary] = useState<DashboardSummaryResponse | null>(null);
  const [topRatedProducts, setTopRatedProducts] = useState<TopRatedProductsResponse | null>(null);
  const [ratingDistribution, setRatingDistribution] = useState<RatingDistributionResponse | null>(null);
  const [reviewTrends, setReviewTrends] = useState<ReviewTrendsResponse | null>(null);
  const [categoryPerformance, setCategoryPerformance] = useState<CategoryPerformanceResponse | null>(null);
  const [sentimentDistribution, setSentimentDistribution] = useState<SentimentDistributionResponse | null>(null);
  const [priceSegments, setPriceSegments] = useState<PriceSegmentsResponse | null>(null);
  const [priceVsRating, setPriceVsRating] = useState<PriceVsRatingResponse | null>(null);
  const [platformComparison, setPlatformComparison] = useState<PlatformComparisonResponse | null>(null);
  const [platformPriceComparison, setPlatformPriceComparison] = useState<PlatformPriceComparisonResponse | null>(null);

  // Load analytics data
  useEffect(() => {
    const loadAnalyticsData = async () => {
      try {
        setLoading(true);
        setError(null);

        const [
          summaryData,
          topRatedData,
          ratingDistData,
          trendsData,
          categoryData,
          sentimentData,
          priceSegData,
          priceVsRatingData,
          platformCompData,
          platformPriceData,
        ] = await Promise.all([
          getDashboardSummary(),
          getTopRatedProducts({ limit: 10 }),
          getRatingDistribution(),
          getReviewTrends({ days: 7 }),
          getCategoryPerformance(),
          getSentimentDistribution(),
          getPriceSegments(),
          getPriceVsRating(),
          getPlatformComparison(),
          getPlatformPriceComparison(),
        ]);

        setDashboardSummary(summaryData);
        setTopRatedProducts(topRatedData);
        setRatingDistribution(ratingDistData);
        setReviewTrends(trendsData);
        setCategoryPerformance(categoryData);
        setSentimentDistribution(sentimentData);
        setPriceSegments(priceSegData);
        setPriceVsRating(priceVsRatingData);
        setPlatformComparison(platformCompData);
        setPlatformPriceComparison(platformPriceData);
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

  // Mock DSS recommendations (you can integrate real DSS API later)
  const dssRecommendations = [
    {
      id: 1,
      title: 'Xu hướng sản phẩm chất lượng cao',
      type: 'Positive',
      description: `${categoryPerformance?.data[0]?.category || 'Laptops'} có ${categoryPerformance?.data[0]?.high_rated_count || 0} sản phẩm đánh giá cao. Đề xuất tập trung marketing vào danh mục này.`,
      impact: 'High',
    },
    {
      id: 2,
      title: 'Cảnh báo sản phẩm chưa đánh giá',
      type: 'Warning',
      description: `Có ${ratingDistribution?.data[0]?.product_count || 0} sản phẩm chưa có đánh giá. Cần khuyến khích khách hàng đánh giá sau mua hàng.`,
      impact: 'Medium',
    },
    {
      id: 3,
      title: 'Phân khúc giá tiềm năng',
      type: 'Opportunity',
      description: `Phân khúc ${priceSegments?.data[0]?.price_segment || 'Mid-range'} có ${priceSegments?.data[0]?.product_count || 0} sản phẩm với rating trung bình ${priceSegments?.data[0]?.avg_rating.toFixed(2) || 0}. Cơ hội tăng trưởng tốt.`,
      impact: 'High',
    },
    {
      id: 4,
      title: 'Xu hướng đánh giá tích cực',
      type: 'Positive',
      description: `${sentimentDistribution?.data.find(s => s.sentiment === 'Excellent')?.product_count || 0} sản phẩm có đánh giá xuất sắc. Tiếp tục duy trì chất lượng sản phẩm.`,
      impact: 'Medium',
    },
    {
      id: 5,
      title: 'Tối ưu hóa giá sản phẩm',
      type: 'Opportunity',
      description: `Giá trung bình toàn hệ thống: ${(dashboardSummary?.summary.avg_price || 0).toLocaleString('vi-VN')} ₫. Xem xét điều chỉnh giá theo phân khúc để tăng cạnh tranh.`,
      impact: 'High',
    },
  ];

  const displayedRecommendations = dssRecommendations.slice(0, parseInt(itemsPerPage));

  const getTypeIcon = (type: string) => {
    switch (type) {
      case 'Positive':
        return <CheckCircle className="h-4 w-4 text-green-500" />;
      case 'Warning':
        return <AlertCircle className="h-4 w-4 text-yellow-500" />;
      case 'Critical':
        return <AlertCircle className="h-4 w-4 text-red-500" />;
      case 'Opportunity':
        return <Lightbulb className="h-4 w-4 text-blue-500" />;
      default:
        return <AlertCircle className="h-4 w-4 text-gray-500" />;
    }
  };

  const getTypeVariant = (type: string): "default" | "secondary" | "destructive" | "outline" => {
    switch (type) {
      case 'Positive':
        return 'default';
      case 'Warning':
        return 'secondary';
      case 'Critical':
        return 'destructive';
      case 'Opportunity':
        return 'outline';
      default:
        return 'secondary';
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
              <div className="flex items-center gap-2">
                <span className="text-gray-600 text-sm">Hiển thị:</span>
                <Select value={itemsPerPage} onValueChange={setItemsPerPage}>
                  <SelectTrigger className="w-24 bg-white border-gray-300">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="5">5</SelectItem>
                    <SelectItem value="6">6</SelectItem>
                    <SelectItem value="10">10</SelectItem>
                  </SelectContent>
                </Select>
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
                  <div className="text-sm text-gray-600 mb-1">SP chất lượng cao</div>
                  <div className="text-2xl font-bold text-green-600">
                    {dashboardSummary?.summary?.high_rated_products?.toLocaleString('vi-VN')}
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* Dashboard Charts */}
          <div className="px-6 py-4 border-b border-gray-200 bg-white overflow-auto">
            <h3 className="text-gray-900 font-semibold mb-4">Biểu Đồ Phân Tích</h3>
            
            {/* Row 1: Top Products, Rating Distribution, Review Trends */}
            <div className="grid grid-cols-3 gap-4 mb-4">
              {topRatedProducts && (
                <TopRatedProductsChart data={topRatedProducts.data} />
              )}
              {ratingDistribution && (
                <RatingDistributionChart data={ratingDistribution.data} />
              )}
              {reviewTrends && (
                <ReviewTrendsChart data={reviewTrends.data} />
              )}
            </div>

            {/* Row 2: Category Performance, Sentiment, Price Segments */}
            <div className="grid grid-cols-3 gap-4 mb-4">
              {categoryPerformance && (
                <CategoryPerformanceChart data={categoryPerformance.data} />
              )}
              {sentimentDistribution && (
                <SentimentDistributionChart data={sentimentDistribution.data} />
              )}
              {priceSegments && (
                <PriceSegmentsChart data={priceSegments.data} />
              )}
            </div>

            {/* Row 3: Price vs Rating, Platform Comparison, Platform Price Comparison */}
            <div className="grid grid-cols-3 gap-4">
              {priceVsRating && (
                <PriceVsRatingChart data={priceVsRating.data} />
              )}
              {platformComparison && (
                <PlatformComparisonChart data={platformComparison.data} />
              )}
              {platformPriceComparison && (
                <PlatformPriceComparisonChart data={platformPriceComparison.data} />
              )}
            </div>
          </div>

          {/* DSS Recommendations Section */}
          <div className="flex-1 overflow-auto px-6 py-4 bg-white">
            <div className="mb-4 flex items-center gap-2">
              <Lightbulb className="h-5 w-5 text-blue-500" />
              <h3 className="text-gray-900 font-semibold">Đề xuất DSS (Decision Support System)</h3>
            </div>
            
            <Table>
              <TableHeader>
                <TableRow className="border-gray-200 hover:bg-gray-50">
                  <TableHead className="text-gray-600 w-12">ID</TableHead>
                  <TableHead className="text-gray-600 w-48">Tiêu đề</TableHead>
                  <TableHead className="text-gray-600 w-96">Mô tả</TableHead>
                  <TableHead className="text-gray-600 w-28">Loại</TableHead>
                  <TableHead className="text-gray-600 w-24">Mức độ</TableHead>
                  <TableHead className="text-gray-600 w-28">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {displayedRecommendations.map((rec) => (
                  <TableRow key={rec.id} className="border-gray-200 hover:bg-gray-50">
                    <TableCell className="text-gray-700">{rec.id}</TableCell>
                    <TableCell className="text-gray-900">
                      <div className="flex items-center gap-2">
                        {getTypeIcon(rec.type)}
                        <span className="line-clamp-2">{rec.title}</span>
                      </div>
                    </TableCell>
                    <TableCell className="text-gray-700 text-sm">
                      <div className="line-clamp-2">{rec.description}</div>
                    </TableCell>
                    <TableCell>
                      <Badge variant={getTypeVariant(rec.type)}>
                        {rec.type}
                      </Badge>
                    </TableCell>
                    <TableCell>
                      <Badge 
                        variant={
                          rec.impact === 'High' ? 'destructive' : 'default'
                        }
                      >
                        {rec.impact}
                      </Badge>
                    </TableCell>
                    <TableCell>
                      <div className="flex gap-2">
                        <Button size="sm" variant="outline">
                          <FileText className="h-4 w-4" />
                        </Button>
                        <Button size="sm" variant="ghost">
                          <Download className="h-4 w-4" />
                        </Button>
                      </div>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>

          {/* Footer */}
          <div className="px-6 py-4 border-t border-gray-200 bg-gray-50 flex justify-between items-center">
            <div className="text-gray-600 text-sm">
              Hiển thị {displayedRecommendations.length} / {dssRecommendations.length} đề xuất
            </div>
            <Button>
              <FileDown className="h-4 w-4 mr-2" />
              Xuất báo cáo DSS
            </Button>
          </div>
        </div>
      </div>
    </div>
  );
}