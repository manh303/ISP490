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
  getReviewTrends,
  getSentimentDistribution,
  type ReviewTrendsResponse,
  type SentimentDistributionResponse,
} from '../../services/analyticsApi';
import { ReviewTrendsChart } from '../../components/analytics/ReviewTrendsChart';
import { SentimentDistributionChart } from '../../components/analytics/SentimentDistributionChart';

export function ReviewAnalytics() {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Analytics data state
  const [reviewTrends, setReviewTrends] = useState<ReviewTrendsResponse | null>(null);
  const [sentimentDistribution, setSentimentDistribution] = useState<SentimentDistributionResponse | null>(null);

  // Load analytics data
  useEffect(() => {
    const loadAnalyticsData = async () => {
      try {
        setLoading(true);
        setError(null);

        const [
          trendsData,
          sentimentData,
        ] = await Promise.all([
          getReviewTrends({ days: 7 }),
          getSentimentDistribution(),
        ]);

        setReviewTrends(trendsData);
        setSentimentDistribution(sentimentData);
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
            <h3 className="text-gray-900 font-semibold mb-4">Biểu Đồ Phân Tích Đánh Giá</h3>

            {/* Row 1: Review Trends, Sentiment Distribution */}
            <div className="grid grid-cols-2 gap-4">
              {reviewTrends && (
                <ReviewTrendsChart data={reviewTrends.data} />
              )}
              {sentimentDistribution && (
                <SentimentDistributionChart data={sentimentDistribution.data} />
              )}
            </div>
          </div>

          {/* Footer */}
          <div className="px-6 py-4 border-t border-gray-200 bg-gray-50 flex justify-between items-center">
            <div className="text-gray-600 text-sm">
              Review Analytics - Phân tích đánh giá
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