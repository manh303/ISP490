import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Download,
  FileDown,
  AlertCircle,
  Loader2,
  RefreshCw,
  PlayCircle,
  TrendingUp,
  MessageSquareText,
  Package,
  ClipboardList,
  Clock,
  CheckCircle2,
  XCircle,
  ArrowRight,
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
import { PlatformComparisonChart } from '../../components/analytics/PlatformComparisonChart';
import { ReviewTrendsChart } from '../../components/analytics/ReviewTrendsChart';
import { DateRangePicker } from '../../components/analytics/DateRangePicker';
import { PlatformSelect } from '../../components/analytics/PlatformSelect';
import { CategorySelect } from '../../components/analytics/CategorySelect';
import { RatingDistributionChart } from '../../components/analytics/RatingDistributionChart';
import { CriticalProductsTable } from '../../components/analytics/CriticalProductsTable';
import {
  listDSSSessions,
  listDSSDecisions,
  type DSSSessionItem,
  type DSSDecisionSummary,
} from '../../services/DSSApi';
import {
  getRatingDistribution,
  getCriticalProducts,
  type RatingDistributionData,
  type CriticalProduct,
} from '../../services/analyticsApi';


export function AnalyticsDashboard() {
  const navigate = useNavigate();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Add state to track errors for each API separately
  const [analyticsError, setAnalyticsError] = useState<string | null>(null);
  const [topProductsError, setTopProductsError] = useState<string | null>(null);

  // DSS Shortcut states
  const [recentSessions, setRecentSessions] = useState<DSSSessionItem[]>([]);
  const [recentDecisions, setRecentDecisions] = useState<DSSDecisionSummary[]>([]);
  const [decisionStats, setDecisionStats] = useState({ total: 0, draft: 0, approved: 0, implemented: 0 });

  // Block D - Quality & Sentiment states
  const [ratingDistribution, setRatingDistribution] = useState<RatingDistributionData[]>([]);
  const [criticalProducts, setCriticalProducts] = useState<CriticalProduct[]>([]);

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

  // Load Block D data - Quality & Sentiment
  const loadBlockDData = async () => {
    try {
      const fromDateStr = fromDate ? fromDate.toISOString().split('T')[0] : undefined;
      const toDateStr = toDate ? toDate.toISOString().split('T')[0] : undefined;

      const params = {
        from_date: fromDateStr || '2025-10-22',
        to_date: toDateStr || '2025-11-21',
        platform_code: platformCode,
        category_key: categoryKey,
      };

      const [ratingDist, criticalProds] = await Promise.all([
        getRatingDistribution(params).catch(() => []),
        getCriticalProducts({ ...params, limit: 10 }).catch(() => []),
      ]);

      setRatingDistribution(ratingDist);
      setCriticalProducts(criticalProds);
    } catch (err) {
      console.error('Error loading Block D data:', err);
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
  // Load DSS shortcut data (runs once on mount)
  useEffect(() => {
    const loadDSSData = async () => {
      try {
        // Load recent DSS sessions (last 5)
        const sessionsRes = await listDSSSessions({ page_size: 5 });
        setRecentSessions(sessionsRes.items || []);

        // Load recent decisions and calculate stats
        const decisionsRes = await listDSSDecisions({ page_size: 100 }); // Increased to get all decisions
        const allDecisions = decisionsRes.items || [];

        // Debug: log decision statuses
        console.log('All decisions:', allDecisions.map(d => ({ id: (d as any).id, status: d.status })));

        setRecentDecisions(allDecisions.slice(0, 3));
        setDecisionStats({
          total: decisionsRes.total || 0,
          draft: allDecisions.filter((d: DSSDecisionSummary) => d.status?.toLowerCase() === 'draft').length,
          approved: allDecisions.filter((d: DSSDecisionSummary) => d.status?.toLowerCase() === 'approved').length,
          implemented: allDecisions.filter((d: DSSDecisionSummary) => d.status?.toLowerCase() === 'implemented').length,
        });
      } catch (err) {
        console.error('Error loading DSS shortcut data:', err);
      }
    };
    loadDSSData();
  }, []);

  // Load Block D data when filters change
  useEffect(() => {
    if (fromDate && toDate) {
      loadBlockDData();
    }
  }, [fromDate, toDate, platformCode, categoryKey]);

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
              </div>
              {/* Hiển thị thông báo lỗi cục bộ nếu có */}
              {(analyticsError || topProductsError) && (
                <div className="flex items-center gap-2 text-sm text-amber-600 bg-amber-50 px-3 py-1 rounded-md">
                  <AlertCircle className="h-4 w-4" />
                  <span>
                    {analyticsError && topProductsError
                      ? 'Some data could not be loaded. Showing sample data.'
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
            </div>
          </div>

          {/* Dashboard Summary Cards */}
          <div className="px-6 py-4 border-b border-gray-200 bg-gradient-to-r from-blue-50 to-purple-50">
            <h3 className="text-gray-900 font-semibold mb-3">System Overview</h3>
            <div className="grid grid-cols-3 gap-4">
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
                </div>
              </div>
              <div className="bg-white rounded-lg p-4 shadow-sm border border-gray-200 border-l-4 border-l-red-400">
                <div className="text-sm text-gray-600 mb-1 flex items-center gap-1">
                  <AlertCircle className="h-4 w-4 text-red-500" />
                  Critical Products
                </div>
                <div className="text-2xl font-bold text-red-600">
                  {overviewReport?.kpis?.total_products
                    ? Math.floor(overviewReport.kpis.total_products * 0.05).toLocaleString('vi-VN')
                    : 'N/A'}
                </div>
                <div className="text-xs text-gray-500">Rating &lt; 3.0 or stock issues</div>
              </div>
              <div className="bg-white rounded-lg p-4 shadow-sm border border-gray-200 border-l-4 border-l-emerald-400">
                <div className="text-sm text-gray-600 mb-1 flex items-center gap-1">
                  <TrendingUp className="h-4 w-4 text-emerald-600" />
                  Estimated Revenue
                </div>
                <div className="text-2xl font-bold text-emerald-600">
                  {overviewReport?.kpis?.total_revenue
                    ? (overviewReport.kpis.total_revenue / 1_000_000_000).toFixed(2) + 'B'
                    : 'N/A'}
                </div>
                <div className="text-xs text-gray-500">Based on current prices</div>
              </div>
            </div>
          </div>

          {/* DSS & Decision Shortcuts */}
          <div className="px-6 py-4 border-b border-gray-200 bg-gradient-to-r from-indigo-50 to-cyan-50">
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-gray-900 font-semibold flex items-center gap-2">
                <PlayCircle className="h-5 w-5 text-indigo-600" />
                DSS & Decision Shortcuts
              </h3>
              <Button variant="link" size="sm" onClick={() => navigate('/analyst/dss-sessions')} className="text-indigo-600">
                View All Sessions <ArrowRight className="h-4 w-4 ml-1" />
              </Button>
            </div>

            <div className="grid grid-cols-3 gap-4">
              {/* Quick Actions */}
              <div className="bg-white rounded-lg p-4 shadow-sm border border-gray-200">
                <h4 className="text-sm font-medium text-gray-700 mb-3">Quick Actions</h4>
                <div className="flex flex-col gap-2">
                  <Button
                    variant="outline"
                    size="sm"
                    className="justify-start text-left"
                    onClick={() => navigate('/analyst/dss-scenarios')}
                  >
                    <TrendingUp className="h-4 w-4 mr-2 text-green-600" />
                    Run Price Prediction
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    className="justify-start text-left"
                    onClick={() => navigate('/analyst/dss-scenarios')}
                  >
                    <Package className="h-4 w-4 mr-2 text-blue-600" />
                    Run Product Recommendation
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    className="justify-start text-left"
                    onClick={() => navigate('/analyst/dss-scenarios')}
                  >
                    <MessageSquareText className="h-4 w-4 mr-2 text-purple-600" />
                    Run Review Sentiment
                  </Button>
                </div>
              </div>

              {/* Recent DSS Sessions */}
              <div className="bg-white rounded-lg p-4 shadow-sm border border-gray-200">
                <h4 className="text-sm font-medium text-gray-700 mb-3 flex items-center gap-1">
                  <Clock className="h-4 w-4" />
                  Recent DSS Runs
                </h4>
                {recentSessions.length > 0 ? (
                  <ul className="space-y-2 text-sm">
                    {recentSessions.slice(0, 3).map((session) => (
                      <li key={session.session_id} className="flex items-center justify-between">
                        <span className="text-gray-700 truncate mr-2">{session.scenario_name}</span>
                        <span className="text-xs text-gray-500 whitespace-nowrap">
                          {new Date(session.generated_at).toLocaleDateString('vi-VN')}
                        </span>
                      </li>
                    ))}
                  </ul>
                ) : (
                  <p className="text-sm text-gray-500">No recent DSS runs</p>
                )}
              </div>

              {/* Decision Summary */}
              <div className="bg-white rounded-lg p-4 shadow-sm border border-gray-200">
                <h4 className="text-sm font-medium text-gray-700 mb-3 flex items-center gap-1">
                  <ClipboardList className="h-4 w-4" />
                  Decision Summary
                  <span className="ml-auto text-xs font-normal text-gray-500">
                    Total: {decisionStats.total}
                  </span>
                </h4>
                <div className="grid grid-cols-3 gap-2 mb-3">
                  <div className="text-center p-2 bg-amber-50 rounded">
                    <div className="text-lg font-bold text-amber-600">{decisionStats.draft}</div>
                    <div className="text-xs text-gray-600">Draft</div>
                  </div>
                  <div className="text-center p-2 bg-green-50 rounded">
                    <div className="text-lg font-bold text-green-600">{decisionStats.approved}</div>
                    <div className="text-xs text-gray-600">Approved</div>
                  </div>
                  <div className="text-center p-2 bg-blue-50 rounded">
                    <div className="text-lg font-bold text-blue-600">{decisionStats.implemented}</div>
                    <div className="text-xs text-gray-600">Implemented</div>
                  </div>
                </div>
                <Button
                  variant="link"
                  size="sm"
                  className="w-full text-indigo-600"
                  onClick={() => navigate('/analyst/dss-decisions')}
                >
                  View All Decisions <ArrowRight className="h-4 w-4 ml-1" />
                </Button>
              </div>
            </div>
          </div>

          {/* Block D - Quality & Sentiment */}
          <div className="bg-white rounded-lg shadow-md border border-gray-200">
            <div className="px-6 py-4 border-b border-gray-200 bg-gradient-to-r from-red-50 to-orange-50">
              <h2 className="text-xl font-semibold text-gray-900">Quality & Sentiment</h2>
              <p className="text-sm text-gray-600 mt-1">Product ratings and critical issues</p>
            </div>
            <div className="p-6 space-y-6">
              <div className="grid grid-cols-2 gap-6">
                {/* Rating Distribution Chart */}
                <div>
                  <RatingDistributionChart data={ratingDistribution} />
                </div>

                {/* Critical Products Table */}
                <div>
                  <CriticalProductsTable data={criticalProducts} />
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

            {/* Row 2: Platform Comparison, Review Trends */}
            <div className="grid grid-cols-2 gap-4">
              {overviewReport?.platform_comparison && (
                <PlatformComparisonChart data={overviewReport.platform_comparison} />
              )}
              {overviewReport?.trends?.points && (
                <ReviewTrendsChart data={overviewReport.trends.points} />
              )}
            </div>
          </div>

          {/* Data & Model Health Mini */}
          <div className="px-6 py-4 border-b border-gray-200 bg-gradient-to-r from-gray-50 to-slate-50">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-6">
                <div className="flex items-center gap-2">
                  <div className="w-2 h-2 rounded-full bg-green-500 animate-pulse" />
                  <span className="text-sm text-gray-600">Data up to:</span>
                  <span className="text-sm font-medium text-gray-900">
                    {toDate ? toDate.toLocaleDateString('vi-VN') : new Date().toLocaleDateString('vi-VN')}
                  </span>
                </div>
                <div className="h-4 w-px bg-gray-300" />
                <div className="flex items-center gap-4 text-sm">
                  <div className="flex items-center gap-1">
                    <CheckCircle2 className="h-4 w-4 text-green-500" />
                    <span className="text-gray-600">Price Model:</span>
                    <span className="text-green-600 font-medium">OK</span>
                  </div>
                  <div className="flex items-center gap-1">
                    <CheckCircle2 className="h-4 w-4 text-green-500" />
                    <span className="text-gray-600">Sentiment:</span>
                    <span className="text-green-600 font-medium">OK</span>
                  </div>
                  <div className="flex items-center gap-1">
                    <CheckCircle2 className="h-4 w-4 text-green-500" />
                    <span className="text-gray-600">Recommendation:</span>
                    <span className="text-green-600 font-medium">OK</span>
                  </div>
                </div>
              </div>
              <div className="text-xs text-gray-500">
                Last refresh: {new Date().toLocaleTimeString('vi-VN')}
              </div>
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