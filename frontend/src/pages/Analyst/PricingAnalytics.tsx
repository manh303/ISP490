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
  getPriceDistribution,
  getPriceVsRevenue,
  getPlatforms,
  getCategories,
  type PriceDistribution,
  type PriceVsRevenueItem,
  type Platform,
  type Category,
  type GetPriceDistributionParams,
  type GetPriceVsRevenueParams,
} from '../../services/analyticsApi';
import { PriceVsRatingChart } from '../../components/analytics/PriceVsRatingChart';
import { PlatformSelect } from '../../components/analytics/PlatformSelect';
import { CategorySelect } from '../../components/analytics/CategorySelect';
import { DateRangePicker } from '../../components/analytics/DateRangePicker';

export function PricingAnalytics() {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Filter states
  const [fromDate, setFromDate] = useState<Date>();
  const [toDate, setToDate] = useState<Date>();
  const [platformCode, setPlatformCode] = useState<string>('tiki'); // Default to tiki
  const [categoryKey, setCategoryKey] = useState<string>();

  // Analytics data state
  const [priceDistribution, setPriceDistribution] = useState<PriceDistribution | null>(null);
  const [priceVsRevenue, setPriceVsRevenue] = useState<PriceVsRevenueItem[] | null>(null);

  // Load analytics data
  const loadAnalyticsData = async () => {
    try {
      setLoading(true);
      setError(null);

      const fromDateStr = fromDate ? fromDate.toISOString().split('T')[0] : undefined;
      const toDateStr = toDate ? toDate.toISOString().split('T')[0] : undefined;

      const priceDistParams: GetPriceDistributionParams = {
        from_date: fromDateStr || '2025-10-22',
        to_date: toDateStr || '2025-11-21',
        platform_code: platformCode || 'tiki',
        category_key: categoryKey,
      };

      const priceVsRevenueParams: GetPriceVsRevenueParams = {
        from_date: fromDateStr || '2025-10-22',
        to_date: toDateStr || '2025-11-21',
        platform_code: platformCode || 'tiki',
        category_key: categoryKey,
        limit: 100,
      };

      const [
        priceDistData,
        priceVsRevenueData,
      ] = await Promise.all([
        getPriceDistribution(priceDistParams),
        getPriceVsRevenue(priceVsRevenueParams),
      ]);

      setPriceDistribution(priceDistData);
      setPriceVsRevenue(priceVsRevenueData);
    } catch (err) {
      console.error('Error loading pricing analytics data:', err);
      setError('Unable to load pricing analytics data. Please try again.');
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
      loadAnalyticsData();
    }
  }, [fromDate, toDate, platformCode, categoryKey]);  const handleRefresh = () => {
    window.location.reload();
  };

  if (loading) {
    return (
      <div className="border border-gray-200 bg-white rounded-lg overflow-hidden shadow-sm flex items-center justify-center" style={{ height: '800px' }}>
        <div className="text-center">
          <Loader2 className="h-12 w-12 text-blue-500 animate-spin mx-auto mb-4" />
          <p className="text-gray-600">Loading pricing analytics data...</p>
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
            Retry
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
                  onValueChange={(value) => setPlatformCode(value || 'tiki')}
                />
              </div>
              <div className="flex items-center gap-2">
                <label className="text-sm font-medium">Category:</label>
                <CategorySelect
                  value={categoryKey}
                  onValueChange={setCategoryKey}
                  platformCode={platformCode}
                />
              </div>
            </div>
          </div>

          {/* Price Distribution Summary */}
          {priceDistribution && (
            <div className="px-6 py-4 border-b border-gray-200 bg-gradient-to-r from-green-50 to-blue-50">
              <h3 className="text-gray-900 font-semibold mb-3">Price Distribution</h3>
              <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-5 gap-4">
                <div className="bg-white rounded-lg p-4 shadow-sm border border-gray-200 flex flex-col items-start">
                  <div className="text-sm text-gray-600 mb-1">Lowest Price</div>
                  <div className="font-bold text-gray-900 break-words text-base md:text-xl max-w-[120px] md:max-w-full" style={{wordBreak: 'break-word'}}>
                    {priceDistribution.min_price?.toLocaleString('vi-VN')} VND
                  </div>
                </div>
                <div className="bg-white rounded-lg p-4 shadow-sm border border-gray-200 flex flex-col items-start">
                  <div className="text-sm text-gray-600 mb-1">P25</div>
                  <div className="font-bold text-blue-600 break-words text-base md:text-xl max-w-[120px] md:max-w-full" style={{wordBreak: 'break-word'}}>
                    {priceDistribution.p25_price?.toLocaleString('vi-VN')} VND
                  </div>
                </div>
                <div className="bg-white rounded-lg p-4 shadow-sm border border-gray-200 flex flex-col items-start">
                  <div className="text-sm text-gray-600 mb-1">Median Price</div>
                  <div className="font-bold text-purple-600 break-words text-base md:text-xl max-w-[120px] md:max-w-full" style={{wordBreak: 'break-word'}}>
                    {priceDistribution.median_price?.toLocaleString('vi-VN')} VND
                  </div>
                </div>
                <div className="bg-white rounded-lg p-4 shadow-sm border border-gray-200 flex flex-col items-start">
                  <div className="text-sm text-gray-600 mb-1">P75</div>
                  <div className="font-bold text-green-600 break-words text-base md:text-xl max-w-[120px] md:max-w-full" style={{wordBreak: 'break-word'}}>
                    {priceDistribution.p75_price?.toLocaleString('vi-VN')} VND
                  </div>
                </div>
                <div className="bg-white rounded-lg p-4 shadow-sm border border-gray-200 flex flex-col items-start">
                  <div className="text-sm text-gray-600 mb-1">Highest Price</div>
                  <div className="font-bold text-red-600 break-words text-base md:text-xl max-w-[120px] md:max-w-full" style={{wordBreak: 'break-word'}}>
                    {priceDistribution.max_price?.toLocaleString('vi-VN')} VND
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* Pricing Charts */}
          <div className="px-6 py-4 border-b border-gray-200 bg-white overflow-auto">
            <h3 className="text-gray-900 font-semibold mb-4">Pricing Analysis Charts</h3>

            <div className="grid grid-cols-1 gap-4">
              {priceVsRevenue && (
                <PriceVsRatingChart data={priceVsRevenue} />
              )}
            </div>
          </div>

          {/* Footer */}
          <div className="px-6 py-4 border-t border-gray-200 bg-gray-50 flex justify-between items-center">
            <div className="text-gray-600 text-sm">
              Pricing Analytics - Price Analysis
            </div>
            <Button>
              <FileDown className="h-4 w-4 mr-2" />
              Export Report
            </Button>
          </div>
        </div>
      </div>
    </div>
  );
}