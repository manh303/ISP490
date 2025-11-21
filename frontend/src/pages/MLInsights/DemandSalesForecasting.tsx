import React, { useEffect, useState } from 'react';
import {
  getDemandForecast,
  getWeeklySalesForecast,
  getSalesTrend,
  getSeasonality,
  DemandForecast as DemandForecastType,
  WeeklySalesForecast as WeeklySalesForecastType,
  SalesTrend as SalesTrendType,
  Seasonality as SeasonalityType,
} from '../../services/MLInsightsApi';
import DemandForecast from '../../components/analytics/DemandForecast';
import WeeklySalesForecast from '../../components/analytics/WeeklySalesForecast';
import SalesTrend from '../../components/analytics/SalesTrend';
import Seasonality from '../../components/analytics/Seasonality';

const DemandSalesForecasting: React.FC = () => {
  const [demandForecast, setDemandForecast] = useState<DemandForecastType[]>([]);
  const [weeklySales, setWeeklySales] = useState<WeeklySalesForecastType[]>([]);
  const [salesTrend, setSalesTrend] = useState<SalesTrendType[]>([]);
  const [seasonality, setSeasonality] = useState<SeasonalityType[]>([]);

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchData();
  }, []);

  const fetchData = async () => {
    setLoading(true);
    setError(null);

    try {
      const [
        demandData,
        weeklyData,
        trendData,
        seasonData,
      ] = await Promise.all([
        getDemandForecast(1000),
        getWeeklySalesForecast(),
        getSalesTrend(),
        getSeasonality(),
      ]);

      setDemandForecast(demandData);
      setWeeklySales(weeklyData);
      setSalesTrend(trendData);
      setSeasonality(seasonData);
    } catch (err: any) {
      console.error('Error fetching demand & sales forecasting:', err);
      setError(err.response?.data?.message || err.message || 'Failed to load demand & sales forecasting');
    } finally {
      setLoading(false);
    }
  };

  if (error) {
    return (
      <div className="min-h-screen bg-gray-50 p-6">
        <div className="max-w-7xl mx-auto">
          <div className="bg-red-50 border border-red-200 rounded-lg p-6 text-center">
            <div className="text-4xl mb-4">❌</div>
            <h2 className="text-2xl font-bold text-red-800 mb-2">Error Loading Demand & Sales Forecasting</h2>
            <p className="text-red-600 mb-4">{error}</p>
            <button
              onClick={fetchData}
              className="px-6 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 transition-colors"
            >
              Retry
            </button>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50 p-6">
      <div className="max-w-7xl mx-auto">
        {/* Header */}
        <div className="mb-8">
          <h1 className="text-4xl font-bold text-gray-900 mb-2">📈 Demand & Sales Forecasting</h1>
          <p className="text-gray-600">
            Dự báo nhu cầu và doanh số bán hàng theo thời gian
          </p>
          <button
            onClick={fetchData}
            disabled={loading}
            className="mt-4 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {loading ? '🔄 Refreshing...' : '🔄 Refresh Data'}
          </button>
        </div>

        {/* Charts */}
        <div className="space-y-6">
          <DemandForecast data={demandForecast} loading={loading} />
          <WeeklySalesForecast data={weeklySales} loading={loading} />
          <SalesTrend data={salesTrend} loading={loading} />
          <Seasonality data={seasonality} loading={loading} />
        </div>

        {/* Loading Overlay */}
        {loading && (
          <div className="fixed inset-0 bg-black bg-opacity-20 flex items-center justify-center z-50">
            <div className="bg-white rounded-lg p-8 shadow-xl">
              <div className="flex flex-col items-center">
                <div className="animate-spin rounded-full h-16 w-16 border-b-2 border-blue-600 mb-4"></div>
                <p className="text-lg font-semibold text-gray-700">Loading Demand & Sales Forecasting...</p>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default DemandSalesForecasting;