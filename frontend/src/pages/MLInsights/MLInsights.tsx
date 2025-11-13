import React, { useEffect, useState } from 'react';
import {
  getMLSummary,
  getPriceOptimization,
  getDemandForecast,
  getWeeklySalesForecast,
  getSalesTrend,
  getSeasonality,
  MLSummary as MLSummaryType,
  PriceOptimization as PriceOptimizationType,
  DemandForecast as DemandForecastType,
  WeeklySalesForecast as WeeklySalesForecastType,
  SalesTrend as SalesTrendType,
  Seasonality as SeasonalityType,
} from '../../services/MLInsightsApi';
import MLSummaryCards from '../../components/analytics/MLSummaryCards';
import PriceOptimization from '../../components/analytics/PriceOptimization';
import DemandForecast from '../../components/analytics/DemandForecast';
import WeeklySalesForecast from '../../components/analytics/WeeklySalesForecast';
import SalesTrend from '../../components/analytics/SalesTrend';
import Seasonality from '../../components/analytics/Seasonality';

const MLInsights: React.FC = () => {
  const [summary, setSummary] = useState<MLSummaryType | null>(null);
  const [priceOptimization, setPriceOptimization] = useState<PriceOptimizationType[]>([]);
  const [demandForecast, setDemandForecast] = useState<DemandForecastType[]>([]);
  const [weeklySales, setWeeklySales] = useState<WeeklySalesForecastType[]>([]);
  const [salesTrend, setSalesTrend] = useState<SalesTrendType[]>([]);
  const [seasonality, setSeasonality] = useState<SeasonalityType[]>([]);

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<'overview' | 'price' | 'demand' | 'forecast'>('overview');

  useEffect(() => {
    fetchAllData();
  }, []);

  const fetchAllData = async () => {
    setLoading(true);
    setError(null);

    try {
      const [
        summaryData,
        priceData,
        demandData,
        weeklyData,
        trendData,
        seasonData,
      ] = await Promise.all([
        getMLSummary(),
        getPriceOptimization(1000),
        getDemandForecast(1000),
        getWeeklySalesForecast(),
        getSalesTrend(),
        getSeasonality(),
      ]);

      setSummary(summaryData);
      setPriceOptimization(priceData);
      setDemandForecast(demandData);
      setWeeklySales(weeklyData);
      setSalesTrend(trendData);
      setSeasonality(seasonData);
    } catch (err: any) {
      console.error('Error fetching ML insights:', err);
      setError(err.response?.data?.message || err.message || 'Failed to load ML insights');
    } finally {
      setLoading(false);
    }
  };

  const tabs = [
    { id: 'overview', label: '📊 Overview', icon: '📊' },
    { id: 'price', label: '🏷️ Price Optimization', icon: '🏷️' },
    { id: 'demand', label: '📦 Demand Forecast', icon: '📦' },
    { id: 'forecast', label: '📈 Sales Forecast', icon: '📈' },
  ];

  if (error) {
    return (
      <div className="min-h-screen bg-gray-50 p-6">
        <div className="max-w-7xl mx-auto">
          <div className="bg-red-50 border border-red-200 rounded-lg p-6 text-center">
            <div className="text-4xl mb-4">❌</div>
            <h2 className="text-2xl font-bold text-red-800 mb-2">Error Loading ML Insights</h2>
            <p className="text-red-600 mb-4">{error}</p>
            <button
              onClick={fetchAllData}
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
          <h1 className="text-4xl font-bold text-gray-900 mb-2">🤖 ML Insights Dashboard</h1>
          <p className="text-gray-600">
            AI-powered analytics and predictions for your e-commerce business
          </p>
          <button
            onClick={fetchAllData}
            disabled={loading}
            className="mt-4 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {loading ? '🔄 Refreshing...' : '🔄 Refresh Data'}
          </button>
        </div>

        {/* Summary Cards */}
        <MLSummaryCards data={summary} loading={loading} />

        {/* Tabs */}
        <div className="bg-white rounded-lg shadow-md mb-6">
          <div className="border-b border-gray-200">
            <nav className="flex -mb-px overflow-x-auto">
              {tabs.map((tab) => (
                <button
                  key={tab.id}
                  onClick={() => setActiveTab(tab.id as any)}
                  className={`flex-shrink-0 px-6 py-4 text-sm font-medium border-b-2 transition-colors ${
                    activeTab === tab.id
                      ? 'border-blue-500 text-blue-600'
                      : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                  }`}
                >
                  <span className="flex items-center">
                    <span className="mr-2">{tab.icon}</span>
                    {tab.label}
                  </span>
                </button>
              ))}
            </nav>
          </div>
        </div>

        {/* Tab Content */}
        <div className="space-y-6">
          {activeTab === 'overview' && (
            <>
              <SalesTrend data={salesTrend} loading={loading} />
              <WeeklySalesForecast data={weeklySales} loading={loading} />
              <Seasonality data={seasonality} loading={loading} />
            </>
          )}

          {activeTab === 'price' && (
            <PriceOptimization data={priceOptimization} loading={loading} />
          )}

          {activeTab === 'demand' && (
            <DemandForecast data={demandForecast} loading={loading} />
          )}

          {activeTab === 'forecast' && (
            <>
              <SalesTrend data={salesTrend} loading={loading} />
              <WeeklySalesForecast data={weeklySales} loading={loading} />
              <Seasonality data={seasonality} loading={loading} />
            </>
          )}
        </div>

        {/* Loading Overlay */}
        {loading && (
          <div className="fixed inset-0 bg-black bg-opacity-20 flex items-center justify-center z-50">
            <div className="bg-white rounded-lg p-8 shadow-xl">
              <div className="flex flex-col items-center">
                <div className="animate-spin rounded-full h-16 w-16 border-b-2 border-blue-600 mb-4"></div>
                <p className="text-lg font-semibold text-gray-700">Loading ML Insights...</p>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default MLInsights;
