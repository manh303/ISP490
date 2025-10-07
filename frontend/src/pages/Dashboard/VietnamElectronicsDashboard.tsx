import React, { useState, useEffect } from 'react';
import PageBreadcrumb from "../../components/common/PageBreadCrumb";
import PageMeta from "../../components/common/PageMeta";
import authService from '../../services/authService';

// Types for our data
interface PlatformData {
  platform: string;
  products: number;
  revenue: number;
  growth: number;
}

interface PipelineStatus {
  status: string;
  lastRun: string;
  nextRun: string;
  totalProducts: number;
}

export default function VietnamElectronicsDashboard() {
  const [platformData, setPlatformData] = useState<PlatformData[]>([]);
  const [pipelineStatus, setPipelineStatus] = useState<PipelineStatus | null>(null);
  const [loading, setLoading] = useState(true);

  // Mock data - replace with real API calls
  useEffect(() => {
    const loadDashboardData = async () => {
      try {
        // Simulate API call
        setTimeout(() => {
          setPlatformData([
            { platform: 'Tiki', products: 1250, revenue: 2500000, growth: 12.5 },
            { platform: 'Shopee', products: 980, revenue: 1900000, growth: 8.3 },
            { platform: 'Lazada', products: 750, revenue: 1500000, growth: -2.1 },
            { platform: 'FPT Shop', products: 620, revenue: 1200000, growth: 15.7 },
            { platform: 'Sendo', products: 450, revenue: 900000, growth: 5.2 },
          ]);

          setPipelineStatus({
            status: 'Running',
            lastRun: '2025-10-06 18:30:00',
            nextRun: '2025-10-07 00:30:00',
            totalProducts: 4050
          });

          setLoading(false);
        }, 1000);
      } catch (error) {
        console.error('Failed to load dashboard data:', error);
        setLoading(false);
      }
    };

    loadDashboardData();
  }, []);

  const formatCurrency = (amount: number) => {
    return new Intl.NumberFormat('vi-VN', {
      style: 'currency',
      currency: 'VND'
    }).format(amount);
  };

  const formatNumber = (num: number) => {
    return new Intl.NumberFormat('vi-VN').format(num);
  };

  if (loading) {
    return (
      <div>
        <PageMeta
          title="Vietnam Electronics Dashboard | E-commerce DSS"
          description="Vietnam Electronics E-commerce Data Analytics Dashboard"
        />
        <PageBreadcrumb pageTitle="Vietnam Electronics Dashboard" />
        <div className="min-h-screen rounded-2xl border border-gray-200 bg-white px-5 py-7 dark:border-gray-800 dark:bg-white/[0.03] xl:px-10 xl:py-12">
          <div className="flex items-center justify-center h-64">
            <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
            <span className="ml-3 text-gray-600">Loading dashboard data...</span>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div>
      <PageMeta
        title="Vietnam Electronics Dashboard | E-commerce DSS"
        description="Vietnam Electronics E-commerce Data Analytics Dashboard"
      />
      <PageBreadcrumb pageTitle="Vietnam Electronics Dashboard" />

      <div className="rounded-2xl border border-gray-200 bg-white px-5 py-7 dark:border-gray-800 dark:bg-white/[0.03] xl:px-10 xl:py-12">

        {/* Header */}
        <div className="mb-8">
          <h1 className="text-3xl font-bold text-gray-900 dark:text-white mb-2">
            🇻🇳 Vietnam Electronics Dashboard
          </h1>
          <p className="text-gray-600 dark:text-gray-400">
            Real-time analytics from Vietnamese e-commerce platforms
          </p>
        </div>

        {/* Pipeline Status Card */}
        <div className="mb-8 rounded-lg border border-gray-200 bg-gradient-to-r from-blue-50 to-blue-100 p-6 dark:border-gray-700 dark:from-blue-900/20 dark:to-blue-800/20">
          <div className="flex items-center justify-between">
            <div>
              <h3 className="text-lg font-semibold text-gray-900 dark:text-white">
                🔄 Pipeline Status
              </h3>
              <div className="mt-2 space-y-1">
                <p className="text-sm text-gray-600 dark:text-gray-400">
                  Status: <span className={`font-semibold ${pipelineStatus?.status === 'Running' ? 'text-green-600' : 'text-red-600'}`}>
                    {pipelineStatus?.status}
                  </span>
                </p>
                <p className="text-sm text-gray-600 dark:text-gray-400">
                  Last Run: {pipelineStatus?.lastRun}
                </p>
                <p className="text-sm text-gray-600 dark:text-gray-400">
                  Next Run: {pipelineStatus?.nextRun}
                </p>
              </div>
            </div>
            <div className="text-right">
              <div className="text-2xl font-bold text-blue-600 dark:text-blue-400">
                {formatNumber(pipelineStatus?.totalProducts || 0)}
              </div>
              <div className="text-sm text-gray-600 dark:text-gray-400">Total Products</div>
            </div>
          </div>
        </div>

        {/* Platform Overview Cards */}
        <div className="mb-8">
          <h2 className="text-xl font-semibold text-gray-900 dark:text-white mb-4">
            📊 Platform Overview
          </h2>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5 gap-4">
            {platformData.map((platform, index) => (
              <div key={platform.platform} className="rounded-lg border border-gray-200 bg-white p-4 shadow-sm dark:border-gray-700 dark:bg-gray-800">
                <div className="flex items-center justify-between mb-3">
                  <h3 className="font-semibold text-gray-900 dark:text-white">
                    {platform.platform}
                  </h3>
                  <span className={`text-xs px-2 py-1 rounded-full ${
                    platform.growth > 0
                      ? 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300'
                      : 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-300'
                  }`}>
                    {platform.growth > 0 ? '+' : ''}{platform.growth}%
                  </span>
                </div>

                <div className="space-y-2">
                  <div>
                    <div className="text-lg font-bold text-gray-900 dark:text-white">
                      {formatNumber(platform.products)}
                    </div>
                    <div className="text-xs text-gray-500 dark:text-gray-400">Products</div>
                  </div>

                  <div>
                    <div className="text-sm font-semibold text-blue-600 dark:text-blue-400">
                      {formatCurrency(platform.revenue)}
                    </div>
                    <div className="text-xs text-gray-500 dark:text-gray-400">Est. Revenue</div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Quick Stats */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
          <div className="rounded-lg border border-gray-200 bg-white p-6 dark:border-gray-700 dark:bg-gray-800">
            <div className="flex items-center">
              <div className="rounded-full bg-blue-100 p-3 dark:bg-blue-900">
                <svg className="w-6 h-6 text-blue-600 dark:text-blue-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6" />
                </svg>
              </div>
              <div className="ml-4">
                <div className="text-2xl font-bold text-gray-900 dark:text-white">
                  {formatNumber(platformData.reduce((sum, p) => sum + p.products, 0))}
                </div>
                <div className="text-sm text-gray-600 dark:text-gray-400">Total Products Tracked</div>
              </div>
            </div>
          </div>

          <div className="rounded-lg border border-gray-200 bg-white p-6 dark:border-gray-700 dark:bg-gray-800">
            <div className="flex items-center">
              <div className="rounded-full bg-green-100 p-3 dark:bg-green-900">
                <svg className="w-6 h-6 text-green-600 dark:text-green-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8c-1.657 0-3 .895-3 2s1.343 2 3 2 3 .895 3 2-1.343 2-3 2m0-8c1.11 0 2.08.402 2.599 1M12 8V7m0 1v8m0 0v1m0-1c-1.11 0-2.08-.402-2.599-1" />
                </svg>
              </div>
              <div className="ml-4">
                <div className="text-2xl font-bold text-gray-900 dark:text-white">
                  {formatCurrency(platformData.reduce((sum, p) => sum + p.revenue, 0))}
                </div>
                <div className="text-sm text-gray-600 dark:text-gray-400">Total Est. Revenue</div>
              </div>
            </div>
          </div>

          <div className="rounded-lg border border-gray-200 bg-white p-6 dark:border-gray-700 dark:bg-gray-800">
            <div className="flex items-center">
              <div className="rounded-full bg-purple-100 p-3 dark:bg-purple-900">
                <svg className="w-6 h-6 text-purple-600 dark:text-purple-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 21V5a2 2 0 00-2-2H7a2 2 0 00-2 2v16m14 0h2m-2 0h-5m-9 0H3m2 0h5M9 7h1m-1 4h1m4-4h1m-1 4h1m-5 10v-5a1 1 0 011-1h2a1 1 0 011 1v5m-4 0h4" />
                </svg>
              </div>
              <div className="ml-4">
                <div className="text-2xl font-bold text-gray-900 dark:text-white">5</div>
                <div className="text-sm text-gray-600 dark:text-gray-400">Active Platforms</div>
              </div>
            </div>
          </div>
        </div>

        {/* Action Buttons */}
        <div className="flex flex-wrap gap-4">
          <button
            onClick={() => window.open('http://localhost:8080', '_blank')}
            className="bg-blue-600 hover:bg-blue-700 text-white px-6 py-3 rounded-lg font-semibold transition-colors"
          >
            📊 View Pipeline in Airflow
          </button>

          <button
            onClick={() => window.open('http://localhost:3001', '_blank')}
            className="bg-green-600 hover:bg-green-700 text-white px-6 py-3 rounded-lg font-semibold transition-colors"
          >
            📈 System Monitoring
          </button>

          <button
            onClick={() => window.open('http://localhost:8000/docs', '_blank')}
            className="bg-purple-600 hover:bg-purple-700 text-white px-6 py-3 rounded-lg font-semibold transition-colors"
          >
            🔧 API Documentation
          </button>
        </div>

        {/* Footer Info */}
        <div className="mt-8 pt-6 border-t border-gray-200 dark:border-gray-700">
          <p className="text-sm text-gray-500 dark:text-gray-400 text-center">
            🇻🇳 Vietnam Electronics E-commerce Decision Support System |
            Data updated every 6 hours |
            Pipeline: <span className="font-semibold">vietnam_electronics_direct</span>
          </p>
        </div>
      </div>
    </div>
  );
}