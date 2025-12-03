import React, { useState, useEffect } from 'react';
import PageBreadcrumb from "../../../components/common/PageBreadCrumb";
import PageMeta from "../../../components/common/PageMeta";
import { getPlatforms, getCategories, getOverviewReport, type Platform as ApiPlatform, type Category as ApiCategory, type OverviewReport as ApiOverviewReport } from '../../../services/analyticsApi';
import { getDSSHealth, getDataStatus, listDSSDecisions, getDSSDecisionDetail, type DSSHealthResponse, type DataStatusResponse, type DSSDecisionSummary, type DSSDecisionDetailResponse } from '../../../services/DSSApi';
import { getActivityLogs } from '../../../services/adminApi';
import AdminDashboardFilters from './components/AdminDashboardFilters';
import AdminDashboardOverviewCards from './components/AdminDashboardOverviewCards';
import AdminDashboardCharts from './components/AdminDashboardCharts';
import AdminDashboardDSSDecisions from './components/AdminDashboardDSSDecisions';
import AdminDashboardUserActivity from './components/AdminDashboardUserActivity';
import DSSDecisionModal from './components/DSSDecisionModal';

// Types for our data
interface Platform extends ApiPlatform {}
interface Category extends ApiCategory {}
interface OverviewReport extends ApiOverviewReport {}

export default function AdminDashboard() {
  // Filter states
  const [fromDate, setFromDate] = useState(() => {
    const date = new Date();
    date.setDate(date.getDate() - 7);
    return date.toISOString().split('T')[0];
  });
  const [toDate, setToDate] = useState(() => new Date().toISOString().split('T')[0]);
  const [selectedPlatform, setSelectedPlatform] = useState<string>('');
  const [selectedCategory, setSelectedCategory] = useState<string>('');
  
  // Data states
  const [platforms, setPlatforms] = useState<Platform[]>([]);
  const [categories, setCategories] = useState<Category[]>([]);
  const [overviewReport, setOverviewReport] = useState<OverviewReport | null>(null);
  const [dssHealth, setDssHealth] = useState<DSSHealthResponse | null>(null);
  const [dataStatus, setDataStatus] = useState<DataStatusResponse | null>(null);
  const [dssDecisions, setDssDecisions] = useState<DSSDecisionSummary[]>([]);
  const [decisionDetail, setDecisionDetail] = useState<DSSDecisionDetailResponse | null>(null);
  const [activityLogs, setActivityLogs] = useState<any[]>([]);
  
  // UI states
  const [loading, setLoading] = useState(true);
  const [showDecisionModal, setShowDecisionModal] = useState(false);
  const [selectedMetric, setSelectedMetric] = useState<'revenue' | 'reviews' | 'price' | 'rating'>('revenue');
  const [decisionPage, setDecisionPage] = useState(1);
  const [decisionScenario, setDecisionScenario] = useState('');
  const [decisionStatus, setDecisionStatus] = useState('');

  // Load initial data
  useEffect(() => {
    loadFilters();
  }, []);

  // Load dashboard data when filters change
  useEffect(() => {
    if (platforms.length > 0) {
      loadDashboardData();
    }
  }, [fromDate, toDate, selectedPlatform, selectedCategory]);

  // Load DSS decisions
  useEffect(() => {
    loadDSSDecisions();
  }, [decisionPage, decisionScenario, decisionStatus, fromDate, toDate]);

  const loadFilters = async () => {
    try {
      const [platformsRes, categoriesRes] = await Promise.all([
        getPlatforms(),
        getCategories()
      ]);
      setPlatforms(platformsRes);
      setCategories(categoriesRes);
    } catch (error) {
      console.error('Failed to load filters:', error);
    }
  };

  const loadDashboardData = async () => {
    setLoading(true);
    try {
      const params = {
        from_date: fromDate,
        to_date: toDate,
        platform_code: selectedPlatform || undefined,
        category_key: selectedCategory || undefined,
      };

      const [
        overviewRes,
        healthRes,
        dataStatusRes,
        activityRes
      ] = await Promise.all([
        getOverviewReport(params),
        getDSSHealth(),
        getDataStatus(),
        getActivityLogs({ limit: 10, sort: '-created_at' })
      ]);

      setOverviewReport(overviewRes);
      setDssHealth(healthRes);
      setDataStatus(dataStatusRes);
      setActivityLogs(activityRes.data || []);
    } catch (error) {
      console.error('Failed to load dashboard data:', error);
    } finally {
      setLoading(false);
    }
  };

  const loadDSSDecisions = async () => {
    try {
      const params: any = {
        from_date: fromDate,
        to_date: toDate,
        page: decisionPage,
        page_size: 10,
      };
      if (decisionScenario) params.scenario_key = decisionScenario;
      if (decisionStatus) params.status = decisionStatus;

      const response = await listDSSDecisions(params);
      setDssDecisions(response.items);
    } catch (error) {
      console.error('Failed to load DSS decisions:', error);
    }
  };

  const loadDecisionDetail = async (decisionId: number) => {
    try {
      const response = await getDSSDecisionDetail(decisionId);
      setDecisionDetail(response);
      setShowDecisionModal(true);
    } catch (error) {
      console.error('Failed to load decision detail:', error);
    }
  };

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
          title="Admin Dashboard"
          description="Vietnam Electronics E-commerce Admin Dashboard"
        />
        <PageBreadcrumb pageTitle="Admin Dashboard" />
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
        title="Admin Dashboard"
        description="Vietnam Electronics E-commerce Admin Dashboard"
      />
      <PageBreadcrumb pageTitle="Admin Dashboard" />

      <div className="rounded-2xl border border-gray-200 bg-white px-5 py-7 dark:border-gray-800 dark:bg-white/[0.03] xl:px-10 xl:py-12 space-y-8">

        {/* Hàng 0: Filters Bar */}
        <AdminDashboardFilters
          fromDate={fromDate}
          toDate={toDate}
          selectedPlatform={selectedPlatform}
          selectedCategory={selectedCategory}
          platforms={platforms}
          categories={categories}
          onFromDateChange={setFromDate}
          onToDateChange={setToDate}
          onPlatformChange={setSelectedPlatform}
          onCategoryChange={setSelectedCategory}
          onApplyFilters={() => loadDashboardData()}
        />

        {/* Hàng 1: Cards Overview */}
        <AdminDashboardOverviewCards
          overviewReport={overviewReport}
          dssHealth={dssHealth}
          dataStatus={dataStatus}
          formatCurrency={formatCurrency}
          formatNumber={formatNumber}
        />

        {/* Hàng 2: Charts */}
        <AdminDashboardCharts
          overviewReport={overviewReport}
          selectedMetric={selectedMetric}
          selectedPlatform={selectedPlatform}
          formatCurrency={formatCurrency}
          formatNumber={formatNumber}
          onMetricChange={setSelectedMetric}
        />

        {/* Hàng 3: DSS Decisions */}
        <AdminDashboardDSSDecisions
          dssDecisions={dssDecisions}
          decisionScenario={decisionScenario}
          decisionStatus={decisionStatus}
          onScenarioChange={setDecisionScenario}
          onStatusChange={setDecisionStatus}
          onViewDecision={loadDecisionDetail}
        />

        {/* Hàng 4: User & Activity Log (Optional) */}
        <AdminDashboardUserActivity
          activityLogs={activityLogs}
        />

        {/* Decision Detail Modal */}
        <DSSDecisionModal
          showModal={showDecisionModal}
          decisionDetail={decisionDetail}
          onClose={() => setShowDecisionModal(false)}
        />
      </div>
    </div>
  );
}