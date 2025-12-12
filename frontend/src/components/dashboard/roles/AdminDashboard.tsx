import React, { useState, useEffect } from 'react';
import PageBreadcrumb from "../../../components/common/PageBreadCrumb";
import PageMeta from "../../../components/common/PageMeta";
import { getPlatforms, getCategories, type Platform as ApiPlatform, type Category as ApiCategory } from '../../../services/analyticsApi';
import { getDSSDecisionDetail, type DSSDecisionDetailResponse } from '../../../services/DSSApi';
import { exportAuditLogsCSV } from '../../../services/adminDashboardApi';
import {
  fetchSystemGovernanceData,
  fetchUserRoleData,
  fetchDataCatalogData,
  fetchPipelinesHealthData,
  fetchDSSUsageData,
  fetchAuditLogData,
  fetchNotificationsData,
  type SystemGovernanceData,
  type UserRoleData,
  type DataCatalogData,
  type PipelinesHealthData,
  type DSSUsageData,
  type AuditLogData,
  type AdminNotification,
} from '../../../services/adminDashboardApi';

// Import new components
import SystemGovernanceOverview from '../../../pages/Admin/components/SystemGovernanceOverview';
import UserRoleSnapshot from '../../../pages/Admin/components/UserRoleSnapshot';
import DataCatalogHealth from '../../../pages/Admin/components/DataCatalogHealth';
import PipelinesSystemHealth from '../../../pages/Admin/components/PipelinesSystemHealth';
import DSSAnalyticsUsage from '../../../pages/Admin/components/DSSAnalyticsUsage';
import SecurityAuditLog from '../../../pages/Admin/components/SecurityAuditLog';
import NotificationsPendingTasks from '../../../pages/Admin/components/NotificationsPendingTasks';
import DSSDecisionModal from './components/DSSDecisionModal';

// Types
interface Platform extends ApiPlatform { }
interface Category extends ApiCategory { }

export default function AdminDashboard() {
  // ===================== Filter States =====================
  const [timeRange, setTimeRange] = useState('7d');
  const [selectedPlatform, setSelectedPlatform] = useState<string>('all');
  const [platforms, setPlatforms] = useState<Platform[]>([]);
  const [categories, setCategories] = useState<Category[]>([]);

  // ===================== Section Data States =====================
  const [governanceData, setGovernanceData] = useState<SystemGovernanceData | null>(null);
  const [userRoleData, setUserRoleData] = useState<UserRoleData | null>(null);
  const [catalogData, setCatalogData] = useState<DataCatalogData | null>(null);
  const [pipelinesData, setPipelinesData] = useState<PipelinesHealthData | null>(null);
  const [dssUsageData, setDssUsageData] = useState<DSSUsageData | null>(null);
  const [auditLogData, setAuditLogData] = useState<AuditLogData | null>(null);
  const [notifications, setNotifications] = useState<AdminNotification[]>([]);

  // ===================== UI States =====================
  const [loading, setLoading] = useState(true);
  const [sectionLoading, setSectionLoading] = useState<Record<string, boolean>>({});
  const [showDecisionModal, setShowDecisionModal] = useState(false);
  const [decisionDetail, setDecisionDetail] = useState<DSSDecisionDetailResponse | null>(null);

  // Audit log filters
  const [auditTimeRange, setAuditTimeRange] = useState('24h');
  const [auditUser, setAuditUser] = useState('all');
  const [auditAction, setAuditAction] = useState('all');
  const [auditStatus, setAuditStatus] = useState('all');

  // DSS filters
  const [dssStatusFilter, setDssStatusFilter] = useState('all');

  // User role filter
  const [userRoleFilter, setUserRoleFilter] = useState('all');

  // ===================== Load Initial Data =====================
  useEffect(() => {
    loadAllData();
  }, []);

  // ===================== Reload Audit Logs on Filter Change =====================
  useEffect(() => {
    loadAuditLogs();
  }, [auditTimeRange, auditUser, auditAction, auditStatus]);

  const loadAllData = async () => {
    setLoading(true);
    try {
      // Load all sections in parallel
      const [
        governanceRes,
        userRoleRes,
        catalogRes,
        pipelinesRes,
        dssUsageRes,
        auditRes,
        notificationsRes,
        platformsRes,
        categoriesRes,
      ] = await Promise.all([
        fetchSystemGovernanceData().catch(err => { console.error('Governance error:', err); return null; }),
        fetchUserRoleData().catch(err => { console.error('UserRole error:', err); return null; }),
        fetchDataCatalogData().catch(err => { console.error('Catalog error:', err); return null; }),
        fetchPipelinesHealthData().catch(err => { console.error('Pipelines error:', err); return null; }),
        fetchDSSUsageData().catch(err => { console.error('DSS usage error:', err); return null; }),
        fetchAuditLogData({ limit: 30 }).catch(err => { console.error('Audit error:', err); return null; }),
        fetchNotificationsData().catch(err => { console.error('Notifications error:', err); return []; }),
        getPlatforms().catch(() => []),
        getCategories().catch(() => []),
      ]);

      setGovernanceData(governanceRes);
      setUserRoleData(userRoleRes);
      setCatalogData(catalogRes);
      setPipelinesData(pipelinesRes);
      setDssUsageData(dssUsageRes);
      setAuditLogData(auditRes);
      setNotifications(notificationsRes);
      setPlatforms(platformsRes);
      setCategories(categoriesRes);
    } catch (error) {
      console.error('Failed to load dashboard data:', error);
    } finally {
      setLoading(false);
    }
  };

  const loadAuditLogs = async () => {
    setSectionLoading(prev => ({ ...prev, audit: true }));
    try {
      const params: any = { limit: 30 };

      // Calculate date range based on timeRange filter
      const now = new Date();
      if (auditTimeRange === '1h') {
        params.start_date = new Date(now.getTime() - 60 * 60 * 1000).toISOString();
      } else if (auditTimeRange === '24h') {
        params.start_date = new Date(now.getTime() - 24 * 60 * 60 * 1000).toISOString().split('T')[0];
      } else if (auditTimeRange === '7d') {
        const d = new Date();
        d.setDate(d.getDate() - 7);
        params.start_date = d.toISOString().split('T')[0];
      } else if (auditTimeRange === '30d') {
        const d = new Date();
        d.setDate(d.getDate() - 30);
        params.start_date = d.toISOString().split('T')[0];
      }

      if (auditUser !== 'all') params.user_email = auditUser;
      if (auditAction !== 'all') params.action = auditAction;
      if (auditStatus !== 'all') params.status = auditStatus;

      const result = await fetchAuditLogData(params);
      setAuditLogData(result);
    } catch (error) {
      console.error('Failed to reload audit logs:', error);
    } finally {
      setSectionLoading(prev => ({ ...prev, audit: false }));
    }
  };

  // ===================== Action Handlers =====================
  const handleViewDecision = async (decisionId: number) => {
    try {
      const detail = await getDSSDecisionDetail(decisionId);
      setDecisionDetail(detail);
      setShowDecisionModal(true);
    } catch (error) {
      console.error('Failed to load decision detail:', error);
    }
  };

  const handleExportAuditCSV = async () => {
    try {
      const params: any = {};
      if (auditUser !== 'all') params.user_email = auditUser;
      if (auditAction !== 'all') params.action = auditAction;
      if (auditStatus !== 'all') params.status = auditStatus;

      const blob = await exportAuditLogsCSV(params);
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `audit_logs_${new Date().toISOString().split('T')[0]}.csv`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      window.URL.revokeObjectURL(url);
    } catch (error) {
      console.error('Failed to export audit logs:', error);
    }
  };

  const handleViewLogDetail = (logId: number) => {
    // Could open a modal with detailed log info
    console.log('View log detail:', logId);
  };

  const handleViewNotificationDetail = (notification: AdminNotification) => {
    if (notification.action_url) {
      window.location.href = notification.action_url;
    }
  };

  const handleApproveNotification = (notificationId: number, relatedId: number) => {
    // Implement approval logic
    console.log('Approve:', notificationId, relatedId);
    // Remove from list after approval
    setNotifications(prev => prev.filter(n => n.notification_id !== notificationId));
  };

  const handleRejectNotification = (notificationId: number, relatedId: number) => {
    // Implement rejection logic
    console.log('Reject:', notificationId, relatedId);
    setNotifications(prev => prev.filter(n => n.notification_id !== notificationId));
  };

  const handleMarkNotificationRead = (notificationId: number) => {
    setNotifications(prev =>
      prev.map(n => n.notification_id === notificationId ? { ...n, is_read: true } : n)
    );
  };

  // ===================== Render =====================
  if (loading) {
    return (
      <div>
        <PageMeta
          title="Administration Dashboard"
          description="Administration Dashboard Hệ thống DSS"
        />
        <PageBreadcrumb pageTitle="Administration Dashboard" />
        <div className="min-h-screen rounded-2xl border border-gray-200 bg-white px-5 py-7 dark:border-gray-800 dark:bg-white/[0.03] xl:px-10 xl:py-12">
          <div className="flex items-center justify-center h-64">
            <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
            <span className="ml-3 text-gray-600 dark:text-gray-400">Đang tải dữ liệu bảng điều khiển...</span>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div>
      <PageMeta
        title="Administration Dashboard"
        description="Administration Dashboard Hệ thống DSS"
      />
      <PageBreadcrumb pageTitle="Administration Dashboard" />

      <div className="rounded-2xl border border-gray-200 bg-white px-5 py-7 dark:border-gray-800 dark:bg-white/[0.03] xl:px-10 xl:py-12 space-y-10">

        {/* ==================== Section 1: System & Governance Overview ==================== */}
        <SystemGovernanceOverview
          totalUsers={governanceData?.totalUsers || 0}
          activeUsersLast30Days={governanceData?.activeUsersLast30Days || 0}
          totalRoles={governanceData?.totalRoles || 0}
          totalPermissions={governanceData?.totalPermissions || 0}
          avgPermissionsPerRole={governanceData?.avgPermissionsPerRole || 0}
          totalDatasets={governanceData?.totalDatasets || 0}
          datasetsWithOwner={governanceData?.datasetsWithOwner || 0}
          datasetsWithoutOwner={governanceData?.datasetsWithoutOwner || 0}
          totalDSSScenarios={governanceData?.totalDSSScenarios || 0}
          activeMLModels={governanceData?.activeMLModels || 0}
          lastRetrainDate={governanceData?.lastRetrainDate || null}
          isLoading={loading}
        />

        <hr className="border-gray-200 dark:border-gray-700" />

        {/* ==================== Section 2: User & Role Management ==================== */}
        <UserRoleSnapshot
          roleDistribution={userRoleData?.roleDistribution || []}
          recentUsers={userRoleData?.recentUsers || []}
          selectedRoleFilter={userRoleFilter}
          onRoleFilterChange={setUserRoleFilter}
          isLoading={loading}
        />

        <hr className="border-gray-200 dark:border-gray-700" />

        {/* ==================== Section 3: Data Catalog & Dataset Health ==================== */}
        <DataCatalogHealth
          totalDatasets={catalogData?.totalDatasets || 0}
          datasetsWithOwner={catalogData?.datasetsWithOwner || 0}
          datasetsWithoutDescription={catalogData?.datasetsWithoutDescription || 0}
          datasetsNotUpdated={catalogData?.datasetsNotUpdated || 0}
          notUpdatedDays={7}
          datasetsBySchema={catalogData?.datasetsBySchema || []}
          atRiskDatasets={catalogData?.atRiskDatasets || []}
          isLoading={loading}
        />

        <hr className="border-gray-200 dark:border-gray-700" />

        {/* ==================== Section 4: Pipelines & System Health ==================== */}
        <PipelinesSystemHealth
          etlRunsLast24h={pipelinesData?.etlRunsLast24h || 0}
          etlFailuresLast24h={pipelinesData?.etlFailuresLast24h || 0}
          mlTrainsLast7d={pipelinesData?.mlTrainsLast7d || 0}
          mlFailuresLast7d={pipelinesData?.mlFailuresLast7d || 0}
          pipelineRunsOverTime={pipelinesData?.pipelineRunsOverTime || []}
          recentPipelineRuns={pipelinesData?.recentPipelineRuns || []}
          isLoading={loading}
        />

        <hr className="border-gray-200 dark:border-gray-700" />

        {/* ==================== Section 5: DSS & Analytics Usage ==================== */}
        <DSSAnalyticsUsage
          dssRunsLast7d={dssUsageData?.dssRunsLast7d || 0}
          decisionsCreatedLast30d={dssUsageData?.decisionsCreatedLast30d || 0}
          decisionsImplemented={dssUsageData?.decisionsImplemented || 0}
          uniqueAnalystUsers={dssUsageData?.uniqueAnalystUsers || 0}
          runsByScenario={dssUsageData?.runsByScenario || []}
          recentDecisions={dssUsageData?.recentDecisions || []}
          selectedStatusFilter={dssStatusFilter}
          onStatusFilterChange={setDssStatusFilter}
          onViewDecision={handleViewDecision}
          isLoading={loading}
        />

        <hr className="border-gray-200 dark:border-gray-700" />

        {/* ==================== Section 6: Security & Audit Log ==================== */}
        <SecurityAuditLog
          activityLogs={auditLogData?.activityLogs || []}
          totalLogs={auditLogData?.totalLogs || 0}
          selectedTimeRange={auditTimeRange}
          selectedUser={auditUser}
          selectedAction={auditAction}
          selectedStatus={auditStatus}
          onTimeRangeChange={setAuditTimeRange}
          onUserChange={setAuditUser}
          onActionChange={setAuditAction}
          onStatusChange={setAuditStatus}
          onViewDetail={handleViewLogDetail}
          onExportCSV={handleExportAuditCSV}
          availableUsers={auditLogData?.availableUsers || []}
          availableActions={auditLogData?.availableActions || []}
          isLoading={sectionLoading.audit || loading}
        />

        <hr className="border-gray-200 dark:border-gray-700" />

        {/* ==================== Section 7: Notifications & Pending Tasks ==================== */}
        <NotificationsPendingTasks
          notifications={notifications}
          onViewDetail={handleViewNotificationDetail}
          onApprove={handleApproveNotification}
          onReject={handleRejectNotification}
          onMarkAsRead={handleMarkNotificationRead}
          isLoading={false}
        />

        {/* ==================== DSS Decision Modal ==================== */}
        <DSSDecisionModal
          showModal={showDecisionModal}
          decisionDetail={decisionDetail}
          onClose={() => setShowDecisionModal(false)}
        />
      </div>
    </div>
  );
}