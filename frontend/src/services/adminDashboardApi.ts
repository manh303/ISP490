/**
 * Admin Dashboard API Service
 * Unified service for fetching all data needed for the Admin Dashboard
 */

import { userApi } from './userApi';
import { getAllRoles } from './roleApi';
import { getAllDatasets, getAllSchemas, type DatasetDetail } from './businessMetadataApi';
import { getDSSHealth, getDataStatus, listDSSDecisions, getDSSScenarios, type DSSDecisionSummary } from './DSSApi';
import { getActivityLogs, exportActivityLogs } from './adminApi';
import { getETLJobs, getETLRunHistory, type ETLJob, type ETLRun } from './dataEngineerApi';

/* ===================== Type Definitions ===================== */

export interface SystemGovernanceData {
    totalUsers: number;
    activeUsersLast30Days: number;
    totalRoles: number;
    totalPermissions: number;
    avgPermissionsPerRole: number;
    totalDatasets: number;
    datasetsWithOwner: number;
    datasetsWithoutOwner: number;
    totalDSSScenarios: number;
    activeMLModels: number;
    lastRetrainDate: string | null;
}

export interface RoleDistribution {
    role_name: string;
    count: number;
}

export interface UserSummary {
    user_id: number;
    email: string;
    full_name: string;
    role_name: string;
    status: string;
    last_login_at: string | null;
}

export interface UserRoleData {
    roleDistribution: RoleDistribution[];
    recentUsers: UserSummary[];
}

export interface DatasetBySchema {
    schema_name: string;
    count: number;
}

export interface AtRiskDataset {
    dataset_id: number;
    table_name: string;
    schema_name: string;
    source_name?: string;
    last_loaded_at: string | null;
    missingFields: string[];
}

export interface DataCatalogData {
    totalDatasets: number;
    datasetsWithOwner: number;
    datasetsWithoutDescription: number;
    datasetsNotUpdated: number;
    datasetsBySchema: DatasetBySchema[];
    atRiskDatasets: AtRiskDataset[];
}

export interface PipelineRunsOverTime {
    date: string;
    success: number;
    failed: number;
}

export interface RecentPipelineRun {
    run_id: number;
    job_name: string;
    job_type: 'ETL' | 'ML';
    status: 'SUCCESS' | 'FAILED' | 'RUNNING';
    started_at: string;
    duration_minutes: number;
    triggered_by: string;
}

export interface PipelinesHealthData {
    etlRunsLast24h: number;
    etlFailuresLast24h: number;
    mlTrainsLast7d: number;
    mlFailuresLast7d: number;
    pipelineRunsOverTime: PipelineRunsOverTime[];
    recentPipelineRuns: RecentPipelineRun[];
}

export interface DSSRunsByScenario {
    scenario_name: string;
    scenario_key: string;
    runs: number;
}

export interface DSSUsageData {
    dssRunsLast7d: number;
    decisionsCreatedLast30d: number;
    decisionsImplemented: number;
    uniqueAnalystUsers: number;
    runsByScenario: DSSRunsByScenario[];
    recentDecisions: DSSDecisionSummary[];
}

export interface ActivityLogSummary {
    log_id: number;
    created_at: string;
    email: string;
    full_name?: string;
    role: string;
    action: string;
    module: string;
    resource?: string;
    status: 'SUCCESS' | 'FAILED';
    latency_ms?: number;
    is_high_risk?: boolean;
}

export interface AuditLogData {
    activityLogs: ActivityLogSummary[];
    totalLogs: number;
    availableUsers: string[];
    availableActions: string[];
}

export type NotificationType = 'USER' | 'DSS' | 'PIPELINE' | 'DATASET';
export type NotificationPriority = 'HIGH' | 'MEDIUM' | 'LOW';

export interface AdminNotification {
    notification_id: number;
    type: NotificationType;
    title: string;
    message: string;
    priority: NotificationPriority;
    created_at: string;
    is_read: boolean;
    action_url?: string;
    requires_approval?: boolean;
    related_id?: number;
}

/* ===================== API Functions ===================== */

/**
 * Fetch System & Governance Overview data
 */
export const fetchSystemGovernanceData = async (): Promise<SystemGovernanceData> => {
    try {
        const [usersRes, rolesRes, datasetsRes, dssHealthRes, dssStatusRes, scenariosRes] = await Promise.all([
            userApi.getActiveUsers().catch(() => ({ users: [] })),
            getAllRoles({ active_only: true }).catch(() => ({ data: [], total: 0 })),
            getAllDatasets().catch(() => []),
            getDSSHealth().catch(() => null),
            getDataStatus().catch(() => null),
            getDSSScenarios().catch(() => ({ scenarios: [] })),
        ]);

        const users = usersRes.data || usersRes || [];
        const roles = rolesRes.data || rolesRes || [];
        const datasets = datasetsRes || [];

        // Calculate active users in last 30 days
        const thirtyDaysAgo = new Date();
        thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
        const activeUsersLast30Days = users.filter((u: any) =>
            u.last_login_at && new Date(u.last_login_at) > thirtyDaysAgo
        ).length;

        // Calculate datasets with/without owner
        const datasetsWithOwner = datasets.filter((d: DatasetDetail) => d.source_name).length;

        return {
            totalUsers: users.length,
            activeUsersLast30Days,
            totalRoles: roles.length,
            totalPermissions: roles.length * 5, // Approximate
            avgPermissionsPerRole: 5, // Approximate
            totalDatasets: datasets.length,
            datasetsWithOwner,
            datasetsWithoutOwner: datasets.length - datasetsWithOwner,
            totalDSSScenarios: scenariosRes.scenarios?.length || 3,
            activeMLModels: dssHealthRes?.components?.ml_tables?.count || 0,
            lastRetrainDate: dssStatusRes?.latest_ml_date || null,
        };
    } catch (error) {
        console.error('Failed to fetch system governance data:', error);
        throw error;
    }
};

/**
 * Fetch User & Role Management data
 */
export const fetchUserRoleData = async (): Promise<UserRoleData> => {
    try {
        const [usersRes, rolesRes] = await Promise.all([
            userApi.getActiveUsers().catch(() => ({ users: [] })),
            getAllRoles({ active_only: true }).catch(() => ({ data: [], total: 0 })),
        ]);

        const users = usersRes.data || usersRes || [];
        const roles = rolesRes.data || rolesRes || [];

        // Calculate role distribution
        const roleCountMap: Record<string, number> = {};
        users.forEach((user: any) => {
            const roleName = user.role_name || user.role || 'Unknown';
            roleCountMap[roleName] = (roleCountMap[roleName] || 0) + 1;
        });

        const roleDistribution = Object.entries(roleCountMap).map(([role_name, count]) => ({
            role_name,
            count,
        }));

        // Get recent users (sorted by last_login_at or created_at)
        const recentUsers = users
            .sort((a: any, b: any) => {
                const dateA = new Date(a.last_login_at || a.created_at || 0);
                const dateB = new Date(b.last_login_at || b.created_at || 0);
                return dateB.getTime() - dateA.getTime();
            })
            .slice(0, 10)
            .map((user: any) => ({
                user_id: user.user_id || user.id,
                email: user.email,
                full_name: user.full_name || user.name || '',
                role_name: user.role_name || user.role || 'Unknown',
                status: user.status || 'active',
                last_login_at: user.last_login_at,
            }));

        return {
            roleDistribution,
            recentUsers,
        };
    } catch (error) {
        console.error('Failed to fetch user role data:', error);
        throw error;
    }
};

/**
 * Fetch Data Catalog Health data
 */
export const fetchDataCatalogData = async (notUpdatedDays: number = 7): Promise<DataCatalogData> => {
    try {
        const [datasetsRes, schemasRes] = await Promise.all([
            getAllDatasets().catch(() => []),
            getAllSchemas().catch(() => []),
        ]);

        const datasets = datasetsRes || [];
        const schemas = schemasRes || [];

        const now = new Date();
        const cutoffDate = new Date();
        cutoffDate.setDate(cutoffDate.getDate() - notUpdatedDays);

        // Calculate stats
        const datasetsWithOwner = datasets.filter((d: DatasetDetail) => d.source_name).length;
        const datasetsWithoutDescription = datasets.filter((d: DatasetDetail) => !d.dataset_type).length;
        const datasetsNotUpdated = datasets.filter((d: DatasetDetail) => {
            if (!d.last_loaded_at) return true;
            return new Date(d.last_loaded_at) < cutoffDate;
        }).length;

        // Group by schema
        const schemaCountMap: Record<string, number> = {};
        datasets.forEach((d: DatasetDetail) => {
            const schema = d.schema_name || 'unknown';
            schemaCountMap[schema] = (schemaCountMap[schema] || 0) + 1;
        });

        const datasetsBySchema = Object.entries(schemaCountMap).map(([schema_name, count]) => ({
            schema_name,
            count,
        }));

        // Find at-risk datasets
        const atRiskDatasets: AtRiskDataset[] = datasets
            .filter((d: DatasetDetail) => {
                const missingFields: string[] = [];
                if (!d.source_name) missingFields.push('Owner');
                if (!d.dataset_type) missingFields.push('Description');
                if (!d.last_loaded_at || new Date(d.last_loaded_at) < cutoffDate) missingFields.push('Stale');
                return missingFields.length > 0;
            })
            .slice(0, 10)
            .map((d: DatasetDetail) => {
                const missingFields: string[] = [];
                if (!d.source_name) missingFields.push('Owner');
                if (!d.dataset_type) missingFields.push('Description');
                if (!d.last_loaded_at || new Date(d.last_loaded_at) < cutoffDate) missingFields.push('Stale');

                return {
                    dataset_id: d.dataset_id,
                    table_name: d.table_name,
                    schema_name: d.schema_name,
                    source_name: d.source_name,
                    last_loaded_at: d.last_loaded_at,
                    missingFields,
                };
            });

        return {
            totalDatasets: datasets.length,
            datasetsWithOwner,
            datasetsWithoutDescription,
            datasetsNotUpdated,
            datasetsBySchema,
            atRiskDatasets,
        };
    } catch (error) {
        console.error('Failed to fetch data catalog data:', error);
        throw error;
    }
};

/**
 * Fetch Pipelines & System Health data
 */
export const fetchPipelinesHealthData = async (): Promise<PipelinesHealthData> => {
    try {
        const etlJobsRes = await getETLJobs().catch(() => []);
        const etlJobs = etlJobsRes || [];

        // Get run history for each job
        const runHistoryPromises = etlJobs.slice(0, 5).map((job: ETLJob) =>
            getETLRunHistory(job.job_code, 20).catch(() => [])
        );
        const runHistories = await Promise.all(runHistoryPromises);
        const allRuns = runHistories.flat();

        const now = new Date();
        const oneDayAgo = new Date(now.getTime() - 24 * 60 * 60 * 1000);
        const sevenDaysAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);

        // Calculate ETL stats (last 24h)
        const etlRunsLast24h = allRuns.filter((run: ETLRun) =>
            new Date(run.started_at) > oneDayAgo
        ).length;
        const etlFailuresLast24h = allRuns.filter((run: ETLRun) =>
            new Date(run.started_at) > oneDayAgo && run.status === 'FAILED'
        ).length;

        // ML stats (approximate - use 7 days)
        const mlTrainsLast7d = etlJobs.filter((job: ETLJob) =>
            job.job_name.toLowerCase().includes('ml') || job.job_name.toLowerCase().includes('model')
        ).length;
        const mlFailuresLast7d = 0; // Would need ML-specific API

        // Generate pipeline runs over time (last 7 days)
        const pipelineRunsOverTime: PipelineRunsOverTime[] = [];
        for (let i = 6; i >= 0; i--) {
            const date = new Date();
            date.setDate(date.getDate() - i);
            const dateStr = date.toISOString().split('T')[0];

            const dayRuns = allRuns.filter((run: ETLRun) =>
                run.started_at.startsWith(dateStr)
            );

            pipelineRunsOverTime.push({
                date: dateStr.slice(5), // MM-DD format
                success: dayRuns.filter((r: ETLRun) => r.status === 'SUCCESS').length,
                failed: dayRuns.filter((r: ETLRun) => r.status === 'FAILED').length,
            });
        }

        // Recent pipeline runs
        const recentPipelineRuns: RecentPipelineRun[] = allRuns
            .sort((a: ETLRun, b: ETLRun) =>
                new Date(b.started_at).getTime() - new Date(a.started_at).getTime()
            )
            .slice(0, 10)
            .map((run: ETLRun) => ({
                run_id: run.run_id,
                job_name: run.job_code,
                job_type: 'ETL' as const,
                status: run.status as 'SUCCESS' | 'FAILED' | 'RUNNING',
                started_at: run.started_at,
                duration_minutes: run.duration_minutes || 0,
                triggered_by: run.airflow_run_id ? 'Airflow' : 'Manual',
            }));

        return {
            etlRunsLast24h,
            etlFailuresLast24h,
            mlTrainsLast7d,
            mlFailuresLast7d,
            pipelineRunsOverTime,
            recentPipelineRuns,
        };
    } catch (error) {
        console.error('Failed to fetch pipelines health data:', error);
        throw error;
    }
};

/**
 * Fetch DSS & Analytics Usage data
 */
export const fetchDSSUsageData = async (): Promise<DSSUsageData> => {
    try {
        const now = new Date();
        const sevenDaysAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
        const thirtyDaysAgo = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);

        const [scenariosRes, decisionsRes] = await Promise.all([
            getDSSScenarios().catch(() => ({ scenarios: [] })),
            listDSSDecisions({
                from_date: thirtyDaysAgo.toISOString().split('T')[0],
                to_date: now.toISOString().split('T')[0],
                page_size: 50,
            }).catch(() => ({ items: [], total: 0 })),
        ]);

        const scenarios = scenariosRes.scenarios || [];
        const decisions = decisionsRes.items || [];

        // Calculate stats
        const decisionsCreatedLast30d = decisions.length;
        const decisionsImplemented = decisions.filter((d: DSSDecisionSummary) =>
            d.status === 'IMPLEMENTED'
        ).length;

        // Unique analysts
        const uniqueAnalysts = new Set(decisions.map((d: DSSDecisionSummary) => d.created_by_email || `user_${d.created_by}`));

        // Runs by scenario (approximate based on decisions)
        const scenarioCountMap: Record<string, number> = {};
        decisions.forEach((d: DSSDecisionSummary) => {
            const scenario = d.scenario_key || 'unknown';
            scenarioCountMap[scenario] = (scenarioCountMap[scenario] || 0) + 1;
        });

        const runsByScenario: DSSRunsByScenario[] = scenarios.map((s: any) => ({
            scenario_name: s.name || s.key,
            scenario_key: s.key,
            runs: scenarioCountMap[s.key] || 0,
        }));

        return {
            dssRunsLast7d: decisions.filter((d: DSSDecisionSummary) =>
                new Date(d.created_at) > sevenDaysAgo
            ).length,
            decisionsCreatedLast30d,
            decisionsImplemented,
            uniqueAnalystUsers: uniqueAnalysts.size,
            runsByScenario,
            recentDecisions: decisions.slice(0, 10),
        };
    } catch (error) {
        console.error('Failed to fetch DSS usage data:', error);
        throw error;
    }
};

/**
 * Fetch Security & Audit Log data
 */
export const fetchAuditLogData = async (params?: {
    limit?: number;
    user_email?: string;
    action?: string;
    status?: string;
    start_date?: string;
    end_date?: string;
}): Promise<AuditLogData> => {
    try {
        const logsRes = await getActivityLogs({
            limit: params?.limit || 50,
            user_email: params?.user_email,
            action: params?.action,
            status: params?.status,
            start_date: params?.start_date,
            end_date: params?.end_date,
            sort: '-created_at',
        }).catch(() => ({ data: [], total: 0 }));

        const logs = logsRes.data || [];
        const total = logsRes.total || logs.length;

        // Extract unique users and actions
        const availableUsers = [...new Set(logs.map((l: any) => l.email).filter(Boolean))];
        const availableActions = [...new Set(logs.map((l: any) => l.action).filter(Boolean))];

        const HIGH_RISK_ACTIONS = [
            'GRANT_ADMIN_ROLE', 'CHANGE_ROLE', 'DELETE_USER', 'EXPORT_DATA',
            'CHANGE_DATASET_ACCESS', 'UPDATE_PERMISSIONS', 'CREATE_ADMIN',
        ];

        const activityLogs: ActivityLogSummary[] = logs.map((log: any) => ({
            log_id: log.log_id || log.id,
            created_at: log.created_at,
            email: log.email || log.user_email,
            full_name: log.full_name,
            role: log.role || 'Unknown',
            action: log.action,
            module: log.module || 'System',
            resource: log.resource,
            status: log.status || 'SUCCESS',
            latency_ms: log.latency_ms,
            is_high_risk: HIGH_RISK_ACTIONS.some(a =>
                (log.action || '').toUpperCase().includes(a)
            ),
        }));

        return {
            activityLogs,
            totalLogs: total,
            availableUsers: availableUsers as string[],
            availableActions: availableActions as string[],
        };
    } catch (error) {
        console.error('Failed to fetch audit log data:', error);
        throw error;
    }
};

/**
 * Export activity logs to CSV
 */
export const exportAuditLogsCSV = async (params?: {
    user_email?: string;
    action?: string;
    status?: string;
    start_date?: string;
    end_date?: string;
}): Promise<Blob> => {
    return exportActivityLogs(params);
};

/**
 * Fetch Notifications data (mock for now - can be replaced with real API)
 */
export const fetchNotificationsData = async (): Promise<AdminNotification[]> => {
    // This would be replaced with a real notifications API
    // For now, generate notifications based on other data
    try {
        const [pipelinesData, catalogData, dssData] = await Promise.all([
            fetchPipelinesHealthData().catch(() => null),
            fetchDataCatalogData().catch(() => null),
            fetchDSSUsageData().catch(() => null),
        ]);

        const notifications: AdminNotification[] = [];
        let notificationId = 1;

        // Pipeline failure notifications
        if (pipelinesData && pipelinesData.etlFailuresLast24h > 0) {
            notifications.push({
                notification_id: notificationId++,
                type: 'PIPELINE',
                title: 'Pipeline Failures Detected',
                message: `${pipelinesData.etlFailuresLast24h} pipeline(s) failed in the last 24 hours`,
                priority: 'HIGH',
                created_at: new Date().toISOString(),
                is_read: false,
                action_url: '/data-engineer/pipelines',
            });
        }

        // Dataset health notifications
        if (catalogData && catalogData.datasetsWithoutDescription > 5) {
            notifications.push({
                notification_id: notificationId++,
                type: 'DATASET',
                title: 'Datasets Missing Metadata',
                message: `${catalogData.datasetsWithoutDescription} datasets are missing descriptions`,
                priority: 'MEDIUM',
                created_at: new Date().toISOString(),
                is_read: false,
                action_url: '/data-catalog',
            });
        }

        // DSS approval notifications
        if (dssData) {
            const pendingApproval = dssData.recentDecisions.filter(
                (d) => d.status === 'DRAFT' || d.status === 'NEED_APPROVAL'
            );
            pendingApproval.forEach((decision) => {
                notifications.push({
                    notification_id: notificationId++,
                    type: 'DSS',
                    title: `DSS Decision Pending: ${decision.title}`,
                    message: `Decision by ${decision.created_by_email || 'User'} needs review`,
                    priority: 'MEDIUM',
                    created_at: decision.created_at,
                    is_read: false,
                    requires_approval: true,
                    related_id: decision.decision_id,
                    action_url: `/dss/decisions/${decision.decision_id}`,
                });
            });
        }

        return notifications.sort((a, b) =>
            new Date(b.created_at).getTime() - new Date(a.created_at).getTime()
        );
    } catch (error) {
        console.error('Failed to fetch notifications:', error);
        return [];
    }
};
