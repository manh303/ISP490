import axios from 'axios';
import Cookies from 'js-cookie';

/** API root */
const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

/** Axios instance trỏ tới /api */
const api = axios.create({
  baseURL: `${API_BASE_URL}/api`,
  timeout: 30000,
  headers: { 'Content-Type': 'application/json' },
});

/* ------------------------- Interceptors ------------------------- */

/** Gắn Bearer token cho mọi request */
api.interceptors.request.use(
  (config) => {
    const token = Cookies.get('access_token');
    if (token) {
      config.headers = config.headers ?? {};
      (config.headers as any).Authorization = `Bearer ${token}`;
    }
    return config;
  },
  (error) => Promise.reject(error)
);

/* ------------------------- Type Definitions ------------------------- */

// Health Check
export interface HealthResponse {
  status: string;
  timestamp: string;
}

// ETL Jobs
export interface ETLJob {
  job_code: string;
  job_name: string;
  is_active: boolean;
  last_run_date: string;
  last_run_status: string;
  last_run_duration_minutes: number | null;
  total_runs: number;
  success_rate: number;
  avg_duration_minutes: number;
}

// ETL Run History
export interface ETLRun {
  run_id: number;
  job_code: string;
  run_date: string;
  started_at: string;
  finished_at: string;
  status: string;
  rows_read: number;
  rows_written: number;
  duration_minutes: number;
  error_message: string;
  airflow_run_id: string;
}

// Table Health
export interface TableHealth {
  schema_name: string;
  table_name: string;
  row_count: number;
  size_mb: number;
  last_loaded_at: string;
  freshness_hours: number;
  health_status: string;
}

// Data Quality Issues
export interface DataQualityIssue {
  issue_id: number;
  schema_name: string;
  table_name: string;
  issue_type: string;
  severity: string;
  status: string;
  affected_rows: number;
  issue_description: string;
  detected_at: string;
}

// Data Quality Summary
export interface DataQualitySummaryItem {
  status: string;
  severity: string;
  issue_count: number;
  total_affected_rows: number;
}

// Database Health
export interface DatabaseHealth {
  status: string;
  active_connections: number;
  idle_connections: number;
  max_connections: number;
  connection_usage_pct: number;
  avg_query_time_ms: number;
  slow_queries_count: number;
  check_time: string;
}

// Table Lineage
export interface TableLineage {
  source_schema: string;
  source_table: string;
  target_schema: string;
  target_table: string;
  transformation_type: string;
  job_code: string;
}

// Alert Summary
export interface AlertSummary {
  alert_name: string;
  alert_type: string;
  severity: string;
  target_name: string;
  triggered_count_24h: number;
  last_triggered_at: string;
  status: string;
}

/* ------------------------- API Functions ------------------------- */

/**
 * Health Check
 */
export const getHealth = async (): Promise<HealthResponse> => {
  const response = await api.get('/v1/data-engineer/health');
  return response.data;
};

/**
 * Get ETL Jobs List
 */
export const getETLJobs = async (): Promise<ETLJob[]> => {
  const response = await api.get('/v1/data-engineer/etl/jobs');
  return response.data;
};

/**
 * Get ETL Run History for a specific job
 */
export const getETLRunHistory = async (
  jobCode: string,
  limit: number = 20,
  status?: string
): Promise<ETLRun[]> => {
  const params = new URLSearchParams();
  if (limit) params.append('limit', limit.toString());
  if (status) params.append('status', status);

  const response = await api.get(`/v1/data-engineer/etl/runs/${jobCode}?${params}`);
  return response.data;
};

/**
 * Get ETL Run Logs
 */
export const getETLRunLogs = async (runId: number): Promise<string> => {
  const response = await api.get(`/v1/data-engineer/etl/logs/${runId}`);
  return response.data;
};

/**
 * Get Table Health Status
 */
export const getTableHealth = async (
  schemaName?: string,
  staleHours: number = 24
): Promise<TableHealth[]> => {
  const params = new URLSearchParams();
  if (schemaName) params.append('schema_name', schemaName);
  params.append('stale_hours', staleHours.toString());

  const response = await api.get(`/v1/data-engineer/tables/health?${params}`);
  return response.data;
};

/**
 * Get Table Growth History
 */
export const getTableGrowth = async (
  schemaName: string,
  tableName: string,
  days: number = 30
): Promise<any> => {
  const response = await api.get(`/v1/data-engineer/tables/growth/${schemaName}/${tableName}?days=${days}`);
  return response.data;
};

/**
 * Get Data Quality Issues
 */
export const getDataQualityIssues = async (
  status: string = 'OPEN',
  severity?: string,
  schemaName?: string
): Promise<DataQualityIssue[]> => {
  const params = new URLSearchParams();
  params.append('status', status);
  if (severity) params.append('severity', severity);
  if (schemaName) params.append('schema_name', schemaName);

  const response = await api.get(`/v1/data-engineer/data-quality/issues?${params}`);
  return response.data;
};

/**
 * Get Data Quality Summary
 */
export const getDataQualitySummary = async (): Promise<DataQualitySummaryItem[]> => {
  const response = await api.get('/v1/data-engineer/data-quality/summary');
  return response.data;
};

/**
 * Get Database Health
 */
export const getDatabaseHealth = async (): Promise<DatabaseHealth> => {
  const response = await api.get('/v1/data-engineer/database/health');
  return response.data;
};

/**
 * Get Table Lineage
 */
export const getTableLineage = async (
  schemaName: string,
  tableName: string,
  direction: 'upstream' | 'downstream' | 'both' = 'both'
): Promise<TableLineage[]> => {
  const response = await api.get(`/v1/data-engineer/lineage/table/${schemaName}/${tableName}?direction=${direction}`);
  return response.data;
};

/**
 * Get Alert Summary
 */
export const getAlertSummary = async (): Promise<AlertSummary[]> => {
  const response = await api.get('/v1/data-engineer/alerts/summary');
  return response.data;
};

/**
 * Get Alert History
 */
export const getAlertHistory = async (
  hours: number = 24,
  status?: string
): Promise<any> => {
  const params = new URLSearchParams();
  params.append('hours', hours.toString());
  if (status) params.append('status', status);

  const response = await api.get(`/v1/data-engineer/alerts/history?${params}`);
  return response.data;
};

/**
 * Get Pipeline Performance Stats
 */
export const getPipelinePerformanceStats = async (days: number = 7): Promise<any> => {
  const response = await api.get(`/v1/data-engineer/stats/pipeline-performance?days=${days}`);
  return response.data;
};

/**
 * Get Data Volume Trends
 */
export const getDataVolumeTrends = async (days: number = 30): Promise<any> => {
  const response = await api.get(`/v1/data-engineer/stats/data-volume?days=${days}`);
  return response.data;
};
