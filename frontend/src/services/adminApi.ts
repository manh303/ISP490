import axios from 'axios';
import Cookies from 'js-cookie';

/** API root (ví dụ: http://localhost:8000) */
const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

/** Axios instance trỏ tới /api */
const api = axios.create({
  baseURL: `${API_BASE_URL}/api`,
  timeout: 10000,
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

/* ------------------------- Activity Logs ------------------------- */

/**
 * Get all activity logs with comprehensive filtering options
 * @param params - Filters: page, limit, sort, user_id, user_email, role, module, action, status, start_date, end_date, keyword
 * @returns Promise with activity logs data including pagination
 */
export const getActivityLogs = async (params?: {
  page?: number;
  limit?: number;
  sort?: string;
  user_id?: string;
  user_email?: string;
  role?: string;
  module?: string;
  action?: string;
  status?: string;
  start_date?: string;
  end_date?: string;
  keyword?: string;
}) => {
  const response = await api.get('/v1/admin/activity-logs', { params });
  return response.data;
};

/**
 * Get detailed information for a single activity log
 * @param logId - The ID of the activity log
 * @returns Promise with detailed log data
 */
export const getActivityLogDetail = async (logId: number) => {
  const response = await api.get(`/v1/admin/activity-logs/${logId}`);
  return response.data;
};

/**
 * Export activity logs to CSV format
 * @param params - Same filters as getActivityLogs except page/limit
 * @returns Promise with CSV blob for download
 */
export const exportActivityLogs = async (params?: {
  user_id?: string;
  user_email?: string;
  role?: string;
  module?: string;
  action?: string;
  status?: string;
  start_date?: string;
  end_date?: string;
  keyword?: string;
}) => {
  const response = await api.get('/v1/admin/activity-logs/export', {
    params,
    responseType: 'blob',
  });
  return response.data;
};



