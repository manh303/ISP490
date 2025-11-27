import axios from 'axios';
import Cookies from 'js-cookie';

/** API root (ví dụ: http://localhost:8000) */
const API_BASE_URL = import.meta.env.VITE_API_URL || 'https://isp490.onrender.com';

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
 * Get all activity logs with optional filters
 * @param params - Optional filters: user_id, action, start_date, end_date
 * @returns Promise with activity logs data
 */
export const getActivityLogs = async (params?: {
  user_id?: string;
  action?: string;
  start_date?: string;
  end_date?: string;
}) => {
  const response = await api.get('/v1/admin/activity-logs', { params });
  return response.data;
};

/**
 * Get activity statistics
 * @param params - Optional: days (default 7)
 * @returns Promise with activity stats data
 */
export const getActivityStats = async (params?: { days?: number }) => {
  const response = await api.get('/v1/admin/activity-stats', { params });
  return response.data;
};

