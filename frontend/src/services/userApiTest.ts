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


export const userApi = {
  // GET /api/v1/test-admin/session-info
  getSessionInfo: async () => {
    const res = await api.get(`/v1/test-admin/session-info`);
    return res.data;
  },

  // GET /api/v1/test-admin/users
  getActiveUsers: async (page = 1, limit = 20) => {
    const res = await api.get(`/v1/test-admin/users`, { params: { page, limit } });
    return res.data;
  },

  // POST /api/v1/test-admin/users
  createUser: async (data: any) => {
    const res = await api.post(`/v1/test-admin/users`, data);
    return res.data;
  },

  // PUT /api/v1/test-admin/users/{user_id}/disable
  disableUser: async (userId: number) => {
    const res = await api.put(`/v1/test-admin/users/${userId}/disable`);
    return res.data;
  },

  // GET /api/v1/test-admin/users/deleted
  getDeletedUsers: async (page = 1, limit = 20) => {
    const res = await api.get(`/v1/test-admin/users/deleted`, { params: { page, limit } });
    return res.data;
  },

  // PUT /api/v1/test-admin/users/{user_id}/restore
  restoreUser: async (userId: number) => {
    const res = await api.put(`/v1/test-admin/users/${userId}/restore`);
    return res.data;
  },

  // GET /api/v1/test-admin/profile/{user_id}
  getUser: async (userId: number) => {
    const res = await api.get(`/v1/test-admin/profile/${userId}`);
    return res.data.data;
  },

  // PUT /api/v1/test-admin/profile/{user_id}
  updateUser: async (userId: number, data: any) => {
    const res = await api.put(`/v1/test-admin/profile/${userId}`, data);
    return res.data.data;
  },

  // DELETE /api/v1/test-admin/users/{user_id}/permanent?confirm=true
  permanentDeleteUser: async (userId: number) => {
    const res = await api.delete(`/v1/test-admin/users/${userId}/permanent`, { params: { confirm: true } });
    return res.data;
  },
    updateUserPassword: async (userId: number, newPassword: string) => {
    const res = await api.put(`/v1/admin/users/${userId}/password`, { new_password: newPassword });
    return res.data;
  },
};
