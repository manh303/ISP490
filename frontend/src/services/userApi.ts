import axios from 'axios';
import Cookies from 'js-cookie';

/** API root (ví dụ: http://localhost:8000) */
const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

/** Axios instance trỏ tới /api */
const api = axios.create({
  baseURL: import.meta.env.DEV ? '/api' : `${API_BASE_URL}/api`,
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
  // GET /api/v1/admin/users (no pagination)
  getActiveUsers: async () => {
    const res = await api.get(`/v1/admin/users`);
    return res.data;
  },

  // POST /api/v1/admin/users
  createUser: async (data: any) => {
    const res = await api.post(`/v1/admin/users`, data);
    return res.data;
  },

  // GET /api/v1/admin/users/deleted (no pagination)
  getDeletedUsers: async () => {
    const res = await api.get(`/v1/admin/users/deleted`);
    return res.data;
  },

  // GET /api/v1/admin/users/{user_id}
  getUser: async (userId: number) => {
    const res = await api.get(`/v1/admin/users/${userId}`);
    return res.data;
  },

  // PUT /api/v1/admin/users/{user_id}
  updateUser: async (userId: number, data: any) => {
    const res = await api.put(`/v1/admin/users/${userId}`, data);
    return res.data;
  },

  // PUT /api/v1/admin/users/{user_id}/password
  updateUserPassword: async (userId: number, newPassword: string) => {
    const res = await api.put(`/v1/admin/users/${userId}/password`, { new_password: newPassword });
    return res.data;
  },

  // PUT /api/v1/admin/users/{user_id}/disable
  disableUser: async (userId: number) => {
    const res = await api.put(`/v1/admin/users/${userId}/disable`);
    return res.data;
  },

  // PUT /api/v1/admin/users/{user_id}/restore
  restoreUser: async (userId: number) => {
    const res = await api.put(`/v1/admin/users/${userId}/restore`);
    return res.data;
  },

  // DELETE /api/v1/admin/users/{user_id}?confirm=true
  deleteUser: async (userId: number) => {
    const res = await api.delete(`/v1/admin/users/${userId}`, { params: { confirm: true } });
    return res.data;
  },

  // DELETE /api/v1/admin/users/{user_id}/permanent?confirm=true
  permanentDeleteUser: async (userId: number) => {
    const res = await api.delete(`/v1/admin/users/${userId}/permanent`, { params: { confirm: true } });
    return res.data;
  }
};
