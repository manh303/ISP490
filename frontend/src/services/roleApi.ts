import axios from 'axios';
import Cookies from 'js-cookie';

/** API root (ví dụ: http://localhost:8000) */
const API_BASE_URL = import.meta.env.VITE_API_URL || 'https://isp490.onrender.com';

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

/* ------------------------- Type Definitions ------------------------- */

export interface Role {
  role_id: number;
  role_code: string;
  role_name: string;
  description: string;
  is_active: boolean;
}

export interface RoleDetails extends Role {
  permissions?: string[];
  modules?: string[];
  actions?: string[];
  admin_features?: Record<string, boolean>;
  user_count?: number;
}

export interface GetAllRolesParams {
  page?: number;
  limit?: number;
  active_only?: boolean;
}

export interface GetAllRolesResponse {
  success: boolean;
  data: Role[];
  total: number;
  page: number;
  limit: number;
}

export interface CreateRoleData {
  role_code: string;
  role_name: string;
  description: string;
}

export interface UpdateRoleData {
  role_name?: string;
  description?: string;
}

export interface RoleResponse {
  success: boolean;
  message: string;
  role_id: number;
}

export interface GetRoleUsersParams {
  page?: number;
  limit?: number;
}

/* ------------------------- API Functions ------------------------- */

/**
 * Get paginated list of all roles in the system
 * @param params - Query parameters (page, limit, active_only)
 */
export const getAllRoles = async (params?: GetAllRolesParams): Promise<GetAllRolesResponse> => {
  const response = await api.get('/v1/roles/', { params });
  return response.data;
};

/**
 * Create a new role in the system
 * @param data - Role data (role_code, role_name, description)
 */
export const createRole = async (data: CreateRoleData): Promise<RoleResponse> => {
  const response = await api.post('/v1/roles/', data);
  return response.data;
};

/**
 * Get detailed information about a specific role
 * @param roleId - Role ID
 */
export const getRoleDetails = async (roleId: number): Promise<RoleDetails> => {
  const response = await api.get(`/v1/roles/${roleId}`);
  return response.data;
};

/**
 * Update role information
 * @param roleId - Role ID
 * @param data - Updated role data (role_name, description)
 */
export const updateRole = async (roleId: number, data: UpdateRoleData): Promise<RoleResponse> => {
  const response = await api.put(`/v1/roles/${roleId}`, data);
  return response.data;
};

/**
 * Delete role if no users are assigned to it
 * @param roleId - Role ID
 */
export const deleteRole = async (roleId: number): Promise<RoleResponse> => {
  const response = await api.delete(`/v1/roles/${roleId}`);
  return response.data;
};

/**
 * Deactivate a role (users keep role but it becomes inactive)
 * @param roleId - Role ID
 */
export const deactivateRole = async (roleId: number): Promise<RoleResponse> => {
  const response = await api.patch(`/v1/roles/${roleId}/deactivate`);
  return response.data;
};

/**
 * Activate a deactivated role
 * @param roleId - Role ID
 */
export const activateRole = async (roleId: number): Promise<RoleResponse> => {
  const response = await api.patch(`/v1/roles/${roleId}/activate`);
  return response.data;
};

/**
 * Get list of users assigned to a specific role
 * @param roleId - Role ID
 * @param params - Query parameters (page, limit)
 */
export const getRoleUsers = async (roleId: number, params?: GetRoleUsersParams): Promise<any> => {
  const response = await api.get(`/v1/roles/users/${roleId}`, { params });
  return response.data.data;
};

export default api;
