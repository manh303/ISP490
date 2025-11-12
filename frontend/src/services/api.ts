import axios from 'axios';
import Cookies from 'js-cookie';

/** API root (ví dụ: http://localhost:8000) */
const API_BASE_URL = import.meta.env.VITE_API_URL || 'https://ecommerce-dss-backend.onrender.com';

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

/** Tự refresh access_token khi 401 (nếu có refresh_token) */
api.interceptors.response.use(
  (response) => response,
  async (error) => {
    const originalRequest: any = error.config;
    if (error.response?.status === 401 && !originalRequest?._retry) {
      originalRequest._retry = true;
      try {
        const refreshToken = Cookies.get('refresh_token');
        if (refreshToken) {
          const resp = await axios.post(
            `${API_BASE_URL}/api/auth/refresh`,
            { refresh_token: refreshToken },
            { headers: { 'Content-Type': 'application/json' } }
          );
          const access_token =
            resp?.data?.data?.access_token ||
            resp?.data?.access_token ||
            '';

          if (access_token) {
            Cookies.set('access_token', access_token, { expires: 0.5 }); // 12h
            originalRequest.headers = originalRequest.headers ?? {};
            originalRequest.headers.Authorization = `Bearer ${access_token}`;
            return api(originalRequest);
          }
        }
      } catch (e) {
        // Refresh fail -> xoá token & chuyển về signin
        Cookies.remove('access_token');
        Cookies.remove('refresh_token');
        window.location.href = '/auth/signin';
        return Promise.reject(e);
      }
    }
    return Promise.reject(error);
  }
);

/* ---------------------------- Types ----------------------------- */

export interface ApiResponse<T = any> {
  success: boolean;
  message: string;
  data?: T;
}

export interface SignInRequest {
  email: string;
  password: string;
}

/** Dạng data FE mong đợi sau login */
export interface SignInResponse {
  access_token: string; // lặp lại trong tokens.access_token cho dễ dùng
  user: {
    user_id: string;
    email: string;
    full_name: string;
    phone: string;
    status: string;
    roles: Array<{ role_id: string; role_code: string; role_name: string; description: string }>;
    permissions: Array<{ perm_id: string; perm_code: string; perm_name: string; module: string; action: string }>;
  };
  tokens: {
    access_token: string;
    refresh_token: string;
    token_type: string;
    expires_in: string;
  };
}

export interface UserProfile {
  roles?: any[];
  permissions?: any[];
  user_id?: string;
  email?: string;
  full_name?: string;
  phone?: string;
  status?: string;
}

export interface UserProfileResponse {
  user_id: number;
  email: string;
  full_name: string;
  phone: string;
  status: string;
  role_code: string;
  role_name: string;
  last_login_at: string;
  created_at: string;
  updated_at: string;
}

export interface ChangePasswordRequest { current_password: string; new_password: string; }
export interface UpdateProfileRequest { full_name?: string; phone?: string; email?: string; }
export interface UpdateProfileResponse { 
  success: boolean; 
  message: string; 
  data?: any; 
  user_id?: number; 
}
export interface SignupRequest { name: string; email: string; password: string; confirm_password: string; }
export interface SignupResponse { success: boolean; message: string; verification_sent: boolean; email: string; }
export interface VerifyEmailRequest { email: string; verification_code: string; }
export interface VerifyEmailResponse { success: boolean; message: string; user_created: boolean; user_id?: number; }
export interface ForgotPasswordRequest { email: string; }
export interface ForgotPasswordResponse { success: boolean; message: string; data?: { reset_token?: string; note?: string; } }
export interface ResetPasswordRequest { email: string; otp: string; new_password: string; }
export interface ResetPasswordResponse { success: boolean; message: string; }

/* ----------------------------- API ------------------------------ */

export const adminAPI = {
  getActivityLogs: async (params: {
    page?: number;
    limit?: number;
    user_id?: number;
    action?: string;
    start_date?: string;
    end_date?: string;
  } = {}) => {
    const queryParams = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined) queryParams.append(key, value.toString());
    });
    
    const response = await api.get(`/v1/admin/activity-logs?${queryParams}`);
    return response.data;
  },

  getActivityStats: async (days: number = 7) => {
    const response = await api.get(`/v1/admin/activity-stats?days=${days}`);
    return response.data;
  },

  getUserActivity: async (userId: number, page: number = 1, limit: number = 20) => {
    const response = await api.get(`/v1/admin/user-activity/${userId}?page=${page}&limit=${limit}`);
    return response.data;
  },

  clearActivityLogs: async (daysOlderThan: number = 30) => {
    const response = await api.post(`/v1/admin/clear-activity-logs?days_older_than=${daysOlderThan}`);
    return response.data;
  }
};

export const authAPI = {
  /** Đăng nhập DB (FastAPI) */
  loginDatabase: async (data: SignInRequest): Promise<ApiResponse<SignInResponse>> => {
    const response = await api.post('/v1/auth/signin', data);
    const backendData = response.data;

    // Lấy access_token & role
    const access = backendData?.access_token || backendData?.data?.access_token || '';
    const roleRaw = String(backendData?.user?.role || 'customer');
    const roleCode = roleRaw.toUpperCase(); // ADMIN / ANALYST / CUSTOMER

    // --- set cookies ngay sau login ---
    if (access) {
      Cookies.set('access_token', access, { expires: 0.5 }); // 12h
      // nếu backend chưa trả refresh → set tạm để interceptor không crash
      const fallbackRefresh = `refresh_${Date.now()}`;
      const refresh = backendData?.refresh_token || backendData?.data?.tokens?.refresh_token || fallbackRefresh;
      Cookies.set('refresh_token', refresh, { expires: 7 });
    }

    // Chuẩn hoá user gửi cho FE/Context
    const userData = {
      user_id: String(backendData?.user?.user_id ?? backendData?.user?.id ?? '0'),
      email: backendData?.user?.email ?? '',
      full_name: backendData?.user?.full_name ?? '',
      phone: '+84901234567',
      status: 'active',
      roles: [
        {
          role_id: '1',
          role_code: roleCode,
          role_name: roleCode,
          description: 'System Role'
        }
      ],
      permissions:
        roleCode === 'ADMIN'
          ? [
              { perm_id: '1', perm_code: 'system.admin',    perm_name: 'System Administration', module: 'system',   action: 'admin' },
              { perm_id: '2', perm_code: 'user.manage',     perm_name: 'User Management',       module: 'user',     action: 'manage' },
              { perm_id: '3', perm_code: 'data.write',      perm_name: 'Write Data',            module: 'data',     action: 'write' },
              { perm_id: '4', perm_code: 'analytics.view',  perm_name: 'View Analytics',        module: 'analytics',action: 'view' },
              { perm_id: '5', perm_code: 'dss.dashboard',   perm_name: 'DSS Dashboard',         module: 'dss',      action: 'read' }
            ]
          : roleCode === 'ANALYST'
          ? [
              { perm_id: '3', perm_code: 'data.read',       perm_name: 'Read Data',             module: 'data',     action: 'read' },
              { perm_id: '4', perm_code: 'analytics.view',  perm_name: 'View Analytics',        module: 'analytics',action: 'view' },
              { perm_id: '6', perm_code: 'reports.generate',perm_name: 'Generate Reports',      module: 'reports',  action: 'generate' },
              { perm_id: '5', perm_code: 'dss.dashboard',   perm_name: 'DSS Dashboard',         module: 'dss',      action: 'read' }
            ]
          : [
              { perm_id: '7', perm_code: 'profile.view',    perm_name: 'View Profile',          module: 'profile',  action: 'view' },
              { perm_id: '8', perm_code: 'orders.create',   perm_name: 'Create Orders',         module: 'orders',   action: 'create' },
              { perm_id: '9', perm_code: 'data.read_own',   perm_name: 'Read Own Data',         module: 'data',     action: 'read_own' }
            ]
    };

    // Lưu user_data để AuthContext khôi phục sau reload
    Cookies.set('user_data', JSON.stringify(userData), { expires: 7 });

    // Trả đúng shape FE mong đợi
    return {
      success: backendData?.success !== undefined ? backendData.success : true,
      message: backendData?.message || 'Login success',
      data: {
        access_token: access, // <- lặp lại ở đây cho khớp SignInResponse
        user: userData,
        tokens: {
          access_token: access,
          refresh_token: Cookies.get('refresh_token') || '',
          token_type: 'bearer',
          expires_in: '86400'
        }
      }
    };
  },

  logout: async (refreshToken: string): Promise<ApiResponse> => {
    const res = await api.post('/auth/signout', { refresh_token: refreshToken });
    Cookies.remove('access_token');
    Cookies.remove('refresh_token');
    return res.data;
  },

  /** Lấy profile: BE route nằm ngoài /v1 */
  getProfile: async () => {
    const token = Cookies.get('access_token');
    console.log('getProfile: Token exists:', !!token);
    if (!token) throw new Error('No access token available');

    const url = `${API_BASE_URL}/api/auth/profile`;
    console.log('getProfile: Calling', url);

    const response = await fetch(url, {
      method: 'GET',
      headers: {
        Authorization: `Bearer ${token}`,
        'Content-Type': 'application/json'
      }
    });

    console.log('getProfile: Response status', response.status);

    if (!response.ok) {
      const errorText = await response.text();
      console.error('getProfile: Error response', errorText);
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    const data = await response.json();
    console.log('getProfile: Response data', data);
    // Trả nguyên data (AuthContext đã xử lý normalize)
    return data;
  },

  /** Get current user's profile - v1 API */
  getMyProfile: async (): Promise<UserProfileResponse> => {
    console.log('getMyProfile: Start fetching profile');
    try {
      const response = await api.get('/v1/profile');
      console.log('getMyProfile: Response', response.data);
      return response.data;
    } catch (error: any) {
      console.error('getMyProfile: Error', error);
      throw new Error(error.response?.data?.message || error.message || 'Failed to fetch profile');
    }
  },

  updateProfile: async (data: UpdateProfileRequest): Promise<UpdateProfileResponse> => {
    const response = await api.put('/v1/profile', data);
    return response.data;
  },

  /** Update current user's profile - v1 API */
  updateMyProfile: async (data: UpdateProfileRequest): Promise<UpdateProfileResponse> => {
    console.log('updateMyProfile: Sending data', data);
    try {
      const response = await api.put('/v1/profile', data);
      console.log('updateMyProfile: Response', response.data);
      return response.data;
    } catch (error: any) {
      console.error('updateMyProfile: Error', error);
      throw new Error(error.response?.data?.message || error.message || 'Update profile failed');
    }
  },

  changePassword: async (data: ChangePasswordRequest) =>
    (await api.post('/auth/change-password', data)).data,

  checkPermission: async (permission: string) =>
    (await api.get(`/auth/permissions/${permission}`)).data,

  refreshToken: async (refreshToken: string) =>
    (await api.post('/auth/refresh', { refresh_token: refreshToken })).data,

  signup: async (data: SignupRequest) =>
    (await api.post('/v1/auth/signup', data)).data,

  signout: async (refreshToken: string) =>
    (await api.post('/v1/auth/signout', { refresh_token: refreshToken })).data,

  verifyEmail: async (data: VerifyEmailRequest) =>
    (await api.post('/v1/auth/verify-email', data)).data,

  forgotPassword: async (data: ForgotPasswordRequest): Promise<ForgotPasswordResponse> =>
    (await api.post('/v1/auth/forgot-password', data)).data,

  resetPassword: async (data: ResetPasswordRequest): Promise<ResetPasswordResponse> =>
    (await api.post('/v1/auth/reset-password', data)).data
};

export default api;
