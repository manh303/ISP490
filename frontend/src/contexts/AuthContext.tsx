import React, {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useState,
  ReactNode
} from 'react';
import Cookies from 'js-cookie';
import { authAPI, UserProfile, SignInRequest } from '../services/api';

interface AuthContextType {
  user: UserProfile | null;
  loading: boolean;
  isAuthenticated: boolean;
  signin: (credentials: SignInRequest) => Promise<void>;
  logout: () => Promise<void>;
  updateProfile: (data: { full_name?: string; phone?: string; email?: string }) => Promise<void>;
  changePassword: (data: { current_password: string; new_password: string }) => Promise<void>;
  hasPermission: (permission: string) => boolean;
  hasRole: (role: string) => boolean;
  refreshUserData: () => Promise<void>;
}

const defaultGuest: UserProfile = {
  user_id: '0',
  email: '',
  full_name: 'Guest',
  phone: '',
  status: 'inactive',
  roles: [],
  permissions: []
};

function tryParseJSON<T = any>(s?: string | null): T | null {
  if (!s) return null;
  try { return JSON.parse(s) as T; } catch { return null; }
}

/** Chuẩn hoá mọi dạng user (role string / roles[]) về UserProfile nhất quán */
function normalizeUser(raw: any): UserProfile {
  if (!raw || typeof raw !== 'object') return { ...defaultGuest };

  const user_id = String(raw.user_id ?? raw.id ?? '0');
  const email = raw.email ?? '';
  const full_name = raw.full_name ?? raw.name ?? '';
  const phone = raw.phone ?? '';
  const status = raw.status ?? (raw.is_active === false ? 'inactive' : 'active');

  // role có thể là string hoặc nằm trong mảng roles[]
  const roleFromArray: string =
    (Array.isArray(raw.roles) && raw.roles[0] &&
      (raw.roles[0].role || raw.roles[0].role_code || raw.roles[0].code)) ?? '';
  const role = (raw.role || roleFromArray || '').toString().toLowerCase();

   // chuẩn hoá roles[]
   const roles =
     Array.isArray(raw.roles) && raw.roles.length
       ? raw.roles.map((r: any) => ({
           role_id: String(r.role_id ?? r.id ?? ''),
           role_code: String((r.role_code ?? r.role ?? r.code ?? role) || '').toUpperCase(),
           role_name: String((r.role_name ?? r.name ?? r.role ?? role) || ''),
           description: r.description ?? ''
         }))
       : (role ? [{ role_id: '0', role_code: role.toUpperCase(), role_name: role, description: '' }] : []);

  // chuẩn hoá permissions[]
  const permissions =
    Array.isArray(raw.permissions)
      ? raw.permissions.map((p: any, idx: number) =>
          typeof p === 'string'
            ? {
                perm_id: String(idx + 1),
                perm_code: p,
                perm_name: p,
                module: p.split('.')[0] || 'app',
                action: p.split('.')[1] || 'access'
              }
            : {
                perm_id: String(p.perm_id ?? idx + 1),
                perm_code: String(p.perm_code ?? p.code ?? ''),
                perm_name: String(p.perm_name ?? p.name ?? p.perm_code ?? ''),
                module: String(p.module ?? p.perm_code?.split('.')[0] ?? 'app'),
                action: String(p.action ?? p.perm_code?.split('.')[1] ?? 'access')
              }
        )
      : [];

  const normalized: UserProfile = {
    user_id, email, full_name, phone, status, roles, permissions
  };
  return normalized;
}

/** Convert v1 profile response to UserProfile format */
function convertV1ProfileToUserProfile(profileData: any): UserProfile {
  const roleCode = String(profileData.role_code || '').toUpperCase();
  
  return {
    user_id: String(profileData.user_id || '0'),
    email: profileData.email || '',
    full_name: profileData.full_name || '',
    phone: profileData.phone || '',
    status: profileData.status || 'inactive',
    roles: [
      {
        role_id: '1',
        role_code: roleCode,
        role_name: profileData.role_name || roleCode,
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
        : roleCode === 'DATA_ENGINEER'
        ? [
            { perm_id: '10', perm_code: 'data.pipeline',  perm_name: 'Data Pipeline',         module: 'data',     action: 'pipeline' },
            { perm_id: '11', perm_code: 'etl.manage',     perm_name: 'ETL Management',        module: 'etl',      action: 'manage' },
            { perm_id: '3', perm_code: 'data.write',      perm_name: 'Write Data',            module: 'data',     action: 'write' },
            { perm_id: '12', perm_code: 'system.monitor', perm_name: 'System Monitoring',     module: 'system',   action: 'monitor' }
          ]
        : roleCode === 'CUSTOMER'
        ? [
            { perm_id: '7', perm_code: 'profile.view',    perm_name: 'View Profile',          module: 'profile',  action: 'view' },
            { perm_id: '8', perm_code: 'orders.create',   perm_name: 'Create Orders',         module: 'orders',   action: 'create' },
            { perm_id: '9', perm_code: 'data.read_own',   perm_name: 'Read Own Data',         module: 'data',     action: 'read_own' }
          ]
        : roleCode === 'ML' || roleCode === 'MLI'
        ? [
            { perm_id: '13', perm_code: 'ml.model.manage', perm_name: 'ML Model Management',   module: 'ml',       action: 'manage' },
            { perm_id: '14', perm_code: 'ml.train',        perm_name: 'ML Training',           module: 'ml',       action: 'train' },
            { perm_id: '15', perm_code: 'ml.predict',      perm_name: 'ML Prediction',         module: 'ml',       action: 'predict' },
            { perm_id: '16', perm_code: 'ml.insights',    perm_name: 'ML Insights',           module: 'ml',       action: 'insights' },
            { perm_id: '3', perm_code: 'data.read',       perm_name: 'Read Data',             module: 'data',     action: 'read' },
            { perm_id: '4', perm_code: 'analytics.view',  perm_name: 'View Analytics',        module: 'analytics',action: 'view' }
          ]
        : []
  };
}

/** Lưu user vào cookie `user_data` để các lần refresh vẫn có */
function persistUser(user: UserProfile | null) {
  if (!user) {
    Cookies.remove('user_data');
    return;
  }
  Cookies.set('user_data', JSON.stringify(user), {
    expires: 7,
    secure: window.location.protocol === 'https:',
    sameSite: 'strict'
  });
}

/** ----- Context + Hook ----- */
const FallbackContext: AuthContextType = {
  user: defaultGuest,
  loading: false,
  isAuthenticated: false,
  signin: async () => { throw new Error('AuthProvider is not mounted'); },
  logout: async () => {},
  updateProfile: async () => { throw new Error('AuthProvider is not mounted'); },
  changePassword: async () => { throw new Error('AuthProvider is not mounted'); },
  hasPermission: () => false,
  hasRole: () => false,
  refreshUserData: async () => { throw new Error('AuthProvider is not mounted'); },
};

const AuthContext = createContext<AuthContextType | undefined>(undefined);

export const useAuth = () => {
  // Trả fallback thay vì ném lỗi để không bao giờ là undefined (tránh .user của undefined)
  return useContext(AuthContext) ?? FallbackContext;
};

interface AuthProviderProps { children: ReactNode; }

export const AuthProvider: React.FC<AuthProviderProps> = ({ children }) => {
  const [user, setUser] = useState<UserProfile | null>(null);
  const [loading, setLoading] = useState(true);

  /** Xác định đã đăng nhập chưa: cần token + user hợp lệ */
  const isAuthenticated = !!Cookies.get('access_token') && !!(user && user.user_id !== '0');

  const refreshUserData = async () => {
    try {
      console.log('[Auth] refreshUserData: start');
      
      // Try new v1 API first
      try {
        const profileData = await authAPI.getMyProfile();
        console.log('[Auth] v1 profile response:', profileData);
        
        // Convert v1 response to UserProfile format
        const userProfile = convertV1ProfileToUserProfile(profileData);
        console.log('[Auth] v1 profile converted:', userProfile);
        
        setUser(userProfile);
        persistUser(userProfile);
        return;
      } catch (v1Error) {
        console.warn('[Auth] v1 profile failed, falling back to old API:', v1Error);
      }

      // Fallback to old API
      const response = await authAPI.getProfile();
      console.log('[Auth] fallback profile raw response:', response);

      // Chấp nhận cả 2 dạng: {success, user} hoặc {success, data:{...}}
      const payload: any = response?.data ?? response;
      if (!payload) throw new Error('No profile payload');
      const rawUser = payload.user ?? payload;
      if (!rawUser) throw new Error('No user in profile payload');

      const normalized = normalizeUser(rawUser);
      console.log('[Auth] fallback profile normalized:', normalized);

      setUser(normalized);
      persistUser(normalized);
    } catch (err) {
      console.error('[Auth] refreshUserData error:', err);
      setUser(null);
      persistUser(null);
      throw err;
    }
  };

  // Khởi tạo từ cookies
  useEffect(() => {
    const init = async () => {
      const token = Cookies.get('access_token');
      const storedRaw = Cookies.get('user_data');

      console.log('AuthContext: Initializing with token:', !!token, 'Stored user:', !!storedRaw);

      if (token && storedRaw) {
        const parsed = tryParseJSON<UserProfile>(storedRaw);
        if (parsed) {
          console.log('AuthContext: Restoring user from cookies:', parsed);
          const normalized = normalizeUser(parsed);
          setUser(normalized);
        } else {
          console.warn('[Auth] invalid user_data cookie -> clearing');
          Cookies.remove('access_token');
          Cookies.remove('refresh_token');
          Cookies.remove('user_data');
          setUser(null);
        }
      } else if (token && !storedRaw) {
        console.log('AuthContext: Token exists but no stored user → fetch profile...');
        try {
          await refreshUserData();
        } catch (e) {
          Cookies.remove('access_token');
          Cookies.remove('refresh_token');
          Cookies.remove('user_data');
          setUser(null);
        }
      } else {
        setUser(null);
      }
      setLoading(false);
    };
    init();
  }, []);

  const signin = async (credentials: SignInRequest) => {
    try {
      setLoading(true);
      const response = await authAPI.loginDatabase(credentials);
      // Chuẩn hoá 2 dạng trả về: {success, data:{user, tokens}} hoặc {success, user, access_token}
      let userData: any;
      let accessToken = '';

      if (response?.data?.user) {
        userData = response.data.user;
        accessToken = response.data.tokens?.access_token || response.data.access_token || '';
      } else if ((response as any)?.user) {
        userData = (response as any).user;
        accessToken = (response as any).access_token || '';
      } else {
        throw new Error(response?.message || 'SignIn failed: invalid payload');
      }

      if (!accessToken) throw new Error('SignIn failed: missing access_token');

      // Lưu token
      Cookies.set('access_token', accessToken, {
        expires: 0.5, // 12h
        secure: window.location.protocol === 'https:',
        sameSite: 'strict'
      });

      // Lưu refresh (nếu có)
      const refreshToken =
        response?.data?.tokens?.refresh_token ||
        (response as any)?.refresh_token ||
        '';
      if (refreshToken) {
        Cookies.set('refresh_token', refreshToken, {
          expires: 7,
          secure: window.location.protocol === 'https:',
          sameSite: 'strict'
        });
      }

      // Chuẩn hoá user và lưu cookie
      const normalized = normalizeUser(userData);
      setUser(normalized);
      persistUser(normalized);
    } catch (error: any) {
      console.error('SignIn error:', error);
      let msg = 'SignIn failed';
      if (error.response?.data?.detail) msg = error.response.data.detail;
      else if (error.response?.data?.message) msg = error.response.data.message;
      else if (error.message) msg = error.message;
      throw new Error(msg);
    } finally {
      setLoading(false);
    }
  };

  const logout = async () => {
    try {
      const refreshToken = Cookies.get('refresh_token');
      if (refreshToken) await authAPI.logout(refreshToken);
    } catch (error) {
      console.error('Logout error:', error);
    } finally {
      Cookies.remove('access_token');
      Cookies.remove('refresh_token');
      Cookies.remove('user_data');
      setUser(null);
    }
  };

  const updateProfile = async (data: { full_name?: string; phone?: string; email?: string }) => {
    try {
      console.log('[Auth] updateProfile: start with data', data);
      
      // Sử dụng API mới v1/profile
      const response = await authAPI.updateMyProfile(data);
      console.log('[Auth] updateProfile: response', response);
      
      if (response.success) {
        // Refresh user data từ server để có thông tin mới nhất
        await refreshUserData();
        console.log('[Auth] updateProfile: success, user data refreshed');
      } else {
        throw new Error(response.message || 'Update profile failed');
      }
    } catch (error: any) {
      console.error('Update profile error:', error);
      throw new Error(error.message || 'Update profile failed');
    }
  };

  const changePassword = async (data: { current_password: string; new_password: string }) => {
    try {
      const response = await authAPI.changePassword(data);
      if (!response.success) {
        throw new Error(response.message || 'Change password failed');
      }
    } catch (error: any) {
      console.error('Change password error:', error);
      throw new Error(error.response?.data?.message || error.message || 'Change password failed');
    }
  };

  /** ADMIN có tất cả quyền. So sánh không phân biệt hoa-thường. */
  const hasPermission = (permission: string): boolean => {
    if (!user) return false;
    const isAdmin =
      user.roles?.some(r => String(r.role_code || r.role_name || r.role_id).toUpperCase() === 'ADMIN') ||
      // fallback: backend nào chỉ có role string
      (user as any).role?.toString().toLowerCase() === 'admin';
    if (isAdmin) return true;

    if (!Array.isArray(user.permissions)) return false;
    const p = permission.toLowerCase();
    return user.permissions.some((perm: any) =>
      String(perm.perm_code ?? perm.code ?? '')
        .toLowerCase() === p
    );
  };

  const hasRole = (role: string): boolean => {
    if (!user) return false;
    const want = role.toLowerCase();
    const fromRoles =
      user.roles?.some(r => String(r.role_code || r.role_name || r.role_id).toLowerCase() === want) || false;
    const fromString = (user as any).role?.toString().toLowerCase() === want;
    return !!(fromRoles || fromString);
  };

  const value: AuthContextType = {
    user: user ?? defaultGuest,
    loading,
    isAuthenticated,
    signin,
    logout,
    updateProfile,
    changePassword,
    hasPermission,
    hasRole,
    refreshUserData
  };

  return (
    <AuthContext.Provider value={value}>
      {children}
    </AuthContext.Provider>
  );
};

/** --------- ProtectedRoute --------- */
interface ProtectedRouteProps {
  children: ReactNode;
  requireAuth?: boolean;
  requiredPermission?: string;
  requiredRole?: string | string[];
  fallback?: ReactNode;
}

export function ProtectedRoute({
  children,
  requireAuth = true,
  requiredPermission,
  requiredRole,
  fallback
}: ProtectedRouteProps) {
  const { user, loading, isAuthenticated, hasPermission, hasRole } = useAuth();

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <div className="flex flex-col items-center space-y-4">
          <div className="w-8 h-8 border-4 border-blue-200 border-t-blue-600 rounded-full animate-spin"></div>
          <p className="text-gray-600">Loading...</p>
        </div>
      </div>
    );
  }

  if (requireAuth && !isAuthenticated) {
    return fallback || (
      <div className="flex items-center justify-center min-h-screen">
        <div className="text-center">
          <h2 className="text-xl font-semibold text-gray-800 mb-2">Authentication Required</h2>
          <p className="text-gray-600">Please log in to access this page.</p>
        </div>
      </div>
    );
  }

  if (requiredPermission && !hasPermission(requiredPermission)) {
    return fallback || (
      <div className="flex items-center justify-center min-h-screen">
        <div className="text-center">
          <h2 className="text-xl font-semibold text-gray-800 mb-2">Access Denied</h2>
          <p className="text-gray-600">You don't have permission to access this page.</p>
          <p className="text-sm text-gray-500 mt-2">Required permission: {requiredPermission}</p>
        </div>
      </div>
    );
  }

  if (requiredRole) {
    const roles = Array.isArray(requiredRole) ? requiredRole : [requiredRole];
    const hasRequiredRole = roles.some(role => hasRole(role));
    if (!hasRequiredRole) {
      return fallback || (
        <div className="flex items-center justify-center min-h-screen">
          <div className="text-center">
            <h2 className="text-xl font-semibold text-gray-800 mb-2">Access Denied</h2>
            <p className="text-gray-600">You don't have the required role to access this page.</p>
            <p className="text-sm text-gray-500 mt-2">Required roles: {roles.join(', ')}</p>
          </div>
        </div>
      );
    }
  }

  return <>{children}</>;
}

export default AuthContext;
