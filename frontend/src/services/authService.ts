// authService.ts (REPLACE ENTIRE FILE)
import Cookies from 'js-cookie';

type AnyJson = any;

interface LoginCredentials {
  email?: string;
  username?: string;   // legacy
  password: string;
  remember_me?: boolean;
}

interface User {
  id: string;
  username: string;
  email: string;
  role: string;        // "admin"
  full_name: string;
  is_active: boolean;
}

interface NormalizedLogin {
  success: boolean;
  message: string;
  access_token: string;
  user: {
    user_id: string | number;
    email: string;
    full_name: string;
    role: string;      // "admin"
  };
}

class AuthService {
  async forgotPassword(email: string) {
    const payload = { email: email.trim() };
    return await this.call('/api/v1/auth/forgot-password', { method: 'POST', body: JSON.stringify(payload) });
  }
  private baseURL: string;
  private token: string | null;

  constructor() {
    this.baseURL = (import.meta as any).env?.VITE_API_URL || 'http://localhost:8000';
    this.token = Cookies.get('access_token') || null;
  }

  private headers() {
    const h: Record<string, string> = { 'Content-Type': 'application/json' };
    const t = this.getToken();
    if (t) h['Authorization'] = `Bearer ${t}`;
    return h;
  }

  private async call<T = AnyJson>(path: string, init: RequestInit): Promise<T> {
    const res = await fetch(`${this.baseURL}${path}`, { ...init, headers: { ...this.headers(), ...(init.headers || {}) } });
    if (!res.ok) {
      let msg = `HTTP ${res.status}`;
      try { const j = await res.json(); msg = j.detail || j.message || msg; } catch {}
      throw new Error(msg);
    }
    return res.json();
  }

  /** Chuẩn hóa mọi kiểu payload của backend thành 1 dạng duy nhất */
  private normalizeLoginPayload(raw: AnyJson): NormalizedLogin {
    // Các khả năng thường gặp
    const fromTop = raw?.user && raw?.access_token;
    const fromData = raw?.data?.user && (raw?.data?.tokens?.access_token || raw?.data?.access_token);
    const minimal  = raw?.access_token && (raw?.email || raw?.role);

    if (fromTop) {
      return {
        success: !!raw.success,
        message: raw.message || 'OK',
        access_token: raw.access_token,
        user: {
          user_id: raw.user.user_id ?? raw.user.id ?? '0',
          email: raw.user.email ?? '',
          full_name: raw.user.full_name ?? raw.user.name ?? '',
          role: (raw.user.role || 'admin').toLowerCase()
        }
      };
    }

    if (fromData) {
      return {
        success: !!raw.success,
        message: raw.message || 'OK',
        access_token: raw.data.tokens?.access_token || raw.data.access_token,
        user: {
          user_id: raw.data.user.user_id ?? raw.data.user.id ?? '0',
          email: raw.data.user.email ?? '',
          full_name: raw.data.user.full_name ?? raw.data.user.name ?? '',
          role: (raw.data.user.role || 'admin').toLowerCase()
        }
      };
    }

    if (minimal) {
      return {
        success: !!raw.success,
        message: raw.message || 'OK',
        access_token: raw.access_token,
        user: {
          user_id: raw.user_id ?? '0',
          email: raw.email ?? '',
          full_name: raw.full_name ?? '',
          role: (raw.role || 'admin').toLowerCase()
        }
      };
    }

    // Nếu không khớp gì → ném lỗi rõ ràng
    throw new Error('Login payload missing `user` or `access_token`');
  }

  private persist(normalized: NormalizedLogin) {
    this.token = normalized.access_token;
    // lưu 12h
    Cookies.set('access_token', normalized.access_token, { expires: 0.5 });

    const userCookie: User = {
      id: String(normalized.user.user_id || ''),
      username: (normalized.user.email || '').split('@')[0] || 'admin',
      email: normalized.user.email || '',
      role: (normalized.user.role || 'admin').toLowerCase(),
      full_name: normalized.user.full_name || '',
      is_active: true
    };
    Cookies.set('user_data', JSON.stringify(userCookie), { expires: 7 });
  }

  // --- API ---
  async login({ email, username, password }: LoginCredentials) {
    const payload = { email: (email || username || '').trim(), password };
    const raw = await this.call('/api/v1/auth/signin', { method: 'POST', body: JSON.stringify(payload) });
    const normalized = this.normalizeLoginPayload(raw);
    // chuẩn hóa role -> "admin"
    normalized.user.role = 'admin';
    this.persist(normalized);
    return normalized;   // <-- LUÔN có normalized.user
  }

  async logout() {
    this.token = null;
    Cookies.remove('access_token');
    Cookies.remove('user_data');
    try { await this.call('/api/v1/auth/logout', { method: 'POST' }); } catch {}
  }

  getToken() { return this.token || Cookies.get('access_token') || null; }

  getStoredUser(): User | null {
    try { const s = Cookies.get('user_data'); return s ? JSON.parse(s) as User : null; }
    catch { return null; }
  }

  /** Trả user chắc chắn có dạng hợp lệ, tránh UI đọc undefined.user */
  getSafeUser(): User {
    return this.getStoredUser() || {
      id: '0', username: 'guest', email: '', role: 'guest', full_name: 'Guest', is_active: false
    };
  }

  isAuthenticated(): boolean {
    return !!(this.getToken() && this.getStoredUser());
  }
}

const authService = new AuthService();
export default authService;
export type { LoginCredentials, User };
