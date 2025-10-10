// Authentication Service for communicating with backend

interface LoginCredentials {
  username: string;
  password: string;
  remember_me?: boolean;
}

interface LoginResponse {
  access_token: string;
  token_type: string;
  expires_in: number;
  user: User;
}

interface User {
  id: string;
  username: string;
  email: string;
  role: string;
  full_name: string;
  is_active: boolean;
}

interface ApiResponse<T> {
  success?: boolean;
  message?: string;
  data?: T;
}

class AuthService {
  private baseURL: string;
  private token: string | null = null;

  constructor() {
    // Use environment variable or default to localhost
    this.baseURL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

    // Load token from localStorage on initialization
    this.token = localStorage.getItem('auth_token');
  }

  // Set authorization header
  private getHeaders(): Record<string, string> {
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
    };

    if (this.token) {
      headers['Authorization'] = `Bearer ${this.token}`;
    }

    return headers;
  }

  // Generic API call method
  private async apiCall<T>(
    endpoint: string,
    options: RequestInit = {}
  ): Promise<T> {
    const url = `${this.baseURL}${endpoint}`;

    const config: RequestInit = {
      ...options,
      headers: {
        ...this.getHeaders(),
        ...options.headers,
      },
    };

    try {
      const response = await fetch(url, config);

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData.detail || `HTTP ${response.status}: ${response.statusText}`);
      }

      return await response.json();
    } catch (error) {
      console.error('API call failed:', error);
      throw error;
    }
  }

  // Login method
  async login(credentials: LoginCredentials): Promise<LoginResponse> {
    const response = await this.apiCall<LoginResponse>('/api/v1/simple-auth/login', {
      method: 'POST',
      body: JSON.stringify(credentials),
    });

    // Store token in localStorage and instance
    this.token = response.access_token;
    localStorage.setItem('auth_token', response.access_token);
    localStorage.setItem('user_data', JSON.stringify(response.user));

    return response;
  }

  // Logout method
  async logout(): Promise<void> {
    try {
      // Call backend logout endpoint
      await this.apiCall('/api/v1/simple-auth/logout', {
        method: 'POST',
      });
    } catch (error) {
      console.warn('Backend logout failed, but clearing local data:', error);
    } finally {
      // Clear local storage and token regardless of backend response
      this.token = null;
      localStorage.removeItem('auth_token');
      localStorage.removeItem('user_data');
    }
  }

  // Get current user info
  async getCurrentUser(): Promise<User> {
    return await this.apiCall<User>('/api/v1/simple-auth/me');
  }

  // Validate current token
  async validateToken(): Promise<{ valid: boolean; user?: User }> {
    try {
      const response = await this.apiCall<{ valid: boolean; user: User }>('/api/v1/simple-auth/validate');
      return response;
    } catch (error) {
      return { valid: false };
    }
  }

  // Get stored user data
  getStoredUser(): User | null {
    try {
      const userData = localStorage.getItem('user_data');
      return userData ? JSON.parse(userData) : null;
    } catch {
      return null;
    }
  }

  // Check if user is authenticated
  isAuthenticated(): boolean {
    return !!this.token && !!this.getStoredUser();
  }

  // Get current token
  getToken(): string | null {
    return this.token;
  }

  // Get test credentials (for development)
  async getTestCredentials(): Promise<Array<{
    username: string;
    password: string;
    role: string;
    description: string;
  }>> {
    try {
      const response = await this.apiCall<{
        credentials: Array<{
          username: string;
          password: string;
          role: string;
          description: string;
        }>;
      }>('/api/v1/simple-auth/test-credentials');

      return response.credentials;
    } catch (error) {
      console.error('Failed to fetch test credentials:', error);
      // Return default credentials if API fails
      return [
        {
          username: 'admin',
          password: 'admin123',
          role: 'admin',
          description: 'System Administrator (admin)',
        },
        {
          username: 'user1',
          password: 'user123',
          role: 'user',
          description: 'Test User One (user)',
        },
        {
          username: 'manager',
          password: 'manager123',
          role: 'manager',
          description: 'System Manager (manager)',
        },
        {
          username: 'analyst',
          password: 'analyst123',
          role: 'analyst',
          description: 'Data Analyst (analyst)',
        },
      ];
    }
  }

  // Make authenticated API calls
  async authenticatedCall<T>(endpoint: string, options: RequestInit = {}): Promise<T> {
    return this.apiCall<T>(endpoint, options);
  }

  // Clear all auth data (for force logout)
  clearAuthData(): void {
    this.token = null;
    localStorage.removeItem('auth_token');
    localStorage.removeItem('user_data');
  }
}

// Create and export singleton instance
const authService = new AuthService();
export default authService;

// Export types for use in components
export type { LoginCredentials, LoginResponse, User, ApiResponse };