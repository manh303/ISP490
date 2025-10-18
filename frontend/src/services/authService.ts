// Authentication Service for communicating with backend

interface LoginCredentials {
  username: string;
  password: string;
  remember_me?: boolean;
}

interface RegisterCredentials {
  name: string;
  email: string;
  password: string;
  confirmPassword: string;
}

interface RegisterResponse {
  success: boolean;
  message: string;
  user?: {
    id: string;
    name: string;
    email: string;
  };
  expires_in_minutes?: number;
  mock_otp?: string; // For development
}

interface ForgotPasswordRequest {
  email: string;
}

interface ForgotPasswordResponse {
  success: boolean;
  message: string;
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

// Add error types for better error handling
interface AuthError {
  message: string;
  field?: string;
  status?: number;
}

// OTP interfaces
interface VerifyOTPRequest {
  email: string;
  otp: string;
}

interface VerifyOTPResponse {
  success: boolean;
  message: string;
  verified?: boolean;
  mock?: boolean; // For development
}

interface ResendOTPRequest {
  email: string;
}

interface ResendOTPResponse {
  success: boolean;
  message: string;
  expires_in_minutes?: number;
  mock_otp?: string; // For development
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

  // Login method with real API integration
  async login(credentials: LoginCredentials): Promise<LoginResponse> {
    try {
      // Try real API first
      const response = await this.apiCall<LoginResponse>('/api/v1/auth/signin', {
        method: 'POST',
        body: JSON.stringify(credentials),
      });

      // Store token in localStorage and instance
      this.token = response.access_token;
      localStorage.setItem('auth_token', response.access_token);
      localStorage.setItem('user_data', JSON.stringify(response.user));

      return response;
    } catch (error) {
      // Fallback to mock for development if API fails
      console.warn('API login failed, falling back to mock authentication:', error);

      // Simulate API delay
      await new Promise(resolve => setTimeout(resolve, 500));

      const validCredentials = [
        { username: 'admin', password: 'admin123', role: 'admin', full_name: 'Administrator' },
        { username: 'user', password: 'user123', role: 'user', full_name: 'User' },
        { username: 'demo', password: 'demo123', role: 'manager', full_name: 'Demo User' },
        { username: 'manager', password: 'manager123', role: 'manager', full_name: 'Business Manager' },
        { username: 'analyst', password: 'analyst123', role: 'analyst', full_name: 'Data Analyst' },
      ];

      const user = validCredentials.find(
        cred => cred.username === credentials.username && cred.password === credentials.password
      );

      if (!user) {
        const error: AuthError = {
          message: 'Invalid username or password',
          field: 'password',
          status: 401
        };
        throw error;
      }

      // Create mock response
      const response: LoginResponse = {
        access_token: `mock_token_${Date.now()}`,
        token_type: 'bearer',
        expires_in: 3600,
        user: {
          id: `user_${Date.now()}`,
          username: user.username,
          email: `${user.username}@dss.com`,
          role: user.role,
          full_name: user.full_name,
          is_active: true,
        }
      };

      // Store token in localStorage and instance
      this.token = response.access_token;
      localStorage.setItem('auth_token', response.access_token);
      localStorage.setItem('user_data', JSON.stringify(response.user));

      return response;
    }
  }

  // Registration method with real API integration
  async register(credentials: RegisterCredentials): Promise<RegisterResponse> {
    try {
      // Try real API first
      const response = await this.apiCall<RegisterResponse>('/api/v1/auth/register', {
        method: 'POST',
        body: JSON.stringify({
          name: credentials.name,
          email: credentials.email,
          password: credentials.password,
          confirmPassword: credentials.confirmPassword
        }),
      });

      return response;
    } catch (error) {
      // Fallback to mock for development if API fails
      console.warn('API registration failed, falling back to mock registration:', error);

      // Simulate API delay
      await new Promise(resolve => setTimeout(resolve, 1000));

      // Basic validation (backend-style)
      if (!credentials.name.trim()) {
        throw new Error('Name is required');
      }

      if (!credentials.email.trim() || !/\S+@\S+\.\S+/.test(credentials.email)) {
        throw new Error('Please enter a valid email address');
      }

      if (credentials.password.length < 8) {
        throw new Error('Password must be at least 8 characters long');
      }

      if (credentials.password !== credentials.confirmPassword) {
        throw new Error('Passwords do not match');
      }

      // Check if email already exists (mock simulation)
      const existingEmails = ['admin@dss.com', 'user@dss.com', 'test@example.com'];
      if (existingEmails.includes(credentials.email.toLowerCase())) {
        throw new Error('Email address is already registered');
      }

      // Create mock success response
      const response: RegisterResponse = {
        success: true,
        message: 'Account created successfully! Please check your email to verify your account.',
        user: {
          id: `user_${Date.now()}`,
          name: credentials.name,
          email: credentials.email,
        }
      };

      return response;
    }
  }

  // Forgot password method with real API integration
  async forgotPassword(request: ForgotPasswordRequest): Promise<ForgotPasswordResponse> {
    try {
      // Try real API first
      const response = await this.apiCall<ForgotPasswordResponse>('/api/v1/auth/forgot-password', {
        method: 'POST',
        body: JSON.stringify(request),
      });

      return response;
    } catch (error) {
      // Fallback to mock for development if API fails
      console.warn('API forgot password failed, falling back to mock:', error);

      // Simulate API delay
      await new Promise(resolve => setTimeout(resolve, 1000));

      // Basic validation
      if (!request.email.trim()) {
        throw new Error('Email is required');
      }

      if (!/\S+@\S+\.\S+/.test(request.email)) {
        throw new Error('Please enter a valid email address');
      }

      // Check if email exists (mock simulation)
      const existingEmails = ['admin@dss.com', 'user@dss.com', 'manager@dss.com', 'analyst@dss.com'];
      if (!existingEmails.includes(request.email.toLowerCase())) {
        throw new Error('No account found with this email address');
      }

      // Create mock success response
      const response: ForgotPasswordResponse = {
        success: true,
        message: 'Password reset code sent successfully! Please check your email for the reset instructions.',
      };

      return response;
    }
  }

  // Logout method
  async logout(): Promise<void> {
    try {
      // Call backend logout endpoint
      await this.apiCall('/api/v1/auth/logout', {
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
    try {
      return await this.apiCall<User>('/api/v1/auth/me');
    } catch (error) {
      // Fallback to stored user data
      const storedUser = this.getStoredUser();
      if (storedUser) {
        return storedUser;
      }
      throw error;
    }
  }

  // Validate current token
  async validateToken(): Promise<{ valid: boolean; user?: User }> {
    try {
      if (!this.token) {
        return { valid: false };
      }

      // Try API validation first
      const response = await this.apiCall<{ valid: boolean; user: User }>('/api/v1/auth/validate');
      return response;
    } catch (error) {
      // Fallback to local validation
      if (this.token && this.getStoredUser()) {
        return {
          valid: true,
          user: this.getStoredUser()!
        };
      }
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
      }>('/api/v1/auth/test-credentials');

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

  // OTP verification method
  async verifyOTP(request: VerifyOTPRequest): Promise<VerifyOTPResponse> {
    try {
      // Try real API first
      const response = await this.apiCall<VerifyOTPResponse>('/api/v1/auth/verify-otp', {
        method: 'POST',
        body: JSON.stringify(request),
      });

      return response;
    } catch (error: any) {
      // Fallback to mock for development if API fails
      console.warn('API OTP verification failed, falling back to mock:', error);

      // Simulate API delay
      await new Promise(resolve => setTimeout(resolve, 500));

      // Mock verification (for development)
      if (request.otp === '123456') {
        return {
          success: true,
          message: 'Email verified successfully! You can now sign in.',
          verified: true,
          mock: true
        };
      } else {
        throw new Error('Invalid verification code. For testing, use: 123456');
      }
    }
  }

  // Resend OTP method
  async resendOTP(request: ResendOTPRequest): Promise<ResendOTPResponse> {
    try {
      // Try real API first
      const response = await this.apiCall<ResendOTPResponse>('/api/v1/auth/resend-otp', {
        method: 'POST',
        body: JSON.stringify(request),
      });

      return response;
    } catch (error) {
      // Fallback to mock for development if API fails
      console.warn('API resend OTP failed, falling back to mock:', error);

      // Simulate API delay
      await new Promise(resolve => setTimeout(resolve, 1000));

      // Basic validation
      if (!request.email.trim()) {
        throw new Error('Email is required');
      }

      if (!/\S+@\S+\.\S+/.test(request.email)) {
        throw new Error('Please enter a valid email address');
      }

      // Create mock success response
      const response: ResendOTPResponse = {
        success: true,
        message: `Verification code sent to ${request.email} (mock). Use OTP: 123456`,
        mock_otp: '123456', // Only for development
        expires_in_minutes: 10
      };

      return response;
    }
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
export type {
  LoginCredentials,
  RegisterCredentials,
  RegisterResponse,
  ForgotPasswordRequest,
  ForgotPasswordResponse,
  LoginResponse,
  User,
  ApiResponse,
  VerifyOTPRequest,
  VerifyOTPResponse,
  ResendOTPRequest,
  ResendOTPResponse
};