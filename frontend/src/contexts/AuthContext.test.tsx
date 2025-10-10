import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { BrowserRouter } from 'react-router'
import { AuthProvider, useAuth, ProtectedRoute } from './AuthContext'
import authService from '../services/authService'

// Mock authService
vi.mock('../services/authService', () => ({
  default: {
    login: vi.fn(),
    logout: vi.fn(),
    validateToken: vi.fn(),
    getToken: vi.fn(),
  },
}))

// Mock navigate
const mockNavigate = vi.fn()
vi.mock('react-router', async () => {
  const actual = await vi.importActual('react-router')
  return {
    ...actual,
    useNavigate: () => mockNavigate,
  }
})

// Test component that uses AuthContext
const TestComponent = () => {
  const { state, login, logout } = useAuth()

  return (
    <div>
      <div data-testid="auth-status">
        {state.isAuthenticated ? 'Authenticated' : 'Not Authenticated'}
      </div>
      <div data-testid="loading-status">
        {state.isLoading ? 'Loading' : 'Not Loading'}
      </div>
      {state.user && (
        <div data-testid="user-info">
          {state.user.username} - {state.user.role}
        </div>
      )}
      {state.error && (
        <div data-testid="error-message">{state.error}</div>
      )}
      <button onClick={() => login({ username: 'test', password: 'test' })}>
        Login
      </button>
      <button onClick={logout}>Logout</button>
    </div>
  )
}

const ProtectedComponent = () => <div>Protected Content</div>
const PublicComponent = () => <div>Public Content</div>

const renderWithAuth = (component: React.ReactElement) => {
  return render(
    <BrowserRouter>
      <AuthProvider>
        {component}
      </AuthProvider>
    </BrowserRouter>
  )
}

describe('AuthContext', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    localStorage.clear()
  })

  describe('AuthProvider', () => {
    it('provides initial unauthenticated state', () => {
      renderWithAuth(<TestComponent />)

      expect(screen.getByTestId('auth-status')).toHaveTextContent('Not Authenticated')
      expect(screen.getByTestId('loading-status')).toHaveTextContent('Not Loading')
    })

    it('handles successful login', async () => {
      const mockUser = {
        id: '1',
        username: 'testuser',
        email: 'test@example.com',
        role: 'user',
        full_name: 'Test User',
        is_active: true
      }

      vi.mocked(authService.login).mockResolvedValue({
        access_token: 'fake-token',
        user: mockUser
      })

      renderWithAuth(<TestComponent />)

      const loginButton = screen.getByText('Login')
      await userEvent.click(loginButton)

      await waitFor(() => {
        expect(screen.getByTestId('auth-status')).toHaveTextContent('Authenticated')
        expect(screen.getByTestId('user-info')).toHaveTextContent('testuser - user')
      })
    })

    it('handles login failure', async () => {
      vi.mocked(authService.login).mockRejectedValue(new Error('Login failed'))

      renderWithAuth(<TestComponent />)

      const loginButton = screen.getByText('Login')
      await userEvent.click(loginButton)

      await waitFor(() => {
        expect(screen.getByTestId('error-message')).toHaveTextContent('Login failed')
        expect(screen.getByTestId('auth-status')).toHaveTextContent('Not Authenticated')
      })
    })

    it('handles logout', async () => {
      const mockUser = {
        id: '1',
        username: 'testuser',
        email: 'test@example.com',
        role: 'user',
        full_name: 'Test User',
        is_active: true
      }

      vi.mocked(authService.login).mockResolvedValue({
        access_token: 'fake-token',
        user: mockUser
      })

      vi.mocked(authService.logout).mockResolvedValue({})

      renderWithAuth(<TestComponent />)

      // Login first
      const loginButton = screen.getByText('Login')
      await userEvent.click(loginButton)

      await waitFor(() => {
        expect(screen.getByTestId('auth-status')).toHaveTextContent('Authenticated')
      })

      // Then logout
      const logoutButton = screen.getByText('Logout')
      await userEvent.click(logoutButton)

      await waitFor(() => {
        expect(screen.getByTestId('auth-status')).toHaveTextContent('Not Authenticated')
      })
    })
  })

  describe('ProtectedRoute', () => {
    it('renders protected content when authenticated', async () => {
      const mockUser = {
        id: '1',
        username: 'testuser',
        email: 'test@example.com',
        role: 'user',
        full_name: 'Test User',
        is_active: true
      }

      // Mock token validation
      vi.mocked(authService.getToken).mockReturnValue('fake-token')
      vi.mocked(authService.validateToken).mockResolvedValue({ user: mockUser })

      renderWithAuth(
        <ProtectedRoute>
          <ProtectedComponent />
        </ProtectedRoute>
      )

      await waitFor(() => {
        expect(screen.getByText('Protected Content')).toBeInTheDocument()
      })
    })

    it('renders public content when requireAuth is false', () => {
      renderWithAuth(
        <ProtectedRoute requireAuth={false}>
          <PublicComponent />
        </ProtectedRoute>
      )

      expect(screen.getByText('Public Content')).toBeInTheDocument()
    })

    it('redirects unauthenticated users from protected routes', async () => {
      vi.mocked(authService.getToken).mockReturnValue(null)

      renderWithAuth(
        <ProtectedRoute>
          <ProtectedComponent />
        </ProtectedRoute>
      )

      await waitFor(() => {
        expect(mockNavigate).toHaveBeenCalledWith('/')
      })
    })

    it('supports role-based access control', async () => {
      const mockUser = {
        id: '1',
        username: 'user',
        email: 'user@example.com',
        role: 'user',
        full_name: 'Regular User',
        is_active: true
      }

      vi.mocked(authService.getToken).mockReturnValue('fake-token')
      vi.mocked(authService.validateToken).mockResolvedValue({ user: mockUser })

      renderWithAuth(
        <ProtectedRoute requiredRole="admin">
          <ProtectedComponent />
        </ProtectedRoute>
      )

      await waitFor(() => {
        expect(mockNavigate).toHaveBeenCalledWith('/')
      })
    })
  })
})