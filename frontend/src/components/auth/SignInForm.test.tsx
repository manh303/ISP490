import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { BrowserRouter } from 'react-router'
import SignInForm from './SignInForm'
import { AuthProvider } from '../../contexts/AuthContext'
import authService from '../../services/authService'

// Mock authService
vi.mock('../../services/authService', () => ({
  default: {
    getTestCredentials: vi.fn(),
    login: vi.fn(),
  },
}))

// Mock useNavigate
const mockNavigate = vi.fn()
vi.mock('react-router', async () => {
  const actual = await vi.importActual('react-router')
  return {
    ...actual,
    useNavigate: () => mockNavigate,
  }
})

const renderWithProviders = (component: React.ReactElement) => {
  return render(
    <BrowserRouter>
      <AuthProvider>
        {component}
      </AuthProvider>
    </BrowserRouter>
  )
}

describe('SignInForm', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    // Mock test credentials
    vi.mocked(authService.getTestCredentials).mockResolvedValue([
      {
        username: 'admin',
        password: 'admin123',
        role: 'admin',
        description: 'System Administrator (admin)'
      },
      {
        username: 'user1',
        password: 'user123',
        role: 'user',
        description: 'Test User One (user)'
      }
    ])
  })

  it('renders login form correctly', () => {
    renderWithProviders(<SignInForm />)

    expect(screen.getByText('Welcome Back')).toBeInTheDocument()
    expect(screen.getByLabelText(/username/i)).toBeInTheDocument()
    expect(screen.getByLabelText(/password/i)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /login/i })).toBeInTheDocument()
  })

  it('shows test credentials when button is clicked', async () => {
    renderWithProviders(<SignInForm />)

    const testCredentialsButton = screen.getByText(/test credentials/i)
    await userEvent.click(testCredentialsButton)

    await waitFor(() => {
      expect(screen.getByText('admin')).toBeInTheDocument()
      expect(screen.getByText('user1')).toBeInTheDocument()
    })
  })

  it('fills form when test credential is clicked', async () => {
    renderWithProviders(<SignInForm />)

    // Open test credentials
    const testCredentialsButton = screen.getByText(/test credentials/i)
    await userEvent.click(testCredentialsButton)

    // Click on admin credential
    await waitFor(() => {
      const adminCredential = screen.getByText('admin')
      userEvent.click(adminCredential)
    })

    // Check if form is filled
    const usernameInput = screen.getByLabelText(/username/i) as HTMLInputElement
    const passwordInput = screen.getByLabelText(/password/i) as HTMLInputElement

    expect(usernameInput.value).toBe('admin')
    expect(passwordInput.value).toBe('admin123')
  })

  it('shows validation errors for empty fields', async () => {
    renderWithProviders(<SignInForm />)

    const loginButton = screen.getByRole('button', { name: /login/i })
    await userEvent.click(loginButton)

    expect(screen.getByText('Username is required')).toBeInTheDocument()
    expect(screen.getByText('Password is required')).toBeInTheDocument()
  })

  it('handles login submission with valid credentials', async () => {
    const mockLogin = vi.fn().mockResolvedValue({})
    vi.mocked(authService.login).mockImplementation(mockLogin)

    renderWithProviders(<SignInForm />)

    const usernameInput = screen.getByLabelText(/username/i)
    const passwordInput = screen.getByLabelText(/password/i)
    const loginButton = screen.getByRole('button', { name: /login/i })

    await userEvent.type(usernameInput, 'admin')
    await userEvent.type(passwordInput, 'admin123')
    await userEvent.click(loginButton)

    expect(mockLogin).toHaveBeenCalledWith({
      username: 'admin',
      password: 'admin123',
      remember_me: false
    })
  })

  it('shows error message on login failure', async () => {
    const mockLogin = vi.fn().mockRejectedValue(new Error('Invalid credentials'))
    vi.mocked(authService.login).mockImplementation(mockLogin)

    renderWithProviders(<SignInForm />)

    const usernameInput = screen.getByLabelText(/username/i)
    const passwordInput = screen.getByLabelText(/password/i)
    const loginButton = screen.getByRole('button', { name: /login/i })

    await userEvent.type(usernameInput, 'wrong')
    await userEvent.type(passwordInput, 'wrong')
    await userEvent.click(loginButton)

    await waitFor(() => {
      expect(screen.getByText('Invalid credentials')).toBeInTheDocument()
    })
  })

  it('toggles password visibility', async () => {
    renderWithProviders(<SignInForm />)

    const passwordInput = screen.getByLabelText(/password/i) as HTMLInputElement
    const toggleButton = screen.getByRole('button', { name: /toggle password/i })

    expect(passwordInput.type).toBe('password')

    await userEvent.click(toggleButton)
    expect(passwordInput.type).toBe('text')

    await userEvent.click(toggleButton)
    expect(passwordInput.type).toBe('password')
  })

  it('handles remember me checkbox', async () => {
    renderWithProviders(<SignInForm />)

    const rememberMeCheckbox = screen.getByLabelText(/remember me/i) as HTMLInputElement

    expect(rememberMeCheckbox.checked).toBe(false)

    await userEvent.click(rememberMeCheckbox)
    expect(rememberMeCheckbox.checked).toBe(true)
  })
})