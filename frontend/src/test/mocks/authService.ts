import { vi } from 'vitest'

export const mockAuthService = {
  login: vi.fn(),
  logout: vi.fn(),
  validateToken: vi.fn(),
  getTestCredentials: vi.fn(),
  getToken: vi.fn(),
  getStoredUser: vi.fn(),
}

export default mockAuthService