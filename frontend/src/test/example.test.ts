import { describe, it, expect } from 'vitest'

describe('Example Test Suite', () => {
  it('should pass basic math test', () => {
    expect(2 + 2).toBe(4)
  })

  it('should handle string operations', () => {
    expect('Hello ' + 'World').toBe('Hello World')
  })

  it('should work with arrays', () => {
    const arr = [1, 2, 3]
    expect(arr).toHaveLength(3)
    expect(arr).toContain(2)
  })
})