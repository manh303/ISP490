import unittest
from unittest.mock import AsyncMock, MagicMock
from fastapi import HTTPException
from app.services.iam_service import IAMService
from app.api.v1.auth import LoginRequest, signin

class TestAuthService(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        """Setup mock IAM service"""
        self.db = MagicMock()
        self.iam_service = IAMService(self.db)
        
        # Mock all methods we need
        self.iam_service.authenticate_user = AsyncMock()
        self.iam_service.create_access_token = AsyncMock()
        self.iam_service.create_refresh_token = AsyncMock()
        self.iam_service.log_user_action = AsyncMock()
        
    async def test_signin_success_returns_token_and_user_info(self):
        """Test successful signin returns token and user info"""
        # Mock user data with role
        fake_user = {
            'user_id': 1,
            'email': 'admin@example.com',
            'full_name': 'Admin User',
            'phone': '0123456789',
            'status': 'active',
            'mfa_enabled': False,
            'last_login_at': None,
            'created_at': '2025-11-30 00:00:00',
            'updated_at': '2025-11-30 00:00:00',
            'roles': [{'role_code': 'ADMIN', 'role_name': 'Administrator'}],
            'permissions': []
        }
        
        # Mock authenticate_user to return user with valid password
        self.iam_service.authenticate_user.return_value = fake_user
        
        # Mock token creation
        self.iam_service.create_access_token.return_value = "fake_access_token_123"
        self.iam_service.create_refresh_token.return_value = "fake_refresh_token_456"
      
        # Create login request
        login_request = LoginRequest(email="admin@example.com", password="password123")
        
        # Call signin
        response = await signin(request=login_request, iam=self.iam_service)
        
        # Assertions
        self.assertTrue(response['success'])
        self.assertEqual(response['message'], 'Login successful')
        
        # Check user info in response
        user_data = response['data']['user']
        self.assertEqual(user_data['email'], 'admin@example.com')
        self.assertEqual(user_data['user_id'], 1)
        self.assertEqual(user_data['full_name'], 'Admin User')
        self.assertIn('roles', user_data)
        
        # Check tokens in response
        tokens = response['data']['tokens']
        self.assertEqual(tokens['access_token'], 'fake_access_token_123')
        self.assertEqual(tokens['refresh_token'], 'fake_refresh_token_456')
        self.assertEqual(tokens['token_type'], 'bearer')
        
        # Verify methods were called
        self.iam_service.authenticate_user.assert_awaited_once_with('admin@example.com', 'password123')
        self.iam_service.create_access_token.assert_awaited_once_with(fake_user)
        self.iam_service.create_refresh_token.assert_awaited_once_with(1)
        self.iam_service.log_user_action.assert_awaited_once()
    
    async def test_signin_wrong_password_raises_401(self):
        """Test signin with wrong password raises HTTPException 401"""
        # Mock authenticate_user to return None (invalid credentials = verify_password failed)
        self.iam_service.authenticate_user.return_value = None
        
        # Create login request with wrong password
        login_request = LoginRequest(email="admin@example.com", password="wrong_password")
        
        # Call signin and expect HTTPException
        with self.assertRaises(HTTPException) as ctx:
            await signin(request=login_request, iam=self.iam_service)
        
        # Verify exception details
        exc = ctx.exception
        self.assertEqual(exc.status_code, 401)
        self.assertIn("Invalid email or password", str(exc.detail))
        
        # Verify authenticate was called but no tokens created
        self.iam_service.authenticate_user.assert_awaited_once_with("admin@example.com", "wrong_password")
        self.iam_service.create_access_token.assert_not_awaited()
        self.iam_service.create_refresh_token.assert_not_awaited()
        self.iam_service.log_user_action.assert_not_awaited()
    
    async def test_signin_user_not_found_raises_401(self):
        """Test signin with non-existent user raises HTTPException 401"""
        # Mock authenticate_user to return None (user not found)
        self.iam_service.authenticate_user.return_value = None
        
        # Create login request with non-existent email
        login_request = LoginRequest(email="Oqy0X@example.com", password="password123")
        
        # Call signin and expect HTTPException
        with self.assertRaises(HTTPException) as ctx:
            await signin(request=login_request, iam=self.iam_service)
        
        # Verify exception details
        exc = ctx.exception
        self.assertEqual(exc.status_code, 401)
        self.assertIn("Invalid email or password", str(exc.detail))
        
        # Verify authenticate was called but no tokens created
        self.iam_service.authenticate_user.assert_awaited_once_with("Oqy0X@example.com", "password123")
        self.iam_service.create_access_token.assert_not_awaited()
        self.iam_service.create_refresh_token.assert_not_awaited()
        self.iam_service.log_user_action.assert_not_awaited()
    
    async def test_signin_inactive_user_raises_401(self):
        """Test signin with inactive user raises HTTPException 401"""
        self.iam_service.authenticate_user.return_value = None
        
        # Create login request for inactive user
        login_request = LoginRequest(email="inactive@example.com", password="password123")
        
        # Call signin and expect HTTPException
        with self.assertRaises(HTTPException) as ctx:
            await signin(request=login_request, iam=self.iam_service)
        
        # Verify exception details
        exc = ctx.exception
        self.assertEqual(exc.status_code, 401)
        self.assertIn("Invalid email or password", str(exc.detail))
        
        # Verify authenticate was called but no tokens created
        self.iam_service.authenticate_user.assert_awaited_once_with("inactive@example.com", "password123")
        self.iam_service.create_access_token.assert_not_awaited()
        self.iam_service.create_refresh_token.assert_not_awaited()
        self.iam_service.log_user_action.assert_not_awaited()
    
    async def test_verify_password_success(self):
        """Test verify password with correct password"""
        # Mock bcrypt.checkpw to return True
        import bcrypt
        original_checkpw = bcrypt.checkpw
        bcrypt.checkpw = lambda pwd, hashed: True
        
        try:
            result = await self.iam_service.verify_password("correct_password", "hashed_password_123")
            self.assertTrue(result)
        finally:
            # Restore original function
            bcrypt.checkpw = original_checkpw
    
    async def test_verify_password_wrong(self):
        """Test verify password with wrong password"""
        # Mock bcrypt.checkpw to return False
        import bcrypt
        original_checkpw = bcrypt.checkpw
        bcrypt.checkpw = lambda pwd, hashed: False
        
        try:
            result = await self.iam_service.verify_password("wrong_password", "hashed_password_123")
            self.assertFalse(result)
        finally:
            # Restore original function
            bcrypt.checkpw = original_checkpw

if __name__ == '__main__':
    unittest.main()

