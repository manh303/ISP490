import unittest
from unittest.mock import MagicMock, AsyncMock
import asyncio
from fastapi import HTTPException

from app.services.admin_service import AdminService
from app.models.admin import UserCreateRequest
    
class TestAdmin(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # Mock database manager with proper query responses
        self.db = MagicMock()
        self.db.execute_query = AsyncMock()
        
        self.conn = MagicMock()
        self.conn.fetchrow = AsyncMock()
        self.conn.fetchval = AsyncMock()
        self.conn.execute = AsyncMock()

        tx_cm = AsyncMock()
        tx_cm.__aenter__.return_value = self.conn
        tx_cm.__aexit__.return_value = False
        self.db.transaction.return_value = tx_cm

        self.service = AdminService(self.db)

        self.service.iam_service = MagicMock()
        self.service.iam_service.hash_password = AsyncMock(return_value="hashed_password_123")

        self.service.activity_logger = MagicMock()
        self.service.activity_logger.log_activity = AsyncMock()

    async def test_create_user(self):
        """Test create user - async method"""
        user_data = UserCreateRequest(
            email="admin12ab123c@example.com",
            password="securepassword123",
            re_enter_password="securepassword123",
            full_name="Admin User",
            phone="0123456789",
            role="ADMIN"
        )
        
        self.db.execute_query.return_value = []
        fake_user_row = {
            "user_id": 1,
            "email": user_data.email,
            "full_name": user_data.full_name,
            "phone": user_data.phone,
            "status": "active",
            "created_at": "2025-11-30 00:00:00",
            "updated_at": "2025-11-30 00:00:00",
        }
        self.conn.fetchrow.return_value = fake_user_row
        self.conn.fetchval.return_value = 10  # Mock role_id

        result = await self.service.create_user(user_data)

         # Đã check email 1 lần
        self.db.execute_query.assert_awaited_once()
        # Có hash password
        self.service.iam_service.hash_password.assert_awaited_once_with(user_data.password)
        # Có insert user
        self.conn.fetchrow.assert_awaited_once()
        # Có lookup role
        self.conn.fetchval.assert_awaited_once()
        # Có insert vào iam_user_role + user_activity_logs
        self.assertGreaterEqual(self.conn.execute.await_count, 1)

        self.assertEqual(result["user_id"], 1)
        self.assertEqual(result["email"], user_data.email)
        self.assertEqual(result["full_name"], user_data.full_name)
        self.assertEqual(result["phone"], user_data.phone)
        self.assertEqual(result["status"], "active")
        self.assertIn("created_at", result)

    async def test_create_user_with_existing_email(self):
        """Test create user with existing email"""
        user_data = UserCreateRequest(
            email="admin12ab123c@example.com",
            password="securepassword123",
            re_enter_password="securepassword123",
            full_name="Admin User",
            phone="0123456789",
            role="ADMIN"
        )
        self.db.execute_query.return_value = [{"user_id": 999}]

        with self.assertRaises(HTTPException) as ctx:
            await self.service.create_user(user_data)

        exc = ctx.exception
        self.assertEqual(exc.status_code, 400)
        self.assertIn("Email already exists", exc.detail)

        # Không được mở transaction, không insert gì nữa
        self.db.transaction.assert_not_called()
        self.service.iam_service.hash_password.assert_not_called()

    async def test_create_user_role_not_found(self):
        """Test create user when role exists in VALID_ROLES but not in database"""
        user_data = UserCreateRequest(
            email="norole@example.com",
            full_name="No Role User",
            password="StrongPass123!",
            re_enter_password="StrongPass123!",
            phone="0123456789",
            role="ADMIN",  # Valid role code, but we'll mock database to return None
        )

        # 1) Email chưa tồn tại
        self.db.execute_query.return_value = []

        # 2) INSERT user OK
        fake_user_row = {
            "user_id": 2,
            "email": user_data.email,
            "full_name": user_data.full_name,
            "phone": user_data.phone,
            "status": "active",
            "created_at": "2025-11-30 00:00:00",
            "updated_at": "2025-11-30 00:00:00",
        }
        self.conn.fetchrow.return_value = fake_user_row

        # 3) Tìm role_id nhưng không có → fetchval trả None
        self.conn.fetchval.return_value = None

        with self.assertRaises(HTTPException) as ctx:
            await self.service.create_user(user_data)

        exc = ctx.exception
        self.assertEqual(exc.status_code, 400)
        self.assertIn("Role", exc.detail)  # "Role 'UNKNOWN_ROLE' not found"

        # Đã insert user nhưng không insert role
        self.conn.fetchrow.assert_awaited_once()
        self.conn.fetchval.assert_awaited_once()
    
    async def test_get_user_by_id(self):
        """Test get user by ID"""
        fake_user = {
            "user_id": 1,
            "email": "admin12ab123c@example.com",
            "full_name": "Admin User",
            "phone": "0123456789",
            "status": "active",
            "mfa_enabled": False,
            "last_login_at": None,
            "created_at": "2025-11-30 00:00:00",
            "updated_at": "2025-11-30 00:00:00",
        }
        # Mock 3 queries: user info, roles, permissions
        self.db.execute_query.side_effect = [
            [fake_user],  # User query
            [],           # Roles query (no roles)
            []            # Permissions query (no permissions)
        ]

        result = await self.service.get_user_by_id(1)
        
        # Should return user with roles and permissions arrays
        self.assertIsNotNone(result)
        self.assertEqual(result["user_id"], 1)
        self.assertEqual(result["email"], "admin12ab123c@example.com")

    async def test_get_user_by_id_not_found(self):
        """Test get user by ID when user does not exist"""
        self.db.execute_query.return_value = []

        result = await self.service.get_user_by_id(9999)
        self.assertIsNone(result)

    async def test_get_list_users(self):
        """Test get list of users with pagination"""
        fake_users = [
            {
                "user_id": 1,
                "email": "admin12ab123c@example.com",
                "full_name": "Admin User",
                "phone": "0123456789",
                "status": "active",
                "mfa_enabled": False,
                "last_login_at": None,
                "created_at": "2025-11-30 00:00:00",
                "updated_at": "2025-11-30 00:00:00",
            },
            {
                "user_id": 2,
                "email": "user12ab123c@example.com",
                "full_name": "User",
                "phone": "0123456789",
                "status": "active",
                "mfa_enabled": False,
                "last_login_at": None,
                "created_at": "2025-11-30 00:00:00",
                "updated_at": "2025-11-30 00:00:00",
            }
        ]
        self.db.execute_query.return_value = fake_users

        result = await self.service.get_users()
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["user_id"], 1)
        self.assertEqual(result[0]["email"], "admin12ab123c@example.com")
        self.assertEqual(result[1]["user_id"], 2)
        self.assertEqual(result[1]["email"], "user12ab123c@example.com")

    async def test_get_list_users_empty(self):
        """Test get list of users when no users exist"""
        self.db.execute_query.return_value = []

        result = await self.service.get_users()
        self.assertEqual(len(result), 0)
if __name__ == '__main__':
    unittest.main()