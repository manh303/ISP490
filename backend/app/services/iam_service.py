"""
IAM (Identity & Access Management) Service
Handles authentication, authorization, and user management using PostgreSQL IAM schema
"""

import hashlib
import secrets
import datetime
from typing import Optional, Dict, List, Any
import bcrypt
# JWT removed
from fastapi import HTTPException
import logging

logger = logging.getLogger(__name__)

class IAMService:
    def __init__(self, db_manager, secret_key: str = "sY-A335Mj9qloyUE94maevhmrg25MZ3RxbVhBYAhmu5QnIS1qsCKIiiGjRshkZA4OSwZN2k2O5VSzDn3XdZo5A"):
        self.db = db_manager
        self.secret_key = secret_key
        self.jwt_algorithm = "HS256"
        self.token_expire_hours = 24

    async def hash_password(self, password: str) -> str:
        """Hash password using bcrypt"""
        salt = bcrypt.gensalt()
        hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
        return hashed.decode('utf-8')

    async def verify_password(self, password: str, hashed: str) -> bool:
        """Verify password against hash"""
        try:
            return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))
        except Exception:
            return False

    # create_user() removed - use AdminService.create_user() instead


    async def authenticate_user(self, email: str, password: str) -> Optional[Dict[str, Any]]:
        """Authenticate user with email and password"""
        try:
            # Get user by email
            user = await self.get_user_by_email(email)
            if not user:
                return None

            # Check if user is active
            if user['status'] != 'active':
                return None

            # Verify password
            if not await self.verify_password(password, user['password_hash']):
                return None

            # Update last login
            await self.update_last_login(user['user_id'])

            # Get full user data with roles and permissions from AdminService
            from app.services.admin_service import AdminService
            admin_service = AdminService(self.db)
            full_user = await admin_service.get_user_by_id(user['user_id'])
            return full_user

        except Exception as e:
            logger.error(f"Authenticate user error: {e}")
            return None

    async def update_last_login(self, user_id: int):
        """Update user's last login timestamp
        Note: AdminService also has this method - use either one
        """
        try:
            query = "UPDATE iam_user SET last_login_at = $1, updated_at = $2 WHERE user_id = $3"
            await self.db.execute_query(query, (datetime.datetime.utcnow(), datetime.datetime.utcnow(), user_id))
        except Exception as e:
            logger.error(f"Update last login error: {e}")


    async def check_permission(self, user_id: int, permission_code: str) -> bool:
        """Check if user has specific permission"""
        try:
            query = """
                SELECT 1 FROM iam_permission p
                JOIN iam_role_permission rp ON p.perm_id = rp.perm_id
                JOIN iam_user_role ur ON rp.role_id = ur.role_id
                WHERE ur.user_id = $1 AND p.perm_code = $2
            """
            result = await self.db.execute_query(query, (user_id, permission_code))

            return bool(result)

        except Exception as e:
            logger.error(f"Check permission error: {e}")
            return False

    # update_user_profile() removed - use AdminService.update_user() instead

    async def change_password(self, user_id: int, current_password: str, new_password: str):
        """Change user password"""
        try:
            # Get current user
            user = await self.get_user_by_email_by_id(user_id)
            if not user:
                raise HTTPException(status_code=404, detail="User not found")

            # Verify current password
            if not await self.verify_password(current_password, user['password_hash']):
                raise HTTPException(status_code=400, detail="Current password is incorrect")

            # Hash new password
            new_password_hash = await self.hash_password(new_password)

            # Update password
            query = """
                UPDATE iam_user
                SET password_hash = $1, updated_at = $2
                WHERE user_id = $3
            """
            await self.db.execute_query(query, (new_password_hash, datetime.datetime.utcnow(), user_id))

        except Exception as e:
            logger.error(f"Change password error: {e}")
            raise HTTPException(status_code=500, detail=str(e))

   

    # Password reset token functionality removed

    async def log_user_action(self, user_id: int, action: str, target_type: str = None, target_id: str = None, details: str = None):
        """Log user action for audit"""
        try:
            query = """
                INSERT INTO iam_audit_log (user_id, action, target_type, target_id, details_text, created_at)
                VALUES ($1, $2, $3, $4, $5, $6)
            """
            await self.db.execute_query(query, (user_id, action, target_type, target_id, details, datetime.datetime.utcnow()))
        except Exception as e:
            logger.error(f"Log user action error: {e}")