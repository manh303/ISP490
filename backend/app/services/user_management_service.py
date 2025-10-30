"""
User Management Service
Business logic for admin user management
"""
import bcrypt
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class UserManagementService:
    def __init__(self, db_manager):
        self.db = db_manager

    async def get_users(self, status: str = 'active', page: int = 1, limit: int = 20) -> Dict[str, Any]:
        """Get users by status with pagination"""
        offset = (page - 1) * limit
        
        query = """
        SELECT u.user_id, u.email, u.full_name, u.phone, u.status,
               u.last_login_at, u.created_at, u.updated_at,
               r.role_code, r.role_name
        FROM iam_user u
        LEFT JOIN iam_user_role ur ON u.user_id = ur.user_id
        LEFT JOIN iam_role r ON ur.role_id = r.role_id
        WHERE u.status = $1
        ORDER BY u.created_at DESC
        LIMIT $2 OFFSET $3
        """
        
        users = await self.db.execute_query(query, (status, limit, offset))
        
        # Count total
        count_query = "SELECT COUNT(*) as total FROM iam_user WHERE status = $1"
        count_result = await self.db.execute_query(count_query, (status,))
        total = count_result[0]['total'] if count_result else 0
        
        return {
            'users': users,
            'total': total,
            'page': page,
            'limit': limit
        }

    async def get_user_by_id(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Get user by ID"""
        query = """
        SELECT u.user_id, u.email, u.full_name, u.phone, u.status,
               u.last_login_at, u.created_at, u.updated_at,
               r.role_code, r.role_name
        FROM iam_user u
        LEFT JOIN iam_user_role ur ON u.user_id = ur.user_id
        LEFT JOIN iam_role r ON ur.role_id = r.role_id
        WHERE u.user_id = $1
        """
        
        result = await self.db.execute_query(query, (user_id,))
        return result[0] if result else None

    async def create_user(self, email: str, password: str, full_name: str, 
                         phone: Optional[str] = None, role_code: str = 'CUSTOMER') -> Dict[str, Any]:
        """Create new user"""
        # Check if email exists
        existing = await self.db.execute_query(
            "SELECT user_id FROM iam_user WHERE email = $1", (email,)
        )
        if existing:
            raise ValueError("Email already exists")

        # Hash password
        password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        
        # Create user
        user_query = """
        INSERT INTO iam_user (email, password_hash, full_name, phone, status, created_at, updated_at)
        VALUES ($1, $2, $3, $4, 'active', NOW(), NOW())
        RETURNING user_id, email, full_name, phone, status, created_at, updated_at
        """
        
        user_result = await self.db.execute_query(
            user_query, (email, password_hash, full_name, phone)
        )
        
        if not user_result:
            raise Exception("Failed to create user")
            
        user = user_result[0]
        
        # Assign role
        await self._assign_role(user['user_id'], role_code)
        
        return user

    async def update_user(self, user_id: int, full_name: Optional[str] = None, 
                         phone: Optional[str] = None, role_code: Optional[str] = None) -> Dict[str, Any]:
        """Update user information"""
        # Update user basic info
        if full_name is not None or phone is not None:
            update_query = """
            UPDATE iam_user 
            SET full_name = COALESCE($1, full_name),
                phone = COALESCE($2, phone),
                updated_at = NOW()
            WHERE user_id = $3
            """
            await self.db.execute_query(update_query, (full_name, phone, user_id))
        
        # Update role if provided
        if role_code:
            await self._update_user_role(user_id, role_code)
        
        return await self.get_user_by_id(user_id)

    async def update_password(self, user_id: int, new_password: str) -> bool:
        """Update user password"""
        password_hash = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        
        query = """
        UPDATE iam_user 
        SET password_hash = $1, updated_at = NOW()
        WHERE user_id = $2
        """
        
        await self.db.execute_query(query, (password_hash, user_id))
        return True

    async def soft_delete_user(self, user_id: int) -> bool:
        """Soft delete user (set status to disabled)"""
        query = """
        UPDATE iam_user 
        SET status = 'disabled', updated_at = NOW()
        WHERE user_id = $1 AND status = 'active'
        """
        
        await self.db.execute_query(query, (user_id,))
        return True

    async def restore_user(self, user_id: int) -> bool:
        """Restore user (set status back to active)"""
        query = """
        UPDATE iam_user 
        SET status = 'active', updated_at = NOW()
        WHERE user_id = $1 AND status = 'disabled'
        """
        
        await self.db.execute_query(query, (user_id,))
        return True

    async def permanent_delete_user(self, user_id: int) -> bool:
        """Permanently delete user from database"""
        # Delete user roles first
        await self.db.execute_query("DELETE FROM iam_user_role WHERE user_id = $1", (user_id,))
        
        # Delete user sessions
        await self.db.execute_query("DELETE FROM iam_user_session WHERE user_id = $1", (user_id,))
        
        # Delete user
        await self.db.execute_query("DELETE FROM iam_user WHERE user_id = $1", (user_id,))
        
        return True

    async def _assign_role(self, user_id: int, role_code: str):
        """Assign role to user"""
        # Get role_id
        role_query = "SELECT role_id FROM iam_role WHERE role_code = $1"
        role_result = await self.db.execute_query(role_query, (role_code,))
        
        if not role_result:
            # Create role if not exists
            create_role_query = """
            INSERT INTO iam_role (role_code, role_name, description)
            VALUES ($1, $2, $3)
            RETURNING role_id
            """
            role_result = await self.db.execute_query(
                create_role_query, (role_code, role_code.title(), f"{role_code} role")
            )
        
        role_id = role_result[0]['role_id']
        
        # Assign role
        assign_query = """
        INSERT INTO iam_user_role (user_id, role_id, assigned_at)
        VALUES ($1, $2, NOW())
        ON CONFLICT (user_id, role_id) DO NOTHING
        """
        await self.db.execute_query(assign_query, (user_id, role_id))

    async def _update_user_role(self, user_id: int, role_code: str):
        """Update user role"""
        # Remove existing roles
        await self.db.execute_query("DELETE FROM iam_user_role WHERE user_id = $1", (user_id,))
        
        # Assign new role
        await self._assign_role(user_id, role_code)