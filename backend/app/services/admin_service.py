"""
Admin Service - Business Logic
"""
import logging
import bcrypt
from typing import List, Dict, Any, Optional, Tuple
from models.admin import UserCreateRequest, UserUpdateRequest, PasswordChangeRequest, UserPasswordUpdateRequest, UserActionResponse

logger = logging.getLogger(__name__)

class AdminService:
    def __init__(self, db):
        self.db = db

    async def get_users(self, page: int = 1, limit: int = 20, status_filter: str = None) -> Tuple[List[Dict], int]:
        """Get paginated list of users"""
        offset = (page - 1) * limit
        
        # Build query with optional status filter
        where_clause = ""
        if status_filter:
            where_clause = f"WHERE u.status = '{status_filter}'"
        
        query = f"""
        SELECT u.user_id, u.email, u.full_name, u.phone, u.status, 
               u.created_at, u.last_login_at,
               r.role_code as role
        FROM iam_user u
        LEFT JOIN iam_user_role ur ON u.user_id = ur.user_id
        LEFT JOIN iam_role r ON ur.role_id = r.role_id
        {where_clause}
        ORDER BY u.created_at DESC
        LIMIT $1 OFFSET $2
        """
        
        count_query = f"""
        SELECT COUNT(*) as total FROM iam_user u
        {where_clause}
        """
        
        users = await self.db.execute_query(query, (limit, offset))
        count_result = await self.db.execute_query(count_query)
        total = count_result[0]['total'] if count_result else 0
        
        return users, total

    async def get_user_by_id(self, user_id: int) -> Optional[Dict]:
        """Get user by ID"""
        query = """
        SELECT u.user_id, u.email, u.full_name, u.phone, u.status, 
               u.created_at, u.last_login_at,
               r.role_code as role
        FROM iam_user u
        LEFT JOIN iam_user_role ur ON u.user_id = ur.user_id
        LEFT JOIN iam_role r ON ur.role_id = r.role_id
        WHERE u.user_id = $1
        """
        result = await self.db.execute_query(query, (user_id,))
        return result[0] if result else None

    async def create_user(self, user_data: UserCreateRequest) -> int:
        """Create new user"""
        # Check if email already exists
        check_query = "SELECT user_id FROM iam_user WHERE email = $1"
        existing = await self.db.execute_query(check_query, (user_data.email.lower(),))
        
        if existing:
            raise ValueError("Email already exists")
        
        # Hash password
        password_hash = bcrypt.hashpw(user_data.password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        
        # Create user
        insert_query = """
        INSERT INTO iam_user (email, password_hash, full_name, phone, status, created_at, updated_at)
        VALUES ($1, $2, $3, $4, 'active', NOW(), NOW())
        RETURNING user_id
        """
        
        result = await self.db.execute_query(insert_query, (
            user_data.email.lower(),
            password_hash,
            user_data.full_name,
            user_data.phone
        ))
        
        if not result:
            raise Exception("Failed to create user")
        
        user_id = result[0]['user_id']
        
        # Assign role
        await self._assign_role(user_id, user_data.role)
        
        return user_id

    async def update_user(self, user_id: int, user_data: UserUpdateRequest) -> bool:
        """Update user"""
        # Check if user exists
        existing = await self.get_user_by_id(user_id)
        if not existing:
            raise ValueError("User not found")
        
        # Build update query
        update_fields = []
        values = []
        param_count = 1
        
        if user_data.full_name is not None:
            update_fields.append(f"full_name = ${param_count}")
            values.append(user_data.full_name)
            param_count += 1
            
        if user_data.phone is not None:
            update_fields.append(f"phone = ${param_count}")
            values.append(user_data.phone)
            param_count += 1
            
        if user_data.status is not None:
            update_fields.append(f"status = ${param_count}")
            values.append(user_data.status)
            param_count += 1
        
        if update_fields:
            update_fields.append(f"updated_at = NOW()")
            values.append(user_id)
            
            update_query = f"""
            UPDATE iam_user 
            SET {', '.join(update_fields)}
            WHERE user_id = ${param_count}
            """
            
            await self.db.execute_query(update_query, values)
        
        # Update role if provided
        if user_data.role is not None:
            await self._assign_role(user_id, user_data.role)
        
        return True

    async def change_password(self, user_id: int, password_data: PasswordChangeRequest) -> bool:
        """Change user password"""
        # Check if user exists
        existing = await self.get_user_by_id(user_id)
        if not existing:
            raise ValueError("User not found")
        
        # Hash new password
        password_hash = bcrypt.hashpw(password_data.new_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        
        # Update password
        update_query = """
        UPDATE iam_user 
        SET password_hash = $1, updated_at = NOW()
        WHERE user_id = $2
        """
        
        await self.db.execute_query(update_query, (password_hash, user_id))
        return True

    async def delete_user(self, user_id: int) -> str:
        """Delete user"""
        user = await self.get_user_by_id(user_id)
        if not user:
            raise ValueError("User not found")
        
        # Delete user roles first
        await self.db.execute_query("DELETE FROM iam_user_role WHERE user_id = $1", (user_id,))
        
        # Delete user
        await self.db.execute_query("DELETE FROM iam_user WHERE user_id = $1", (user_id,))
        
        return user['email']

    async def get_activity_logs(self, page: int = 1, limit: int = 20, user_id: int = None) -> Tuple[List[Dict], int]:
        """Get activity logs"""
        offset = (page - 1) * limit
        
        where_clause = ""
        params = [limit, offset]
        
        if user_id:
            where_clause = "WHERE user_id = $3"
            params.append(user_id)
        
        query = f"""
        SELECT log_id, user_id, email, action, resource, details, 
               ip_address, status, created_at
        FROM user_activity_logs
        {where_clause}
        ORDER BY created_at DESC
        LIMIT $1 OFFSET $2
        """
        
        count_query = f"""
        SELECT COUNT(*) as total FROM user_activity_logs
        {where_clause.replace('$3', '$1') if where_clause else ''}
        """
        
        logs = await self.db.execute_query(query, params)
        
        count_params = [user_id] if user_id else []
        count_result = await self.db.execute_query(count_query, count_params)
        total = count_result[0]['total'] if count_result else 0
        
        return logs, total

    async def get_activity_stats(self) -> Dict:
        """Get activity statistics"""
        stats_query = """
        SELECT 
            COUNT(*) as total_activities,
            COUNT(CASE WHEN status = 'success' THEN 1 END) as successful_activities,
            COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_activities,
            COUNT(DISTINCT user_id) as unique_users
        FROM user_activity_logs
        WHERE created_at >= NOW() - INTERVAL '30 days'
        """
        
        top_actions_query = """
        SELECT action, COUNT(*) as count
        FROM user_activity_logs
        WHERE created_at >= NOW() - INTERVAL '7 days'
        GROUP BY action
        ORDER BY count DESC
        LIMIT 5
        """
        
        recent_query = """
        SELECT log_id, user_id, email, action, resource, details, 
               ip_address, status, created_at
        FROM user_activity_logs
        ORDER BY created_at DESC
        LIMIT 10
        """
        
        stats = await self.db.execute_query(stats_query)
        top_actions = await self.db.execute_query(top_actions_query)
        recent = await self.db.execute_query(recent_query)
        
        return {
            **(stats[0] if stats else {}),
            "top_actions": top_actions,
            "recent_activities": recent
        }

    async def _assign_role(self, user_id: int, role_code: str) -> None:
        """Assign role to user"""
        # Get role ID
        role_query = "SELECT role_id FROM iam_role WHERE role_code = $1"
        role_result = await self.db.execute_query(role_query, (role_code,))
        
        if not role_result:
            raise ValueError(f"Role '{role_code}' not found")
        
        role_id = role_result[0]['role_id']
        
        # Remove existing roles
        await self.db.execute_query("DELETE FROM iam_user_role WHERE user_id = $1", (user_id,))
        
        # Assign new role
        assign_query = """
        INSERT INTO iam_user_role (user_id, role_id, assigned_at)
        VALUES ($1, $2, NOW())
        """
        await self.db.execute_query(assign_query, (user_id, role_id))
