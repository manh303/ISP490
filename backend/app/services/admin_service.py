"""
Admin Service - Business Logic Layer
Handles user management, activity logging, and admin operations
"""
import logging
import datetime
from typing import List, Dict, Any, Optional, Tuple
from fastapi import HTTPException

from models.admin import (
    UserCreateRequest, 
    UserUpdateRequest, 
    PasswordChangeRequest, 
    UserActionResponse
)
from app.services.activity_logger import ActivityLogger
from app.services.iam_service import IAMService

logger = logging.getLogger(__name__)


class AdminService:
    """Service layer for admin operations"""
    
    def __init__(self, db):
        self.db = db
        self.iam_service = IAMService(db)
        self.activity_logger = ActivityLogger(db)
       

    # ==================== USER QUERIES ====================

    async def get_users(self, status_filter: Optional[str] = None) -> List[Dict]:
        """Get all users with optional status filter"""
        where_clause = "WHERE status = $1" if status_filter else ""
        params = [status_filter] if status_filter else []
        
        query = f"""
            SELECT 
                u.user_id, u.email, u.full_name, u.phone, u.status, 
                u.created_at, u.updated_at, u.last_login_at,
                r.role_code, r.role_name
            FROM iam.iam_user u
            LEFT JOIN iam.iam_user_role ur ON u.user_id = ur.user_id
            LEFT JOIN iam.iam_role r ON ur.role_id = r.role_id
            {where_clause}
            ORDER BY u.created_at DESC
        """
        
        users = await self.db.execute_query(query, tuple(params) if params else None)
        logger.info(f"✅ Retrieved {len(users)} users")
        return users

    async def get_user_by_id(self, user_id: int) -> Optional[Dict]:
        """Get user by ID with roles and permissions"""
        try:
            # Get user basic info
            user_query = "SELECT * FROM iam.iam_user WHERE user_id = $1"
            user_result = await self.db.execute_query(user_query, (user_id,))

            if not user_result:
                return None

            user = user_result[0]

<<<<<<< HEAD
            # Get all user roles (for detailed roles array)
=======
            # Get roles
>>>>>>> f990657e46599176c49ea15f7f4ba09c3ad15e5a
            roles_query = """
                SELECT r.role_id, r.role_code, r.role_name, r.description
                FROM iam.iam_role r
                JOIN iam.iam_user_role ur ON r.role_id = ur.role_id
                WHERE ur.user_id = $1
<<<<<<< HEAD
                ORDER BY r.role_code
            """
            roles_result = await self.db.execute_query(roles_query, (user_id,))

            # Get user permissions
=======
            """
            roles_result = await self.db.execute_query(roles_query, (user_id,))

            # Get permissions
>>>>>>> f990657e46599176c49ea15f7f4ba09c3ad15e5a
            permissions_query = """
                SELECT DISTINCT p.perm_id, p.perm_code, p.perm_name, p.module, p.action, p.description
                FROM iam.iam_permission p
                JOIN iam.iam_role_permission rp ON p.perm_id = rp.perm_id
                JOIN iam.iam_user_role ur ON rp.role_id = ur.role_id
                WHERE ur.user_id = $1
<<<<<<< HEAD
                ORDER BY p.perm_code
            """
            permissions_result = await self.db.execute_query(permissions_query, (user_id,))

=======
            """
            permissions_result = await self.db.execute_query(permissions_query, (user_id,))

            primary_role = roles_result[0] if roles_result else None

>>>>>>> f990657e46599176c49ea15f7f4ba09c3ad15e5a
            return {
                'user_id': user['user_id'],
                'email': user['email'],
                'full_name': user['full_name'],
                'phone': user['phone'],
                'status': user['status'],
<<<<<<< HEAD
                'mfa_enabled': user.get('mfa_enabled', False),
                'last_login_at': user['last_login_at'],
                'created_at': user['created_at'],
                'updated_at': user['updated_at'],
                'role_code': user['role_code'],  # Primary role code (top level)
                'role_name': user['role_name'],  # Primary role name (top level)
                'roles': [
                    {
                        'role_id': role['role_id'],
                        'role_code': role['role_code'],
                        'role_name': role['role_name'],
                        'description': role['description']
                    }
                    for role in roles_result
                ],
                'permissions': [
                    {
                        'perm_id': perm['perm_id'],
                        'perm_code': perm['perm_code'],
                        'perm_name': perm['perm_name'],
                        'module': perm['module'],
                        'action': perm['action'],
                        'description': perm['description']
                    }
                    for perm in permissions_result
=======
                'mfa_enabled': user['mfa_enabled'],
                'last_login_at': user['last_login_at'],
                'created_at': user['created_at'],
                'updated_at': user['updated_at'],
                'role_code': primary_role['role_code'] if primary_role else None,
                'role_name': primary_role['role_name'] if primary_role else None,
                'roles': [
                    {
                        'role_id': r['role_id'],
                        'role_code': r['role_code'],
                        'role_name': r['role_name'],
                        'description': r['description']
                    }
                    for r in roles_result
                ],
                'permissions': [
                    {
                        'perm_id': p['perm_id'],
                        'perm_code': p['perm_code'],
                        'perm_name': p['perm_name'],
                        'module': p['module'],
                        'action': p['action'],
                        'description': p['description']
                    }
                    for p in permissions_result
>>>>>>> f990657e46599176c49ea15f7f4ba09c3ad15e5a
                ]
            }

        except Exception as e:
            logger.error(f"Get user by ID error: {e}")
            return None

<<<<<<< HEAD
    async def create_user(self, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create new user (Admin function)"""
        try:
            from app.services.iam_service import IAMService
            iam_service = IAMService(self.db)
            
            # Check if email exists
            check_query = "SELECT user_id FROM iam.iam_user WHERE email = $1"
            existing = await self.db.execute_query(check_query, (user_data['email'],))
            if existing:
                raise HTTPException(status_code=400, detail="Email already exists")
            
            # Hash password using IAMService
            password_hash = await iam_service.hash_password(user_data['password'])
            
            # Insert user
            query = """
                INSERT INTO iam.iam_user (email, password_hash, full_name, phone, status, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                RETURNING user_id, email, full_name, phone, status, created_at
            """
            now = datetime.datetime.utcnow()
            result = await self.db.execute_query(query, (
                user_data['email'],
                password_hash,
                user_data.get('full_name', ''),
                user_data.get('phone', ''),
                user_data.get('status', 'active'),
                now,
                now
            ))
            
            if result:
                user = result[0]
                # Assign default role if specified
                if 'role_code' in user_data:
                    await self._assign_role(user['user_id'], user_data['role_code'])
                
                await self._log_activity(user['user_id'], "USER_CREATED", f"Admin created user: {user['email']}")
                return user
            
            raise HTTPException(status_code=500, detail="Failed to create user")
=======
    # ==================== USER MUTATIONS ====================

    async def create_user(self, user_data: UserCreateRequest) -> Dict[str, Any]:
        """Create new user with role assignment"""
        email = user_data.email
        password = user_data.password
        full_name = user_data.full_name or ""
        phone = user_data.phone or ""
        status = "active"
        role_code = user_data.role

        # Check email exists
        check_query = "SELECT user_id FROM iam.iam_user WHERE email = $1"
        existing_user = await self.db.execute_query(check_query, (email,))
        if existing_user:
            raise HTTPException(status_code=400, detail="Email already exists")

        # Hash password
        password_hash = await self.iam_service.hash_password(password)

        try:
            async with self.db.transaction() as conn:
                # Insert user
                insert_query = """
                    INSERT INTO iam.iam_user (
                        email, password_hash, full_name, phone, status, created_at, updated_at
                    )
                    VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
                    RETURNING user_id, email, full_name, phone, status, created_at, updated_at
                """
                user_row = await conn.fetchrow(
                    insert_query, email, password_hash, full_name, phone, status
                )

                if not user_row:
                    raise HTTPException(status_code=500, detail="Failed to create user")

                user_id = user_row["user_id"]

                # Assign role
                if role_code:
                    role_id = await conn.fetchval(
                        "SELECT role_id FROM iam.iam_role WHERE role_code = $1", role_code
                    )
                    if not role_id:
                        raise HTTPException(status_code=400, detail=f"Role '{role_code}' not found")

                    await conn.execute(
                        "INSERT INTO iam.iam_user_role (user_id, role_id, assigned_at) VALUES ($1, $2, NOW())",
                        user_id, role_id
                    )
                    logger.info(f"✅ Role {role_code} assigned to user {user_id}")

                # Log activity
                await conn.execute(
                    "INSERT INTO iam.user_activity_logs (user_id, action, details, created_at) VALUES ($1, $2, $3, NOW())",
                    user_id, "USER_CREATED", f"Admin created user: {email} with role: {role_code}"
                )

                result = {
                    "user_id": user_row["user_id"],
                    "email": user_row["email"],
                    "full_name": user_row["full_name"],
                    "phone": user_row["phone"],
                    "status": user_row["status"],
                    "created_at": user_row["created_at"],
                }

            logger.info(f"✅ User created: {email} (ID: {user_id})")
            return result

>>>>>>> f990657e46599176c49ea15f7f4ba09c3ad15e5a
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Create user error: {e}")
<<<<<<< HEAD
            raise HTTPException(status_code=500, detail=str(e))

    async def update_user(self, user_id: int, user_data: UserUpdateRequest) -> bool:
        """Update user"""
        # Check if user exists
=======
            raise HTTPException(status_code=500, detail="Internal server error")

    async def update_user(self, user_id: int, user_data: UserUpdateRequest) -> bool:
        """Update user information"""
>>>>>>> f990657e46599176c49ea15f7f4ba09c3ad15e5a
        existing = await self.get_user_by_id(user_id)
        if not existing:
            raise ValueError("User not found")
        
<<<<<<< HEAD
        # Build update query
=======
>>>>>>> f990657e46599176c49ea15f7f4ba09c3ad15e5a
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
<<<<<<< HEAD
            update_fields.append(f"updated_at = NOW()")
            values.append(user_id)
            
            update_query = f"""
            UPDATE iam.iam_user 
            SET {', '.join(update_fields)}
            WHERE user_id = ${param_count}
            """
            
            await self.db.execute_query(update_query, values)
=======
            update_fields.append("updated_at = NOW()")
            values.append(user_id)
            
            query = f"UPDATE iam.iam_user SET {', '.join(update_fields)} WHERE user_id = ${param_count}"
            await self.db.execute_query(query, values)
>>>>>>> f990657e46599176c49ea15f7f4ba09c3ad15e5a
        
        # Update role if provided
        if user_data.role is not None:
            await self._assign_role(user_id, user_data.role)
        
        return True

<<<<<<< HEAD
    async def change_password(self, user_id: int, new_password: str) -> bool:
        """Change user password (admin function - no current password verification)
        Note: For user self-service password change, use IAMService.change_password() which requires current password
        """
        try:
            from app.services.iam_service import IAMService
            iam_service = IAMService(self.db)
            
            password_hash = await iam_service.hash_password(new_password)
=======
    async def change_password(self, user_id: int, new_password: PasswordChangeRequest) -> bool:
        """Change user password (admin function - no verification required)"""
        try:
            password_hash = await self.iam_service.hash_password(new_password.new_password)
>>>>>>> f990657e46599176c49ea15f7f4ba09c3ad15e5a
            
            query = """
                UPDATE iam.iam_user 
                SET password_hash = $1, updated_at = $2 
                WHERE user_id = $3
                RETURNING user_id
            """
<<<<<<< HEAD
            result = await self.db.execute_query(query, (
                password_hash, 
                datetime.datetime.utcnow(), 
                user_id
            ))
            
            if result:
                await self._log_activity(user_id, "PASSWORD_CHANGED", "Admin changed user password")
=======
            result = await self.db.execute_query(
                query, (password_hash, datetime.datetime.utcnow(), user_id)
            )
            
            if result:
                await self.activity_logger.log_activity(user_id, "PASSWORD_CHANGED", "Admin changed user password")
>>>>>>> f990657e46599176c49ea15f7f4ba09c3ad15e5a
                return True
            return False
        except Exception as e:
            logger.error(f"Change password error: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    async def delete_user(self, user_id: int) -> str:
<<<<<<< HEAD
        """Delete user permanently - removes all related data"""
=======
        """Permanently delete user and all related data"""
>>>>>>> f990657e46599176c49ea15f7f4ba09c3ad15e5a
        user = await self.get_user_by_id(user_id)
        if not user:
            raise ValueError("User not found")
        
        try:
<<<<<<< HEAD
            # Step 1: Delete user roles (iam_user_role)
            await self.db.execute_query("DELETE FROM iam.iam_user_role WHERE user_id = $1", (user_id,))
            logger.info(f"Deleted roles for user_id: {user_id}")
            
            # Step 2: Delete user sessions (if table exists)
            try:
                await self.db.execute_query("DELETE FROM iam.iam_user_session WHERE user_id = $1", (user_id,))
                logger.info(f"Deleted sessions for user_id: {user_id}")
            except Exception as e:
                logger.warning(f"Could not delete sessions (table may not exist): {e}")
            
            # Step 3: Delete activity logs (user_activity_logs)
            try:
                await self.db.execute_query("DELETE FROM iam.user_activity_logs WHERE user_id = $1", (user_id,))
                logger.info(f"Deleted activity logs for user_id: {user_id}")
            except Exception as e:
                logger.warning(f"Could not delete activity logs: {e}")
            
            # Step 4: Delete password reset tokens (if table exists)
            try:
                await self.db.execute_query("DELETE FROM iam.iam_password_reset_token WHERE user_id = $1", (user_id,))
                logger.info(f"Deleted password reset tokens for user_id: {user_id}")
            except Exception as e:
                logger.warning(f"Could not delete password reset tokens: {e}")
            
            # Step 5: Delete email verification tokens (if exists)
            try:
                await self.db.execute_query("DELETE FROM iam.iam_email_verification_token WHERE email = $1", (user['email'],))
                logger.info(f"Deleted email verification tokens for email: {user['email']}")
            except Exception as e:
                logger.warning(f"Could not delete email verification tokens: {e}")
            
            # Step 6: Finally, delete the user
            await self.db.execute_query("DELETE FROM iam.iam_user WHERE user_id = $1", (user_id,))
            logger.info(f"✅ Successfully deleted user: {user['email']} (ID: {user_id})")
=======
            # Delete in order: roles, sessions, logs, tokens, then user
            await self.db.execute_query("DELETE FROM iam.iam_user_role WHERE user_id = $1", (user_id,))
            logger.info(f"Deleted roles for user {user_id}")
            
            # Optional tables (may not exist)
            for table, condition in [
                ("iam.iam_user_session", f"user_id = {user_id}"),
                ("user_activity_logs", f"user_id = {user_id}"),
                ("iam.iam_password_reset_token", f"user_id = {user_id}"),
                ("iam.iam_email_verification_token", f"email = '{user['email']}'"),
            ]:
                try:
                    await self.db.execute_query(f"DELETE FROM {table} WHERE {condition}")
                except Exception as e:
                    logger.warning(f"Could not delete from {table}: {e}")
            
            # Finally delete user
            await self.db.execute_query("DELETE FROM iam.iam_user WHERE user_id = $1", (user_id,))
            logger.info(f"✅ Deleted user: {user['email']} (ID: {user_id})")
>>>>>>> f990657e46599176c49ea15f7f4ba09c3ad15e5a
            
            return user['email']
            
        except Exception as e:
<<<<<<< HEAD
            logger.error(f"❌ Error deleting user {user_id}: {e}")
            raise Exception(f"Failed to delete user: {str(e)}")

    async def disable_user(self, user_id: int) -> bool:
        """Soft delete user - change status to disabled"""
=======
            logger.error(f"Delete user error: {e}")
            raise Exception(f"Failed to delete user: {str(e)}")

    async def disable_user(self, user_id: int) -> bool:
        """Soft delete - change status to disabled"""
>>>>>>> f990657e46599176c49ea15f7f4ba09c3ad15e5a
        user = await self.get_user_by_id(user_id)
        if not user:
            raise ValueError("User not found")
        
<<<<<<< HEAD
        query = """
        UPDATE iam.iam_user 
        SET status = 'disabled', updated_at = NOW()
        WHERE user_id = $1
        """
=======
        query = "UPDATE iam.iam_user SET status = 'disabled', updated_at = NOW() WHERE user_id = $1"
>>>>>>> f990657e46599176c49ea15f7f4ba09c3ad15e5a
        await self.db.execute_query(query, (user_id,))
        return True

    async def restore_user(self, user_id: int) -> bool:
        """Restore user - change status back to active"""
        user = await self.get_user_by_id(user_id)
        if not user:
            raise ValueError("User not found")
        
<<<<<<< HEAD
        query = """
        UPDATE iam.iam_user 
        SET status = 'active', updated_at = NOW()
        WHERE user_id = $1
        """
=======
        query = "UPDATE iam.iam_user SET status = 'active', updated_at = NOW() WHERE user_id = $1"
>>>>>>> f990657e46599176c49ea15f7f4ba09c3ad15e5a
        await self.db.execute_query(query, (user_id,))
        return True

    async def update_last_login(self, user_id: int) -> bool:
        """Update user's last login timestamp"""
<<<<<<< HEAD
        query = """
        UPDATE iam.iam_user 
        SET last_login_at = NOW(), updated_at = NOW()
        WHERE user_id = $1
        """
        try:
            await self.db.execute_query(query, (user_id,))
            logger.info(f"Last login updated for user_id: {user_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to update last login for user_id {user_id}: {e}")
            return False

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
        FROM iam.user_activity_logs
        {where_clause}
        ORDER BY created_at DESC
        LIMIT $1 OFFSET $2
        """
        
        count_query = f"""
        SELECT COUNT(*) as total FROM iam.user_activity_logs
        {where_clause.replace('$3', '$1') if where_clause else ''}
        """
        
        logs = await self.db.execute_query(query, params)
        
=======
        query = "UPDATE iam.iam_user SET last_login_at = NOW(), updated_at = NOW() WHERE user_id = $1"
        try:
            await self.db.execute_query(query, (user_id,))
            logger.info(f"Last login updated for user {user_id}")
            return True
        except Exception as e:
            logger.error(f"Update last login error: {e}")
            return False

    # ==================== ACTIVITY LOGS ====================

    async def get_activity_logs(
        self, page: int = 1, limit: int = 20, user_id: Optional[int] = None
    ) -> Tuple[List[Dict], int]:
        """Get activity logs with pagination"""
        offset = (page - 1) * limit
        where_clause = "WHERE user_id = $3" if user_id else ""
        params = [limit, offset] + ([user_id] if user_id else [])
        
        query = f"""
            SELECT log_id, user_id, email, action, resource, details, 
                   ip_address, status, created_at
            FROM iam.user_activity_logs
            {where_clause}
            ORDER BY created_at DESC
            LIMIT $1 OFFSET $2
        """
        
        count_query = f"SELECT COUNT(*) as total FROM iam.user_activity_logs {where_clause.replace('$3', '$1') if where_clause else ''}"
        
        logs = await self.db.execute_query(query, params)
>>>>>>> f990657e46599176c49ea15f7f4ba09c3ad15e5a
        count_params = [user_id] if user_id else []
        count_result = await self.db.execute_query(count_query, count_params)
        total = count_result[0]['total'] if count_result else 0
        
        return logs, total

    async def get_activity_stats(self) -> Dict:
<<<<<<< HEAD
        """Get activity statistics"""
        stats_query = """
        SELECT 
            COUNT(*) as total_activities,
            COUNT(CASE WHEN status = 'success' THEN 1 END) as successful_activities,
            COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_activities,
            COUNT(DISTINCT user_id) as unique_users
        FROM iam.user_activity_logs
        WHERE created_at >= NOW() - INTERVAL '30 days'
        """
        
        top_actions_query = """
        SELECT action, COUNT(*) as count
        FROM iam.user_activity_logs
        WHERE created_at >= NOW() - INTERVAL '7 days'
        GROUP BY action
        ORDER BY count DESC
        LIMIT 5
        """
        
        recent_query = """
        SELECT log_id, user_id, email, action, resource, details, 
               ip_address, status, created_at
        FROM iam.user_activity_logs
        ORDER BY created_at DESC
        LIMIT 10
=======
        """Get activity statistics for dashboard"""
        stats_query = """
            SELECT 
                COUNT(*) as total_activities,
                COUNT(CASE WHEN status = 'success' THEN 1 END) as successful_activities,
                COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_activities,
                COUNT(DISTINCT user_id) as unique_users
            FROM iam.user_activity_logs
            WHERE created_at >= NOW() - INTERVAL '30 days'
        """
        
        top_actions_query = """
            SELECT action, COUNT(*) as count
            FROM iam.user_activity_logs
            WHERE created_at >= NOW() - INTERVAL '7 days'
            GROUP BY action
            ORDER BY count DESC
            LIMIT 5
        """
        
        recent_query = """
            SELECT log_id, user_id, email, action, resource, details, 
                   ip_address, status, created_at
            FROM iam.user_activity_logs
            ORDER BY created_at DESC
            LIMIT 10
>>>>>>> f990657e46599176c49ea15f7f4ba09c3ad15e5a
        """
        
        stats = await self.db.execute_query(stats_query)
        top_actions = await self.db.execute_query(top_actions_query)
        recent = await self.db.execute_query(recent_query)
        
        return {
            **(stats[0] if stats else {}),
            "top_actions": top_actions,
            "recent_activities": recent
        }

<<<<<<< HEAD
    async def _assign_role(self, user_id: int, role_code: str) -> None:
        """Assign role to user"""
=======
    # ==================== HELPER METHODS ====================

    async def _assign_role(self, user_id: int, role_code: str) -> None:
        """Assign role to user (replaces existing role)"""
>>>>>>> f990657e46599176c49ea15f7f4ba09c3ad15e5a
        # Get role ID
        role_query = "SELECT role_id FROM iam.iam_role WHERE role_code = $1"
        role_result = await self.db.execute_query(role_query, (role_code,))
        
        if not role_result:
            raise ValueError(f"Role '{role_code}' not found")
        
        role_id = role_result[0]['role_id']
        
        # Remove existing roles
        await self.db.execute_query("DELETE FROM iam.iam_user_role WHERE user_id = $1", (user_id,))
        
        # Assign new role
<<<<<<< HEAD
        assign_query = """
        INSERT INTO iam.iam_user_role (user_id, role_id, assigned_at)
        VALUES ($1, $2, NOW())
        """
=======
        assign_query = "INSERT INTO iam.iam_user_role (user_id, role_id, assigned_at) VALUES ($1, $2, NOW())"
>>>>>>> f990657e46599176c49ea15f7f4ba09c3ad15e5a
        await self.db.execute_query(assign_query, (user_id, role_id))
