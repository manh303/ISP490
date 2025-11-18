"""
Admin Service - Business Logic
"""
import logging
import bcrypt
import datetime
from typing import List, Dict, Any, Optional, Tuple
from fastapi import HTTPException
from models.admin import UserCreateRequest, UserUpdateRequest, PasswordChangeRequest, UserPasswordUpdateRequest, UserActionResponse
from app.services.activity_logger import ActivityLogger

logger = logging.getLogger(__name__)

class AdminService:
    def __init__(self, db):
        self.db = db

    async def get_users(self, status_filter: str = None) -> List[Dict]:
        """Get all users without pagination
        
        Uses subquery to avoid duplicate rows from JOIN
        """
        
        # Build query with optional status filter
        where_clause = ""
        params = []
        if status_filter:
            where_clause = "WHERE status = $1"
            params.append(status_filter)
        
        query = f"""
       SELECT u.user_id, u.email, u.full_name, u.phone, u.status, 
               u.created_at, u.updated_at, u.last_login_at,
               r.role_code, r.role_name
        FROM iam.iam_user u
        LEFT JOIN iam.iam_user_role ur ON u.user_id = ur.user_id
        LEFT JOIN iam.iam_role r ON ur.role_id = r.role_id
        {where_clause}
        ORDER BY u.created_at DESC
        """
        
        if params:
            users = await self.db.execute_query(query, tuple(params))
        else:
            users = await self.db.execute_query(query)
        
        logger.info(f"✅ Query returned {len(users)} users")
        return users

    async def get_user_by_id(self, user_id: int) -> Optional[Dict]:
        """Get user by ID with full roles and permissions array
        Enhanced version - returns complete user data with roles[] and permissions[]
        """
        try:
            # Get user basic info
            user_query = "SELECT * FROM iam.iam_user WHERE user_id = $1"
            user_result = await self.db.execute_query(user_query, (user_id,))

            if not user_result:
                return None

            user = user_result[0]

            # Get user roles
            roles_query = """
                SELECT r.role_id, r.role_code, r.role_name, r.description
                FROM iam.iam_role r
                JOIN iam.iam_user_role ur ON r.role_id = ur.role_id
                WHERE ur.user_id = $1
            """
            roles_result = await self.db.execute_query(roles_query, (user_id,))

            # Get user permissions
            permissions_query = """
                SELECT DISTINCT p.perm_id, p.perm_code, p.perm_name, p.module, p.action, p.description
                FROM iam.iam_permission p
                JOIN iam.iam_role_permission rp ON p.perm_id = rp.perm_id
                JOIN iam.iam_user_role ur ON rp.role_id = ur.role_id
                WHERE ur.user_id = $1
            """
            permissions_result = await self.db.execute_query(permissions_query, (user_id,))

            primary_role = roles_result[0] if roles_result else None

            return {
                'user_id': user['user_id'],
                'email': user['email'],
                'full_name': user['full_name'],
                'phone': user['phone'],
                'status': user['status'],
                'mfa_enabled': user['mfa_enabled'],
                'last_login_at': user['last_login_at'],
                'created_at': user['created_at'],
                'updated_at': user['updated_at'],
                # Single role fields for UserResponse compatibility
                'role_code': primary_role['role_code'] if primary_role else None,
                'role_name': primary_role['role_name'] if primary_role else None,
                # Full roles array for advanced usage
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
                ]
            }

        except Exception as e:
            logger.error(f"Get user by ID error: {e}")
            return None

    async def create_user(self, user_data: UserCreateRequest) -> Dict[str, Any]:
        email = user_data.email
        password = user_data.password
        full_name = user_data.full_name or ""
        phone = user_data.phone or ""
        status = "active"
        role_code = user_data.role

        # 1. Kiểm tra email đã tồn tại chưa
        check_query = "SELECT user_id FROM iam.iam_user WHERE email = $1"
        existing_user = await self.db.execute_query(check_query, (email,))
        if existing_user:
            raise HTTPException(status_code=400, detail="Email already exists")

        # 2. Mã hóa password
        password_hash = await self.iam_service.hash_password(password)

        try:
            # 3. Transaction: tạo user + gán role + ghi log
            async with self.db.transaction() as conn:
                # 3.1 Insert user
                insert_user_query = """
                    INSERT INTO iam.iam_user (
                        email, password_hash, full_name, phone, status, created_at, updated_at
                    )
                    VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
                    RETURNING user_id, email, full_name, phone, status, created_at, updated_at
                """
                user_row = await conn.fetchrow(
                    insert_user_query,
                    email,
                    password_hash,
                    full_name,
                    phone,
                    status,
                )

                if not user_row:
                    raise HTTPException(status_code=500, detail="Failed to create user")

                user_id = user_row["user_id"]

                # 3.2 Gán role nếu có
                if role_code:
                    role_id = await conn.fetchval(
                        "SELECT role_id FROM iam.iam_role WHERE role_code = $1",
                        role_code,
                    )
                    if not role_id:
                        raise HTTPException(
                            status_code=400,
                            detail=f"Role '{role_code}' not found",
                        )

                    await conn.execute(
                        """
                        INSERT INTO iam.iam_user_role (user_id, role_id, assigned_at)
                        VALUES ($1, $2, NOW())
                        """,
                        user_id,
                        role_id,
                    )
                    logger.info(f"✅ Role {role_code} assigned to user {user_id}")

                # 3.3 Ghi log activity
                await conn.execute(
                    """
                    INSERT INTO iam.user_activity_logs (user_id, action, details, created_at)
                    VALUES ($1, $2, $3, NOW())
                    """,
                    user_id,
                    "USER_CREATED",
                    f"Admin created user: {email} with role: {role_code}",
                )

                # 3.4 Build response dict gọn
                result_dict = {
                    "user_id": user_row["user_id"],
                    "email": user_row["email"],
                    "full_name": user_row["full_name"],
                    "phone": user_row["phone"],
                    "status": user_row["status"],
                    "created_at": user_row["created_at"],
                }

            logger.info(f"✅ User created successfully: {email} (ID: {user_id})")
            return result_dict

        except HTTPException:
            # ném lại cho FastAPI xử lý
            raise
        except Exception as e:
            logger.error(f"Create user error: {e}")
            raise HTTPException(status_code=500, detail="Internal server error")

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
            UPDATE iam.iam_user 
            SET {', '.join(update_fields)}
            WHERE user_id = ${param_count}
            """
            
            await self.db.execute_query(update_query, values)
        
        # Update role if provided
        if user_data.role is not None:
            await self._assign_role(user_id, user_data.role)
        
        return True

    async def change_password(self, user_id: int, new_password: str) -> bool:
        """Change user password (admin function - no current password verification)
        Note: For user self-service password change, use IAMService.change_password() which requires current password
        """
        try:
            from app.services.iam_service import IAMService
            iam_service = IAMService(self.db)
            
            password_hash = await iam_service.hash_password(new_password)
            
            query = """
                UPDATE iam_user 
                SET password_hash = $1, updated_at = $2 
                WHERE user_id = $3
                RETURNING user_id
            """
            result = await self.db.execute_query(query, (
                password_hash, 
                datetime.datetime.utcnow(), 
                user_id
            ))
            
            if result:
                await self._log_activity(user_id, "PASSWORD_CHANGED", "Admin changed user password")
                return True
            return False
        except Exception as e:
            logger.error(f"Change password error: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    async def delete_user(self, user_id: int) -> str:
        """Delete user permanently - removes all related data"""
        user = await self.get_user_by_id(user_id)
        if not user:
            raise ValueError("User not found")
        
        try:
            # Step 1: Delete user roles (iam_user_role)
            await self.db.execute_query("DELETE FROM iam_user_role WHERE user_id = $1", (user_id,))
            logger.info(f"Deleted roles for user_id: {user_id}")
            
            # Step 2: Delete user sessions (if table exists)
            try:
                await self.db.execute_query("DELETE FROM iam_user_session WHERE user_id = $1", (user_id,))
                logger.info(f"Deleted sessions for user_id: {user_id}")
            except Exception as e:
                logger.warning(f"Could not delete sessions (table may not exist): {e}")
            
            # Step 3: Delete activity logs (user_activity_logs)
            try:
                await self.db.execute_query("DELETE FROM user_activity_logs WHERE user_id = $1", (user_id,))
                logger.info(f"Deleted activity logs for user_id: {user_id}")
            except Exception as e:
                logger.warning(f"Could not delete activity logs: {e}")
            
            # Step 4: Delete password reset tokens (if table exists)
            try:
                await self.db.execute_query("DELETE FROM iam_password_reset_token WHERE user_id = $1", (user_id,))
                logger.info(f"Deleted password reset tokens for user_id: {user_id}")
            except Exception as e:
                logger.warning(f"Could not delete password reset tokens: {e}")
            
            # Step 5: Delete email verification tokens (if exists)
            try:
                await self.db.execute_query("DELETE FROM iam_email_verification_token WHERE email = $1", (user['email'],))
                logger.info(f"Deleted email verification tokens for email: {user['email']}")
            except Exception as e:
                logger.warning(f"Could not delete email verification tokens: {e}")
            
            # Step 6: Finally, delete the user
            await self.db.execute_query("DELETE FROM iam_user WHERE user_id = $1", (user_id,))
            logger.info(f"✅ Successfully deleted user: {user['email']} (ID: {user_id})")
            
            return user['email']
            
        except Exception as e:
            logger.error(f"❌ Error deleting user {user_id}: {e}")
            raise Exception(f"Failed to delete user: {str(e)}")

    async def disable_user(self, user_id: int) -> bool:
        """Soft delete user - change status to disabled"""
        user = await self.get_user_by_id(user_id)
        if not user:
            raise ValueError("User not found")
        
        query = """
        UPDATE iam.iam_user 
        SET status = 'disabled', updated_at = NOW()
        WHERE user_id = $1
        """
        await self.db.execute_query(query, (user_id,))
        return True

    async def restore_user(self, user_id: int) -> bool:
        """Restore user - change status back to active"""
        user = await self.get_user_by_id(user_id)
        if not user:
            raise ValueError("User not found")
        
        query = """
        UPDATE iam.iam_user 
        SET status = 'active', updated_at = NOW()
        WHERE user_id = $1
        """
        await self.db.execute_query(query, (user_id,))
        return True

    async def update_last_login(self, user_id: int) -> bool:
        """Update user's last login timestamp"""
        query = """
        UPDATE iam_user 
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
        """
        
        stats = await self.db.execute_query(stats_query)
        top_actions = await self.db.execute_query(top_actions_query)
        recent = await self.db.execute_query(recent_query)
        
        return {
            **(stats[0] if stats else {}),
            "top_actions": top_actions,
            "recent_activities": recent
        }

    async def _log_activity(self, user_id: int, action: str, details: str = None) -> None:
        """Log user activity to database - details is now TEXT type"""
        try:
            query = """
                INSERT INTO iam.user_activity_logs (user_id, action, details, created_at)
                VALUES ($1, $2, $3, NOW())
            """
            await self.db.execute_query(query, (user_id, action, details or ''))
        except Exception as e:
            logger.warning(f"Failed to log activity: {e}")

    async def _assign_role(self, user_id: int, role_code: str) -> None:
        """Assign role to user"""
        # Get role ID
        role_query = "SELECT role_id FROM iam.iam_role WHERE role_code = $1"
        role_result = await self.db.execute_query(role_query, (role_code,))
        
        if not role_result:
            raise ValueError(f"Role '{role_code}' not found")
        
        role_id = role_result[0]['role_id']
        
        # Remove existing roles
        await self.db.execute_query("DELETE FROM iam.iam_user_role WHERE user_id = $1", (user_id,))
        
        # Assign new role
        assign_query = """
        INSERT INTO iam.iam_user_role (user_id, role_id, assigned_at)
        VALUES ($1, $2, NOW())
        """
        await self.db.execute_query(assign_query, (user_id, role_id))
