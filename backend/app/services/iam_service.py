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

    async def create_user(self, email: str, password: str, full_name: str = None, phone: str = None) -> Dict[str, Any]:
        """Create new user in IAM system"""
        try:
            # Check if user already exists
            existing_user = await self.get_user_by_email(email)
            if existing_user:
                raise HTTPException(status_code=400, detail="Email already registered")

            # Hash password
            password_hash = await self.hash_password(password)

            # Insert user
            user_query = """
                INSERT INTO iam.iam_user (email, password_hash, full_name, phone, status, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                RETURNING user_id, email, full_name, phone, status, created_at
            """
            now = datetime.datetime.utcnow()
            safe_name = full_name or ""
            safe_phone = phone or ""

            logger.info("create_user SQL: %s", user_query.strip())
            logger.info("create_user params: email=%s, full_name=%s, phone=%s, status=active, ts=%s",
                        email, safe_name, safe_phone, now)

            result = await self.db.execute_query(
                user_query,
                (email, password_hash, safe_name, safe_phone, 'active', now, now)
            )

            if result:
                user_data = result[0]

                # Assign default role (CUSTOMER) - temporarily disabled for simplicity
                # await self.assign_role_to_user(user_data['user_id'], 'CUSTOMER')
                logger.info(f"User created successfully: {user_data['user_id']} - role assignment skipped for now")

                return {
                    'user_id': user_data['user_id'],
                    'email': user_data['email'],
                    'full_name': user_data['full_name'],
                    'phone': user_data['phone'],
                    'status': user_data['status'],
                    'created_at': user_data['created_at']
                }
            else:
                raise HTTPException(status_code=500, detail="Failed to create user")

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Create user error: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to create user: {str(e)}")

    async def get_user_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        """Get user by email"""
        try:
            query = "SELECT * FROM iam.iam_user WHERE email = $1"
            logger.info(f"Executing query: {query} with email: {email}")
            result = await self.db.execute_query(query, (email,))
            logger.info(f"Query result: {result}")
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Get user by email error: {e}")
            logger.error(f"Query was: {query}")
            logger.error(f"Email was: {email}")
            return None

    async def get_user_by_id(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Get user by ID with roles and permissions"""
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

            # Get full user data with roles and permissions
            full_user = await self.get_user_by_id(user['user_id'])
            return full_user

        except Exception as e:
            logger.error(f"Authenticate user error: {e}")
            return None

    async def update_last_login(self, user_id: int):
        """Update user's last login timestamp"""
        try:
            query = "UPDATE iam.iam_user SET last_login_at = $1 WHERE user_id = $2"
            await self.db.execute_query(query, (datetime.datetime.utcnow(), user_id))
        except Exception as e:
            logger.error(f"Update last login error: {e}")

    # Token functionality removed - using session-based auth

    async def assign_role_to_user(self, user_id: int, role_code: str):
        """Assign role to user"""
        try:
            # Get role ID
            role_query = "SELECT role_id FROM iam.iam_role WHERE role_code = $1"
            role_result = await self.db.execute_query(role_query, (role_code,))

            if not role_result:
                logger.warning(f"Role {role_code} not found")
                return

            role_id = role_result[0]['role_id']

            # Check if already assigned
            check_query = "SELECT 1 FROM iam.iam_user_role WHERE user_id = $1 AND role_id = $2"
            check_result = await self.db.execute_query(check_query, (user_id, role_id))

            if check_result:
                return  # Already assigned

            # Assign role
            assign_query = """
                INSERT INTO iam.iam_user_role (user_id, role_id, assigned_at)
                VALUES ($1, $2, $3)
            """
            await self.db.execute_query(assign_query, (
                user_id, role_id, datetime.datetime.utcnow()
            ))

        except Exception as e:
            logger.error(f"Assign role error: {e}")

    async def check_permission(self, user_id: int, permission_code: str) -> bool:
        """Check if user has specific permission"""
        try:
            query = """
                SELECT 1 FROM iam.iam_permission p
                JOIN iam.iam_role_permission rp ON p.perm_id = rp.perm_id
                JOIN iam.iam_user_role ur ON rp.role_id = ur.role_id
                WHERE ur.user_id = $1 AND p.perm_code = $2
            """
            result = await self.db.execute_query(query, (user_id, permission_code))

            return bool(result)

        except Exception as e:
            logger.error(f"Check permission error: {e}")
            return False

    async def update_user_profile(self, user_id: int, data: Dict[str, Any]) -> Dict[str, Any]:
        """Update user profile"""
        try:
            # Build update query dynamically
            allowed_fields = ['full_name', 'phone']
            update_fields = []
            update_values = {'user_id': user_id, 'updated_at': datetime.datetime.utcnow()}

            for field, value in data.items():
                if field in allowed_fields and value is not None:
                    update_fields.append(f"{field} = ${len(update_values) + 1}")
                    update_values[field] = value

            if not update_fields:
                raise HTTPException(status_code=400, detail="No valid fields to update")

            query = f"""
                UPDATE iam.iam_user
                SET {', '.join(update_fields)}, updated_at = $2
                WHERE user_id = $1
                RETURNING user_id, email, full_name, phone, status, updated_at
            """

            result = await self.db.execute_query(query, update_values)

            if result:
                return result[0]
            else:
                raise HTTPException(status_code=404, detail="User not found")

        except Exception as e:
            logger.error(f"Update user profile error: {e}")
            raise HTTPException(status_code=500, detail=str(e))

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
                UPDATE iam.iam_user
                SET password_hash = $1, updated_at = $2
                WHERE user_id = $3
            """
            await self.db.execute_query(query, (new_password_hash, datetime.datetime.utcnow(), user_id))

        except Exception as e:
            logger.error(f"Change password error: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    async def get_user_by_email_by_id(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Get basic user info by ID (including password hash for verification)"""
        try:
            query = "SELECT * FROM iam.iam_user WHERE user_id = $1"
            result = await self.db.execute_query(query, (user_id,))
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Get user by ID error: {e}")
            return None

    # Password reset token functionality removed

    async def log_user_action(self, user_id: int, action: str, target_type: str = None, target_id: str = None, details: str = None):
        """Log user action for audit"""
        try:
            query = """
                INSERT INTO iam.iam_audit_log (user_id, action, target_type, target_id, details_text, created_at)
                VALUES ($1, $2, $3, $4, $5, $6)
            """
            await self.db.execute_query(query, (user_id, action, target_type, target_id, details, datetime.datetime.utcnow()))
        except Exception as e:
            logger.error(f"Log user action error: {e}")