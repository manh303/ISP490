"""
IAM (Identity & Access Management) Service
Handles authentication, authorization, and user management using PostgreSQL IAM schema
"""

import hashlib
import secrets
import datetime
from typing import Optional, Dict, List, Any
import bcrypt
import jwt
from fastapi import HTTPException
import logging

logger = logging.getLogger(__name__)

class IAMService:
    def __init__(self, db_manager, secret_key: str = "your-secret-key"):
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
                INSERT INTO iam_user (email, password_hash, full_name, phone, status, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                RETURNING user_id, email, full_name, phone, status, created_at
            """
            now = datetime.datetime.utcnow()
            result = await self.db.execute_query(user_query, {
                'email': email,
                'password_hash': password_hash,
                'full_name': full_name,
                'phone': phone,
                'status': 'active',
                'created_at': now,
                'updated_at': now
            })

            if result:
                user_data = result[0]

                # Assign default role (CUSTOMER)
                await self.assign_role_to_user(user_data['user_id'], 'CUSTOMER')

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

        except Exception as e:
            logger.error(f"Create user error: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    async def get_user_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        """Get user by email"""
        try:
            query = "SELECT * FROM iam_user WHERE email = $1"
            result = await self.db.execute_query(query, {'email': email})
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Get user by email error: {e}")
            return None

    async def get_user_by_id(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Get user by ID with roles and permissions"""
        try:
            # Get user basic info
            user_query = "SELECT * FROM iam_user WHERE user_id = $1"
            user_result = await self.db.execute_query(user_query, {'user_id': user_id})

            if not user_result:
                return None

            user = user_result[0]

            # Get user roles
            roles_query = """
                SELECT r.role_id, r.role_code, r.role_name, r.description
                FROM iam_role r
                JOIN iam_user_role ur ON r.role_id = ur.role_id
                WHERE ur.user_id = $1
            """
            roles_result = await self.db.execute_query(roles_query, {'user_id': user_id})

            # Get user permissions
            permissions_query = """
                SELECT DISTINCT p.perm_id, p.perm_code, p.perm_name, p.module, p.action, p.description
                FROM iam_permission p
                JOIN iam_role_permission rp ON p.perm_id = rp.perm_id
                JOIN iam_user_role ur ON rp.role_id = ur.role_id
                WHERE ur.user_id = $1
            """
            permissions_result = await self.db.execute_query(permissions_query, {'user_id': user_id})

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
            query = "UPDATE iam_user SET last_login_at = $1 WHERE user_id = $2"
            await self.db.execute_query(query, {
                'last_login_at': datetime.datetime.utcnow(),
                'user_id': user_id
            })
        except Exception as e:
            logger.error(f"Update last login error: {e}")

    async def create_access_token(self, user: Dict[str, Any]) -> str:
        """Create JWT access token"""
        try:
            payload = {
                'user_id': user['user_id'],
                'email': user['email'],
                'roles': [role['role_code'] for role in user.get('roles', [])],
                'permissions': [perm['perm_code'] for perm in user.get('permissions', [])],
                'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=self.token_expire_hours),
                'iat': datetime.datetime.utcnow()
            }

            token = jwt.encode(payload, self.secret_key, algorithm=self.jwt_algorithm)
            return token

        except Exception as e:
            logger.error(f"Create access token error: {e}")
            raise HTTPException(status_code=500, detail="Failed to create token")

    def verify_access_token(self, token: str) -> Optional[Dict[str, Any]]:
        """Verify JWT access token"""
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.jwt_algorithm])
            user_id = payload.get('user_id')

            if not user_id:
                return None

            # Return minimal user data from token for quick verification
            return {
                'user_id': user_id,
                'email': payload.get('email'),
                'roles': [{'role_code': role} for role in payload.get('roles', [])],
                'permissions': [{'perm_code': perm} for perm in payload.get('permissions', [])],
                'token_valid': True
            }

        except jwt.ExpiredSignatureError:
            return None
        except jwt.InvalidTokenError:
            return None
        except Exception as e:
            logger.error(f"Verify access token error: {e}")
            return None

    async def create_refresh_token(self, user_id: int) -> str:
        """Create refresh token and store in database"""
        try:
            # Generate refresh token
            refresh_token = secrets.token_urlsafe(32)
            refresh_token_hash = hashlib.sha256(refresh_token.encode()).hexdigest()

            # Store in database
            session_query = """
                INSERT INTO iam_user_session (user_id, issued_at, expires_at, refresh_token_hash)
                VALUES ($1, $2, $3, $4)
                RETURNING session_id
            """
            expires_at = datetime.datetime.utcnow() + datetime.timedelta(days=7)  # 7 days

            result = await self.db.execute_query(session_query, {
                'user_id': user_id,
                'issued_at': datetime.datetime.utcnow(),
                'expires_at': expires_at,
                'refresh_token_hash': refresh_token_hash
            })

            return refresh_token

        except Exception as e:
            logger.error(f"Create refresh token error: {e}")
            raise HTTPException(status_code=500, detail="Failed to create refresh token")

    async def verify_refresh_token(self, refresh_token: str) -> Optional[int]:
        """Verify refresh token and return user_id"""
        try:
            refresh_token_hash = hashlib.sha256(refresh_token.encode()).hexdigest()

            query = """
                SELECT user_id FROM iam_user_session
                WHERE refresh_token_hash = $1
                AND expires_at > $2
                AND revoked_at IS NULL
            """
            result = await self.db.execute_query(query, {
                'refresh_token_hash': refresh_token_hash,
                'expires_at': datetime.datetime.utcnow()
            })

            return result[0]['user_id'] if result else None

        except Exception as e:
            logger.error(f"Verify refresh token error: {e}")
            return None

    async def revoke_refresh_token(self, refresh_token: str):
        """Revoke refresh token"""
        try:
            refresh_token_hash = hashlib.sha256(refresh_token.encode()).hexdigest()

            query = """
                UPDATE iam_user_session
                SET revoked_at = $1
                WHERE refresh_token_hash = $2
            """
            await self.db.execute_query(query, {
                'revoked_at': datetime.datetime.utcnow(),
                'refresh_token_hash': refresh_token_hash
            })

        except Exception as e:
            logger.error(f"Revoke refresh token error: {e}")

    async def assign_role_to_user(self, user_id: int, role_code: str):
        """Assign role to user"""
        try:
            # Get role ID
            role_query = "SELECT role_id FROM iam_role WHERE role_code = $1"
            role_result = await self.db.execute_query(role_query, {'role_code': role_code})

            if not role_result:
                logger.warning(f"Role {role_code} not found")
                return

            role_id = role_result[0]['role_id']

            # Check if already assigned
            check_query = "SELECT 1 FROM iam_user_role WHERE user_id = $1 AND role_id = $2"
            check_result = await self.db.execute_query(check_query, {
                'user_id': user_id,
                'role_id': role_id
            })

            if check_result:
                return  # Already assigned

            # Assign role
            assign_query = """
                INSERT INTO iam_user_role (user_id, role_id, assigned_at)
                VALUES ($1, $2, $3)
            """
            await self.db.execute_query(assign_query, {
                'user_id': user_id,
                'role_id': role_id,
                'assigned_at': datetime.datetime.utcnow()
            })

        except Exception as e:
            logger.error(f"Assign role error: {e}")

    async def check_permission(self, user_id: int, permission_code: str) -> bool:
        """Check if user has specific permission"""
        try:
            query = """
                SELECT 1 FROM iam_permission p
                JOIN iam_role_permission rp ON p.perm_id = rp.perm_id
                JOIN iam_user_role ur ON rp.role_id = ur.role_id
                WHERE ur.user_id = $1 AND p.perm_code = $2
            """
            result = await self.db.execute_query(query, {
                'user_id': user_id,
                'permission_code': permission_code
            })

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
                UPDATE iam_user
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
                UPDATE iam_user
                SET password_hash = $1, updated_at = $2
                WHERE user_id = $3
            """
            await self.db.execute_query(query, {
                'password_hash': new_password_hash,
                'updated_at': datetime.datetime.utcnow(),
                'user_id': user_id
            })

        except Exception as e:
            logger.error(f"Change password error: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    async def get_user_by_email_by_id(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Get basic user info by ID (including password hash for verification)"""
        try:
            query = "SELECT * FROM iam_user WHERE user_id = $1"
            result = await self.db.execute_query(query, {'user_id': user_id})
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Get user by ID error: {e}")
            return None

    async def log_user_action(self, user_id: int, action: str, target_type: str = None, target_id: str = None, details: str = None):
        """Log user action for audit"""
        try:
            query = """
                INSERT INTO iam_audit_log (user_id, action, target_type, target_id, details_text, created_at)
                VALUES ($1, $2, $3, $4, $5, $6)
            """
            await self.db.execute_query(query, {
                'user_id': user_id,
                'action': action,
                'target_type': target_type,
                'target_id': target_id,
                'details_text': details,
                'created_at': datetime.datetime.utcnow()
            })
        except Exception as e:
            logger.error(f"Log user action error: {e}")