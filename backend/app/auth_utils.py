#!/usr/bin/env python3
"""
JWT Authentication Utilities for Vietnam E-commerce DSS
Handles JWT token creation, validation, and user authentication
"""

import os
import secrets
from datetime import datetime, timedelta
from typing import Dict, Optional, Any

# Try different JWT libraries
try:
    import jwt
    JWT_LIB = "PyJWT"
except ImportError:
    try:
        from jose import jwt, JWTError
        JWT_LIB = "python-jose"
    except ImportError:
        jwt = None
        JWT_LIB = None

# Try to import password hashing
try:
    from passlib.context import CryptContext
    pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
    PASSLIB_AVAILABLE = True
except ImportError:
    pwd_context = None
    PASSLIB_AVAILABLE = False

# JWT Configuration
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "vietnam-ecommerce-dss-super-secret-key-2024-change-in-production")
JWT_ALGORITHM = "HS256"
JWT_ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24  # 24 hours

class JWTManager:
    """JWT Token Manager for authentication"""

    def __init__(self):
        self.secret_key = JWT_SECRET_KEY
        self.algorithm = JWT_ALGORITHM
        self.expire_minutes = JWT_ACCESS_TOKEN_EXPIRE_MINUTES

    def generate_secret_key(self) -> str:
        """Generate a secure random secret key"""
        return secrets.token_urlsafe(32)

    def create_access_token(self, data: Dict[str, Any], expires_delta: Optional[timedelta] = None) -> str:
        """Create JWT access token"""
        if jwt is None:
            # Fallback to mock token if JWT not available
            return f"mock_jwt_token_{data.get('username', 'user')}_{int(datetime.utcnow().timestamp())}"

        to_encode = data.copy()

        if expires_delta:
            expire = datetime.utcnow() + expires_delta
        else:
            expire = datetime.utcnow() + timedelta(minutes=self.expire_minutes)

        to_encode.update({
            "exp": expire,
            "iat": datetime.utcnow(),
            "iss": "vietnam-ecommerce-dss",
            "type": "access_token"
        })

        try:
            encoded_jwt = jwt.encode(to_encode, self.secret_key, algorithm=self.algorithm)
            # Handle different JWT library return types
            if isinstance(encoded_jwt, bytes):
                return encoded_jwt.decode('utf-8')
            return encoded_jwt
        except Exception as e:
            # Fallback to mock token if encoding fails
            return f"mock_jwt_token_{data.get('username', 'user')}_{int(datetime.utcnow().timestamp())}"

    def verify_token(self, token: str) -> Optional[Dict[str, Any]]:
        """Verify and decode JWT token"""
        try:
            payload = jwt.decode(
                token,
                self.secret_key,
                algorithms=[self.algorithm],
                options={"verify_exp": True}
            )
            return payload
        except jwt.ExpiredSignatureError:
            return None  # Token expired
        except jwt.InvalidTokenError:
            return None  # Invalid token
        except Exception:
            return None  # Other errors

    def decode_token(self, token: str) -> Optional[Dict[str, Any]]:
        """Decode token without verification (for debugging)"""
        try:
            # Remove Bearer prefix if present
            if token.startswith("Bearer "):
                token = token[7:]

            payload = jwt.decode(
                token,
                options={"verify_signature": False, "verify_exp": False}
            )
            return payload
        except Exception:
            return None

    def is_token_expired(self, token: str) -> bool:
        """Check if token is expired"""
        payload = self.decode_token(token)
        if not payload:
            return True

        exp = payload.get("exp")
        if not exp:
            return True

        return datetime.utcnow() > datetime.fromtimestamp(exp)

    def get_token_info(self, token: str) -> Dict[str, Any]:
        """Get detailed token information"""
        payload = self.decode_token(token)
        if not payload:
            return {"valid": False, "error": "Invalid token"}

        exp = payload.get("exp")
        iat = payload.get("iat")

        info = {
            "valid": True,
            "payload": payload,
            "issued_at": datetime.fromtimestamp(iat).isoformat() if iat else None,
            "expires_at": datetime.fromtimestamp(exp).isoformat() if exp else None,
            "is_expired": self.is_token_expired(token),
            "algorithm": self.algorithm,
            "issuer": payload.get("iss", "unknown")
        }

        return info

class PasswordManager:
    """Password hashing and verification"""

    @staticmethod
    def hash_password(password: str) -> str:
        """Hash a password"""
        if PASSLIB_AVAILABLE:
            return pwd_context.hash(password)
        else:
            # Simple fallback - NOT FOR PRODUCTION!
            return f"plain:{password}"

    @staticmethod
    def verify_password(plain_password: str, hashed_password: str) -> bool:
        """Verify a password against its hash"""
        if PASSLIB_AVAILABLE:
            return pwd_context.verify(plain_password, hashed_password)
        else:
            # Simple fallback - NOT FOR PRODUCTION!
            return hashed_password == f"plain:{plain_password}"

class UserDatabase:
    """Mock user database with hashed passwords"""

    def __init__(self):
        # Pre-hash passwords for better security simulation
        # Note: Using fallback due to bcrypt compatibility issues
        if False:  # Temporarily disable passlib due to bcrypt issues
            # Use proper password hashing
            self.users = {
                "admin": {
                    "password": PasswordManager.hash_password("admin123"),
                    "role": "admin",
                    "full_name": "System Administrator",
                    "email": "admin@dss.com",
                    "permissions": ["read", "write", "delete", "admin"]
                },
                "manager": {
                    "password": PasswordManager.hash_password("manager123"),
                    "role": "manager",
                    "full_name": "Business Manager",
                    "email": "manager@dss.com",
                    "permissions": ["read", "write"]
                },
                "analyst": {
                    "password": PasswordManager.hash_password("analyst123"),
                    "role": "analyst",
                    "full_name": "Data Analyst",
                    "email": "analyst@dss.com",
                    "permissions": ["read"]
                },
                "user": {
                    "password": PasswordManager.hash_password("user123"),
                    "role": "user",
                    "full_name": "Regular User",
                    "email": "user@dss.com",
                    "permissions": ["read"]
                },
                "demo": {
                    "password": PasswordManager.hash_password("demo123"),
                    "role": "manager",
                    "full_name": "Demo Account",
                    "email": "demo@dss.com",
                    "permissions": ["read", "write"]
                }
            }
        else:
            # Fallback to plain passwords (NOT FOR PRODUCTION!)
            self.users = {
                "admin": {
                    "password": "admin123",
                    "role": "admin",
                    "full_name": "System Administrator",
                    "email": "admin@dss.com",
                    "permissions": ["read", "write", "delete", "admin"]
                },
                "manager": {
                    "password": "manager123",
                    "role": "manager",
                    "full_name": "Business Manager",
                    "email": "manager@dss.com",
                    "permissions": ["read", "write"]
                },
                "analyst": {
                    "password": "analyst123",
                    "role": "analyst",
                    "full_name": "Data Analyst",
                    "email": "analyst@dss.com",
                    "permissions": ["read"]
                },
                "user": {
                    "password": "user123",
                    "role": "user",
                    "full_name": "Regular User",
                    "email": "user@dss.com",
                    "permissions": ["read"]
                },
                "demo": {
                    "password": "demo123",
                    "role": "manager",
                    "full_name": "Demo Account",
                    "email": "demo@dss.com",
                    "permissions": ["read", "write"]
                }
            }

    def authenticate_user(self, username: str, password: str) -> Optional[Dict[str, Any]]:
        """Authenticate user with username and password"""
        user = self.users.get(username)
        if not user:
            return None

        # Check password based on available libraries
        if PASSLIB_AVAILABLE:
            if not PasswordManager.verify_password(password, user["password"]):
                return None
        else:
            # Simple password check for fallback
            if user["password"] != password:
                return None

        # Return user info without password
        user_info = user.copy()
        del user_info["password"]
        user_info["username"] = username
        user_info["id"] = f"user_{username}"
        user_info["is_active"] = True

        return user_info

    def get_user(self, username: str) -> Optional[Dict[str, Any]]:
        """Get user by username"""
        user = self.users.get(username)
        if not user:
            return None

        user_info = user.copy()
        del user_info["password"]
        user_info["username"] = username
        user_info["id"] = f"user_{username}"
        user_info["is_active"] = True

        return user_info

# Global instances
jwt_manager = JWTManager()
user_db = UserDatabase()

# Helper functions for FastAPI dependencies
def create_access_token(username: str, role: str) -> str:
    """Create access token for user"""
    data = {
        "sub": username,
        "role": role,
        "username": username
    }
    return jwt_manager.create_access_token(data)

def verify_access_token(token: str) -> Optional[Dict[str, Any]]:
    """Verify access token and return user info"""
    payload = jwt_manager.verify_token(token)
    if not payload:
        return None

    username = payload.get("sub")
    if not username:
        return None

    user = user_db.get_user(username)
    return user

def get_test_credentials() -> list:
    """Get list of test credentials"""
    return [
        {
            "username": "admin",
            "password": "admin123",
            "role": "admin",
            "description": "System Administrator (full access)"
        },
        {
            "username": "manager",
            "password": "manager123",
            "role": "manager",
            "description": "Business Manager (read/write access)"
        },
        {
            "username": "analyst",
            "password": "analyst123",
            "role": "analyst",
            "description": "Data Analyst (read access)"
        },
        {
            "username": "user",
            "password": "user123",
            "role": "user",
            "description": "Regular User (limited access)"
        },
        {
            "username": "demo",
            "password": "demo123",
            "role": "manager",
            "description": "Demo Account (full demo data)"
        }
    ]

# Export key functions
__all__ = [
    "jwt_manager",
    "user_db",
    "create_access_token",
    "verify_access_token",
    "get_test_credentials",
    "PasswordManager",
    "JWTManager"
]