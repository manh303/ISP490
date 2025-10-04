#!/usr/bin/env python3
"""
Advanced Security Module for E-commerce DSS
Handles authentication, authorization, rate limiting, and security monitoring
"""

import os
import jwt
import redis
import hashlib
import secrets
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from functools import wraps

from fastapi import HTTPException, Request, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from passlib.context import CryptContext
import asyncio
from sqlalchemy import text

# Database
from databases import Database

# ====================================
# SECURITY CONFIGURATION
# ====================================

class SecurityConfig:
    """Security configuration settings"""

    # JWT Settings
    JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your-super-secret-jwt-key-change-this-in-production")
    JWT_ALGORITHM = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES = 30
    REFRESH_TOKEN_EXPIRE_DAYS = 7

    # Password Settings
    PWD_CONTEXT = CryptContext(schemes=["bcrypt"], deprecated="auto")
    MIN_PASSWORD_LENGTH = 8

    # Rate Limiting
    RATE_LIMIT_REQUESTS = 100
    RATE_LIMIT_WINDOW = 60  # seconds

    # API Key Settings
    API_KEY_LENGTH = 32
    API_KEY_PREFIX = "dss_"

    # Security Headers
    SECURITY_HEADERS = {
        "X-Content-Type-Options": "nosniff",
        "X-Frame-Options": "DENY",
        "X-XSS-Protection": "1; mode=block",
        "Strict-Transport-Security": "max-age=31536000; includeSubDomains",
        "Content-Security-Policy": "default-src 'self'"
    }

security_config = SecurityConfig()

# ====================================
# PASSWORD UTILITIES
# ====================================

class PasswordManager:
    """Password hashing and verification utilities"""

    @staticmethod
    def hash_password(password: str) -> str:
        """Hash a password using bcrypt"""
        return security_config.PWD_CONTEXT.hash(password)

    @staticmethod
    def verify_password(plain_password: str, hashed_password: str) -> bool:
        """Verify a password against its hash"""
        return security_config.PWD_CONTEXT.verify(plain_password, hashed_password)

    @staticmethod
    def validate_password_strength(password: str) -> Dict[str, Any]:
        """Validate password strength"""
        issues = []
        score = 0

        if len(password) < security_config.MIN_PASSWORD_LENGTH:
            issues.append(f"Password must be at least {security_config.MIN_PASSWORD_LENGTH} characters")
        else:
            score += 1

        if not any(c.islower() for c in password):
            issues.append("Password must contain lowercase letters")
        else:
            score += 1

        if not any(c.isupper() for c in password):
            issues.append("Password must contain uppercase letters")
        else:
            score += 1

        if not any(c.isdigit() for c in password):
            issues.append("Password must contain numbers")
        else:
            score += 1

        if not any(c in "!@#$%^&*()_+-=[]{}|;:,.<>?" for c in password):
            issues.append("Password must contain special characters")
        else:
            score += 1

        strength = "weak"
        if score >= 4:
            strength = "strong"
        elif score >= 3:
            strength = "medium"

        return {
            "valid": len(issues) == 0,
            "strength": strength,
            "score": score,
            "issues": issues
        }

# ====================================
# JWT TOKEN MANAGER
# ====================================

class JWTManager:
    """JWT token creation and validation"""

    @staticmethod
    def create_access_token(data: Dict[str, Any], expires_delta: Optional[timedelta] = None) -> str:
        """Create JWT access token"""
        to_encode = data.copy()

        if expires_delta:
            expire = datetime.utcnow() + expires_delta
        else:
            expire = datetime.utcnow() + timedelta(minutes=security_config.ACCESS_TOKEN_EXPIRE_MINUTES)

        to_encode.update({
            "exp": expire,
            "iat": datetime.utcnow(),
            "type": "access"
        })

        return jwt.encode(to_encode, security_config.JWT_SECRET_KEY, algorithm=security_config.JWT_ALGORITHM)

    @staticmethod
    def create_refresh_token(user_id: str) -> str:
        """Create JWT refresh token"""
        to_encode = {
            "user_id": user_id,
            "exp": datetime.utcnow() + timedelta(days=security_config.REFRESH_TOKEN_EXPIRE_DAYS),
            "iat": datetime.utcnow(),
            "type": "refresh"
        }

        return jwt.encode(to_encode, security_config.JWT_SECRET_KEY, algorithm=security_config.JWT_ALGORITHM)

    @staticmethod
    def verify_token(token: str) -> Dict[str, Any]:
        """Verify and decode JWT token"""
        try:
            payload = jwt.decode(token, security_config.JWT_SECRET_KEY, algorithms=[security_config.JWT_ALGORITHM])
            return payload
        except jwt.ExpiredSignatureError:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token has expired",
                headers={"WWW-Authenticate": "Bearer"}
            )
        except jwt.JWTError:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Could not validate credentials",
                headers={"WWW-Authenticate": "Bearer"}
            )

# ====================================
# API KEY MANAGER
# ====================================

class APIKeyManager:
    """API key generation and validation"""

    @staticmethod
    def generate_api_key() -> str:
        """Generate a new API key"""
        key = secrets.token_urlsafe(security_config.API_KEY_LENGTH)
        return f"{security_config.API_KEY_PREFIX}{key}"

    @staticmethod
    def hash_api_key(api_key: str) -> str:
        """Hash API key for storage"""
        return hashlib.sha256(api_key.encode()).hexdigest()

    @staticmethod
    async def validate_api_key(api_key: str, db: Database) -> Dict[str, Any]:
        """Validate API key against database"""
        hashed_key = APIKeyManager.hash_api_key(api_key)

        query = text("""
            SELECT ak.*, u.username, u.email, u.role
            FROM api_keys ak
            JOIN users u ON ak.user_id = u.id
            WHERE ak.key_hash = :key_hash
            AND ak.is_active = true
            AND (ak.expires_at IS NULL OR ak.expires_at > NOW())
        """)

        result = await db.fetch_one(query, {"key_hash": hashed_key})

        if not result:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid API key"
            )

        # Update last used timestamp
        update_query = text("""
            UPDATE api_keys
            SET last_used_at = NOW(), usage_count = usage_count + 1
            WHERE id = :key_id
        """)
        await db.execute(update_query, {"key_id": result["id"]})

        return dict(result)

# ====================================
# RATE LIMITING
# ====================================

class RateLimiter:
    """Redis-based rate limiting"""

    def __init__(self, redis_client):
        self.redis = redis_client

    async def is_allowed(self, key: str, limit: int = None, window: int = None) -> Dict[str, Any]:
        """Check if request is allowed based on rate limit"""
        limit = limit or security_config.RATE_LIMIT_REQUESTS
        window = window or security_config.RATE_LIMIT_WINDOW

        pipe = self.redis.pipeline()
        pipe.incr(key)
        pipe.expire(key, window)
        results = await pipe.execute()

        current_requests = results[0]

        return {
            "allowed": current_requests <= limit,
            "current_requests": current_requests,
            "limit": limit,
            "window": window,
            "reset_time": window,
            "remaining": max(0, limit - current_requests)
        }

    async def get_rate_limit_key(self, request: Request, user_id: str = None) -> str:
        """Generate rate limit key"""
        if user_id:
            return f"rate_limit:user:{user_id}"

        # Use IP address as fallback
        client_ip = request.client.host
        return f"rate_limit:ip:{client_ip}"

# ====================================
# SECURITY MIDDLEWARE
# ====================================

class SecurityMiddleware:
    """Security middleware for FastAPI"""

    def __init__(self, app, redis_client):
        self.app = app
        self.rate_limiter = RateLimiter(redis_client)

    async def __call__(self, request: Request, call_next):
        """Process security middleware"""

        # Add security headers
        response = await call_next(request)
        for header, value in security_config.SECURITY_HEADERS.items():
            response.headers[header] = value

        return response

# ====================================
# AUTHENTICATION DEPENDENCIES
# ====================================

class AuthenticationService:
    """Authentication service for FastAPI dependencies"""

    def __init__(self, db: Database, redis_client):
        self.db = db
        self.redis = redis_client
        self.rate_limiter = RateLimiter(redis_client)
        self.security = HTTPBearer()

    async def get_current_user(self, credentials: HTTPAuthorizationCredentials = Depends(HTTPBearer())) -> Dict[str, Any]:
        """Get current authenticated user"""
        token = credentials.credentials
        payload = JWTManager.verify_token(token)

        if payload.get("type") != "access":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token type"
            )

        user_id = payload.get("user_id")
        if not user_id:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token payload"
            )

        # Get user from database
        query = text("""
            SELECT id, username, email, role, is_active, created_at, last_login
            FROM users
            WHERE id = :user_id AND is_active = true
        """)

        user = await self.db.fetch_one(query, {"user_id": user_id})

        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User not found or inactive"
            )

        return dict(user)

    async def get_current_user_with_api_key(self, request: Request) -> Dict[str, Any]:
        """Authenticate user with API key"""
        api_key = request.headers.get("X-API-Key")

        if not api_key:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="API key required"
            )

        return await APIKeyManager.validate_api_key(api_key, self.db)

    async def require_role(self, required_roles: List[str]):
        """Dependency to require specific roles"""
        def role_checker(current_user: Dict[str, Any] = Depends(self.get_current_user)):
            user_role = current_user.get("role")
            if user_role not in required_roles:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Insufficient permissions. Required: {required_roles}"
                )
            return current_user
        return role_checker

    async def apply_rate_limit(self, request: Request, user: Dict[str, Any] = None):
        """Apply rate limiting"""
        rate_limit_key = await self.rate_limiter.get_rate_limit_key(
            request,
            user.get("id") if user else None
        )

        rate_limit_result = await self.rate_limiter.is_allowed(rate_limit_key)

        if not rate_limit_result["allowed"]:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Rate limit exceeded",
                headers={
                    "X-RateLimit-Limit": str(rate_limit_result["limit"]),
                    "X-RateLimit-Remaining": str(rate_limit_result["remaining"]),
                    "X-RateLimit-Reset": str(rate_limit_result["reset_time"])
                }
            )

        return rate_limit_result

# ====================================
# SECURITY MONITORING
# ====================================

class SecurityMonitor:
    """Security event monitoring and logging"""

    def __init__(self, redis_client):
        self.redis = redis_client

    async def log_security_event(self, event_type: str, details: Dict[str, Any]):
        """Log security events"""
        event = {
            "type": event_type,
            "timestamp": datetime.utcnow().isoformat(),
            "details": details
        }

        # Store in Redis for real-time monitoring
        await self.redis.lpush("security_events", str(event))
        await self.redis.ltrim("security_events", 0, 1000)  # Keep last 1000 events

    async def get_security_stats(self) -> Dict[str, Any]:
        """Get security statistics"""
        events = await self.redis.lrange("security_events", 0, -1)

        stats = {
            "total_events": len(events),
            "event_types": {},
            "recent_events": events[:10] if events else [],
            "generated_at": datetime.utcnow().isoformat()
        }

        # Count event types
        for event_str in events:
            try:
                event = eval(event_str)  # In production, use json.loads
                event_type = event.get("type", "unknown")
                stats["event_types"][event_type] = stats["event_types"].get(event_type, 0) + 1
            except:
                continue

        return stats

# ====================================
# UTILITY FUNCTIONS
# ====================================

def create_database_tables():
    """SQL to create security-related database tables"""
    return """
    -- Users table
    CREATE TABLE IF NOT EXISTS users (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        username VARCHAR(50) UNIQUE NOT NULL,
        email VARCHAR(100) UNIQUE NOT NULL,
        password_hash VARCHAR(255) NOT NULL,
        role VARCHAR(20) DEFAULT 'user',
        is_active BOOLEAN DEFAULT true,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_login TIMESTAMP,
        failed_login_attempts INTEGER DEFAULT 0,
        locked_until TIMESTAMP
    );

    -- API Keys table
    CREATE TABLE IF NOT EXISTS api_keys (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id UUID REFERENCES users(id) ON DELETE CASCADE,
        name VARCHAR(100) NOT NULL,
        key_hash VARCHAR(64) NOT NULL UNIQUE,
        is_active BOOLEAN DEFAULT true,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        expires_at TIMESTAMP,
        last_used_at TIMESTAMP,
        usage_count INTEGER DEFAULT 0,
        permissions JSONB DEFAULT '{}'
    );

    -- Security Events table
    CREATE TABLE IF NOT EXISTS security_events (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        event_type VARCHAR(50) NOT NULL,
        user_id UUID REFERENCES users(id),
        ip_address INET,
        user_agent TEXT,
        details JSONB,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Create indexes
    CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
    CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
    CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash);
    CREATE INDEX IF NOT EXISTS idx_security_events_type ON security_events(event_type);
    CREATE INDEX IF NOT EXISTS idx_security_events_created_at ON security_events(created_at);
    """

# Export main classes
__all__ = [
    'SecurityConfig', 'PasswordManager', 'JWTManager', 'APIKeyManager',
    'RateLimiter', 'SecurityMiddleware', 'AuthenticationService',
    'SecurityMonitor', 'create_database_tables'
]