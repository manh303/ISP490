#!/usr/bin/env python3
"""
Authentication and Authorization Endpoints
Handles user registration, login, API key management, and security
"""

from fastapi import APIRouter, HTTPException, Depends, Request, status, BackgroundTasks
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field, EmailStr
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import secrets
import uuid

# Database
from databases import Database
from sqlalchemy import text

# Security
try:
    from .security import (
        PasswordManager, JWTManager, APIKeyManager, SecurityMonitor,
        SecurityConfig, AuthenticationService, create_database_tables
    )
except ImportError:
    from security import (
        PasswordManager, JWTManager, APIKeyManager, SecurityMonitor,
        SecurityConfig, AuthenticationService, create_database_tables
    )

# ====================================
# PYDANTIC MODELS
# ====================================

class UserRegistration(BaseModel):
    username: str = Field(..., min_length=3, max_length=50)
    email: EmailStr = Field(...)
    password: str = Field(..., min_length=8)
    full_name: Optional[str] = Field(None, max_length=100)
    role: Optional[str] = Field(default="user")

class UserLogin(BaseModel):
    username: str = Field(...)
    password: str = Field(...)
    remember_me: bool = Field(default=False)

class PasswordChange(BaseModel):
    current_password: str = Field(...)
    new_password: str = Field(..., min_length=8)

class APIKeyCreate(BaseModel):
    name: str = Field(..., max_length=100)
    expires_in_days: Optional[int] = Field(default=None, ge=1, le=365)
    permissions: Optional[Dict[str, Any]] = Field(default={})

class TokenRefresh(BaseModel):
    refresh_token: str = Field(...)

# ====================================
# ROUTER SETUP
# ====================================

auth_router = APIRouter(prefix="/api/v1/auth", tags=["Authentication"])
security = HTTPBearer()

# ====================================
# DEPENDENCY INJECTION
# ====================================

async def get_auth_service(request: Request) -> AuthenticationService:
    """Get authentication service instance"""
    # This would be injected in main app
    return getattr(request.app.state, 'auth_service', None)

# ====================================
# PUBLIC ENDPOINTS
# ====================================

@auth_router.post("/register")
async def register_user(
    user_data: UserRegistration,
    request: Request,
    background_tasks: BackgroundTasks,
    db: Database = Depends(lambda: None)  # Will be properly injected
) -> Dict[str, Any]:
    """
    Register a new user account
    """
    try:
        # Validate password strength
        password_validation = PasswordManager.validate_password_strength(user_data.password)
        if not password_validation["valid"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "message": "Password does not meet requirements",
                    "issues": password_validation["issues"]
                }
            )

        # Check if user already exists
        existing_user_query = text("""
            SELECT id FROM users
            WHERE username = :username OR email = :email
        """)

        existing_user = await db.fetch_one(existing_user_query, {
            "username": user_data.username,
            "email": user_data.email
        })

        if existing_user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Username or email already registered"
            )

        # Hash password
        password_hash = PasswordManager.hash_password(user_data.password)

        # Create user
        user_id = str(uuid.uuid4())
        create_user_query = text("""
            INSERT INTO users (id, username, email, password_hash, role, full_name)
            VALUES (:id, :username, :email, :password_hash, :role, :full_name)
            RETURNING id, username, email, role, created_at
        """)

        new_user = await db.fetch_one(create_user_query, {
            "id": user_id,
            "username": user_data.username,
            "email": user_data.email,
            "password_hash": password_hash,
            "role": user_data.role,
            "full_name": user_data.full_name
        })

        # Log security event
        security_monitor = SecurityMonitor(request.app.state.redis_client)
        await security_monitor.log_security_event("user_registration", {
            "user_id": user_id,
            "username": user_data.username,
            "email": user_data.email,
            "ip_address": request.client.host
        })

        # Create tokens
        access_token = JWTManager.create_access_token({"user_id": user_id})
        refresh_token = JWTManager.create_refresh_token(user_id)

        return {
            "success": True,
            "message": "User registered successfully",
            "user": dict(new_user),
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": "bearer"
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Registration failed: {str(e)}"
        )

@auth_router.post("/login")
async def login_user(
    login_data: UserLogin,
    request: Request,
    background_tasks: BackgroundTasks,
    db: Database = Depends(lambda: None)
) -> Dict[str, Any]:
    """
    Authenticate user and return tokens
    """
    try:
        # Get user from database
        user_query = text("""
            SELECT id, username, email, password_hash, role, is_active,
                   failed_login_attempts, locked_until
            FROM users
            WHERE username = :username OR email = :username
        """)

        user = await db.fetch_one(user_query, {"username": login_data.username})

        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid username or password"
            )

        # Check if account is locked
        if user["locked_until"] and user["locked_until"] > datetime.utcnow():
            raise HTTPException(
                status_code=status.HTTP_423_LOCKED,
                detail="Account is temporarily locked due to too many failed attempts"
            )

        # Check if account is active
        if not user["is_active"]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Account is disabled"
            )

        # Verify password
        if not PasswordManager.verify_password(login_data.password, user["password_hash"]):
            # Increment failed attempts
            await db.execute(text("""
                UPDATE users
                SET failed_login_attempts = failed_login_attempts + 1,
                    locked_until = CASE
                        WHEN failed_login_attempts >= 4 THEN NOW() + INTERVAL '30 minutes'
                        ELSE locked_until
                    END
                WHERE id = :user_id
            """), {"user_id": user["id"]})

            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid username or password"
            )

        # Reset failed attempts and update last login
        await db.execute(text("""
            UPDATE users
            SET failed_login_attempts = 0, locked_until = NULL, last_login = NOW()
            WHERE id = :user_id
        """), {"user_id": user["id"]})

        # Create tokens
        token_data = {"user_id": str(user["id"])}

        if login_data.remember_me:
            access_token_expires = timedelta(days=7)
        else:
            access_token_expires = timedelta(minutes=SecurityConfig.ACCESS_TOKEN_EXPIRE_MINUTES)

        access_token = JWTManager.create_access_token(token_data, access_token_expires)
        refresh_token = JWTManager.create_refresh_token(str(user["id"]))

        # Log security event
        security_monitor = SecurityMonitor(request.app.state.redis_client)
        await security_monitor.log_security_event("user_login", {
            "user_id": str(user["id"]),
            "username": user["username"],
            "ip_address": request.client.host,
            "remember_me": login_data.remember_me
        })

        return {
            "success": True,
            "message": "Login successful",
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": "bearer",
            "expires_in": int(access_token_expires.total_seconds()),
            "user": {
                "id": str(user["id"]),
                "username": user["username"],
                "email": user["email"],
                "role": user["role"]
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Login failed: {str(e)}"
        )

@auth_router.post("/refresh")
async def refresh_token(
    refresh_data: TokenRefresh,
    request: Request,
    db: Database = Depends(lambda: None)
) -> Dict[str, Any]:
    """
    Refresh access token using refresh token
    """
    try:
        # Verify refresh token
        payload = JWTManager.verify_token(refresh_data.refresh_token)

        if payload.get("type") != "refresh":
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

        # Verify user still exists and is active
        user_query = text("""
            SELECT id, username, email, role, is_active
            FROM users
            WHERE id = :user_id AND is_active = true
        """)

        user = await db.fetch_one(user_query, {"user_id": user_id})

        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User not found or inactive"
            )

        # Create new access token
        new_access_token = JWTManager.create_access_token({"user_id": user_id})

        # Log security event
        security_monitor = SecurityMonitor(request.app.state.redis_client)
        await security_monitor.log_security_event("token_refresh", {
            "user_id": user_id,
            "ip_address": request.client.host
        })

        return {
            "success": True,
            "access_token": new_access_token,
            "token_type": "bearer",
            "expires_in": SecurityConfig.ACCESS_TOKEN_EXPIRE_MINUTES * 60
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Token refresh failed: {str(e)}"
        )

# ====================================
# PROTECTED ENDPOINTS
# ====================================

@auth_router.get("/me")
async def get_current_user_info(
    request: Request,
    auth_service: AuthenticationService = Depends(get_auth_service),
    current_user: Dict[str, Any] = Depends(lambda: None)  # Will be properly injected
) -> Dict[str, Any]:
    """
    Get current user information
    """
    return {
        "success": True,
        "user": current_user,
        "timestamp": datetime.utcnow().isoformat()
    }

@auth_router.post("/change-password")
async def change_password(
    password_data: PasswordChange,
    request: Request,
    current_user: Dict[str, Any] = Depends(lambda: None),
    db: Database = Depends(lambda: None)
) -> Dict[str, Any]:
    """
    Change user password
    """
    try:
        # Get current password hash
        user_query = text("SELECT password_hash FROM users WHERE id = :user_id")
        user = await db.fetch_one(user_query, {"user_id": current_user["id"]})

        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )

        # Verify current password
        if not PasswordManager.verify_password(password_data.current_password, user["password_hash"]):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Current password is incorrect"
            )

        # Validate new password
        password_validation = PasswordManager.validate_password_strength(password_data.new_password)
        if not password_validation["valid"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "message": "New password does not meet requirements",
                    "issues": password_validation["issues"]
                }
            )

        # Hash new password
        new_password_hash = PasswordManager.hash_password(password_data.new_password)

        # Update password
        await db.execute(text("""
            UPDATE users
            SET password_hash = :password_hash,
                password_changed_at = NOW()
            WHERE id = :user_id
        """), {
            "password_hash": new_password_hash,
            "user_id": current_user["id"]
        })

        # Log security event
        security_monitor = SecurityMonitor(request.app.state.redis_client)
        await security_monitor.log_security_event("password_change", {
            "user_id": current_user["id"],
            "ip_address": request.client.host
        })

        return {
            "success": True,
            "message": "Password changed successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Password change failed: {str(e)}"
        )

# ====================================
# API KEY MANAGEMENT
# ====================================

@auth_router.post("/api-keys")
async def create_api_key(
    key_data: APIKeyCreate,
    request: Request,
    current_user: Dict[str, Any] = Depends(lambda: None),
    db: Database = Depends(lambda: None)
) -> Dict[str, Any]:
    """
    Create a new API key for the user
    """
    try:
        # Generate API key
        api_key = APIKeyManager.generate_api_key()
        key_hash = APIKeyManager.hash_api_key(api_key)

        # Calculate expiry date
        expires_at = None
        if key_data.expires_in_days:
            expires_at = datetime.utcnow() + timedelta(days=key_data.expires_in_days)

        # Save to database
        key_id = str(uuid.uuid4())
        create_key_query = text("""
            INSERT INTO api_keys (id, user_id, name, key_hash, expires_at, permissions)
            VALUES (:id, :user_id, :name, :key_hash, :expires_at, :permissions)
            RETURNING id, name, created_at, expires_at
        """)

        new_key = await db.fetch_one(create_key_query, {
            "id": key_id,
            "user_id": current_user["id"],
            "name": key_data.name,
            "key_hash": key_hash,
            "expires_at": expires_at,
            "permissions": key_data.permissions or {}
        })

        # Log security event
        security_monitor = SecurityMonitor(request.app.state.redis_client)
        await security_monitor.log_security_event("api_key_created", {
            "user_id": current_user["id"],
            "key_id": key_id,
            "key_name": key_data.name,
            "ip_address": request.client.host
        })

        return {
            "success": True,
            "message": "API key created successfully",
            "api_key": api_key,  # Only shown once!
            "key_info": dict(new_key),
            "warning": "Save this API key securely. It will not be shown again."
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"API key creation failed: {str(e)}"
        )

@auth_router.get("/api-keys")
async def list_api_keys(
    current_user: Dict[str, Any] = Depends(lambda: None),
    db: Database = Depends(lambda: None)
) -> Dict[str, Any]:
    """
    List user's API keys (without the actual keys)
    """
    try:
        keys_query = text("""
            SELECT id, name, is_active, created_at, expires_at,
                   last_used_at, usage_count, permissions
            FROM api_keys
            WHERE user_id = :user_id
            ORDER BY created_at DESC
        """)

        keys = await db.fetch_all(keys_query, {"user_id": current_user["id"]})

        return {
            "success": True,
            "api_keys": [dict(key) for key in keys],
            "total": len(keys)
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"API key listing failed: {str(e)}"
        )

@auth_router.delete("/api-keys/{key_id}")
async def revoke_api_key(
    key_id: str,
    request: Request,
    current_user: Dict[str, Any] = Depends(lambda: None),
    db: Database = Depends(lambda: None)
) -> Dict[str, Any]:
    """
    Revoke (delete) an API key
    """
    try:
        # Verify key belongs to user
        key_query = text("""
            SELECT id, name FROM api_keys
            WHERE id = :key_id AND user_id = :user_id
        """)

        key = await db.fetch_one(key_query, {
            "key_id": key_id,
            "user_id": current_user["id"]
        })

        if not key:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="API key not found"
            )

        # Delete key
        await db.execute(text("""
            DELETE FROM api_keys
            WHERE id = :key_id AND user_id = :user_id
        """), {
            "key_id": key_id,
            "user_id": current_user["id"]
        })

        # Log security event
        security_monitor = SecurityMonitor(request.app.state.redis_client)
        await security_monitor.log_security_event("api_key_revoked", {
            "user_id": current_user["id"],
            "key_id": key_id,
            "key_name": key["name"],
            "ip_address": request.client.host
        })

        return {
            "success": True,
            "message": "API key revoked successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"API key revocation failed: {str(e)}"
        )

# ====================================
# SECURITY MONITORING
# ====================================

@auth_router.get("/security/events")
async def get_security_events(
    request: Request,
    current_user: Dict[str, Any] = Depends(lambda: None)
) -> Dict[str, Any]:
    """
    Get security events (admin only)
    """
    if current_user.get("role") != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required"
        )

    try:
        security_monitor = SecurityMonitor(request.app.state.redis_client)
        stats = await security_monitor.get_security_stats()

        return {
            "success": True,
            "security_stats": stats,
            "timestamp": datetime.utcnow().isoformat()
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Security events retrieval failed: {str(e)}"
        )

# Export router
__all__ = ['auth_router']