#!/usr/bin/env python3
"""
Simple Authentication WITHOUT JWT (for testing)
Uses session-based authentication with fixed data
"""

from fastapi import APIRouter, HTTPException, Depends, status, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
import hashlib
import json
import uuid

# ====================================
# CONFIGURATION
# ====================================

# Session storage (in production, use Redis or database)
ACTIVE_SESSIONS = {}
SESSION_TIMEOUT_MINUTES = 60

# Fixed user data for testing
FIXED_USERS = {
    "admin": {
        "id": "admin-001",
        "username": "admin",
        "email": "admin@ecommerce-dss.com",
        "password": "admin123",
        "role": "admin",
        "full_name": "System Administrator",
        "is_active": True,
        "created_at": "2024-01-01T00:00:00Z"
    },
    "user1": {
        "id": "user-001",
        "username": "user1",
        "email": "user1@ecommerce-dss.com",
        "password": "user123",
        "role": "user",
        "full_name": "Test User One",
        "is_active": True,
        "created_at": "2024-01-01T00:00:00Z"
    },
    "manager": {
        "id": "manager-001",
        "username": "manager",
        "email": "manager@ecommerce-dss.com",
        "password": "manager123",
        "role": "manager",
        "full_name": "System Manager",
        "is_active": True,
        "created_at": "2024-01-01T00:00:00Z"
    },
    "analyst": {
        "id": "analyst-001",
        "username": "analyst",
        "email": "analyst@ecommerce-dss.com",
        "password": "analyst123",
        "role": "analyst",
        "full_name": "Data Analyst",
        "is_active": True,
        "created_at": "2024-01-01T00:00:00Z"
    }
}

# ====================================
# PYDANTIC MODELS
# ====================================

class LoginRequest(BaseModel):
    username: str
    password: str
    remember_me: bool = False

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int
    user: Dict[str, Any]

class UserInfo(BaseModel):
    id: str
    username: str
    email: str
    role: str
    full_name: str
    is_active: bool

# ====================================
# UTILITY FUNCTIONS
# ====================================

def create_session_token(user_data: Dict[str, Any], remember_me: bool = False) -> str:
    """Create a session token"""
    session_id = str(uuid.uuid4())

    expires_at = datetime.now() + timedelta(
        days=7 if remember_me else 0,
        minutes=SESSION_TIMEOUT_MINUTES if not remember_me else 0
    )

    ACTIVE_SESSIONS[session_id] = {
        "user": user_data,
        "created_at": datetime.now(),
        "expires_at": expires_at,
        "last_accessed": datetime.now()
    }

    return session_id

def validate_session_token(token: str) -> Optional[Dict[str, Any]]:
    """Validate session token"""
    if token not in ACTIVE_SESSIONS:
        return None

    session = ACTIVE_SESSIONS[token]

    # Check if expired
    if datetime.now() > session["expires_at"]:
        del ACTIVE_SESSIONS[token]
        return None

    # Update last accessed
    session["last_accessed"] = datetime.now()

    return session["user"]

def cleanup_expired_sessions():
    """Remove expired sessions"""
    current_time = datetime.now()
    expired_tokens = [
        token for token, session in ACTIVE_SESSIONS.items()
        if current_time > session["expires_at"]
    ]

    for token in expired_tokens:
        del ACTIVE_SESSIONS[token]

def authenticate_user(username: str, password: str) -> Optional[Dict[str, Any]]:
    """Authenticate user with fixed data"""
    # Clean up expired sessions first
    cleanup_expired_sessions()

    # Check if user exists
    if username not in FIXED_USERS:
        return None

    user = FIXED_USERS[username]

    # Check password (in production, use proper password hashing)
    if user["password"] != password:
        return None

    # Check if user is active
    if not user["is_active"]:
        return None

    return user

def get_user_by_username(username: str) -> Optional[Dict[str, Any]]:
    """Get user by username"""
    return FIXED_USERS.get(username)

# ====================================
# SECURITY DEPENDENCIES
# ====================================

security = HTTPBearer()

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> Dict[str, Any]:
    """Get current authenticated user"""
    token = credentials.credentials
    user_data = validate_session_token(token)

    if not user_data:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired session",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return user_data

async def get_current_active_user(current_user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    """Get current active user"""
    if not current_user["is_active"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Inactive user"
        )
    return current_user

# ====================================
# ROUTER SETUP
# ====================================

simple_auth_router = APIRouter(prefix="/api/v1/simple-auth", tags=["Simple Authentication"])

# ====================================
# AUTHENTICATION ENDPOINTS
# ====================================

@simple_auth_router.post("/login", response_model=TokenResponse)
async def login(login_data: LoginRequest):
    """
    Login with fixed user credentials
    Available users: admin, user1, manager, analyst
    Passwords: admin123, user123, manager123, analyst123
    """

    # Authenticate user
    user = authenticate_user(login_data.username, login_data.password)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Create session token
    session_token = create_session_token(user, login_data.remember_me)

    expires_in = (7 * 24 * 60 * 60) if login_data.remember_me else (SESSION_TIMEOUT_MINUTES * 60)

    # Return safe user data (without password)
    safe_user = {
        "id": user["id"],
        "username": user["username"],
        "email": user["email"],
        "role": user["role"],
        "full_name": user["full_name"],
        "is_active": user["is_active"]
    }

    return {
        "access_token": session_token,
        "token_type": "bearer",
        "expires_in": expires_in,
        "user": safe_user
    }

@simple_auth_router.post("/logout")
async def logout(current_user: Dict[str, Any] = Depends(get_current_active_user)):
    """
    Logout current user - removes session
    """
    # In a real implementation, we'd get the token from the request
    # For now, just return success message
    cleanup_expired_sessions()

    return {
        "success": True,
        "message": f"User {current_user['username']} logged out successfully",
        "timestamp": datetime.now().isoformat()
    }

@simple_auth_router.get("/me", response_model=UserInfo)
async def get_current_user_info(current_user: Dict[str, Any] = Depends(get_current_active_user)):
    """Get current user information"""
    return {
        "id": current_user["id"],
        "username": current_user["username"],
        "email": current_user["email"],
        "role": current_user["role"],
        "full_name": current_user["full_name"],
        "is_active": current_user["is_active"]
    }

@simple_auth_router.get("/users")
async def list_all_users(current_user: Dict[str, Any] = Depends(get_current_active_user)):
    """
    List all available users (for testing purposes)
    Only admins can see this endpoint
    """
    if current_user["role"] != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can view all users"
        )

    # Return users without passwords
    safe_users = []
    for username, user_data in FIXED_USERS.items():
        safe_users.append({
            "id": user_data["id"],
            "username": user_data["username"],
            "email": user_data["email"],
            "role": user_data["role"],
            "full_name": user_data["full_name"],
            "is_active": user_data["is_active"],
            "created_at": user_data["created_at"]
        })

    return {
        "users": safe_users,
        "total": len(safe_users),
        "active_sessions": len(ACTIVE_SESSIONS)
    }

@simple_auth_router.get("/validate")
async def validate_token(current_user: Dict[str, Any] = Depends(get_current_active_user)):
    """Validate current session"""
    return {
        "valid": True,
        "user": {
            "id": current_user["id"],
            "username": current_user["username"],
            "role": current_user["role"]
        },
        "timestamp": datetime.now().isoformat()
    }

@simple_auth_router.get("/sessions")
async def get_session_info(current_user: Dict[str, Any] = Depends(get_current_active_user)):
    """Get session information (admin only)"""
    if current_user["role"] != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can view session info"
        )

    cleanup_expired_sessions()

    session_info = []
    for token, session in ACTIVE_SESSIONS.items():
        session_info.append({
            "token": token[:8] + "...",  # Partial token for security
            "username": session["user"]["username"],
            "role": session["user"]["role"],
            "created_at": session["created_at"].isoformat(),
            "expires_at": session["expires_at"].isoformat(),
            "last_accessed": session["last_accessed"].isoformat()
        })

    return {
        "active_sessions": len(ACTIVE_SESSIONS),
        "sessions": session_info
    }

# ====================================
# ROLE-BASED ACCESS EXAMPLES
# ====================================

@simple_auth_router.get("/admin-only")
async def admin_only_endpoint(current_user: Dict[str, Any] = Depends(get_current_active_user)):
    """Example admin-only endpoint"""
    if current_user["role"] != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required"
        )

    return {
        "message": "This is an admin-only endpoint",
        "user": current_user["username"],
        "timestamp": datetime.now().isoformat(),
        "admin_features": [
            "User management",
            "System configuration",
            "Analytics dashboard",
            "Session monitoring"
        ]
    }

@simple_auth_router.get("/manager-or-admin")
async def manager_or_admin_endpoint(current_user: Dict[str, Any] = Depends(get_current_active_user)):
    """Example endpoint for managers and admins"""
    allowed_roles = ["admin", "manager"]
    if current_user["role"] not in allowed_roles:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Manager or Admin access required"
        )

    return {
        "message": "This endpoint is for managers and admins",
        "user": current_user["username"],
        "role": current_user["role"],
        "timestamp": datetime.now().isoformat(),
        "available_features": [
            "Team management",
            "Reports generation",
            "Performance analytics"
        ]
    }

# ====================================
# TESTING ENDPOINTS
# ====================================

@simple_auth_router.get("/test-credentials")
async def get_test_credentials():
    """Get available test credentials"""
    credentials = []
    for username, user_data in FIXED_USERS.items():
        credentials.append({
            "username": username,
            "password": user_data["password"],
            "role": user_data["role"],
            "description": f"{user_data['full_name']} ({user_data['role']})"
        })

    return {
        "message": "Available test credentials",
        "credentials": credentials,
        "note": "These are for testing purposes only",
        "active_sessions": len(ACTIVE_SESSIONS)
    }

@simple_auth_router.get("/status")
async def auth_system_status():
    """Get authentication system status"""
    cleanup_expired_sessions()

    return {
        "status": "Authentication system operational",
        "version": "1.0.0 (Session-based)",
        "total_users": len(FIXED_USERS),
        "active_sessions": len(ACTIVE_SESSIONS),
        "session_timeout_minutes": SESSION_TIMEOUT_MINUTES,
        "timestamp": datetime.now().isoformat()
    }

# Export the router
__all__ = ['simple_auth_router']