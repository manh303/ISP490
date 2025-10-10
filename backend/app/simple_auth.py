#!/usr/bin/env python3
"""
Simple Authentication with Fixed Data
For testing and development purposes
"""

from fastapi import APIRouter, HTTPException, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
import jwt  # PyJWT library
import hashlib
import json

# ====================================
# CONFIGURATION
# ====================================

SECRET_KEY = "ecommerce-dss-secret-key-2024"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

# Fixed user data for testing
FIXED_USERS = {
    "admin": {
        "id": "admin-001",
        "username": "admin",
        "email": "admin@ecommerce-dss.com",
        "password": "admin123",  # In production, this would be hashed
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

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    """Create JWT access token"""
    to_encode = data.copy()

    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def verify_token(token: str) -> Dict[str, Any]:
    """Verify and decode JWT token"""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid authentication credentials",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return payload
    except jwt.PyJWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

def authenticate_user(username: str, password: str) -> Optional[Dict[str, Any]]:
    """Authenticate user with fixed data"""
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
    payload = verify_token(token)

    username = payload.get("sub")
    user = get_user_by_username(username)

    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return user

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

    # Create access token
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    if login_data.remember_me:
        access_token_expires = timedelta(days=7)

    access_token = create_access_token(
        data={"sub": user["username"], "role": user["role"]},
        expires_delta=access_token_expires
    )

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
        "access_token": access_token,
        "token_type": "bearer",
        "expires_in": int(access_token_expires.total_seconds()),
        "user": safe_user
    }

@simple_auth_router.post("/logout")
async def logout(current_user: Dict[str, Any] = Depends(get_current_active_user)):
    """
    Logout current user
    Note: JWT tokens are stateless, so this is mainly for client-side cleanup
    """
    return {
        "success": True,
        "message": f"User {current_user['username']} logged out successfully",
        "timestamp": datetime.utcnow().isoformat()
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
        "total": len(safe_users)
    }

@simple_auth_router.get("/validate")
async def validate_token(current_user: Dict[str, Any] = Depends(get_current_active_user)):
    """Validate current token"""
    return {
        "valid": True,
        "user": {
            "id": current_user["id"],
            "username": current_user["username"],
            "role": current_user["role"]
        },
        "timestamp": datetime.utcnow().isoformat()
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
        "timestamp": datetime.utcnow().isoformat()
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
        "timestamp": datetime.utcnow().isoformat()
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
        "note": "These are for testing purposes only"
    }

# Export the router
__all__ = ['simple_auth_router']