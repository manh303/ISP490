"""
Admin Helper Functions
"""
from typing import Dict, Any, Optional
from fastapi import HTTPException, Request
import jwt
import os

def get_current_admin_user(request: Request) -> Dict[str, Any]:
    """Extract and validate admin user from JWT token"""
    auth_header = request.headers.get("authorization") or request.headers.get("Authorization")
    if not auth_header:
        raise HTTPException(status_code=401, detail="Authorization header missing")

    try:
        token = auth_header.split()[1]
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid authorization format")

    try:
        secret_key = os.getenv("JWT_SECRET_KEY", "KvyFNJHBkDzgAwCsx659EvNCa9tWUsOlIKpoQZztIyg")
        payload = jwt.decode(token, secret_key, algorithms=["HS256"])
        
        # Check if user has admin role
        user_role = payload.get("role")
        if user_role != "ADMIN":
            raise HTTPException(status_code=403, detail="Admin access required")
            
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

def validate_role_code(role_code: str) -> bool:
    """Validate role code"""
    valid_roles = ["ADMIN", "ANALYST", "CUSTOMER", "MANAGER"]
    return role_code in valid_roles

def format_user_response(user_data: Dict[str, Any]) -> Dict[str, Any]:
    """Format user data for API response"""
    return {
        "user_id": user_data.get("user_id"),
        "email": user_data.get("email"),
        "full_name": user_data.get("full_name"),
        "phone": user_data.get("phone"),
        "status": user_data.get("status"),
        "role_code": user_data.get("role_code"),
        "role_name": user_data.get("role_name"),
        "last_login_at": user_data.get("last_login_at"),
        "created_at": user_data.get("created_at"),
        "updated_at": user_data.get("updated_at")
    }