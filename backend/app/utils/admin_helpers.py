"""
Admin helper functions (cleaned up)
"""
from typing import Dict, Any, Optional
from fastapi import Request, HTTPException
import logging

logger = logging.getLogger(__name__)

def get_current_admin_user(request: Request) -> Dict[str, Any]:
    """Validate admin access from JWT token"""
    from utils.auth_helpers import decode_access_token
    from main import settings
    
    # Get Authorization header
    auth_header = request.headers.get("authorization") or request.headers.get("Authorization")
    if not auth_header:
        raise HTTPException(status_code=401, detail="Authorization header required")
    
    # Extract token
    try:
        token = auth_header.split()[1]  # Remove "Bearer " prefix
    except (IndexError, AttributeError):
        raise HTTPException(status_code=401, detail="Invalid authorization format")
    
    # Decode token
    payload = decode_access_token(token, settings.JWT_SECRET_KEY, settings.JWT_ALGORITHM)
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    
    # Check admin role
    user_role = payload.get("role")
    if user_role != "ADMIN":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    return {
        "user_id": payload.get("user_id"),
        "email": payload.get("email"),
        "role": user_role,
        "full_name": payload.get("full_name", "Admin User")
    }

def format_user_response(user: Dict[str, Any]) -> Dict[str, Any]:
    """Format user data for API response"""
    return {
        "user_id": user.get("user_id"),
        "email": user.get("email"),
        "full_name": user.get("full_name"),
        "phone": user.get("phone"),
        "status": user.get("status", "active"),
        "role_code": user.get("role_code"),
        "role_name": user.get("role_name"),
        "last_login_at": user.get("last_login_at"),
        "created_at": user.get("created_at"),
        "updated_at": user.get("updated_at")
    }