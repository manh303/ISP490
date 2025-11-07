"""
Admin Helper Functions
"""
from typing import Dict, Any, Optional
from fastapi import HTTPException, Request
# JWT removed
import os

def get_current_admin_user(request: Request) -> Dict[str, Any]:
    """Simple admin validation - no token needed"""
    # Return mock admin user for testing
    return {
        "user_id": 1,
        "email": "admin@dss.com",
        "role": "ADMIN",
        "full_name": "System Administrator"
    }

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