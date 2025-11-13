"""
Authentication helper functions
"""
import jwt
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from constants.roles import get_role_menu, ROLE_MENUS

logger = logging.getLogger(__name__)

def create_access_token(user_data: dict, email: str, jwt_secret: str, jwt_algorithm: str = "HS256", expire_hours: int = 24) -> str:
    """Create JWT access token with role, roles[], permissions for cross-service guards"""
    exp_time = datetime.utcnow() + timedelta(hours=expire_hours)
    
    role = user_data['role']
    role_config = get_role_menu(role)
    
    token_payload = {
        "user_id": user_data['user_id'],
        "email": email.lower(),
        "full_name": user_data.get('full_name', 'User'),
        "role": role,
        "roles": [role],
        "permissions": role_config.get('permissions', []),
        "exp": exp_time,
        "iat": datetime.utcnow()
    }
    return jwt.encode(token_payload, jwt_secret, algorithm=jwt_algorithm)

def decode_access_token(token: str, jwt_secret: str, jwt_algorithm: str = "HS256") -> Optional[dict]:
    """Decode JWT token and return payload or None"""
    try:
        payload = jwt.decode(token, jwt_secret, algorithms=[jwt_algorithm])
        return payload
    except Exception as e:
        logger.debug(f"Token decode failed: {e}")
        return None

def get_user_from_token(token: str, jwt_secret: str) -> Optional[Dict[str, Any]]:
    """Extract user info from JWT token"""
    payload = decode_access_token(token, jwt_secret)
    if not payload:
        return None
    
    return {
        "user_id": payload.get("user_id"),
        "email": payload.get("email"),
        "full_name": payload.get("full_name", "User"),
        "role": payload.get("role"),
        "roles": payload.get("roles", []),
        "permissions": payload.get("permissions", [])
    }