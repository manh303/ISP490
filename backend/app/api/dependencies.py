from fastapi import HTTPException, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Any, Dict
import logging

logger = logging.getLogger(__name__)
security = HTTPBearer()

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> Dict[str, Any]:
    try:
        from app.utils.auth_helpers import decode_access_token
        from app.core.config import settings
        
        token = credentials.credentials
        payload = decode_access_token(token, settings.JWT_SECRET_KEY, settings.JWT_ALGORITHM)
        
        if not payload:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
        
        return payload
    except Exception as e:
        logger.error(f"Auth error: {e}")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication failed")

def require_role(*role_codes: str):
    """Require user to have one of the specified roles"""
    async def dependency(
        current_user: Dict[str, Any] = Depends(get_current_user),
    ):
        user_roles = current_user.get("roles", [])
        if not any(r in user_roles for r in role_codes):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Required roles: {role_codes}",
            )
        return current_user
    return dependency