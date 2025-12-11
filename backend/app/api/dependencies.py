from fastapi import HTTPException, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Any, Dict
import logging
from app.constants.roles import ROLE_HIERARCHY

logger = logging.getLogger(__name__)
security = HTTPBearer()


async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> Dict[str, Any]:
    try:
        from app.utils.auth_helpers import decode_access_token
        from app.core.settings import settings

        token = credentials.credentials
        payload = decode_access_token(token, settings.JWT_SECRET_KEY, settings.JWT_ALGORITHM)

        if not payload:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

        return payload
    except Exception as e:
        logger.error(f"Auth error: {e}")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication failed")


def _normalize_roles(payload: Dict[str, Any]) -> list[str]:
    """Normalize roles from token payload (supports legacy `role`)."""
    roles = payload.get("roles") or []
    primary_role = payload.get("role")
    if primary_role and primary_role not in roles:
        roles = [primary_role, *roles]
    # Ensure uppercase for comparison
    return [r.upper() for r in roles if isinstance(r, str)]


def require_role(*role_codes: str):
    """
    Require user to have one of the specified roles.
    Admin bypass: ADMIN can access all role-protected endpoints.
    """

    required = {r.upper() for r in role_codes}

    async def dependency(current_user: Dict[str, Any] = Depends(get_current_user)):
        user_roles = _normalize_roles(current_user)

        # Super-role: ADMIN always allowed
        if "ADMIN" in user_roles:
            return current_user

        # Allow if user has any required role
        if required.intersection(user_roles):
            return current_user

        # Fallback to hierarchy check if provided (higher roles can access lower)
        for user_role in user_roles:
            for req in required:
                if ROLE_HIERARCHY.get(user_role, 0) >= ROLE_HIERARCHY.get(req, 0):
                    return current_user

        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Required roles: {role_codes}",
        )

    return dependency
