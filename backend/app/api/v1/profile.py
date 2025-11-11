"""
Profile Management API - Clean Version
"""
from fastapi import APIRouter, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)

# Create router
router = APIRouter(
    prefix="/profile",
    tags=["👤 Profile Management"]
)

security = HTTPBearer()

# Models
class ProfileResponse(BaseModel):
    user_id: int
    email: str
    full_name: str
    phone: Optional[str] = None
    role: str

class ProfileUpdateRequest(BaseModel):
    full_name: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None

class ActionResponse(BaseModel):
    success: bool
    message: str
    user_id: Optional[int] = None

# Dependencies
async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> Dict[str, Any]:
    """Get current user from JWT token"""
    try:
        from utils.auth_helpers import decode_access_token
        import os
        
        token = credentials.credentials
        jwt_secret = os.getenv("JWT_SECRET_KEY", "KvyFNJHBkDzgAwCsx659EvNCa9tWUsOlIKpoQZztIyg")
        jwt_algorithm = os.getenv("JWT_ALGORITHM", "HS256")
        
        user_data = decode_access_token(token, jwt_secret, jwt_algorithm)
        if not user_data:
            raise HTTPException(status_code=401, detail="Invalid token")
        return user_data
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Token validation error: {e}")
        raise HTTPException(status_code=401, detail="Invalid or expired token")

# Endpoints
@router.get("", response_model=ProfileResponse, summary="👤 View My Profile")
async def get_my_profile(current_user: Dict[str, Any] = Depends(get_current_user)):
    """Get current user's profile"""
    return ProfileResponse(
        user_id=current_user.get('user_id'),
        email=current_user.get('email'),
        full_name=current_user.get('full_name', 'User'),
        phone=current_user.get('phone'),
        role=current_user.get('role')
    )

@router.get("/debug-token", summary="🔍 Debug Token")
async def debug_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Debug JWT token to see what's inside"""
    try:
        from utils.auth_helpers import decode_access_token
        import os
        
        token = credentials.credentials
        jwt_secret = os.getenv("JWT_SECRET_KEY", "KvyFNJHBkDzgAwCsx659EvNCa9tWUsOlIKpoQZztIyg")
        jwt_algorithm = os.getenv("JWT_ALGORITHM", "HS256")
        
        logger.info(f"Token: {token[:50]}...")
        logger.info(f"Secret: {jwt_secret[:10]}...")
        logger.info(f"Algorithm: {jwt_algorithm}")
        
        user_data = decode_access_token(token, jwt_secret, jwt_algorithm)
        
        return {
            "token_valid": user_data is not None,
            "token_data": user_data,
            "token_length": len(token),
            "secret_length": len(jwt_secret)
        }
    except Exception as e:
        return {
            "error": str(e),
            "token_valid": False
        }

@router.put("", response_model=ActionResponse, summary="✏️ Update My Profile")
async def update_my_profile(
    profile_data: ProfileUpdateRequest,
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Update current user's profile"""
    return ActionResponse(
        success=True,
        message="Profile updated successfully",
        user_id=current_user.get('user_id')
    )