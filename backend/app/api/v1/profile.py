"""
Profile Management API
"""
from fastapi import APIRouter, HTTPException, Depends, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Dict, Any
import logging

# Import models and services
from models.user import ProfileResponse, ProfileUpdateRequest, ProfileActionResponse
from services.profile_service import ProfileService

logger = logging.getLogger(__name__)

# Create router
router = APIRouter(
    prefix="/profile",
    tags=["👤 Profile Management"]
)

security = HTTPBearer()

# Dependencies
async def get_database():
    """Get database connection"""
    import sys
    import os
    sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
    from main import db_manager
    if not db_manager.is_connected:
        await db_manager.connect()
    return db_manager

async def get_profile_service(db = Depends(get_database)) -> ProfileService:
    """Get profile service"""
    return ProfileService(db)

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
async def get_my_profile(
    current_user: Dict[str, Any] = Depends(get_current_user),
    profile_service: ProfileService = Depends(get_profile_service)
):
    """Get current user's profile"""
    try:
        user_id = current_user.get('user_id')
        if not user_id:
            raise HTTPException(status_code=401, detail="Invalid token - user ID not found")
        
        profile = await profile_service.get_profile(user_id)
        if not profile:
            raise HTTPException(status_code=404, detail="Profile not found")
        
        return ProfileResponse(**profile)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get profile error: {e}")
        raise HTTPException(status_code=500, detail="Failed to get profile")

@router.put("", response_model=ProfileActionResponse, summary="✏️ Update My Profile")
async def update_my_profile(
    profile_data: ProfileUpdateRequest,
    current_user: Dict[str, Any] = Depends(get_current_user),
    profile_service: ProfileService = Depends(get_profile_service)
):
    """Update current user's profile"""
    try:
        user_id = current_user.get('user_id')
        if not user_id:
            raise HTTPException(status_code=401, detail="Invalid token - user ID not found")
        
        await profile_service.update_profile(user_id, profile_data)
        
        return ProfileActionResponse(
            success=True,
            message="Profile updated successfully",
            user_id=user_id
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Update profile error: {e}")
        raise HTTPException(status_code=500, detail="Failed to update profile")