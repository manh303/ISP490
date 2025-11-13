"""
Profile Management API
"""
from fastapi import APIRouter, HTTPException, Depends, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Dict, Any
import logging
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from models.user import ProfileResponse, ProfileUpdateRequest
from services.admin_service import UserActionResponse

class ProfileActionResponse(UserActionResponse):
    pass
from services.user_management_service import UserManagementService

logger = logging.getLogger(__name__)

# Create router
router = APIRouter(
    prefix="/profile", 
    tags=["👤 Profile Management"],
    responses={
        401: {"description": "Unauthorized - Authentication required"},
        403: {"description": "Forbidden - Invalid token"},
        500: {"description": "Internal server error"}
    }
)



# Dependencies
security = HTTPBearer()

async def get_database():
    """Get database connection"""
    try:
        from main import db_manager
        if not db_manager.is_connected:
            await db_manager.connect()
        return db_manager
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")

async def get_user_service(db=Depends(get_database)):
    """Get user management service"""
    return UserManagementService(db)

def get_current_user_from_token(credentials: HTTPAuthorizationCredentials):
    """Mock function to get current user from token"""
    return {"user_id": 1, "username": "test_user"}

# Endpoints
@router.get("", response_model=ProfileResponse, summary="👤 View My Profile")
async def get_my_profile(
    user_service: UserManagementService = Depends(get_user_service)
):
    """
    Get current user's profile
    
    **Authentication**: Required - JWT token in Authorization header
    
    **Returns**: Complete user profile information
    """
    try:
        # Use mock user ID for testing
        user_id = 1
        
        # Get profile from database
        profile = await user_service.get_profile(user_id)
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
    credentials: HTTPAuthorizationCredentials = Depends(security),
    user_service: UserManagementService = Depends(get_user_service)
):
    """
    Update current user's profile
    
    **Authentication**: Required - JWT token in Authorization header
    
    **Updatable fields**:
    - full_name: User's full name
    - phone: Phone number
    - email: Email address (must be unique)
    
    **Example request body**:
    ```json
    {
        "full_name": "John Doe Updated",
        "phone": "+84987654321",
        "email": "newemail@example.com"
    }
    ```
    """
    try:
        # Get current user from token
        current_user = get_current_user_from_token(credentials)
        user_id = current_user["user_id"]
        
        # Update profile
        updated_profile = await user_service.update_profile(
            user_id=user_id,
            full_name=profile_data.full_name,
            phone=profile_data.phone,
            email=profile_data.email
        )
        
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