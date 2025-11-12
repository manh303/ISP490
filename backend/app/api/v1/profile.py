"""
Profile Management API
"""
from fastapi import APIRouter, HTTPException, Depends
from typing import Dict, Any
import logging
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from models.user import ProfileResponse, ProfileUpdateRequest
from app.services.admin_service import UserActionResponse

class ProfileActionResponse(UserActionResponse):
    pass
from app.services.user_management_service import UserManagementService

logger = logging.getLogger(__name__)

# Create router
router = APIRouter(
    prefix="/profile", 
    tags=["👤 Profile Management"],
    responses={
        500: {"description": "Internal server error"}
    }
)

# Dependencies
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

# Endpoints
@router.get("", response_model=ProfileResponse, summary="👤 View My Profile")
async def get_my_profile(
    user_id: int = 1,
    user_service: UserManagementService = Depends(get_user_service)
):
    """
    Get user profile by user_id
    
    **No Authentication Required**
    
    **Returns**: Complete user profile information
    """
    try:
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

@router.put("", response_model=ProfileActionResponse, summary="✏️ Update Profile")
async def update_my_profile(
    user_id: int,
    profile_data: ProfileUpdateRequest,
    user_service: UserManagementService = Depends(get_user_service)
):
    """
    Update user profile
    
    **No Authentication Required**
    
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