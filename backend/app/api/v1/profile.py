"""
Profile Management API Endpoints
"""
from fastapi import APIRouter, HTTPException, Depends, Request

from typing import Dict, Any
import logging
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from models.user import ProfileResponse, ProfileUpdateRequest
from models.admin import UserActionResponse
from services.user_management_service import UserManagementService

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/profile", 
    tags=["👤 Profile Management"],
    responses={
        401: {"description": "Unauthorized - Authentication required"},
        403: {"description": "Forbidden - Invalid token"},
        500: {"description": "Internal server error"}
    }
)



# Dependency to get database manager
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

# Dependency to get user management service
async def get_user_service(db = Depends(get_database)) -> UserManagementService:
    """Get user management service"""
    return UserManagementService(db)

@router.get("", response_model=ProfileResponse,
           summary="👤 View My Profile",
           description="Get current user's profile information from JWT token")
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

@router.put("", response_model=UserActionResponse,
           summary="✏️ Update My Profile", 
           description="Update current user's profile information")
async def update_my_profile(
    profile_data: ProfileUpdateRequest,
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
        # Use mock user ID for testing
        user_id = 1
        
        # Update profile
        updated_profile = await user_service.update_profile(
            user_id=user_id,
            full_name=profile_data.full_name,
            phone=profile_data.phone,
            email=profile_data.email
        )
        
        return UserActionResponse(
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