"""
Profile Management API
"""
from fastapi import APIRouter, HTTPException, Depends
import logging
from app.api.dependencies import get_current_user
from app.models.user import (
    EmailChangeConfirmIn, 
    EmailChangeRequestIn,
    ProfileUpdateRequest,
    UserProfileResponse
)
from app.services.profile_service import ProfileService

class ProfileActionResponse(UserProfileResponse):
    pass

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
        from backend.main import db_manager
        if not db_manager.is_connected:
            await db_manager.connect()
        return db_manager
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")

async def get_user_service(db=Depends(get_database)):
    """Get profile service for user management"""
    return ProfileService(db)

# Endpoints
@router.get("", response_model=UserProfileResponse, summary="👤 View My Profile")
async def get_my_profile(
    current_user: dict = Depends(get_current_user),
    user_service: ProfileService = Depends(get_user_service)
):
    """
    Get current user profile
    
    **Authentication Required**
    """
    try:
        user_id = current_user.get("user_id")
        if not user_id:
            raise HTTPException(status_code=401, detail="User ID not found in token")
        
        # Get profile from database
        profile = await user_service.get_profile(user_id)
        if not profile:
            raise HTTPException(status_code=404, detail="Profile not found")
        
        return UserProfileResponse(**profile)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get profile error: {e}")
        raise HTTPException(status_code=500, detail="Failed to get profile")

@router.put("", response_model=ProfileActionResponse, summary="✏️ Update Profile")
async def update_my_profile(
    profile_data: ProfileUpdateRequest,
    current_user: dict = Depends(get_current_user),
    user_service: ProfileService = Depends(get_user_service)
):
    """
    Update current user profile
    
    **Authentication Required**
    
    **Updatable fields**:
    - full_name: User's full name
    - phone: Phone number
    
    **Example request body**:
    ```json
    {
        "full_name": "John Doe Updated",
        "phone": "+84987654321"
    }
    ```
    """
    try:
        user_id = current_user.get("user_id")
        if not user_id:
            raise HTTPException(status_code=401, detail="User ID not found in token")
        
        # Update profile using update_user from AdminService
        from app.models.admin import UserUpdateRequest
        update_data = UserUpdateRequest(
            full_name=profile_data.full_name,
            phone=profile_data.phone
        )
        await user_service.update_user(user_id, update_data)
        
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
    
@router.post("/email/change-request")
async def request_email_change(
    data: EmailChangeRequestIn,
    current_user = Depends(get_current_user),
    service = Depends(get_user_service),
):
    return await service.request_email_change(current_user["user_id"], data.new_email)

@router.post("/email/confirm")
async def confirm_email_change(
    data: EmailChangeConfirmIn,
    current_user = Depends(get_current_user),
    service = Depends(get_user_service),
):
    return await service.confirm_email_change(
        current_user["user_id"], data.request_id, data.otp
    )