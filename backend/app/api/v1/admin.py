"""
Admin User Management API
Handles user CRUD operations, activity logs, and statistics
"""
from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Optional
import logging

from app.services.activity_logger import ActivityLogger
from app.services.admin_service import AdminService
from models.admin import (
    UserCreateRequest, 
    UserUpdateRequest, 
    UserPasswordUpdateRequest,
    UserListResponse, 
    UserActionResponse
)
from models.shared import UserResponse
from app.constants.roles import validate_role_code

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/admin", 
    tags=["Admin - User Management"],
    responses={500: {"description": "Internal server error"}}
)

# ==================== DEPENDENCIES ====================

async def get_database():
    """Get database connection from main app"""
    try:
        from main import db_manager
        if not db_manager.is_connected:
            await db_manager.connect()
        return db_manager
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")

async def get_admin_service(db=Depends(get_database)) -> AdminService:
    """Get admin service instance"""
    return AdminService(db)

# ==================== USER ENDPOINTS ====================

@router.get("/users", response_model=UserListResponse)
async def get_users(admin_service: AdminService = Depends(get_admin_service),):
    """Get all active users"""
    
    try:
        users = await admin_service.get_users('active')
        return {
            "success": True,
            "data": users,
            "total": len(users),
            "page": 1,
            "limit": len(users)
        }
    except Exception as e:
        logger.error(f"Get users error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get users: {str(e)}")

@router.get("/users/deleted", response_model=UserListResponse)
async def get_deleted_users(admin_service: AdminService = Depends(get_admin_service)):
    """Get all deleted users (status = disabled)"""
    try:
        users = await admin_service.get_users('disabled')
        return {
            "success": True,
            "data": users,
            "total": len(users),
            "page": 1,
            "limit": len(users)
        }
    except Exception as e:
        logger.error(f"Get deleted users error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get deleted users: {str(e)}")

@router.get("/users/{user_id}", response_model=UserResponse)
async def get_user(user_id: int, admin_service: AdminService = Depends(get_admin_service)):
    """Get user by ID"""
    try:
        user = await admin_service.get_user_by_id(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        return UserResponse(**user)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get user error: {e}")
        raise HTTPException(status_code=500, detail="Failed to get user")

@router.post("/users", response_model=UserActionResponse)
async def create_user(
    user_data: UserCreateRequest,
    admin_service: AdminService = Depends(get_admin_service)
):
    """Create new user"""
    try:
        if not validate_role_code(user_data.role):
            raise HTTPException(status_code=400, detail="Invalid role code")
        
        result = await admin_service.create_user(user_data)
        
        return UserActionResponse(
            success=True,
            message="User created successfully",
            user_id=result['user_id']
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Create user error: {e}")
        raise HTTPException(status_code=500, detail="Failed to create user")

@router.put("/users/{user_id}", response_model=UserActionResponse)
async def update_user(
    user_id: int,
    user_data: UserUpdateRequest,
    admin_service: AdminService = Depends(get_admin_service)
):
    """Update user information"""
    try:
        if user_data.role and not validate_role_code(user_data.role):
            raise HTTPException(status_code=400, detail="Invalid role code")
        
        await admin_service.update_user(user_id, user_data)
        
        return UserActionResponse(
            success=True,
            message="User updated successfully",
            user_id=user_id
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Update user error: {e}")
        raise HTTPException(status_code=500, detail="Failed to update user")

@router.put("/users/{user_id}/password", response_model=UserActionResponse)
async def update_user_password(
    user_id: int,
    password_data: UserPasswordUpdateRequest,
    admin_service: AdminService = Depends(get_admin_service)
):
    """Update user password"""
    try:
        await admin_service.change_password(user_id, password_data)
        
        return UserActionResponse(
            success=True,
            message="Password updated successfully",
            user_id=user_id
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Update password error: {e}")
        raise HTTPException(status_code=500, detail="Failed to update password")

# ==================== USER STATUS MANAGEMENT ====================

@router.put("/users/{user_id}/disable", response_model=UserActionResponse)
async def disable_user(user_id: int, admin_service: AdminService = Depends(get_admin_service)):
    """Soft delete user (move to deleted list)"""
    try:
        existing_user = await admin_service.get_user_by_id(user_id)
        if not existing_user:
            raise HTTPException(status_code=404, detail="User not found")
        
        if existing_user['status'] != 'active':
            raise HTTPException(status_code=400, detail="User is not active")
        
        await admin_service.disable_user(user_id)
        
        return UserActionResponse(
            success=True,
            message="User moved to deleted list",
            user_id=user_id
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Disable user error: {e}")
        raise HTTPException(status_code=500, detail="Failed to disable user")

@router.put("/users/{user_id}/restore", response_model=UserActionResponse)
async def restore_user(user_id: int, admin_service: AdminService = Depends(get_admin_service)):
    """Restore user from deleted list"""
    try:
        existing_user = await admin_service.get_user_by_id(user_id)
        if not existing_user:
            raise HTTPException(status_code=404, detail="User not found")
        
        if existing_user['status'] != 'disabled':
            raise HTTPException(status_code=400, detail="User is not in deleted list")
        
        await admin_service.restore_user(user_id)
        
        return UserActionResponse(
            success=True,
            message="User restored successfully",
            user_id=user_id
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Restore user error: {e}")
        raise HTTPException(status_code=500, detail="Failed to restore user")

@router.delete("/users/{user_id}", response_model=UserActionResponse)
async def delete_user(
    user_id: int,
    confirm: bool = Query(False, description="⚠️ Set to true to confirm permanent deletion"),
    admin_service: AdminService = Depends(get_admin_service)
):
    """
    Permanently delete user and all related data
    
    **Deleted data includes:**
    - Account information
    - Roles and permissions
    - Activity logs
    - Authentication tokens
    
    **Usage**: Add `?confirm=true` to URL
    **Example**: `DELETE /api/v1/admin/users/123?confirm=true`
    """
    try:
        if not confirm:
            raise HTTPException(
                status_code=400, 
                detail="⚠️ Permanent deletion requires confirmation. Add ?confirm=true to URL"
            )
        
        user = await admin_service.get_user_by_id(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        email = await admin_service.delete_user(user_id)
        logger.info(f"✅ Permanently deleted user: {email} (ID: {user_id})")
        
        return UserActionResponse(
            success=True,
            message=f"✅ Permanently deleted account {email}",
            user_id=user_id
        )
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Delete user error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to delete user: {str(e)}")

# ==================== ACTIVITY LOGS ====================

@router.get("/activity-logs")
async def get_activity_logs(
    user_id: Optional[int] = Query(None, description="Filter by user ID"),
    action: Optional[str] = Query(None, description="Filter by action"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    db=Depends(get_database)
):
    """Get all activity logs with optional filters"""
    try:
        activity_logger = ActivityLogger(db)
        result = await activity_logger.get_activity_logs(
            page=1,
            limit=10000,
            user_id=user_id,
            action=action,
            start_date=start_date,
            end_date=end_date
        )
        return {"success": True, "data": result}
    except Exception as e:
        logger.error(f"Get activity logs error: {e}")
        raise HTTPException(status_code=500, detail="Failed to get activity logs")

@router.get("/activity-stats")
async def get_activity_stats(
    days: int = Query(7, ge=1, le=365, description="Number of days to analyze"),
    db=Depends(get_database)
):
    """Get activity statistics"""
    try:
        admin_service = AdminService(db)
        stats = await admin_service.get_activity_stats()
        return {"success": True, "data": stats}
    except Exception as e:
        logger.error(f"Get activity stats error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get activity stats: {str(e)}")

