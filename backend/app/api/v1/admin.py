"""
Admin User Management API Endpoints
"""
from fastapi import APIRouter, HTTPException, Depends, Query
from typing import List, Dict, Any, Optional
import logging

# Import models and services
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from app.services.activity_logger import ActivityLogger
from app.services.admin_service import AdminService
from models.admin import (
    UserCreateRequest, UserUpdateRequest, UserPasswordUpdateRequest,
    UserListResponse, UserActionResponse
)
from models.shared import UserResponse
from app.utils.admin_helpers import get_current_admin_user, format_user_response
from app.constants.roles import validate_role_code

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/admin", 
    tags=["Admin - User Management"],
    responses={
        500: {"description": "Internal server error"}
    }
)

# Dependency to get database manager
async def get_database():
    """Get database connection - will be injected from main app"""
    try:
        from main import db_manager
        if not db_manager.is_connected:
            await db_manager.connect()
        return db_manager
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")

# Dependency to get admin service
async def get_admin_service(db = Depends(get_database)) -> AdminService:
    """Get admin service"""
    return AdminService(db)

# Dependency to get user service (alias for admin service)
async def get_user_service(db = Depends(get_database)) -> AdminService:
    """Get user service (alias for admin service)"""
    return AdminService(db)

@router.get("/users", response_model=UserListResponse, 
           summary=" Get Active Users",
           description="Get all active users (no pagination)")
async def get_users(
    admin_service: AdminService = Depends(get_admin_service)
):
    """Get all active users - NO AUTH REQUIRED, NO PAGINATION"""
    try:
        logger.info( f" Getting all active users")
        users = await admin_service.get_users('active')
        
        # Return raw dict data - Pydantic will validate and convert
        return {
            "success": True,
            "data": users,
            "total": len(users),
            "page": 1,
            "limit": len(users)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get users error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get users: {str(e)}")

@router.get("/users/deleted", response_model=UserListResponse)
async def get_deleted_users(
    admin_service: AdminService = Depends(get_admin_service)
):
    """Get all deleted users (status = disabled) - NO PAGINATION"""
    try:
        # Get deleted users
        users = await admin_service.get_users('disabled')
        
        # Return raw dict data
        return {
            "success": True,
            "data": users,
            "total": len(users),
            "page": 1,
            "limit": len(users)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get deleted users error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get deleted users: {str(e)}")

@router.get("/users/{user_id}", response_model=UserResponse)
async def get_user(
    user_id: int,
    admin_service: AdminService = Depends(get_admin_service)
):
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
        
        # Extract user_id from result dict
        user_id = result.get('user_id') if isinstance(result, dict) else result
        
        return UserActionResponse(
            success=True,
            message="User created successfully",
            user_id=user_id
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
        # Validate role code if provided
        if user_data.role and not validate_role_code(user_data.role):
            raise HTTPException(status_code=400, detail="Invalid role code")
        
        # Update user
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
        # Update password
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

@router.put("/users/{user_id}/disable", response_model=UserActionResponse)
async def disable_user(
    user_id: int,
    admin_service: AdminService = Depends(get_admin_service)
):
    """
    Soft delete user (move to deleted list)
    
    **Action**: Changes user status from 'active' to 'disabled'
    
    **Note**: This is the first step of deletion. User can be restored later.
    """
    try:
        # Check if user exists and is active
        existing_user = await admin_service.get_user_by_id(user_id)
        if not existing_user:
            raise HTTPException(status_code=404, detail="User not found")
        
        if existing_user['status'] != 'active':
            raise HTTPException(status_code=400, detail="User is not active")
        
        # Soft delete
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
async def restore_user(
    user_id: int,
    admin_service: AdminService = Depends(get_admin_service)
):
    """Restore user from deleted list"""
    try:
        # Check if user exists and is disabled
        existing_user = await admin_service.get_user_by_id(user_id)
        if not existing_user:
            raise HTTPException(status_code=404, detail="User not found")
        
        if existing_user['status'] != 'disabled':
            raise HTTPException(status_code=400, detail="User is not in deleted list")
        
        # Restore user
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
    confirm: bool = Query(False, description="⚠️ REQUIRED: Set to true to confirm permanent deletion"),
    admin_service: AdminService = Depends(get_admin_service)
):
    """
    **Dữ liệu bị xóa bao gồm:**
    - Thông tin tài khoản
    - Vai trò (roles)
    - Phiên đăng nhập (sessions)
    - Lịch sử hoạt động (activity logs)
    - Token xác thực
    
    **Cách sử dụng**: Thêm `?confirm=true` vào URL
    
    **Ví dụ**: `DELETE /api/v1/admin/users/123?confirm=true`
    """
    try:
        logger.info(f"🗑️ Attempting to permanently delete user_id: {user_id}")
        
        # Require confirmation
        if not confirm:
            raise HTTPException(
                status_code=400, 
                detail="⚠️ Xóa vĩnh viễn cần xác nhận. Vui lòng thêm ?confirm=true vào URL"
            )
        
        # Get user info before deletion
        user = await admin_service.get_user_by_id(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="Không tìm thấy người dùng")
        
        logger.info(f"🔍 Found user to delete: {user['email']} (ID: {user_id})")
        
        # Delete user (this will also delete all related data)
        email = await admin_service.delete_user(user_id)
        
        logger.info(f"✅ Successfully deleted user: {email} (ID: {user_id})")
        
        return UserActionResponse(
            success=True,
            message=f"✅ Đã xóa vĩnh viễn tài khoản {email}",
            user_id=user_id
        )
        
    except HTTPException:
        raise
    except ValueError as e:
        logger.error(f"❌ User not found: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"❌ Delete user error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Không thể xóa người dùng: {str(e)}")

@router.get("/activity-logs")
async def get_activity_logs(
    user_id: Optional[int] = Query(None, description="Filter by user ID"),
    action: Optional[str] = Query(None, description="Filter by action"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    db = Depends(get_database)
):
    """Get all activity logs with filters - NO PAGINATION"""
    try:
        activity_logger = ActivityLogger(db)
        # Get all logs without pagination
        result = await activity_logger.get_activity_logs(
            page=1,
            limit=10000,  # Large number to get all
            user_id=user_id,
            action=action,
            start_date=start_date,
            end_date=end_date
        )
        
        return {
            "success": True,
            "data": result
        }
    except Exception as e:
        logger.error(f"Get activity logs error: {e}")
        raise HTTPException(status_code=500, detail="Failed to get activity logs")



@router.get("/activity-stats")
async def get_activity_stats(
    days: int = Query(7, ge=1, le=365, description="Number of days to analyze"),
    db = Depends(get_database)
):
    """Get activity statistics"""
    try:
        admin_service = AdminService(db)
        stats = await admin_service.get_activity_stats()
        
        return {
            "success": True,
            "data": stats
        }
    except Exception as e:
        logger.error(f"Get activity stats error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get activity stats: {str(e)}")

@router.get("/user-activity/{user_id}")
async def get_user_activity(
    user_id: int,
    db = Depends(get_database)
):
    """Get all activity logs for specific user - NO PAGINATION"""
    try:
        activity_logger = ActivityLogger(db)
        result = await activity_logger.get_activity_logs(
            page=1,
            limit=10000,  # Large number to get all
            user_id=user_id
        )
        
        return {
            "success": True,
            "data": result
        }
    except Exception as e:
        logger.error(f"Get user activity error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get user activity: {str(e)}")

@router.post("/clear-activity-logs")
async def clear_activity_logs(
    days_older_than: int = Query(30, ge=1, description="Clear logs older than X days"),
    db = Depends(get_database)
):
    """Get activity statistics"""
    try:
        activity_logger = ActivityLogger(db)
        stats = await activity_logger.get_activity_stats()
        
        return {
            "success": True,
            "data": stats
        }
    except Exception as e:
        logger.error(f"Get activity stats error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get activity stats: {str(e)}")