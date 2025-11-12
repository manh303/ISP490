"""
Admin User Management API Endpoints
"""
from fastapi import APIRouter, HTTPException, Depends, Request, Query
from typing import List, Dict, Any, Optional
import logging
from services.activity_logger import ActivityLogger

# Import models and services
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from services.admin_service import (
    UserCreateRequest, UserUpdateRequest, UserPasswordUpdateRequest,
    UserListResponse, UserActionResponse
)
from models.shared import UserResponse
from services.user_management_service import UserManagementService
from utils.admin_helpers import get_current_admin_user, format_user_response
from constants.roles import validate_role_code

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/admin", 
    tags=["Admin - User Management"],
    responses={
        401: {"description": "Unauthorized - Admin access required"},
        403: {"description": "Forbidden - Invalid admin token"},
        500: {"description": "Internal server error"}
    }
)

# Admin access dependency
async def require_admin_access(request: Request) -> Dict[str, Any]:
    """Dependency to require admin access for endpoints"""
    from utils.auth_helpers import decode_access_token
    from main import settings
    
    token = credentials.credentials
    logger.info(f"Admin access check - Token: {token[:20]}...")
    
    # Decode token
    payload = decode_access_token(token, settings.JWT_SECRET_KEY, settings.JWT_ALGORITHM)
    if not payload:
        logger.error("Token decode failed")
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    
    logger.info(f"Token payload: {payload}")
    
    # Check admin role
    user_role = payload.get("role")
    if user_role not in ["ADMIN"]:
        logger.error(f"Access denied - Role: {user_role}, Required: ADMIN")
        raise HTTPException(status_code=403, detail=f"Admin access required. Current role: {user_role}")
    
    logger.info(f"Admin access granted for user: {payload.get('email')}")
    return {
        "user_id": payload.get("user_id"),
        "email": payload.get("email"),
        "role": user_role,
        "full_name": payload.get("full_name", "Admin User")
    }

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

@router.get("/users", response_model=UserListResponse, 
           summary="📋 Get Active Users",
           description="Get paginated list of active users. **Requires Admin Token!**")
async def get_users(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    admin_user: Dict[str, Any] = Depends(require_admin_access),
    user_service: UserManagementService = Depends(get_user_service)
):
    """Get list of users"""
    try:
        users, total = await admin_service.get_users(page, limit, status_filter or 'active')
        
        user_responses = []
        for user in users:
            try:
                user_response = UserResponse(**user)
                user_responses.append(user_response)
            except Exception as e:
                logger.error(f"Error converting user {user}: {e}")
        
        return UserListResponse(
            success=True,
            data=user_responses,
            total=total,
            page=page,
            limit=limit
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get users error: {e}")
        raise HTTPException(status_code=500, detail="Failed to get users")

@router.get("/users/deleted", response_model=UserListResponse)
async def get_deleted_users(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(20, ge=1, le=100, description="Items per page"),
    user_service: UserManagementService = Depends(get_user_service)
):
    """Get list of deleted users (status = disabled)"""
    try:
        # Get deleted users
        users, total = await admin_service.get_users(page, limit, 'disabled')
        
        user_responses = [UserResponse(**user) for user in users]
        
        return UserListResponse(
            success=True,
            data=user_responses,
            total=total,
            page=page,
            limit=limit
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get deleted users error: {e}")
        raise HTTPException(status_code=500, detail="Failed to get deleted users")

@router.get("/users/{user_id}", response_model=UserResponse)
async def get_user(
    user_id: int,
    user_service: UserManagementService = Depends(get_user_service)
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
    admin_user: Dict[str, Any] = Depends(require_admin_access),
    user_service: UserManagementService = Depends(get_user_service)
):
    """Create new user"""
    try:
        if not validate_role_code(user_data.role):
            raise HTTPException(status_code=400, detail="Invalid role code")
        
        user_id = await admin_service.create_user(user_data)
        
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
    admin_user: Dict[str, Any] = Depends(require_admin_access),
    user_service: UserManagementService = Depends(get_user_service)
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
    user_service: UserManagementService = Depends(get_user_service)
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
    user_service: UserManagementService = Depends(get_user_service)
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
    admin_service: AdminService = Depends(get_admin_service),
    admin_user: Dict[str, Any] = Depends(require_admin_access)
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
    admin_user: Dict[str, Any] = Depends(require_admin_access),
    user_service: UserManagementService = Depends(get_user_service)
):
    """
    ⚠️ Delete user (IRREVERSIBLE)
    
    **WARNING**: This action cannot be undone!
    
    **Example**: `/api/v1/admin/users/123?confirm=true`
    """
    try:
        # Require confirmation
        if not confirm:
            raise HTTPException(
                status_code=400, 
                detail="Deletion requires confirmation. Add ?confirm=true to the request"
            )
        
        # Delete user
        email = await admin_service.delete_user(user_id)
        
        return UserActionResponse(
            success=True,
            message=f"User {email} deleted successfully",
            user_id=user_id
        )
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Delete user error: {e}")
        raise HTTPException(status_code=500, detail="Failed to delete user")

@router.get("/activity-logs")
async def get_activity_logs(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(50, ge=1, le=100, description="Items per page"),
    user_id: Optional[int] = Query(None, description="Filter by user ID"),
    action: Optional[str] = Query(None, description="Filter by action"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    db = Depends(get_database),
    admin_user: Dict[str, Any] = Depends(require_admin_access)
):
    """Get user activity logs with filters"""
    try:
        activity_logger = ActivityLogger(db)
        result = await activity_logger.get_activity_logs(
            page=page,
            limit=limit,
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
        logger.error(f"Permanent delete user error: {e}")
        raise HTTPException(status_code=500, detail="Failed to permanently delete user")

@router.get("/activity-logs")
async def get_activity_logs(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(50, ge=1, le=100, description="Items per page"),
    user_id: Optional[int] = Query(None, description="Filter by user ID"),
    action: Optional[str] = Query(None, description="Filter by action"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    db = Depends(get_database)
):
    """Get user activity logs with filters"""
    try:
        logs, total = await admin_service.get_activity_logs(page, limit, user_id)
        
        return {
            "success": True,
            "data": {
                "logs": logs,
                "total": total,
                "page": page,
                "limit": limit
            }
        }
    except Exception as e:
        logger.error(f"Get activity logs error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get activity logs: {str(e)}")

@router.get("/activity-stats")
async def get_activity_stats(
    days: int = Query(7, ge=1, le=365, description="Number of days to analyze"),
    db = Depends(get_database)
):
    """Get activity statistics"""
    try:
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
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(20, ge=1, le=100, description="Items per page"),
    db = Depends(get_database)
):
    """Get activity logs for specific user"""
    try:
        activity_logger = ActivityLogger(db)
        result = await activity_logger.get_activity_logs(
            page=page,
            limit=limit,
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
        stats = await activity_logger.get_activity_stats(days)
        
        return {
            "success": True,
            "data": stats
        }
    except Exception as e:
        logger.error(f"Get activity stats error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get activity stats: {str(e)}")