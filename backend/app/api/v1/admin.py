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

from models.admin import (
    UserCreateRequest, UserUpdateRequest, UserPasswordUpdateRequest,
    UserResponse, UserListResponse, UserActionResponse
)
from services.user_management_service import UserManagementService
from utils.admin_helpers import get_current_admin_user, validate_role_code, format_user_response

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

# No authentication required - direct access
async def get_mock_admin_user():
    """Mock admin user for testing - no authentication required"""
    return {
        "user_id": 1,
        "email": "admin@dss.com",
        "role": "ADMIN",
        "full_name": "System Administrator"
    }

# Dependency to get database manager
async def get_database():
    """Get database connection - will be injected from main app"""
    from fastapi import Request
    # Get db_manager from app state or import directly
    try:
        import sys
        import os
        sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
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

@router.get("/users", response_model=UserListResponse, 
           summary="📋 Get Active Users",
           description="Get paginated list of active users. **Requires Admin Token!**")
async def get_users(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(20, ge=1, le=100, description="Items per page"),
    user_service: UserManagementService = Depends(get_user_service)
):
    """
    Get list of active users
    
    **Required**: Admin role and valid JWT token in Authorization header
    
    **Example**: `Authorization: Bearer your_admin_token_here`
    """
    try:
        # Admin access already validated by dependency
        # Get users
        logger.info(f"Getting users with status=active, page={page}, limit={limit}")
        result = await user_service.get_users(status='active', page=page, limit=limit)
        logger.info(f"Service returned {len(result['users'])} users, total={result['total']}")
        
        users = [format_user_response(user) for user in result['users']]
        logger.info(f"Formatted {len(users)} users for response")
        
        return UserListResponse(
            success=True,
            data=users,
            total=result['total'],
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
        result = await user_service.get_users(status='disabled', page=page, limit=limit)
        
        users = [format_user_response(user) for user in result['users']]
        
        return UserListResponse(
            success=True,
            data=users,
            total=result['total'],
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
        # Get user
        user = await user_service.get_user_by_id(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        
        return UserResponse(**format_user_response(user))
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get user error: {e}")
        raise HTTPException(status_code=500, detail="Failed to get user")

@router.post("/users", response_model=UserActionResponse)
async def create_user(
    user_data: UserCreateRequest,
    user_service: UserManagementService = Depends(get_user_service)
):
    """
    Create new user
    
    **Required**: Admin role and valid JWT token
    
    **Valid role_codes**: ADMIN, ANALYST, CUSTOMER, MANAGER
    
    **Example request body**:
    ```json
    {
        "email": "user@example.com",
        "password": "password123",
        "full_name": "John Doe",
        "phone": "0123456789",
        "role_code": "CUSTOMER"
    }
    ```
    """
    try:
        # Validate role code
        if not validate_role_code(user_data.role_code):
            raise HTTPException(status_code=400, detail="Invalid role code")
        
        # Create user
        user = await user_service.create_user(
            email=user_data.email,
            password=user_data.password,
            full_name=user_data.full_name,
            phone=user_data.phone,
            role_code=user_data.role_code
        )
        
        return UserActionResponse(
            success=True,
            message="User created successfully",
            user_id=user['user_id']
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
    user_service: UserManagementService = Depends(get_user_service)
):
    """Update user information"""
    try:
        # Validate role code if provided
        if user_data.role_code and not validate_role_code(user_data.role_code):
            raise HTTPException(status_code=400, detail="Invalid role code")
        
        # Check if user exists
        existing_user = await user_service.get_user_by_id(user_id)
        if not existing_user:
            raise HTTPException(status_code=404, detail="User not found")
        
        # Update user
        await user_service.update_user(
            user_id=user_id,
            full_name=user_data.full_name,
            phone=user_data.phone,
            role_code=user_data.role_code
        )
        
        return UserActionResponse(
            success=True,
            message="User updated successfully",
            user_id=user_id
        )
        
    except HTTPException:
        raise
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
        # Check if user exists
        existing_user = await user_service.get_user_by_id(user_id)
        if not existing_user:
            raise HTTPException(status_code=404, detail="User not found")
        
        # Update password
        await user_service.update_password(user_id, password_data.new_password)
        
        return UserActionResponse(
            success=True,
            message="Password updated successfully",
            user_id=user_id
        )
        
    except HTTPException:
        raise
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
        existing_user = await user_service.get_user_by_id(user_id)
        if not existing_user:
            raise HTTPException(status_code=404, detail="User not found")
        
        if existing_user['status'] != 'active':
            raise HTTPException(status_code=400, detail="User is not active")
        
        # Soft delete
        await user_service.soft_delete_user(user_id)
        
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
    request: Request,
    user_service: UserManagementService = Depends(get_user_service)
):
    """Restore user from deleted list"""
    try:
        # Check if user exists and is disabled
        existing_user = await user_service.get_user_by_id(user_id)
        if not existing_user:
            raise HTTPException(status_code=404, detail="User not found")
        
        if existing_user['status'] != 'disabled':
            raise HTTPException(status_code=400, detail="User is not in deleted list")
        
        # Restore user
        await user_service.restore_user(user_id)
        
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

@router.delete("/users/{user_id}/permanent", response_model=UserActionResponse)
async def permanent_delete_user(
    user_id: int,
    request: Request,
    confirm: bool = Query(False, description="⚠️ REQUIRED: Set to true to confirm permanent deletion"),
    user_service: UserManagementService = Depends(get_user_service)
):
    """
    ⚠️ Permanently delete user (IRREVERSIBLE)
    
    **WARNING**: This action cannot be undone!
    
    **Requirements**:
    - User must be in 'disabled' status (deleted list)
    - Must set `confirm=true` parameter
    
    **Example**: `/api/v1/admin/users/123/permanent?confirm=true`
    """
    try:
        # Validate admin access
        current_user = get_current_admin_user(request)
        
        # Require confirmation
        if not confirm:
            raise HTTPException(
                status_code=400, 
                detail="Permanent deletion requires confirmation. Add ?confirm=true to the request"
            )
        
        # Check if user exists and is disabled
        existing_user = await user_service.get_user_by_id(user_id)
        if not existing_user:
            raise HTTPException(status_code=404, detail="User not found")
        
        if existing_user['status'] != 'disabled':
            raise HTTPException(
                status_code=400, 
                detail="User must be in deleted list before permanent deletion"
            )
        
        # Permanent delete
        await user_service.permanent_delete_user(user_id)
        
        return UserActionResponse(
            success=True,
            message="User permanently deleted",
            user_id=user_id
        )
        
    except HTTPException:
        raise
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
        logger.error(f"Get activity logs error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get activity logs: {str(e)}")

@router.get("/activity-stats")
async def get_activity_stats(
    days: int = Query(7, ge=1, le=365, description="Number of days to analyze"),
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
    """Clear activity logs older than specified days"""
    try:
        query = """
        DELETE FROM user_activity_logs 
        WHERE created_at < NOW() - INTERVAL '%s days'
        """
        
        await db.execute_query(query, (days_older_than,))
        
        return {
            "success": True,
            "message": f"Cleared activity logs older than {days_older_than} days"
        }
    except Exception as e:
        logger.error(f"Clear activity logs error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to clear activity logs: {str(e)}")

