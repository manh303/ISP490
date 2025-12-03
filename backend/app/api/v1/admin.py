"""
Admin User Management API
Handles user CRUD operations, activity logs, and statistics
"""
from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Optional
import logging
from datetime import datetime
from app.services.activity_logger import ActivityLogger
from app.services.admin_service import AdminService
from app.models.admin import (
    UserCreateRequest, 
    UserUpdateRequest, 
    UserPasswordUpdateRequest,
    UserListResponse, 
    UserActionResponse
)
from app.models.shared import UserResponse
from app.constants.roles import validate_role_code
from app.api.dependencies import require_role
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
        from backend.main import db_manager
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

@router.get("/users", response_model=UserListResponse,dependencies=[Depends(require_role("ADMIN"))])
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

@router.get("/users/deleted", response_model=UserListResponse,dependencies=[Depends(require_role("ADMIN"))])
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

@router.get("/users/{user_id}", response_model=UserResponse,dependencies=[Depends(require_role("ADMIN"))])
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

@router.post("/users", response_model=UserActionResponse,dependencies=[Depends(require_role("ADMIN"))])
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

@router.put("/users/{user_id}", response_model=UserActionResponse,dependencies=[Depends(require_role("ADMIN"))])
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

@router.put("/users/{user_id}/password", response_model=UserActionResponse,dependencies=[Depends(require_role("ADMIN"))])
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

@router.put("/users/{user_id}/disable", response_model=UserActionResponse,dependencies=[Depends(require_role("ADMIN"))])
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

@router.put("/users/{user_id}/restore", response_model=UserActionResponse,dependencies=[Depends(require_role("ADMIN"))])
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

@router.delete("/users/{user_id}", response_model=UserActionResponse,dependencies=[Depends(require_role("ADMIN"))])
async def delete_user(
    user_id: int,
    confirm: bool = Query(False, description="⚠️ Set to true to confirm permanent deletion"),
    admin_service: AdminService = Depends(get_admin_service)
):
    """
    Xóa vĩnh viễn người dùng và tất cả dữ liệu liên quan

    **Dữ liệu đã xóa bao gồm:**
    - Thông tin tài khoản
    - Vai trò và quyền
    - Nhật ký hoạt động
    - Mã thông báo xác thực

    """
    try:
        if not confirm:
            raise HTTPException(
                status_code=400, 
                detail="⚠️ Xóa vĩnh viễn yêu cầu xác nhận. Thêm ?confirm=true vào URL"
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

@router.get("/activity-logs", dependencies=[Depends(require_role("ADMIN"))])
async def get_activity_logs(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(20, ge=1, le=100, description="Results per page"),
    sort: str = Query("-created_at", description="Sort field (prefix with - for descending)"),
    user_id: Optional[int] = Query(None, description="Filter by user ID"),
    user_email: Optional[str] = Query(None, description="Filter by user email"),
    role: Optional[str] = Query(None, description="Filter by role"),
    module: Optional[str] = Query(None, description="Filter by module (IAM, Analytics, DSS, ML, DataPipeline)"),
    action: Optional[str] = Query(None, description="Filter by action"),
    status: Optional[str] = Query(None, description="Filter by status (success/error)"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    keyword: Optional[str] = Query(None, description="Keyword search in resource, message, action"),
    db=Depends(get_database)
):
    """
    Get activity logs with comprehensive filtering options
    
    **Filters:**
    - `user_email`: Search by email (partial match)
    - `role`: Filter by user role
    - `module`: Filter by system module
    - `action`: Filter by action type
    - `status`: Filter by status (success/error)
    - `start_date`, `end_date`: Date range filter
    - `keyword`: Search across multiple fields
    
    **Sorting:**
    - Use field name for ascending, prefix with `-` for descending
    - Example: `sort=-created_at` for newest first
    """
    try:
        activity_logger = ActivityLogger(db)
        result = await activity_logger.get_activity_logs(
            page=page,
            limit=limit,
            sort=sort,
            user_id=user_id,
            user_email=user_email,
            role=role,
            module=module,
            action=action,
            status=status,
            start_date=start_date,
            end_date=end_date,
            keyword=keyword
        )
        return {
            "success": True,
            "data": result["logs"],
            "pagination": result["pagination"]
        }
    except Exception as e:
        logger.error(f"Get activity logs error: {e}")
        raise HTTPException(status_code=500, detail="Failed to get activity logs")

@router.get("/activity-logs/export", dependencies=[Depends(require_role("ADMIN"))])
async def export_activity_logs(
    user_id: Optional[int] = Query(None, description="Filter by user ID"),
    user_email: Optional[str] = Query(None, description="Filter by user email"),
    role: Optional[str] = Query(None, description="Filter by role"),
    module: Optional[str] = Query(None, description="Filter by module"),
    action: Optional[str] = Query(None, description="Filter by action"),
    status: Optional[str] = Query(None, description="Filter by status"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    keyword: Optional[str] = Query(None, description="Keyword search"),
    db=Depends(get_database)
):
    """
    Export activity logs to CSV format
    
    **Uses same filters as the main activity logs endpoint**
    
    Returns a CSV file with columns:
    - Time
    - User Email
    - Full Name
    - Role
    - Action
    - Module
    - Resource Type
    - Resource
    - Status
    - IP Address
    - Message
    """
    try:
        from fastapi.responses import StreamingResponse
        import io
        
        activity_logger = ActivityLogger(db)
        csv_data = await activity_logger.export_logs(
            user_id=user_id,
            user_email=user_email,
            role=role,
            module=module,
            action=action,
            status=status,
            start_date=start_date,
            end_date=end_date,
            keyword=keyword
        )
        
        # Create filename with timestamp
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"activity_logs_{timestamp}.csv"
        
        # Return as streaming response
        output = io.BytesIO(csv_data.encode('utf-8'))
        
        return StreamingResponse(
            output,
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except Exception as e:
        logger.error(f"Export activity logs error: {e}")
        raise HTTPException(status_code=500, detail="Failed to export activity logs")

@router.get("/activity-logs/{log_id}", dependencies=[Depends(require_role("ADMIN"))])
async def get_activity_log_detail(
    log_id: int,
    db=Depends(get_database)
):
    """
    Get detailed information for a single activity log
    
    **Returns comprehensive details including:**
    - User information
    - Module and action
    - Resource details
    - Request information (method, path, payload)
    - Before/After data (for modifications)
    - Response status and messages
    """
    try:
        activity_logger = ActivityLogger(db)
        log_detail = await activity_logger.get_log_detail(log_id)
        
        if not log_detail:
            raise HTTPException(status_code=404, detail="Activity log not found")
        
        return {
            "success": True,
            "data": log_detail
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get activity log detail error: {e}")
        raise HTTPException(status_code=500, detail="Failed to get activity log detail")


