"""
Admin User Management API Endpoints
"""
from fastapi import APIRouter, HTTPException, Depends, Request, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import List, Dict, Any, Optional
import logging
from services.activity_logger import ActivityLogger

# Import models and services
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from models.admin import (
    UserResponse, UserCreateRequest, UserUpdateRequest, PasswordChangeRequest,
    UserListResponse, UserActionResponse, ActivityLogResponse, ActivityStatsResponse
)
from services.admin_service import AdminService
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

security = HTTPBearer()

# Admin access dependency
async def require_admin_access(credentials: HTTPAuthorizationCredentials = Depends(security)) -> Dict[str, Any]:
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
    status_filter: Optional[str] = Query(None, description="Filter by status"),
    admin_service: AdminService = Depends(get_admin_service),
    admin_user: Dict[str, Any] = Depends(require_admin_access)
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

@router.post("/users", response_model=UserActionResponse)
async def create_user(
    user_data: UserCreateRequest,
    admin_service: AdminService = Depends(get_admin_service),
    admin_user: Dict[str, Any] = Depends(require_admin_access)
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
        logger.error(f"Get activity logs error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get activity logs: {str(e)}")

@router.get("/activity-stats")
async def get_activity_stats(
    days: int = Query(7, ge=1, le=365, description="Number of days to analyze"),
    db = Depends(get_database),
    admin_user: Dict[str, Any] = Depends(require_admin_access)
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