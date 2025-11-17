"""
Role Management API Endpoints
"""
from fastapi import APIRouter, HTTPException, Depends, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Dict, Any
import logging

# Import models and services
from models.role import (
    RoleResponse, RoleDetailResponse, RoleCreateRequest, 
    RoleUpdateRequest, RoleListResponse, RoleActionResponse
)
from app.services.role_service import RoleService

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/roles", 
    tags=["Role Management"],
    responses={
        401: {"description": "Unauthorized - Admin access required"},
        403: {"description": "Forbidden - Invalid admin token"},
        500: {"description": "Internal server error"}
    }
)

security = HTTPBearer()

# ====================================
# DEPENDENCIES
# ====================================

async def get_database():
    """Get database connection"""
    import sys
    import os
    sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
    from main import db_manager
    if not db_manager.is_connected:
        await db_manager.connect()
    return db_manager

async def get_role_service(db = Depends(get_database)) -> RoleService:
    """Get role service"""
    return RoleService(db)

# ====================================
# ROLE ENDPOINTS
# ====================================



@router.get("/", response_model=RoleListResponse, 
           summary="Get All Roles",
           description="Get paginated list of all roles in the system")
async def get_roles(
    active_only: bool = Query(False, description="Show only active roles"),
    role_service: RoleService = Depends(get_role_service)
):
    """Get list of all roles"""
    try:
        roles = await role_service.get_roles(active_only)
        
        # Convert roles to RoleResponse objects
        role_responses = []
        for role in roles:
            try:
                role_response = RoleResponse(**role)
                role_responses.append(role_response)
            except Exception as e:
                logger.error(f"Error converting role {role}: {e}")
        
        return RoleListResponse(
            success=True,
            data=role_responses,
            total=len(roles),
            page=1,
            limit=len(roles)
        )
        
    except Exception as e:
        logger.error(f"Get roles error: {e}")
        raise HTTPException(status_code=500, detail="Failed to get roles")

@router.get("/{role_id}", response_model=RoleDetailResponse,
           summary="🔍 Get Role Details",
           description="Get detailed information about a specific role")
async def get_role_detail(
    role_id: int,
    role_service: RoleService = Depends(get_role_service)
):
    """Get detailed role information"""
    try:
        role = await role_service.get_role_by_id(role_id)
        if not role:
            raise HTTPException(status_code=404, detail="Role not found")
        
        user_count = await role_service.get_role_user_count(role_id)
        
        # Get role configuration from constants
        from app.constants.roles import get_role_menu
        role_config = get_role_menu(role['role_code'])
        
        return RoleDetailResponse(
            role_id=role['role_id'],
            role_code=role['role_code'],
            role_name=role['role_name'],
            description=role['description'],
            is_active=role['is_active'],
            permissions=role_config.get('permissions', []),
            modules=role_config.get('modules', []),
            actions=role_config.get('actions', []),
            admin_features=role_config.get('admin_features', {}),
            user_count=user_count
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get role detail error: {e}")
        raise HTTPException(status_code=500, detail="Failed to get role details")

@router.post("/", response_model=RoleActionResponse,
           summary="➕ Create New Role",
           description="Create a new role in the system")
async def create_role(
    role_data: RoleCreateRequest,
    role_service: RoleService = Depends(get_role_service)
):
    """Create new role"""
    try:
        role_id = await role_service.create_role(role_data)
        
        return RoleActionResponse(
            success=True,
            message=f"Role '{role_data.role_code}' created successfully",
            role_id=role_id
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Create role error: {e}")
        raise HTTPException(status_code=500, detail="Failed to create role")

@router.put("/{role_id}", response_model=RoleActionResponse,
           summary="✏️ Update Role",
           description="Update role information")
async def update_role(
    role_id: int,
    role_data: RoleUpdateRequest,
    role_service: RoleService = Depends(get_role_service)
):
    """Update role"""
    try:
        await role_service.update_role(role_id, role_data)
        
        return RoleActionResponse(
            success=True,
            message="Role updated successfully",
            role_id=role_id
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Update role error: {e}")
        raise HTTPException(status_code=500, detail="Failed to update role")

@router.patch("/{role_id}/deactivate", response_model=RoleActionResponse,
            summary="🚫 Deactivate Role",
            description="Deactivate a role (users keep role but it becomes inactive)")
async def deactivate_role(
    role_id: int,
    role_service: RoleService = Depends(get_role_service)
):
    """Deactivate role"""
    try:
        role_code = await role_service.deactivate_role(role_id)
        
        return RoleActionResponse(
            success=True,
            message=f"Role '{role_code}' deactivated successfully",
            role_id=role_id
        )
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Deactivate role error: {e}")
        raise HTTPException(status_code=500, detail="Failed to deactivate role")

@router.get("/users/{role_id}",
           summary="👥 Get Role Users",
           description="Get list of users assigned to a specific role")
async def get_role_users(
    role_id: int,
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(20, ge=1, le=100, description="Items per page"),
    role_service: RoleService = Depends(get_role_service)
):
    """Get users assigned to a role"""
    try:
        users, total = await role_service.get_role_users(role_id, page, limit)
        
        return {
            "success": True,
            "data": {
                "users": users,
                "total": total,
                "page": page,
                "limit": limit
            }
        }
        
    except Exception as e:
        logger.error(f"Get role users error: {e}")
        raise HTTPException(status_code=500, detail="Failed to get role users")

@router.patch("/{role_id}/activate", response_model=RoleActionResponse,
            summary="✅ Activate Role",
            description="Activate a deactivated role")
async def activate_role(
    role_id: int,
    role_service: RoleService = Depends(get_role_service)
):
    """Activate role"""
    try:
        role_code = await role_service.activate_role(role_id)
        
        return RoleActionResponse(
            success=True,
            message=f"Role '{role_code}' activated successfully",
            role_id=role_id
        )
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Activate role error: {e}")
        raise HTTPException(status_code=500, detail="Failed to activate role")

@router.delete("/{role_id}", response_model=RoleActionResponse,
             summary="🗑️ Delete Role",
             description="Delete role if no users are assigned to it")
async def delete_role(
    role_id: int,
    role_service: RoleService = Depends(get_role_service)
):
    """Delete role if no users assigned"""
    try:
        role_code = await role_service.delete_role(role_id)
        
        return RoleActionResponse(
            success=True,
            message=f"Role '{role_code}' deleted successfully",
            role_id=role_id
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Delete role error: {e}")
        raise HTTPException(status_code=500, detail="Failed to delete role")

