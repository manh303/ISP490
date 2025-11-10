"""
Role Management API Endpoints
"""
from fastapi import APIRouter, HTTPException, Depends, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field
from datetime import datetime
import logging

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
# PYDANTIC MODELS
# ====================================

class RoleResponse(BaseModel):
    """Role response model"""
    role_id: int
    role_code: str
    role_name: str
    description: Optional[str]
    is_active: bool
    created_at: datetime
    updated_at: datetime

class RoleDetailResponse(BaseModel):
    """Detailed role response with permissions"""
    role_id: int
    role_code: str
    role_name: str
    description: Optional[str]
    is_active: bool
    permissions: List[str]
    modules: List[str]
    actions: List[str]
    admin_features: Dict[str, bool]
    user_count: int
    created_at: datetime
    updated_at: datetime

class RoleCreateRequest(BaseModel):
    """Create role request"""
    role_code: str = Field(..., max_length=50, description="Role code (e.g., MANAGER)")
    role_name: str = Field(..., max_length=100, description="Role display name")
    description: Optional[str] = Field(None, max_length=255, description="Role description")

class RoleUpdateRequest(BaseModel):
    """Update role request"""
    role_name: Optional[str] = Field(None, max_length=100, description="Role display name")
    description: Optional[str] = Field(None, max_length=255, description="Role description")
    is_active: Optional[bool] = Field(None, description="Role active status")

class RoleListResponse(BaseModel):
    """Role list response"""
    success: bool
    data: List[RoleResponse]
    total: int
    page: int
    limit: int

class RoleActionResponse(BaseModel):
    """Role action response"""
    success: bool
    message: str
    role_id: Optional[int] = None

# ====================================
# DEPENDENCIES
# ====================================

# Temporarily disabled for debugging
# async def require_admin_access(credentials: HTTPAuthorizationCredentials = Depends(security)) -> Dict[str, Any]:

async def get_database():
    """Get database connection"""
    import sys
    import os
    sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
    from main import db_manager
    if not db_manager.is_connected:
        await db_manager.connect()
    return db_manager

# ====================================
# ROLE ENDPOINTS
# ====================================





@router.get("/", response_model=RoleListResponse, 
           summary="Get All Roles",
           description="Get paginated list of all roles in the system")
async def get_roles(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(20, ge=1, le=100, description="Items per page"),
    active_only: bool = Query(False, description="Show only active roles"),
    db = Depends(get_database)
):
    """Get list of all roles"""
    try:
        offset = (page - 1) * limit
        
        # Simple query without parameters first
        if active_only:
            query = """
            SELECT role_id, role_code, role_name, description, is_active, created_at, updated_at
            FROM iam_role
            WHERE is_active = true
            ORDER BY role_code
            """
            count_query = "SELECT COUNT(*) as total FROM iam_role WHERE is_active = true"
        else:
            query = """
            SELECT role_id, role_code, role_name, description, is_active, created_at, updated_at
            FROM iam_role
            ORDER BY role_code
            """
            count_query = "SELECT COUNT(*) as total FROM iam_role"
        
        logger.info(f"Executing query: {query}")
        all_roles = await db.execute_query(query)
        logger.info(f"Query returned {len(all_roles)} roles: {all_roles}")
        
        # Manual pagination - fix the slicing
        start_idx = offset
        end_idx = offset + limit
        roles = all_roles[start_idx:end_idx] if all_roles else []
        logger.info(f"Paginated roles ({start_idx}:{end_idx}): {len(roles)} items")
        
        # Get total count
        count_result = await db.execute_query(count_query)
        total = count_result[0]['total'] if count_result else 0
        logger.info(f"Total count: {total}")
        
        # Convert roles to RoleResponse objects
        role_responses = []
        for role in roles:
            try:
                role_response = RoleResponse(**role)
                role_responses.append(role_response)
            except Exception as e:
                logger.error(f"Error converting role {role}: {e}")
        
        logger.info(f"Successfully converted {len(role_responses)} roles")
        
        return RoleListResponse(
            success=True,
            data=role_responses,
            total=total,
            page=page,
            limit=limit
        )
        
    except Exception as e:
        logger.error(f"Get roles error: {e}")
        raise HTTPException(status_code=500, detail="Failed to get roles")

@router.get("/{role_id}", response_model=RoleDetailResponse,
           summary="🔍 Get Role Details",
           description="Get detailed information about a specific role")
async def get_role_detail(
    role_id: int,
    db = Depends(get_database)
):
    """Get detailed role information"""
    try:
        # Get role basic info
        role_query = """
        SELECT role_id, role_code, role_name, description, is_active, created_at, updated_at
        FROM iam_role
        WHERE role_id = $1
        """
        role_result = await db.execute_query(role_query, (role_id,))
        
        if not role_result:
            raise HTTPException(status_code=404, detail="Role not found")
        
        role = role_result[0]
        
        # Get user count for this role
        user_count_query = """
        SELECT COUNT(*) as user_count
        FROM iam_user_role ur
        JOIN iam_user u ON ur.user_id = u.user_id
        WHERE ur.role_id = $1 AND u.status = 'active'
        """
        user_count_result = await db.execute_query(user_count_query, (role_id,))
        user_count = user_count_result[0]['user_count'] if user_count_result else 0
        
        # Get role configuration from constants
        from constants.roles import ROLE_MENUS, get_role_menu
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
            user_count=user_count,
            created_at=role['created_at'],
            updated_at=role['updated_at']
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get role detail error: {e}")
        raise HTTPException(status_code=500, detail="Failed to get role details")

@router.get("/check/database", 
           summary="🔍 Check Database Roles",
           description="Check what roles exist in the database")
async def check_database_roles(
    db = Depends(get_database)
):
    """Check roles in database"""
    try:
        # First check if table exists
        table_check = """
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name = 'iam_role'
        """
        table_result = await db.execute_query(table_check)
        
        if not table_result:
            return {
                "success": False,
                "message": "Table 'iam_role' does not exist",
                "data": {"table_exists": False}
            }
        
        # Get all roles from database
        query = """
        SELECT role_id, role_code, role_name, description, is_active, created_at, updated_at
        FROM iam_role
        ORDER BY role_code
        """
        
        logger.info(f"Executing check query: {query}")
        roles = await db.execute_query(query)
        logger.info(f"Check query returned: {roles}")
        
        # Get user counts per role
        user_count_query = """
        SELECT r.role_code, COUNT(ur.user_id) as user_count
        FROM iam_role r
        LEFT JOIN iam_user_role ur ON r.role_id = ur.role_id
        LEFT JOIN iam_user u ON ur.user_id = u.user_id AND u.status = 'active'
        GROUP BY r.role_id, r.role_code
        ORDER BY r.role_code
        """
        
        user_counts = await db.execute_query(user_count_query)
        user_count_map = {row['role_code']: row['user_count'] for row in user_counts}
        
        # Add user counts to roles
        for role in roles:
            role['user_count'] = user_count_map.get(role['role_code'], 0)
        
        return {
            "success": True,
            "message": f"Found {len(roles)} roles in database",
            "data": {
                "table_exists": True,
                "roles": roles,
                "total_roles": len(roles),
                "active_roles": len([r for r in roles if r.get('is_active', False)]),
                "inactive_roles": len([r for r in roles if not r.get('is_active', True)])
            }
        }
        
    except Exception as e:
        logger.error(f"Check database roles error: {e}")
        raise HTTPException(status_code=500, detail="Failed to check database roles")

@router.post("/", response_model=RoleActionResponse,
           summary="➕ Create New Role",
           description="Create a new role in the system")
async def create_role(
    role_data: RoleCreateRequest,
    db = Depends(get_database)
):
    """Create new role"""
    try:
        # Check if role code already exists
        check_query = "SELECT role_id FROM iam_role WHERE role_code = $1"
        existing = await db.execute_query(check_query, (role_data.role_code.upper(),))
        
        if existing:
            raise HTTPException(status_code=400, detail="Role code already exists")
        
        # Create role
        insert_query = """
        INSERT INTO iam_role (role_code, role_name, description, is_active, created_at, updated_at)
        VALUES ($1, $2, $3, true, NOW(), NOW())
        RETURNING role_id
        """
        
        result = await db.execute_query(insert_query, (
            role_data.role_code.upper(),
            role_data.role_name,
            role_data.description
        ))
        
        if not result:
            raise HTTPException(status_code=500, detail="Failed to create role")
        
        role_id = result[0]['role_id']
        
        return RoleActionResponse(
            success=True,
            message=f"Role '{role_data.role_code}' created successfully",
            role_id=role_id
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Create role error: {e}")
        raise HTTPException(status_code=500, detail="Failed to create role")

@router.put("/{role_id}", response_model=RoleActionResponse,
           summary="✏️ Update Role",
           description="Update role information")
async def update_role(
    role_id: int,
    role_data: RoleUpdateRequest,
    db = Depends(get_database)
):
    """Update role"""
    try:
        # Check if role exists
        check_query = "SELECT role_id FROM iam_role WHERE role_id = $1"
        existing = await db.execute_query(check_query, (role_id,))
        
        if not existing:
            raise HTTPException(status_code=404, detail="Role not found")
        
        # Build update query
        update_fields = []
        values = []
        param_count = 1
        
        if role_data.role_name is not None:
            update_fields.append(f"role_name = ${param_count}")
            values.append(role_data.role_name)
            param_count += 1
            
        if role_data.description is not None:
            update_fields.append(f"description = ${param_count}")
            values.append(role_data.description)
            param_count += 1
            
        if role_data.is_active is not None:
            update_fields.append(f"is_active = ${param_count}")
            values.append(role_data.is_active)
            param_count += 1
        
        if not update_fields:
            raise HTTPException(status_code=400, detail="No fields to update")
        
        update_fields.append(f"updated_at = NOW()")
        values.append(role_id)
        
        update_query = f"""
        UPDATE iam_role 
        SET {', '.join(update_fields)}
        WHERE role_id = ${param_count}
        """
        
        await db.execute_query(update_query, values)
        
        return RoleActionResponse(
            success=True,
            message="Role updated successfully",
            role_id=role_id
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Update role error: {e}")
        raise HTTPException(status_code=500, detail="Failed to update role")

@router.post("/setup",
           summary="🔧 Setup Default Roles",
           description="Create default roles in database")
async def setup_default_roles(
    db = Depends(get_database)
):
    """Setup default roles in database"""
    try:
        # Default roles to create
        default_roles = [
            {"role_code": "SUPER_ADMIN", "role_name": "Super Administrator", "description": "Full system access"},
            {"role_code": "ADMIN", "role_name": "Administrator", "description": "System administration"},
            {"role_code": "MANAGER", "role_name": "Business Manager", "description": "Business operations"},
            {"role_code": "ANALYST", "role_name": "Data Analyst", "description": "Data analysis and reporting"},
            {"role_code": "CUSTOMER", "role_name": "Customer", "description": "Basic customer access"},
            {"role_code": "VIEWER", "role_name": "Viewer", "description": "Read-only access"}
        ]
        
        created_roles = []
        
        for role in default_roles:
            # Check if role exists
            check_query = "SELECT role_id FROM iam_role WHERE role_code = $1"
            existing = await db.execute_query(check_query, (role["role_code"],))
            
            if not existing:
                # Create role
                insert_query = """
                INSERT INTO iam_role (role_code, role_name, description, is_active, created_at, updated_at)
                VALUES ($1, $2, $3, true, NOW(), NOW())
                RETURNING role_id
                """
                result = await db.execute_query(insert_query, (
                    role["role_code"],
                    role["role_name"],
                    role["description"]
                ))
                
                if result:
                    created_roles.append({
                        "role_id": result[0]["role_id"],
                        "role_code": role["role_code"],
                        "status": "created"
                    })
            else:
                created_roles.append({
                    "role_id": existing[0]["role_id"],
                    "role_code": role["role_code"],
                    "status": "already_exists"
                })
        
        return {
            "success": True,
            "message": f"Setup completed. {len([r for r in created_roles if r['status'] == 'created'])} roles created.",
            "data": {
                "roles": created_roles,
                "total_processed": len(created_roles)
            }
        }
        
    except Exception as e:
        logger.error(f"Setup roles error: {e}")
        raise HTTPException(status_code=500, detail="Failed to setup roles")

@router.get("/users/{role_id}",
           summary="👥 Get Role Users",
           description="Get list of users assigned to a specific role")
async def get_role_users(
    role_id: int,
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(20, ge=1, le=100, description="Items per page"),
    db = Depends(get_database)
):
    """Get users assigned to a role"""
    try:
        offset = (page - 1) * limit
        
        # Get users with this role
        query = """
        SELECT u.user_id, u.email, u.full_name, u.status, u.last_login_at, ur.assigned_at
        FROM iam_user u
        JOIN iam_user_role ur ON u.user_id = ur.user_id
        WHERE ur.role_id = $1
        ORDER BY ur.assigned_at DESC
        LIMIT $2 OFFSET $3
        """
        
        users = await db.execute_query(query, (role_id, limit, offset))
        
        # Get total count
        count_query = """
        SELECT COUNT(*) as total
        FROM iam_user u
        JOIN iam_user_role ur ON u.user_id = ur.user_id
        WHERE ur.role_id = $1
        """
        count_result = await db.execute_query(count_query, (role_id,))
        total = count_result[0]['total'] if count_result else 0
        
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