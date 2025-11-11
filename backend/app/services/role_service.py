"""
Role Service - Business Logic
"""
import logging
from typing import List, Dict, Any, Optional, Tuple
from models.role import RoleResponse, RoleDetailResponse, RoleCreateRequest, RoleUpdateRequest

logger = logging.getLogger(__name__)

class RoleService:
    def __init__(self, db):
        self.db = db

    async def get_roles(self, page: int = 1, limit: int = 20, active_only: bool = False) -> Tuple[List[Dict], int]:
        """Get paginated list of roles"""
        offset = (page - 1) * limit
        
        # Query with is_active column
        where_clause = "WHERE COALESCE(is_active, true) = true" if active_only else ""
        query = f"""
        SELECT role_id, role_code, role_name, description, 
               COALESCE(is_active, true) as is_active
        FROM iam_role
        {where_clause}
        ORDER BY role_code
        """
        count_where = "WHERE COALESCE(is_active, true) = true" if active_only else ""
        count_query = f"SELECT COUNT(*) as total FROM iam_role {count_where}"
        
        logger.info(f"Executing query: {query}")
        all_roles = await self.db.execute_query(query)
        logger.info(f"Query returned {len(all_roles)} roles")
        
        # Manual pagination
        start_idx = offset
        end_idx = offset + limit
        roles = all_roles[start_idx:end_idx] if all_roles else []
        
        # Get total count
        count_result = await self.db.execute_query(count_query)
        total = count_result[0]['total'] if count_result else 0
        
        return roles, total

    async def get_role_by_id(self, role_id: int) -> Optional[Dict]:
        """Get role by ID"""
        role_query = """
        SELECT role_id, role_code, role_name, description,
               COALESCE(is_active, true) as is_active
        FROM iam_role
        WHERE role_id = $1
        """
        role_result = await self.db.execute_query(role_query, (role_id,))
        return role_result[0] if role_result else None

    async def get_role_user_count(self, role_id: int) -> int:
        """Get number of users assigned to role"""
        user_count_query = """
        SELECT COUNT(*) as user_count
        FROM iam_user_role ur
        JOIN iam_user u ON ur.user_id = u.user_id
        WHERE ur.role_id = $1 AND u.status = 'active'
        """
        user_count_result = await self.db.execute_query(user_count_query, (role_id,))
        return user_count_result[0]['user_count'] if user_count_result else 0

    async def create_role(self, role_data: RoleCreateRequest) -> int:
        """Create new role"""
        # Check if role code already exists
        check_query = "SELECT role_id FROM iam_role WHERE role_code = $1"
        existing = await self.db.execute_query(check_query, (role_data.role_code.upper(),))
        
        if existing:
            raise ValueError("Role code already exists")
        
        # Create role
        insert_query = """
        INSERT INTO iam_role (role_code, role_name, description)
        VALUES ($1, $2, $3)
        RETURNING role_id
        """
        
        result = await self.db.execute_query(insert_query, (
            role_data.role_code.upper(),
            role_data.role_name,
            role_data.description
        ))
        
        if not result:
            raise Exception("Failed to create role")
        
        return result[0]['role_id']

    async def update_role(self, role_id: int, role_data: RoleUpdateRequest) -> bool:
        """Update role"""
        # Check if role exists
        existing = await self.get_role_by_id(role_id)
        if not existing:
            raise ValueError("Role not found")
        
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
        
        if not update_fields:
            raise ValueError("No fields to update")
        
        values.append(role_id)
        
        update_query = f"""
        UPDATE iam_role 
        SET {', '.join(update_fields)}
        WHERE role_id = ${param_count}
        """
        
        await self.db.execute_query(update_query, values)
        return True

    async def deactivate_role(self, role_id: int) -> str:
        """Deactivate role"""
        role = await self.get_role_by_id(role_id)
        if not role:
            raise ValueError("Role not found")
        
        update_query = """
        UPDATE iam_role 
        SET is_active = false
        WHERE role_id = $1
        """
        
        await self.db.execute_query(update_query, (role_id,))
        return role['role_code']

    async def activate_role(self, role_id: int) -> str:
        """Activate role"""
        role = await self.get_role_by_id(role_id)
        if not role:
            raise ValueError("Role not found")
        
        update_query = """
        UPDATE iam_role 
        SET is_active = true
        WHERE role_id = $1
        """
        
        await self.db.execute_query(update_query, (role_id,))
        return role['role_code']

    async def delete_role(self, role_id: int) -> str:
        """Delete role if no users assigned"""
        role = await self.get_role_by_id(role_id)
        if not role:
            raise ValueError("Role not found")
        
        # Check if any users have this role
        user_count_query = """
        SELECT COUNT(*) as user_count
        FROM iam_user_role
        WHERE role_id = $1
        """
        user_count_result = await self.db.execute_query(user_count_query, (role_id,))
        user_count = user_count_result[0]['user_count'] if user_count_result else 0
        
        if user_count > 0:
            raise ValueError(f"Cannot delete role '{role['role_code']}'. {user_count} users are assigned to this role. Deactivate the role instead.")
        
        # Delete role
        delete_query = "DELETE FROM iam_role WHERE role_id = $1"
        await self.db.execute_query(delete_query, (role_id,))
        return role['role_code']

    async def get_role_users(self, role_id: int, page: int = 1, limit: int = 20) -> Tuple[List[Dict], int]:
        """Get users assigned to role"""
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
        
        users = await self.db.execute_query(query, (role_id, limit, offset))
        
        # Get total count
        count_query = """
        SELECT COUNT(*) as total
        FROM iam_user u
        JOIN iam_user_role ur ON u.user_id = ur.user_id
        WHERE ur.role_id = $1
        """
        count_result = await self.db.execute_query(count_query, (role_id,))
        total = count_result[0]['total'] if count_result else 0
        
        return users, total