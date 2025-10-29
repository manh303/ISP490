"""
Test Admin Endpoints - Simple working version
"""
from fastapi import APIRouter, HTTPException
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

router = APIRouter(prefix="/test-admin", tags=["🧪 Test Admin"])

@router.get("/users")
async def test_get_users():
    """Test get users directly from database"""
    try:
        from main import db_manager
        
        if not db_manager.is_connected:
            await db_manager.connect()
            
        query = """
        SELECT u.user_id, u.email, u.full_name, u.phone, u.status,
               u.created_at, r.role_code, r.role_name
        FROM iam_user u
        LEFT JOIN iam_user_role ur ON u.user_id = ur.user_id
        LEFT JOIN iam_role r ON ur.role_id = r.role_id
        WHERE u.status = 'active'
        ORDER BY u.created_at DESC
        """
        
        users = await db_manager.execute_query(query)
        
        return {
            "success": True,
            "message": f"Found {len(users)} active users",
            "data": users
        }
        
    except Exception as e:
        return {
            "success": False,
            "message": "Failed to get users",
            "error": str(e)
        }

@router.post("/users")
async def test_create_user():
    """Test create user"""
    try:
        from main import db_manager
        import bcrypt
        
        if not db_manager.is_connected:
            await db_manager.connect()
            
        # Create test user
        email = "test@example.com"
        password = "test123"
        password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        
        # Insert user
        user_query = """
        INSERT INTO iam_user (email, password_hash, full_name, status, created_at, updated_at)
        VALUES ($1, $2, $3, 'active', NOW(), NOW())
        ON CONFLICT (email) DO UPDATE SET updated_at = NOW()
        RETURNING user_id, email, full_name, status
        """
        
        result = await db_manager.execute_query(
            user_query, (email, password_hash, "Test User")
        )
        
        if result:
            user = result[0]
            # Assign CUSTOMER role
            role_query = """
            INSERT INTO iam_user_role (user_id, role_id, assigned_at)
            SELECT $1, role_id, NOW() FROM iam_role WHERE role_code = 'CUSTOMER'
            ON CONFLICT (user_id, role_id) DO NOTHING
            """
            await db_manager.execute_query(role_query, (user['user_id'],))
            
            return {
                "success": True,
                "message": "User created successfully",
                "data": user
            }
        else:
            return {
                "success": False,
                "message": "Failed to create user"
            }
            
    except Exception as e:
        return {
            "success": False,
            "message": "Failed to create user",
            "error": str(e)
        }

@router.put("/users/{user_id}/disable")
async def test_disable_user(user_id: int):
    """Test soft delete user"""
    try:
        from main import db_manager
        
        if not db_manager.is_connected:
            await db_manager.connect()
            
        query = """
        UPDATE iam_user 
        SET status = 'disabled', updated_at = NOW()
        WHERE user_id = $1 AND status = 'active'
        RETURNING user_id, email, status
        """
        
        result = await db_manager.execute_query(query, (user_id,))
        
        if result:
            return {
                "success": True,
                "message": "User disabled successfully",
                "data": result[0]
            }
        else:
            return {
                "success": False,
                "message": "User not found or already disabled"
            }
            
    except Exception as e:
        return {
            "success": False,
            "message": "Failed to disable user",
            "error": str(e)
        }

@router.get("/users/deleted")
async def test_get_deleted_users():
    """Test get deleted users"""
    try:
        from main import db_manager
        
        if not db_manager.is_connected:
            await db_manager.connect()
            
        query = """
        SELECT u.user_id, u.email, u.full_name, u.status, u.updated_at
        FROM iam_user u
        WHERE u.status = 'disabled'
        ORDER BY u.updated_at DESC
        """
        
        users = await db_manager.execute_query(query)
        
        return {
            "success": True,
            "message": f"Found {len(users)} deleted users",
            "data": users
        }
        
    except Exception as e:
        return {
            "success": False,
            "message": "Failed to get deleted users",
            "error": str(e)
        }

@router.put("/users/{user_id}/restore")
async def test_restore_user(user_id: int):
    """Test restore user"""
    try:
        from main import db_manager
        
        if not db_manager.is_connected:
            await db_manager.connect()
            
        query = """
        UPDATE iam_user 
        SET status = 'active', updated_at = NOW()
        WHERE user_id = $1 AND status = 'disabled'
        RETURNING user_id, email, status
        """
        
        result = await db_manager.execute_query(query, (user_id,))
        
        if result:
            return {
                "success": True,
                "message": "User restored successfully",
                "data": result[0]
            }
        else:
            return {
                "success": False,
                "message": "User not found or not in deleted list"
            }
            
    except Exception as e:
        return {
            "success": False,
            "message": "Failed to restore user",
            "error": str(e)
        }

@router.delete("/users/{user_id}/permanent")
async def test_permanent_delete(user_id: int, confirm: bool = False):
    """Test permanent delete user"""
    try:
        if not confirm:
            return {
                "success": False,
                "message": "Add ?confirm=true to permanently delete user"
            }
            
        from main import db_manager
        
        if not db_manager.is_connected:
            await db_manager.connect()
            
        # Check if user is in deleted list
        check_query = "SELECT user_id, email, status FROM iam_user WHERE user_id = $1"
        user_result = await db_manager.execute_query(check_query, (user_id,))
        
        if not user_result:
            return {
                "success": False,
                "message": "User not found"
            }
            
        user = user_result[0]
        if user['status'] != 'disabled':
            return {
                "success": False,
                "message": "User must be in deleted list before permanent deletion"
            }
        
        # Delete user roles first
        await db_manager.execute_query("DELETE FROM iam_user_role WHERE user_id = $1", (user_id,))
        
        # Delete user
        delete_result = await db_manager.execute_query(
            "DELETE FROM iam_user WHERE user_id = $1 RETURNING email", (user_id,)
        )
        
        if delete_result:
            return {
                "success": True,
                "message": f"User {delete_result[0]['email']} permanently deleted"
            }
        else:
            return {
                "success": False,
                "message": "Failed to delete user"
            }
            
    except Exception as e:
        return {
            "success": False,
            "message": "Failed to permanently delete user",
            "error": str(e)
        }