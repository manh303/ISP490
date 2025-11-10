#!/usr/bin/env python3
"""
Script để xóa SUPER_ADMIN và điều chỉnh lại role_id
"""
import asyncio
import sys
import os

sys.path.append(os.path.dirname(__file__))
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from app.main import db_manager

async def remove_super_admin():
    """Xóa SUPER_ADMIN và điều chỉnh lại role_id"""
    try:
        print("[INFO] Connecting to database...")
        await db_manager.connect()
        
        if not db_manager.is_connected:
            print("[ERROR] Database connection failed!")
            return
            
        print("[SUCCESS] Database connected!")
        
        # Check current roles
        print("\n[INFO] Current roles:")
        current_query = "SELECT role_id, role_code, role_name FROM iam_role ORDER BY role_id"
        current_roles = await db_manager.execute_query(current_query)
        for role in current_roles:
            print(f"  - ID {role['role_id']}: {role['role_code']} - {role['role_name']}")
        
        # Delete SUPER_ADMIN role
        print("\n[INFO] Deleting SUPER_ADMIN role...")
        delete_query = "DELETE FROM iam_role WHERE role_code = 'SUPER_ADMIN'"
        await db_manager.execute_query(delete_query)
        print("[SUCCESS] SUPER_ADMIN deleted!")
        
        # Reset sequence and update role_ids
        print("\n[INFO] Updating role IDs...")
        
        # Create new roles with correct IDs
        new_roles = [
            (1, 'ADMIN', 'Administrator', 'System administration'),
            (2, 'ANALYST', 'Data Analyst', 'Data analysis'),
            (3, 'MANAGER', 'Business Manager', 'Business operations'),
            (4, 'CUSTOMER', 'Customer', 'Basic access'),
            (5, 'VIEWER', 'Viewer', 'Read-only access')
        ]
        
        # Delete all existing roles
        await db_manager.execute_query("DELETE FROM iam_role")
        
        # Reset sequence
        await db_manager.execute_query("ALTER SEQUENCE iam_role_role_id_seq RESTART WITH 1")
        
        # Insert roles with correct IDs
        for role_id, role_code, role_name, description in new_roles:
            insert_query = """
            INSERT INTO iam_role (role_id, role_code, role_name, description, is_active, created_at, updated_at)
            VALUES (:role_id, :role_code, :role_name, :description, true, NOW(), NOW())
            """
            await db_manager.execute_query(insert_query, {
                'role_id': role_id,
                'role_code': role_code,
                'role_name': role_name,
                'description': description
            })
            print(f"  [SUCCESS] Created ID {role_id}: {role_code}")
        
        # Update sequence to continue from 6
        await db_manager.execute_query("SELECT setval('iam_role_role_id_seq', 5)")
        
        # Final check
        print("\n[INFO] Final roles:")
        final_roles = await db_manager.execute_query(current_query)
        for role in final_roles:
            print(f"  - ID {role['role_id']}: {role['role_code']} - {role['role_name']}")
        
        print("\n[SUCCESS] Role IDs updated successfully!")
        
    except Exception as e:
        print(f"[ERROR] Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await db_manager.disconnect()
        print("[INFO] Database disconnected")

if __name__ == "__main__":
    asyncio.run(remove_super_admin())