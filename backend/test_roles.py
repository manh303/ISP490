#!/usr/bin/env python3
"""
Test script để kiểm tra và khởi tạo roles trong database
"""
import asyncio
import sys
import os

# Add current directory to path
sys.path.append(os.path.dirname(__file__))
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from app.main import db_manager

async def test_and_init_roles():
    """Test database connection và khởi tạo roles"""
    try:
        print("[INFO] Connecting to database...")
        await db_manager.connect()
        
        if not db_manager.is_connected:
            print("[ERROR] Database connection failed!")
            return
            
        print("[SUCCESS] Database connected successfully!")
        
        # Test simple query
        print("\n[INFO] Testing database query...")
        test_query = "SELECT 1 as test"
        test_result = await db_manager.execute_query(test_query)
        print(f"Test query result: {test_result}")
        
        # Check if iam_role table exists
        print("\n[INFO] Checking if iam_role table exists...")
        table_check = """
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name = 'iam_role'
        """
        table_result = await db_manager.execute_query(table_check)
        
        if not table_result:
            print("[ERROR] Table 'iam_role' does not exist!")
            print("Creating iam_role table...")
            
            # Create table
            create_table = """
            CREATE TABLE IF NOT EXISTS iam_role (
                role_id SERIAL PRIMARY KEY,
                role_code VARCHAR(50) UNIQUE NOT NULL,
                role_name VARCHAR(100) NOT NULL,
                description TEXT,
                is_active BOOLEAN DEFAULT true,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
            """
            await db_manager.execute_query(create_table)
            print("[SUCCESS] Table created!")
        else:
            print("[SUCCESS] Table 'iam_role' exists!")
        
        # Check current roles
        print("\n[INFO] Checking current roles...")
        roles_query = "SELECT role_id, role_code, role_name FROM iam_role ORDER BY role_code"
        current_roles = await db_manager.execute_query(roles_query)
        
        print(f"Current roles count: {len(current_roles)}")
        for role in current_roles:
            print(f"  - {role['role_code']}: {role['role_name']} (ID: {role['role_id']})")
        
        # Insert default roles if none exist
        if len(current_roles) == 0:
            print("\n[INFO] Inserting default roles...")
            
            default_roles = [
                ('SUPER_ADMIN', 'Super Administrator', 'Full system access'),
                ('ADMIN', 'Administrator', 'System administration'),
                ('MANAGER', 'Business Manager', 'Business operations'),
                ('ANALYST', 'Data Analyst', 'Data analysis'),
                ('CUSTOMER', 'Customer', 'Basic access'),
                ('VIEWER', 'Viewer', 'Read-only access')
            ]
            
            for role_code, role_name, description in default_roles:
                insert_query = """
                INSERT INTO iam_role (role_code, role_name, description, is_active, created_at, updated_at)
                VALUES ($1, $2, $3, true, NOW(), NOW())
                """
                # Convert to named parameters for databases library
                converted_query = """
                INSERT INTO iam_role (role_code, role_name, description, is_active, created_at, updated_at)
                VALUES (:role_code, :role_name, :description, true, NOW(), NOW())
                """
                
                await db_manager.execute_query(converted_query, {
                    'role_code': role_code,
                    'role_name': role_name,
                    'description': description
                })
                print(f"  [SUCCESS] Inserted: {role_code}")
        
        # Final check
        print("\n[INFO] Final roles check...")
        final_roles = await db_manager.execute_query(roles_query)
        print(f"Total roles: {len(final_roles)}")
        
        for role in final_roles:
            print(f"  - {role['role_code']}: {role['role_name']} (ID: {role['role_id']})")
        
        print("\n[SUCCESS] Test completed successfully!")
        
    except Exception as e:
        print(f"[ERROR] Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await db_manager.disconnect()
        print("[INFO] Database disconnected")

if __name__ == "__main__":
    asyncio.run(test_and_init_roles())