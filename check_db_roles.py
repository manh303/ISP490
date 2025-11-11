#!/usr/bin/env python3
"""
Check roles in the actual database
"""
import asyncio
import asyncpg
import os

DATABASE_URL = "postgresql://dss_user:IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4@dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com/ecommerce_dss"

async def check_database_roles():
    """Check what roles exist in the database"""
    try:
        # Connect to database
        conn = await asyncpg.connect(DATABASE_URL)
        print("[OK] Connected to database successfully!")
        
        # Check if iam_role table exists
        table_check = await conn.fetch("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'iam_role'
            );
        """)
        
        if not table_check[0]['exists']:
            print("[ERROR] Table 'iam_role' does not exist!")
            
            # Check what tables do exist
            tables = await conn.fetch("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name;
            """)
            
            print("\nAvailable tables:")
            for table in tables:
                print(f"  - {table['table_name']}")
            
            await conn.close()
            return
        
        print("[OK] Table 'iam_role' exists!")
        
        # Get all roles
        roles = await conn.fetch("""
            SELECT role_id, role_code, role_name, description 
            FROM iam_role 
            ORDER BY role_code;
        """)
        
        print(f"\nFound {len(roles)} roles in database:")
        print("-" * 60)
        for role in roles:
            print(f"ID: {role['role_id']:2d} | Code: {role['role_code']:12s} | Name: {role['role_name']}")
        
        # Check users and their roles
        user_roles = await conn.fetch("""
            SELECT 
                u.user_id,
                u.email,
                u.full_name,
                r.role_code,
                r.role_name
            FROM iam_user u
            JOIN iam_user_role ur ON u.user_id = ur.user_id
            JOIN iam_role r ON ur.role_id = r.role_id
            ORDER BY u.email, r.role_code;
        """)
        
        print(f"\nFound {len(user_roles)} user-role assignments:")
        print("-" * 80)
        for ur in user_roles:
            print(f"User: {ur['email']:20s} | Role: {ur['role_code']:12s} | Name: {ur['full_name']}")
        
        await conn.close()
        print("\n[OK] Database check completed!")
        
    except Exception as e:
        print(f"[ERROR] Database error: {e}")

if __name__ == "__main__":
    asyncio.run(check_database_roles())