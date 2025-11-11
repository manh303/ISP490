#!/usr/bin/env python3
import asyncio
import asyncpg

async def get_user_ids():
    conn = await asyncpg.connect('postgresql://dss_user:IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4@dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com/ecommerce_dss')
    
    users = await conn.fetch('''
        SELECT u.user_id, u.email, u.full_name, r.role_code
        FROM iam_user u
        JOIN iam_user_role ur ON u.user_id = ur.user_id  
        JOIN iam_role r ON ur.role_id = r.role_id
        ORDER BY u.email
    ''')
    
    print("User ID mapping:")
    for user in users:
        print(f"  {user['email']} -> user_id: {user['user_id']}, role: {user['role_code']}")
    
    await conn.close()

asyncio.run(get_user_ids())