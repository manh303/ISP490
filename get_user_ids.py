#!/usr/bin/env python3
import asyncio
import asyncpg

async def get_user_ids():
    conn = await asyncpg.connect('postgresql://dss_user:6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G@dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com/ecommerce_dss_1')
    
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