#!/usr/bin/env python3
import psycopg2
import os

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://dss_user:IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4@dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com/ecommerce_dss"
)

import urllib.parse
result = urllib.parse.urlparse(DATABASE_URL)

conn = psycopg2.connect(
    host=result.hostname,
    port=result.port,
    database=result.path[1:],
    user=result.username,
    password=result.password
)

cursor = conn.cursor()
cursor.execute("""
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'iam' 
        AND table_name = 'user_activity_logs'
    )
""")
exists = cursor.fetchone()[0]
print("Table exists:", exists)

cursor.close()
conn.close()
