import psycopg2
from psycopg2.extras import RealDictCursor
import os

conn = psycopg2.connect(
    host=os.getenv('DB_HOST', 'dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com'),
    port=int(os.getenv('DB_PORT', 5432)),
    database=os.getenv('DB_NAME', 'ecommerce_dss'),
    user=os.getenv('DB_USER', 'dss_user'),
    password=os.getenv('DB_PASSWORD', 'IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4')
)

cursor = conn.cursor(cursor_factory=RealDictCursor)

# Check what tables exist in ml schema
print('=== Tables in ML Schema ===')
cursor.execute("""
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'ml'
ORDER BY table_name
""")
tables = cursor.fetchall()
if tables:
    for t in tables:
        print(f"  - {t['table_name']}")
else:
    print("  (no tables found)")

# Check if dwh tables exist
print('\n=== Tables in DWH Schema ===')
cursor.execute("""
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'dwh'
ORDER BY table_name
""")
tables = cursor.fetchall()
if tables:
    for t in tables:
        print(f"  - {t['table_name']}")
else:
    print("  (no tables found)")

# Check public schema
print('\n=== Tables in Public Schema ===')
cursor.execute("""
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public'
ORDER BY table_name
""")
tables = cursor.fetchall()
if tables:
    for t in tables:
        print(f"  - {t['table_name']}")
else:
    print("  (no tables found)")

cursor.close()
conn.close()
