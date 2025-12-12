import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="ecommerce_dss",
    user="dss_user",
    password="dss_password_123"
)

cursor = conn.cursor()

print('=== Available schemas ===')
cursor.execute("""
    SELECT schema_name
    FROM information_schema.schemata
    WHERE schema_name NOT LIKE 'pg_%' AND schema_name != 'information_schema'
    ORDER BY schema_name
""")
schemas = cursor.fetchall()
for schema in schemas:
    print(f'  {schema[0]}')

print('\n=== Tables in ml schema ===')
cursor.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema='ml'
    ORDER BY table_name
""")
tables = cursor.fetchall()
if tables:
    for table in tables:
        print(f'  {table[0]}')
else:
    print('  No tables found in ml schema')

cursor.close()
conn.close()
