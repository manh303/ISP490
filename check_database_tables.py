# -*- coding: utf-8 -*-
import psycopg2
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

PG_HOST = "dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com"
PG_PORT = "5432"
PG_DB = "ecommerce_dss_1"
PG_USER = "dss_user"
PG_PASS = "6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G"

conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASS)
cur = conn.cursor()

print("[DATABASE] Checking all tables...\n")

# Get all tables
cur.execute("SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename")
tables = [row[0] for row in cur.fetchall()]

print(f"Total tables: {len(tables)}\n")

# Categorize tables
stg_tables = [t for t in tables if t.startswith('stg_')]
ods_tables = [t for t in tables if t.startswith('ods_')]
dim_tables = [t for t in tables if t.startswith('dim_')]
fact_tables = [t for t in tables if t.startswith('fact_')]
mart_tables = [t for t in tables if t.startswith('mart_')]
other_tables = [t for t in tables if not any(t.startswith(p) for p in ['stg_', 'ods_', 'dim_', 'fact_', 'mart_'])]

print("=" * 80)
print("STAGING TABLES (STG)")
print("=" * 80)
for t in stg_tables:
    cur.execute(f"SELECT COUNT(*) FROM {t}")
    count = cur.fetchone()[0]
    print(f"  {t}: {count} rows")

print("\n" + "=" * 80)
print("OPERATIONAL DATA STORE (ODS)")
print("=" * 80)
for t in ods_tables:
    cur.execute(f"SELECT COUNT(*) FROM {t}")
    count = cur.fetchone()[0]
    print(f"  {t}: {count} rows")

print("\n" + "=" * 80)
print("DIMENSION TABLES (DIM)")
print("=" * 80)
for t in dim_tables:
    cur.execute(f"SELECT COUNT(*) FROM {t}")
    count = cur.fetchone()[0]
    print(f"  {t}: {count} rows")

print("\n" + "=" * 80)
print("FACT TABLES (FACT)")
print("=" * 80)
for t in fact_tables:
    cur.execute(f"SELECT COUNT(*) FROM {t}")
    count = cur.fetchone()[0]
    print(f"  {t}: {count} rows")

print("\n" + "=" * 80)
print("DATAMART TABLES (MART)")
print("=" * 80)
for t in mart_tables:
    cur.execute(f"SELECT COUNT(*) FROM {t}")
    count = cur.fetchone()[0]
    print(f"  {t}: {count} rows")

print("\n" + "=" * 80)
print("OTHER TABLES")
print("=" * 80)
for t in other_tables:
    cur.execute(f"SELECT COUNT(*) FROM {t}")
    count = cur.fetchone()[0]
    print(f"  {t}: {count} rows")

# Check table schemas for key tables
print("\n" + "=" * 80)
print("TABLE SCHEMAS (Key Tables)")
print("=" * 80)

key_tables = ['ods_product_clean', 'ods_review_clean']
for table in key_tables:
    if table in tables:
        print(f"\n{table}:")
        cur.execute(f"""
            SELECT column_name, data_type, character_maximum_length 
            FROM information_schema.columns 
            WHERE table_name = '{table}' 
            ORDER BY ordinal_position
        """)
        for col in cur.fetchall():
            col_name, data_type, max_len = col
            if max_len:
                print(f"  - {col_name}: {data_type}({max_len})")
            else:
                print(f"  - {col_name}: {data_type}")

conn.close()
print("\n[DONE]")
