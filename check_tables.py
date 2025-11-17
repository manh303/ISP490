from utils.db_connector import DWHConnector

conn = DWHConnector()

# Check fact_product_daily_agg columns
print("=== fact_product_daily_agg columns ===")
result = conn.query("""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_schema='dwh' AND table_name='fact_product_daily_agg'
    ORDER BY ordinal_position
""")
if not result.empty:
    for idx, row in result.iterrows():
        print(f"  {row['column_name']}: {row['data_type']}")
else:
    print("  Table not found")

print("\n=== fact_review_daily_agg columns ===")
result = conn.query("""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_schema='dwh' AND table_name='fact_review_daily_agg'
    ORDER BY ordinal_position
""")
if not result.empty:
    for idx, row in result.iterrows():
        print(f"  {row['column_name']}: {row['data_type']}")
else:
    print("  Table not found")

conn.close()
