import os
import sys
import asyncio

# Add backend to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'backend'))

from app.main import db_manager

async def create_mock_dwh():
    await db_manager.connect()

    print("Creating mock DWH tables...")

    # Create tables in public schema (since dwh schema might not exist)
    await db_manager.execute_query("""
    CREATE TABLE IF NOT EXISTS dwh_fact_product_daily (
        product_sk BIGINT,
        price_current DECIMAL(15,2),
        rating_avg DECIMAL(3,2),
        review_count INTEGER,
        sold_count INTEGER,
        discount_pct DECIMAL(5,2),
        date_sk INTEGER
    )
    """)

    await db_manager.execute_query("""
    CREATE TABLE IF NOT EXISTS dwh_dim_product (
        product_sk BIGINT PRIMARY KEY,
        global_product_id VARCHAR(100),
        category_sk INTEGER
    )
    """)

    await db_manager.execute_query("""
    CREATE TABLE IF NOT EXISTS dwh_dim_category (
        category_sk INTEGER PRIMARY KEY,
        category_code VARCHAR(50)
    )
    """)

    await db_manager.execute_query("""
    CREATE TABLE IF NOT EXISTS dwh_dim_date (
        date_sk INTEGER PRIMARY KEY,
        date_value DATE
    )
    """)

    await db_manager.execute_query("""
    CREATE TABLE IF NOT EXISTS dwh_fact_product_daily_agg (
        product_sk BIGINT,
        price_current DECIMAL(15,2),
        rating_avg DECIMAL(3,2),
        review_count INTEGER,
        date_sk INTEGER
    )
    """)

    print("Mock tables created. Inserting sample data...")

    # Insert sample data
    await db_manager.execute_query("""
    INSERT INTO dwh_dim_product (product_sk, global_product_id, category_sk)
    VALUES (1, 'PROD001', 1), (2, 'PROD002', 1)
    ON CONFLICT DO NOTHING
    """)

    await db_manager.execute_query("""
    INSERT INTO dwh_dim_category (category_sk, category_code)
    VALUES (1, 'electronics')
    ON CONFLICT DO NOTHING
    """)

    await db_manager.execute_query("""
    INSERT INTO dwh_dim_date (date_sk, date_value)
    VALUES (20241117, '2024-11-17')
    ON CONFLICT DO NOTHING
    """)

    await db_manager.execute_query("""
    INSERT INTO dwh_fact_product_daily (product_sk, price_current, rating_avg, review_count, sold_count, discount_pct, date_sk)
    VALUES
    (1, 25000000, 4.8, 500, 200, 5, 20241117),
    (2, 8000000, 4.0, 100, 50, 15, 20241117)
    """)

    await db_manager.execute_query("""
    INSERT INTO dwh_fact_product_daily_agg (product_sk, price_current, rating_avg, review_count, date_sk)
    VALUES
    (1, 25000000, 4.8, 500, 20241117),
    (2, 8000000, 4.0, 100, 20241117)
    """)

    print("Sample data inserted successfully")

if __name__ == "__main__":
    asyncio.run(create_mock_dwh())
