#!/usr/bin/env python3
"""
Stage 5: Build Data Marts (DM Layer)
"""
import psycopg2
import os
from datetime import datetime

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'ecommerce_dss'),
    'user': os.getenv('DB_USER', 'dss_user'),
    'password': os.getenv('DB_PASSWORD', 'dss_password_123')
}

def create_datamart_tables(conn):
    """Create data mart tables"""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS meta_etl_log (
                log_id BIGSERIAL PRIMARY KEY,
                job_name VARCHAR(100) NOT NULL,
                stage VARCHAR(20) NOT NULL,
                status VARCHAR(20) NOT NULL,
                start_time TIMESTAMP NOT NULL,
                end_time TIMESTAMP,
                records_processed INT DEFAULT 0,
                records_failed INT DEFAULT 0,
                error_message TEXT,
                load_id VARCHAR(36)
            );
            
            CREATE TABLE IF NOT EXISTS dm_price_analytics (
                product_sk BIGINT NOT NULL,
                platform_sk INT NOT NULL,
                date_sk INT NOT NULL,
                price_current DECIMAL(15,2),
                price_original DECIMAL(15,2),
                discount_pct DECIMAL(5,2),
                competitor_min_price DECIMAL(15,2),
                competitor_max_price DECIMAL(15,2),
                price_rank INT,
                price_trend VARCHAR(20),
                PRIMARY KEY (product_sk, platform_sk, date_sk)
            );
        """)
        conn.commit()
        print("✅ Data mart tables created")

def build_price_analytics(conn):
    """Build price analytics data mart"""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO dm_price_analytics
            (product_sk, platform_sk, date_sk, price_current, price_original, discount_pct, 
             competitor_min_price, competitor_max_price, price_rank, price_trend)
            SELECT 
                f.product_sk,
                f.platform_sk,
                f.date_sk,
                f.price_current,
                f.price_original,
                f.discount_pct,
                comp.min_price,
                comp.max_price,
                RANK() OVER (PARTITION BY f.product_sk, f.date_sk ORDER BY f.price_current) as price_rank,
                CASE 
                    WHEN LAG(f.price_current) OVER (PARTITION BY f.product_sk, f.platform_sk ORDER BY f.date_sk) < f.price_current THEN 'increasing'
                    WHEN LAG(f.price_current) OVER (PARTITION BY f.product_sk, f.platform_sk ORDER BY f.date_sk) > f.price_current THEN 'decreasing'
                    ELSE 'stable'
                END as price_trend
            FROM dwh_fact_product_daily f
            LEFT JOIN (
                SELECT 
                    product_sk,
                    date_sk,
                    MIN(price_current) as min_price,
                    MAX(price_current) as max_price
                FROM dwh_fact_product_daily
                WHERE price_current > 0
                GROUP BY product_sk, date_sk
            ) comp ON f.product_sk = comp.product_sk AND f.date_sk = comp.date_sk
            WHERE f.price_current > 0
            ON CONFLICT (product_sk, platform_sk, date_sk) DO UPDATE SET
                price_current = EXCLUDED.price_current,
                price_original = EXCLUDED.price_original,
                discount_pct = EXCLUDED.discount_pct,
                competitor_min_price = EXCLUDED.competitor_min_price,
                competitor_max_price = EXCLUDED.competitor_max_price,
                price_rank = EXCLUDED.price_rank,
                price_trend = EXCLUDED.price_trend;
        """)
        count = cur.rowcount
        conn.commit()
        print(f"✅ Built price analytics: {count} records")
        return count

def log_etl(conn, job_name, stage, status, start_time, records_processed=0, error_msg=None):
    """Log ETL metadata"""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO meta_etl_log 
            (job_name, stage, status, start_time, end_time, records_processed, error_message)
            VALUES (%s, %s, %s, %s, NOW(), %s, %s)
        """, (job_name, stage, status, start_time, records_processed, error_msg))
        conn.commit()

def main():
    start_time = datetime.now()
    
    print("STAGE 5: Build Data Marts (DM)")
    print("=" * 60)
    
    conn = psycopg2.connect(**DB_CONFIG)
    print("✅ Connected to database")
    
    try:
        create_datamart_tables(conn)
        
        print("\n📊 Building Price Analytics Mart...")
        price_count = build_price_analytics(conn)
        
        log_etl(conn, 'datamart_build', 'DM', 'SUCCESS', start_time, price_count)
        
        print(f"\n✅ COMPLETE! Total records: {price_count}")
        print(f"📊 Metadata logged to meta_etl_log")
        
    except Exception as e:
        log_etl(conn, 'datamart_build', 'DM', 'FAILED', start_time, 0, str(e))
        print(f"\n❌ FAILED: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
