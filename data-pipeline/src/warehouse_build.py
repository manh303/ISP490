#!/usr/bin/env python3
"""
Stage 4: Build Data Warehouse - Star Schema (DWH Layer)
"""
import psycopg2
import os
from datetime import datetime

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'ecommerce_dss_1'),
    'user': os.getenv('DB_USER', 'dss_user'),
    'password': os.getenv('DB_PASSWORD', '6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G')
}

def create_dwh_dimensions(conn):
    """Create DWH dimension tables"""
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
            
            CREATE TABLE IF NOT EXISTS dwh_dim_date (
                date_sk INT PRIMARY KEY,
                date_value DATE UNIQUE NOT NULL,
                day INT,
                month INT,
                quarter INT,
                year INT,
                week_of_year INT,
                is_weekend BOOLEAN,
                day_name VARCHAR(10),
                month_name VARCHAR(12)
            );
            
            CREATE TABLE IF NOT EXISTS dwh_dim_platform (
                platform_sk SERIAL PRIMARY KEY,
                platform_code VARCHAR(20) UNIQUE NOT NULL,
                platform_name TEXT NOT NULL,
                website_url TEXT,
                country_code VARCHAR(2) DEFAULT 'VN',
                is_active BOOLEAN DEFAULT TRUE
            );
            
            CREATE TABLE IF NOT EXISTS dwh_dim_brand (
                brand_sk SERIAL PRIMARY KEY,
                brand_code VARCHAR(100) UNIQUE,
                brand_name TEXT NOT NULL
            );
            
            CREATE TABLE IF NOT EXISTS dwh_dim_category (
                category_sk SERIAL PRIMARY KEY,
                category_code VARCHAR(100) UNIQUE NOT NULL,
                category_name TEXT NOT NULL,
                parent_category_sk INT,
                category_level INT DEFAULT 1,
                category_path TEXT
            );
            
            CREATE TABLE IF NOT EXISTS dwh_dim_product (
                product_sk BIGSERIAL PRIMARY KEY,
                global_product_id VARCHAR(36) NOT NULL,
                product_name TEXT NOT NULL,
                brand_sk INT,
                category_sk INT,
                seller_name TEXT,
                seller_type VARCHAR(50),
                effective_from DATE NOT NULL,
                effective_to DATE NOT NULL,
                is_current BOOLEAN NOT NULL,
                UNIQUE (global_product_id, effective_from)
            );
        """)
        conn.commit()
        print(" DWH dimension tables created")

def create_dwh_facts(conn):
    """Create DWH fact tables"""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dwh_fact_product_daily (
                date_sk INT NOT NULL,
                product_sk BIGINT NOT NULL,
                platform_sk INT NOT NULL,
                price_current DECIMAL(15,2),
                price_original DECIMAL(15,2),
                discount_pct DECIMAL(5,2),
                rating_avg DECIMAL(3,2),
                rating_count INT,
                review_count INT,
                sold_count INT,
                is_available BOOLEAN DEFAULT TRUE,
                captured_at TIMESTAMP,
                PRIMARY KEY (date_sk, product_sk, platform_sk)
            );
            
            CREATE TABLE IF NOT EXISTS dwh_fact_review_summary (
                date_sk INT NOT NULL,
                product_sk BIGINT NOT NULL,
                platform_sk INT NOT NULL,
                total_reviews INT DEFAULT 0,
                avg_rating DECIMAL(3,2),
                positive_reviews INT DEFAULT 0,
                negative_reviews INT DEFAULT 0,
                neutral_reviews INT DEFAULT 0,
                sentiment_score DECIMAL(5,2),
                PRIMARY KEY (date_sk, product_sk, platform_sk)
            );
        """)
        conn.commit()
        print(" DWH fact tables created")

def populate_dim_date(conn):
    """Populate date dimension"""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO dwh_dim_date (date_sk, date_value, day, month, quarter, year, week_of_year, is_weekend, day_name, month_name)
            SELECT 
                TO_CHAR(d, 'YYYYMMDD')::INT,
                d::DATE,
                EXTRACT(DAY FROM d)::INT,
                EXTRACT(MONTH FROM d)::INT,
                EXTRACT(QUARTER FROM d)::INT,
                EXTRACT(YEAR FROM d)::INT,
                EXTRACT(WEEK FROM d)::INT,
                EXTRACT(DOW FROM d) IN (0, 6),
                TO_CHAR(d, 'Day'),
                TO_CHAR(d, 'Month')
            FROM generate_series('2024-01-01'::DATE, '2026-12-31'::DATE, '1 day'::INTERVAL) d
            ON CONFLICT (date_value) DO NOTHING;
        """)
        count = cur.rowcount
        conn.commit()
        print(f" Populated {count} dates")
        return count

def load_dim_platform(conn):
    """Load platform dimension from ODS"""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO dwh_dim_platform (platform_sk, platform_code, platform_name, website_url, country_code, is_active)
            SELECT platform_sk, platform_code, platform_name, website_url, country_code, is_active
            FROM ods_platform_ref
            ON CONFLICT (platform_code) DO NOTHING;
        """)
        count = cur.rowcount
        conn.commit()
        print(f" Loaded {count} platforms")
        return count

def load_dim_brand(conn):
    """Load brand dimension from ODS"""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO dwh_dim_brand (brand_name)
            SELECT DISTINCT brand_name
            FROM ods_product_clean
            WHERE brand_name IS NOT NULL
            AND brand_name NOT IN (SELECT brand_name FROM dwh_dim_brand);
        """)
        count = cur.rowcount
        conn.commit()
        print(f" Loaded {count} brands")
        return count

def load_dim_product(conn):
    """Load product dimension from ODS (SCD Type 2)"""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO dwh_dim_product 
            (global_product_id, product_name, brand_sk, seller_name, effective_from, effective_to, is_current)
            SELECT 
                p.global_product_id,
                p.product_name,
                b.brand_sk,
                p.seller_name,
                CURRENT_DATE,
                '9999-12-31'::DATE,
                TRUE
            FROM ods_product_clean p
            LEFT JOIN dwh_dim_brand b ON p.brand_name = b.brand_name
            WHERE NOT EXISTS (
                SELECT 1 FROM dwh_dim_product dp 
                WHERE dp.global_product_id = p.global_product_id 
                AND dp.is_current = TRUE
            );
        """)
        count = cur.rowcount
        conn.commit()
        print(f" Loaded {count} products")
        return count

def load_fact_product_daily(conn):
    """Load daily product facts from ODS"""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO dwh_fact_product_daily
            (date_sk, product_sk, platform_sk, price_current, price_original, discount_pct, is_available, captured_at)
            SELECT DISTINCT ON (date_sk, product_sk, platform_sk)
                TO_CHAR(pp.captured_at, 'YYYYMMDD')::INT as date_sk,
                dp.product_sk,
                pp.platform_sk,
                pp.price_current,
                pp.price_original,
                pp.discount_percent,
                pp.is_available,
                pp.captured_at
            FROM ods_price_point pp
            JOIN dwh_dim_product dp ON pp.global_product_id = dp.global_product_id AND dp.is_current = TRUE
            ORDER BY date_sk, product_sk, platform_sk, pp.captured_at DESC;
        """)
        count = cur.rowcount
        conn.commit()
        print(f" Loaded {count} daily product facts")
        return count

def load_fact_review_summary(conn):
    """Load review summary facts from ODS"""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO dwh_fact_review_summary
            (date_sk, product_sk, platform_sk, total_reviews, avg_rating, positive_reviews, negative_reviews, neutral_reviews)
            SELECT 
                TO_CHAR(rc.review_time, 'YYYYMMDD')::INT,
                dp.product_sk,
                rc.platform_sk,
                COUNT(*),
                AVG(rc.rating),
                SUM(CASE WHEN rc.rating >= 4 THEN 1 ELSE 0 END),
                SUM(CASE WHEN rc.rating <= 2 THEN 1 ELSE 0 END),
                SUM(CASE WHEN rc.rating = 3 THEN 1 ELSE 0 END)
            FROM ods_review_clean rc
            JOIN dwh_dim_product dp ON rc.global_product_id = dp.global_product_id AND dp.is_current = TRUE
            GROUP BY TO_CHAR(rc.review_time, 'YYYYMMDD')::INT, dp.product_sk, rc.platform_sk;
        """)
        count = cur.rowcount
        conn.commit()
        print(f" Loaded {count} review summaries")
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
    
    print("STAGE 4: Build Data Warehouse (DWH)")
    print("=" * 60)
    
    conn = psycopg2.connect(**DB_CONFIG)
    print(" Connected to database")
    
    try:
        create_dwh_dimensions(conn)
        create_dwh_facts(conn)
        
        print("\n Populating Date Dimension...")
        date_count = populate_dim_date(conn)
        
        print("\n Loading Dimensions...")
        plat_count = load_dim_platform(conn)
        brand_count = load_dim_brand(conn)
        prod_count = load_dim_product(conn)
        
        print("\n Loading Facts...")
        daily_count = load_fact_product_daily(conn)
        review_count = load_fact_review_summary(conn)
        
        total = date_count + plat_count + brand_count + prod_count + daily_count + review_count
        log_etl(conn, 'warehouse_build', 'DWH', 'SUCCESS', start_time, total)
        
        print(f"\n COMPLETE! Total records: {total}")
        print(f" Metadata logged to meta_etl_log")
        
    except Exception as e:
        log_etl(conn, 'warehouse_build', 'DWH', 'FAILED', start_time, 0, str(e))
        print(f"\n FAILED: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
