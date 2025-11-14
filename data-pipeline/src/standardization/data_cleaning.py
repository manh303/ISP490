#!/usr/bin/env python3
"""
Stage 2: Data Cleaning & Standardization
"""
import psycopg2
import os

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'ecommerce_dss'),
    'user': os.getenv('DB_USER', 'dss_user'),
    'password': os.getenv('DB_PASSWORD', 'IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4')
}

def create_ods_tables(conn):
    """Create ODS tables per datawarehouse.sql"""
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
            
            CREATE TABLE IF NOT EXISTS ods_platform_ref (
                platform_sk SERIAL PRIMARY KEY,
                platform_code VARCHAR(20) UNIQUE NOT NULL,
                platform_name TEXT NOT NULL,
                website_url TEXT,
                country_code VARCHAR(2) DEFAULT 'VN',
                is_active BOOLEAN DEFAULT TRUE
            );
            
            CREATE TABLE IF NOT EXISTS ods_product_clean (
                global_product_id VARCHAR(36) PRIMARY KEY,
                product_name TEXT NOT NULL,
                brand_name TEXT,
                category_sk INT,
                seller_name TEXT,
                image_urls TEXT[],
                first_seen TIMESTAMP DEFAULT NOW(),
                last_seen TIMESTAMP DEFAULT NOW(),
                is_active BOOLEAN DEFAULT TRUE
            );
            
            CREATE TABLE IF NOT EXISTS ods_price_point (
                id BIGSERIAL PRIMARY KEY,
                global_product_id VARCHAR(36) NOT NULL,
                platform_sk INT NOT NULL,
                captured_at TIMESTAMP NOT NULL,
                price_current DECIMAL(15,2),
                price_original DECIMAL(15,2),
                discount_percent DECIMAL(5,2),
                is_available BOOLEAN DEFAULT TRUE
            );
            
            CREATE TABLE IF NOT EXISTS ods_review_clean (
                id BIGSERIAL PRIMARY KEY,
                global_product_id VARCHAR(36) NOT NULL,
                platform_sk INT NOT NULL,
                reviewer_name TEXT,
                rating INT CHECK (rating >= 1 AND rating <= 5),
                review_content TEXT,
                review_time TIMESTAMP,
                helpful_count INT DEFAULT 0,
                created_at TIMESTAMP DEFAULT NOW()
            );
            
            INSERT INTO ods_platform_ref (platform_code, platform_name, website_url)
            VALUES 
                ('lazada', 'Lazada Vietnam', 'https://www.lazada.vn'),
                ('tiki', 'Tiki Vietnam', 'https://tiki.vn')
            ON CONFLICT (platform_code) DO NOTHING;
        """)
        conn.commit()
        print("✅ ODS tables created")

def clean_products(conn):
    """Transform STG to ODS products"""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO ods_product_clean 
            (global_product_id, source_platform, platform_product_id, product_name, brand_name, seller_name)
            SELECT DISTINCT ON (source_platform || '_' || platform_product_id)
                source_platform || '_' || platform_product_id as global_product_id,
                source_platform,
                platform_product_id,
                TRIM(raw_data->>'product_name') as product_name,
                COALESCE(TRIM(raw_data->>'brand'), 'Unknown') as brand_name,
                raw_data->>'seller_name' as seller_name
            FROM stg_raw_products
            WHERE platform_product_id IS NOT NULL
            AND source_platform IS NOT NULL
            ORDER BY source_platform || '_' || platform_product_id, crawled_at DESC
            ON CONFLICT (global_product_id) DO NOTHING;
                
            INSERT INTO ods_price_point
            (global_product_id, platform_sk, captured_at, price_current, price_original, discount_percent)
            SELECT 
                source_platform || '_' || platform_product_id as global_product_id,
                (SELECT platform_sk FROM ods_platform_ref WHERE platform_code = source_platform),
                crawled_at,
                (raw_data->>'price_current')::DECIMAL,
                (raw_data->>'price_original')::DECIMAL,
                (raw_data->>'discount_percent')::DECIMAL
            FROM stg_raw_products
            WHERE platform_product_id IS NOT NULL
            AND source_platform IS NOT NULL;
        """)
        count = cur.rowcount
        conn.commit()
        print(f"✅ Cleaned {count} products")
        return count

def clean_reviews(conn):
    """Transform STG to ODS reviews"""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO ods_review_clean
            (global_product_id, platform_sk, reviewer_name, rating, review_content, review_time, helpful_count)
            SELECT 
                source_platform || '_' || platform_product_id as global_product_id,
                (SELECT platform_sk FROM ods_platform_ref WHERE platform_code = source_platform),
                COALESCE(TRIM(raw_data->>'reviewer_name'), 'Anonymous'),
                (raw_data->>'rating')::INT,
                TRIM(raw_data->>'review_text'),
                crawled_at,
                COALESCE((raw_data->>'helpful_count')::INT, 0)
            FROM stg_raw_reviews
            WHERE platform_product_id IS NOT NULL 
            AND raw_data->>'review_text' IS NOT NULL;
        """)
        count = cur.rowcount
        conn.commit()
        print(f"✅ Cleaned {count} reviews")
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
    from datetime import datetime
    start_time = datetime.now()
    
    print("STAGE 2: Data Cleaning & Standardization")
    print("=" * 60)
    
    conn = psycopg2.connect(**DB_CONFIG)
    print("✅ Connected to database")
    
    create_ods_tables(conn)
    
    try:
        print("\n🧹 Cleaning Products...")
        products_count = clean_products(conn)
        
        print("\n🧹 Cleaning Reviews...")
        reviews_count = clean_reviews(conn)
        
        total_records = products_count + reviews_count
        log_etl(conn, 'data_cleaning', 'ODS', 'SUCCESS', start_time, total_records)
        
        print(f"\n✅ COMPLETE! Products: {products_count}, Reviews: {reviews_count}")
        print(f"📊 Metadata logged to meta_etl_log")
        
    except Exception as e:
        log_etl(conn, 'data_cleaning', 'ODS', 'FAILED', start_time, 0, str(e))
        print(f"\n❌ FAILED: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
