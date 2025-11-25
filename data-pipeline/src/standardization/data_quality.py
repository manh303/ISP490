#!/usr/bin/env python3
"""
Stage 3: Data Quality & Deduplication
"""
import psycopg2
import os

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'ecommerce_dss_1'),
    'user': os.getenv('DB_USER', 'dss_user'),
    'password': os.getenv('DB_PASSWORD', '6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G')
}

def create_metadata_table(conn):
    """Ensure metadata table exists"""
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
        """)
        conn.commit()

def check_data_quality(conn):
    """Check ODS data quality"""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM ods_product_clean")
        total_products = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM ods_price_point WHERE price_current > 0")
        valid_price = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM ods_review_clean")
        total_reviews = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM ods_review_clean WHERE rating > 0")
        valid_reviews = cur.fetchone()[0]
        
        print("\n📊 Data Quality Report:")
        print(f"   Products: {total_products}")
        print(f"   Price Points: {valid_price}")
        print(f"   Reviews: {total_reviews}")
        print(f"   - With rating: {valid_reviews} ({valid_reviews/total_reviews*100:.1f}%)" if total_reviews > 0 else "   - No reviews")

def remove_duplicates(conn):
    """Remove duplicate ODS records"""
    with conn.cursor() as cur:
        # Remove exact timestamp duplicates
        cur.execute("""
            DELETE FROM ods_price_point
            WHERE id IN (
                SELECT a.id FROM ods_price_point a
                INNER JOIN ods_price_point b ON 
                    a.global_product_id = b.global_product_id 
                    AND a.platform_sk = b.platform_sk
                    AND a.captured_at = b.captured_at
                    AND a.id < b.id
            )
        """)
        dup_exact = cur.rowcount
        
        # Remove date-level duplicates (keep latest per day)
        cur.execute("""
            DELETE FROM ods_price_point
            WHERE id NOT IN (
                SELECT DISTINCT ON (global_product_id, platform_sk, DATE(captured_at))
                    id
                FROM ods_price_point
                ORDER BY global_product_id, platform_sk, DATE(captured_at), captured_at DESC
            )
        """)
        dup_daily = cur.rowcount
        
        cur.execute("""
            DELETE FROM ods_review_clean
            WHERE id IN (
                SELECT a.id FROM ods_review_clean a
                INNER JOIN ods_review_clean b ON
                    a.global_product_id = b.global_product_id 
                    AND a.review_content = b.review_content
                    AND a.id < b.id
            )
        """)
        dup_reviews = cur.rowcount
        
        conn.commit()
        print(f"\n🗑️  Removed duplicates:")
        print(f"   Price Points (exact): {dup_exact}")
        print(f"   Price Points (daily): {dup_daily}")
        print(f"   Reviews: {dup_reviews}")

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
    
    print("STAGE 3: Data Quality & Deduplication")
    print("=" * 60)
    
    conn = psycopg2.connect(**DB_CONFIG)
    print("✅ Connected to database")
    
    create_metadata_table(conn)
    
    try:
        check_data_quality(conn)
        remove_duplicates(conn)
        
        log_etl(conn, 'data_quality', 'ODS', 'SUCCESS', start_time, 0)
        
        print("\n✅ COMPLETE!")
        print(f"📊 Metadata logged to meta_etl_log")
        
    except Exception as e:
        conn.rollback()
        try:
            log_etl(conn, 'data_quality', 'ODS', 'FAILED', start_time, 0, str(e))
        except:
            pass
        print(f"\n❌ FAILED: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
