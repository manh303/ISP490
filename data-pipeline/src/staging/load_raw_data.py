#!/usr/bin/env python3
"""
Stage 1: Load Raw Data from JSONL files to Staging Database
"""
import json
import psycopg2
from pathlib import Path
from datetime import datetime
import os

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'ecommerce_dss'),
    'user': os.getenv('DB_USER', 'dss_user'),
    'password': os.getenv('DB_PASSWORD', 'dss_password_123')
}

OUTPUT_DIR = os.getenv('CRAWLER_OUTPUT_DIR', '/tmp/data/outputs')

def create_staging_tables(conn):
    """Create staging tables per datawarehouse.sql"""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stg_raw_products (
                id BIGSERIAL PRIMARY KEY,
                source_platform VARCHAR(20) NOT NULL,
                url TEXT,
                platform_product_id TEXT,
                crawled_at TIMESTAMP NOT NULL,
                raw_data JSONB NOT NULL,
                checksum TEXT,
                load_id VARCHAR(36),
                created_at TIMESTAMP DEFAULT NOW()
            );
            
            CREATE TABLE IF NOT EXISTS stg_raw_reviews (
                id BIGSERIAL PRIMARY KEY,
                source_platform VARCHAR(20) NOT NULL,
                platform_product_id TEXT,
                crawled_at TIMESTAMP NOT NULL,
                raw_data JSONB NOT NULL,
                load_id VARCHAR(36),
                created_at TIMESTAMP DEFAULT NOW()
            );
            
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
        print("✅ Staging tables created")

def load_products(conn, date_str: str, load_id: str):
    """Load products to stg_raw_products with JSONB"""
    total_count = 0
    
    # Load Lazada products
    products_dir = Path(OUTPUT_DIR) / "lazada" / f"date={date_str}"
    if not products_dir.exists():
        print(f"⚠️  No Lazada products for {date_str}")
    else:
        total_count += _load_products_from_dir(conn, products_dir, 'lazada', load_id)
    
    # Load Tiki products
    tiki_dir = Path(OUTPUT_DIR) / "tiki" / f"date={date_str}"
    if not tiki_dir.exists():
        print(f"⚠️  No Tiki products for {date_str}")
    else:
        total_count += _load_products_from_dir(conn, tiki_dir, 'tiki', load_id)
    
    return total_count

def _load_products_from_dir(conn, products_dir: Path, source: str, load_id: str):
    """Helper to load products from a directory"""
    if not products_dir.exists():
        return 0
    
    count = 0
    with conn.cursor() as cur:
        for jsonl_file in products_dir.glob("*.jsonl"):
            print(f"   📄 {jsonl_file.name}")
            batch = []
            with open(jsonl_file, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        p = json.loads(line)
                        batch.append((
                            source,
                            p.get('url'),
                            p.get('product_id'),
                            p.get('crawl_date', datetime.now()),
                            json.dumps(p),
                            load_id
                        ))
                        if len(batch) >= 100:
                            cur.executemany("""
                                INSERT INTO stg_raw_products 
                                (source_platform, url, platform_product_id, crawled_at, raw_data, load_id)
                                VALUES (%s, %s, %s, %s, %s, %s)
                            """, batch)
                            count += len(batch)
                            batch = []
                    except:
                        continue
            if batch:
                cur.executemany("""
                    INSERT INTO stg_raw_products 
                    (source_platform, url, platform_product_id, crawled_at, raw_data, load_id)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, batch)
                count += len(batch)
        conn.commit()
    print(f"✅ Loaded {count} {source} products")
    return count

def load_reviews(conn, date_str: str, load_id: str):
    """Load reviews to stg_raw_reviews with JSONB"""
    total_count = 0
    
    # Load Lazada reviews
    lazada_reviews_dir = Path(OUTPUT_DIR) / "lazada_reviews" / f"date={date_str}"
    if not lazada_reviews_dir.exists():
        print(f"⚠️  No Lazada reviews for {date_str}")
    else:
        total_count += _load_reviews_from_dir(conn, lazada_reviews_dir, 'lazada', load_id)
    
    # Load Tiki reviews
    tiki_reviews_dir = Path(OUTPUT_DIR) / "tiki_reviews" / f"date={date_str}"
    if not tiki_reviews_dir.exists():
        print(f"⚠️  No Tiki reviews for {date_str}")
    else:
        total_count += _load_reviews_from_dir(conn, tiki_reviews_dir, 'tiki', load_id)
    
    return total_count

def _load_reviews_from_dir(conn, reviews_dir: Path, source: str, load_id: str):
    """Helper to load reviews from a directory"""
    if not reviews_dir.exists():
        return 0
    
    count = 0
    with conn.cursor() as cur:
        for jsonl_file in reviews_dir.glob("*.jsonl"):
            print(f"   📄 {jsonl_file.name}")
            batch = []
            with open(jsonl_file, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        r = json.loads(line)
                        batch.append((
                            source,
                            r.get('product_id'),
                            r.get('crawl_timestamp') or r.get('crawl_date') or datetime.now(),
                            json.dumps(r),
                            load_id
                        ))
                        if len(batch) >= 100:
                            cur.executemany("""
                                INSERT INTO stg_raw_reviews
                                (source_platform, platform_product_id, crawled_at, raw_data, load_id)
                                VALUES (%s, %s, %s, %s, %s)
                            """, batch)
                            count += len(batch)
                            batch = []
                    except:
                        continue
            if batch:
                cur.executemany("""
                    INSERT INTO stg_raw_reviews
                    (source_platform, platform_product_id, crawled_at, raw_data, load_id)
                    VALUES (%s, %s, %s, %s, %s)
                """, batch)
                count += len(batch)
        conn.commit()
    print(f"✅ Loaded {count} {source} reviews")
    return count

def log_etl(conn, job_name, stage, status, start_time, records_processed=0, error_msg=None, load_id=None):
    """Log ETL metadata"""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO meta_etl_log 
            (job_name, stage, status, start_time, end_time, records_processed, error_message, load_id)
            VALUES (%s, %s, %s, %s, NOW(), %s, %s, %s)
        """, (job_name, stage, status, start_time, records_processed, error_msg, load_id))
        conn.commit()

def main():
    import uuid
    import sys
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    
    load_id = str(uuid.uuid4())
    start_time = datetime.now()
    
    print("STAGE 1: Load Raw Data to Staging")
    print("=" * 60)
    print(f"Load ID: {load_id}")
    
    today = datetime.now().strftime("%Y-%m-%d")
    print(f"📅 Processing date: {today}")
    
    print("🔌 Connecting to database...")
    conn = psycopg2.connect(**DB_CONFIG)
    print("✅ Connected to database")
    
    create_staging_tables(conn)
    
    try:
        print("\n📦 Loading Products...")
        products_count = load_products(conn, today, load_id)
        
        print("\n💬 Loading Reviews...")
        reviews_count = load_reviews(conn, today, load_id)
        
        total_records = products_count + reviews_count
        log_etl(conn, 'load_raw_data', 'STG', 'SUCCESS', start_time, total_records, None, load_id)
        
        print(f"\n✅ COMPLETE! Products: {products_count}, Reviews: {reviews_count}")
        print(f"📊 Metadata logged to meta_etl_log (load_id: {load_id})")
        
    except Exception as e:
        log_etl(conn, 'load_raw_data', 'STG', 'FAILED', start_time, 0, str(e), load_id)
        print(f"\n❌ FAILED: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
