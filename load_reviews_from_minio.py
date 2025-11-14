#!/usr/bin/env python3
"""Load reviews from MinIO to staging and ODS"""
import json
import psycopg2
from minio import Minio
from datetime import datetime

# MinIO config
minio_client = Minio(
    'localhost:9000',
    access_key='minioadmin',
    secret_key='minioadmin123',
    secure=False
)

# DB config
DB_CONFIG = {
    'host': 'dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com',
    'port': '5432',
    'database': 'ecommerce_dss',
    'user': 'dss_user',
    'password': 'IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4'
}

def load_to_staging(conn):
    """Load reviews from MinIO to staging"""
    cur = conn.cursor()
    
    # Get all review files
    objects = minio_client.list_objects('crawler-data', recursive=True)
    review_files = [obj for obj in objects if 'review' in obj.object_name]
    
    total = 0
    for obj in review_files:
        platform = 'lazada' if 'lazada' in obj.object_name else 'tiki'
        
        # Download and parse
        response = minio_client.get_object('crawler-data', obj.object_name)
        content = response.read().decode('utf-8')
        
        for line in content.strip().split('\n'):
            if not line:
                continue
            data = json.loads(line)
            
            cur.execute("""
                INSERT INTO stg_raw_reviews (source_platform, platform_product_id, raw_data, crawled_at, created_at)
                VALUES (%s, %s, %s, NOW(), NOW())
                ON CONFLICT DO NOTHING
            """, (platform, data.get('product_id'), json.dumps(data)))
            total += 1
        
        if total % 1000 == 0:
            conn.commit()
            print(f"  Loaded {total} reviews...")
    
    conn.commit()
    print(f"✓ Loaded {total} reviews to staging")
    return total

def load_to_ods(conn):
    """Load reviews from staging to ODS"""
    cur = conn.cursor()
    
    cur.execute("""
        INSERT INTO ods_review_clean (
            global_review_id, source_platform, platform_product_id, review_id,
            reviewer_name, rating, review_text, review_time, helpful_count,
            crawled_at, created_at, last_seen
        )
        SELECT 
            source_platform || '_' || (raw_data->>'review_id') as global_review_id,
            source_platform,
            raw_data->>'product_id' as platform_product_id,
            raw_data->>'review_id' as review_id,
            raw_data->>'reviewer_name' as reviewer_name,
            (raw_data->>'rating')::int as rating,
            raw_data->>'content' as review_text,
            (raw_data->>'review_time')::timestamp as review_time,
            0 as helpful_count,
            (raw_data->>'crawl_date')::timestamp as crawled_at,
            NOW() as created_at,
            NOW() as last_seen
        FROM stg_raw_reviews
        WHERE (raw_data->>'review_id') IS NOT NULL
        ON CONFLICT (global_review_id) DO NOTHING
    """)
    
    count = cur.rowcount
    conn.commit()
    print(f"✓ Loaded {count} reviews to ODS")
    return count

def main():
    print("=" * 60)
    print("LOAD REVIEWS FROM MINIO")
    print("=" * 60)
    
    conn = psycopg2.connect(**DB_CONFIG)
    
    print("\n1. Loading to staging...")
    stg_count = load_to_staging(conn)
    
    print("\n2. Loading to ODS...")
    ods_count = load_to_ods(conn)
    
    conn.close()
    
    print("\n✅ COMPLETE!")
    print(f"   Staging: {stg_count} reviews")
    print(f"   ODS: {ods_count} reviews")

if __name__ == "__main__":
    main()
