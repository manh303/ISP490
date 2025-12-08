#!/usr/bin/env python3
"""
Load Tiki Reviews from JSONL to Database
"""
import json
import psycopg2
import os
from pathlib import Path
from datetime import datetime

OUTPUT_DIR = os.environ.get("CRAWLER_OUTPUT_DIR", "/app/data/outputs")
LOG_PREFIX = "[Tiki-Loader]"

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'ecommerce_dss'),
    'user': os.getenv('DB_USER', 'dss_user'),
    'password': os.getenv('DB_PASSWORD', 'dss_password_123')
}

def load_reviews_to_staging(conn):
    """Load reviews from JSONL to staging table"""
    reviews_dir = Path(OUTPUT_DIR) / "tiki_reviews"
    
    if not reviews_dir.exists():
        print(f"{LOG_PREFIX} No reviews directory found!")
        return 0
    
    total_loaded = 0
    
    with conn.cursor() as cur:
        for jsonl_file in reviews_dir.rglob("*.jsonl"):
            print(f"{LOG_PREFIX} Loading {jsonl_file.name}...")
            
            with open(jsonl_file, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        review = json.loads(line)
                        
                        raw_data = json.dumps({
                            'reviewer_name': review['reviewer_name'],
                            'rating': review['rating'],
                            'review_text': f"{review['title']} {review['content']}".strip(),
                            'review_time': review['review_time'],
                            'helpful_count': review['helpful_count']
                        })
                        
                        cur.execute("""
                            INSERT INTO stg_raw_reviews 
                            (source_platform, platform_product_id, raw_data, crawled_at)
                            VALUES (%s, %s, %s::jsonb, %s)
                            ON CONFLICT DO NOTHING
                        """, (
                            'tiki',
                            review['product_id'],
                            raw_data,
                            datetime.now()
                        ))
                        total_loaded += 1
                        
                    except Exception as e:
                        print(f"{LOG_PREFIX} Error loading review: {e}")
            
            conn.commit()
            print(f"{LOG_PREFIX} Loaded {jsonl_file.name}")
    
    return total_loaded

def main():
    print(f"{LOG_PREFIX} Loading Tiki reviews to database...")
    
    conn = psycopg2.connect(**DB_CONFIG)
    
    try:
        count = load_reviews_to_staging(conn)
        print(f"{LOG_PREFIX} SUCCESS: Loaded {count} reviews")
    except Exception as e:
        print(f"{LOG_PREFIX} FAILED: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
