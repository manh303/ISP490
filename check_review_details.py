#!/usr/bin/env python3
"""
Check fact_reviews_detail table from DWH
"""

import os
import sys
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

load_dotenv()

# Try multiple DB config sources
DB_HOST = os.getenv("DB_HOST") or os.getenv("PGHOST")
if not DB_HOST or DB_HOST == "postgres":
    DB_HOST = "localhost"
    
DB_PORT = os.getenv("DB_PORT") or os.getenv("PGPORT", "5432")
DB_NAME = os.getenv("DB_NAME") or os.getenv("PGDB", "ecommerce_dss_1")
DB_USER = os.getenv("DB_USER") or os.getenv("PGUSER", "admin")
DB_PASSWORD = os.getenv("DB_PASSWORD") or os.getenv("PGPASSWORD", "admin")
DWH_SCHEMA = os.getenv("DWH_SCHEMA", "dwh")

def check_review_details():
    """Check fact_reviews_detail table"""
    
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        table_name = f"{DWH_SCHEMA}.fact_reviews_detail"
        
        # Check if table exists
        check_sql = f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = '{DWH_SCHEMA}' 
                AND table_name = 'fact_reviews_detail'
            ) as exists;
        """
        cur.execute(check_sql)
        table_exists = cur.fetchone()['exists']
        
        if not table_exists:
            print(f"⚠ Table {table_name} does not exist yet")
            cur.close()
            conn.close()
            return
        
        # Count records
        count_sql = f"SELECT COUNT(*) as total FROM {table_name};"
        cur.execute(count_sql)
        total = cur.fetchone()['total']
        print(f"\n✓ Table {table_name} exists")
        print(f"  Total records: {total:,}")
        
        if total == 0:
            print("  No data in table yet")
            cur.close()
            conn.close()
            return
        
        # Get sample data
        sample_sql = f"""
            SELECT 
                review_id,
                global_product_id,
                source_platform_std,
                reviewer_name,
                rating,
                review_text,
                review_date,
                helpful_count,
                sentiment_score,
                sentiment_label,
                review_quality_score
            FROM {table_name}
            ORDER BY created_at DESC
            LIMIT 3;
        """
        cur.execute(sample_sql)
        samples = cur.fetchall()
        
        print(f"\n Sample Data ({len(samples)} rows):")
        for i, row in enumerate(samples, 1):
            print(f"\n  Row {i}:")
            print(f"    Review ID: {row['review_id']}")
            print(f"    Product ID: {row['global_product_id']}")
            print(f"    Platform: {row['source_platform_std']}")
            print(f"    Reviewer: {row['reviewer_name']}")
            print(f"    Rating: {row['rating']}/5.0")
            print(f"    Review Text: {row['review_text'][:100] if row['review_text'] else '(empty)'}...")
            print(f"    Review Date: {row['review_date']}")
            print(f"    Helpful Count: {row['helpful_count']}")
            print(f"    Sentiment Score: {row['sentiment_score']}")
            print(f"    Sentiment Label: {row['sentiment_label']}")
            print(f"    Quality Score: {row['review_quality_score']}")
        
        # Statistics
        stats_sql = f"""
            SELECT 
                COUNT(*) as total_reviews,
                COUNT(DISTINCT global_product_id) as unique_products,
                COUNT(DISTINCT reviewer_name) as unique_reviewers,
                COUNT(DISTINCT source_platform_std) as platforms,
                AVG(rating) as avg_rating,
                MIN(review_date) as earliest_review,
                MAX(review_date) as latest_review,
                AVG(sentiment_score) as avg_sentiment,
                COUNT(CASE WHEN sentiment_label = 'positive' THEN 1 END) as positive_count,
                COUNT(CASE WHEN sentiment_label = 'neutral' THEN 1 END) as neutral_count,
                COUNT(CASE WHEN sentiment_label = 'negative' THEN 1 END) as negative_count
            FROM {table_name};
        """
        cur.execute(stats_sql)
        stats = cur.fetchone()
        
        print(f"\n Statistics:")
        print(f"  Total Reviews: {stats['total_reviews']:,}")
        print(f"  Unique Products: {stats['unique_products']:,}")
        print(f"  Unique Reviewers: {stats['unique_reviewers']:,}")
        print(f"  Platforms: {stats['platforms']}")
        print(f"  Avg Rating: {stats['avg_rating']:.2f}")
        print(f"  Date Range: {stats['earliest_review']} to {stats['latest_review']}")
        print(f"  Avg Sentiment Score: {stats['avg_sentiment']:.3f}")
        print(f"  Sentiment Distribution:")
        print(f"    Positive: {stats['positive_count']:,}")
        print(f"    Neutral: {stats['neutral_count']:,}")
        print(f"    Negative: {stats['negative_count']:,}")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = check_review_details()
    sys.exit(exit_code)
