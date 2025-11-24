#!/usr/bin/env python3
"""
Check fact_review table data
"""
import asyncio
import asyncpg
import os

# Database config
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "ecommerce_dss"),
    "user": os.getenv("DB_USER", "dss_user"),
    "password": os.getenv("DB_PASSWORD", "IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4"),
}

async def check_fact_review():
    """Check fact_review table"""
    print("="*60)
    print("CHECKING FACT_REVIEW TABLE")
    print("="*60)
    
    conn = await asyncpg.connect(**DB_CONFIG)
    
    try:
        # 1. Check table structure
        print("\n1. Table structure:")
        columns = await conn.fetch("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'dwh' 
            AND table_name = 'fact_review'
            ORDER BY ordinal_position
        """)
        
        for col in columns:
            print(f"   - {col['column_name']:20s} {col['data_type']:15s} (null: {col['is_nullable']})")
        
        # 2. Check row count
        print("\n2. Row count:")
        count = await conn.fetchval("SELECT COUNT(*) FROM dwh.fact_review")
        print(f"   Total rows: {count:,}")
        
        if count == 0:
            print("\n   ❌ Table is EMPTY! No review data available.")
            return
        
        # 3. Check data distribution
        print("\n3. Data distribution:")
        
        # By rating
        rating_dist = await conn.fetch("""
            SELECT rating, COUNT(*) as count
            FROM dwh.fact_review
            GROUP BY rating
            ORDER BY rating DESC
        """)
        print("\n   By Rating:")
        for row in rating_dist:
            print(f"   ⭐ {row['rating']}: {row['count']:,} reviews")
        
        # By date
        date_range = await conn.fetchrow("""
            SELECT 
                MIN(d.date_value) as min_date,
                MAX(d.date_value) as max_date,
                COUNT(*) as total
            FROM dwh.fact_review r
            JOIN dwh.dim_date d ON d.date_sk = r.date_sk
        """)
        print(f"\n   Date range: {date_range['min_date']} to {date_range['max_date']}")
        print(f"   Total: {date_range['total']:,} reviews")
        
        # 4. Sample data
        print("\n4. Sample reviews:")
        samples = await conn.fetch("""
            SELECT 
                r.review_sk,
                p.product_name,
                r.rating,
                r.helpful_votes,
                LEFT(r.review_body, 80) as review_snippet,
                d.date_value
            FROM dwh.fact_review r
            JOIN dwh.dim_product p ON p.product_sk = r.product_sk
            JOIN dwh.dim_date d ON d.date_sk = r.date_sk
            ORDER BY r.review_sk
            LIMIT 5
        """)
        
        for i, row in enumerate(samples, 1):
            print(f"\n   {i}. {row['product_name'][:50]}")
            print(f"      Rating: {row['rating']} ⭐ | Helpful: {row['helpful_votes']}")
            print(f"      Date: {row['date_value']}")
            print(f"      Review: {row['review_snippet']}...")
        
        # 5. Test review summary query for a product
        print("\n5. Testing review summary for top product:")
        
        # Get a product with reviews
        test_product = await conn.fetchrow("""
            SELECT 
                p.product_key,
                p.product_name,
                p.product_sk,
                COUNT(r.review_sk) as review_count
            FROM dwh.dim_product p
            JOIN dwh.fact_review r ON r.product_sk = p.product_sk
            GROUP BY p.product_key, p.product_name, p.product_sk
            ORDER BY review_count DESC
            LIMIT 1
        """)
        
        if test_product:
            print(f"\n   Product: {test_product['product_name']}")
            print(f"   Product Key: {test_product['product_key']}")
            print(f"   Total Reviews: {test_product['review_count']:,}")
            
            # Test the actual query from get_review_summary
            summary = await conn.fetchrow("""
                SELECT
                    COUNT(*) AS total_reviews,
                    AVG(r.rating) AS avg_rating,
                    COUNT(*) FILTER (WHERE r.rating = 5) AS rating_5,
                    COUNT(*) FILTER (WHERE r.rating = 4) AS rating_4,
                    COUNT(*) FILTER (WHERE r.rating = 3) AS rating_3,
                    COUNT(*) FILTER (WHERE r.rating = 2) AS rating_2,
                    COUNT(*) FILTER (WHERE r.rating = 1) AS rating_1
                FROM dwh.fact_review r
                JOIN dwh.dim_date d ON d.date_sk = r.date_sk
                WHERE r.product_sk = $1
                  AND d.date_value BETWEEN '2025-11-01' AND '2025-11-23'
            """, test_product['product_sk'])
            
            print(f"\n   Summary (Nov 1-23):")
            print(f"   - Total reviews: {summary['total_reviews']}")
            print(f"   - Avg rating: {summary['avg_rating']:.2f if summary['avg_rating'] else 'N/A'}")
            print(f"   - Breakdown:")
            print(f"     5⭐: {summary['rating_5']}")
            print(f"     4⭐: {summary['rating_4']}")
            print(f"     3⭐: {summary['rating_3']}")
            print(f"     2⭐: {summary['rating_2']}")
            print(f"     1⭐: {summary['rating_1']}")
        
        # 6. Check products WITHOUT reviews
        print("\n6. Products without reviews:")
        no_reviews = await conn.fetchval("""
            SELECT COUNT(*) 
            FROM dwh.dim_product p
            WHERE NOT EXISTS (
                SELECT 1 FROM dwh.fact_review r 
                WHERE r.product_sk = p.product_sk
            )
        """)
        
        total_products = await conn.fetchval("SELECT COUNT(*) FROM dwh.dim_product")
        print(f"   Products without reviews: {no_reviews:,} / {total_products:,} ({no_reviews/total_products*100:.1f}%)")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(check_fact_review())

