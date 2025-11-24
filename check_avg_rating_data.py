#!/usr/bin/env python3
"""
Check avg_rating data in fact_product_daily table
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

async def check_avg_rating():
    """Check avg_rating column in fact_product_daily"""
    print("="*60)
    print("CHECKING AVG_RATING DATA")
    print("="*60)
    
    conn = await asyncpg.connect(**DB_CONFIG)
    
    try:
        # 1. Check if avg_rating column exists
        print("\n1. Checking table structure...")
        columns = await conn.fetch("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'dwh' 
            AND table_name = 'fact_product_daily'
            AND column_name LIKE '%rating%'
        """)
        
        print(f"   Found {len(columns)} rating-related columns:")
        for col in columns:
            print(f"   - {col['column_name']} ({col['data_type']}, nullable: {col['is_nullable']})")
        
        # 2. Check avg_rating values
        print("\n2. Checking avg_rating values...")
        stats = await conn.fetchrow("""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(avg_rating) as non_null_count,
                COUNT(*) - COUNT(avg_rating) as null_count,
                MIN(avg_rating) as min_rating,
                MAX(avg_rating) as max_rating,
                AVG(avg_rating) as avg_rating,
                STDDEV(avg_rating) as stddev_rating
            FROM dwh.fact_product_daily
        """)
        
        print(f"\n   Statistics:")
        print(f"   - Total rows: {stats['total_rows']:,}")
        print(f"   - Non-null avg_rating: {stats['non_null_count']:,} ({stats['non_null_count']/stats['total_rows']*100:.1f}%)")
        print(f"   - Null avg_rating: {stats['null_count']:,} ({stats['null_count']/stats['total_rows']*100:.1f}%)")
        print(f"   - Min rating: {stats['min_rating']}")
        print(f"   - Max rating: {stats['max_rating']}")
        print(f"   - Avg rating: {stats['avg_rating']}")
        print(f"   - Stddev: {stats['stddev_rating']}")
        
        # 3. Sample data
        print("\n3. Sample data (first 10 rows)...")
        samples = await conn.fetch("""
            SELECT 
                f.date_sk,
                d.date_value,
                p.product_name,
                f.avg_rating,
                f.total_review_count,
                f.avg_price
            FROM dwh.fact_product_daily f
            JOIN dwh.dim_date d ON d.date_sk = f.date_sk
            JOIN dwh.dim_product p ON p.product_sk = f.product_sk
            ORDER BY f.date_sk DESC, f.product_sk
            LIMIT 10
        """)
        
        for i, row in enumerate(samples, 1):
            print(f"\n   {i}. {row['product_name'][:50]}")
            print(f"      Date: {row['date_value']}")
            print(f"      Avg Rating: {row['avg_rating']}")
            print(f"      Reviews: {row['total_review_count']}")
            print(f"      Avg Price: {row['avg_price']}")
        
        # 4. Check top products query
        print("\n4. Testing TOP PRODUCTS query...")
        top_products = await conn.fetch("""
            SELECT
                p.product_key,
                p.product_name,
                pl.platform_code,
                COALESCE(SUM(f.avg_price * f.total_review_count), 0) AS total_revenue,
                COALESCE(SUM(f.total_review_count), 0) AS total_reviews,
                AVG(f.avg_rating) AS avg_rating,
                AVG(f.avg_price) AS avg_price
            FROM dwh.fact_product_daily f
            JOIN dwh.dim_date d      ON d.date_sk = f.date_sk
            JOIN dwh.dim_product p   ON p.product_sk = f.product_sk
            JOIN dwh.dim_platform pl ON pl.platform_sk = f.platform_sk
            WHERE d.date_value BETWEEN '2025-11-16' AND '2025-11-23'
            GROUP BY
                p.product_key,
                p.product_name,
                pl.platform_code,
                p.category_sk
            ORDER BY total_revenue DESC
            LIMIT 5
        """)
        
        print(f"\n   Top 5 products by revenue:")
        for i, row in enumerate(top_products, 1):
            print(f"\n   {i}. {row['product_name'][:60]}")
            print(f"      Platform: {row['platform_code']}")
            print(f"      Revenue: {row['total_revenue']:,.0f}")
            print(f"      Reviews: {row['total_reviews']:,}")
            print(f"      ⭐ Avg Rating: {row['avg_rating']}")  # This is the issue!
            print(f"      Avg Price: {row['avg_price']}")
        
        # 5. Check if there's rating data in dim_product or fact_review
        print("\n5. Checking other tables for rating data...")
        
        # Check dim_product
        dim_product_rating = await conn.fetchrow("""
            SELECT COUNT(*) as total, COUNT(avg_rating) as has_rating
            FROM dwh.dim_product
            WHERE avg_rating IS NOT NULL
        """)
        print(f"   dim_product: {dim_product_rating['has_rating']} products have avg_rating")
        
        # Check fact_review if exists
        try:
            fact_review_count = await conn.fetchval("""
                SELECT COUNT(*) FROM dwh.fact_review
            """)
            print(f"   fact_review: {fact_review_count:,} rows")
            
            fact_review_rating = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(rating) as has_rating,
                    MIN(rating) as min_rating,
                    MAX(rating) as max_rating,
                    AVG(rating) as avg_rating
                FROM dwh.fact_review
            """)
            print(f"      - Has rating: {fact_review_rating['has_rating']:,}")
            print(f"      - Min: {fact_review_rating['min_rating']}")
            print(f"      - Max: {fact_review_rating['max_rating']}")
            print(f"      - Avg: {fact_review_rating['avg_rating']:.2f}")
        except:
            print("   fact_review: table not found or no data")
        
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(check_avg_rating())

