import asyncio
import asyncpg
import json
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

async def load_tiki_to_ods():
    conn = await asyncpg.connect(
        host="localhost",
        port=5433,
        database="ecommerce_dss",
        user="dss_user",
        password="dss_password_123"
    )
    
    print("Loading Tiki products from stg_raw_products to ods_product_clean...")
    
    # Delete existing Tiki products first
    deleted = await conn.execute("DELETE FROM ods_product_clean WHERE source_platform = 'tiki'")
    print(f"Deleted existing Tiki products: {deleted}")
    
    result = await conn.execute("""
        INSERT INTO ods_product_clean (
            global_product_id, source_platform, platform_product_id,
            product_name, brand_name, category, seller_name,
            price_current, price_original, discount_percent,
            rating_avg, review_count, url, image_url,
            crawled_at, created_at, last_seen
        )
        SELECT DISTINCT ON (raw_data->>'product_id') 
            'tiki_' || (raw_data->>'product_id')::text,
            source_platform,
            (raw_data->>'product_id')::text,
            (raw_data->>'product_name')::text,
            (raw_data->>'brand')::text,
            (raw_data->>'category')::text,
            (raw_data->>'seller_name')::text,
            (raw_data->>'price_current')::numeric,
            (raw_data->>'price_original')::numeric,
            (raw_data->>'discount_percent')::numeric,
            (raw_data->>'rating_avg')::numeric,
            (raw_data->>'review_count')::integer,
            (raw_data->>'url')::text,
            (raw_data->'image_urls'->>0)::text,
            (raw_data->>'crawl_date')::timestamp,
            NOW(),
            NOW()
        FROM stg_raw_products
        WHERE source_platform = 'tiki'
        AND raw_data->>'product_id' IS NOT NULL
    """)
    
    await conn.close()
    
    print(f"SUCCESS: Loaded Tiki products to ODS")
    print(f"Result: {result}")

if __name__ == "__main__":
    asyncio.run(load_tiki_to_ods())
