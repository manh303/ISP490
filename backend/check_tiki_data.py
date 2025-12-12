import asyncio
import asyncpg
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

async def check_tiki_data():
    conn = await asyncpg.connect(
        host="localhost",
        port=5433,
        database="ecommerce_dss",
        user="dss_user",
        password="dss_password_123"
    )
    
    print("=" * 60)
    print("KIỂM TRA DỮ LIỆU TIKI")
    print("=" * 60)
    
    # Check ods_product_clean
    tiki_products = await conn.fetchval(
        "SELECT COUNT(*) FROM ods_product_clean WHERE source_platform = 'tiki'"
    )
    print(f"\n✓ Tiki products trong ods_product_clean: {tiki_products:,}")
    
    # Check ods_review_clean
    tiki_reviews = await conn.fetchval(
        "SELECT COUNT(*) FROM ods_review_clean WHERE source_platform = 'tiki'"
    )
    print(f"✓ Tiki reviews trong ods_review_clean: {tiki_reviews:,}")
    
    # Check all platforms
    platforms = await conn.fetch(
        "SELECT source_platform, COUNT(*) as count FROM ods_product_clean GROUP BY source_platform"
    )
    print(f"\n{'Platform':<15} {'Products':>10}")
    print("-" * 30)
    for row in platforms:
        print(f"{row['source_platform']:<15} {row['count']:>10,}")
    
    # Sample Tiki products
    if tiki_products > 0:
        samples = await conn.fetch(
            "SELECT product_name, price_current, rating_avg FROM ods_product_clean WHERE source_platform = 'tiki' LIMIT 5"
        )
        print(f"\n📦 Sample Tiki products:")
        for i, row in enumerate(samples, 1):
            print(f"{i}. {row['product_name'][:50]} - {row['price_current']:,.0f}đ - ⭐{row['rating_avg']}")
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(check_tiki_data())
