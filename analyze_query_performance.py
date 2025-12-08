"""
Analyze DSS query performance and suggest optimizations
"""
import asyncio
import asyncpg
import time
from datetime import date

async def analyze_query_performance():
    # Connect to Render database
    conn = await asyncpg.connect(
        host="dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com",
        port=5432,
        database="ecommerce_dss_1",
        user="dss_user",
        password="6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G",
        ssl="require"
    )
    
    try:
        print("=" * 80)
        print("DSS Query Performance Analysis")
        print("=" * 80)
        
        # Test parameters (simplified)
        from_date = date(2025, 12, 8)
        to_date = date(2025, 12, 8)
        platforms = ["tiki"]
        categories = ["1"]
        
        # 1. Check table sizes
        print("\n📊 Table Sizes:")
        tables = [
            "dwh.fact_product_daily",
            "dwh.dim_product",
            "dwh.dim_platform",
            "dwh.dim_category",
            "dwh.dim_date",
            "ml.fact_price_prediction"
        ]
        
        for table in tables:
            result = await conn.fetchrow(f"SELECT COUNT(*) as count FROM {table}")
            print(f"  {table}: {result['count']:,} rows")
        
        # 2. Check indexes
        print("\n🔍 Existing Indexes on key tables:")
        indexes_query = """
            SELECT 
                tablename,
                indexname,
                indexdef
            FROM pg_indexes
            WHERE schemaname IN ('dwh', 'ml')
            AND tablename IN ('fact_product_daily', 'fact_price_prediction', 'dim_product')
            ORDER BY tablename, indexname
        """
        indexes = await conn.fetch(indexes_query)
        for idx in indexes:
            print(f"\n  {idx['tablename']}.{idx['indexname']}:")
            print(f"    {idx['indexdef']}")
        
        # 3. Analyze specific bottlenecks
        print("\n\n⏱️  Performance Analysis:")
        
        # Test 1: Product metrics aggregation
        print("\n  1. Product metrics aggregation (fact_product_daily)...")
        start = time.perf_counter()
        result = await conn.fetch("""
            SELECT
                f.product_sk,
                f.platform_sk,
                AVG(f.avg_price) AS avg_price,
                SUM(COALESCE(f.total_orders, 0)) AS total_orders
            FROM dwh.fact_product_daily f
            JOIN dwh.dim_date dd ON dd.date_sk = f.date_sk
            WHERE dd.date_value BETWEEN $1 AND $2
            GROUP BY f.product_sk, f.platform_sk
            LIMIT 100
        """, from_date, to_date)
        duration1 = time.perf_counter() - start
        print(f"     Time: {duration1:.2f}s ({len(result)} rows)")
        
        # Test 2: Latest predictions
        print("\n  2. Latest predictions (ml.fact_price_prediction)...")
        start = time.perf_counter()
        result = await conn.fetch("""
            SELECT 
                pred.product_sk,
                pred.platform_sk,
                pred.predicted_price,
                pred.prediction_confidence,
                ROW_NUMBER() OVER (
                    PARTITION BY pred.product_sk, pred.platform_sk
                    ORDER BY pred.created_at DESC
                ) AS rn
            FROM ml.fact_price_prediction pred
            LIMIT 100
        """)
        duration2 = time.perf_counter() - start
        print(f"     Time: {duration2:.2f}s ({len(result)} rows)")
        
        # Test 3: Full join
        print("\n  3. Join predictions + metrics...")
        start = time.perf_counter()
        result = await conn.fetch("""
            WITH product_metrics AS (
                SELECT
                    f.product_sk,
                    f.platform_sk,
                    AVG(f.avg_price) AS avg_price
                FROM dwh.fact_product_daily f
                JOIN dwh.dim_date dd ON dd.date_sk = f.date_sk
                WHERE dd.date_value BETWEEN $1 AND $2
                GROUP BY f.product_sk, f.platform_sk
            ),
            latest_predictions AS (
                SELECT 
                    pred.product_sk,
                    pred.platform_sk,
                    pred.predicted_price,
                    ROW_NUMBER() OVER (
                        PARTITION BY pred.product_sk, pred.platform_sk
                        ORDER BY pred.created_at DESC
                    ) AS rn
                FROM ml.fact_price_prediction pred
            )
            SELECT COUNT(*) as count
            FROM latest_predictions pred
            JOIN product_metrics pm 
                ON pm.product_sk = pred.product_sk 
                AND pm.platform_sk = pred.platform_sk
            WHERE pred.rn = 1
              AND pm.avg_price > 0
        """, from_date, to_date)
        duration3 = time.perf_counter() - start
        print(f"     Time: {duration3:.2f}s (matched: {result[0]['count']})")
        
        # Summary
        print(f"\n\n📈 Performance Breakdown:")
        print(f"  Product metrics:  {duration1:.2f}s")
        print(f"  Predictions:      {duration2:.2f}s")
        print(f"  Join + filter:    {duration3:.2f}s")
        print(f"  Estimated total:  {duration1 + duration2 + duration3:.2f}s")
        
        # Recommendations
        print("\n\n💡 Optimization Recommendations:")
        print("  1. Add index on fact_product_daily(date_sk) - for date filtering")
        print("  2. Add index on fact_price_prediction(created_at) - for latest predictions")
        print("  3. Add index on fact_price_prediction(product_sk, platform_sk, created_at) - for window function")
        print("  4. Consider materialized view for product_metrics if date range is stable")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(analyze_query_performance())
