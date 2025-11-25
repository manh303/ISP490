"""Check DSS data availability for debugging"""
import asyncio
import asyncpg
import os
from datetime import date, timedelta

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "ecommerce_dss_1"),
    "user": os.getenv("DB_USER", "dss_user"),
    "password": os.getenv("DB_PASSWORD", "6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G"),
}

async def check_data():
    conn = await asyncpg.connect(**DB_CONFIG)
    
    print("=" * 80)
    print("CHECKING DSS DATA AVAILABILITY")
    print("=" * 80)
    
    # 1. Check ml.fact_price_prediction table
    print("\n1. ML Price Predictions Table:")
    count = await conn.fetchval("SELECT COUNT(*) FROM ml.fact_price_prediction")
    print(f"   Total predictions: {count}")
    
    if count > 0:
        # Check date range
        date_range = await conn.fetchrow("""
            SELECT 
                MIN(dd.date_value) as min_date,
                MAX(dd.date_value) as max_date,
                COUNT(DISTINCT pred.date_sk) as distinct_dates
            FROM ml.fact_price_prediction pred
            JOIN dwh.dim_date dd ON pred.date_sk = dd.date_sk
        """)
        print(f"   Date range: {date_range['min_date']} to {date_range['max_date']}")
        print(f"   Distinct dates: {date_range['distinct_dates']}")
        
        # Check recent predictions
        recent = await conn.fetch("""
            SELECT dd.date_value, COUNT(*) as count
            FROM ml.fact_price_prediction pred
            JOIN dwh.dim_date dd ON pred.date_sk = dd.date_sk
            GROUP BY dd.date_value
            ORDER BY dd.date_value DESC
            LIMIT 5
        """)
        print(f"   Recent predictions:")
        for row in recent:
            print(f"     - {row['date_value']}: {row['count']} predictions")
    
    # 2. Check fact_product_daily for today
    print("\n2. Fact Product Daily (source data):")
    today = date.today()
    yesterday = today - timedelta(days=1)
    
    for check_date in [today, yesterday]:
        count = await conn.fetchval("""
            SELECT COUNT(*)
            FROM dwh.fact_product_daily f
            JOIN dwh.dim_date dd ON f.date_sk = dd.date_sk
            WHERE dd.date_value = $1
        """, check_date)
        print(f"   {check_date}: {count} records")
    
    # 3. Check categories
    print("\n3. Available Categories:")
    categories = await conn.fetch("""
        SELECT dc.category_sk, dc.category_lvl1, dc.category_lvl2, COUNT(dp.product_sk) as product_count
        FROM dwh.dim_category dc
        LEFT JOIN dwh.dim_product dp ON dc.category_sk = dp.category_sk
        GROUP BY dc.category_sk, dc.category_lvl1, dc.category_lvl2
        ORDER BY dc.category_sk
        LIMIT 10
    """)
    for cat in categories:
        print(f"   Category {cat['category_sk']}: {cat['category_lvl1']} > {cat['category_lvl2']} ({cat['product_count']} products)")
    
    # 4. Check platforms
    print("\n4. Available Platforms:")
    platforms = await conn.fetch("""
        SELECT platform_sk, platform_code, platform_name
        FROM dwh.dim_platform
    """)
    for plat in platforms:
        print(f"   {plat['platform_sk']}: {plat['platform_code']} ({plat['platform_name']})")
    
    # 5. Try the actual DSS query with relaxed filters
    print("\n5. Testing DSS Query (relaxed filters):")
    
    # Get the most recent date with predictions
    latest_pred_date = await conn.fetchval("""
        SELECT MAX(dd.date_value)
        FROM ml.fact_price_prediction pred
        JOIN dwh.dim_date dd ON pred.date_sk = dd.date_sk
    """)
    
    if latest_pred_date:
        print(f"   Using latest prediction date: {latest_pred_date}")
        
        # Try query without category filter
        test_query = """
            WITH latest_predictions AS (
                SELECT DISTINCT ON (pred.product_sk, pred.platform_sk)
                    pred.product_sk,
                    pred.platform_sk,
                    pred.predicted_price,
                    (pred.ci_upper - pred.ci_lower) AS confidence_range,
                    1.0 - LEAST((pred.ci_upper - pred.ci_lower) / NULLIF(pred.predicted_price, 0), 1.0) AS confidence
                FROM ml.fact_price_prediction pred
                ORDER BY pred.product_sk, pred.platform_sk, pred.created_at DESC
            ),
            product_metrics AS (
                SELECT
                    f.product_sk,
                    f.platform_sk,
                    AVG(f.avg_price) AS current_price,
                    SUM(f.avg_price * f.total_review_count) AS current_revenue,
                    SUM(f.total_review_count) AS total_orders
                FROM dwh.fact_product_daily f
                JOIN dwh.dim_date dd ON f.date_sk = dd.date_sk
                WHERE dd.date_value = $1
                GROUP BY f.product_sk, f.platform_sk
            )
            SELECT COUNT(*) as matching_products
            FROM latest_predictions pred
            JOIN dwh.dim_product dp ON pred.product_sk = dp.product_sk
            JOIN dwh.dim_platform dpl ON pred.platform_sk = dpl.platform_sk
            LEFT JOIN product_metrics pm ON dp.product_sk = pm.product_sk AND pred.platform_sk = pm.platform_sk
            WHERE pm.current_price > 0
              AND pred.predicted_price > 0
              AND pred.confidence >= 0.70
        """
        
        count = await conn.fetchval(test_query, latest_pred_date)
        print(f"   Products matching DSS criteria (no category filter): {count}")
        
        # Now try with platform filter
        count_tiki = await conn.fetchval(test_query + " AND dpl.platform_code = 'tiki'", latest_pred_date)
        count_lazada = await conn.fetchval(test_query + " AND dpl.platform_code = 'lazada'", latest_pred_date)
        print(f"   - Tiki: {count_tiki}")
        print(f"   - Lazada: {count_lazada}")
    else:
        print("   ⚠️ NO PREDICTIONS FOUND IN DATABASE!")
    
    # 6. Check if ML pipeline has been run
    print("\n6. ML Model Metadata:")
    models = await conn.fetch("""
        SELECT model_name, model_version, model_type, status, 
               training_data_until, created_at
        FROM ml.dim_ml_model
        ORDER BY created_at DESC
        LIMIT 5
    """)
    if models:
        print(f"   Found {len(models)} models:")
        for model in models:
            print(f"     - {model['model_name']} v{model['model_version']} ({model['model_type']}) - {model['status']}")
            print(f"       Trained until: {model['training_data_until']}, Created: {model['created_at']}")
    else:
        print("   ⚠️ NO ML MODELS FOUND!")
    
    print("\n" + "=" * 80)
    print("DIAGNOSIS:")
    print("=" * 80)
    
    if count == 0:
        print("❌ NO DATA AVAILABLE FOR DSS ANALYSIS")
        print("\nRECOMMENDATIONS:")
        print("1. Run ML price prediction pipeline:")
        print("   python ml/run_price_predictions.py")
        print("\n2. Or populate mock ML data for testing:")
        print("   python populate_mock_ml_data.py")
        print("\n3. Check if fact_product_daily has recent data")
    else:
        print(f"✅ Found {count} products available for analysis")
        print(f"   Use date: {latest_pred_date} instead of today's date")
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(check_data())

