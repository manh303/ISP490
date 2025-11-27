"""
Detailed revenue calculation explanation based on actual database data
"""
import asyncio
import asyncpg
from app.db_config import DATABASE_URL


async def main():
    conn = await asyncpg.connect(dsn=DATABASE_URL)
    
    # Get actual data
    query = """
    SELECT
        dp.product_key,
        dp.product_name,
        AVG(f.avg_price) AS current_price,
        SUM(f.total_review_count) AS total_reviews,
        AVG(f.avg_rating) AS avg_rating,
        -- Calculate revenue same as DSS service
        CASE 
            WHEN SUM(f.total_review_count) > 0 THEN
                SUM(f.avg_price * f.total_review_count * 75)
            ELSE
                CASE 
                    WHEN AVG(f.avg_price) < 100000 THEN AVG(f.avg_price) * 300
                    WHEN AVG(f.avg_price) < 500000 THEN AVG(f.avg_price) * 150
                    WHEN AVG(f.avg_price) < 2000000 THEN AVG(f.avg_price) * 50
                    ELSE AVG(f.avg_price) * 20
                END
        END AS current_revenue
    FROM dwh.dim_product dp
    JOIN dwh.fact_product_daily f ON dp.product_sk = f.product_sk
    JOIN dwh.dim_date dd ON f.date_sk = dd.date_sk
    WHERE dp.product_key IN ('tiki_200027267', 'tiki_200026981')
      AND dd.date_value BETWEEN '2025-11-01' AND '2025-11-20'
    GROUP BY dp.product_key, dp.product_name, f.product_sk, f.platform_sk
    ORDER BY dp.product_key;
    """
    
    rows = await conn.fetch(query)
    
    # Get predictions
    pred_query = """
    WITH latest_pred AS (
        SELECT
            dp.product_key,
            pred.predicted_price,
            pred.ci_upper,
            pred.ci_lower,
            GREATEST(0.0, LEAST(1.0, 
                1.0 - (pred.ci_upper - pred.ci_lower) / NULLIF(pred.predicted_price, 0)
            )) AS confidence,
            ROW_NUMBER() OVER (PARTITION BY dp.product_key ORDER BY pred.created_at DESC) as rn
        FROM ml.fact_price_prediction pred
        JOIN dwh.dim_product dp ON pred.product_sk = dp.product_sk
        WHERE dp.product_key IN ('tiki_200027267', 'tiki_200026981')
    )
    SELECT * FROM latest_pred WHERE rn = 1;
    """
    
    pred_rows = await conn.fetch(pred_query)
    predictions = {row['product_key']: row for row in pred_rows}
    
    print("\n" + "="*100)
    print("DETAILED REVENUE CALCULATION FROM DATABASE")
    print("="*100)
    
    for row in rows:
        product_key = row['product_key']
        pred = predictions.get(product_key)
        
        print(f"\n{'='*100}")
        print(f"Product: {row['product_name'][:80]}")
        print(f"Product Key: {product_key}")
        print(f"{'='*100}")
        
        print(f"\n📊 CURRENT DATA:")
        print(f"   Current Price: {row['current_price']:,.2f} VND")
        print(f"   Total Reviews: {row['total_reviews']}")
        print(f"   Avg Rating: {row['avg_rating'] if row['avg_rating'] else 'N/A'}")
        
        print(f"\n💰 CURRENT REVENUE CALCULATION:")
        if row['total_reviews'] > 0:
            print(f"   Method: HAS REVIEWS (Real Revenue)")
            print(f"   Formula: current_price × total_reviews × 75")
            print(f"   Calculation: {row['current_price']:,.2f} × {row['total_reviews']} × 75")
            print(f"   Current Revenue: {row['current_revenue']:,.2f} VND")
        else:
            price = row['current_price']
            if price < 100000:
                mult, tier = 300, "Low price (< 100k)"
            elif price < 500000:
                mult, tier = 150, "Mid price (100k-500k)"
            elif price < 2000000:
                mult, tier = 50, "High price (500k-2M)"
            else:
                mult, tier = 20, "Very high price (> 2M)"
            
            print(f"   Method: NO REVIEWS (Mock Revenue)")
            print(f"   Price Tier: {tier}")
            print(f"   Multiplier: {mult}x")
            print(f"   Formula: current_price × {mult}")
            print(f"   Calculation: {price:,.2f} × {mult}")
            print(f"   Current Revenue: {row['current_revenue']:,.2f} VND")
        
        if pred:
            print(f"\n🔮 PRICE PREDICTION:")
            print(f"   Predicted Price: {pred['predicted_price']:,.2f} VND")
            print(f"   Confidence: {pred['confidence']:.2%}")
            print(f"   Price Change: {(pred['predicted_price'] / row['current_price'] - 1):.2%}")
            
            projected_revenue = row['current_revenue'] * (pred['predicted_price'] / row['current_price'])
            
            print(f"\n💵 PROJECTED REVENUE CALCULATION:")
            print(f"   Formula: current_revenue × (predicted_price / current_price)")
            print(f"   Calculation: {row['current_revenue']:,.2f} × ({pred['predicted_price']:,.2f} / {row['current_price']:,.2f})")
            print(f"   Calculation: {row['current_revenue']:,.2f} × {pred['predicted_price'] / row['current_price']:.4f}")
            print(f"   Projected Revenue: {projected_revenue:,.2f} VND")
            
            revenue_change = (projected_revenue / row['current_revenue'] - 1) if row['current_revenue'] > 0 else 0
            print(f"\n📈 REVENUE IMPACT:")
            print(f"   Revenue Change: {revenue_change:.2%}")
            print(f"   Revenue Increase: {(projected_revenue - row['current_revenue']):,.2f} VND")
    
    await conn.close()
    print("\n" + "="*100)
    print("✅ Analysis Complete!")
    print("="*100 + "\n")


asyncio.run(main())
