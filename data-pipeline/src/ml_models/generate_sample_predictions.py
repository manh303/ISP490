#!/usr/bin/env python3
"""Generate sample ML predictions for demo"""
import psycopg2
import random
from datetime import datetime, timedelta

DB_CONFIG = {
    'host': 'dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com',
    'port': '5432',
    'database': 'ecommerce_dss_1',
    'user': 'dss_user',
    'password': '6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G'
}

def generate_recommendations(conn):
    """Generate sample product recommendations"""
    cur = conn.cursor()
    
    # Get random products
    cur.execute("SELECT product_sk FROM dwh_dim_product WHERE is_current = TRUE LIMIT 100")
    products = [r[0] for r in cur.fetchall()]
    
    count = 0
    for product_sk in products[:20]:
        # Generate 5 recommendations per product
        recommended = random.sample([p for p in products if p != product_sk], 5)
        for rec_sk in recommended:
            cur.execute("""
                INSERT INTO ml_product_recommendations 
                (product_sk, recommended_product_sk, similarity_score, recommendation_type)
                VALUES (%s, %s, %s, %s)
            """, (product_sk, rec_sk, random.uniform(0.7, 0.99), 'collaborative'))
            count += 1
    
    conn.commit()
    print(f"Generated {count} recommendations")

def generate_price_predictions(conn):
    """Generate sample price predictions"""
    cur = conn.cursor()
    
    # Get products with current prices
    cur.execute("""
        SELECT DISTINCT f.product_sk, f.platform_sk, f.price_current
        FROM dwh_fact_product_daily f
        WHERE f.price_current > 0
        LIMIT 50
    """)
    
    count = 0
    for product_sk, platform_sk, current_price in cur.fetchall():
        # Predict next 7 days
        for days_ahead in range(1, 8):
            pred_date = datetime.now().date() + timedelta(days=days_ahead)
            # Random walk with slight trend
            predicted = float(current_price) * random.uniform(0.95, 1.05)
            lower = predicted * 0.95
            upper = predicted * 1.05
            
            cur.execute("""
                INSERT INTO ml_price_predictions
                (product_sk, platform_sk, prediction_date, predicted_price, 
                 confidence_interval_lower, confidence_interval_upper, model_version)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (product_sk, platform_sk, pred_date, predicted, lower, upper, 'v1.0'))
            count += 1
    
    conn.commit()
    print(f"Generated {count} price predictions")

def generate_demand_forecast(conn):
    """Generate sample demand forecasts"""
    cur = conn.cursor()
    
    cur.execute("SELECT product_sk FROM dwh_dim_product WHERE is_current = TRUE LIMIT 30")
    
    count = 0
    for (product_sk,) in cur.fetchall():
        # Forecast next 30 days
        for days_ahead in range(1, 31):
            forecast_date = datetime.now().date() + timedelta(days=days_ahead)
            demand = random.randint(10, 100)
            confidence = random.uniform(0.7, 0.95)
            
            cur.execute("""
                INSERT INTO ml_demand_forecast
                (product_sk, forecast_date, predicted_demand, confidence_level, model_version)
                VALUES (%s, %s, %s, %s, %s)
            """, (product_sk, forecast_date, demand, confidence, 'v1.0'))
            count += 1
    
    conn.commit()
    print(f"Generated {count} demand forecasts")

def main():
    print("Generating sample ML predictions...")
    conn = psycopg2.connect(**DB_CONFIG)
    
    generate_recommendations(conn)
    generate_price_predictions(conn)
    generate_demand_forecast(conn)
    
    conn.close()
    print("\nDone!")

if __name__ == "__main__":
    main()
