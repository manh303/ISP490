"""ML Model Serving API"""
from fastapi import APIRouter, HTTPException
from typing import List
from sqlalchemy import create_engine, text
from pydantic import BaseModel
from datetime import date
import psycopg2
import os

router = APIRouter(prefix="/ml", tags=["ML Insights"])

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'ecommerce_dss'),
    'user': os.getenv('DB_USER', 'dss_user'),
    'password'
    : os.getenv('DB_PASSWORD', 'IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4')
}

class ProductRecommendation(BaseModel):
    product_sk: int
    product_name: str
    similarity_score: float
    recommendation_type: str

class PricePrediction(BaseModel):
    product_sk: int
    platform_sk: int
    prediction_date: date
    predicted_price: float
    confidence_lower: float
    confidence_upper: float

class DemandForecast(BaseModel):
    product_sk: int
    forecast_date: date
    predicted_demand: int
    confidence_level: float

@router.get("/recommendations/{product_sk}", response_model=List[ProductRecommendation])
def get_product_recommendations(product_sk: int, limit: int = 10):
    """Get product recommendations"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            r.recommended_product_sk,
            p.product_name,
            r.similarity_score,
            r.recommendation_type
        FROM ml_product_recommendations r
        JOIN dwh_dim_product p ON r.recommended_product_sk = p.product_sk
        WHERE r.product_sk = %s
        ORDER BY r.similarity_score DESC
        LIMIT %s
    """, (product_sk, limit))
    
    results = []
    for row in cur.fetchall():
        results.append(ProductRecommendation(
            product_sk=row[0],
            product_name=row[1],
            similarity_score=float(row[2]),
            recommendation_type=row[3]
        ))
    
    conn.close()
    return results

@router.get("/price-prediction/{product_sk}", response_model=List[PricePrediction])
def get_price_predictions(product_sk: int, days: int = 7):
    """Get price predictions for next N days"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            product_sk,
            platform_sk,
            prediction_date,
            predicted_price,
            confidence_interval_lower,
            confidence_interval_upper
        FROM ml_price_predictions
        WHERE product_sk = %s
        AND prediction_date >= CURRENT_DATE
        ORDER BY prediction_date
        LIMIT %s
    """, (product_sk, days))
    
    results = []
    for row in cur.fetchall():
        results.append(PricePrediction(
            product_sk=row[0],
            platform_sk=row[1],
            prediction_date=row[2],
            predicted_price=float(row[3]),
            confidence_lower=float(row[4]),
            confidence_upper=float(row[5])
        ))
    
    conn.close()
    return results

@router.get("/demand-forecast/{product_sk}", response_model=List[DemandForecast])
def get_demand_forecast(product_sk: int, days: int = 30):
    """Get demand forecast for next N days"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            product_sk,
            forecast_date,
            predicted_demand,
            confidence_level
        FROM ml_demand_forecast
        WHERE product_sk = %s
        AND forecast_date >= CURRENT_DATE
        ORDER BY forecast_date
        LIMIT %s
    """, (product_sk, days))
    
    results = []
    for row in cur.fetchall():
        results.append(DemandForecast(
            product_sk=row[0],
            forecast_date=row[1],
            predicted_demand=row[2],
            confidence_level=float(row[3])
        ))
    
    conn.close()
    return results

@router.post("/what-if/price")
def price_what_if_analysis(product_sk: int, new_price: float):
    """What-if analysis: predict demand change with new price"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    cur.execute("""
        SELECT price_current 
        FROM dwh_fact_product_daily 
        WHERE product_sk = %s 
        ORDER BY date_sk DESC 
        LIMIT 1
    """, (product_sk,))
    
    result = cur.fetchone()
    if not result:
        raise HTTPException(404, "Product not found")
    
    current_price = float(result[0])
    price_change_pct = ((new_price - current_price) / current_price) * 100
    demand_change_pct = price_change_pct * -1.5
    
    conn.close()
    
    return {
        "product_sk": product_sk,
        "current_price": current_price,
        "new_price": new_price,
        "price_change_pct": round(price_change_pct, 2),
        "estimated_demand_change_pct": round(demand_change_pct, 2),
        "recommendation": "increase" if demand_change_pct > 0 else "decrease"
    }
