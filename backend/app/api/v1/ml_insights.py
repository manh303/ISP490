# -*- coding: utf-8 -*-
from fastapi import APIRouter, Query
from sqlalchemy import create_engine, text
from typing import List, Optional
import os

router = APIRouter(prefix="/ml", tags=["ML Insights"])

DB_URL = os.getenv("DATABASE_URL", "postgresql://dss_user:IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4@dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com/ecommerce_dss")
engine = create_engine(DB_URL)

@router.get("/price-optimization")
def get_price_optimization(
    limit: int = Query(100, ge=1, le=1000),
    recommendation: Optional[str] = Query(None, description="Filter: Increase Price, Decrease Price, Maintain Price")
):
    query = "SELECT product_sk, product_name, current_price, optimal_price, expected_margin_change, recommendation, price_position FROM mart_price_optimization"
    if recommendation:
        query += f" WHERE recommendation = '{recommendation}'"
    query += f" ORDER BY ABS(expected_margin_change) DESC LIMIT {limit}"
    
    with engine.connect() as conn:
        result = conn.execute(text(query))
        return [{"product_sk": r[0], "product_name": r[1], "current_price": float(r[2]), "optimal_price": float(r[3]), "expected_margin_change": float(r[4]), "recommendation": r[5], "price_position": r[6]} for r in result]

@router.get("/demand-forecast")
def get_demand_forecast(
    limit: int = Query(100, ge=1, le=1000),
    trend: Optional[str] = Query(None, description="Filter: Growing, Declining, Stable")
):
    query = "SELECT product_sk, product_name, recent_demand, baseline_demand, demand_trend, forecast_7d, forecast_30d, quality_score, stock_recommendation FROM mart_demand_forecast"
    if trend:
        query += f" WHERE demand_trend = '{trend}'"
    query += f" ORDER BY forecast_7d DESC LIMIT {limit}"
    
    with engine.connect() as conn:
        result = conn.execute(text(query))
        return [{"product_sk": r[0], "product_name": r[1], "recent_demand": float(r[2]) if r[2] else 0, "baseline_demand": float(r[3]) if r[3] else 0, "demand_trend": r[4], "forecast_7d": float(r[5]) if r[5] else 0, "forecast_30d": float(r[6]) if r[6] else 0, "quality_score": float(r[7]) if r[7] else 0, "stock_recommendation": r[8]} for r in result]

@router.get("/sales-forecast/weekly")
def get_weekly_sales_forecast():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT year_num, day_of_week, avg_weekly_reviews, avg_weekly_rating FROM mart_sales_forecast_weekly ORDER BY year_num, day_of_week"))
        return [{"year": r[0], "day_of_week": r[1], "avg_reviews": float(r[2]) if r[2] else 0, "avg_rating": float(r[3]) if r[3] else 0} for r in result]

@router.get("/sales-forecast/trend")
def get_sales_trend():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT year_num, month_num, total_monthly_reviews, avg_monthly_rating, prev_month_reviews, growth_rate, trend FROM mart_sales_trend ORDER BY year_num, month_num"))
        return [{"year": r[0], "month": r[1], "total_reviews": int(r[2]) if r[2] else 0, "avg_rating": float(r[3]) if r[3] else 0, "prev_month_reviews": int(r[4]) if r[4] else 0, "growth_rate": float(r[5]) if r[5] else 0, "trend": r[6]} for r in result]

@router.get("/sales-forecast/seasonality")
def get_seasonality():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT season, avg_seasonal_reviews, avg_seasonal_rating, seasonality_index FROM mart_seasonality ORDER BY seasonality_index DESC"))
        return [{"season": r[0], "avg_reviews": float(r[1]) if r[1] else 0, "avg_rating": float(r[2]) if r[2] else 0, "seasonality_index": float(r[3]) if r[3] else 1} for r in result]

@router.get("/insights/summary")
def get_ml_summary():
    with engine.connect() as conn:
        price_increase = conn.execute(text("SELECT COUNT(*) FROM mart_price_optimization WHERE recommendation = 'Increase Price'")).scalar()
        price_decrease = conn.execute(text("SELECT COUNT(*) FROM mart_price_optimization WHERE recommendation = 'Decrease Price'")).scalar()
        growing_demand = conn.execute(text("SELECT COUNT(*) FROM mart_demand_forecast WHERE demand_trend = 'Growing'")).scalar()
        declining_demand = conn.execute(text("SELECT COUNT(*) FROM mart_demand_forecast WHERE demand_trend = 'Declining'")).scalar()
        total_products = conn.execute(text("SELECT COUNT(*) FROM mart_price_optimization")).scalar()
        
        return {
            "price_optimization": {"increase": price_increase, "decrease": price_decrease, "maintain": total_products - price_increase - price_decrease},
            "demand_forecast": {"growing": growing_demand, "declining": declining_demand, "stable": conn.execute(text("SELECT COUNT(*) FROM mart_demand_forecast")).scalar() - growing_demand - declining_demand},
            "total_products_analyzed": total_products
        }
