"""
DSS (Decision Support System) API Router
AI-powered decision support endpoints for analysts
"""

import os
import logging
from typing import Dict, Any, Optional
from fastapi import APIRouter, Depends, HTTPException
import asyncpg

from app.schemas.dss import (
    PricePredictionRequest,
    PricePredictionResponse,
    ProductRecommendationRequest,
    ProductRecommendationResponse,
    ReviewSentimentRequest,
    ReviewSentimentResponse,
)
from app.services.dss_service import DSSService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/dss", tags=["DSS - Decision Support System"])

# Database configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "ecommerce_dss_1"),
    "user": os.getenv("DB_USER", "dss_user"),
    "password": os.getenv("DB_PASSWORD", "6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G"),
}

# Connection pool configuration
POOL_CONFIG = {
    **DB_CONFIG,
    "min_size": int(os.getenv("DB_POOL_MIN_SIZE", "2")),
    "max_size": int(os.getenv("DB_POOL_MAX_SIZE", "10")),
    "command_timeout": int(os.getenv("DB_COMMAND_TIMEOUT", "60")),
    "timeout": int(os.getenv("DB_CONNECTION_TIMEOUT", "30")),
}

# Global connection pool
_db_pool: Optional[asyncpg.Pool] = None


async def get_db_pool() -> asyncpg.Pool:
    """Get or create database connection pool"""
    global _db_pool
    if _db_pool is None:
        logger.info("Creating database connection pool...")
        _db_pool = await asyncpg.create_pool(**POOL_CONFIG)
        logger.info(f"Connection pool created: min={POOL_CONFIG['min_size']}, max={POOL_CONFIG['max_size']}")
    return _db_pool


async def get_db():
    """Get database connection from pool"""
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        yield conn


async def get_dss_service(db=Depends(get_db)) -> DSSService:
    """Get DSS service instance"""
    return DSSService(db)


# ============================================
# PRICE PREDICTION DSS
# ============================================

@router.post("/price/run", response_model=Dict[str, Any])
async def run_price_prediction_dss(
    request: PricePredictionRequest,
    service: DSSService = Depends(get_dss_service),
):
    """
    Run Price Prediction DSS Analysis
    
    **Purpose:**
    Analyze product pricing and generate AI-powered recommendations for price optimization.
    
    **Workflow:**
    1. Query fact tables + ML price predictions from database
    2. Calculate KPIs (revenue impact, number of products with recommendations)
    3. Generate AI insights and action recommendations
    4. Return structured results with AI analysis
    
    **Use Cases:**
    - Identify products with pricing opportunities
    - Estimate revenue impact of price changes
    - Get AI recommendations for pricing strategy
    
    **Example Request:**
    ```json
    {
      "from_date": "2025-11-23",
      "to_date": "2025-11-24",
      "platforms": ["tiki", "lazada"],
      "categories": ["1", "2"],
      "scope_mode": "top_n",
      "top_n": 50,
      "max_discount_pct": 0.15,
      "min_confidence": 0.70,
      "min_price_change_pct": 0.02
    }
    ```
    
    **Note:** API supports auto-fallback to latest available date if requested date has no data.
    Check `date_adjustment_info` in response for actual dates used.
    
    **Response Includes:**
    - `kpi_summary`: Overall metrics (num_products, revenue impact)
    - `table_data`: Detailed product-level data
    - `ai_summary_insights`: 3-5 AI-generated insights
    - `ai_recommended_actions`: 3-7 actionable recommendations
    - `date_adjustment_info`: (optional) Date adjustment details if fallback occurred
    """
    
    try:
        # Convert request to dict
        request_dict = request.model_dump()
        
        # Run DSS analysis
        result = await service.run_price_prediction_dss(request_dict)
        
        return result
        
    except Exception as e:
        logger.error(f"Error in price prediction DSS: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to run price prediction DSS: {str(e)}"
        )


# ============================================
# PRODUCT RECOMMENDATION DSS
# ============================================

@router.post("/reco/run", response_model=Dict[str, Any])
async def run_product_recommendation_dss(
    request: ProductRecommendationRequest,
    service: DSSService = Depends(get_dss_service),
):
    """
    Run Product Recommendation DSS Analysis
    
    **Purpose:**
    Analyze product relationships and generate AI-powered cross-sell/upsell recommendations.
    
    **Workflow:**
    1. Query ML product recommendations from database
    2. Calculate KPIs (similarity scores, bundle opportunities)
    3. Generate AI insights for cross-sell/upsell strategies
    4. Return structured results with AI analysis
    
    **Use Cases:**
    - Find products frequently bought together
    - Identify bundle opportunities
    - Get AI recommendations for product placement
    - Optimize "You might also like" sections
    
    **Modes:**
    - `by_product`: Get recommendations for a specific product
    - `by_category`: Get top recommendations within category
    
    **Example Request (by_product):**
    ```json
    {
      "from_date": "2025-11-23",
      "to_date": "2025-11-24",
      "platforms": ["tiki"],
      "scope_mode": "by_product",
      "source_product_key": "tiki_9975869",
      "top_k": 10,
      "min_similarity": 0.5
    }
    ```
    
    **Example Request (by_category):**
    ```json
    {
      "from_date": "2025-11-23",
      "to_date": "2025-11-24",
      "platforms": ["tiki", "lazada"],
      "categories": ["1", "2"],
      "scope_mode": "by_category",
      "top_k": 20,
      "min_similarity": 0.6
    }
    ```
    
    **Response Includes:**
    - `kpi_summary`: Overall metrics (num_recommendations, avg_similarity)
    - `table_data`: Product pairs with similarity and co-purchase rates
    - `ai_summary_insights`: 3-5 AI-generated insights
    - `ai_recommended_actions`: 3-7 actionable recommendations for cross-sell
    """
    
    try:
        request_dict = request.model_dump()
        result = await service.run_product_recommendation_dss(request_dict)
        return result
        
    except Exception as e:
        logger.error(f"Error in product recommendation DSS: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to run product recommendation DSS: {str(e)}"
        )


# ============================================
# REVIEW SENTIMENT DSS
# ============================================

@router.post("/review/run", response_model=Dict[str, Any])
async def run_review_sentiment_dss(
    request: ReviewSentimentRequest,
    service: DSSService = Depends(get_dss_service),
):
    """
    Run Review Sentiment Analysis DSS
    
    **Purpose:**
    Analyze customer review sentiment and generate AI-powered recommendations for quality improvement.
    
    **Workflow:**
    1. Query review data + ML sentiment analysis from database
    2. Calculate KPIs (positive/negative percentages, critical products)
    3. Generate AI insights about customer satisfaction issues
    4. Return structured results with AI analysis
    
    **Use Cases:**
    - Identify products with negative sentiment issues
    - Understand top customer complaints
    - Get AI recommendations for quality/CS improvements
    - Monitor brand reputation
    
    **Example Request:**
    ```json
    {
      "from_date": "2025-11-23",
      "to_date": "2025-11-24",
      "platforms": ["tiki", "lazada"],
      "categories": ["1", "2"],
      "min_reviews_per_product": 10,
      "sentiment_focus": "all",
      "negative_threshold": 0.25
    }
    ```
    
    **Sentiment Focus Options:**
    - `all`: Analyze all products
    - `only_negative`: Focus on products with high negative sentiment
    - `only_positive`: Focus on products with high positive sentiment
    
    **Response Includes:**
    - `kpi_summary`: Overall metrics (avg sentiment, critical products count)
    - `table_data`: Product-level sentiment breakdown with top reasons
    - `ai_summary_insights`: 3-5 main customer satisfaction issues
    - `ai_recommended_actions`: 3-7 actionable recommendations for CS/Quality teams
    """
    
    try:
        request_dict = request.model_dump()
        result = await service.run_review_sentiment_dss(request_dict)
        return result
        
    except Exception as e:
        logger.error(f"Error in review sentiment DSS: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to run review sentiment DSS: {str(e)}"
        )


# ============================================
# UTILITY ENDPOINTS
# ============================================

@router.get("/health")
async def dss_health_check():
    """
    Health check for DSS system
    
    Returns status of DSS components:
    - Database connection
    - AI/LLM availability
    - ML tables accessibility
    """
    
    health_status = {
        "status": "healthy",
        "components": {}
    }
    
    # Check database
    try:
        async with asyncpg.create_pool(**DB_CONFIG, min_size=1, max_size=1) as pool:
            async with pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
        health_status["components"]["database"] = "healthy"
    except Exception as e:
        health_status["components"]["database"] = f"unhealthy: {str(e)}"
        health_status["status"] = "degraded"
    
    # Check AI availability
    from app.services.ai_summarizer import get_ai_summarizer
    ai_summarizer = get_ai_summarizer()
    
    if ai_summarizer.available:
        health_status["components"]["ai"] = {
            "status": "healthy",
            "model": ai_summarizer.model
        }
    else:
        health_status["components"]["ai"] = {
            "status": "degraded",
            "mode": "rule-based fallback"
        }
    
    # Check ML tables
    try:
        async with asyncpg.create_pool(**DB_CONFIG, min_size=1, max_size=1) as pool:
            async with pool.acquire() as conn:
                tables = await conn.fetch("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'ml'
                """)
                health_status["components"]["ml_tables"] = {
                    "status": "healthy",
                    "count": len(tables)
                }
    except Exception as e:
        health_status["components"]["ml_tables"] = f"unhealthy: {str(e)}"
        health_status["status"] = "degraded"
    
    return health_status


@router.get("/data/status")
async def get_data_status():
    """
    Check data availability and freshness
    
    Returns:
    - Latest data dates for fact tables and ML predictions
    - How many days behind current date
    - Warnings if data is stale
    - Recommendations for action
    
    Use this endpoint to:
    - Show data freshness warning in UI
    - Monitor data pipeline health
    - Determine if manual refresh is needed
    """
    from datetime import date, timedelta
    
    today = date.today()
    status = {
        "status": "healthy",
        "current_date": str(today),
        "warnings": [],
        "recommendations": []
    }
    
    try:
        async with asyncpg.create_pool(**DB_CONFIG, min_size=1, max_size=1) as pool:
            async with pool.acquire() as conn:
                # Check latest fact_product_daily
                latest_fact = await conn.fetchval("""
                    SELECT MAX(dd.date_value)
                    FROM dwh.fact_product_daily f
                    JOIN dwh.dim_date dd ON f.date_sk = dd.date_sk
                """)
                
                # Check latest ML predictions
                latest_pred = await conn.fetchval("""
                    SELECT MAX(dd.date_value)
                    FROM ml.fact_price_prediction pred
                    JOIN dwh.dim_date dd ON pred.date_sk = dd.date_sk
                """)
                
                # Calculate staleness
                fact_days_old = (today - latest_fact).days if latest_fact else None
                pred_days_old = (today - latest_pred).days if latest_pred else None
                
                status["latest_fact_date"] = str(latest_fact) if latest_fact else None
                status["latest_prediction_date"] = str(latest_pred) if latest_pred else None
                status["fact_days_behind"] = fact_days_old
                status["prediction_days_behind"] = pred_days_old
                
                # Check for issues
                if not latest_fact:
                    status["status"] = "critical"
                    status["warnings"].append("No source data found in database!")
                    status["recommendations"].append("Run initial ETL pipeline")
                elif fact_days_old > 2:
                    status["status"] = "degraded"
                    status["warnings"].append(f"Source data is {fact_days_old} days old (latest: {latest_fact})")
                    status["recommendations"].append("Run ETL pipeline: python load_tiki_pipeline.py")
                
                if not latest_pred:
                    status["status"] = "critical"
                    status["warnings"].append("No ML predictions found!")
                    status["recommendations"].append("Run ML pipeline: python ml/run_price_predictions.py")
                elif pred_days_old > 2:
                    status["status"] = "degraded"
                    status["warnings"].append(f"ML predictions are {pred_days_old} days old (latest: {latest_pred})")
                    status["recommendations"].append("Run ML pipeline: python ml/run_price_predictions.py")
                
                # Get data coverage stats
                coverage = await conn.fetchrow("""
                    SELECT 
                        MIN(dd.date_value) as min_date,
                        MAX(dd.date_value) as max_date,
                        COUNT(DISTINCT dd.date_value) as total_days,
                        COUNT(DISTINCT f.product_sk) as total_products
                    FROM dwh.fact_product_daily f
                    JOIN dwh.dim_date dd ON f.date_sk = dd.date_sk
                """)
                
                status["data_coverage"] = {
                    "min_date": str(coverage['min_date']) if coverage['min_date'] else None,
                    "max_date": str(coverage['max_date']) if coverage['max_date'] else None,
                    "total_days": coverage['total_days'],
                    "total_products": coverage['total_products']
                }
                
                # Add helpful info
                if status["status"] == "healthy":
                    status["message"] = "All systems operational. Data is up-to-date."
                elif status["status"] == "degraded":
                    status["message"] = "Data is available but may be outdated. Consider running refresh."
                else:
                    status["message"] = "Critical data issues detected. Immediate action required."
                
    except Exception as e:
        logger.error(f"Error checking data status: {e}")
        status["status"] = "error"
        status["error"] = str(e)
    
    return status


@router.get("/scenarios")
async def list_dss_scenarios():
    """
    List available DSS scenarios
    
    Returns information about each scenario including:
    - Name and description
    - Required inputs
    - Output format
    - Use cases
    """
    
    return {
        "scenarios": [
            {
                "code": "price_prediction",
                "name": "Price Prediction & Optimization",
                "description": "Analyze pricing opportunities and estimate revenue impact",
                "endpoint": "/api/v1/dss/price/run",
                "use_cases": [
                    "Identify products with pricing opportunities",
                    "Estimate revenue impact of price changes",
                    "Get AI recommendations for pricing strategy"
                ],
                "required_inputs": ["from_date", "to_date"],
                "optional_inputs": ["platforms", "categories", "top_n", "max_discount_pct"]
            },
            {
                "code": "product_recommendation",
                "name": "Product Recommendation & Cross-sell",
                "description": "Find cross-sell/upsell opportunities based on product relationships",
                "endpoint": "/api/v1/dss/reco/run",
                "use_cases": [
                    "Find products frequently bought together",
                    "Identify bundle opportunities",
                    "Optimize product recommendations"
                ],
                "required_inputs": ["from_date", "to_date", "scope_mode"],
                "optional_inputs": ["platforms", "categories", "source_product_key", "top_k"]
            },
            {
                "code": "review_sentiment",
                "name": "Review Sentiment Analysis",
                "description": "Analyze customer sentiment and identify quality issues",
                "endpoint": "/api/v1/dss/review/run",
                "use_cases": [
                    "Identify products with negative sentiment",
                    "Understand customer complaints",
                    "Get recommendations for quality improvement"
                ],
                "required_inputs": ["from_date", "to_date"],
                "optional_inputs": ["platforms", "categories", "min_reviews_per_product", "sentiment_focus"]
            }
        ]
    }

