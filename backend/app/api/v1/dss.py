"""
DSS (Decision Support System) API Router
AI-powered decision support endpoints for analysts
"""

import logging
from typing import Dict, Any, Optional

from fastapi import APIRouter, Depends, HTTPException
import asyncpg
from app.db_pool import get_pool
from app.db_config import DATABASE_URL  # Needed for health check endpoints

from app.schemas.dss import (
    PricePredictionRequest,
    PricePredictionResponse,
    ProductRecommendationRequest,
    ProductRecommendationResponse,
    ReviewSentimentRequest,
    ReviewSentimentResponse,
)
from app.schemas.dss_decision import (
    SaveDSSDecisionRequest,
    DSSDecisionListResponse,
    DSSDecisionDetailResponse,
)
from app.services.dss_service import DSSService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/dss", tags=["DSS - Decision Support System"])


async def get_db_connection():
    """
    Get a database connection from the pool for each DSS request.
    Uses connection pooling for better performance.
    """
    try:
        pool = await get_pool()
        logger.debug("Pool retrieved successfully from get_pool()")
    except RuntimeError as e:
        logger.warning(f"Pool not initialized, attempting fallback initialization... Error: {e}")
        # Fallback: try to initialize pool if not initialized
        from app.db_config import DATABASE_URL
        from app.db_pool import init_pool
        try:
            logger.info("Starting fallback pool initialization...")
            
            # Determine SSL requirements
            import os
            ssl_mode = None
            if os.getenv("RENDER") or "render.com" in DATABASE_URL:
                ssl_mode = "require"
                logger.info("🔒 SSL enabled for fallback database connection (Render environment detected)")
            
            await init_pool(DATABASE_URL, min_size=1, max_size=5, ssl=ssl_mode)
            pool = await get_pool()
            if pool is None:
                raise Exception("Pool is still None after initialization")
            logger.info("Fallback pool initialization successful")
        except Exception as init_e:
            logger.error(f"Fallback pool initialization failed: {init_e}")
            raise HTTPException(
                status_code=500,
                detail=f"Database connection failed: {str(init_e)}"
            )

    async with pool.acquire() as connection:
        try:
            yield connection
        except RuntimeError as e:
            if "Database connection pool not initialized" in str(e):
                raise HTTPException(
                    status_code=500,
                    detail="Database connection pool not initialized. Server startup issue."
                )
            raise
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Database connection error: {str(e)}"
            )


async def get_dss_service(db=Depends(get_db_connection)) -> DSSService:
    """Get DSS service instance with pooled connection"""
    return DSSService(db)


# ============================================
# PRICE PREDICTION DSS
# ============================================


@router.post(
    "/price/run",
    response_model=Dict[str, Any],
    operation_id="run_price_prediction_dss_v1",
)
async def run_price_prediction_dss(
    request: PricePredictionRequest,
    user_id: int = 1,  # TODO: Get from authentication middleware
    service: DSSService = Depends(get_dss_service),
):
    """
    Run Price Prediction DSS Analysis

    **Purpose:**
    Analyze product price optimization opportunities and revenue impact.

    **Workflow:**
    1. Query fact_product_daily + ml.fact_price_prediction
    2. Calculate KPIs (revenue uplift, confidence…)
    3. AI-powered summary & recommended actions
    4. Create analysis session for decision linking

    **Example Request:**
    ```json
    {
      "from_date": "2025-11-01",
      "to_date": "2025-11-20",
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
    
    **Response includes `session_id`** that can be used when saving decisions via `POST /dss/decisions`.
    """
    try:
        result = await service.run_price_prediction_dss(request.dict(), user_id=user_id)
        return result
    except Exception as e:
        logger.exception(f"Error in run_price_prediction_dss: {e}")
        raise HTTPException(status_code=500, detail="Internal server error in DSS")


# ============================================
# PRODUCT RECOMMENDATION DSS
# ============================================


@router.post(
    "/reco/run",
    response_model=Dict[str, Any],
    operation_id="run_product_recommendation_dss_v1",
)
async def run_product_recommendation_dss(
    request: ProductRecommendationRequest,
    user_id: int = 1,  # TODO: Get from authentication middleware
    service: DSSService = Depends(get_dss_service),
):
    """
    Run Product Recommendation DSS Analysis

    **Purpose:**
    Analyze product relationships and generate AI-powered cross-sell/upsell recommendations.

    **Scope Modes:**
    - `by_product`: given a `source_product_key`, trả về danh sách sản phẩm gợi ý.
    - `by_category`: top recommended products trong category/platform.

    **Example Request (by_product):**
    ```json
    {
      "scope_mode": "by_product",
      "source_product_key": "tiki_123456",
      "top_k": 10,
      "min_similarity": 0.5
    }
    ```

    **Example Request (by_category):**
    ```json
    {
      "scope_mode": "by_category",
      "platforms": ["tiki"],
      "categories": ["123"],
      "top_k": 50,
      "min_similarity": 0.5
    }
    ```
    
    **Response includes `session_id`** that can be used when saving decisions via `POST /dss/decisions`.
    """
    try:
        result = await service.run_product_recommendation_dss(request.dict(), user_id=user_id)
        return result
    except Exception as e:
        logger.exception(f"Error in run_product_recommendation_dss: {e}")
        raise HTTPException(status_code=500, detail="Internal server error in DSS")


# ============================================
# REVIEW SENTIMENT DSS
# ============================================


@router.post(
    "/review/run",
    response_model=Dict[str, Any],
    operation_id="run_review_sentiment_dss_v1",
)
async def run_review_sentiment_dss(
    request: ReviewSentimentRequest,
    user_id: int = 1,  # TODO: Get from authentication middleware
    service: DSSService = Depends(get_dss_service),
):
    """
    Run Review Sentiment DSS Analysis

    **Purpose:**
    Analyze customer sentiment and identify products with quality issues or high negative feedback.

    **Example Request:**
    ```json
    {
      "from_date": "2025-11-01",
      "to_date": "2025-11-20",
      "platforms": ["tiki"],
      "categories": ["123"],
      "min_reviews_per_product": 10,
      "negative_threshold": 0.25,
      "sentiment_focus": "only_negative"
    }
    ```
    
    **Response includes `session_id`** that can be used when saving decisions via `POST /dss/decisions`.
    """
    try:
        result = await service.run_review_sentiment_dss(request.dict(), user_id=user_id)
        return result
    except Exception as e:
        logger.exception(f"Error in run_review_sentiment_dss: {e}")
        raise HTTPException(status_code=500, detail="Internal server error in DSS")


@router.get(
    "/review/{product_key}/details",
    response_model=Dict[str, Any],
    operation_id="get_product_review_details_v1",
)
async def get_product_review_details(
    product_key: str,
    sentiment_filter: str = "all",
    sort_by: str = "helpful_votes",
    limit: int = 50,
    service: DSSService = Depends(get_dss_service),
):
    """
    Get detailed reviews for a specific product
    
    **Purpose:**
    Drilldown into individual reviews for a product to understand customer feedback in detail.
    
    **Parameters:**
    - `product_key`: Product identifier (e.g., 'tiki_123456')
    - `sentiment_filter`: Filter by sentiment - 'all', 'positive', 'negative', 'neutral'
    - `sort_by`: Sort reviews by - 'helpful_votes', 'rating', 'date'
    - `limit`: Maximum number of reviews to return (default: 50)
    
    **Example:**
    ```
    GET /dss/review/tiki_123456/details?sentiment_filter=negative&sort_by=helpful_votes&limit=20
    ```
    
    **Response:**
    ```json
    {
      "product_key": "tiki_123456",
      "product_name": "...",
      "total_reviews": 150,
      "sentiment_breakdown": {
        "positive": 90,
        "neutral": 30,
        "negative": 30
      },
      "reviews": [
        {
          "review_id": "...",
          "rating": 1,
          "sentiment_label": "negative",
          "sentiment_score": 0.85,
          "review_title": "Sản phẩm kém chất lượng",
          "review_body": "...",
          "helpful_votes": 45,
          "reviewer_name": "...",
          "review_date": "2025-11-20"
        }
      ]
    }
    ```
    """
    try:
        result = await service.get_product_review_details(
            product_key=product_key,
            sentiment_filter=sentiment_filter,
            sort_by=sort_by,
            limit=limit
        )
        return result
    except Exception as e:
        logger.exception(f"Error in get_product_review_details: {e}")
        raise HTTPException(status_code=500, detail="Internal server error in DSS")


# ============================================
# HEALTH & DATA STATUS
# ============================================


@router.get("/health", operation_id="dss_health_check_v1")
async def dss_health_check():
    """
    Health check for DSS system

    Returns status of DSS components:
    - Database connection
    - AI/LLM availability
    - ML tables accessibility
    """

    health_status: Dict[str, Any] = {
        "status": "healthy",
        "components": {},
    }

    # Determine SSL requirements
    import os
    ssl_mode = None
    if os.getenv("RENDER") or "render.com" in DATABASE_URL:
        ssl_mode = "require"

    # Check database
    try:
        conn = await asyncpg.connect(dsn=DATABASE_URL, ssl=ssl_mode)
        await conn.fetchval("SELECT 1")
        await conn.close()
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
            "model": ai_summarizer.model,
        }
    else:
        health_status["components"]["ai"] = {
            "status": "degraded",
            "mode": "rule-based fallback",
        }

    # Check ML tables
    try:
        conn = await asyncpg.connect(dsn=DATABASE_URL, ssl=ssl_mode)
        tables = await conn.fetch(
            """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'ml'
            """
        )
        await conn.close()
        health_status["components"]["ml_tables"] = {
            "status": "healthy",
            "count": len(tables),
        }
    except Exception as e:
        health_status["components"]["ml_tables"] = f"unhealthy: {str(e)}"
        health_status["status"] = "degraded"

    return health_status


@router.get("/data/status", operation_id="get_data_status_v1")
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

    from datetime import date

    today = date.today()
    status: Dict[str, Any] = {
        "status": "ok",
        "latest_fact_date": None,
        "latest_ml_date": None,
        "days_since_last_fact": None,
        "days_since_last_ml": None,
        "warnings": [],
        "recommendations": [],
    }

    try:
        # Determine SSL requirements
        import os
        ssl_mode = None
        if os.getenv("RENDER") or "render.com" in DATABASE_URL:
            ssl_mode = "require"

        async with asyncpg.create_pool(
            dsn=DATABASE_URL, min_size=1, max_size=1, ssl=ssl_mode
        ) as pool:
            async with pool.acquire() as conn:
                # Check latest fact_product_daily
                latest_fact = await conn.fetchval(
                    """
                    SELECT MAX(dd.date_value)
                    FROM dwh.fact_product_daily f
                    JOIN dwh.dim_date dd ON f.date_sk = dd.date_sk
                    """
                )

                # Check latest ML predictions
                latest_pred = await conn.fetchval(
                    """
                    SELECT MAX(dd.date_value)
                    FROM ml.fact_price_prediction pred
                    JOIN dwh.dim_date dd ON pred.date_sk = dd.date_sk
                    """
                )

        status["latest_fact_date"] = (
            latest_fact.isoformat() if latest_fact else None
        )
        status["latest_ml_date"] = latest_pred.isoformat() if latest_pred else None

        if latest_fact:
            status["days_since_last_fact"] = (today - latest_fact).days
        if latest_pred:
            status["days_since_last_ml"] = (today - latest_pred).days

        # Simple warning logic
        if status["days_since_last_fact"] is not None and status[
            "days_since_last_fact"
        ] > 2:
            status["warnings"].append(
                "Fact data is older than 2 days. Consider rerunning ELT pipeline."
            )

        if status["days_since_last_ml"] is not None and status[
            "days_since_last_ml"
        ] > 7:
            status["warnings"].append(
                "ML predictions are older than 7 days. Consider retraining/re-running ML models."
            )

        if status["warnings"]:
            status["status"] = "warning"
            status["recommendations"].append(
                "Check Airflow / ELT jobs and ML pipelines. At least one component is stale."
            )
        else:
            status["recommendations"].append(
                "Data and ML predictions are fresh. No immediate action required."
            )

    except Exception as e:
        logger.error(f"Error checking data status: {e}")
        status["status"] = "error"
        status["error"] = str(e)

    return status


@router.get("/scenarios", operation_id="list_dss_scenarios_v1")
async def list_dss_scenarios():
    """
    List available DSS scenarios

    Returns information about each scenario including:
    - Scenario key
    - Description
    - API endpoint
    - Required & optional inputs
    """

    return {
        "scenarios": [
            {
                "key": "price_prediction",
                "name": "Price Optimization & Revenue Impact",
                "description": "Optimize product prices and simulate revenue impact",
                "endpoint": "/api/v1/dss/price/run",
                "use_cases": [
                    "Identify products with sub-optimal prices",
                    "Simulate revenue impact of price changes",
                    "Prioritize high-confidence recommendations",
                ],
                "required_inputs": ["from_date", "to_date"],
                "optional_inputs": [
                    "platforms",
                    "categories",
                    "min_confidence",
                    "min_price_change_pct",
                ],
            },
            {
                "key": "product_recommendation",
                "name": "Cross-sell / Upsell Recommendations",
                "description": "Suggest related products for cross-sell and upsell",
                "endpoint": "/api/v1/dss/reco/run",
                "use_cases": [
                    "Recommend related products on PDP",
                    "Find cross-sell bundles",
                    "Identify high-value complementary products",
                ],
                "required_inputs": [],
                "optional_inputs": [
                    "scope_mode",
                    "source_product_key",
                    "platforms",
                    "categories",
                    "top_k",
                    "min_similarity",
                ],
            },
            {
                "key": "review_sentiment",
                "name": "Review Sentiment Analysis",
                "description": "Analyze customer sentiment and identify quality issues",
                "endpoint": "/api/v1/dss/review/run",
                "use_cases": [
                    "Identify products with negative sentiment",
                    "Understand customer complaints",
                    "Get recommendations for quality improvement",
                ],
                "required_inputs": ["from_date", "to_date"],
                "optional_inputs": [
                    "platforms",
                    "categories",
                    "min_reviews_per_product",
                    "sentiment_focus",
                ],
            },
        ]
    }


# ============================================
# DECISION & ACTION MANAGEMENT
# ============================================


@router.post(
    "/decisions",
    response_model=Dict[str, Any],
    operation_id="save_dss_decision_v1",
)
async def save_dss_decision(
    request: SaveDSSDecisionRequest,
    user_id: int = 1,  # TODO: Get from authentication middleware
    service: DSSService = Depends(get_dss_service),
):
    """
    Save a DSS Decision with Action Plan
    
    **Purpose:**
    Save analyst decisions and action plans based on DSS analysis results.
    
    **Workflow:**
    1. Create or link to existing analysis session
    2. Save decision details (title, description, status)
    3. Save action items (price changes, campaigns, etc.)
    4. Log activity
    
    **Example Request:**
    ```json
    {
      "scenario_key": "price_prediction",
      "session_id": null,
      "filters": {
        "from_date": "2025-11-23",
        "to_date": "2025-11-24",
        "platforms": ["tiki"]
      },
      "kpi_summary": {
        "num_products": 150,
        "current_total_revenue": 50000000
      },
      "ai_summary_insights": ["Insight 1", "Insight 2"],
      "ai_recommended_actions": ["Action 1", "Action 2"],
      "title": "Tăng giá 2% cho nhóm máy in Tiki",
      "description": "Dựa trên phân tích ML",
      "status": "DRAFT",
      "actions": [
        {
          "action_type": "change_price",
          "target_level": "product",
          "product_sk": 12345,
          "current_value": 100000,
          "recommended_value": 102000,
          "chosen_value": 102000,
          "unit": "VND",
          "status": "PLANNED"
        }
      ]
    }
    ```
    
    **Response:**
    Returns full decision detail including generated IDs and timestamps.
    """
    try:
        result = await service.save_decision(user_id, request.dict())
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Error saving decision: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/decisions",
    response_model=DSSDecisionListResponse,
    operation_id="list_dss_decisions_v1",
)
async def list_dss_decisions(
    scenario_key: Optional[str] = None,
    status: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    page: int = 1,
    page_size: int = 10,
    service: DSSService = Depends(get_dss_service),
):
    """
    List DSS Decisions with Filters
    
    **Purpose:**
    Retrieve paginated list of decisions with optional filters.
    
    **Query Parameters:**
    - `scenario_key`: Filter by scenario (price_prediction, product_recommendation, review_sentiment)
    - `status`: Filter by status (DRAFT, APPROVED, REJECTED, IMPLEMENTED)
    - `from_date`: Filter by created_at >= from_date (ISO format)
    - `to_date`: Filter by created_at <= to_date (ISO format)
    - `page`: Page number (default: 1)
    - `page_size`: Items per page (default: 10)
    
    **Example:**
    ```
    GET /dss/decisions?scenario_key=price_prediction&status=DRAFT&page=1&page_size=10
    ```
    
    **Response:**
    ```json
    {
      "total": 25,
      "page": 1,
      "page_size": 10,
      "items": [
        {
          "decision_id": 1,
          "scenario_key": "price_prediction",
          "title": "Tăng giá 2% cho nhóm máy in",
          "status": "DRAFT",
          "created_by": 3,
          "created_by_email": "analyst@example.com",
          "created_at": "2025-11-30T14:30:00",
          "num_actions": 5
        }
      ]
    }
    ```
    """
    try:
        result = await service.list_decisions(
            scenario_key=scenario_key,
            status=status,
            from_date=from_date,
            to_date=to_date,
            page=page,
            page_size=page_size
        )
        return result
    except Exception as e:
        logger.exception(f"Error listing decisions: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/decisions/{decision_id}",
    response_model=DSSDecisionDetailResponse,
    operation_id="get_dss_decision_detail_v1",
)
async def get_dss_decision_detail(
    decision_id: int,
    service: DSSService = Depends(get_dss_service),
):
    """
    Get DSS Decision Detail
    
    **Purpose:**
    Retrieve full details of a specific decision including:
    - Decision metadata
    - Analysis session snapshot
    - All action items with enriched data (product names, category names, etc.)
    
    **Example:**
    ```
    GET /dss/decisions/1
    ```
    
    **Response:**
    ```json
    {
      "decision_id": 1,
      "session_id": 10,
      "scenario_key": "price_prediction",
      "title": "Tăng giá 2% cho nhóm máy in Tiki",
      "description": "Dựa trên phân tích ML",
      "status": "DRAFT",
      "created_by": 3,
      "created_by_email": "analyst@example.com",
      "created_at": "2025-11-30T14:30:00",
      "updated_at": "2025-11-30T14:30:00",
      "filters": {...},
      "kpi_summary": {...},
      "ai_summary_insights": [...],
      "ai_recommended_actions": [...],
      "actions": [
        {
          "action_id": 1,
          "action_type": "change_price",
          "target_level": "product",
          "product_sk": 12345,
          "product_name": "Máy in HP LaserJet",
          "current_value": 100000,
          "recommended_value": 102000,
          "chosen_value": 102000,
          "unit": "VND",
          "status": "PLANNED",
          "category_name": "Máy in",
          "platform_name": "Tiki"
        }
      ]
    }
    ```
    """
    try:
        result = await service.get_decision_detail(decision_id)
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception(f"Error getting decision detail: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
