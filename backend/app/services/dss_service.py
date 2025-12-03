"""
DSS Service - Business logic for Decision Support System
Queries data from Postgres (fact & ML tables) and structures results
"""

import logging
import json
import asyncio
from datetime import date, datetime
from typing import Dict, Any, List, Optional
import asyncpg

from app.services.ai_summarizer import get_ai_summarizer
from app.services.activity_logger import ACTIVITY_LOG_TABLE

logger = logging.getLogger(__name__)


class DSSService:
    """Service for DSS operations."""

    def __init__(self, db: asyncpg.Connection):
        self.db = db
        # AI summarizer for DSS insights
        self.ai_summarizer = get_ai_summarizer()

    # ============================================
    # HELPER METHODS
    # ============================================

    def _convert_decimals_to_float(self, obj):
        """
        Recursively convert Decimal objects to float for JSON serialization.
        PostgreSQL numeric fields return Decimal which json.dumps() can't handle.
        """
        from decimal import Decimal
        
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, dict):
            return {k: self._convert_decimals_to_float(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_decimals_to_float(item) for item in obj]
        else:
            return obj

    async def _get_latest_available_date(self, target_date: date) -> Optional[date]:
        """
        Get the latest available date in fact_product_daily that is <= target_date.
        Used to auto-fallback when user chọn to_date nhưng chưa có dữ liệu.
        """
        try:
            row = await self.db.fetchrow(
                """
                SELECT MAX(dd.date_value) AS latest_date
                FROM dwh.fact_product_daily f
                JOIN dwh.dim_date dd ON f.date_sk = dd.date_sk
                WHERE dd.date_value <= $1
                """,
                target_date,
            )
            return row["latest_date"] if row and row["latest_date"] else None
        except Exception as e:
            logger.error(f"Error checking date availability: {e}")
            return None

    # ============================================
    # PRICE PREDICTION DSS
    # ============================================

    async def run_price_prediction_dss(
        self, 
        request: Dict[str, Any],
        user_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Run Price Prediction DSS analysis.
        Now creates an analysis session and returns session_id for decision linking.
        """
        logger.info(f"Running Price Prediction DSS: {request}")

        # 1. Query price predictions + fact data
        data = await self._query_price_predictions(request)

        # 2. Calculate KPIs
        kpi_summary = self._calculate_price_kpis(data, request)

        # 3. Build DSS_RESULT_RAW (input cho AI summarizer)
        dss_result_raw = {
            "scenario": "price_prediction",
            "filters": {
                "from_date": str(request.get("from_date")),
                "to_date": str(request.get("to_date")),
                "platforms": request.get("platforms", []),
                "categories": request.get("categories", []),
            },
            "kpi_summary": kpi_summary,
            "table_data": data["items"],
            "date_adjustment_info": {
                "requested_from_date": str(request.get("from_date")),
                "requested_to_date": str(request.get("to_date")),
                "actual_from_date": str(data.get("actual_from_date")),
                "actual_to_date": str(data.get("actual_to_date")),
                "date_adjusted": data.get("date_adjusted", False),
            },
        }

        # 4. Generate AI insights
        ai_result = self.ai_summarizer.summarize_with_ai("price_prediction", dss_result_raw)
        
        ai_summary_insights = ai_result.get("summary_insights", [])
        ai_recommended_actions = ai_result.get("recommended_actions", [])

        # 5. Create analysis session for decision linking
        session_id = None
        if user_id:
            try:
                session_row = await self.db.fetchrow(
                    """
                    INSERT INTO dss.dss_analysis_session (
                        scenario_key, user_id, filters_json, kpi_summary_json,
                        ai_summary_insights, ai_recommended_actions, date_adjustment_info,
                        generated_at, source_endpoint
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), $8)
                    RETURNING session_id
                    """,
                    "price_prediction",
                    user_id,
                    json.dumps(dss_result_raw["filters"]),
                    json.dumps(self._convert_decimals_to_float(kpi_summary)),
                    json.dumps(ai_summary_insights),
                    json.dumps(ai_recommended_actions),
                    json.dumps(dss_result_raw["date_adjustment_info"]),
                    "/dss/price/run"
                )
                session_id = session_row["session_id"]
                logger.info(f"Created analysis session {session_id} for price prediction")
            except Exception as e:
                logger.warning(f"Failed to create analysis session: {e}")
                # Continue without session_id - не critical

        # 6. Return with session_id
        return {
            **dss_result_raw,
            "items": data["items"],
            "total_count": data["total_count"],
            "ai_summary_insights": ai_summary_insights,
            "ai_recommended_actions": ai_recommended_actions,
            "generated_at": datetime.now().isoformat(),
            "ai_model_used": self.ai_summarizer.model
            if self.ai_summarizer.available
            else "rule-based-fallback",
            "session_id": session_id,  # NEW: Session ID for decision linking
        }

    async def _query_price_predictions(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """
        Query price predictions from database (OPTIMIZED with dual-mode support)
        
        Supports two modes:
        - by_category: Filter by platforms/categories, get top N products
        - by_product: Get predictions for specific product keys
        """
        scope_mode = request.get("scope_mode", "by_category")
        
        # Route to appropriate method based on scope_mode
        if scope_mode == "by_product":
            return await self._query_price_by_product_keys(request)
        else:  # by_category (default)
            return await self._query_price_by_category(request)
    
    async def _query_price_by_category(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """
        Query price predictions BY_CATEGORY mode
        
        Filter by platforms + categories + ML rules → sort by revenue_uplift DESC → get top N
        
        Optimizations:
        - Window function instead of DISTINCT ON
        - Parameterized queries for all filters
        - Pagination support
        - Early filtering on created_at
        - Better error handling
        """

        from_date = request.get("from_date")
        to_date = request.get("to_date")

        # Auto-fallback to latest available date if requested dates have no data
        if isinstance(from_date, str):
            from_date = datetime.strptime(from_date, "%Y-%m-%d").date()
        if isinstance(to_date, str):
            to_date = datetime.strptime(to_date, "%Y-%m-%d").date()

        # Check and adjust dates
        adjusted_to_date = await self._get_latest_available_date(to_date)
        if adjusted_to_date is None:
            logger.error("No data available in database")
            return {
                "items": [],
                "date_adjusted": False,
                "actual_from_date": from_date,
                "actual_to_date": to_date,
                "total_count": 0,
            }

        # If to_date was adjusted, also adjust from_date if needed
        if adjusted_to_date != to_date and from_date:
            from_date = min(from_date, adjusted_to_date)

        platforms = request.get("platforms")
        categories = request.get("categories")
        min_confidence = request.get("min_confidence", 0.70)
        min_price_change_pct = request.get("min_price_change_pct", 0.02)

        # Pagination
        page = request.get("page", 1)
        page_size = request.get("page_size", 50)
        offset = (page - 1) * page_size

        # Build parameterized query
        params: List[Any] = [from_date, adjusted_to_date]
        param_idx = 3
        conditions: List[str] = []

        if platforms:
            conditions.append(f"dpl.platform_code = ANY(${param_idx})")
            params.append(platforms)
            param_idx += 1

        if categories:
            conditions.append(f"CAST(dc.category_sk AS TEXT) = ANY(${param_idx})")
            params.append(categories)
            param_idx += 1

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        # Add min_confidence and min_price_change_pct as parameters
        confidence_param = f"${param_idx}"
        params.append(min_confidence)
        param_idx += 1

        price_change_param = f"${param_idx}"
        params.append(min_price_change_pct)
        param_idx += 1

        limit_param = f"${param_idx}"
        params.append(page_size)
        param_idx += 1

        offset_param = f"${param_idx}"
        params.append(offset)
        param_idx += 1

        # OPTIMIZED QUERY with window function
        sql = f"""
            WITH ranked_predictions AS (
                -- Use window function instead of DISTINCT ON for better performance
                SELECT 
                    pred.product_sk,
                    pred.platform_sk,
                    pred.predicted_price,
                    pred.ci_upper,
                    pred.ci_lower,
                    pred.created_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY pred.product_sk, pred.platform_sk
                        ORDER BY pred.created_at DESC
                    ) AS rn
                FROM ml.fact_price_prediction pred
            ),
            latest_predictions AS (
                SELECT
                    product_sk,
                    platform_sk,
                    predicted_price,
                    ci_upper,
                    ci_lower,
                    created_at,
                    -- Pre-calculate confidence to avoid repeated computation
                    GREATEST(0.0, LEAST(1.0, 
                        1.0 - (ci_upper - ci_lower) / NULLIF(predicted_price, 0)
                    )) AS confidence
                FROM ranked_predictions
                WHERE rn = 1
            ),
            product_metrics AS (
                SELECT
                    f.product_sk,
                    f.platform_sk,
                    AVG(f.avg_price) AS current_price,
                    SUM(f.total_review_count) AS total_reviews,
                    AVG(f.avg_rating) AS avg_rating
                FROM dwh.fact_product_daily f
                JOIN dwh.dim_date dd ON f.date_sk = dd.date_sk
                WHERE dd.date_value BETWEEN $1 AND $2
                GROUP BY f.product_sk, f.platform_sk
            ),
            filtered_results AS (
                SELECT
                    dp.product_key,
                    dp.product_name,
                    dpl.platform_code AS platform,
                    -- Show only the most specific category level
                    COALESCE(
                        dc.category_lvl2,
                        dc.category_lvl1,
                        'Uncategorized'
                    ) AS category_name,
                    COALESCE(pm.current_price, 0) AS current_price,
                    pred.predicted_price,
                    (pred.predicted_price - COALESCE(pm.current_price, 0)) AS price_diff,
                    CASE 
                        WHEN COALESCE(pm.current_price, 0) = 0 THEN 0
                        ELSE (pred.predicted_price / pm.current_price - 1)
                    END AS price_change_pct,
                    
                    -- CALCULATE ORDERS & REVENUE WITH RANDOMNESS
                    -- 1. Calculate Mock Orders
                    CASE 
                        WHEN COALESCE(pm.total_reviews, 0) > 0 THEN
                            -- Has reviews: 1 review ≈ 75 sales
                            CAST(pm.total_reviews * 75 AS INT)
                        ELSE
                            -- No reviews: Mock based on price + randomness
                            -- Base: <100k=300, <500k=150, <2M=50, >2M=20
                            -- Random: +/- 30% using hash of product_key
                            CAST(
                                (CASE 
                                    WHEN COALESCE(pm.current_price, 0) < 100000 THEN 300
                                    WHEN COALESCE(pm.current_price, 0) < 500000 THEN 150
                                    WHEN COALESCE(pm.current_price, 0) < 2000000 THEN 50
                                    ELSE 20
                                END) * 
                                (1.0 + ((ABS(hashtext(dp.product_key)) % 61) - 30) / 100.0)
                            AS INT)
                    END AS current_orders,

                    -- 2. Calculate Revenue from Orders
                    (
                        CASE 
                            WHEN COALESCE(pm.total_reviews, 0) > 0 THEN
                                CAST(pm.total_reviews * 75 AS INT)
                            ELSE
                                CAST(
                                    (CASE 
                                        WHEN COALESCE(pm.current_price, 0) < 100000 THEN 300
                                        WHEN COALESCE(pm.current_price, 0) < 500000 THEN 150
                                        WHEN COALESCE(pm.current_price, 0) < 2000000 THEN 50
                                        ELSE 20
                                    END) * 
                                    (1.0 + ((ABS(hashtext(dp.product_key)) % 61) - 30) / 100.0)
                                AS INT)
                        END
                    ) * COALESCE(pm.current_price, 0) AS current_revenue,

                    CASE 
                        WHEN COALESCE(pm.current_price, 0) = 0 THEN 0
                        ELSE 
                            -- Projected Revenue = Current Revenue * (Predicted Price / Current Price)
                            (
                                CASE 
                                    WHEN COALESCE(pm.total_reviews, 0) > 0 THEN
                                        CAST(pm.total_reviews * 75 AS INT)
                                    ELSE
                                        CAST(
                                            (CASE 
                                                WHEN COALESCE(pm.current_price, 0) < 100000 THEN 300
                                                WHEN COALESCE(pm.current_price, 0) < 500000 THEN 150
                                                WHEN COALESCE(pm.current_price, 0) < 2000000 THEN 50
                                                ELSE 20
                                            END) * 
                                            (1.0 + ((ABS(hashtext(dp.product_key)) % 61) - 30) / 100.0)
                                        AS INT)
                                END
                            ) * COALESCE(pm.current_price, 0) * (pred.predicted_price / pm.current_price)
                    END AS projected_revenue,
                    
                    CASE 
                        WHEN COALESCE(pm.current_price, 0) = 0 THEN 0
                        ELSE (pred.predicted_price / pm.current_price - 1)
                    END AS expected_revenue_change_pct,
                    pred.confidence,
                    pm.avg_rating,
                    COALESCE(pm.total_reviews, 0) AS total_reviews,
                    ABS(pred.predicted_price / NULLIF(pm.current_price, 0) - 1)
                        AS abs_revenue_change
                FROM latest_predictions pred
                JOIN dwh.dim_product dp 
                    ON pred.product_sk = dp.product_sk
                JOIN dwh.dim_platform dpl 
                    ON pred.platform_sk = dpl.platform_sk
                LEFT JOIN dwh.dim_category dc 
                    ON dp.category_sk = dc.category_sk
                LEFT JOIN product_metrics pm 
                    ON dp.product_sk = pm.product_sk 
                   AND pred.platform_sk = pm.platform_sk
                WHERE {where_clause}
                  AND COALESCE(pm.current_price, 0) > 0
                  AND pred.confidence >= {confidence_param}
                  AND ABS(
                      (pred.predicted_price - COALESCE(pm.current_price, 0))
                      / NULLIF(pm.current_price, 0)
                  ) > {price_change_param}
            )
            SELECT
                *,
                COUNT(*) OVER() AS total_count
            FROM filtered_results
            ORDER BY abs_revenue_change DESC, confidence DESC
            LIMIT {limit_param} OFFSET {offset_param}
        """

        # COUNT query (without pagination)
        count_sql = f"""
            WITH ranked_predictions AS (
                SELECT 
                    pred.product_sk,
                    pred.platform_sk,
                    pred.predicted_price,
                    pred.ci_upper,
                    pred.ci_lower,
                    pred.created_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY pred.product_sk, pred.platform_sk
                        ORDER BY pred.created_at DESC
                    ) AS rn
                FROM ml.fact_price_prediction pred
            ),
            latest_predictions AS (
                SELECT
                    product_sk,
                    platform_sk,
                    predicted_price,
                    ci_upper,
                    ci_lower,
                    created_at,
                    GREATEST(0.0, LEAST(1.0, 
                        1.0 - (ci_upper - ci_lower) / NULLIF(predicted_price, 0)
                    )) AS confidence
                FROM ranked_predictions
                WHERE rn = 1
            ),
            product_metrics AS (
                SELECT
                    f.product_sk,
                    f.platform_sk,
                    AVG(f.avg_price) AS current_price
                FROM dwh.fact_product_daily f
                JOIN dwh.dim_date dd ON f.date_sk = dd.date_sk
                WHERE dd.date_value BETWEEN $1 AND $2
                GROUP BY f.product_sk, f.platform_sk
            )
            SELECT COUNT(*)
            FROM latest_predictions pred
            JOIN dwh.dim_product dp ON pred.product_sk = dp.product_sk
            JOIN dwh.dim_platform dpl ON pred.platform_sk = dpl.platform_sk
            LEFT JOIN dwh.dim_category dc ON dp.category_sk = dc.category_sk
            LEFT JOIN product_metrics pm 
                ON dp.product_sk = pm.product_sk
               AND pred.platform_sk = pm.platform_sk
            WHERE {where_clause}
              AND COALESCE(pm.current_price, 0) > 0
              AND pred.confidence >= {confidence_param}
              AND ABS(
                  (pred.predicted_price - COALESCE(pm.current_price, 0))
                  / NULLIF(pm.current_price, 0)
              ) > {price_change_param}
        """

        # Params for count_sql = params without LIMIT/OFFSET
        count_params = params[:-2]

        try:
            rows = await self.db.fetch(sql, *params)
            total_count_row = await self.db.fetchrow(count_sql, *count_params)
            total_count = total_count_row["count"] if total_count_row else 0

            items = []
            for row in rows:
                items.append(
                    {
                        "product_key": row["product_key"],
                        "product_name": row["product_name"],
                        "platform": row["platform"],
                        "category_name": row["category_name"],
                        "current_price": float(row["current_price"]),

                        # *** CHÍNH Ở ĐÂY ***
                        "recommended_price": float(row["predicted_price"]),
                        "predicted_price": float(row["predicted_price"]),  # giữ lại nếu FE đang dùng
                        "price_diff": float(row["price_diff"]),
                        "price_change_pct": float(row["price_change_pct"]),
                        "current_revenue": float(row["current_revenue"]),
                        "projected_revenue": float(row["projected_revenue"]),
                        "expected_revenue_change_pct": float(
                            row["expected_revenue_change_pct"]
                        ),
                        "confidence": float(row["confidence"]),
                        "current_orders": int(row["current_orders"]),
                        # Mock avg_rating when null: deterministic hash-based value (3.5-4.5 range)
                        "avg_rating": float(row["avg_rating"])
                        if row["avg_rating"] is not None
                        else round(3.5 + (abs(hash(row["product_key"])) % 100) / 100, 1),
                        "total_reviews": int(row["total_reviews"]),
                    }
                )

            return {
                "items": items,
                "total_count": total_count,
                "date_adjusted": adjusted_to_date != to_date,
                "actual_from_date": from_date,
                "actual_to_date": adjusted_to_date,
            }
        except Exception as e:
            logger.exception(f"Error querying price predictions: {e}")
            return {
                "items": [],
                "total_count": 0,
                "date_adjusted": False,
                "actual_from_date": from_date,
                "actual_to_date": to_date,
            }

    async def _query_price_by_product_keys(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """
        Query price predictions BY_PRODUCT mode
        
        Get predictions for specific product keys - returns all records for those products
       (không dùng top_n, mà trả về hết dữ liệu trong date range)
        """
        from_date = request.get("from_date")
        to_date = request.get("to_date")
        product_keys = request.get("product_keys", [])
        
        if not product_keys:
            logger.error("product_keys is empty for BY_PRODUCT mode")
            return {
                "items": [],
                "total_count": 0,
                "date_adjusted": False,
                "actual_from_date": from_date,
                "actual_to_date": to_date,
            }
        
        # Convert dates if needed
        if isinstance(from_date, str):
            from_date = datetime.strptime(from_date, "%Y-%m-%d").date()
        if isinstance(to_date, str):
            to_date = datetime.strptime(to_date, "%Y-%m-%d").date()
        
        # Check and adjust dates
        adjusted_to_date = await self._get_latest_available_date(to_date)
        if adjusted_to_date is None:
            logger.error("No data available in database")
            return {
                "items": [],
                "total_count": 0,
                "date_adjusted": False,
                "actual_from_date": from_date,
                "actual_to_date": to_date,
            }
        
        if adjusted_to_date != to_date and from_date:
            from_date = min(from_date, adjusted_to_date)
        
        min_confidence = request.get("min_confidence", 0.70)
        min_price_change_pct = request.get("min_price_change_pct", 0.02)
        
        # Build parameterized query
        params: List[Any] = [from_date, adjusted_to_date, product_keys]
        
        # Query for BY_PRODUCT mode - get all records for selected products
        sql = """
            WITH ranked_predictions AS (
                SELECT 
                    pred.product_sk,
                    pred.platform_sk,
                    pred.predicted_price,
                    pred.ci_upper,
                    pred.ci_lower,
                    pred.created_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY pred.product_sk, pred.platform_sk
                        ORDER BY pred.created_at DESC
                    ) AS rn
                FROM ml.fact_price_prediction pred
            ),
            latest_predictions AS (
                SELECT
                    product_sk,
                    platform_sk,
                    predicted_price,
                    ci_upper,
                    ci_lower,
                    created_at,
                    GREATEST(0.0, LEAST(1.0, 
                        1.0 - (ci_upper - ci_lower) / NULLIF(predicted_price, 0)
                    )) AS confidence
                FROM ranked_predictions
                WHERE rn = 1
            ),
            product_metrics AS (
                SELECT
                    f.product_sk,
                    f.platform_sk,
                    AVG(f.avg_price) AS current_price,
                    SUM(f.total_review_count) AS total_reviews,
                    AVG(f.avg_rating) AS avg_rating
                FROM dwh.fact_product_daily f
                JOIN dwh.dim_date dd ON f.date_sk = dd.date_sk
                WHERE dd.date_value BETWEEN $1 AND $2
                GROUP BY f.product_sk, f.platform_sk
            )
            SELECT
                dp.product_key,
                dp.product_name,
                dpl.platform_code AS platform,
                COALESCE(
                    dc.category_lvl2,
                    dc.category_lvl1,
                    'Uncategorized'
                ) AS category_name,
                COALESCE(pm.current_price, 0) AS current_price,
                pred.predicted_price,
                (pred.predicted_price - COALESCE(pm.current_price, 0)) AS price_diff,
                CASE 
                    WHEN COALESCE(pm.current_price, 0) = 0 THEN 0
                    ELSE (pred.predicted_price / pm.current_price - 1)
                END AS price_change_pct,
                
                -- Calculate Orders (same logic as by_category)
                CASE 
                    WHEN COALESCE(pm.total_reviews, 0) > 0 THEN
                        CAST(pm.total_reviews * 75 AS INT)
                    ELSE
                        CAST(
                            (CASE 
                                WHEN COALESCE(pm.current_price, 0) < 100000 THEN 300
                                WHEN COALESCE(pm.current_price, 0) < 500000 THEN 150
                                WHEN COALESCE(pm.current_price, 0) < 2000000 THEN 50
                                ELSE 20
                            END) * 
                            (1.0 + ((ABS(hashtext(dp.product_key)) % 61) - 30) / 100.0)
                        AS INT)
                END AS current_orders,
                
                (
                    CASE 
                        WHEN COALESCE(pm.total_reviews, 0) > 0 THEN
                            CAST(pm.total_reviews * 75 AS INT)
                        ELSE
                            CAST(
                                (CASE 
                                    WHEN COALESCE(pm.current_price, 0) < 100000 THEN 300
                                    WHEN COALESCE(pm.current_price, 0) < 500000 THEN 150
                                    WHEN COALESCE(pm.current_price, 0) < 2000000 THEN 50
                                    ELSE 20
                                END) * 
                                (1.0 + ((ABS(hashtext(dp.product_key)) % 61) - 30) / 100.0)
                            AS INT)
                    END
                ) * COALESCE(pm.current_price, 0) AS current_revenue,
                
                CASE 
                    WHEN COALESCE(pm.current_price, 0) = 0 THEN 0
                    ELSE 
                        (
                            CASE 
                                WHEN COALESCE(pm.total_reviews, 0) > 0 THEN
                                    CAST(pm.total_reviews * 75 AS INT)
                                ELSE
                                    CAST(
                                        (CASE 
                                            WHEN COALESCE(pm.current_price, 0) < 100000 THEN 300
                                            WHEN COALESCE(pm.current_price, 0) < 500000 THEN 150
                                            WHEN COALESCE(pm.current_price, 0) < 2000000 THEN 50
                                            ELSE 20
                                        END) * 
                                        (1.0 + ((ABS(hashtext(dp.product_key)) % 61) - 30) / 100.0)
                                    AS INT)
                            END
                        ) * COALESCE(pm.current_price, 0) * (pred.predicted_price / pm.current_price)
                END AS projected_revenue,
                
                CASE 
                    WHEN COALESCE(pm.current_price, 0) = 0 THEN 0
                    ELSE (pred.predicted_price / pm.current_price - 1)
                END AS expected_revenue_change_pct,
                pred.confidence,
                pm.avg_rating,
                COALESCE(pm.total_reviews, 0) AS total_reviews
            FROM latest_predictions pred
            JOIN dwh.dim_product dp 
                ON pred.product_sk = dp.product_sk
            JOIN dwh.dim_platform dpl 
                ON pred.platform_sk = dpl.platform_sk
            LEFT JOIN dwh.dim_category dc 
                ON dp.category_sk = dc.category_sk
            LEFT JOIN product_metrics pm 
                ON dp.product_sk = pm.product_sk 
               AND pred.platform_sk = pm.platform_sk
            WHERE dp.product_key = ANY($3::text[])
              AND COALESCE(pm.current_price, 0) > 0
              AND pred.confidence >= $4
              AND ABS(
                  (pred.predicted_price - COALESCE(pm.current_price, 0))
                  / NULLIF(pm.current_price, 0)
              ) > $5
            ORDER BY dp.product_key, pred.confidence DESC
        """
        
        params.extend([min_confidence, min_price_change_pct])
        
        try:
            rows = await self.db.fetch(sql, *params)
            
            items = []
            for row in rows:
                items.append(
                    {
                        "product_key": row["product_key"],
                        "product_name": row["product_name"],
                        "platform": row["platform"],
                        "category_name": row["category_name"],
                        "current_price": float(row["current_price"]),
                        "recommended_price": float(row["predicted_price"]),
                        "predicted_price": float(row["predicted_price"]),
                        "price_diff": float(row["price_diff"]),
                        "price_change_pct": float(row["price_change_pct"]),
                        "current_revenue": float(row["current_revenue"]),
                        "projected_revenue": float(row["projected_revenue"]),
                        "expected_revenue_change_pct": float(
                            row["expected_revenue_change_pct"]
                        ),
                        "confidence": float(row["confidence"]),
                        "current_orders": int(row["current_orders"]),
                        "avg_rating": float(row["avg_rating"])
                        if row["avg_rating"] is not None
                        else round(3.5 + (abs(hash(row["product_key"])) % 100) / 100, 1),
                        "total_reviews": int(row["total_reviews"]),
                    }
                )
            
            return {
                "items": items,
                "total_count": len(items),
                "date_adjusted": adjusted_to_date != to_date,
                "actual_from_date": from_date,
                "actual_to_date": adjusted_to_date,
            }
        except Exception as e:
            logger.exception(f"Error querying price predictions by product keys: {e}")
            return {
                "items": [],
                "total_count": 0,
                "date_adjusted": False,
                "actual_from_date": from_date,
                "actual_to_date": to_date,
            }


    def _calculate_price_kpis(self, data: Dict[str, Any], request: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate KPI summary for price prediction scenario."""
        items = data.get("items", [])
        if not items:
            return {
            "num_products": num_products,
            "num_with_recommendation": num_with_reco,
            "current_revenue": current_revenue,
            "projected_revenue": projected_revenue,
            # alias cho AI + docs
            "current_total_revenue": current_revenue,
            "projected_total_revenue": projected_revenue,
            "expected_revenue_uplift_pct": expected_uplift_pct,
            "avg_confidence": avg_confidence,
    }

        num_products = len(items)
        num_with_reco = len([i for i in items if i["price_change_pct"] != 0])

        current_revenue = sum(i["current_revenue"] for i in items)
        projected_revenue = sum(i["projected_revenue"] for i in items)

        expected_uplift_pct = (
            (projected_revenue / current_revenue - 1) if current_revenue > 0 else 0.0
        )

        avg_confidence = sum(i["confidence"] for i in items) / num_products

        return {
            "num_products": num_products,
            "num_with_recommendation": num_with_reco,
            "current_revenue": current_revenue,
            "projected_revenue": projected_revenue,
            "expected_revenue_uplift_pct": expected_uplift_pct,
            "avg_confidence": avg_confidence,
        }

    # ============================================
    # PRODUCT RECOMMENDATION DSS
    # ============================================

    async def run_product_recommendation_dss(
        self, 
        request: Dict[str, Any],
        user_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """Run Product Recommendation DSS analysis"""

        logger.info(f"Running Product Recommendation DSS: {request}")

        # 1. Query recommendations
        recommendations = await self._query_product_recommendations(request)

        # 2. Calculate KPIs
        kpi_summary = self._calculate_reco_kpis(recommendations, request)

        # 3. Build DSS_RESULT_RAW
        dss_result_raw = {
            "scenario": "product_recommendation",
            "filters": {
                "from_date": str(request.get("from_date")),
                "to_date": str(request.get("to_date")),
                "platforms": request.get("platforms", []),
                "categories": request.get("categories", []),
                "scope_mode": request.get("scope_mode", "by_category"),
            },
            "kpi_summary": kpi_summary,
            "table_data": recommendations,
        }

        # 4. Generate AI insights
        ai_result = self.ai_summarizer.summarize_with_ai(
            "product_recommendation", dss_result_raw
        )
        
        ai_summary_insights = ai_result.get("summary_insights", [])
        ai_recommended_actions = ai_result.get("recommended_actions", [])

        # 5. Create analysis session for decision linking
        session_id = None
        if user_id:
            try:
                session_row = await self.db.fetchrow(
                    """
                    INSERT INTO dss.dss_analysis_session (
                        scenario_key, user_id, filters_json, kpi_summary_json,
                        ai_summary_insights, ai_recommended_actions,
                        generated_at, source_endpoint
                    ) VALUES ($1, $2, $3, $4, $5, $6, NOW(), $7)
                    RETURNING session_id
                    """,
                    "product_recommendation",
                    user_id,
                    json.dumps(dss_result_raw["filters"]),
                    json.dumps(self._convert_decimals_to_float(kpi_summary)),
                    json.dumps(ai_summary_insights),
                    json.dumps(ai_recommended_actions),
                    "/dss/reco/run"
                )
                session_id = session_row["session_id"]
                logger.info(f"Created analysis session {session_id} for product recommendation")
            except Exception as e:
                logger.warning(f"Failed to create analysis session: {e}")

        # 6. Return with session_id
        return {
            **dss_result_raw,
            "ai_summary_insights": ai_summary_insights,
            "ai_recommended_actions": ai_recommended_actions,
            "generated_at": datetime.now().isoformat(),
            "ai_model_used": self.ai_summarizer.model
            if self.ai_summarizer.available
            else "rule-based-fallback",
            "session_id": session_id,  # NEW: Session ID for decision linking
        }

    async def _query_product_recommendations(
        self, request: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Query product recommendations from database"""

        scope_mode = request.get("scope_mode", "by_category")
        top_k = request.get("top_k", 10)
        min_similarity = request.get("min_similarity", 0.5)

        # Different query based on scope_mode
        if scope_mode == "by_product":
            return await self._query_recommendations_by_product(request)
        else:
            return await self._query_recommendations_by_category(request)

    async def _query_recommendations_by_product(
        self, request: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Get recommendations for specific product"""

        source_product_key = request.get("source_product_key")
        if not source_product_key:
            return []

        top_k = request.get("top_k", 10)
        min_similarity = request.get("min_similarity", 0.5)

        sql = """
            WITH product_metrics AS (
                SELECT
                    f.product_sk,
                    AVG(f.avg_price) AS avg_price,
                    SUM(f.total_review_count) AS total_orders
                FROM dwh.fact_product_daily f
                GROUP BY f.product_sk
            )
            SELECT
                dp_src.product_key AS source_product_key,
                dp_src.product_name AS source_product_name,
                dp_rec.product_key AS recommended_product_key,
                dp_rec.product_name AS recommended_product_name,
                SUBSTRING(dp_rec.product_key FROM '^(.*?)_') AS platform,
                COALESCE(dc.category_lvl2, dc.category_lvl1, 'Uncategorized') AS category_name,
                COALESCE(pm_rec.avg_price, 0) AS avg_price,
                COALESCE(pm_rec.total_orders, 0) AS total_orders,
                rec.similarity_score,
                COALESCE(rec.recommendation_type, 'cross_sell') AS recommendation_type
            FROM ml.fact_product_recommendation rec
            JOIN dwh.dim_product dp_src ON rec.source_product_sk = dp_src.product_sk
            JOIN dwh.dim_product dp_rec ON rec.recommended_product_sk = dp_rec.product_sk
            LEFT JOIN dwh.dim_category dc ON dp_rec.category_sk = dc.category_sk
            LEFT JOIN product_metrics pm_rec ON dp_rec.product_sk = pm_rec.product_sk
            WHERE dp_src.product_key = $1
              AND rec.similarity_score >= $2
            ORDER BY rec.similarity_score DESC, rec.rank ASC
            LIMIT $3
        """

        try:
            rows = await self.db.fetch(sql, source_product_key, min_similarity, top_k)
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error querying recommendations by product: {e}")
            return []

    async def _query_recommendations_by_category(
        self, request: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Get top recommendations within category"""

        platforms = request.get("platforms")
        categories = request.get("categories")
        top_k = request.get("top_k", 10)
        min_similarity = request.get("min_similarity", 0.5)

        params: List[Any] = [min_similarity]
        param_idx = 2

        platform_filter = ""
        if platforms:
            platform_filter = (
                f"AND SUBSTRING(dp_rec.product_key FROM '^(.*?)_') = ANY(${param_idx})"
            )
            params.append(platforms)
            param_idx += 1

        category_filter = ""
        if categories:
            category_filter = (
                f"AND CAST(dc.category_sk AS TEXT) = ANY(${param_idx})"
            )
            params.append(categories)
            param_idx += 1

        sql = f"""
            WITH product_metrics AS (
                SELECT
                    f.product_sk,
                    AVG(f.avg_price) AS avg_price,
                    SUM(f.total_review_count) AS total_orders
                FROM dwh.fact_product_daily f
                GROUP BY f.product_sk
            )
            SELECT
                dp_src.product_key AS source_product_key,
                dp_src.product_name AS source_product_name,
                dp_rec.product_key AS recommended_product_key,
                dp_rec.product_name AS recommended_product_name,
                SUBSTRING(dp_rec.product_key FROM '^(.*?)_') AS platform,
                COALESCE(dc.category_lvl2, dc.category_lvl1, 'Uncategorized') AS category_name,
                COALESCE(pm_rec.avg_price, 0) AS avg_price,
                COALESCE(pm_rec.total_orders, 0) AS total_orders,
                rec.similarity_score,
                COALESCE(rec.recommendation_type, 'cross_sell') AS recommendation_type
            FROM ml.fact_product_recommendation rec
            JOIN dwh.dim_product dp_src ON rec.source_product_sk = dp_src.product_sk
            JOIN dwh.dim_product dp_rec ON rec.recommended_product_sk = dp_rec.product_sk
            LEFT JOIN dwh.dim_category dc ON dp_rec.category_sk = dc.category_sk
            LEFT JOIN product_metrics pm_rec ON dp_rec.product_sk = pm_rec.product_sk
            WHERE rec.similarity_score >= $1
              {platform_filter}
              {category_filter}
            ORDER BY pm_rec.total_orders DESC, rec.similarity_score DESC
            LIMIT {top_k}
        """

        try:
            rows = await self.db.fetch(sql, *params)
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error querying recommendations by category: {e}")
            return []

    def _calculate_reco_kpis(
        self, recommendations: List[Dict[str, Any]], request: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Calculate KPI summary for recommendations."""
        if not recommendations:
            return {
                "num_source_products": 0,
                "num_recommendations": 0,
                "avg_similarity": 0.0,
                "avg_orders_for_recommended": 0.0,
            }

        num_recommendations = len(recommendations)

        # Count distinct source products
        source_products = {r["source_product_key"] for r in recommendations}
        num_source_products = len(source_products)

        avg_similarity = sum(r["similarity_score"] for r in recommendations) / num_recommendations
        avg_orders = sum(r["total_orders"] for r in recommendations) / num_recommendations

        return {
            "num_source_products": num_source_products,
            "num_recommendations": num_recommendations,
            "avg_similarity": avg_similarity,
            "avg_orders_for_recommended": avg_orders,
        }

    # ============================================
    # REVIEW SENTIMENT DSS
    # ============================================

    async def run_review_sentiment_dss(
        self, 
        request: Dict[str, Any],
        user_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """Run Review Sentiment Analysis DSS"""

        logger.info(f"Running Review Sentiment DSS: {request}")

        # 1. Query sentiment data
        sentiment_data = await self._query_review_sentiment(request)

        # 2. Calculate KPIs
        kpi_summary = self._calculate_sentiment_kpis(sentiment_data, request)

        # 3. Build DSS_RESULT_RAW
        dss_result_raw = {
            "scenario": "review_sentiment",
            "filters": {
                "from_date": str(request.get("from_date")),
                "to_date": str(request.get("to_date")),
                "platforms": request.get("platforms", []),
                "categories": request.get("categories", []),
            },
            "kpi_summary": kpi_summary,
            "table_data": sentiment_data,
        }

        # 4. Generate AI insights
        ai_result = self.ai_summarizer.summarize_with_ai(
            "review_sentiment", dss_result_raw
        )
        
        ai_summary_insights = ai_result.get("summary_insights", [])
        ai_recommended_actions = ai_result.get("recommended_actions", [])

        # 5. Create analysis session for decision linking
        session_id = None
        if user_id:
            try:
                session_row = await self.db.fetchrow(
                    """
                    INSERT INTO dss.dss_analysis_session (
                        scenario_key, user_id, filters_json, kpi_summary_json,
                        ai_summary_insights, ai_recommended_actions,
                        generated_at, source_endpoint
                    ) VALUES ($1, $2, $3, $4, $5, $6, NOW(), $7)
                    RETURNING session_id
                    """,
                    "review_sentiment",
                    user_id,
                    json.dumps(dss_result_raw["filters"]),
                    json.dumps(self._convert_decimals_to_float(kpi_summary)),
                    json.dumps(ai_summary_insights),
                    json.dumps(ai_recommended_actions),
                    "/dss/review/run"
                )
                session_id = session_row["session_id"]
                logger.info(f"Created analysis session {session_id} for review sentiment")
            except Exception as e:
                logger.warning(f"Failed to create analysis session: {e}")

        # 6. Return with session_id
        return {
            **dss_result_raw,
            "ai_summary_insights": ai_summary_insights,
            "ai_recommended_actions": ai_recommended_actions,
            "generated_at": datetime.now().isoformat(),
            "ai_model_used": self.ai_summarizer.model
            if self.ai_summarizer.available
            else "rule-based-fallback",
            "session_id": session_id,  # NEW: Session ID for decision linking
        }

    async def _query_review_sentiment(
        self, request: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Query review sentiment from database"""

        from_date = request.get("from_date")
        to_date = request.get("to_date")
        platforms = request.get("platforms")
        categories = request.get("categories")
        min_reviews = request.get("min_reviews_per_product", 10)
        negative_threshold = request.get("negative_threshold", 0.25)

        conditions: List[str] = []
        params: List[Any] = []
        param_idx = 1

        if platforms:
            conditions.append(f"dpl.platform_code = ANY(${param_idx})")
            params.append(platforms)
            param_idx += 1

        if categories:
            conditions.append(f"CAST(dc.category_sk AS TEXT) = ANY(${param_idx})")
            params.append(categories)
            param_idx += 1

        # NOTE: from_date / to_date hiện chưa áp dụng vì fact_review không join dim_date ở query này.
        # Nếu bảng fact_review có cột date_sk / review_date, bạn có thể bổ sung thêm filter theo ngày ở đây.

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        sql = f"""
            WITH review_stats AS (
                SELECT
                    r.product_sk,
                    COUNT(*) AS total_reviews,
                    SUM(CASE WHEN sent.sentiment_label = 'positive' THEN 1 ELSE 0 END) AS positive_count,
                    SUM(CASE WHEN sent.sentiment_label = 'neutral' THEN 1 ELSE 0 END) AS neutral_count,
                    SUM(CASE WHEN sent.sentiment_label = 'negative' THEN 1 ELSE 0 END) AS negative_count,
                    AVG(r.rating) AS avg_rating,
                    AVG(
                        CASE 
                            WHEN sent.sentiment_label = 'positive' 
                            THEN sent.sentiment_score 
                            ELSE NULL 
                        END
                    ) AS avg_positive_score,
                    AVG(
                        CASE 
                            WHEN sent.sentiment_label = 'negative' 
                            THEN sent.sentiment_score 
                            ELSE NULL 
                        END
                    ) AS avg_negative_score,
                    -- Collect sample negative reviews (rating 1-2, top 5 by helpful votes)
                    ARRAY_AGG(
                        CASE WHEN r.rating <= 2
                        THEN jsonb_build_object(
                            'review_body', SUBSTRING(r.review_body FROM 1 FOR 200),
                            'rating', r.rating,
                            'helpful_votes', COALESCE(r.helpful_votes, 0)
                        )
                        END ORDER BY COALESCE(r.helpful_votes, 0) DESC
                    ) FILTER (WHERE r.rating <= 2) AS negative_reviews,
                    -- Collect sample positive reviews (rating 4-5, top 5 by helpful votes)
                    ARRAY_AGG(
                        CASE WHEN r.rating >= 4
                        THEN jsonb_build_object(
                            'review_body', SUBSTRING(r.review_body FROM 1 FOR 200),
                            'rating', r.rating,
                            'helpful_votes', COALESCE(r.helpful_votes, 0)
                        )
                        END ORDER BY COALESCE(r.helpful_votes, 0) DESC
                    ) FILTER (WHERE r.rating >= 4) AS positive_reviews
                FROM dwh.fact_review r
                LEFT JOIN ml.fact_review_sentiment sent 
                    ON r.review_sk = sent.review_id
                JOIN dwh.dim_platform dpl 
                    ON r.platform_sk = dpl.platform_sk
                LEFT JOIN dwh.dim_product dp 
                    ON r.product_sk = dp.product_sk
                LEFT JOIN dwh.dim_category dc 
                    ON dp.category_sk = dc.category_sk
                WHERE {where_clause}
                GROUP BY r.product_sk
                HAVING COUNT(*) >= {min_reviews}
            )
            SELECT
                dp.product_key,
                dp.product_name,
                SUBSTRING(dp.product_key FROM '^(.*?)_') AS platform,
                COALESCE(dc.category_lvl2, dc.category_lvl1, 'Uncategorized') AS category_name,
                rs.total_reviews,
                rs.positive_count,
                rs.neutral_count,
                rs.negative_count,
                CAST(rs.positive_count AS FLOAT) / NULLIF(rs.total_reviews, 0) AS positive_pct,
                CAST(rs.neutral_count AS FLOAT) / NULLIF(rs.total_reviews, 0) AS neutral_pct,
                CAST(rs.negative_count AS FLOAT) / NULLIF(rs.total_reviews, 0) AS negative_pct,
                rs.avg_rating,
                rs.avg_positive_score,
                rs.avg_negative_score,
                rs.negative_reviews,
                rs.positive_reviews
            FROM review_stats rs
            JOIN dwh.dim_product dp ON rs.product_sk = dp.product_sk
            LEFT JOIN dwh.dim_category dc ON dp.category_sk = dc.category_sk
            ORDER BY rs.negative_count DESC, rs.total_reviews DESC, dp.product_sk ASC
            LIMIT 100
        """

        try:
            rows = await self.db.fetch(sql, *params)

            results: List[Dict[str, Any]] = []
            for row in rows:
                negative_pct = float(row["negative_pct"]) if row["negative_pct"] else 0.0

                # Extract sample reviews from ARRAY_AGG results
                # Note: asyncpg may return JSONB as dict or string, handle both
                negative_reviews_raw = row.get("negative_reviews") or []
                positive_reviews_raw = row.get("positive_reviews") or []
                
                # Take top 5 most helpful reviews
                sample_negative_reviews = []
                sample_positive_reviews = []
                
                # Process negative reviews
                for review in negative_reviews_raw[:5]:
                    if review:
                        # Parse JSON string if needed
                        if isinstance(review, str):
                            try:
                                review = json.loads(review)
                            except:
                                continue
                        
                        sample_negative_reviews.append({
                            "review_body": review.get("review_body"),
                            "rating": review.get("rating"),
                            "helpful_votes": review.get("helpful_votes", 0)
                        })
                
                # Process positive reviews
                for review in positive_reviews_raw[:5]:
                    if review:
                        # Parse JSON string if needed
                        if isinstance(review, str):
                            try:
                                review = json.loads(review)
                            except:
                                continue
                        
                        sample_positive_reviews.append({
                            "review_body": review.get("review_body"),
                            "rating": review.get("rating"),
                            "helpful_votes": review.get("helpful_votes", 0)
                        })
                
                # Generate reasons from actual review text
                neg_reasons: List[str] = []
                for review in sample_negative_reviews:
                    body = (review.get("review_body") or "").strip()
                    if body:
                        neg_reasons.append(body[:100] + "..." if len(body) > 100 else body)
                
                # If no actual reviews, use generic reasons based on metrics
                if not neg_reasons:
                    if negative_pct > 0.3:
                        neg_reasons.append("Tỷ lệ đánh giá tiêu cực cao")
                    if row.get("avg_rating", 0) and row.get("avg_rating") < 3.5:
                        neg_reasons.append("Điểm rating thấp")
                    avg_negative_score = float(row.get("avg_negative_score") or 0)
                    if avg_negative_score > 0.5:
                        neg_reasons.append("Cảm xúc tiêu cực mạnh")
                
                # Generate positive reasons from actual review text
                pos_reasons: List[str] = []
                for review in sample_positive_reviews:
                    body = (review.get("review_body") or "").strip()
                    pos_reasons.append(body[:100] + "..." if len(body) > 100 else body)
                
                # If no actual reviews, use generic
                if not pos_reasons:
                    pos_reasons.append("Đánh giá tích cực từ khách hàng")

                results.append(
                    {
                        "product_key": row["product_key"],
                        "product_name": row["product_name"],
                        "platform": row["platform"],
                        "category_name": row["category_name"],
                        "total_reviews": int(row["total_reviews"]),
                        "positive_count": int(row["positive_count"])
                        if row["positive_count"]
                        else 0,
                        "neutral_count": int(row["neutral_count"])
                        if row["neutral_count"]
                        else 0,
                        "negative_count": int(row["negative_count"])
                        if row["negative_count"]
                        else 0,
                        "positive_pct": float(row["positive_pct"])
                        if row["positive_pct"]
                        else 0.0,
                        "neutral_pct": float(row["neutral_pct"])
                        if row["neutral_pct"]
                        else 0.0,
                        "negative_pct": negative_pct,
                        "avg_rating": float(row["avg_rating"])
                        if row["avg_rating"]
                        else None,
                        "sample_negative_reviews": sample_negative_reviews,
                        "sample_positive_reviews": sample_positive_reviews,
                        "top_positive_reasons": pos_reasons[:5],  # Limit to 5
                        "top_negative_reasons": neg_reasons[:5] if neg_reasons else ["Không có vấn đề nghiêm trọng"],
                        "is_critical": negative_pct > negative_threshold,
                    }
                )

            # ✅ NEW: Filter theo sentiment_focus ở tầng service
            sentiment_focus = request.get("sentiment_focus", "all")
            if sentiment_focus == "only_negative":
                results = [item for item in results if item.get("is_critical")]
            elif sentiment_focus == "only_positive":
                # Keep only products with strong positive sentiment
                results = [
                    item
                    for item in results
                    if (item.get("positive_pct") or 0.0) >= 0.7
                ]

            return results

        except Exception as e:
            logger.error(f"Error querying review sentiment: {e}")
            return []

    def _calculate_sentiment_kpis(
        self, sentiment_data: List[Dict[str, Any]], request: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Calculate KPI summary for sentiment"""

        num_products = len(sentiment_data)
        if num_products == 0:
            return {
                "num_products": 0,
                "total_reviews": 0,
                "avg_positive_pct": 0.0,
                "avg_negative_pct": 0.0,
                "num_products_with_critical_negative": 0,
                "avg_rating": 0.0,
            }

        total_reviews = sum(p["total_reviews"] for p in sentiment_data)
        avg_positive_pct = (
            sum(p["positive_pct"] for p in sentiment_data) / num_products
        )

        negative_threshold = request.get("negative_threshold", 0.25)
        num_critical = len(
            [p for p in sentiment_data if p["negative_pct"] > negative_threshold]
        )

        avg_negative_pct = (
            sum(p["negative_pct"] for p in sentiment_data) / num_products
        )

        avg_rating_values = [p["avg_rating"] for p in sentiment_data if p["avg_rating"]]
        avg_rating = (
            sum(avg_rating_values) / len(avg_rating_values) if avg_rating_values else 0
        )

        return {
            "num_products": num_products,
            "total_reviews": total_reviews,
            "avg_positive_pct": avg_positive_pct,
            "avg_negative_pct": avg_negative_pct,
            "num_products_with_critical_negative": num_critical,
            "avg_rating": avg_rating,
        }

    # ============================================
    # REVIEW DRILLDOWN - Detailed Reviews
    # ============================================

    async def get_product_review_details(
        self,
        product_key: str,
        sentiment_filter: str = "all",
        sort_by: str = "helpful_votes",
        limit: int = 50,
    ) -> Dict[str, Any]:
        """
        Get detailed reviews for a specific product.
        
        Args:
            product_key: Product key (e.g., 'tiki_123')
            sentiment_filter: 'all', 'positive', 'negative', 'neutral'
            sort_by: 'helpful_votes', 'rating', 'date'
            limit: Max number of reviews to return
        """
        
        # Build sentiment filter
        sentiment_condition = "1=1"
        params = [product_key]
        
        if sentiment_filter != "all":
            sentiment_condition = "sent.sentiment_label = $2"
            params.append(sentiment_filter)
        
        # Build ORDER BY clause
        order_clause = "r.helpful_votes DESC"
        if sort_by == "rating":
            order_clause = "r.rating DESC, r.helpful_votes DESC"
        elif sort_by == "date":
            order_clause = "dd.date_value DESC, r.helpful_votes DESC"
        
        sql = f"""
            WITH product_info AS (
                SELECT
                    dp.product_sk,
                    dp.product_key,
                    dp.product_name,
                    COUNT(DISTINCT r.review_sk) AS total_reviews,
                    SUM(CASE WHEN sent.sentiment_label = 'positive' THEN 1 ELSE 0 END) AS positive_count,
                    SUM(CASE WHEN sent.sentiment_label = 'neutral' THEN 1 ELSE 0 END) AS neutral_count,
                    SUM(CASE WHEN sent.sentiment_label = 'negative' THEN 1 ELSE 0 END) AS negative_count
                FROM dwh.dim_product dp
                LEFT JOIN dwh.fact_review r ON dp.product_sk = r.product_sk
                LEFT JOIN ml.fact_review_sentiment sent ON r.review_sk = sent.review_id
                WHERE dp.product_key = $1
                GROUP BY dp.product_sk, dp.product_key, dp.product_name
            )
            SELECT
                pi.product_key,
                pi.product_name,
                pi.total_reviews,
                pi.positive_count,
                pi.neutral_count,
                pi.negative_count,
                r.review_id_nk,
                r.rating,
                sent.sentiment_label,
                sent.sentiment_score,
                r.review_body,
                COALESCE(r.helpful_votes, 0) AS helpful_votes,
                r.reviewer_name,
                dd.date_value AS review_date
            FROM product_info pi
            JOIN dwh.fact_review r ON pi.product_sk = r.product_sk
            LEFT JOIN ml.fact_review_sentiment sent ON r.review_sk = sent.review_id
            JOIN dwh.dim_date dd ON r.date_sk = dd.date_sk
            WHERE {sentiment_condition}
            ORDER BY {order_clause}
            LIMIT {limit}
        """
        
        try:
            rows = await self.db.fetch(sql, *params)
            
            if not rows:
                return {
                    "product_key": product_key,
                    "product_name": None,
                    "total_reviews": 0,
                    "sentiment_breakdown": {
                        "positive": 0,
                        "neutral": 0,
                        "negative": 0
                    },
                    "reviews": []
                }
            
            # Extract product info from first row
            first_row = rows[0]
            
            reviews = []
            for row in rows:
                reviews.append({
                    "review_id": row["review_id_nk"],
                    "rating": int(row["rating"]) if row["rating"] else None,
                    "sentiment_label": row["sentiment_label"],
                    "sentiment_score": float(row["sentiment_score"]) if row["sentiment_score"] else None,
                    "review_body": row["review_body"],
                    "helpful_votes": int(row["helpful_votes"]),
                    "reviewer_name": row["reviewer_name"],
                    "review_date": row["review_date"].isoformat() if row["review_date"] else None
                })
            
            return {
                "product_key": first_row["product_key"],
                "product_name": first_row["product_name"],
                "total_reviews": int(first_row["total_reviews"]),
                "sentiment_breakdown": {
                    "positive": int(first_row["positive_count"]) if first_row["positive_count"] else 0,
                    "neutral": int(first_row["neutral_count"]) if first_row["neutral_count"] else 0,
                    "negative": int(first_row["negative_count"]) if first_row["negative_count"] else 0
                },
                "reviews": reviews
            }
            
        except Exception as e:
            logger.error(f"Error getting product review details: {e}")
            return {
                "product_key": product_key,
                "error": str(e),
                "reviews": []
            }

    # ============================================
    # DECISION & ACTION MANAGEMENT
    # ============================================

    async def save_decision(
        self,
        user_id: int,
        payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Save a DSS Decision with action plan.
        
        Steps:
        1. Validate input
        2. Create or load analysis session
        3. Insert decision record
        4. Insert action items
        5. Log activity
        
        Uses transaction for atomicity.
        """
        try:
            scenario_key = payload.get("scenario_key")
            session_id = payload.get("session_id")
            actions = payload.get("actions", [])
            
            # Validate
            if scenario_key not in ["price_prediction", "product_recommendation", "review_sentiment"]:
                raise ValueError(f"Invalid scenario_key: {scenario_key}")
            
            if not actions:
                raise ValueError("Actions list cannot be empty")
            
            # Start transaction
            async with self.db.transaction():
                # Step 1: Get or create session_id
                if session_id:
                    # Verify existing session
                    session_row = await self.db.fetchrow(
                        "SELECT session_id, scenario_key FROM dss.dss_analysis_session WHERE session_id = $1",
                        session_id
                    )
                    if not session_row:
                        raise ValueError(f"Session {session_id} not found")
                    if session_row["scenario_key"] != scenario_key:
                        raise ValueError(f"Session scenario_key mismatch")
                else:
                    # Create new session from snapshot
                    session_row = await self.db.fetchrow(
                        """
                        INSERT INTO dss.dss_analysis_session (
                            scenario_key, user_id, filters_json, kpi_summary_json,
                            ai_summary_insights, ai_recommended_actions, date_adjustment_info,
                            generated_at, source_endpoint
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), $8)
                        RETURNING session_id
                        """,
                        scenario_key,
                        user_id,
                        json.dumps(payload.get("filters", {})),
                        json.dumps(payload.get("kpi_summary", {})),
                        json.dumps(payload.get("ai_summary_insights", [])),
                        json.dumps(payload.get("ai_recommended_actions", [])),
                        json.dumps(payload.get("date_adjustment_info")) if payload.get("date_adjustment_info") else None,
                        f"/dss/{scenario_key}/run"
                    )
                    session_id = session_row["session_id"]
                    logger.info(f"Created new session {session_id} for scenario {scenario_key}")
                
                # Step 2: Insert decision
                decision_row = await self.db.fetchrow(
                    """
                    INSERT INTO dss.dss_decision (
                        session_id, scenario_key, title, description, status, created_by
                    ) VALUES ($1, $2, $3, $4, $5, $6)
                    RETURNING decision_id, created_at
                    """,
                    session_id,
                    scenario_key,
                    payload.get("title"),
                    payload.get("description"),
                    payload.get("status", "DRAFT"),
                    user_id
                )
                decision_id = decision_row["decision_id"]
                logger.info(f"Created decision {decision_id}")
                
                # Step 3: Insert action items
                action_ids = []
                for action in actions:
                    # NEW: Auto-lookup product_sk from product_key if needed
                    product_sk = action.get("product_sk")
                    
                    if not product_sk and action.get("product_key"):
                        product_key = action.get("product_key")
                        logger.info(f"Looking up product_sk for product_key: {product_key}")
                        
                        product_row = await self.db.fetchrow(
                            "SELECT product_sk FROM dwh.dim_product WHERE product_key = $1",
                            product_key
                        )
                        
                        if product_row:
                            product_sk = product_row["product_sk"]
                            logger.info(f"Found product_sk: {product_sk} for {product_key}")
                        else:
                            logger.warning(f"Product not found for product_key: {product_key}")
                            # Continue without product_sk - validation will catch if required
                    
                    action_row = await self.db.fetchrow(
                        """
                        INSERT INTO dss.dss_action_item (
                            decision_id, action_type, target_level,
                            product_sk, platform_sk, category_sk,
                            current_value, recommended_value, chosen_value, unit,
                            planned_start_date, planned_end_date, status, note
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                        RETURNING action_id
                        """,
                        decision_id,
                        action.get("action_type"),
                        action.get("target_level"),
                        action.get("product_sk"),
                        action.get("platform_sk"),
                        action.get("category_sk"),
                        action.get("current_value"),
                        action.get("recommended_value"),
                        action.get("chosen_value"),
                        action.get("unit"),
                        action.get("planned_start_date"),
                        action.get("planned_end_date"),
                        action.get("status", "PLANNED"),
                        action.get("note")
                    )
                    action_ids.append(action_row["action_id"])
                
                logger.info(f"Created {len(action_ids)} actions for decision {decision_id}")
                
                # Step 4: Log activity
                await self.db.execute(
                    f"""
                    INSERT INTO {ACTIVITY_LOG_TABLE} (user_id, action, resource, details, status)
                    VALUES ($1, $2, $3, $4, $5)
                    """,
                    user_id,
                    "dss_save_decision",
                    "/dss/decisions",
                    json.dumps(
                        {
                            "decision_id": decision_id,
                            "scenario_key": scenario_key,
                            "num_actions": len(action_ids),
                            "status": payload.get("status", "DRAFT"),
                        }
                    ),
                    "success",
                )
            
            # Transaction committed
            logger.info(f"Successfully saved decision {decision_id} with {len(action_ids)} actions")
            
            # Return decision detail
            return await self.get_decision_detail(decision_id)
            
        except Exception as e:
            logger.exception(f"Error saving decision: {e}")
            raise

    async def list_decisions(
        self,
        scenario_key: Optional[str] = None,
        status: Optional[str] = None,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        page: int = 1,
        page_size: int = 10
    ) -> Dict[str, Any]:
        """
        List decisions with filters and pagination.
        """
        try:
            conditions = []
            params = []
            param_idx = 1
            
            if scenario_key:
                conditions.append(f"d.scenario_key = ${param_idx}")
                params.append(scenario_key)
                param_idx += 1
            
            if status:
                conditions.append(f"d.status = ${param_idx}")
                params.append(status)
                param_idx += 1
            
            if from_date:
                conditions.append(f"d.created_at >= ${param_idx}")
                # Convert string to date object for asyncpg
                params.append(datetime.fromisoformat(from_date).date() if isinstance(from_date, str) else from_date)
                param_idx += 1
            
            if to_date:
                conditions.append(f"d.created_at <= ${param_idx}")
                # Convert string to date object for asyncpg
                params.append(datetime.fromisoformat(to_date).date() if isinstance(to_date, str) else to_date)
                param_idx += 1
            
            where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
            
            offset = (page - 1) * page_size
            
            # Query decisions with action count
            sql = f"""
                SELECT 
                    d.decision_id,
                    d.scenario_key,
                    d.title,
                    d.status,
                    d.created_by,
                    u.email AS created_by_email,
                    d.created_at,
                    COUNT(a.action_id) AS num_actions
                FROM dss.dss_decision d
                LEFT JOIN iam.iam_user u ON d.created_by = u.user_id
                LEFT JOIN dss.dss_action_item a ON d.decision_id = a.decision_id
                {where_clause}
                GROUP BY d.decision_id, d.scenario_key, d.title, d.status, 
                         d.created_by, u.email, d.created_at
                ORDER BY d.created_at DESC
                LIMIT ${param_idx} OFFSET ${param_idx + 1}
            """
            params.extend([page_size, offset])
            
            rows = await self.db.fetch(sql, *params)
            
            # Get total count
            count_sql = f"""
                SELECT COUNT(DISTINCT d.decision_id)
                FROM dss.dss_decision d
                {where_clause}
            """
            count_params = params[:param_idx-1]  # Exclude LIMIT and OFFSET
            count_row = await self.db.fetchrow(count_sql, *count_params)
            total = count_row["count"] if count_row else 0
            
            items = []
            for row in rows:
                items.append({
                    "decision_id": row["decision_id"],
                    "scenario_key": row["scenario_key"],
                    "title": row["title"],
                    "status": row["status"],
                    "created_by": row["created_by"],
                    "created_by_email": row["created_by_email"],
                    "created_at": row["created_at"].isoformat() if row["created_at"] else None,
                    "num_actions": row["num_actions"]
                })
            
            return {
                "total": total,
                "page": page,
                "page_size": page_size,
                "items": items
            }
            
        except Exception as e:
            logger.exception(f"Error listing decisions: {e}")
            raise

    async def get_decision_detail(self, decision_id: int) -> Dict[str, Any]:
        """
        Get detailed information for a decision including session data and actions.
        """
        try:
            # Query decision with session data
            decision_sql = """
                SELECT 
                    d.decision_id,
                    d.session_id,
                    d.scenario_key,
                    d.title,
                    d.description,
                    d.status,
                    d.created_by,
                    u_created.email AS created_by_email,
                    d.created_at,
                    d.updated_at,
                    d.approved_by,
                    u_approved.email AS approved_by_email,
                    d.approved_at,
                    s.filters_json,
                    s.kpi_summary_json,
                    s.ai_summary_insights,
                    s.ai_recommended_actions,
                    s.date_adjustment_info
                FROM dss.dss_decision d
                JOIN dss.dss_analysis_session s ON d.session_id = s.session_id
                LEFT JOIN iam.iam_user u_created ON d.created_by = u_created.user_id
                LEFT JOIN iam.iam_user u_approved ON d.approved_by = u_approved.user_id
                WHERE d.decision_id = $1
            """
            
            decision_row = await self.db.fetchrow(decision_sql, decision_id)
            
            if not decision_row:
                raise ValueError(f"Decision {decision_id} not found")
            
            # Query actions with enriched data
            actions_sql = """
                SELECT 
                    a.action_id,
                    a.action_type,
                    a.target_level,
                    a.product_sk,
                    a.platform_sk,
                    a.category_sk,
                    a.current_value,
                    a.recommended_value,
                    a.chosen_value,
                    a.unit,
                    a.planned_start_date,
                    a.planned_end_date,
                    a.status,
                    a.note,
                    dp.product_name,
                    COALESCE(dc.category_lvl2, dc.category_lvl1) AS category_name,
                    dpl.platform_code AS platform_name
                FROM dss.dss_action_item a
                LEFT JOIN dwh.dim_product dp ON a.product_sk = dp.product_sk
                LEFT JOIN dwh.dim_category dc ON a.category_sk = dc.category_sk
                LEFT JOIN dwh.dim_platform dpl ON a.platform_sk = dpl.platform_sk
                WHERE a.decision_id = $1
                ORDER BY a.action_id
            """
            
            action_rows = await self.db.fetch(actions_sql, decision_id)
            
            actions = []
            for row in action_rows:
                actions.append({
                    "action_id": row["action_id"],
                    "action_type": row["action_type"],
                    "target_level": row["target_level"],
                    "product_sk": row["product_sk"],
                    "platform_sk": row["platform_sk"],
                    "category_sk": row["category_sk"],
                    "current_value": float(row["current_value"]) if row["current_value"] is not None else None,
                    "recommended_value": float(row["recommended_value"]) if row["recommended_value"] is not None else None,
                    "chosen_value": float(row["chosen_value"]) if row["chosen_value"] is not None else None,
                    "unit": row["unit"],
                    "planned_start_date": row["planned_start_date"].isoformat() if row["planned_start_date"] else None,
                    "planned_end_date": row["planned_end_date"].isoformat() if row["planned_end_date"] else None,
                    "status": row["status"],
                    "note": row["note"],
                    "product_name": row["product_name"],
                    "category_name": row["category_name"],
                    "platform_name": row["platform_name"]
                })
            
            # Build response
            return {
                "decision_id": decision_row["decision_id"],
                "session_id": decision_row["session_id"],
                "scenario_key": decision_row["scenario_key"],
                "title": decision_row["title"],
                "description": decision_row["description"],
                "status": decision_row["status"],
                "created_by": decision_row["created_by"],
                "created_by_email": decision_row["created_by_email"],
                "created_at": decision_row["created_at"].isoformat() if decision_row["created_at"] else None,
                "updated_at": decision_row["updated_at"].isoformat() if decision_row["updated_at"] else None,
                "approved_by": decision_row["approved_by"],
                "approved_by_email": decision_row["approved_by_email"],
                "approved_at": decision_row["approved_at"].isoformat() if decision_row["approved_at"] else None,
                "filters": json.loads(decision_row["filters_json"]) if decision_row["filters_json"] else {},
                "kpi_summary": json.loads(decision_row["kpi_summary_json"]) if decision_row["kpi_summary_json"] else {},
                "ai_summary_insights": json.loads(decision_row["ai_summary_insights"]) if decision_row["ai_summary_insights"] else [],
                "ai_recommended_actions": json.loads(decision_row["ai_recommended_actions"]) if decision_row["ai_recommended_actions"] else [],
                "date_adjustment_info": json.loads(decision_row["date_adjustment_info"]) if decision_row["date_adjustment_info"] else None,
                "actions": actions
            }
            
        except Exception as e:
            logger.exception(f"Error getting decision detail: {e}")
            raise

