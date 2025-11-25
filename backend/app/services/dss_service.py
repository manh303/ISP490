"""
DSS Service - Business logic for Decision Support System
Queries data from Postgres (fact & ML tables) and structures results
"""

import logging
import asyncio
from datetime import date, datetime
from typing import Dict, Any, List, Optional
import asyncpg

from app.services.ai_summarizer import get_ai_summarizer

logger = logging.getLogger(__name__)


class DSSService:
    """Service for DSS analysis - queries data and generates insights"""
    
    def __init__(self, db):
        """
        Args:
            db: asyncpg connection
        """
        self.db = db
        self.ai_summarizer = get_ai_summarizer()
    
    # ============================================
    # PRICE PREDICTION DSS
    # ============================================
    
    async def run_price_prediction_dss(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run Price Prediction DSS analysis
        
        Steps:
        1. Query fact tables + ml_price_predictions
        2. Build DSS_RESULT_RAW
        3. Call AI to generate insights
        4. Return DSS_RESULT_WITH_AI
        """
        
        logger.info(f"Running Price Prediction DSS: {request}")
        
        # 1. Query data (returns data + metadata with adjusted dates)
        query_result = await self._query_price_predictions(request)
        products_data = query_result.get("data", [])
        date_adjusted = query_result.get("date_adjusted", False)
        actual_from_date = query_result.get("actual_from_date", request.get("from_date"))
        actual_to_date = query_result.get("actual_to_date", request.get("to_date"))
        
        # 2. Calculate KPIs
        kpi_summary = self._calculate_price_kpis(products_data)
        
        # 3. Build DSS_RESULT_RAW
        dss_result_raw = {
            "scenario": "price_prediction",
            "filters": {
                "from_date": str(request.get("from_date")),
                "to_date": str(request.get("to_date")),
                "platforms": request.get("platforms", []),
                "categories": request.get("categories", []),
            },
            "kpi_summary": kpi_summary,
            "table_data": products_data
        }
        
        # Add date adjustment info if dates were changed
        if date_adjusted:
            dss_result_raw["date_adjustment_info"] = {
                "requested_from": str(request.get("from_date")),
                "requested_to": str(request.get("to_date")),
                "actual_from": str(actual_from_date),
                "actual_to": str(actual_to_date),
                "message": f"⚠️ Dữ liệu không có sẵn cho ngày yêu cầu. Đã tự động sử dụng dữ liệu gần nhất: {actual_to_date}"
            }
        
        # 4. Generate AI insights
        ai_result = self.ai_summarizer.summarize_with_ai("price_prediction", dss_result_raw)
        
        # 5. Combine and return
        return {
            **dss_result_raw,
            "ai_summary_insights": ai_result.get("summary_insights", []),
            "ai_recommended_actions": ai_result.get("recommended_actions", []),
            "generated_at": datetime.now().isoformat(),
            "ai_model_used": self.ai_summarizer.model if self.ai_summarizer.available else "rule-based"
        }
    
    async def _get_latest_available_date(self, requested_date: date) -> Optional[date]:
        """
        Get the latest date with available data, falling back if requested date has no data.
        Returns the requested date if it has data, otherwise the most recent date with data.
        """
        try:
            # Check if requested date has data
            has_data = await self.db.fetchval("""
                SELECT EXISTS(
                    SELECT 1 FROM dwh.fact_product_daily f
                    JOIN dwh.dim_date dd ON f.date_sk = dd.date_sk
                    WHERE dd.date_value = $1
                    LIMIT 1
                )
            """, requested_date)
            
            if has_data:
                logger.info(f"✅ Data available for requested date: {requested_date}")
                return requested_date
            
            # Fallback: Get the most recent date with data
            latest_date = await self.db.fetchval("""
                SELECT MAX(dd.date_value)
                FROM dwh.fact_product_daily f
                JOIN dwh.dim_date dd ON f.date_sk = dd.date_sk
                WHERE dd.date_value <= $1
            """, requested_date)
            
            if latest_date:
                logger.warning(f"⚠️ No data for {requested_date}, falling back to {latest_date}")
                return latest_date
            
            logger.error(f"❌ No data available at all!")
            return None
            
        except Exception as e:
            logger.error(f"Error checking date availability: {e}")
            return None
    
    async def _query_price_predictions(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """
        Query price predictions from database (OPTIMIZED)
        
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
                "data": [],
                "date_adjusted": False,
                "actual_from_date": from_date,
                "actual_to_date": to_date,
                "total_count": 0
            }
        
        # If to_date was adjusted, also adjust from_date if needed
        if adjusted_to_date != to_date:
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
        params = [from_date, adjusted_to_date]
        param_idx = 3
        conditions = []
        
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
                    ) as rn
                FROM ml.fact_price_prediction pred
                WHERE pred.created_at >= NOW() - INTERVAL '7 days'  -- Reduced from 30 to 7 days for performance
                    AND pred.predicted_price > 0
            ),
            latest_predictions AS (
                SELECT 
                    product_sk,
                    platform_sk,
                    predicted_price,
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
                    SUM(f.avg_price * f.total_review_count) AS current_revenue,
                    SUM(f.total_review_count) AS total_orders,
                    AVG(f.avg_rating) AS avg_rating,
                    SUM(f.total_review_count) AS total_reviews
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
                    COALESCE(dc.category_lvl1 || ' > ' || dc.category_lvl2, dc.category_lvl1, 'Uncategorized') AS category_name,
                    
                    COALESCE(pm.current_price, 0) AS current_price,
                    pred.predicted_price AS recommended_price,
                    (pred.predicted_price - COALESCE(pm.current_price, 0)) / NULLIF(pm.current_price, 0) AS price_change_pct,
                    
                    COALESCE(pm.current_revenue, 0) AS current_revenue,
                    COALESCE(pm.current_revenue * pred.predicted_price / NULLIF(pm.current_price, 0), 0) AS projected_revenue,
                    (pred.predicted_price / NULLIF(pm.current_price, 0) - 1) AS expected_revenue_change_pct,
                    
                    pred.confidence,
                    
                    COALESCE(pm.total_orders, 0) AS current_orders,
                    pm.avg_rating,
                    COALESCE(pm.total_reviews, 0) AS total_reviews,
                    
                    -- For sorting
                    ABS(pred.predicted_price / NULLIF(pm.current_price, 0) - 1) AS abs_revenue_change
                FROM latest_predictions pred
                JOIN dwh.dim_product dp ON pred.product_sk = dp.product_sk
                JOIN dwh.dim_platform dpl ON pred.platform_sk = dpl.platform_sk
                LEFT JOIN dwh.dim_category dc ON dp.category_sk = dc.category_sk
                LEFT JOIN product_metrics pm ON dp.product_sk = pm.product_sk AND pred.platform_sk = pm.platform_sk
                WHERE {where_clause}
                  AND COALESCE(pm.current_price, 0) > 0
                  AND pred.confidence >= {confidence_param}
                  AND ABS((pred.predicted_price - COALESCE(pm.current_price, 0)) / NULLIF(pm.current_price, 0)) > {price_change_param}
            )
            SELECT 
                product_key,
                product_name,
                platform,
                category_name,
                current_price,
                recommended_price,
                price_change_pct,
                current_revenue,
                projected_revenue,
                expected_revenue_change_pct,
                confidence,
                current_orders,
                avg_rating,
                total_reviews
            FROM filtered_results
            ORDER BY abs_revenue_change DESC
            LIMIT {limit_param}
            OFFSET {offset_param}
        """
        
        # Count query for pagination
        count_sql = f"""
            WITH ranked_predictions AS (
                SELECT 
                    pred.product_sk,
                    pred.platform_sk,
                    pred.predicted_price,
                    pred.ci_upper,
                    pred.ci_lower,
                    ROW_NUMBER() OVER (
                        PARTITION BY pred.product_sk, pred.platform_sk 
                        ORDER BY pred.created_at DESC
                    ) as rn
                FROM ml.fact_price_prediction pred
                WHERE pred.created_at >= NOW() - INTERVAL '7 days'  -- Match main query
                    AND pred.predicted_price > 0
            ),
            latest_predictions AS (
                SELECT 
                    product_sk,
                    platform_sk,
                    predicted_price,
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
            LEFT JOIN product_metrics pm ON dp.product_sk = pm.product_sk AND pred.platform_sk = pm.platform_sk
            WHERE {where_clause}
              AND COALESCE(pm.current_price, 0) > 0
              AND pred.confidence >= {confidence_param}
              AND ABS((pred.predicted_price - COALESCE(pm.current_price, 0)) / NULLIF(pm.current_price, 0)) > {price_change_param}
        """
        
        try:
            # Build params for count query
            # Count query needs: dates + filters + confidence + price_change
            # But NOT limit and offset (which are the last 2 params)
            # So we take all params except the last 2
            count_params = params[:-2]
            
            logger.debug(f"Total params: {len(params)}, Count params: {len(count_params)}")
            logger.debug(f"Count query expects params: dates(2) + filters({len(params)-6}) + confidence(1) + price_change(1) = {len(count_params)}")
            
            # Execute count query first to get total count
            # NOTE: Cannot run concurrently on same connection - asyncpg limitation
            total_count = await self.db.fetchval(count_sql, *count_params) or 0
            
            # Then execute data query
            rows = await self.db.fetch(sql, *params)
            
            results = []
            for row in rows:
                results.append({
                    "product_key": row["product_key"],
                    "product_name": row["product_name"],
                    "platform": row["platform"],
                    "category_name": row["category_name"],
                    "current_price": float(row["current_price"]) if row["current_price"] else 0,
                    "recommended_price": float(row["recommended_price"]) if row["recommended_price"] else 0,
                    "price_change_pct": float(row["price_change_pct"]) if row["price_change_pct"] else 0,
                    "current_revenue": float(row["current_revenue"]) if row["current_revenue"] else 0,
                    "projected_revenue": float(row["projected_revenue"]) if row["projected_revenue"] else 0,
                    "expected_revenue_change_pct": float(row["expected_revenue_change_pct"]) if row["expected_revenue_change_pct"] else 0,
                    "confidence": float(row["confidence"]) if row["confidence"] else 0,
                    "current_orders": int(row["current_orders"]) if row["current_orders"] else 0,
                    "avg_rating": float(row["avg_rating"]) if row["avg_rating"] else None,
                    "total_reviews": int(row["total_reviews"]) if row["total_reviews"] else 0,
                    "margin_pct": None  # Would need cost data
                })
            
            # Return data with metadata about date adjustments and pagination
            date_adjusted = (adjusted_to_date != to_date)
            
            return {
                "data": results,
                "date_adjusted": date_adjusted,
                "actual_from_date": from_date,
                "actual_to_date": adjusted_to_date,
                "total_count": total_count,
                "page": page,
                "page_size": page_size,
                "total_pages": (total_count + page_size - 1) // page_size if page_size > 0 else 0
            }
            
        except asyncpg.PostgresError as e:
            logger.error(f"Database error querying price predictions: {e}", exc_info=True)
            return {
                "data": [],
                "date_adjusted": False,
                "actual_from_date": from_date,
                "actual_to_date": to_date,
                "total_count": 0,
                "page": page,
                "page_size": page_size,
                "total_pages": 0
            }
        except asyncpg.QueryCanceledError as e:
            logger.error(f"Query timeout for price predictions: {e}")
            return {
                "data": [],
                "date_adjusted": False,
                "actual_from_date": from_date,
                "actual_to_date": to_date,
                "total_count": 0,
                "page": page,
                "page_size": page_size,
                "total_pages": 0
            }
        except Exception as e:
            logger.error(f"Unexpected error querying price predictions: {e}", exc_info=True)
            return {
                "data": [],
                "date_adjusted": False,
                "actual_from_date": from_date,
                "actual_to_date": to_date,
                "total_count": 0,
                "page": page,
                "page_size": page_size,
                "total_pages": 0
            }
    
    def _calculate_price_kpis(self, products_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate KPI summary for price prediction"""
        
        num_products = len(products_data)
        num_with_recommendation = len([p for p in products_data if abs(p["price_change_pct"]) > 0.02])
        
        current_total_revenue = sum(p["current_revenue"] for p in products_data)
        projected_total_revenue = sum(p["projected_revenue"] for p in products_data)
        
        return {
            "num_products": num_products,
            "num_with_recommendation": num_with_recommendation,
            "current_total_revenue": current_total_revenue,
            "projected_total_revenue": projected_total_revenue,
            "revenue_change_pct": ((projected_total_revenue - current_total_revenue) / current_total_revenue) if current_total_revenue > 0 else 0,
            "avg_confidence": sum(p["confidence"] for p in products_data) / num_products if num_products > 0 else 0
        }
    
    # ============================================
    # PRODUCT RECOMMENDATION DSS
    # ============================================
    
    async def run_product_recommendation_dss(self, request: Dict[str, Any]) -> Dict[str, Any]:
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
                "scope_mode": request.get("scope_mode"),
                "source_product_key": request.get("source_product_key"),
            },
            "kpi_summary": kpi_summary,
            "table_data": recommendations
        }
        
        # 4. Generate AI insights
        ai_result = self.ai_summarizer.summarize_with_ai("product_recommendation", dss_result_raw)
        
        # 5. Return
        return {
            **dss_result_raw,
            "ai_summary_insights": ai_result.get("summary_insights", []),
            "ai_recommended_actions": ai_result.get("recommended_actions", []),
            "generated_at": datetime.now().isoformat(),
            "ai_model_used": self.ai_summarizer.model if self.ai_summarizer.available else "rule-based"
        }
    
    async def _query_product_recommendations(self, request: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Query product recommendations from database"""
        
        scope_mode = request.get("scope_mode", "by_category")
        top_k = request.get("top_k", 10)
        min_similarity = request.get("min_similarity", 0.5)
        
        # Different query based on scope_mode
        if scope_mode == "by_product":
            return await self._query_recommendations_by_product(request)
        else:
            return await self._query_recommendations_by_category(request)
    
    async def _query_recommendations_by_product(self, request: Dict[str, Any]) -> List[Dict[str, Any]]:
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
                dc.category_lvl1 || ' > ' || COALESCE(dc.category_lvl2, '') AS category_name,
                rec.similarity_score,
                0.0 AS co_purchase_rate,
                COALESCE(pm_rec.avg_price, 0) AS avg_bundle_revenue,
                COALESCE(pm_rec.total_orders, 0) AS total_bundle_orders,
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
    
    async def _query_recommendations_by_category(self, request: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Get top recommendations within category"""
        
        platforms = request.get("platforms")
        categories = request.get("categories")
        top_k = request.get("top_k", 10)
        min_similarity = request.get("min_similarity", 0.5)
        
        conditions = ["rec.similarity_score >= $1"]
        params = [min_similarity]
        param_idx = 2
        
        platform_filter = ""
        if platforms:
            platform_filter = f"AND SUBSTRING(dp_rec.product_key FROM '^(.*?)_') = ANY(${param_idx})"
            params.append(platforms)
            param_idx += 1
        
        category_filter = ""
        if categories:
            category_filter = f"AND CAST(dc.category_sk AS TEXT) = ANY(${param_idx})"
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
                dc.category_lvl1 || ' > ' || COALESCE(dc.category_lvl2, '') AS category_name,
                rec.similarity_score,
                0.0 AS co_purchase_rate,
                COALESCE(pm_rec.avg_price, 0) AS avg_bundle_revenue,
                COALESCE(pm_rec.total_orders, 0) AS total_bundle_orders,
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
    
    def _calculate_reco_kpis(self, recommendations: List[Dict[str, Any]], request: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate KPI summary for recommendations"""
        
        num_recommendations = len(recommendations)
        avg_similarity = sum(r["similarity_score"] for r in recommendations) / num_recommendations if num_recommendations > 0 else 0
        total_bundle_opportunity = sum(r.get("avg_bundle_revenue", 0) for r in recommendations)
        
        source_product = request.get("source_product_key", "N/A")
        if request.get("scope_mode") == "by_category":
            source_product = f"{len(set(r['source_product_key'] for r in recommendations))} products"
        
        return {
            "source_product": source_product,
            "num_recommendations": num_recommendations,
            "avg_similarity": avg_similarity,
            "total_bundle_opportunity": total_bundle_opportunity,
            "avg_co_purchase_rate": sum(r.get("co_purchase_rate", 0) for r in recommendations) / num_recommendations if num_recommendations > 0 else 0
        }
    
    # ============================================
    # REVIEW SENTIMENT DSS
    # ============================================
    
    async def run_review_sentiment_dss(self, request: Dict[str, Any]) -> Dict[str, Any]:
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
            "table_data": sentiment_data
        }
        
        # 4. Generate AI insights
        ai_result = self.ai_summarizer.summarize_with_ai("review_sentiment", dss_result_raw)
        
        # 5. Return
        return {
            **dss_result_raw,
            "ai_summary_insights": ai_result.get("summary_insights", []),
            "ai_recommended_actions": ai_result.get("recommended_actions", []),
            "generated_at": datetime.now().isoformat(),
            "ai_model_used": self.ai_summarizer.model if self.ai_summarizer.available else "rule-based"
        }
    
    async def _query_review_sentiment(self, request: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Query review sentiment from database"""
        
        from_date = request.get("from_date")
        to_date = request.get("to_date")
        platforms = request.get("platforms")
        categories = request.get("categories")
        min_reviews = request.get("min_reviews_per_product", 10)
        negative_threshold = request.get("negative_threshold", 0.25)
        
        conditions = []
        params = []
        param_idx = 1
        
        if platforms:
            conditions.append(f"dpl.platform_code = ANY(${param_idx})")
            params.append(platforms)
            param_idx += 1
        
        if categories:
            conditions.append(f"CAST(dc.category_sk AS TEXT) = ANY(${param_idx})")
            params.append(categories)
            param_idx += 1
        
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
                    AVG(CASE WHEN sent.sentiment_label = 'positive' THEN sent.sentiment_score ELSE NULL END) AS avg_positive_score,
                    AVG(CASE WHEN sent.sentiment_label = 'negative' THEN sent.sentiment_score ELSE NULL END) AS avg_negative_score
                FROM dwh.fact_review r
                LEFT JOIN ml.fact_review_sentiment sent ON r.review_sk = sent.review_id
                JOIN dwh.dim_platform dpl ON r.platform_sk = dpl.platform_sk
                LEFT JOIN dwh.dim_product dp ON r.product_sk = dp.product_sk
                LEFT JOIN dwh.dim_category dc ON dp.category_sk = dc.category_sk
                WHERE {where_clause}
                GROUP BY r.product_sk
                HAVING COUNT(*) >= {min_reviews}
            )
            SELECT
                dp.product_key,
                dp.product_name,
                SUBSTRING(dp.product_key FROM '^(.*?)_') AS platform,
                dc.category_lvl1 || ' > ' || COALESCE(dc.category_lvl2, '') AS category_name,
                
                rs.total_reviews,
                rs.positive_count,
                rs.neutral_count,
                rs.negative_count,
                
                CAST(rs.positive_count AS FLOAT) / NULLIF(rs.total_reviews, 0) AS positive_pct,
                CAST(rs.neutral_count AS FLOAT) / NULLIF(rs.total_reviews, 0) AS neutral_pct,
                CAST(rs.negative_count AS FLOAT) / NULLIF(rs.total_reviews, 0) AS negative_pct,
                
                rs.avg_rating,
                rs.avg_positive_score,
                rs.avg_negative_score
            FROM review_stats rs
            JOIN dwh.dim_product dp ON rs.product_sk = dp.product_sk
            LEFT JOIN dwh.dim_category dc ON dp.category_sk = dc.category_sk
            ORDER BY rs.negative_count DESC, rs.total_reviews DESC
            LIMIT 100
        """
        
        try:
            rows = await self.db.fetch(sql, *params)
            
            results = []
            for row in rows:
                negative_pct = float(row["negative_pct"]) if row["negative_pct"] else 0
                
                # Use sentiment scores as proxy for quality (no keywords in schema)
                avg_negative_score = float(row.get("avg_negative_score") or 0)
                
                # Generate simple reasons based on rating and sentiment
                neg_reasons = []
                if negative_pct > 0.3:
                    neg_reasons.append("Tỷ lệ đánh giá tiêu cực cao")
                if row.get("avg_rating", 0) and row.get("avg_rating") < 3.5:
                    neg_reasons.append("Điểm rating thấp")
                if avg_negative_score > 0.5:
                    neg_reasons.append("Cảm xúc tiêu cực mạnh")
                
                results.append({
                    "product_key": row["product_key"],
                    "product_name": row["product_name"],
                    "platform": row["platform"],
                    "category_name": row["category_name"],
                    "total_reviews": int(row["total_reviews"]),
                    "positive_count": int(row["positive_count"]) if row["positive_count"] else 0,
                    "neutral_count": int(row["neutral_count"]) if row["neutral_count"] else 0,
                    "negative_count": int(row["negative_count"]) if row["negative_count"] else 0,
                    "positive_pct": float(row["positive_pct"]) if row["positive_pct"] else 0,
                    "neutral_pct": float(row["neutral_pct"]) if row["neutral_pct"] else 0,
                    "negative_pct": negative_pct,
                    "avg_rating": float(row["avg_rating"]) if row["avg_rating"] else None,
                    "top_positive_reasons": ["Đánh giá tích cực từ khách hàng"],
                    "top_negative_reasons": neg_reasons if neg_reasons else ["Không có vấn đề nghiêm trọng"],
                    "is_critical": negative_pct > negative_threshold
                })
            
            return results
            
        except Exception as e:
            logger.error(f"Error querying review sentiment: {e}")
            return []
    
    def _calculate_sentiment_kpis(self, sentiment_data: List[Dict[str, Any]], request: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate KPI summary for sentiment"""
        
        num_products = len(sentiment_data)
        
        total_reviews = sum(p["total_reviews"] for p in sentiment_data)
        avg_positive_pct = sum(p["positive_pct"] for p in sentiment_data) / num_products if num_products > 0 else 0
        
        negative_threshold = request.get("negative_threshold", 0.25)
        num_critical = len([p for p in sentiment_data if p["negative_pct"] > negative_threshold])
        
        return {
            "num_products": num_products,
            "total_reviews": total_reviews,
            "avg_positive_pct": avg_positive_pct,
            "avg_negative_pct": sum(p["negative_pct"] for p in sentiment_data) / num_products if num_products > 0 else 0,
            "num_products_with_critical_negative": num_critical,
            "avg_rating": sum(p["avg_rating"] for p in sentiment_data if p["avg_rating"]) / len([p for p in sentiment_data if p["avg_rating"]]) if sentiment_data else 0
        }

