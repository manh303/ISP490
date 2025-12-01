# app/services/ml_service.py
import json
from typing import Optional, List, Dict, Any
from datetime import date, timedelta
import math

from app.schemas.ml import (
    MLModelCreate,
    MLModelUpdate,
    MLModelResponse,
    PricePredictionHistoryResponse,
    PricePredictionPoint,
    OnlinePricePredictionRequest,
    OnlinePricePredictionResponse,
    RecommendationResponse,
    RecommendationItem,
    SentimentSummaryResponse,
    SentimentSummaryItem,
    OnlineSentimentRequest,
    OnlineSentimentResponse,
    MLStatusSummary,
)

def _safe_float(value: Any) -> Optional[float]:
    """
    Chuyển value thành float, nếu là NaN / inf / None / lỗi parse thì trả về None.
    Dùng để tránh ValueError khi serialize JSON.
    """
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(f) or math.isinf(f):
        return None
    return f

class MLService:
    """
    Service layer cho actor Machine Learning.
    Làm việc trực tiếp với các bảng:
      - ml.dim_ml_model
      - ml.fact_price_prediction
      - ml.fact_product_recommendation
      - dwh.dim_date / dim_product / dim_platform
    """

    def __init__(self, db):
        """
        db: async connection / pool có method fetch, fetchrow, execute (vd asyncpg connection)
        """
        self.db = db

    async def _resolve_model(
        self,
        model_type: str,
        model_name: Optional[str],
        model_version: Optional[str],
    ) -> MLModelResponse:
        """
        Resolve model từ ml.dim_ml_model registry.
        
        Logic:
        - Nếu truyền model_name/model_version → chọn exact match
        - Nếu không → lấy model mới nhất có status='active' cho model_type
        
        Args:
            model_type: Loại model ('price', 'sentiment', 'recommendation')
            model_name: Tên model (optional)
            model_version: Version (optional)
            
        Returns:
            MLModelResponse object
            
        Raises:
            RuntimeError: Nếu không tìm thấy model phù hợp
        """
        query = """
            SELECT model_sk, model_name, model_type, model_version,
                   training_data_until, metrics, status, created_at
            FROM ml.dim_ml_model
            WHERE model_type = $1
              AND status = 'active'
        """
        params: List[Any] = [model_type]

        if model_name:
            query += f" AND model_name = ${len(params)+1}"
            params.append(model_name)
        if model_version:
            query += f" AND model_version = ${len(params)+1}"
            params.append(model_version)

        query += " ORDER BY created_at DESC LIMIT 1"

        row = await self.db.fetchrow(query, *params)
        if not row:
            raise RuntimeError(
                f"No active model found for type='{model_type}' "
                f"(name='{model_name}', version='{model_version}')"
            )

        metrics = row["metrics"]
        if isinstance(metrics, str):
            metrics = json.loads(metrics)

        return MLModelResponse(
            model_sk=row["model_sk"],
            model_name=row["model_name"],
            model_type=row["model_type"],
            model_version=row["model_version"],
            training_data_until=row["training_data_until"],
            metrics=metrics,
            status=row["status"],
            created_at=row["created_at"],
        )

    # ---------------------------------------------------------------------
    # MODEL REGISTRY
    # ---------------------------------------------------------------------

    async def list_models(
        self,
        model_type: Optional[str] = None,
        status: Optional[str] = None,
    ) -> List[MLModelResponse]:
        query = """
            SELECT model_sk,
                   model_name,
                   model_type,
                   model_version,
                   training_data_until,
                   metrics,
                   status,
                   created_at
            FROM ml.dim_ml_model
            WHERE 1=1
        """
        params = []
        if model_type:
            query += " AND model_type = $%d" % (len(params) + 1)
            params.append(model_type)
        if status:
            query += " AND status = $%d" % (len(params) + 1)
            params.append(status)

        query += " ORDER BY created_at DESC"

        rows = await self.db.fetch(query, *params)
        result: List[MLModelResponse] = []
        for r in rows:
            metrics = r["metrics"]
            if isinstance(metrics, str):
                metrics = json.loads(metrics)
            result.append(
                MLModelResponse(
                    model_sk=r["model_sk"],
                    model_name=r["model_name"],
                    model_type=r["model_type"],
                    model_version=r["model_version"],
                    training_data_until=r["training_data_until"],
                    metrics=metrics,
                    status=r["status"],
                    created_at=r["created_at"],
                )
            )
        return result

    async def create_model(self, payload: MLModelCreate) -> MLModelResponse:
        query = """
            INSERT INTO ml.dim_ml_model (
                model_name, model_type, model_version,
                training_data_until, metrics, status
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING model_sk, created_at
        """

        metrics_json: Optional[Dict[str, Any]] = payload.metrics or None

        row = await self.db.fetchrow(
            query,
            payload.model_name,
            payload.model_type,
            payload.model_version,
            payload.training_data_until,
            metrics_json,
            payload.status,
        )

        return MLModelResponse(
            model_sk=row["model_sk"],
            model_name=payload.model_name,
            model_type=payload.model_type,
            model_version=payload.model_version,
            training_data_until=payload.training_data_until,
            metrics=payload.metrics,
            status=payload.status,
            created_at=row["created_at"],
        )

    async def get_model(self, model_sk: int) -> Optional[MLModelResponse]:
        query = """
            SELECT model_sk,
                   model_name,
                   model_type,
                   model_version,
                   training_data_until,
                   metrics,
                   status,
                   created_at
            FROM ml.dim_ml_model
            WHERE model_sk = $1
        """
        r = await self.db.fetchrow(query, model_sk)
        if not r:
            return None

        metrics = r["metrics"]
        if isinstance(metrics, str):
            metrics = json.loads(metrics)

        return MLModelResponse(
            model_sk=r["model_sk"],
            model_name=r["model_name"],
            model_type=r["model_type"],
            model_version=r["model_version"],
            training_data_until=r["training_data_until"],
            metrics=metrics,
            status=r["status"],
            created_at=r["created_at"],
        )

    async def update_model(self, model_sk: int, payload: MLModelUpdate) -> Optional[MLModelResponse]:
        # Build dynamic update
        fields = []
        params = []
        if payload.training_data_until is not None:
            fields.append(f"training_data_until = ${len(params) + 1}")
            params.append(payload.training_data_until)

        if payload.metrics is not None:
            fields.append(f"metrics = ${len(params) + 1}")
            params.append(payload.metrics)

        if payload.status is not None:
            fields.append(f"status = ${len(params) + 1}")
            params.append(payload.status)

        if not fields:
            # nothing to update
            return await self.get_model(model_sk)

        query = f"""
            UPDATE ml.dim_ml_model
            SET {', '.join(fields)}
            WHERE model_sk = ${len(params) + 1}
            RETURNING model_sk, model_name, model_type, model_version,
                      training_data_until, metrics, status, created_at
        """
        params.append(model_sk)

        r = await self.db.fetchrow(query, *params)
        if not r:
            return None

        metrics = r["metrics"]
        if isinstance(metrics, str):
            metrics = json.loads(metrics)

        return MLModelResponse(
            model_sk=r["model_sk"],
            model_name=r["model_name"],
            model_type=r["model_type"],
            model_version=r["model_version"],
            training_data_until=r["training_data_until"],
            metrics=metrics,
            status=r["status"],
            created_at=r["created_at"],
        )

    # ---------------------------------------------------------------------
    # PRICE PREDICTION HISTORY
    # ---------------------------------------------------------------------

    async def get_price_prediction_history(
        self,
        product_key: str,
        platform_code: str,
        from_date: date,
        to_date: date,
        model_name: Optional[str] = None,
        model_version: Optional[str] = None,
    ) -> PricePredictionHistoryResponse:
        query = """
            SELECT
                d.date_value AS date,
                dp.product_key,
                dp.product_name,
                pl.platform_code,
                m.model_name,
                m.model_version,
                fp.predicted_price,
                fp.ci_lower,
                fp.ci_upper,
                fp.run_id
            FROM ml.fact_price_prediction fp
            JOIN ml.dim_ml_model m ON m.model_sk = fp.model_sk
            JOIN dwh.dim_date d ON d.date_sk = fp.date_sk
            JOIN dwh.dim_product dp ON dp.product_sk = fp.product_sk
            JOIN dwh.dim_platform pl ON pl.platform_sk = fp.platform_sk
            WHERE dp.product_key = $1
              AND pl.platform_code = $2
              AND d.date_value BETWEEN $3 AND $4
        """

        params = [product_key, platform_code, from_date, to_date]

        if model_name:
            query += f" AND m.model_name = ${len(params) + 1}"
            params.append(model_name)

        if model_version:
            query += f" AND m.model_version = ${len(params) + 1}"
            params.append(model_version)

        query += " ORDER BY d.date_value"

        rows = await self.db.fetch(query, *params)

        points: List[PricePredictionPoint] = []
        for r in rows:
            points.append(
                PricePredictionPoint(
                    date=r["date"],
                    platform_code=r["platform_code"],
                    product_key=r["product_key"],
                    product_name=r["product_name"],
                    model_name=r["model_name"],
                    model_version=r["model_version"],
                    predicted_price=float(r["predicted_price"]),
                    ci_lower=float(r["ci_lower"]) if r["ci_lower"] is not None else None,
                    ci_upper=float(r["ci_upper"]) if r["ci_upper"] is not None else None,
                    run_id=r["run_id"],
                )
            )

        model_name_used = model_name or (points[0].model_name if points else None)
        model_version_used = model_version or (points[0].model_version if points else None)

        return PricePredictionHistoryResponse(
            product_key=product_key,
            platform_code=platform_code,
            model_name=model_name_used,
            model_version=model_version_used,
            points=points,
        )

    # ---------------------------------------------------------------------
    # ONLINE PRICE PREDICTION (stub – tuỳ anh nối vào MLPredictionService)
    # ---------------------------------------------------------------------

    async def online_price_prediction(
        self, payload: OnlinePricePredictionRequest
    ) -> OnlinePricePredictionResponse:
        """
        Online price prediction sử dụng model thật từ registry.
        
        Flow:
        1. Resolve model từ ml.dim_ml_model
        2. Lấy features (từ payload hoặc query DWH)
        3. Load model từ file & predict
        4. Return kết quả với latency thực tế
        """
        import time
        import numpy as np
        from app.ml_runtime import load_price_model
        
        t0 = time.perf_counter()

        # 1) Resolve model từ registry
        model_info = await self._resolve_model(
            model_type="price",
            model_name=payload.model_name,
            model_version=payload.model_version,
        )

        # 2) Chuẩn hoá features
        # Ưu tiên lấy từ payload, nếu thiếu thì query DWH
        current_price = payload.current_price
        avg_rating = payload.avg_rating
        review_count = payload.review_count

        if current_price is None or avg_rating is None or review_count is None:
            # Query snapshot mới nhất từ fact_product_daily
            snap_sql = """
                SELECT fpd.min_price, fpd.max_price,
                       fpd.total_review_count, fpd.avg_rating
                FROM dwh.fact_product_daily fpd
                JOIN dwh.dim_product dp ON dp.product_sk = fpd.product_sk
                JOIN dwh.dim_platform pl ON pl.platform_sk = fpd.platform_sk
                JOIN dwh.dim_date d ON d.date_sk = fpd.date_sk
                WHERE dp.product_key = $1
                  AND pl.platform_code = $2
                ORDER BY d.date_value DESC
                LIMIT 1
            """
            snap = await self.db.fetchrow(
                snap_sql,
                payload.product_key,
                payload.platform_code,
            )
            if not snap:
                raise ValueError(
                    f"No product data found for {payload.product_key} "
                    f"on platform {payload.platform_code}"
                )

            min_price = float(snap["min_price"])
            max_price = float(snap["max_price"])
            total_review_count = int(snap["total_review_count"] or 0)
            avg_rating = float(snap["avg_rating"] or 0.0)
        else:
            # Dùng current_price cho cả min/max (simplified)
            min_price = max_price = float(current_price)
            total_review_count = int(review_count or 0)
            avg_rating = float(avg_rating or 0.0)

        # Tạo feature vector [min_price, max_price, review_count, avg_rating]
        # NOTE: Thứ tự features phải khớp với lúc train model
        X = np.array([[min_price, max_price, total_review_count, avg_rating]])

        # 3) Load model & predict
        model = load_price_model(model_info.model_name, model_info.model_version)
        pred = float(model.predict(X)[0])

        # Optional: CI calculation (nếu model support quantile regression)
        ci_lower = None
        ci_upper = None
        # TODO: Implement CI nếu cần (có thể dùng model.predict với quantile params)

        latency_ms = int((time.perf_counter() - t0) * 1000)

        return OnlinePricePredictionResponse(
            predicted_price=pred,
            ci_lower=ci_lower,
            ci_upper=ci_upper,
            model_name=model_info.model_name,
            model_version=model_info.model_version,
            latency_ms=latency_ms,
        )

    # ---------------------------------------------------------------------
    # RECOMMENDATIONS
    # ---------------------------------------------------------------------

    async def get_recommendations(
        self,
        source_product_key: str,
        platform_code: str,
        model_name: Optional[str] = None,
        model_version: Optional[str] = None,
        limit: int = 10,
    ) -> Optional[RecommendationResponse]:
        """
        Đọc recommendation từ ml.fact_product_recommendation + join dim_product, dim_date.
        Lấy thêm min_price, avg_rating snapshot mới nhất từ dwh.fact_product_daily (LATERAL).
        """

        query = """
            SELECT
                d.date_value AS date,
                src_dp.product_key AS source_product_key,
                split_part(src_dp.product_key, '_', 1) AS platform_code,
                m.model_name,
                m.model_version,
                rnk.rank,
                rec_dp.product_key AS recommended_product_key,
                rec_dp.product_name AS recommended_product_name,
                rnk.similarity_score,
                fpd_snap.min_price,
                fpd_snap.avg_rating
            FROM ml.fact_product_recommendation rnk
            JOIN ml.dim_ml_model m ON m.model_sk = rnk.model_sk
            JOIN dwh.dim_date d ON d.date_sk = rnk.date_sk
            JOIN dwh.dim_product src_dp ON src_dp.product_sk = rnk.source_product_sk
            JOIN dwh.dim_product rec_dp ON rec_dp.product_sk = rnk.recommended_product_sk
            -- lấy snapshot mới nhất trong fact_product_daily cho sản phẩm được recommend
            LEFT JOIN LATERAL (
                SELECT
                    fpd.min_price,
                    fpd.avg_rating
                FROM dwh.fact_product_daily fpd
                WHERE fpd.product_sk = rnk.recommended_product_sk
                ORDER BY fpd.date_sk DESC
                LIMIT 1
            ) AS fpd_snap ON TRUE
            WHERE src_dp.product_key = $1
              AND split_part(src_dp.product_key, '_', 1) = $2
        """

        params: List[Any] = [source_product_key, platform_code]

        if model_name:
            query += f" AND m.model_name = ${len(params) + 1}"
            params.append(model_name)

        if model_version:
            query += f" AND m.model_version = ${len(params) + 1}"
            params.append(model_version)

        query += """
            ORDER BY d.date_value DESC, rnk.rank ASC
            LIMIT $%d
        """ % (len(params) + 1)
        params.append(limit)

        rows = await self.db.fetch(query, *params)
        if not rows:
            return None

        first = rows[0]
        rec_items: List[RecommendationItem] = []
        for r in rows:
            rec_items.append(
                RecommendationItem(
                    rank=r["rank"],
                    recommended_product_key=r["recommended_product_key"],
                    product_name=r["recommended_product_name"],
                    similarity_score=_safe_float(r["similarity_score"]),
                    min_price=_safe_float(r["min_price"]),
                    avg_rating=_safe_float(r["avg_rating"]),
                )
            )

        return RecommendationResponse(
            source_product_key=first["source_product_key"],
            platform_code=first["platform_code"],
            model_name=first["model_name"],
            model_version=first["model_version"],
            date=first["date"],
            recommendations=rec_items,
        )

    
    # ------------------------------------------------------------------
    # SENTIMENT SUMMARY (batch, từ bảng ml.fact_review_sentiment)
    # ------------------------------------------------------------------
    async def get_sentiment_summary(
        self,
        product_key: str,
        platform_code: str,
        from_date: date,
        to_date: date,
        model_name: Optional[str] = None,
        model_version: Optional[str] = None,
    ) -> Optional[SentimentSummaryResponse]:
        """
        Tổng hợp sentiment theo ngày cho 1 sản phẩm.
        Dùng ml.fact_review_sentiment + dwh.dim_date + ml.dim_ml_model.
        """
        # 1) map product_key -> product_sk
        prod_sql = """
            SELECT product_sk
            FROM dwh.dim_product
            WHERE product_key = $1
        """
        prod = await self.db.fetchrow(prod_sql, product_key)
        if not prod:
            return None
        product_sk = prod["product_sk"]

        # 2) tổng hợp theo ngày + lấy model_name / model_version từ fact
        sql = """
            SELECT
                d.date_value AS date,
                COUNT(*) AS total_reviews,
                COUNT(*) FILTER (WHERE s.sentiment_label = 'positive') AS positive,
                COUNT(*) FILTER (WHERE s.sentiment_label = 'negative') AS negative,
                COUNT(*) FILTER (WHERE s.sentiment_label = 'neutral')  AS neutral,
                MAX(m.model_name)   AS model_name,
                MAX(m.model_version) AS model_version
            FROM ml.fact_review_sentiment s
            JOIN dwh.dim_date d    ON d.date_sk = s.date_sk
            JOIN ml.dim_ml_model m ON m.model_sk = s.model_sk
            WHERE s.product_sk = $1
              AND s.platform_code = $2
              AND d.date_value BETWEEN $3 AND $4
        """

        params: List[Any] = [product_sk, platform_code, from_date, to_date]

        if model_name:
            sql += " AND m.model_name = $" + str(len(params) + 1)
            params.append(model_name)
        if model_version:
            sql += " AND m.model_version = $" + str(len(params) + 1)
            params.append(model_version)

        sql += " GROUP BY d.date_value ORDER BY d.date_value"

        rows = await self.db.fetch(sql, *params)
        if not rows:
            return None

        # 3) lấy model_name/model_version từ dòng đầu tiên (đã group)
        first = rows[0]
        resolved_model_name = model_name or first["model_name"] or "sentiment_bert"
        resolved_model_version = model_version or first["model_version"] or "v1.0"

        points: List[SentimentSummaryItem] = []
        for r in rows:
            total = r["total_reviews"]
            positive = r["positive"]
            negative = r["negative"]
            neutral = r["neutral"]

            ratio = float(positive) / total if total and total > 0 else 0.0
            ratio = _safe_float(ratio) or 0.0

            points.append(
                SentimentSummaryItem(
                    date=r["date"],
                    product_key=product_key,
                    platform_code=platform_code,
                    total_reviews=total,
                    positive=positive,
                    negative=negative,
                    neutral=neutral,
                    positive_ratio=ratio,
                )
            )

        return SentimentSummaryResponse(
            product_key=product_key,
            platform_code=platform_code,
            model_name=resolved_model_name,
            model_version=resolved_model_version,
            from_date=from_date,
            to_date=to_date,
            points=points,
        )


    # ------------------------------------------------------------------
    # ONLINE SENTIMENT (stub – sau nối với model thật)
    # ------------------------------------------------------------------
    async def online_sentiment(
        self,
        payload: OnlineSentimentRequest,
    ) -> OnlineSentimentResponse:
        """
        Online sentiment analysis sử dụng model thật từ registry.
        
        Flow:
        1. Resolve model từ ml.dim_ml_model
        2. Load vectorizer + classifier từ file
        3. Transform text → predict_proba
        4. Return label, score, latency
        """
        import time
        import numpy as np
        from app.ml_runtime import load_sentiment_pipeline
        
        t0 = time.perf_counter()

        # 1) Resolve model từ registry
        model_info = await self._resolve_model(
            model_type="sentiment",
            model_name=payload.model_name,
            model_version=payload.model_version,
        )

        # 2) Load pipeline (vectorizer + classifier)
        vectorizer, clf = load_sentiment_pipeline(
            model_info.model_name,
            model_info.model_version,
        )

        # 3) Transform & predict
        X = vectorizer.transform([payload.review_text])
        proba = clf.predict_proba(X)[0]
        
        # Lấy label có xác suất cao nhất
        idx = int(np.argmax(proba))
        label = str(clf.classes_[idx])
        score = float(proba[idx])

        latency_ms = int((time.perf_counter() - t0) * 1000)

        return OnlineSentimentResponse(
            label=label,
            score=score,
            model_name=model_info.model_name,
            model_version=model_info.model_version,
            latency_ms=latency_ms,
        )



    # ---------------------------------------------------------------------
    # STATUS SUMMARY
    # ---------------------------------------------------------------------

    async def get_status_summary(self) -> MLStatusSummary:
        # models count by status
        models_sql = """
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE status = 'active') AS active,
                COUNT(*) FILTER (WHERE status = 'deprecated') AS deprecated,
                COUNT(*) FILTER (WHERE status = 'training') AS training
            FROM ml.dim_ml_model
        """
        models = await self.db.fetchrow(models_sql)

        # last 7 days window
        today = date.today()
        from_date = today - timedelta(days=7)

        preds_sql = """
            SELECT COUNT(*) AS cnt
            FROM ml.fact_price_prediction fp
            JOIN dwh.dim_date d ON d.date_sk = fp.date_sk
            WHERE d.date_value BETWEEN $1 AND $2
        """
        recs_sql = """
            SELECT COUNT(*) AS cnt
            FROM ml.fact_product_recommendation r
            JOIN dwh.dim_date d ON d.date_sk = r.date_sk
            WHERE d.date_value BETWEEN $1 AND $2
        """

        preds = await self.db.fetchrow(preds_sql, from_date, today)
        recs = await self.db.fetchrow(recs_sql, from_date, today)

        return MLStatusSummary(
            models_total=models["total"],
            models_active=models["active"],
            models_deprecated=models["deprecated"],
            models_training=models["training"],
            predictions_last_7_days=preds["cnt"],
            recommendations_last_7_days=recs["cnt"],
        )
