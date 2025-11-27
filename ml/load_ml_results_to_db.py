# ml/load_ml_results_to_db.py
import os
from datetime import datetime, date
from typing import List, Dict, Any, Optional

import psycopg2
from psycopg2.extras import execute_batch, RealDictCursor, Json



DB_DSN = os.getenv("DATABASE_URL", "postgresql://dss_user:6wYnk8sndEjkzvOt4LS8sI1beTwdMc6G@dpg-d4j17gn5r7bs73bsoqm0-a.singapore-postgres.render.com/ecommerce_dss_1")


def get_conn():
    return psycopg2.connect(DB_DSN)


# -------------------------------------------------------------------
# Helper: lấy hoặc tạo model_sk trong ml.dim_ml_model
# -------------------------------------------------------------------
def get_or_create_model(
    conn,
    model_name: str,
    model_type: str,
    model_version: str,
    training_data_until: Optional[date] = None,
    metrics: Optional[Dict[str, Any]] = None,
    status: str = "active",
) -> int:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT model_sk
            FROM ml.dim_ml_model
            WHERE model_name = %s AND model_version = %s
            """,
            (model_name, model_version),
        )
        row = cur.fetchone()
        if row:
            return row["model_sk"]

        # 🔹 CHỖ QUAN TRỌNG: wrap dict thành Json
        metrics_json = Json(metrics) if metrics is not None else None

        cur.execute(
            """
            INSERT INTO ml.dim_ml_model (
                model_name, model_type, model_version,
                training_data_until, metrics, status
            )
            VALUES (%s,%s,%s,%s,%s,%s)
            RETURNING model_sk
            """,
            (model_name, model_type, model_version, training_data_until, metrics_json, status),
        )
        model_sk = cur.fetchone()["model_sk"]
        conn.commit()
        return model_sk


# -------------------------------------------------------------------
# Helper: build mapping key -> surrogate key từ DWH
# -------------------------------------------------------------------
def load_dimension_mappings(conn):
    with conn.cursor() as cur:
        # date_value -> date_sk
        cur.execute("SELECT date_sk, date_value FROM dwh.dim_date")
        date_map = {row[1]: row[0] for row in cur.fetchall()}

        # product_key -> product_sk
        cur.execute("SELECT product_sk, product_key FROM dwh.dim_product")
        product_map = {row[1]: row[0] for row in cur.fetchall()}

        # platform_code -> platform_sk
        cur.execute("SELECT platform_sk, platform_code FROM dwh.dim_platform")
        platform_map = {row[1]: row[0] for row in cur.fetchall()}

    return date_map, product_map, platform_map


# -------------------------------------------------------------------
# 1) LOAD PRICE PREDICTIONS INTO ml.fact_price_prediction
# -------------------------------------------------------------------
def load_price_predictions(
    conn,
    predictions: List[Dict[str, Any]],
    model_name: str,
    model_type: str,
    model_version: str,
    run_id: str,
    training_data_until: Optional[date] = None,
    metrics: Optional[Dict[str, Any]] = None,
):
    """
    predictions: list of dicts:
      {
        "date": date or "YYYY-MM-DD",
        "product_key": "tiki_123456",
        "platform_code": "tiki",
        "predicted_price": 1500000.0,
        "ci_lower": 1350000.0,   # optional
        "ci_upper": 1650000.0,   # optional
      }
    """

    model_sk = get_or_create_model(
        conn,
        model_name=model_name,
        model_type=model_type,
        model_version=model_version,
        training_data_until=training_data_until,
        metrics=metrics,
        status="active",
    )

    date_map, product_map, platform_map = load_dimension_mappings(conn)

    rows = []
    for p in predictions:
        # parse date
        d = p["date"]
        if isinstance(d, str):
            d = datetime.strptime(d, "%Y-%m-%d").date()

        date_sk = date_map.get(d)
        product_sk = product_map.get(p["product_key"])
        platform_sk = platform_map.get(p["platform_code"])

        if date_sk is None or product_sk is None or platform_sk is None:
            # skip bản ghi không map được
            continue

        predicted_price = float(p["predicted_price"])
        ci_lower = float(p["ci_lower"]) if p.get("ci_lower") is not None else None
        ci_upper = float(p["ci_upper"]) if p.get("ci_upper") is not None else None

        rows.append(
            (
                model_sk,
                date_sk,
                product_sk,
                platform_sk,
                predicted_price,
                ci_lower,
                ci_upper,
                run_id,
            )
        )

    if not rows:
        print("No valid prediction rows to insert.")
        return

    with conn.cursor() as cur:
        execute_batch(
            cur,
            """
            INSERT INTO ml.fact_price_prediction (
                model_sk, date_sk, product_sk, platform_sk,
                predicted_price, ci_lower, ci_upper, run_id
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            rows,
            page_size=1000,
        )
    conn.commit()
    print(f"Inserted {len(rows)} rows into ml.fact_price_prediction")


# -------------------------------------------------------------------
# 2) LOAD RECOMMENDATIONS INTO ml.fact_product_recommendation
# -------------------------------------------------------------------
def load_recommendations(
    conn,
    recommendations: List[Dict[str, Any]],
    model_name: str,
    model_type: str,
    model_version: str,
    recommendation_type: str,
    training_data_until: Optional[date] = None,
    metrics: Optional[Dict[str, Any]] = None,
):
    """
    recommendations: list of dicts:
      {
        "date": date or "YYYY-MM-DD",
        "source_product_key": "tiki_123456",
        "recommended_product_key": "tiki_789000",
        "rank": 1,
        "similarity_score": 0.9231
      }
    """
    model_sk = get_or_create_model(
        conn,
        model_name=model_name,
        model_type=model_type,
        model_version=model_version,
        training_data_until=training_data_until,
        metrics=metrics,
        status="active",
    )

    date_map, product_map, _ = load_dimension_mappings(conn)

    rows = []
    for rec in recommendations:
        d = rec["date"]
        if isinstance(d, str):
            d = datetime.strptime(d, "%Y-%m-%d").date()

        date_sk = date_map.get(d)
        source_sk = product_map.get(rec["source_product_key"])
        rec_sk = product_map.get(rec["recommended_product_key"])

        if date_sk is None or source_sk is None or rec_sk is None:
            continue

        rank = int(rec["rank"])
        similarity_score = float(rec["similarity_score"]) if rec.get("similarity_score") is not None else None

        rows.append(
            (
                model_sk,
                date_sk,
                source_sk,
                rec_sk,
                rank,
                similarity_score,
                recommendation_type,
            )
        )

    if not rows:
        print("No valid recommendation rows to insert.")
        return

    with conn.cursor() as cur:
        execute_batch(
            cur,
            """
            INSERT INTO ml.fact_product_recommendation (
                model_sk, date_sk,
                source_product_sk, recommended_product_sk,
                rank, similarity_score, recommendation_type
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            """,
            rows,
            page_size=1000,
        )
    conn.commit()
    print(f"Inserted {len(rows)} rows into ml.fact_product_recommendation")


# -------------------------------------------------------------------
# Ví dụ cách dùng (anh có thể xoá phần này nếu dùng từ Airflow)
# -------------------------------------------------------------------
if __name__ == "__main__":
    conn = get_conn()

    # Ví dụ giả lập predictions
    sample_predictions = [
        {
            "date": "2025-11-19",
            "product_key": "tiki_123456",
            "platform_code": "tiki",
            "predicted_price": 1500000,
            "ci_lower": 1350000,
            "ci_upper": 1650000,
        }
    ]

    load_price_predictions(
        conn,
        predictions=sample_predictions,
        model_name="price_forecast_xgb",
        model_type="price",
        model_version="v1.0",
        run_id="price_batch_2025-11-19",
        training_data_until=date(2025, 11, 18),
        metrics={"rmse": 120000, "mae": 90000},
    )

    # Ví dụ giả lập recommendations
    sample_recs = [
        {
            "date": "2025-11-19",
            "source_product_key": "tiki_123456",
            "recommended_product_key": "tiki_789000",
            "rank": 1,
            "similarity_score": 0.9231,
        }
    ]

    load_recommendations(
        conn,
        recommendations=sample_recs,
        model_name="knn_recommender",
        model_type="recommendation",
        model_version="v1.0",
        recommendation_type="content_based",
        training_data_until=date(2025, 11, 18),
        metrics={"topk_precision": 0.42},
    )

    conn.close()
