# ml/run_price_predictions.py
import os
import pickle
from datetime import date, datetime

import pandas as pd

from load_ml_results_to_db import get_conn, load_price_predictions


MODEL_DIR = os.getenv("ML_MODEL_DIR", "./models")
MODEL_NAME = "price_forecast_rf"
MODEL_VERSION = "v1.0"


def fetch_scoring_data(conn):
    """
    Lấy dữ liệu mới nhất để predict.
    Ví dụ: dữ liệu ngày hôm qua.
    Anh có thể đổi thành N ngày gần nhất tuỳ nhu cầu.
    """
    sql = """
    SELECT
        d.date_value,
        p.product_key,
        pl.platform_code,
        fpd.avg_price,
        fpd.min_price,
        fpd.max_price,
        fpd.total_review_count,
        fpd.avg_rating
    FROM dwh.fact_product_daily fpd
    JOIN dwh.dim_date d ON d.date_sk = fpd.date_sk
    JOIN dwh.dim_product p ON p.product_sk = fpd.product_sk
    JOIN dwh.dim_platform pl ON pl.platform_sk = fpd.platform_sk
    WHERE d.date_value = CURRENT_DATE - INTERVAL '1 day'
    """
    return pd.read_sql(sql, conn)


def build_features(df: pd.DataFrame):
    return df[["min_price", "max_price", "total_review_count", "avg_rating"]].fillna(0).astype(float)


def run_batch_predictions():
    conn = get_conn()
    df = fetch_scoring_data(conn)
    if df.empty:
        print("No data to score.")
        return

    X = build_features(df)

    model_path = os.path.join(MODEL_DIR, f"{MODEL_NAME}_{MODEL_VERSION}.pkl")
    with open(model_path, "rb") as f:
        model = pickle.load(f)

    preds = model.predict(X)

    # Chuẩn bị list dict predictions để load vào DB
    scoring_date = df["date_value"].iloc[0]  # vì query 1 ngày
    results = []
    for (_, row), pred in zip(df.iterrows(), preds):
        results.append(
            {
                "date": scoring_date,
                "product_key": row["product_key"],
                "platform_code": row["platform_code"],
                "predicted_price": float(pred),
                "ci_lower": None,  # nếu anh không tính CI, để None
                "ci_upper": None,
            }
        )

    run_id = f"price_batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    load_price_predictions(
        conn,
        predictions=results,
        model_name=MODEL_NAME,
        model_type="price",
        model_version=MODEL_VERSION,
        run_id=run_id,
        training_data_until=date.today(),   # optional
        metrics=None,                       # hoặc update metrics ở bước train
    )

    conn.close()


if __name__ == "__main__":
    run_batch_predictions()
