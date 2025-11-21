# ml/train_price_model.py
import os
import pickle
from datetime import date

import psycopg2
import pandas as pd
from psycopg2.extras import RealDictCursor
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error

from load_ml_results_to_db import get_conn, get_or_create_model  # file em viết trước đó


MODEL_DIR = os.getenv("ML_MODEL_DIR", "./models")


def fetch_training_data(conn):
    """
    Lấy dữ liệu train từ DWH.
    Ví dụ: giá, rating, review_count trong 90 ngày gần nhất.
    Anh chỉnh SQL này theo fact_product_daily của anh.
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
    WHERE d.date_value >= CURRENT_DATE - INTERVAL '90 days'
      AND fpd.avg_price IS NOT NULL
    """
    return pd.read_sql(sql, conn)


def build_features(df: pd.DataFrame):
    """
    Tạo features X, y cho model giá.
    Ở đây chỉ chơi đơn giản để minh hoạ.
    """
    # target: avg_price
    y = df["avg_price"].astype(float)

    # features: min_price, max_price, total_review_count, avg_rating
    X = df[["min_price", "max_price", "total_review_count", "avg_rating"]].fillna(0).astype(float)

    return X, y


def train_and_save_model():
    conn = get_conn()
    df = fetch_training_data(conn)
    if df.empty:
        print("No training data found.")
        return

    X, y = build_features(df)
    X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)

    model = RandomForestRegressor(
        n_estimators=200,
        max_depth=10,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train, y_train)

    y_pred = model.predict(X_val)
    mae = float(mean_absolute_error(y_val, y_pred))
    rmse = float((( (y_val - y_pred) ** 2 ).mean()) ** 0.5)

    print(f"MAE: {mae:.2f}, RMSE: {rmse:.2f}")

    os.makedirs(MODEL_DIR, exist_ok=True)
    model_name = "price_forecast_rf"
    model_version = "v1.0"
    model_path = os.path.join(MODEL_DIR, f"{model_name}_{model_version}.pkl")

    with open(model_path, "wb") as f:
        pickle.dump(model, f)

    # Đăng ký vào dim_ml_model
    training_data_until = date.today()
    metrics = {"mae": mae, "rmse": rmse}

    model_sk = get_or_create_model(
        conn,
        model_name=model_name,
        model_type="price",
        model_version=model_version,
        training_data_until=training_data_until,
        metrics=metrics,
        status="active",
    )
    print(f"Registered model {model_name} v{model_version} with model_sk={model_sk}")

    conn.close()


if __name__ == "__main__":
    train_and_save_model()
