# ml/train_recommender.py
import os
import pickle
from datetime import date

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer

from load_ml_results_to_db import get_conn, get_or_create_model

MODEL_DIR = os.getenv("ML_MODEL_DIR", "./models")
MODEL_NAME = "content_recommender_tfidf"
MODEL_VERSION = "v1.0"


def fetch_products_for_training(conn) -> pd.DataFrame:
    """
    Lấy danh sách sản phẩm + brand + category từ DWH
    để train vectorizer content-based.
    Platform_code được suy ra từ prefix của product_key (vd: 'tiki', 'lazada').
    """
    sql = """
    SELECT
        p.product_sk,
        p.product_key,
        p.product_name,
        COALESCE(b.brand_name, '')       AS brand_name,
        COALESCE(c.full_path, '')        AS category_path,
        split_part(p.product_key, '_', 1) AS platform_code
    FROM dwh.dim_product p
    LEFT JOIN dwh.dim_brand b ON b.brand_sk = p.brand_sk
    LEFT JOIN dwh.dim_category c ON c.category_sk = p.category_sk
    WHERE p.product_key IS NOT NULL
      AND p.product_name IS NOT NULL
      AND split_part(p.product_key, '_', 1) IN ('tiki', 'lazada') 
    """
    return pd.read_sql(sql, conn)


def build_text_column(df: pd.DataFrame) -> pd.Series:
    """
    Ghép product_name + brand + category path + platform_code
    thành 1 text để vectorizer xử lý.
    """
    return (
        df["product_name"].fillna("")
        + " [BRAND] " + df["brand_name"].fillna("")
        + " [CATEGORY] " + df["category_path"].fillna("")
        + " [PLATFORM] " + df["platform_code"].fillna("")
    )


def train_vectorizer():
    conn = get_conn()
    df = fetch_products_for_training(conn)
    if df.empty:
        print("No products found for training recommender.")
        conn.close()
        return

    text_series = build_text_column(df)

    vectorizer = TfidfVectorizer(
        max_features=5000,
        ngram_range=(1, 2),
        min_df=2
    )
    vectorizer.fit(text_series)

    os.makedirs(MODEL_DIR, exist_ok=True)
    model_path = os.path.join(MODEL_DIR, f"{MODEL_NAME}_{MODEL_VERSION}.pkl")

    with open(model_path, "wb") as f:
        pickle.dump(
            {
                "vectorizer": vectorizer,
                "trained_on_date": date.today(),
                "note": "Content-based TF-IDF recommender on product_name + brand + category + platform"
            },
            f,
        )

    # Đăng ký model vào dim_ml_model
    vocab_size = len(vectorizer.vocabulary_)
    metrics = {"vocab_size": vocab_size}

    model_sk = get_or_create_model(
        conn,
        model_name=MODEL_NAME,
        model_type="recommendation",
        model_version=MODEL_VERSION,
        training_data_until=date.today(),
        metrics=metrics,
        status="active",
    )

    print(f"[INFO] Trained TF-IDF recommender. Vocab size = {vocab_size}, model_sk = {model_sk}")
    print(f"[INFO] Saved model file to: {model_path}")

    conn.close()


if __name__ == "__main__":
    train_vectorizer()
