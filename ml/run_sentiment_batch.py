# ml/run_sentiment_batch.py

import os
import pickle
from datetime import date
from typing import List, Dict, Any

import numpy as np
import pandas as pd
from psycopg2.extras import execute_batch

from load_ml_results_to_db import get_conn, get_or_create_model

MODEL_NAME = "sentiment_tfidf_logreg"
MODEL_VERSION = "v1.0"
MODEL_DIR = os.getenv("ML_MODEL_DIR", "./models")


def load_model():
    model_path = os.path.join(MODEL_DIR, f"{MODEL_NAME}_{MODEL_VERSION}.pkl")
    if not os.path.exists(model_path):
        raise RuntimeError(
            f"Model file not found: {model_path}. "
            f"Run train_sentiment_model.py first."
        )
    with open(model_path, "rb") as f:
        obj = pickle.load(f)
    vectorizer = obj["vectorizer"]
    clf = obj["model"]
    return vectorizer, clf


def fetch_unscored_reviews(conn, model_sk: int) -> pd.DataFrame:
    """
    Lấy các review chưa có bản ghi trong ml.fact_review_sentiment cho model_sk này.

    Thực tế dùng:
      - dwh.fact_review(review_sk, product_sk, date_sk, rating, review_text)
      - dwh.dim_product(product_sk, product_key)
    """
    sql = """
        SELECT
            r.review_sk AS review_id,
            r.product_sk,
            r.date_sk,
            r.rating,
            r.review_text,
            p.product_key,
            split_part(p.product_key, '_', 1) AS platform_code
        FROM dwh.fact_review r
        JOIN dwh.dim_product p ON p.product_sk = r.product_sk
        LEFT JOIN ml.fact_review_sentiment s
            ON s.review_id = r.review_sk
           AND s.model_sk = %s
        WHERE r.review_text IS NOT NULL
          AND r.rating IS NOT NULL
          AND s.review_id IS NULL
    """
    return pd.read_sql(sql, conn, params=(model_sk,))


def predict_sentiment(vectorizer, clf, texts: pd.Series):
    X = vectorizer.transform(texts.astype(str))
    proba = clf.predict_proba(X)
    labels = clf.classes_[np.argmax(proba, axis=1)]
    scores = np.max(proba, axis=1)
    return labels, scores


def insert_sentiment_rows(conn, rows: List[Dict[str, Any]]):
    if not rows:
        print("[INFO] No sentiment rows to insert.")
        return

    insert_sql = """
        INSERT INTO ml.fact_review_sentiment (
            review_id,
            product_sk,
            platform_code,
            date_sk,
            sentiment_label,
            sentiment_score,
            model_sk
        )
        VALUES (%(review_id)s, %(product_sk)s, %(platform_code)s,
                %(date_sk)s, %(sentiment_label)s, %(sentiment_score)s,
                %(model_sk)s)
    """

    with conn.cursor() as cur:
        execute_batch(cur, insert_sql, rows, page_size=500)
    conn.commit()
    print(f"[INFO] Inserted {len(rows)} rows into ml.fact_review_sentiment")


def main():
    conn = get_conn()

    # Lấy hoặc tạo meta model trong dim_ml_model (nếu chưa có)
    model_sk = get_or_create_model(
        conn,
        model_name=MODEL_NAME,
        model_type="sentiment",
        model_version=MODEL_VERSION,
        training_data_until=date.today(),
        metrics=None,
        status="active",
    )
    print(f"[INFO] Using model_sk = {model_sk} for sentiment batch scoring")

    # Load model từ file
    vectorizer, clf = load_model()

    # Lấy các review chưa được gán sentiment cho model này
    df = fetch_unscored_reviews(conn, model_sk)
    if df.empty:
        print("[INFO] No new reviews to score.")
        conn.close()
        return

    print(f"[INFO] Scoring {len(df)} reviews")

    labels, scores = predict_sentiment(vectorizer, clf, df["review_text"])

    rows: List[Dict[str, Any]] = []
    for i, row in df.iterrows():
        rows.append(
            {
                "review_id": int(row["review_id"]),
                "product_sk": int(row["product_sk"]),
                "platform_code": row["platform_code"],
                "date_sk": int(row["date_sk"]),
                "sentiment_label": str(labels[i]),
                "sentiment_score": float(scores[i]),
                "model_sk": int(model_sk),
            }
        )

    insert_sentiment_rows(conn, rows)
    conn.close()


if __name__ == "__main__":
    main()
