# ml/train_sentiment_model.py

import os
import pickle
from datetime import date
from typing import Dict, Any

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report

from load_ml_results_to_db import get_conn, get_or_create_model

MODEL_NAME = "sentiment_tfidf_logreg"
MODEL_VERSION = "v1.0"
MODEL_DIR = os.getenv("ML_MODEL_DIR", "./models")


def fetch_training_data(conn) -> pd.DataFrame:
    """
    Lấy dữ liệu review từ DWH để train sentiment.

    Thực tế dùng:
        dwh.fact_review(review_sk, product_sk, date_sk, rating, review_text)
    → alias review_sk AS review_id cho dễ xử lý về sau.
    """
    sql = """
        SELECT
            r.review_sk AS review_id,
            r.product_sk,
            r.date_sk,
            r.rating,
            r.review_text
        FROM dwh.fact_review r
        WHERE r.review_text IS NOT NULL
          AND r.rating IS NOT NULL
    """
    return pd.read_sql(sql, conn)



def build_labels_from_rating(df: pd.DataFrame) -> pd.Series:
    """
    Quy tắc gán nhãn:
      - rating >= 4  -> positive
      - rating <= 2  -> negative
      - còn lại      -> neutral
    """
    def to_label(r):
        if r >= 4:
            return "positive"
        elif r <= 2:
            return "negative"
        else:
            return "neutral"

    return df["rating"].astype(int).apply(to_label)


def train_and_save_model() -> None:
    conn = get_conn()
    df = fetch_training_data(conn)

    if df.empty:
        print("[ERROR] No training data found in dwh.fact_review")
        return

    print(f"[INFO] Loaded {len(df)} reviews for training sentiment model")

    df = df.dropna(subset=["review_text"])
    texts = df["review_text"].astype(str)
    labels = build_labels_from_rating(df)

    # Train / test split đơn giản
    # (nếu muốn anh có thể dùng train_test_split)
    n = len(df)
    split_idx = int(n * 0.8)
    X_train_text = texts.iloc[:split_idx]
    y_train = labels.iloc[:split_idx]
    X_test_text = texts.iloc[split_idx:]
    y_test = labels.iloc[split_idx:]

    # TF-IDF vectorizer
    vectorizer = TfidfVectorizer(
        max_features=10000,
        ngram_range=(1, 2),
        min_df=5,
    )
    X_train = vectorizer.fit_transform(X_train_text)
    X_test = vectorizer.transform(X_test_text)

    # Logistic Regression multi-class
    clf = LogisticRegression(
        max_iter=1000,
        n_jobs=-1,
        multi_class="auto",
    )
    clf.fit(X_train, y_train)

    # Eval
    y_pred = clf.predict(X_test)
    acc = accuracy_score(y_test, y_pred)
    print(f"[INFO] Sentiment model accuracy = {acc:.4f}")
    print("[INFO] Classification report:")
    print(classification_report(y_test, y_pred))

    # Lưu model + vectorizer
    os.makedirs(MODEL_DIR, exist_ok=True)
    model_path = os.path.join(MODEL_DIR, f"{MODEL_NAME}_{MODEL_VERSION}.pkl")

    with open(model_path, "wb") as f:
        pickle.dump(
            {
                "vectorizer": vectorizer,
                "model": clf,
                "trained_on_date": date.today(),
            },
            f,
        )

    print(f"[INFO] Saved sentiment model to {model_path}")

    # Đăng ký model vào dim_ml_model
    metrics: Dict[str, Any] = {"accuracy": float(acc)}

    model_sk = get_or_create_model(
        conn,
        model_name=MODEL_NAME,
        model_type="sentiment",  # anh có thể dùng "classification" nếu muốn
        model_version=MODEL_VERSION,
        training_data_until=date.today(),
        metrics=metrics,
        status="active",
    )

    print(f"[INFO] Registered model in ml.dim_ml_model with model_sk = {model_sk}")

    conn.close()


if __name__ == "__main__":
    train_and_save_model()
