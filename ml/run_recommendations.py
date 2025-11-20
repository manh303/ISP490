# ml/run_recommendations.py
import os
import pickle
from datetime import date
from typing import List, Dict, Any

import pandas as pd
from sklearn.neighbors import NearestNeighbors

from load_ml_results_to_db import get_conn, load_recommendations

MODEL_DIR = os.getenv("ML_MODEL_DIR", "./models")
MODEL_NAME = "content_recommender_tfidf"
MODEL_VERSION = "v1.0"
TOP_K = 10  # mỗi sản phẩm gợi ý tối đa TOP_K sản phẩm


def fetch_products(conn) -> pd.DataFrame:
    """
    Lấy danh sách sản phẩm + brand + category từ DWH
    dùng để sinh recommendation.
    Platform_code suy ra từ product_key.
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
      AND split_part(p.product_key, '_', 1) IN ('tiki', 'lazada') -- tuỳ anh
    """
    return pd.read_sql(sql, conn)


def build_text_column(df: pd.DataFrame) -> pd.Series:
    """
    Cùng logic với train_recommender.py để đảm bảo vectorizer apply đúng.
    """
    return (
        df["product_name"].fillna("")
        + " [BRAND] " + df["brand_name"].fillna("")
        + " [CATEGORY] " + df["category_path"].fillna("")
        + " [PLATFORM] " + df["platform_code"].fillna("")
    )


def build_recommendations_for_platform(
    df_platform: pd.DataFrame,
    vectorizer,
    top_k: int,
    snapshot_date: date,
) -> List[Dict[str, Any]]:

    """
    Sinh recommendation cho 1 platform (tiki hoặc lazada).
    Dùng KNN với cosine similarity trên TF-IDF vector.
    """
    if df_platform.empty:
        return []

    df_platform = df_platform.reset_index(drop=True)
    text_series = build_text_column(df_platform)
    X = vectorizer.transform(text_series)

    # fit KNN trên chính X
    n_neighbors = min(top_k + 1, df_platform.shape[0])  # +1 để bao gồm cả chính nó
    knn = NearestNeighbors(metric="cosine", n_neighbors=n_neighbors, n_jobs=-1)
    knn.fit(X)

    recs: List[Dict[str, Any]] = []

    for i in range(df_platform.shape[0]):
        distances, indices = knn.kneighbors(X[i], n_neighbors=n_neighbors)
        distances = distances[0]
        indices = indices[0]

        rank = 1
        for dist, idx in zip(distances, indices):
            if idx == i:
                # bỏ chính nó
                continue
            if rank > top_k:
                break

            source_key = df_platform.loc[i, "product_key"]
            rec_key = df_platform.loc[idx, "product_key"]

            # similarity = 1 - distance (vì dùng cosine distance)
            similarity = 1.0 - float(dist)

            recs.append(
                {
                    "date": snapshot_date,
                    "source_product_key": source_key,
                    "recommended_product_key": rec_key,
                    "rank": rank,
                    "similarity_score": similarity,
                }
            )
            rank += 1

    return recs


def main():
    conn = get_conn()

    with conn.cursor() as cur:
        cur.execute("SELECT MAX(date_value) FROM dwh.dim_date")
        row = cur.fetchone()
        snapshot_date = row[0]

    if snapshot_date is None:
        print("No dates found in dwh.dim_date, cannot generate recommendations.")
        conn.close()
        return

    print(f"[INFO] Using snapshot_date = {snapshot_date} for recommendations")

    # 1) Load vectorizer đã train
    model_path = os.path.join(MODEL_DIR, f"{MODEL_NAME}_{MODEL_VERSION}.pkl")
    if not os.path.exists(model_path):
        raise RuntimeError(f"Model file not found: {model_path}. Run train_recommender.py first.")

    with open(model_path, "rb") as f:
        model_obj = pickle.load(f)
    vectorizer = model_obj["vectorizer"]

    # 2) Lấy sản phẩm từ DWH
    df = fetch_products(conn)
    if df.empty:
        print("No products found for generating recommendations.")
        conn.close()
        return

    # 3) Sinh recommendation theo từng platform (để tránh cross-platform recs)
    all_recs: List[Dict[str, Any]] = []
    for platform_code, df_plat in df.groupby("platform_code"):
        print(f"[INFO] Generating recommendations for platform: {platform_code}, n_products={len(df_plat)}")
        recs_plat = build_recommendations_for_platform(
            df_platform=df_plat,
            vectorizer=vectorizer,
            top_k=TOP_K,
            snapshot_date=snapshot_date,
        )
        all_recs.extend(recs_plat)

    if not all_recs:
        print("No recommendations generated.")
        conn.close()
        return

    # 4) Đẩy vào ml.fact_product_recommendation
    from datetime import date as _date
    load_recommendations(
        conn,
        recommendations=all_recs,
        model_name=MODEL_NAME,
        model_type="recommendation",
        model_version=MODEL_VERSION,
        recommendation_type="content_based_tfidf",
        training_data_until=_date.today(),
        metrics=None,  # có thể update metrics sau nếu anh đo được
    )

    conn.close()


if __name__ == "__main__":
    main()
