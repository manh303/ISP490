# -*- coding: utf-8 -*-
"""
Train Product Clustering Model (KMeans)
Mô hình phân cụm sản phẩm theo đặc tính (Phân khúc Sản phẩm)
"""

import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import os
import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score, davies_bouldin_score
import joblib
import yaml
from utils.logger import get_logger
from utils.db_connector import DWHConnector

logger = get_logger("product_clustering")

# Load config
with open('config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)


def extract_product_clustering_data():
    """
    Extract product data từ DWH để train clustering model
    """
    logger.info("\n" + "="*60)
    logger.info("EXTRACTING PRODUCT CLUSTERING DATA")
    logger.info("="*60)
    
    conn = DWHConnector()
    
    # Query lấy product features để clustering từ product fact table
    # Cấp độ: Category x Platform x Date
    sql = """
    SELECT 
        fpd.category_std,
        fpd.source_platform_std,
        fpd.agg_date,
        fpd.avg_price,
        fpd.min_price,
        fpd.max_price,
        fpd.total_review_count,
        fpd.distinct_products
    FROM dwh.fact_product_daily_agg fpd
    WHERE fpd.agg_date >= CURRENT_DATE - INTERVAL '90 days'
    AND fpd.total_review_count > 0
    ORDER BY fpd.category_std, fpd.source_platform_std, fpd.agg_date DESC
    LIMIT 5000
    """
    
    try:
        df = conn.query(sql)
        logger.info(f"[OK] Total records: {len(df)}")
        logger.info(f"[OK] Categories: {df['category_std'].nunique()}")
        logger.info(f"[OK] Platforms: {df['source_platform_std'].nunique()}")
        
        # Group by category and platform để tạo segment-level aggregates
        df_agg = df.groupby(['category_std', 'source_platform_std']).agg({
            'avg_price': 'mean',
            'min_price': 'min',
            'max_price': 'max',
            'total_review_count': 'sum',
            'distinct_products': 'mean',
            'agg_date': 'count'  # Days active
        }).reset_index()
        df_agg.rename(columns={'agg_date': 'active_days'}, inplace=True)
        
        logger.info(f"[OK] Aggregated to: {len(df_agg)} category-platform combinations")
        
        # Data quality checks
        logger.info("\nData Quality Checks:")
        logger.info(f"  Missing avg_price: {df_agg['avg_price'].isna().sum()}")
        logger.info(f"  Missing distinct_products: {df_agg['distinct_products'].isna().sum()}")
        logger.info(f"  Avg reviews per segment: {df_agg['total_review_count'].mean():.0f}")
        
        # Save raw data
        output_dir = Path(config['data_extraction']['product_clustering']['output_dir'])
        output_dir.mkdir(parents=True, exist_ok=True)
        
        df_agg.to_csv(output_dir / 'raw_clustering_data.csv', index=False)
        logger.info(f"[OK] Saved to {output_dir / 'raw_clustering_data.csv'}")
        
        return df_agg
    
    except Exception as e:
        logger.error(f"[ERROR] Error extracting clustering data: {e}")
        raise
    
    finally:
        conn.close()


def prepare_clustering_features(df):
    """
    Prepare features cho product clustering
    """
    logger.info("\n" + "="*60)
    logger.info("PREPARING CLUSTERING FEATURES")
    logger.info("="*60)
    
    df = df.copy()
    
    # Fill missing values
    df['avg_price'].fillna(df['avg_price'].median(), inplace=True)
    df['min_price'].fillna(0, inplace=True)
    df['max_price'].fillna(df['avg_price'], inplace=True)
    df['distinct_products'].fillna(0, inplace=True)
    
    # Calculate additional features
    df['price_range'] = df['max_price'] - df['min_price']
    df['avg_product_reviews'] = df['total_review_count'] / (df['distinct_products'] + 1)  # Avoid division by zero
    df['engagement_score'] = df['total_review_count'] / (df['active_days'] + 1)  # Avoid division by zero
    
    # Log transformation for price (để cân bằng scale)
    df['log_avg_price'] = np.log1p(df['avg_price'])
    df['log_engagement'] = np.log1p(df['engagement_score'])
    df['log_product_reviews'] = np.log1p(df['avg_product_reviews'])
    
    # Select features for clustering
    feature_cols = [
        'log_avg_price',
        'price_range',
        'distinct_products',
        'log_engagement',
        'log_product_reviews'
    ]
    
    logger.info(f"[OK] Features selected for clustering: {feature_cols}")
    logger.info(f"\nFeature statistics:")
    logger.info(df[feature_cols].describe())
    
    return df, feature_cols


def find_optimal_clusters(X):
    """
    Find optimal number of clusters using Elbow method and Silhouette
    """
    logger.info("\n" + "="*60)
    logger.info("FINDING OPTIMAL NUMBER OF CLUSTERS")
    logger.info("="*60)
    
    inertias = []
    silhouette_scores = []
    davies_bouldin_scores = []
    K_range = range(2, 11)
    
    for k in K_range:
        logger.info(f"\nTesting K={k}...")
        kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
        labels = kmeans.fit_predict(X)
        
        inertia = kmeans.inertia_
        silhouette = silhouette_score(X, labels)
        davies_bouldin = davies_bouldin_score(X, labels)
        
        inertias.append(inertia)
        silhouette_scores.append(silhouette)
        davies_bouldin_scores.append(davies_bouldin)
        
        logger.info(f"  Inertia: {inertia:.2f}")
        logger.info(f"  Silhouette Score: {silhouette:.4f}")
        logger.info(f"  Davies-Bouldin Index: {davies_bouldin:.4f}")
    
    # Find best K based on Silhouette score (higher is better)
    best_k = K_range[np.argmax(silhouette_scores)]
    logger.info(f"\n[OK] Optimal K = {best_k} (based on Silhouette score)")
    
    return best_k


def train_clustering_model(X, scaler):
    """
    Train KMeans clustering model
    """
    logger.info("\n" + "="*60)
    logger.info("TRAINING CLUSTERING MODEL")
    logger.info("="*60)
    
    # Find optimal clusters
    optimal_k = find_optimal_clusters(X)
    
    # Train final model with optimal K
    logger.info(f"\n[Step 2] Training KMeans with K={optimal_k}...")
    model = KMeans(n_clusters=optimal_k, random_state=42, n_init=10)
    labels = model.fit_predict(X)
    
    # Evaluate
    logger.info("\n[Step 3] Evaluating Model...")
    silhouette = silhouette_score(X, labels)
    davies_bouldin = davies_bouldin_score(X, labels)
    
    logger.info(f"[OK] Model trained!")
    logger.info(f"  Silhouette Score: {silhouette:.4f}")
    logger.info(f"  Davies-Bouldin Index: {davies_bouldin:.4f}")
    logger.info(f"  Inertia: {model.inertia_:.2f}")
    
    # Cluster distribution
    logger.info(f"\nCluster Distribution:")
    unique, counts = np.unique(labels, return_counts=True)
    for cluster_id, count in zip(unique, counts):
        pct = (count / len(labels)) * 100
        logger.info(f"  Cluster {cluster_id}: {count} products ({pct:.1f}%)")
    
    metrics = {
        'optimal_k': int(optimal_k),
        'silhouette_score': float(silhouette),
        'davies_bouldin_index': float(davies_bouldin),
        'inertia': float(model.inertia_)
    }
    
    return model, labels, metrics


def save_clustering_model(model, scaler, feature_cols, metrics):
    """
    Save trained clustering model
    """
    logger.info("\n" + "="*60)
    logger.info("SAVING CLUSTERING MODEL")
    logger.info("="*60)
    
    models_dir = Path(config['models']['output_dir'])
    models_dir.mkdir(parents=True, exist_ok=True)
    
    # Save model
    model_path = models_dir / "recommendation_kmeans.pkl"
    joblib.dump(model, model_path)
    logger.info(f"[OK] Model saved: {model_path}")
    
    # Save scaler
    scaler_path = models_dir / "clustering_scaler.pkl"
    joblib.dump(scaler, scaler_path)
    logger.info(f"[OK] Scaler saved: {scaler_path}")
    
    # Save feature columns
    feature_path = models_dir / "clustering_features.pkl"
    joblib.dump(feature_cols, feature_path)
    logger.info(f"[OK] Feature columns saved: {feature_path}")
    
    # Save metrics
    metrics_path = models_dir.parent / "logs" / "clustering_metrics.json"
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    
    import json
    with open(metrics_path, 'w') as f:
        json.dump(metrics, f, indent=2)
    logger.info(f"[OK] Metrics saved: {metrics_path}")
    
    logger.info(f"\n[OK] Product clustering training COMPLETED!")


if __name__ == "__main__":
    try:
        logger.info("[ML PIPELINE] Product Clustering Training")
        
        # Extract data
        clustering_df = extract_product_clustering_data()
        
        # Prepare features
        clustering_df, feature_cols = prepare_clustering_features(clustering_df)
        
        # Scale features
        logger.info("\nScaling features...")
        X = clustering_df[feature_cols].values
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        logger.info(f"[OK] Features scaled: {X_scaled.shape}")
        
        # Train model
        model, labels, metrics = train_clustering_model(X_scaled, scaler)
        
        # Save model
        save_clustering_model(model, scaler, feature_cols, metrics)
        
        logger.info("\n" + "="*60)
        logger.info("[SUCCESS] PRODUCT CLUSTERING TRAINING COMPLETED")
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"\n[FAILED] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
