# -*- coding: utf-8 -*-
"""
Evaluation Metrics
"""

import numpy as np
from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    r2_score,
    silhouette_score,
    davies_bouldin_score
)
import logging

logger = logging.getLogger(__name__)


class RegressionMetrics:
    """Regression evaluation metrics"""
    
    @staticmethod
    def mae(y_true, y_pred):
        """Mean Absolute Error"""
        return mean_absolute_error(y_true, y_pred)
    
    @staticmethod
    def rmse(y_true, y_pred):
        """Root Mean Squared Error"""
        return np.sqrt(mean_squared_error(y_true, y_pred))
    
    @staticmethod
    def mape(y_true, y_pred):
        """Mean Absolute Percentage Error"""
        mask = y_true != 0
        if len(y_true[mask]) == 0:
            return 0
        return np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100
    
    @staticmethod
    def r2(y_true, y_pred):
        """R-squared Score"""
        return r2_score(y_true, y_pred)
    
    @staticmethod
    def evaluate(y_true, y_pred) -> dict:
        """Evaluate all regression metrics"""
        return {
            'mae': RegressionMetrics.mae(y_true, y_pred),
            'rmse': RegressionMetrics.rmse(y_true, y_pred),
            'mape': RegressionMetrics.mape(y_true, y_pred),
            'r2': RegressionMetrics.r2(y_true, y_pred)
        }


class ClusteringMetrics:
    """Clustering evaluation metrics"""
    
    @staticmethod
    def silhouette(X, labels):
        """Silhouette Score (higher is better, -1 to 1)"""
        if len(np.unique(labels)) < 2:
            return 0
        return silhouette_score(X, labels)
    
    @staticmethod
    def davies_bouldin(X, labels):
        """Davies-Bouldin Index (lower is better)"""
        if len(np.unique(labels)) < 2:
            return np.inf
        return davies_bouldin_score(X, labels)
    
    @staticmethod
    def evaluate(X, labels) -> dict:
        """Evaluate all clustering metrics"""
        return {
            'silhouette': ClusteringMetrics.silhouette(X, labels),
            'davies_bouldin': ClusteringMetrics.davies_bouldin(X, labels)
        }


class RecommendationMetrics:
    """Recommendation system metrics"""
    
    @staticmethod
    def precision_at_k(y_true, y_pred, k=5):
        """Precision@k"""
        if len(y_pred) < k:
            k = len(y_pred)
        return len(np.intersect1d(y_true, y_pred[:k])) / k
    
    @staticmethod
    def recall_at_k(y_true, y_pred, k=5):
        """Recall@k"""
        if len(y_true) == 0:
            return 0
        if len(y_pred) < k:
            k = len(y_pred)
        return len(np.intersect1d(y_true, y_pred[:k])) / len(y_true)
    
    @staticmethod
    def ndcg_at_k(y_true, y_pred, k=5):
        """Normalized Discounted Cumulative Gain@k"""
        if len(y_true) == 0:
            return 0
        
        if len(y_pred) < k:
            k = len(y_pred)
        
        # Calculate DCG
        dcg = 0
        for i, item in enumerate(y_pred[:k]):
            if item in y_true:
                dcg += 1 / np.log2(i + 2)  # +2 because i is 0-indexed
        
        # Calculate IDCG
        idcg = sum([1 / np.log2(i + 2) for i in range(min(k, len(y_true)))])
        
        return dcg / idcg if idcg > 0 else 0


def log_metrics(metrics: dict, model_name: str):
    """Log metrics"""
    logger.info(f"\n{'='*50}")
    logger.info(f"Model: {model_name}")
    logger.info(f"{'='*50}")
    for metric_name, value in metrics.items():
        logger.info(f"  {metric_name}: {value:.4f}")
