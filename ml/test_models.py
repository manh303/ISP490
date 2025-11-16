# -*- coding: utf-8 -*-
"""
Test Script for Trained Models
Verify models are working correctly
"""

import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from pathlib import Path
import joblib
import numpy as np
from utils.logger import get_logger

logger = get_logger("model_testing")


def test_sentiment_model():
    """Test sentiment classifier"""
    logger.info("\n" + "="*60)
    logger.info("TESTING SENTIMENT CLASSIFIER")
    logger.info("="*60)
    
    try:
        models_dir = Path("models/ml-models")
        
        # Load components
        model = joblib.load(models_dir / "sentiment_classifier.pkl")
        tfidf = joblib.load(models_dir / "sentiment_tfidf_vectorizer.pkl")
        label_encoder = joblib.load(models_dir / "sentiment_label_encoder.pkl")
        
        logger.info("[✓] Model components loaded successfully")
        
        # Test data
        test_texts = [
            "Sản phẩm rất tốt, giao hàng nhanh, chất lượng tuyệt vời!",
            "Tệ quá, hàng kém chất lượng, không đáng tiền",
            "Sản phẩm bình thường, chẳng có gì đặc biệt"
        ]
        
        test_ratings = [5.0, 1.0, 3.0]
        
        # Vectorize text
        X_text = tfidf.transform(test_texts).toarray()
        
        # Create numeric features
        numeric_features = np.array([
            [5.0, 50, 12, 2, 0],  # positive review
            [1.0, 40, 8, 0, 1],    # negative review
            [3.0, 30, 6, 0, 0]     # neutral review
        ])
        
        from scipy.sparse import hstack
        X = hstack([tfidf.transform(test_texts), numeric_features])
        
        # Predict
        predictions = model.predict(X)
        probabilities = model.predict_proba(X)
        
        # Decode labels
        predicted_labels = label_encoder.inverse_transform(predictions)
        
        logger.info("\nTest Predictions:")
        for i, (text, label, probs) in enumerate(zip(test_texts[:50], predicted_labels, probabilities)):
            logger.info(f"\nReview {i+1}: {text[:60]}...")
            logger.info(f"  Prediction: {label}")
            logger.info(f"  Probabilities: {dict(zip(label_encoder.classes_, probs))}")
        
        logger.info("\n[✓] Sentiment classifier test PASSED")
        return True
    
    except Exception as e:
        logger.error(f"[✗] Sentiment classifier test FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_clustering_model():
    """Test clustering model"""
    logger.info("\n" + "="*60)
    logger.info("TESTING CLUSTERING MODEL")
    logger.info("="*60)
    
    try:
        models_dir = Path("models/ml-models")
        
        # Load components
        model = joblib.load(models_dir / "recommendation_kmeans.pkl")
        scaler = joblib.load(models_dir / "clustering_scaler.pkl")
        features = joblib.load(models_dir / "clustering_features.pkl")
        
        logger.info("[✓] Clustering model components loaded successfully")
        logger.info(f"  Model type: {type(model).__name__}")
        logger.info(f"  Number of clusters: {model.n_clusters}")
        logger.info(f"  Features: {features}")
        
        # Test data - simulate product features
        test_data = np.array([
            [12.0, 5000, 4.2, 0.8, 2.5, 0.7],   # Premium product
            [10.5, 3000, 3.8, 0.75, 1.5, 0.6],  # Mid-range
            [8.0, 1000, 3.0, 0.6, 0.5, 0.5]      # Budget
        ])
        
        # Scale
        test_data_scaled = scaler.transform(test_data)
        
        # Predict clusters
        clusters = model.predict(test_data_scaled)
        distances = model.transform(test_data_scaled)
        
        logger.info("\nTest Predictions:")
        for i, (cluster, dist) in enumerate(zip(clusters, distances)):
            logger.info(f"  Product {i+1} -> Cluster {cluster}, Distance: {dist.min():.4f}")
        
        logger.info("\n[✓] Clustering model test PASSED")
        return True
    
    except Exception as e:
        logger.error(f"[✗] Clustering model test FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


def validate_model_files():
    """Validate all required model files exist"""
    logger.info("\n" + "="*60)
    logger.info("VALIDATING MODEL FILES")
    logger.info("="*60)
    
    models_dir = Path("models/ml-models")
    
    required_files = {
        "sentiment_classifier.pkl": "Sentiment Classifier Model",
        "sentiment_tfidf_vectorizer.pkl": "TF-IDF Vectorizer",
        "sentiment_label_encoder.pkl": "Label Encoder",
        "recommendation_kmeans.pkl": "KMeans Clustering Model",
        "clustering_scaler.pkl": "Feature Scaler",
        "clustering_features.pkl": "Feature Names"
    }
    
    all_exist = True
    for filename, description in required_files.items():
        filepath = models_dir / filename
        if filepath.exists():
            size_mb = filepath.stat().st_size / (1024 * 1024)
            logger.info(f"[✓] {description:.<40} ({size_mb:.2f} MB)")
        else:
            logger.warning(f"[✗] {description:.<40} MISSING")
            all_exist = False
    
    return all_exist


def validate_metrics():
    """Validate metrics files"""
    logger.info("\n" + "="*60)
    logger.info("VALIDATING METRICS")
    logger.info("="*60)
    
    import json
    
    metrics_dir = Path("logs/metrics")
    
    files_to_check = [
        ("sentiment_metrics.json", "Sentiment Classification Metrics"),
        ("clustering_metrics.json", "Clustering Metrics")
    ]
    
    all_valid = True
    for filename, description in files_to_check:
        filepath = metrics_dir / filename
        if filepath.exists():
            try:
                with open(filepath, 'r') as f:
                    metrics = json.load(f)
                logger.info(f"[✓] {description:.<40}")
                for key, value in metrics.items():
                    if isinstance(value, float):
                        logger.info(f"    {key}: {value:.4f}")
                    else:
                        logger.info(f"    {key}: {value}")
            except Exception as e:
                logger.warning(f"[✗] {description:.<40} (Error: {e})")
                all_valid = False
        else:
            logger.warning(f"[✗] {description:.<40} MISSING")
            all_valid = False
    
    return all_valid


if __name__ == "__main__":
    try:
        logger.info("[ML MODELS TEST SUITE]")
        
        results = {
            "File Validation": validate_model_files(),
            "Metrics Validation": validate_metrics(),
            "Sentiment Classifier": test_sentiment_model(),
            "Clustering Model": test_clustering_model()
        }
        
        # Summary
        logger.info("\n" + "="*60)
        logger.info("TEST SUMMARY")
        logger.info("="*60)
        
        passed = sum(1 for v in results.values() if v)
        total = len(results)
        
        for test_name, passed_flag in results.items():
            status = "[✓]" if passed_flag else "[✗]"
            logger.info(f"{status} {test_name}")
        
        logger.info(f"\nTotal: {passed}/{total} tests passed")
        
        if passed == total:
            logger.info("\n[✓✓✓] ALL TESTS PASSED ✓✓✓")
            sys.exit(0)
        else:
            logger.error(f"\n[✗✗✗] {total - passed} TESTS FAILED ✗✗✗")
            sys.exit(1)
    
    except Exception as e:
        logger.error(f"[FATAL] Test suite error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
