# -*- coding: utf-8 -*-
"""
Quick Sentiment Classifier Training - Standalone version
Trains sentiment classifier without database dependency
"""

import sys
import os
from pathlib import Path
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
import joblib
import numpy as np

# Sample review data for training
SAMPLE_REVIEWS = [
    ("Sản phẩm rất tốt, giao hàng nhanh, chất lượng cao", "positive"),
    ("Tôi rất hài lòng với sản phẩm này", "positive"),
    ("Giao dịch mượt mà, hàng đúng như mô tả", "positive"),
    ("Tuyệt vời, sẽ mua lại", "positive"),
    ("Chất lượng tốt, đáng tiền", "positive"),
    ("Sản phẩm ok, lên hàng đúng hẹn", "positive"),
    ("Rất vừa ý, cảm ơn shop", "positive"),
    ("Sản phẩm kém chất lượng", "negative"),
    ("Hàng không đúng như mô tả", "negative"),
    ("Giao hàng chậm, chất lượng kém", "negative"),
    ("Không hài lòng, sản phẩm bị hư", "negative"),
    ("Chất lượng tệ, tiền bị lãng phí", "negative"),
    ("Sản phẩm không như kỳ vọng", "negative"),
    ("Khó tiếp nhận, giao hàng lâu", "negative"),
    ("Sản phẩm bình thường", "neutral"),
    ("Cũng được, không có gì đặc biệt", "neutral"),
    ("Chất lượng trung bình", "neutral"),
    ("Sản phẩm ok, giá hợp lý", "neutral"),
    ("Không tệ nhưng cũng không tốt", "neutral"),
    ("Bình thường, mua thêm sẽ cân nhắc", "neutral"),
    # English samples
    ("Great product, very happy with my purchase", "positive"),
    ("Excellent quality and fast delivery", "positive"),
    ("Love it, will buy again", "positive"),
    ("Perfect, exactly as described", "positive"),
    ("Highly recommend this product", "positive"),
    ("Poor quality, waste of money", "negative"),
    ("Not as described, very disappointed", "negative"),
    ("Terrible experience, do not recommend", "negative"),
    ("Defective product, got refund", "negative"),
    ("Not worth the price", "negative"),
    ("Average product, nothing special", "neutral"),
    ("Okay, meets expectations", "neutral"),
    ("Decent quality for the price", "neutral"),
    ("Not bad but not great either", "neutral"),
    ("It's fine, nothing exceptional", "neutral"),
]

def train_sentiment_classifier():
    """Train a quick sentiment classifier"""
    
    print("=" * 60)
    print("QUICK SENTIMENT CLASSIFIER TRAINING")
    print("=" * 60)
    
    # Prepare data
    texts = [review[0] for review in SAMPLE_REVIEWS]
    labels = [review[1] for review in SAMPLE_REVIEWS]
    
    print(f"\n[INFO] Total samples: {len(texts)}")
    print(f"[INFO] Label distribution:")
    for label in set(labels):
        count = labels.count(label)
        print(f"  - {label}: {count}")
    
    # Split data
    X_train, X_test, y_train, y_test = train_test_split(
        texts, labels, test_size=0.2, random_state=42, stratify=labels
    )
    
    print(f"\n[INFO] Training set: {len(X_train)} samples")
    print(f"[INFO] Test set: {len(X_test)} samples")
    
    # TF-IDF Vectorization
    print("\n[STEP 1] TF-IDF Vectorization...")
    tfidf = TfidfVectorizer(max_features=100, max_df=0.8, min_df=1)
    X_train_tfidf = tfidf.fit_transform(X_train)
    X_test_tfidf = tfidf.transform(X_test)
    
    print(f"[OK] Feature matrix shape: {X_train_tfidf.shape}")
    
    # Encode labels
    le = LabelEncoder()
    y_train_encoded = le.fit_transform(y_train)
    y_test_encoded = le.transform(y_test)
    
    print(f"[OK] Label mapping: {dict(zip(le.classes_, le.transform(le.classes_)))}")
    
    # Train classifier
    print("\n[STEP 2] Training Random Forest Classifier...")
    model = RandomForestClassifier(n_estimators=100, max_depth=15, random_state=42, n_jobs=-1)
    model.fit(X_train_tfidf, y_train_encoded)
    
    # Evaluate
    print("\n[STEP 3] Evaluating Model...")
    y_train_pred = model.predict(X_train_tfidf)
    y_test_pred = model.predict(X_test_tfidf)
    
    train_accuracy = accuracy_score(y_train_encoded, y_train_pred)
    test_accuracy = accuracy_score(y_test_encoded, y_test_pred)
    test_precision = precision_score(y_test_encoded, y_test_pred, average='weighted', zero_division=0)
    test_recall = recall_score(y_test_encoded, y_test_pred, average='weighted', zero_division=0)
    test_f1 = f1_score(y_test_encoded, y_test_pred, average='weighted', zero_division=0)
    
    print(f"\n[METRICS] Training Accuracy: {train_accuracy:.4f}")
    print(f"[METRICS] Test Accuracy: {test_accuracy:.4f}")
    print(f"[METRICS] Precision: {test_precision:.4f}")
    print(f"[METRICS] Recall: {test_recall:.4f}")
    print(f"[METRICS] F1-Score: {test_f1:.4f}")
    
    return model, tfidf, le

def save_models(model, tfidf, label_encoder):
    """Save trained models"""
    
    print("\n" + "=" * 60)
    print("SAVING MODELS")
    print("=" * 60)
    
    models_dir = Path(__file__).parent / "models" / "ml-models"
    models_dir.mkdir(parents=True, exist_ok=True)
    
    # Save sentiment classifier
    model_path = models_dir / "sentiment_classifier.pkl"
    joblib.dump(model, model_path)
    print(f"[OK] Sentiment classifier saved: {model_path}")
    
    # Save TF-IDF vectorizer
    tfidf_path = models_dir / "sentiment_tfidf_vectorizer.pkl"
    joblib.dump(tfidf, tfidf_path)
    print(f"[OK] TF-IDF vectorizer saved: {tfidf_path}")
    
    # Save label encoder
    le_path = models_dir / "sentiment_label_encoder.pkl"
    joblib.dump(label_encoder, le_path)
    print(f"[OK] Label encoder saved: {le_path}")
    
    print("\n[SUCCESS] All models saved successfully!")

if __name__ == "__main__":
    try:
        print("[ML PIPELINE] Quick Sentiment Classifier Training\n")
        
        # Train models
        model, tfidf, le = train_sentiment_classifier()
        
        # Save models
        save_models(model, tfidf, le)
        
        print("\n" + "=" * 60)
        print("[SUCCESS] SENTIMENT CLASSIFIER TRAINING COMPLETED")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
