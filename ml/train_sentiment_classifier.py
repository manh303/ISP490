# -*- coding: utf-8 -*-
"""
Train Sentiment Classification Model
Mô hình phân loại cảm xúc từ review (Positive, Negative, Neutral)
"""

import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import os
import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.metrics import (accuracy_score, precision_score, recall_score, 
                             f1_score, confusion_matrix, classification_report)
import joblib
import yaml
from utils.logger import get_logger
from utils.db_connector import DWHConnector

logger = get_logger("sentiment_training")

# Load config
with open('config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)


def extract_sentiment_training_data():
    """
    Extract data từ DWH để train sentiment classifier
    Sử dụng review data với sentiment labels
    """
    logger.info("\n" + "="*60)
    logger.info("EXTRACTING SENTIMENT TRAINING DATA")
    logger.info("="*60)
    
    conn = DWHConnector()
    
    # Query lấy review data với sentiment information
    sql = """
    SELECT 
        review_id,
        global_product_id,
        review_text,
        rating,
        LENGTH(review_text) as review_length,
        CASE 
            WHEN rating >= 4.0 THEN 'positive'
            WHEN rating <= 2.0 THEN 'negative'
            ELSE 'neutral'
        END as sentiment_label,
        created_at
    FROM dwh.dim_review
    WHERE review_text IS NOT NULL 
    AND review_text != ''
    AND LENGTH(review_text) > 10
    LIMIT 5000
    """
    
    try:
        df = conn.query(sql)
        logger.info(f"[OK] Total records: {len(df)}")
        logger.info(f"[OK] Sentiment distribution:")
        logger.info(df['sentiment_label'].value_counts().to_string())
        
        # Data quality checks
        logger.info("\nData Quality Checks:")
        logger.info(f"  Missing review_text: {df['review_text'].isna().sum()}")
        logger.info(f"  Missing rating: {df['rating'].isna().sum()}")
        logger.info(f"  Avg review length: {df['review_length'].mean():.0f} chars")
        
        # Save raw data
        output_dir = Path(config['data_extraction']['sentiment']['output_dir'])
        output_dir.mkdir(parents=True, exist_ok=True)
        
        df.to_csv(output_dir / 'raw_sentiment_data.csv', index=False)
        logger.info(f"[OK] Saved to {output_dir / 'raw_sentiment_data.csv'}")
        
        return df
    
    except Exception as e:
        logger.error(f"[ERROR] Error extracting sentiment data: {e}")
        raise
    
    finally:
        conn.close()


def prepare_sentiment_features(df):
    """
    Prepare features cho sentiment classification
    """
    logger.info("\n" + "="*60)
    logger.info("PREPARING SENTIMENT FEATURES")
    logger.info("="*60)
    
    df = df.copy()
    
    # Text-based features
    df['word_count'] = df['review_text'].str.split().str.len()
    df['exclamation_count'] = df['review_text'].str.count('!')
    df['question_count'] = df['review_text'].str.count('\\?')
    df['uppercase_count'] = df['review_text'].str.count('[A-Z]')
    
    # Rating-based features
    df['rating_normalized'] = df['rating'] / 5.0
    
    logger.info(f"[OK] Features created: word_count, exclamation_count, question_count, uppercase_count, rating_normalized")
    logger.info(f"\nFeature statistics:")
    logger.info(df[['word_count', 'exclamation_count', 'question_count', 'rating_normalized']].describe())
    
    return df


def train_sentiment_classifier(train_df, test_df):
    """
    Train sentiment classification model
    """
    logger.info("\n" + "="*60)
    logger.info("TRAINING SENTIMENT CLASSIFIER")
    logger.info("="*60)
    
    # Prepare features
    X_train_text = train_df['review_text'].values
    X_train_numeric = train_df[['rating', 'review_length', 'word_count', 
                                 'exclamation_count', 'question_count']].fillna(0).values
    y_train = train_df['sentiment_label'].values
    
    X_test_text = test_df['review_text'].values
    X_test_numeric = test_df[['rating', 'review_length', 'word_count',
                              'exclamation_count', 'question_count']].fillna(0).values
    y_test = test_df['sentiment_label'].values
    
    # TF-IDF vectorization for text
    logger.info("\n[Step 1] TF-IDF Vectorization...")
    tfidf = TfidfVectorizer(max_features=100, stop_words='english', max_df=0.8, min_df=2)
    X_train_tfidf = tfidf.fit_transform(X_train_text)
    X_test_tfidf = tfidf.transform(X_test_text)
    
    # Combine text features with numeric features
    from scipy.sparse import hstack
    X_train = hstack([X_train_tfidf, X_train_numeric])
    X_test = hstack([X_test_tfidf, X_test_numeric])
    
    logger.info(f"[OK] Training features shape: {X_train.shape}")
    logger.info(f"[OK] Test features shape: {X_test.shape}")
    
    # Encode labels
    from sklearn.preprocessing import LabelEncoder
    le = LabelEncoder()
    y_train_encoded = le.fit_transform(y_train)
    y_test_encoded = le.transform(y_test)
    
    logger.info(f"[OK] Label mapping: {dict(zip(le.classes_, le.transform(le.classes_)))}")
    
    # Train Random Forest classifier
    logger.info("\n[Step 2] Training Random Forest Classifier...")
    model = RandomForestClassifier(n_estimators=100, max_depth=20, random_state=42, n_jobs=-1)
    model.fit(X_train, y_train_encoded)
    
    # Predictions
    y_train_pred = model.predict(X_train)
    y_test_pred = model.predict(X_test)
    
    # Evaluate
    logger.info("\n[Step 3] Evaluating Model...")
    logger.info(f"\n--- Training Metrics ---")
    logger.info(f"Accuracy:  {accuracy_score(y_train_encoded, y_train_pred):.4f}")
    logger.info(f"Precision: {precision_score(y_train_encoded, y_train_pred, average='weighted'):.4f}")
    logger.info(f"Recall:    {recall_score(y_train_encoded, y_train_pred, average='weighted'):.4f}")
    logger.info(f"F1-Score:  {f1_score(y_train_encoded, y_train_pred, average='weighted'):.4f}")
    
    logger.info(f"\n--- Test Metrics ---")
    test_accuracy = accuracy_score(y_test_encoded, y_test_pred)
    test_precision = precision_score(y_test_encoded, y_test_pred, average='weighted')
    test_recall = recall_score(y_test_encoded, y_test_pred, average='weighted')
    test_f1 = f1_score(y_test_encoded, y_test_pred, average='weighted')
    
    logger.info(f"Accuracy:  {test_accuracy:.4f}")
    logger.info(f"Precision: {test_precision:.4f}")
    logger.info(f"Recall:    {test_recall:.4f}")
    logger.info(f"F1-Score:  {test_f1:.4f}")
    
    logger.info(f"\n--- Classification Report ---")
    logger.info(classification_report(y_test_encoded, y_test_pred, 
                                     target_names=le.classes_))
    
    logger.info(f"\n--- Confusion Matrix ---")
    logger.info(confusion_matrix(y_test_encoded, y_test_pred))
    
    # Feature importance
    logger.info(f"\n[OK] Model trained successfully!")
    
    return model, tfidf, le, {
        'accuracy': test_accuracy,
        'precision': test_precision,
        'recall': test_recall,
        'f1_score': test_f1
    }


def save_sentiment_model(model, tfidf, label_encoder, metrics):
    """
    Save trained sentiment model
    """
    logger.info("\n" + "="*60)
    logger.info("SAVING SENTIMENT MODEL")
    logger.info("="*60)
    
    models_dir = Path(config['models']['output_dir'])
    models_dir.mkdir(parents=True, exist_ok=True)
    
    # Save model
    model_path = models_dir / "sentiment_classifier.pkl"
    joblib.dump(model, model_path)
    logger.info(f"[OK] Model saved: {model_path}")
    
    # Save tfidf vectorizer
    tfidf_path = models_dir / "sentiment_tfidf_vectorizer.pkl"
    joblib.dump(tfidf, tfidf_path)
    logger.info(f"[OK] TF-IDF vectorizer saved: {tfidf_path}")
    
    # Save label encoder
    le_path = models_dir / "sentiment_label_encoder.pkl"
    joblib.dump(label_encoder, le_path)
    logger.info(f"[OK] Label encoder saved: {le_path}")
    
    # Save metrics
    metrics_path = models_dir.parent / "logs" / "sentiment_metrics.json"
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    
    import json
    with open(metrics_path, 'w') as f:
        json.dump(metrics, f, indent=2)
    logger.info(f"[OK] Metrics saved: {metrics_path}")
    
    logger.info(f"\n[OK] Sentiment classifier training COMPLETED!")


if __name__ == "__main__":
    try:
        logger.info("[ML PIPELINE] Sentiment Classifier Training")
        
        # Extract data
        sentiment_df = extract_sentiment_training_data()
        
        # Prepare features
        sentiment_df = prepare_sentiment_features(sentiment_df)
        
        # Split data
        logger.info("\nSplitting data (80/20)...")
        train_df, test_df = train_test_split(
            sentiment_df, 
            test_size=0.2, 
            random_state=42,
            stratify=sentiment_df['sentiment_label']
        )
        logger.info(f"[OK] Train: {len(train_df)}, Test: {len(test_df)}")
        
        # Train model
        model, tfidf, label_encoder, metrics = train_sentiment_classifier(train_df, test_df)
        
        # Save model
        save_sentiment_model(model, tfidf, label_encoder, metrics)
        
        logger.info("\n" + "="*60)
        logger.info("[SUCCESS] SENTIMENT CLASSIFIER TRAINING COMPLETED")
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"\n[FAILED] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
