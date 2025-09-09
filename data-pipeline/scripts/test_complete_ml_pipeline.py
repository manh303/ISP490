#!/usr/bin/env python3

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent / 'src'))

import pandas as pd
from ml.forecasting.sales_predictor import SalesPredictor
from ml.clustering.customer_segmentation import CustomerSegmentation
from ml.recommendation.product_recommender import ProductRecommender
from processors.feature_engineer import FeatureEngineer
from utils.database import DatabaseManager
from loguru import logger

def test_complete_pipeline():
    logger.info("=== Testing Complete ML Pipeline ===")
    
    # Initialize components
    db_manager = DatabaseManager()
    feature_engineer = FeatureEngineer()
    
    # Load data
    df = db_manager.load_from_postgres("SELECT * FROM featured_data LIMIT 3000")
    
    if df.empty:
        logger.error("No data found. Run previous scripts first")
        return
    
    logger.info(f"Loaded data shape: {df.shape}")
    
    # Test 1: Sales Forecasting
    logger.info("\n--- Testing Sales Forecasting ---")
    sales_predictor = SalesPredictor()
    
    sales_data = sales_predictor.prepare_sales_data(df)
    ts_data = sales_predictor.create_time_series_features(sales_data)
    sales_scores = sales_predictor.train_models(ts_data)
    
    # Make predictions
    sales_predictions = sales_predictor.predict_sales(ts_data, days_ahead=7)
    logger.info(f"Generated {len(sales_predictions)} sales predictions")
    
    # Save sales model
    sales_model_path = sales_predictor.save_model()
    
    # Test 2: Customer Segmentation
    logger.info("\n--- Testing Customer Segmentation ---")
    customer_segmentation = CustomerSegmentation()
    
    customer_features = customer_segmentation.create_customer_features(df)
    segment_performance, segmented_customers = customer_segmentation.train_clustering_models(customer_features)
    
    logger.info("Segment Profiles:")
    print(customer_segmentation.segment_profiles)
    
    # Save segmentation model
    segmentation_model_path = customer_segmentation.save_model()
    
    # Test 3: Product Recommendations
    logger.info("\n--- Testing Product Recommendations ---")
    recommender = ProductRecommender()
    
    rec_data = recommender.prepare_recommendation_data(df)
    
    # Build models
    recommender.build_content_based_model(rec_data['products'])
    recommender.build_collaborative_model(rec_data['user_item_matrix'])
    
    # Test recommendations
    sample_product = df.iloc[0]['product_id']
    content_recs = recommender.get_content_based_recommendations(
        sample_product, rec_data['products'], n_recommendations=5
    )
    logger.info(f"Content-based recommendations for {sample_product}:")
    print(content_recs[['product_id', 'name', 'similarity_score']])
    
    # Test collaborative recommendations
    sample_user = rec_data['user_item_matrix'].index[0]
    collab_recs = recommender.get_collaborative_recommendations(sample_user, n_recommendations=5)
    logger.info(f"Collaborative recommendations for {sample_user}:")
    print(collab_recs)
    
    # Test hybrid recommendations
    hybrid_recs = recommender.get_hybrid_recommendations(
        sample_user, rec_data['products'], n_recommendations=5
    )
    logger.info("Hybrid recommendations:")
    print(hybrid_recs[['product_id', 'name', 'hybrid_score']])
    
    # Evaluate recommendations
    rec_metrics = recommender.evaluate_recommendations(rec_data['interactions'])
    
    # Save recommendation model
    rec_model_path = recommender.save_model()
    
    # Save all results to database
    logger.info("\n--- Saving Results to Database ---")
    
    # Sales predictions
    db_manager.save_to_postgres(sales_predictions, 'ml_sales_predictions')
    
    # Customer segments
    db_manager.save_to_postgres(segmented_customers, 'ml_customer_segments')
    db_manager.save_to_postgres(customer_segmentation.segment_profiles.reset_index(), 'ml_segment_profiles')
    
    # Sample recommendations
    sample_recommendations = pd.DataFrame({
        'user_id': [sample_user] * len(hybrid_recs),
        'product_id': hybrid_recs['product_id'],
        'recommendation_score': hybrid_recs['hybrid_score'],
        'recommendation_type': 'hybrid'
    })
    db_manager.save_to_postgres(sample_recommendations, 'ml_recommendations')
    
    # Summary report
    logger.info("\n=== ML Pipeline Summary ===")
    logger.info(f"1. Sales Forecasting - Best Model: {sales_predictor.best_model_name}")
    logger.info(f"2. Customer Segmentation - Best Model: {customer_segmentation.best_model_name}")
    logger.info(f"   - Number of segments: {len(customer_segmentation.segment_profiles)}")
    logger.info(f"3. Product Recommendations - Precision: {rec_metrics['precision']:.3f}, Recall: {rec_metrics['recall']:.3f}")
    
    logger.info(f"\nModels saved:")
    logger.info(f"- Sales: {sales_model_path}")
    logger.info(f"- Segmentation: {segmentation_model_path}")
    logger.info(f"- Recommendations: {rec_model_path}")
    
    logger.success("Complete ML pipeline test finished!")

if __name__ == "__main__":
    test_complete_pipeline()