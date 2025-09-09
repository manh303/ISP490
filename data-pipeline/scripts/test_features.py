#!/usr/bin/env python3

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent / 'src'))

import pandas as pd
from processors.feature_engineer import FeatureEngineer
from utils.database import DatabaseManager
from loguru import logger

def test_feature_engineering():
    logger.info("=== Testing Feature Engineering ===")
    
    # Load sample data
    db_manager = DatabaseManager()
    df = db_manager.load_from_postgres("SELECT * FROM raw_scraped_sample LIMIT 1000")
    
    if df.empty:
        logger.error("No data found. Run initial_data_setup.py first")
        return
    
    # Initialize feature engineer
    fe = FeatureEngineer()
    
    # Create features
    df_features = fe.create_ecommerce_features(df)
    
    # Prepare for ML
    df_ml = fe.prepare_for_ml(
        df_features, 
        target_column='price',
        categorical_columns=['category', 'brand', 'availability']
    )
    
    logger.info(f"Original columns: {list(df.columns)}")
    logger.info(f"Feature columns: {list(df_features.columns)}")
    logger.info(f"ML-ready columns: {list(df_ml.columns)}")
    
    # Save featured data
    db_manager.save_to_postgres(df_features, 'featured_data')
    logger.success("Feature engineering test completed!")

if __name__ == "__main__":
    test_feature_engineering()