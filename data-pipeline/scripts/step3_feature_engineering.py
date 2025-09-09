#!/usr/bin/env python3
"""
Step 3: Feature Engineering cho ML models
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent / 'src'))

import pandas as pd
import numpy as np
from processors.feature_engineer import FeatureEngineer
from utils.database import DatabaseManager
from loguru import logger

def load_cleaned_data():
    """Load cleaned data từ database"""
    logger.info("📥 Loading cleaned data...")
    
    db_manager = DatabaseManager()
    
    try:
        df = db_manager.load_from_postgres("SELECT * FROM cleaned_products")
        logger.info(f"✅ Loaded {len(df)} cleaned products")
        return df
    except Exception as e:
        logger.error(f"❌ Error loading cleaned data: {e}")
        return pd.DataFrame()

def create_comprehensive_features(df):
    """Create comprehensive feature set"""
    logger.info("🔧 Creating comprehensive features...")
    
    fe = FeatureEngineer()
    
    # Basic ecommerce features
    df_features = fe.create_ecommerce_features(df)
    
    # Advanced features specific to our use cases
    df_features = create_advanced_ecommerce_features(df_features)
    
    return df_features

def create_advanced_ecommerce_features(df):
    """Create advanced features for ecommerce analysis"""
    logger.info("⚙️ Creating advanced ecommerce features...")
    
    df_advanced = df.copy()
    
    # 1. Price Analysis Features
    if 'price' in df.columns:
        logger.info("   💰 Creating price features...")
        
        # Price percentiles globally
        df_advanced['price_percentile'] = df['price'].rank(pct=True)
        
        # Price categories
        df_advanced['price_category'] = pd.qcut(
            df['price'], 
            q=5, 
            labels=['Very Low', 'Low', 'Medium', 'High', 'Very High']
        )
        
        # Log price for ML
        df_advanced['log_price'] = np.log1p(df['price'])
        
        # Price per word in product name
        if 'product_name' in df.columns:
            df_advanced['price_per_word'] = df['price'] / (df['product_name'].str.split().str.len() + 1)
    
    # 2. Category Analysis Features
    if 'category' in df.columns:
        logger.info("   📂 Creating category features...")
        
        # Category statistics
        category_stats = df.groupby('category').agg({
            'price': ['mean', 'median', 'std', 'count'] if 'price' in df.columns else ['count'],
            'rating': ['mean', 'count'] if 'rating' in df.columns else ['count']
        })
        category_stats.columns = ['_'.join(col) for col in category_stats.columns]
        
        # Merge category stats back
        df_advanced = df_advanced.merge(
            category_stats, 
            left_on='category', 
            right_index=True, 
            suffixes=('', '_category_stat')
        )
        
        # Category market share
        category_counts = df['category'].value_counts()
        df_advanced['category_market_share'] = df_advanced['category'].map(
            category_counts / len(df)
        )
    
    # 3. Brand Analysis Features
    if 'brand' in df.columns:
        logger.info("   🏷️ Creating brand features...")
        
        # Brand statistics
        brand_stats = df.groupby('brand').size().reset_index(name='brand_product_count')
        df_advanced = df_advanced.merge(brand_stats, on='brand', how='left')
        
        # Brand category (based on product count)
        df_advanced['brand_size_category'] = pd.qcut(
            df_advanced['brand_product_count'],
            q=3,
            labels=['Small', 'Medium', 'Large'],
            duplicates='drop'
        )
    
    # 4. Rating Analysis Features
    if 'rating' in df.columns:
        logger.info("   ⭐ Creating rating features...")
        
        # Rating bins
        df_advanced['rating_tier'] = pd.cut(
            df['rating'],
            bins=[0, 2, 3, 4, 4.5, 5],
            labels=['Poor', 'Fair', 'Good', 'Very Good', 'Excellent']
        )
        
        # Rating vs category average
        if 'category' in df.columns:
            category_avg_rating = df.groupby('category')['rating'].mean()
            df_advanced['rating_vs_category_avg'] = df_advanced['rating'] - df_advanced['category'].map(category_avg_rating)
    
    # 5. Text Analysis Features
    if 'product_name' in df.columns:
        logger.info("   📝 Creating text features...")
        
        # Basic text stats
        df_advanced['name_length'] = df_advanced['product_name'].str.len()
        df_advanced['name_word_count'] = df_advanced['product_name'].str.split().str.len()
        
        # Name complexity
        df_advanced['avg_word_length'] = df_advanced['product_name'].str.replace(' ', '').str.len() / df_advanced['name_word_count']
        
        # Contains numbers
        df_advanced['name_contains_numbers'] = df_advanced['product_name'].str.contains(r'\d').astype(int)
        
        # Contains special characters
        df_advanced['name_has_special_chars'] = df_advanced['product_name'].str.contains(r'[^a-zA-Z0-9\s]').astype(int)
    
    # 6. Data Source Features
    if 'data_source' in df.columns:
        logger.info("   🔗 Creating data source features...")
        
        # Source reliability score (mock score based on data completeness)
        source_completeness = df.groupby('data_source').apply(
            lambda x: 1 - (x.isnull().sum().sum() / (len(x) * len(x.columns)))
        )
        df_advanced['source_reliability_score'] = df_advanced['data_source'].map(source_completeness)
    
    # 7. Interaction Features
    logger.info("   🔄 Creating interaction features...")
    
    # Price-Rating interaction
    if 'price' in df.columns and 'rating' in df.columns:
        df_advanced['price_rating_ratio'] = df_advanced['price'] / (df_advanced['rating'] + 1)
        df_advanced['value_score'] = df_advanced['rating'] / (df_advanced['log_price'] + 1)
    
    # Category-Price interaction
    if 'category' in df.columns and 'price' in df.columns:
        df_advanced['is_premium_in_category'] = (
            df_advanced['price'] > df_advanced.groupby('category')['price'].transform('quantile', 0.75)
        ).astype(int)
    
    logger.info(f"   ✅ Created {len(df_advanced.columns) - len(df.columns)} new features")
    
    return df_advanced

def prepare_ml_datasets(df_features):
    """Prepare datasets for different ML tasks"""
    logger.info("🤖 Preparing ML-ready datasets...")
    
    fe = FeatureEngineer()
    ml_datasets = {}
    
    # 1. Price Prediction Dataset
    if 'price' in df_features.columns:
        logger.info("   💰 Preparing price prediction dataset...")
        
        price_features = [
            col for col in df_features.columns 
            if col not in ['price', 'log_price', 'product_id', 'product_name'] 
            and not col.startswith('price_')
        ]
        
        price_df = df_features.dropna(subset=['price'])
        if len(price_df) > 100:  # Minimum data requirement
            price_ml = fe.prepare_for_ml(
                price_df[price_features + ['price']], 
                target_column='price'
            )
            ml_datasets['price_prediction'] = price_ml
            logger.info(f"     ✅ Price prediction: {len(price_ml)} samples, {len(price_features)} features")
    
    # 2. Rating Prediction Dataset
    if 'rating' in df_features.columns:
        logger.info("   ⭐ Preparing rating prediction dataset...")
        
        rating_features = [
            col for col in df_features.columns 
            if col not in ['rating', 'product_id', 'product_name']
            and not col.startswith('rating_')
        ]
        
        rating_df = df_features.dropna(subset=['rating'])
        if len(rating_df) > 100:
            rating_ml = fe.prepare_for_ml(
                rating_df[rating_features + ['rating']], 
                target_column='rating'
            )
            ml_datasets['rating_prediction'] = rating_ml
            logger.info(f"     ✅ Rating prediction: {len(rating_ml)} samples")
    
    # 3. Category Classification Dataset
    if 'category' in df_features.columns:
        logger.info("   📂 Preparing category classification dataset...")
        
        category_features = [
            col for col in df_features.columns 
            if col not in ['category', 'product_id', 'product_name']
            and not col.startswith('category_')
        ]
        
        category_df = df_features.dropna(subset=['category'])
        if len(category_df) > 100:
            category_ml = fe.prepare_for_ml(
                category_df[category_features + ['category']], 
                target_column='category'
            )
            ml_datasets['category_classification'] = category_ml
            logger.info(f"     ✅ Category classification: {len(category_ml)} samples")
    
    # 4. General Analytics Dataset (all features)
    logger.info("   📊 Preparing analytics dataset...")
    
    analytics_features = [
        col for col in df_features.columns 
        if col not in ['product_id', 'product_name', 'data_source']
    ]
    
    analytics_df = df_features[analytics_features].copy()
    # Fill remaining NaNs for analytics
    for col in analytics_df.columns:
        if analytics_df[col].dtype in ['float64', 'int64']:
            analytics_df[col] = analytics_df[col].fillna(analytics_df[col].median())
        else:
            analytics_df[col] = analytics_df[col].fillna('Unknown')
    
    ml_datasets['analytics'] = analytics_df
    logger.info(f"     ✅ Analytics dataset: {len(analytics_df)} samples, {len(analytics_df.columns)} features")
    
    return ml_datasets

def save_feature_datasets(df_features, ml_datasets):
    """Save featured datasets to database"""
    logger.info("💾 Saving featured datasets...")
    
    db_manager = DatabaseManager()
    
    # Save main featured dataset
    db_manager.save_to_postgres(df_features, 'featured_products')
    logger.info(f"   ✅ Saved featured products: {len(df_features)} rows")
    
    # Save ML-ready datasets
    for dataset_name, dataset in ml_datasets.items():
        table_name = f'ml_ready_{dataset_name}'
        db_manager.save_to_postgres(dataset, table_name)
        logger.info(f"   ✅ Saved {dataset_name}: {len(dataset)} rows")
    
    # Create feature summary
    feature_summary = []
    for col in df_features.columns:
        summary = {
            'feature_name': col,
            'data_type': str(df_features[col].dtype),
            'missing_count': int(df_features[col].isnull().sum()),
            'missing_percentage': float(df_features[col].isnull().sum() / len(df_features) * 100),
            'unique_values': int(df_features[col].nunique()),
            'is_numeric': int(df_features[col].dtype in ['int64', 'float64']),
            'is_categorical': int(df_features[col].dtype == 'object'),
            'created_at': pd.Timestamp.now()
        }
        
        # Add statistics for numeric columns
        if df_features[col].dtype in ['int64', 'float64']:
            summary.update({
                'min_value': float(df_features[col].min()),
                'max_value': float(df_features[col].max()),
                'mean_value': float(df_features[col].mean()),
                'std_value': float(df_features[col].std())
            })
        
        feature_summary.append(summary)
    
    # Save feature summary
    feature_summary_df = pd.DataFrame(feature_summary)
    db_manager.save_to_postgres(feature_summary_df, 'feature_summary')
    logger.success("✅ Feature engineering completed and saved!")

def main_feature_pipeline():
    """Main feature engineering pipeline"""
    logger.info("🚀 Starting Feature Engineering Pipeline...")
    
    # Load cleaned data
    df_clean = load_cleaned_data()
    
    if df_clean.empty:
        logger.error("❌ No cleaned data found. Run step2_data_cleaning.py first")
        return
    
    # Create features
    df_features = create_comprehensive_features(df_clean)
    
    # Prepare ML datasets
    ml_datasets = prepare_ml_datasets(df_features)
    
    # Save everything
    save_feature_datasets(df_features, ml_datasets)
    
    # Summary
    logger.info("\n📊 FEATURE ENGINEERING SUMMARY:")
    logger.info(f"   Original features: {len(df_clean.columns)}")
    logger.info(f"   Total features: {len(df_features.columns)}")
    logger.info(f"   New features created: {len(df_features.columns) - len(df_clean.columns)}")
    logger.info(f"   ML datasets created: {len(ml_datasets)}")
    logger.info(f"   Records processed: {len(df_features)}")
    
    logger.info("\n🎯 Next Steps:")
    logger.info("1. Review feature summary in database")
    logger.info("2. Run step4_ml_modeling.py")
    logger.info("3. Validate feature quality")
    
    return df_features, ml_datasets

if __name__ == "__main__":
    main_feature_pipeline()