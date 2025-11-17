# -*- coding: utf-8 -*-
"""
Step 2: Data Preparation
Clean, preprocess, feature engineering
"""

import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.preprocessing import StandardScaler, MinMaxScaler, RobustScaler
from utils.logger import get_logger
import yaml
import warnings

warnings.filterwarnings('ignore')

logger = get_logger("data_preparation")

# Load config
with open('config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)


class DataPreprocessor:
    """Data preprocessing and feature engineering"""
    
    def __init__(self, config: dict):
        self.config = config
        self.scaler = None
        self.scaling_method = config['data_preparation']['scaling']
    
    def handle_missing_values(self, df: pd.DataFrame, method: str = 'mean') -> pd.DataFrame:
        """Handle missing values"""
        logger.info(f"\nHandling missing values (method: {method})...")
        
        initial_missing = df.isna().sum().sum()
        
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        if method == 'mean':
            for col in numeric_cols:
                if df[col].isna().sum() > 0:
                    df[col].fillna(df[col].mean(), inplace=True)
        
        elif method == 'median':
            for col in numeric_cols:
                if df[col].isna().sum() > 0:
                    df[col].fillna(df[col].median(), inplace=True)
        
        elif method == 'forward_fill':
            df = df.fillna(method='ffill').fillna(method='bfill')
        
        final_missing = df.isna().sum().sum()
        logger.info(f"  Before: {initial_missing} missing values")
        logger.info(f"  After: {final_missing} missing values")
        
        return df
    
    def remove_outliers(self, df: pd.DataFrame, method: str = 'iqr', 
                       columns: list = None) -> pd.DataFrame:
        """Remove outliers"""
        if not self.config['data_preparation']['remove_outliers']:
            return df
        
        logger.info(f"\nRemoving outliers (method: {method})...")
        
        initial_rows = len(df)
        
        numeric_cols = columns or df.select_dtypes(include=[np.number]).columns
        
        if method == 'iqr':
            for col in numeric_cols:
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                df = df[(df[col] >= lower_bound) & (df[col] <= upper_bound)]
        
        elif method == 'zscore':
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            for col in numeric_cols:
                z_scores = np.abs((df[col] - df[col].mean()) / df[col].std())
                df = df[z_scores < 3]
        
        removed_rows = initial_rows - len(df)
        logger.info(f"  Removed: {removed_rows} rows ({removed_rows/initial_rows*100:.2f}%)")
        
        return df
    
    def feature_engineering(self, df: pd.DataFrame, task: str = 'demand') -> pd.DataFrame:
        """Create new features"""
        logger.info(f"\nFeature engineering ({task})...")
        
        if task == 'demand':
            # Price features
            if 'avg_price' in df.columns:
                df['price_change_pct'] = df.groupby('source_platform_std')['avg_price'].pct_change() * 100
            else:
                df['price_change_pct'] = 0
            
            if 'max_price' in df.columns:
                df['price_volatility'] = df['max_price'] - df['min_price']
            
            # Trend features (7-day and 30-day moving average)
            if 'agg_date' in df.columns:
                df = df.sort_values('agg_date')
            
            if 'total_review_count' in df.columns:
                df['review_ma7'] = df.groupby('category_std')['total_review_count'].transform(
                    lambda x: x.rolling(window=7, min_periods=1).mean()
                )
                df['review_ma30'] = df.groupby('category_std')['total_review_count'].transform(
                    lambda x: x.rolling(window=30, min_periods=1).mean()
                )
            
            logger.info(f"  Created {len(df.columns)} features")
        
        elif task == 'recommendation':
            # Sentiment features
            if 'avg_sentiment_score' in df.columns:
                df['sentiment_category'] = pd.cut(df['avg_sentiment_score'], 
                                                 bins=[-1, -0.5, 0, 0.5, 1], 
                                                 labels=['very_negative', 'negative', 'positive', 'very_positive'],
                                                 include_lowest=True)
            
            # Review velocity
            if 'active_days' in df.columns:
                df['review_density'] = df['total_reviews'] / (df['active_days'] + 1)
            
            # Sentiment ratio
            if 'positive_reviews' in df.columns and 'negative_reviews' in df.columns:
                total_reviews = df['positive_reviews'] + df['negative_reviews']
                df['positive_ratio'] = np.where(total_reviews > 0, 
                                               df['positive_reviews'] / total_reviews, 
                                               0)
            
            logger.info(f"  Created {len(df.columns)} features")
        
        return df
    
    def scale_features(self, df: pd.DataFrame, fit: bool = True) -> pd.DataFrame:
        """Scale numeric features"""
        logger.info(f"\nScaling features ({self.scaling_method})...")
        
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        if self.scaling_method == 'StandardScaler':
            scaler = StandardScaler()
        elif self.scaling_method == 'MinMaxScaler':
            scaler = MinMaxScaler()
        elif self.scaling_method == 'RobustScaler':
            scaler = RobustScaler()
        else:
            scaler = StandardScaler()
        
        if fit:
            self.scaler = scaler.fit(df[numeric_cols])
        
        df[numeric_cols] = self.scaler.transform(df[numeric_cols])
        
        logger.info(f"  Scaled {len(numeric_cols)} features")
        
        return df
    
    def prepare_demand_data(self, df: pd.DataFrame) -> tuple:
        """Prepare demand prediction data"""
        logger.info("\n" + "="*60)
        logger.info("PREPARING DEMAND PREDICTION DATA")
        logger.info("="*60)
        
        # Remove rows with missing key columns
        initial_rows = len(df)
        df = df[df['total_review_count'].notna() | df['avg_price'].notna()]
        logger.info(f"  Rows with data: {len(df)} (removed {initial_rows - len(df)})")
        
        # Handle missing values
        df = self.handle_missing_values(df, method=self.config['data_preparation']['handle_missing'])
        
        # Remove outliers
        numeric_cols = ['avg_price', 'max_price', 'min_price', 'avg_rating', 'total_review_count']
        outlier_cols = [col for col in numeric_cols if col in df.columns]
        if outlier_cols:
            df = self.remove_outliers(df, method='iqr', columns=outlier_cols)
        
        # Feature engineering
        df = self.feature_engineering(df, task='demand')
        
        # Select features
        feature_cols = [
            'avg_price', 'min_price', 'max_price',
            'avg_rating', 'total_review_count',
            'day_of_week', 'month', 'year',
            'price_change_pct'
        ]
        
        # Add optional features if they exist
        optional_cols = ['price_volatility', 'review_ma7', 'review_ma30']
        for col in optional_cols:
            if col in df.columns:
                feature_cols.append(col)
        
        # Remove features that don't exist
        feature_cols = [col for col in feature_cols if col in df.columns]
        df = df[feature_cols + ['total_review_count', 'source_platform_std', 'agg_date']].dropna()
        
        # Temporal split
        if self.config['data_preparation']['temporal_split']:
            test_days = self.config['data_preparation']['test_days_forward']
            max_date = df['agg_date'].max()
            test_cutoff = max_date - pd.Timedelta(days=test_days)
            
            train_df = df[df['agg_date'] <= test_cutoff].copy()
            test_df = df[df['agg_date'] > test_cutoff].copy()
            
            logger.info(f"\nTemporal split:")
            logger.info(f"  Train: {len(train_df)} ({train_df['agg_date'].min()} to {train_df['agg_date'].max()})")
            logger.info(f"  Test: {len(test_df)} ({test_df['agg_date'].min()} to {test_df['agg_date'].max()})")
            
            return train_df, test_df, feature_cols
        else:
            return df, None, feature_cols
    
    def prepare_recommendation_data(self, df: pd.DataFrame) -> tuple:
        """Prepare recommendation data"""
        logger.info("\n" + "="*60)
        logger.info("PREPARING RECOMMENDATION DATA")
        logger.info("="*60)
        
        # Handle missing values
        df = self.handle_missing_values(df, method=self.config['data_preparation']['handle_missing'])
        
        # Feature engineering
        df = self.feature_engineering(df, task='recommendation')
        
        # Select features
        feature_cols = [
            'avg_rating', 'total_reviews', 'avg_sentiment_score',
            'positive_reviews', 'negative_reviews', 'active_days',
            'positive_sentiment_pct', 'negative_sentiment_pct'
        ]
        
        # Add optional sentiment features
        if 'sentiment_category' in df.columns:
            feature_cols.append('sentiment_category')
        if 'review_density' in df.columns:
            feature_cols.append('review_density')
        if 'positive_ratio' in df.columns:
            feature_cols.append('positive_ratio')
        
        feature_cols = [col for col in feature_cols if col in df.columns]
        df = df[feature_cols + ['global_product_id', 'source_platform_std']].dropna()
        
        logger.info(f"  Final dataset: {len(df)} products")
        
        return df, feature_cols


def main():
    """Main pipeline"""
    try:
        logger.info("[ML PIPELINE] Step 2: Data Preparation")
        
        # Load data
        demand_dir = Path(config['data_extraction']['demand']['output_dir'])
        recommendation_dir = Path(config['data_extraction']['recommendation']['output_dir'])
        
        demand_df = pd.read_csv(demand_dir / 'raw_demand_data.csv')
        demand_df['agg_date'] = pd.to_datetime(demand_df['agg_date'])
        
        recommendation_df = pd.read_csv(recommendation_dir / 'raw_recommendation_data.csv')
        
        logger.info(f"Loaded {len(demand_df)} demand records")
        logger.info(f"Loaded {len(recommendation_df)} recommendation records")
        
        # Prepare data
        preprocessor = DataPreprocessor(config)
        
        train_df, test_df, demand_features = preprocessor.prepare_demand_data(demand_df)
        recommendation_prepared, rec_features = preprocessor.prepare_recommendation_data(recommendation_df)
        
        # Save prepared data
        demand_dir.mkdir(parents=True, exist_ok=True)
        recommendation_dir.mkdir(parents=True, exist_ok=True)
        
        train_df.to_csv(demand_dir / 'train_demand_data.csv', index=False)
        if test_df is not None:
            test_df.to_csv(demand_dir / 'test_demand_data.csv', index=False)
        
        recommendation_prepared.to_csv(recommendation_dir / 'prepared_recommendation_data.csv', index=False)
        
        logger.info("\n" + "="*60)
        logger.info("[OK] DATA PREPARATION COMPLETED")
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"\n[FAILED] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
