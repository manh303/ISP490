import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.feature_extraction.text import TfidfVectorizer
from loguru import logger

class FeatureEngineer:
    def __init__(self):
        self.scalers = {}
        self.encoders = {}
        self.vectorizers = {}
    
    def create_ecommerce_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Tạo features cho ecommerce analysis
        """
        logger.info("Creating ecommerce features...")
        df_features = df.copy()
        
        # 1. Time-based features
        if 'order_date' in df.columns or 'scraped_date' in df.columns:
            df_features = self._create_time_features(df_features)
        
        # 2. Price features
        if 'price' in df.columns:
            df_features = self._create_price_features(df_features)
        
        # 3. Rating features
        if 'rating' in df.columns:
            df_features = self._create_rating_features(df_features)
        
        # 4. Category features
        if 'category' in df.columns:
            df_features = self._create_category_features(df_features)
        
        # 5. Text features (product name, description)
        text_columns = ['name', 'description', 'title']
        for col in text_columns:
            if col in df.columns:
                df_features = self._create_text_features(df_features, col)
        
        logger.info(f"Feature engineering completed. Shape: {df.shape} -> {df_features.shape}")
        return df_features
    
    def _create_time_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Tạo time-based features"""
        date_col = 'order_date' if 'order_date' in df.columns else 'scraped_date'
        
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col])
            
            # Extract time components
            df['year'] = df[date_col].dt.year
            df['month'] = df[date_col].dt.month
            df['day'] = df[date_col].dt.day
            df['weekday'] = df[date_col].dt.weekday
            df['quarter'] = df[date_col].dt.quarter
            
            # Business features
            df['is_weekend'] = df['weekday'].isin([5, 6]).astype(int)
            df['is_month_start'] = df[date_col].dt.is_month_start.astype(int)
            df['is_month_end'] = df[date_col].dt.is_month_end.astype(int)
            
            # Season features
            df['season'] = df['month'].map({
                12: 'Winter', 1: 'Winter', 2: 'Winter',
                3: 'Spring', 4: 'Spring', 5: 'Spring',
                6: 'Summer', 7: 'Summer', 8: 'Summer',
                9: 'Fall', 10: 'Fall', 11: 'Fall'
            })
            
        return df
    
    def _create_price_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Tạo price-based features"""
        if 'price' in df.columns:
            # Price statistics within categories
            if 'category' in df.columns:
                df['price_rank_in_category'] = df.groupby('category')['price'].rank(pct=True)
                df['price_vs_category_mean'] = df['price'] / df.groupby('category')['price'].transform('mean')
                df['price_vs_category_median'] = df['price'] / df.groupby('category')['price'].transform('median')
            
            # Price bins
            df['price_bin'] = pd.qcut(df['price'], q=5, labels=['Very Low', 'Low', 'Medium', 'High', 'Very High'])
            
            # Log transform for skewed price data
            df['log_price'] = np.log1p(df['price'])
            
        return df
    
    def _create_rating_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Tạo rating-based features"""
        if 'rating' in df.columns:
            # Rating bins
            df['rating_category'] = pd.cut(df['rating'], 
                                         bins=[0, 2, 3, 4, 5], 
                                         labels=['Poor', 'Fair', 'Good', 'Excellent'])
            
            # Review count features
            if 'num_reviews' in df.columns:
                df['reviews_per_rating'] = df['num_reviews'] / (df['rating'] + 1)  # +1 to avoid division by 0
                df['review_popularity'] = pd.qcut(df['num_reviews'], q=4, labels=['Low', 'Medium', 'High', 'Very High'])
        
        return df
    
    def _create_category_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Tạo category-based features"""
        if 'category' in df.columns:
            # Category statistics
            category_stats = df.groupby('category').agg({
                'price': ['mean', 'median', 'std', 'count'],
                'rating': ['mean', 'count'] if 'rating' in df.columns else ['count']
            }).round(2)
            
            # Flatten column names
            category_stats.columns = ['_'.join(col).strip() for col in category_stats.columns]
            
            # Merge back to main dataframe
            df = df.merge(category_stats, left_on='category', right_index=True, how='left', suffixes=('', '_category'))
            
        return df
    
    def _create_text_features(self, df: pd.DataFrame, text_column: str) -> pd.DataFrame:
        """Tạo text-based features"""
        if text_column in df.columns:
            # Basic text statistics
            df[f'{text_column}_length'] = df[text_column].str.len()
            df[f'{text_column}_word_count'] = df[text_column].str.split().str.len()
            
            # Brand extraction (if not already present)
            if 'brand' not in df.columns and text_column == 'name':
                # Simple brand extraction - first word của product name
                df['brand_extracted'] = df[text_column].str.split().str[0]
        
        return df
    
    def prepare_for_ml(self, df: pd.DataFrame, target_column: str = None, 
                       categorical_columns: list = None, 
                       numerical_columns: list = None) -> pd.DataFrame:
        """
        Prepare data cho machine learning
        """
        df_ml = df.copy()
        
        # Auto-detect column types nếu không specify
        if categorical_columns is None:
            categorical_columns = df_ml.select_dtypes(include=['object', 'category']).columns.tolist()
            if target_column in categorical_columns:
                categorical_columns.remove(target_column)
        
        if numerical_columns is None:
            numerical_columns = df_ml.select_dtypes(include=['int64', 'float64']).columns.tolist()
            if target_column in numerical_columns:
                numerical_columns.remove(target_column)
        
        # Encode categorical variables
        for col in categorical_columns:
            if col in df_ml.columns:
                le = LabelEncoder()
                df_ml[f'{col}_encoded'] = le.fit_transform(df_ml[col].astype(str))
                self.encoders[col] = le
        
        # Scale numerical variables
        if numerical_columns:
            scaler = StandardScaler()
            df_ml[numerical_columns] = scaler.fit_transform(df_ml[numerical_columns])
            self.scalers['numerical'] = scaler
        
        logger.info(f"Data prepared for ML. Categorical: {len(categorical_columns)}, Numerical: {len(numerical_columns)}")
        return df_ml