import pandas as pd
import numpy as np
from loguru import logger
from pathlib import Path

class DataCleaner:
    def __init__(self):
        self.cleaning_stats = {}
    
    def clean_ecommerce_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean raw ecommerce data
        """
        logger.info(f"Starting data cleaning. Shape: {df.shape}")
        original_shape = df.shape
        
        # 1. Remove duplicates
        df = self._remove_duplicates(df)
        
        # 2. Handle missing values
        df = self._handle_missing_values(df)
        
        # 3. Clean price data
        if 'price' in df.columns:
            df = self._clean_price_column(df)
        
        # 4. Clean rating data
        if 'rating' in df.columns:
            df = self._clean_rating_column(df)
        
        # 5. Standardize text columns
        text_columns = df.select_dtypes(include=['object']).columns
        df = self._clean_text_columns(df, text_columns)
        
        logger.info(f"Data cleaning completed. Shape: {original_shape} -> {df.shape}")
        return df
    
    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        before = len(df)
        df = df.drop_duplicates()
        after = len(df)
        removed = before - after
        logger.info(f"Removed {removed} duplicates")
        return df
    
    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        # Strategy cho từng column type
        for column in df.columns:
            missing_count = df[column].isnull().sum()
            if missing_count > 0:
                if df[column].dtype in ['float64', 'int64']:
                    # Fill numeric with median
                    df[column].fillna(df[column].median(), inplace=True)
                else:
                    # Fill text with 'Unknown'
                    df[column].fillna('Unknown', inplace=True)
                logger.info(f"Filled {missing_count} missing values in {column}")
        return df
    
    def _clean_price_column(self, df: pd.DataFrame) -> pd.DataFrame:
        if 'price' not in df.columns:
            return df
        
        # Remove outliers (beyond 3 standard deviations)
        before = len(df)
        mean = df['price'].mean()
        std = df['price'].std()
        df = df[np.abs(df['price'] - mean) <= 3 * std]
        after = len(df)
        
        logger.info(f"Removed {before - after} price outliers")
        return df
    
    def _clean_rating_column(self, df: pd.DataFrame) -> pd.DataFrame:
        if 'rating' not in df.columns:
            return df
        
        # Ensure ratings are in valid range
        df['rating'] = df['rating'].clip(0, 5)
        return df
    
    def _clean_text_columns(self, df: pd.DataFrame, text_columns) -> pd.DataFrame:
        for col in text_columns:
            if col in df.columns:
                # Strip whitespace and title case
                df[col] = df[col].astype(str).str.strip().str.title()
        return df