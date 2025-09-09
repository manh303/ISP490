import pandas as pd
import numpy as np
import re
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')
from loguru import logger

class AdvancedDataCleaner:
    def __init__(self):
        self.cleaning_stats = {
            'original_rows': 0,
            'final_rows': 0,
            'duplicates_removed': 0,
            'missing_values_filled': {},
            'outliers_removed': {},
            'invalid_data_fixed': {}
        }
    
    def comprehensive_clean(self, df: pd.DataFrame, column_types: dict = None) -> pd.DataFrame:
        """Comprehensive cleaning pipeline"""
        logger.info("🧹 Starting comprehensive data cleaning...")
        
        self.cleaning_stats['original_rows'] = len(df)
        df_clean = df.copy()
        
        # 1. Handle duplicates
        df_clean = self._remove_duplicates(df_clean)
        
        # 2. Auto-detect and fix data types
        df_clean = self._auto_detect_and_fix_types(df_clean)
        
        # 3. Clean text columns
        df_clean = self._clean_text_data(df_clean)
        
        # 4. Handle missing values
        df_clean = self._handle_missing_values_smart(df_clean)
        
        # 5. Remove/fix outliers
        df_clean = self._handle_outliers(df_clean)
        
        # 6. Validate business rules
        df_clean = self._validate_business_rules(df_clean)
        
        # 7. Final validation
        df_clean = self._final_validation(df_clean)
        
        self.cleaning_stats['final_rows'] = len(df_clean)
        
        logger.info(f"✅ Cleaning completed: {self.cleaning_stats['original_rows']} → {self.cleaning_stats['final_rows']} rows")
        return df_clean
    
    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicates"""
        before = len(df)
        df = df.drop_duplicates()
        exact_dupes = before - len(df)
        
        # Business logic duplicates
        if 'product_id' in df.columns:
            if 'scraped_date' in df.columns or 'cleaned_at' in df.columns:
                date_col = 'scraped_date' if 'scraped_date' in df.columns else 'cleaned_at'
                df = df.sort_values(date_col).drop_duplicates('product_id', keep='last')
        
        total_removed = before - len(df)
        self.cleaning_stats['duplicates_removed'] = total_removed
        
        if exact_dupes > 0 or total_removed > exact_dupes:
            logger.info(f"🗑️ Removed {total_removed} duplicates ({exact_dupes} exact, {total_removed-exact_dupes} business logic)")
        
        return df
    
    def _auto_detect_and_fix_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Auto-detect and fix data types"""
        logger.info("🔍 Auto-detecting data types...")
        
        for column in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[column]):
                continue
                
            sample_values = df[column].dropna().head(100)
            
            if len(sample_values) == 0:
                continue
            
            # Check if numeric
            if self._looks_like_numeric(sample_values):
                df[column] = self._convert_to_numeric(df[column])
            elif self._looks_like_datetime(sample_values):
                df[column] = self._convert_to_datetime(df[column])
        
        return df
    
    def _looks_like_numeric(self, series: pd.Series) -> bool:
        """Check if series looks numeric"""
        try:
            cleaned = series.astype(str).str.replace(r'[\$,]', '', regex=True)
            pd.to_numeric(cleaned, errors='raise')
            return True
        except:
            return False
    
    def _looks_like_datetime(self, series: pd.Series) -> bool:
        """Check if series looks like datetime"""
        try:
            pd.to_datetime(series.head(10), errors='raise')
            return True
        except:
            return False
    
    def _convert_to_numeric(self, series: pd.Series) -> pd.Series:
        """Convert series to numeric"""
        logger.info(f"🔢 Converting {series.name} to numeric")
        
        if series.name and 'price' in series.name.lower():
            cleaned = series.astype(str).str.replace(r'[\$,£€¥]', '', regex=True)
        else:
            cleaned = series.astype(str).str.replace(r'[,]', '', regex=True)
        
        return pd.to_numeric(cleaned, errors='coerce')
    
    def _convert_to_datetime(self, series: pd.Series) -> pd.Series:
        """Convert series to datetime"""
        logger.info(f"📅 Converting {series.name} to datetime")
        return pd.to_datetime(series, errors='coerce')
    
    def _clean_text_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean text columns"""
        logger.info("📝 Cleaning text data...")
        
        text_columns = df.select_dtypes(include=['object']).columns
        
        for col in text_columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str)
                df[col] = df[col].str.strip()
                df[col] = df[col].str.replace(r'\s+', ' ', regex=True)
                
                # Handle special cases
                if 'name' in col.lower() or 'title' in col.lower():
                    df[col] = df[col].str.title()
                elif 'category' in col.lower():
                    df[col] = df[col].str.title()
        
        return df
    
    def _handle_missing_values_smart(self, df: pd.DataFrame) -> pd.DataFrame:
        """Smart missing value handling"""
        logger.info("🕳️ Handling missing values...")
        
        for column in df.columns:
            missing_count = df[column].isnull().sum()
            if missing_count == 0:
                continue
            
            # Numeric columns
            if pd.api.types.is_numeric_dtype(df[column]):
                df[column] = df[column].fillna(df[column].median())
            # Text columns
            elif df[column].dtype == 'object':
                mode_value = df[column].mode()
                if not mode_value.empty:
                    df[column] = df[column].fillna(mode_value.iloc[0])
                else:
                    df[column] = df[column].fillna('Unknown')
            # DateTime columns
            elif pd.api.types.is_datetime64_any_dtype(df[column]):
                df[column] = df[column].fillna(df[column].median())
            
            self.cleaning_stats['missing_values_filled'][column] = missing_count
        
        return df
    
    def _handle_outliers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle outliers using IQR method"""
        logger.info("📊 Handling outliers...")
        
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        
        for column in numeric_columns:
            if 'id' in column.lower():
                continue
            
            Q1 = df[column].quantile(0.25)
            Q3 = df[column].quantile(0.75)
            IQR = Q3 - Q1
            
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            outliers_mask = (df[column] < lower_bound) | (df[column] > upper_bound)
            outliers_count = outliers_mask.sum()
            
            if outliers_count > 0:
                df.loc[df[column] < lower_bound, column] = lower_bound
                df.loc[df[column] > upper_bound, column] = upper_bound
                
                self.cleaning_stats['outliers_removed'][column] = outliers_count
                logger.info(f"  🎯 Capped {outliers_count} outliers in {column}")
        
        return df
    
    def _validate_business_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate business rules"""
        logger.info("🏢 Validating business rules...")
        
        before = len(df)
        
        # Price validation
        if 'price' in df.columns:
            invalid_price = (df['price'] <= 0) | (df['price'].isnull())
            df = df[~invalid_price]
            removed = invalid_price.sum()
            if removed > 0:
                logger.info(f"  💰 Removed {removed} records with invalid prices")
        
        # Rating validation
        if 'rating' in df.columns:
            invalid_rating = (df['rating'] < 0) | (df['rating'] > 5)
            df.loc[invalid_rating, 'rating'] = np.nan
            fixed = invalid_rating.sum()
            if fixed > 0:
                logger.info(f"  ⭐ Fixed {fixed} invalid ratings")
        
        total_removed = before - len(df)
        self.cleaning_stats['invalid_data_fixed']['total_records_removed'] = total_removed
        
        return df
    
    def _final_validation(self, df: pd.DataFrame) -> pd.DataFrame:
        """Final data validation"""
        logger.info("✅ Final validation...")
        
        total_cells = df.shape[0] * df.shape[1]
        missing_cells = df.isnull().sum().sum()
        completeness = (total_cells - missing_cells) / total_cells * 100
        
        logger.info(f"  📊 Data completeness: {completeness:.1f}%")
        
        if len(df) == 0:
            raise ValueError("All data was removed during cleaning!")
        
        return df
    
    def generate_cleaning_report(self) -> dict:
        """Generate cleaning report"""
        report = {
            'summary': {
                'original_rows': self.cleaning_stats['original_rows'],
                'final_rows': self.cleaning_stats['final_rows'],
                'rows_removed': self.cleaning_stats['original_rows'] - self.cleaning_stats['final_rows'],
                'removal_percentage': (self.cleaning_stats['original_rows'] - self.cleaning_stats['final_rows']) / self.cleaning_stats['original_rows'] * 100 if self.cleaning_stats['original_rows'] > 0 else 0
            },
            'details': self.cleaning_stats
        }
        return report
