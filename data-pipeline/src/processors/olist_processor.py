import pandas as pd
import numpy as np
from pathlib import Path
from loguru import logger
from datetime import datetime
import sys
sys.path.append('/app/src')
from utils.database import DatabaseManager
from processors.advanced_data_cleaner import AdvancedDataCleaner
from pymongo import MongoClient
   
client = MongoClient(
       host="mongodb",
       port=27017,
       username="admin",
       password="admin_password",
       authSource="ecommerce_dss"  # or your specific auth database
   )
class OlistDatasetProcessor:
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.cleaner = AdvancedDataCleaner()
        
        # Define Olist dataset specifications
        self.dataset_specs = {
            'olist_customers_dataset.csv': {
                'table_name': 'olist_customers_staging',
                'primary_key': 'customer_id',
                'expected_columns': ['customer_id', 'customer_unique_id', 'customer_zip_code_prefix', 'customer_city', 'customer_state'],
                'data_types': {
                    'customer_zip_code_prefix': 'numeric'
                },
                'business_rules': {
                    'customer_id': 'not_null',
                    'customer_state': 'not_null'
                }
            },
            'olist_orders_dataset.csv': {
                'table_name': 'olist_orders_staging',
                'primary_key': 'order_id',
                'expected_columns': ['order_id', 'customer_id', 'order_status', 'order_purchase_timestamp', 'order_approved_at'],
                'data_types': {
                    'order_purchase_timestamp': 'datetime',
                    'order_approved_at': 'datetime',
                    'order_delivered_carrier_date': 'datetime',
                    'order_delivered_customer_date': 'datetime'
                },
                'business_rules': {
                    'order_id': 'not_null',
                    'customer_id': 'not_null'
                }
            },
            'olist_order_items_dataset.csv': {
                'table_name': 'olist_order_items_staging',
                'primary_key': ['order_id', 'order_item_id'],
                'expected_columns': ['order_id', 'order_item_id', 'product_id', 'seller_id', 'price', 'freight_value'],
                'data_types': {
                    'price': 'numeric',
                    'freight_value': 'numeric'
                },
                'business_rules': {
                    'order_id': 'not_null',
                    'product_id': 'not_null',
                    'price': 'positive'
                }
            },
            'olist_products_dataset.csv': {
                'table_name': 'olist_products_staging',
                'primary_key': 'product_id',
                'expected_columns': ['product_id', 'product_category_name', 'product_name_lenght', 'product_description_lenght'],
                'data_types': {
                    'product_name_lenght': 'numeric',
                    'product_description_lenght': 'numeric',
                    'product_photos_qty': 'numeric'
                },
                'business_rules': {
                    'product_id': 'not_null'
                }
            },
            'olist_order_payments_dataset.csv': {
                'table_name': 'olist_payments_staging',
                'primary_key': ['order_id', 'payment_sequential'],
                'expected_columns': ['order_id', 'payment_sequential', 'payment_type', 'payment_installments', 'payment_value'],
                'data_types': {
                    'payment_sequential': 'numeric',
                    'payment_installments': 'numeric',
                    'payment_value': 'numeric'
                },
                'business_rules': {
                    'order_id': 'not_null',
                    'payment_value': 'positive'
                }
            },
            'olist_order_reviews_dataset.csv': {
                'table_name': 'olist_reviews_staging',
                'primary_key': 'review_id',
                'expected_columns': ['review_id', 'order_id', 'review_score', 'review_comment_title', 'review_comment_message'],
                'data_types': {
                    'review_score': 'numeric',
                    'review_creation_date': 'datetime',
                    'review_answer_timestamp': 'datetime'
                },
                'business_rules': {
                    'review_id': 'not_null',
                    'order_id': 'not_null',
                    'review_score': 'range_1_5'
                }
            }
        }
    
    def load_dataset(self, file_path: str, chunk_size: int = 10000):
        """Load dataset in chunks for memory efficiency"""
        try:
            logger.info(f"Loading dataset: {file_path}")
            
            # For large files, process in chunks
            file_size = Path(file_path).stat().st_size / (1024 * 1024)  # MB
            
            if file_size > 100:  # If file > 100MB, use chunking
                logger.info(f"Large file detected ({file_size:.1f}MB), using chunked processing")
                chunks = []
                for chunk in pd.read_csv(file_path, chunksize=chunk_size):
                    chunks.append(chunk)
                df = pd.concat(chunks, ignore_index=True)
            else:
                df = pd.read_csv(file_path)
            
            logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
            return df
        
        except Exception as e:
            logger.error(f"Failed to load {file_path}: {e}")
            return None
    
    def validate_schema(self, df: pd.DataFrame, dataset_name: str):
        """Validate dataset schema against specifications"""
        spec = self.dataset_specs.get(dataset_name, {})
        issues = []
        
        expected_cols = spec.get('expected_columns', [])
        actual_cols = list(df.columns)
        
        # Check for missing required columns
        missing_cols = set(expected_cols) - set(actual_cols)
        if missing_cols:
            issues.append(f"Missing columns: {missing_cols}")
        
        # Check for unexpected columns
        extra_cols = set(actual_cols) - set(expected_cols)
        if extra_cols:
            logger.info(f"Additional columns found: {extra_cols}")
        
        # Check data types
        expected_types = spec.get('data_types', {})
        for col, expected_type in expected_types.items():
            if col in df.columns:
                if expected_type == 'numeric' and not pd.api.types.is_numeric_dtype(df[col]):
                    issues.append(f"{col} should be numeric")
                elif expected_type == 'datetime' and not pd.api.types.is_datetime64_any_dtype(df[col]):
                    issues.append(f"{col} should be datetime")
        
        if issues:
            logger.warning(f"Schema validation issues for {dataset_name}: {issues}")
        else:
            logger.info(f"Schema validation passed for {dataset_name}")
        
        return issues
    
    def apply_business_rules(self, df: pd.DataFrame, dataset_name: str):
        """Apply business-specific validation rules"""
        spec = self.dataset_specs.get(dataset_name, {})
        business_rules = spec.get('business_rules', {})
        
        original_count = len(df)
        
        for column, rule in business_rules.items():
            if column not in df.columns:
                continue
            
            before_count = len(df)
            
            if rule == 'not_null':
                df = df[df[column].notna()]
            elif rule == 'positive':
                if pd.api.types.is_numeric_dtype(df[column]):
                    df = df[df[column] > 0]
            elif rule == 'range_1_5':
                if pd.api.types.is_numeric_dtype(df[column]):
                    df = df[(df[column] >= 1) & (df[column] <= 5)]
            
            removed_count = before_count - len(df)
            if removed_count > 0:
                logger.info(f"Business rule '{rule}' on '{column}': removed {removed_count} rows")
        
        total_removed = original_count - len(df)
        if total_removed > 0:
            logger.info(f"Total rows removed by business rules: {total_removed}")
        
        return df
    
    def process_individual_dataset(self, file_path: str, dataset_name: str):
        """Process individual dataset through complete pipeline"""
        
        logger.info(f"Processing {dataset_name}")
        logger.info("="*60)
        
        # 1. Load data
        df = self.load_dataset(file_path)
        if df is None:
            return None
        
        # 2. Schema validation
        schema_issues = self.validate_schema(df, dataset_name)
        
        # 3. Data cleaning
        logger.info("Applying data cleaning transformations...")
        df_cleaned = self.cleaner.comprehensive_clean(df)
        
        # 4. Apply business rules
        logger.info("Applying business rules...")
        df_validated = self.apply_business_rules(df_cleaned, dataset_name)
        
        # 5. Add processing metadata
        df_validated['processed_at'] = datetime.now()
        df_validated['source_dataset'] = dataset_name
        df_validated['processing_version'] = '1.0'
        
        # 6. Generate processing statistics
        processing_stats = {
            'dataset_name': dataset_name,
            'original_rows': len(df),
            'final_rows': len(df_validated),
            'columns_count': len(df_validated.columns),
            'cleaning_efficiency': len(df_validated) / len(df) * 100,
            'schema_issues_count': len(schema_issues),
            'processed_at': datetime.now()
        }
        
        logger.info(f"Processing complete: {len(df)} → {len(df_validated)} rows ({processing_stats['cleaning_efficiency']:.1f}% retained)")
        
        return df_validated, processing_stats
    # In your processor or config
    ENABLE_MONGO_BACKUP = False

    def save_to_staging(self, df: pd.DataFrame, dataset_name: str):
        """Save processed data to staging table"""
        spec = self.dataset_specs.get(dataset_name, {})
        table_name = spec.get('table_name', f'staging_{dataset_name.replace(".csv", "")}')
        
        try:
            # Save to PostgreSQL staging
            self.db_manager.save_to_postgres(df, table_name)
            logger.info(f"Saved to staging table: {table_name}")
            
            # Also save to MongoDB for backup
            self.db_manager.save_to_mongo(df, table_name)
            logger.info(f"Backup saved to MongoDB: {table_name}")
            
            return table_name
        
        except Exception as e:
            logger.error(f"Failed to save {dataset_name} to staging: {e}")
            return None