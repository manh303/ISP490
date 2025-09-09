#!/usr/bin/env python3
"""
Step 1: Download và verify data từ Kaggle
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent / 'src'))

import pandas as pd
from collectors.kaggle_collector import KaggleCollector
from utils.database import DatabaseManager
from loguru import logger
import os

def download_ecommerce_datasets():
    """Download key ecommerce datasets"""
    
    logger.info("🔽 Step 1: Downloading Ecommerce Datasets")
    
    try:
        # Initialize collector
        collector = KaggleCollector()
    except Exception as e:
        logger.error(f"Failed to initialize Kaggle collector: {e}")
        logger.error("Please ensure kaggle.json is present in the data-pipeline directory")
        return []
    
    # Priority datasets cho ecommerce analysis
    target_datasets = [
        {
            'name': 'carrie1/ecommerce-data',
            'description': 'UK-based online retail dataset',
            'priority': 1
        },
        {
            'name': 'olistbr/brazilian-ecommerce',
            'description': 'Brazilian ecommerce public dataset',
            'priority': 2
        },
        {
            'name': 'mkechinov/ecommerce-behavior-data-from-multi-category-store',
            'description': 'User behavior data',
            'priority': 3
        }
    ]
    
    downloaded_datasets = []
    
    for dataset in target_datasets:
        logger.info(f"📦 Downloading: {dataset['name']}")
        
        try:
            # Download dataset
            download_path = collector.download_dataset(dataset['name'])
            
            if download_path:
                # List files in dataset
                files = collector.list_files_in_dataset(download_path)
                csv_files = [f for f in files if f.suffix == '.csv']
                
                logger.info(f"   Found {len(csv_files)} CSV files")
                
                # Load and preview each CSV
                for csv_file in csv_files:
                    try:
                        df = collector.load_csv_file(csv_file, nrows=1000)  # Sample first
                        if df is not None and len(df) > 0:
                            logger.info(f"   📊 {csv_file.name}: {df.shape} rows")
                            logger.info(f"   📊 Columns: {list(df.columns)}")
                            
                            downloaded_datasets.append({
                                'dataset_name': dataset['name'],
                                'file_name': csv_file.name,
                                'file_path': str(csv_file),
                                'shape': df.shape,
                                'columns': list(df.columns),
                                'sample_data': df.head(3).to_dict('records')
                            })
                    except Exception as e:
                        logger.error(f"   ❌ Error loading {csv_file}: {e}")
                
            else:
                logger.warning(f"   ⚠️ Failed to download {dataset['name']}")
                
        except Exception as e:
            logger.error(f"❌ Error with {dataset['name']}: {e}")
            continue
    
    # Save download summary
    summary_df = pd.DataFrame(downloaded_datasets)
    if not summary_df.empty:
        # Save to database
        db_manager = DatabaseManager()
        db_manager.save_to_postgres(summary_df.drop('sample_data', axis=1), 'data_download_summary')
        
        logger.success(f"✅ Downloaded {len(downloaded_datasets)} datasets")
        logger.info("📊 Summary saved to database: data_download_summary")
        
        return downloaded_datasets
    else:
        logger.error("❌ No datasets downloaded successfully")
        return []

def verify_data_quality(datasets):
    """Verify quality of downloaded data"""
    logger.info("🔍 Step 1.2: Verifying Data Quality")
    
    quality_report = []
    
    for dataset in datasets:
        file_path = dataset['file_path']
        
        try:
            # Load full dataset for quality check
            df = pd.read_csv(file_path, nrows=5000)  # Limit for memory
            
            quality_metrics = {
                'dataset': dataset['dataset_name'],
                'file': dataset['file_name'],
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'missing_percentage': (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100,
                'duplicate_rows': df.duplicated().sum(),
                'numeric_columns': len(df.select_dtypes(include=['number']).columns),
                'text_columns': len(df.select_dtypes(include=['object']).columns),
                'date_columns': len(df.select_dtypes(include=['datetime']).columns)
            }
            
            # Check for potential issues
            issues = []
            if quality_metrics['missing_percentage'] > 30:
                issues.append('High missing data')
            if quality_metrics['duplicate_rows'] > len(df) * 0.1:
                issues.append('High duplicate rate')
            if quality_metrics['numeric_columns'] == 0:
                issues.append('No numeric columns')
            
            quality_metrics['issues'] = ', '.join(issues) if issues else 'None'
            quality_report.append(quality_metrics)
            
            logger.info(f"📊 {dataset['file_name']}: {quality_metrics['missing_percentage']:.1f}% missing, {quality_metrics['duplicate_rows']} duplicates")
            
        except Exception as e:
            logger.error(f"❌ Quality check failed for {dataset['file_name']}: {e}")
    
    # Save quality report
    if quality_report:
        quality_df = pd.DataFrame(quality_report)
        db_manager = DatabaseManager()
        db_manager.save_to_postgres(quality_df, 'data_quality_report')
        
        logger.success("✅ Quality report saved to database")
        return quality_df
    
    return pd.DataFrame()

if __name__ == "__main__":
    # Step 1: Download datasets
    datasets = download_ecommerce_datasets()
    
    if datasets:
        # Step 2: Verify quality
        quality_report = verify_data_quality(datasets)
        
        logger.info("🎯 Next Steps:")
        logger.info("1. Review downloaded data in database")
        logger.info("2. Run step2_data_cleaning.py")
        logger.info("3. Check data quality report")
        
        print("\n" + "="*50)
        print("📋 DOWNLOADED DATASETS SUMMARY")
        print("="*50)
        for i, dataset in enumerate(datasets, 1):
            print(f"{i}. {dataset['dataset_name']}")
            print(f"   File: {dataset['file_name']}")
            print(f"   Shape: {dataset['shape']}")
            print(f"   Columns: {len(dataset['columns'])}")
            print()
    else:
        logger.error("❌ No data downloaded. Check Kaggle credentials and network connection.")