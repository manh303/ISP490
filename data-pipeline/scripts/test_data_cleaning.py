#!/usr/bin/env python3

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent / 'src'))

import pandas as pd
import numpy as np
from processors.advanced_data_cleaner import AdvancedDataCleaner
from loguru import logger

def create_messy_test_data():
    """Create intentionally messy data để test cleaning"""
    
    # Base clean data
    np.random.seed(42)
    n_rows = 1000
    
    data = {
        'product_id': [f'PROD_{i:06d}' for i in range(n_rows)],
        'name': [f'Product {i}' for i in range(n_rows)],
        'category': np.random.choice(['Electronics', 'Clothing', 'Books', 'Home'], n_rows),
        'price': np.random.uniform(10, 500, n_rows),
        'rating': np.random.uniform(1, 5, n_rows),
        'num_reviews': np.random.randint(0, 1000, n_rows),
        'brand': np.random.choice(['BrandA', 'BrandB', 'BrandC'], n_rows),
        'scraped_date': pd.date_range('2024-01-01', periods=n_rows, freq='H')
    }
    
    df = pd.DataFrame(data)
    
    # Now make it messy! 😈
    logger.info("🔥 Creating messy data for testing...")
    
    # 1. Add duplicates
    duplicates = df.sample(n=50)
    df = pd.concat([df, duplicates], ignore_index=True)
    
    # 2. Add missing values
    missing_indices = np.random.choice(df.index, size=100, replace=False)
    df.loc[missing_indices[:30], 'price'] = np.nan
    df.loc[missing_indices[30:60], 'rating'] = np.nan
    df.loc[missing_indices[60:90], 'brand'] = np.nan
    df.loc[missing_indices[90:], 'name'] = np.nan
    
    # 3. Add messy price formats
    price_mess_indices = np.random.choice(df.index, size=50, replace=False)
    df.loc[price_mess_indices[:20], 'price'] = df.loc[price_mess_indices[:20], 'price'].apply(
        lambda x: f'${x:,.2f}' if not pd.isna(x) else x
    )
    df.loc[price_mess_indices[20:40], 'price'] = df.loc[price_mess_indices[20:40], 'price'].apply(
        lambda x: f'€{x:,.2f}' if not pd.isna(x) else x
    )
    
    # 4. Add invalid ratings
    rating_mess = np.random.choice(df.index, size=30, replace=False)
    df.loc[rating_mess[:15], 'rating'] = np.random.uniform(6, 10, 15)  # Invalid high ratings
    df.loc[rating_mess[15:], 'rating'] = np.random.uniform(-2, 0, 15)  # Invalid negative ratings
    
    # 5. Add messy text data
    text_mess = np.random.choice(df.index, size=40, replace=False)
    df.loc[text_mess[:20], 'name'] = df.loc[text_mess[:20], 'name'] + '   '  # Extra spaces
    df.loc[text_mess[20:], 'brand'] = df.loc[text_mess[20:], 'brand'].str.lower()  # Wrong case
    
    # 6. Add extreme outliers
    outlier_indices = np.random.choice(df.index, size=20, replace=False)
    df.loc[outlier_indices[:10], 'price'] = np.random.uniform(10000, 50000, 10)  # Super expensive
    df.loc[outlier_indices[10:], 'num_reviews'] = np.random.randint(50000, 100000, 10)  # Too many reviews
    
    # 7. Add invalid negative prices
    negative_price = np.random.choice(df.index, size=15, replace=False)
    df.loc[negative_price, 'price'] = np.random.uniform(-100, -1, 15)
    
    # 8. Add future dates
    future_dates = np.random.choice(df.index, size=10, replace=False)
    df.loc[future_dates, 'scraped_date'] = pd.date_range('2025-01-01', periods=10, freq='D')
    
    logger.info(f"💀 Created messy dataset with {len(df)} rows")
    logger.info(f"   - Duplicates: ~50")
    logger.info(f"   - Missing values: ~100")
    logger.info(f"   - Invalid prices: ~15")
    logger.info(f"   - Invalid ratings: ~30")
    logger.info(f"   - Outliers: ~20")
    
    return df

def test_comprehensive_cleaning():
    """Test comprehensive data cleaning"""
    
    # Create messy data
    messy_df = create_messy_test_data()
    
    logger.info(f"📊 Original data shape: {messy_df.shape}")
    logger.info(f"📊 Missing values per column:")
    for col, missing in messy_df.isnull().sum().items():
        if missing > 0:
            logger.info(f"   {col}: {missing}")
    
    # Initialize cleaner
    cleaner = AdvancedDataCleaner()
    
    # Define expected column types
    column_types = {
        'price': 'numeric',
        'rating': 'numeric',
        'num_reviews': 'numeric',
        'scraped_date': 'datetime',
        'category': 'categorical',
        'brand': 'categorical'
    }
    
    # Clean data
    clean_df = cleaner.comprehensive_clean(messy_df, column_types)
    
    logger.info(f"✅ Cleaned data shape: {clean_df.shape}")
    logger.info(f"✅ Missing values after cleaning:")
    missing_after = clean_df.isnull().sum()
    for col, missing in missing_after.items():
        if missing > 0:
            logger.info(f"   {col}: {missing}")
    
    # Generate report
    report = cleaner.generate_cleaning_report()
    
    logger.info("📋 Cleaning Report:")
    logger.info(f"   Original rows: {report['summary']['original_rows']}")
    logger.info(f"   Final rows: {report['summary']['final_rows']}")
    logger.info(f"   Rows removed: {report['summary']['rows_removed']}")
    logger.info(f"   Removal percentage: {report['summary']['removal_percentage']:.1f}%")
    
    # Data quality checks
    logger.info("🔍 Data Quality Checks:")
    logger.info(f"   Price range: ${clean_df['price'].min():.2f} - ${clean_df['price'].max():.2f}")
    logger.info(f"   Rating range: {clean_df['rating'].min():.1f} - {clean_df['rating'].max():.1f}")
    logger.info(f"   Categories: {clean_df['category'].unique()}")
    
    return clean_df

if __name__ == "__main__":
    clean_data = test_comprehensive_cleaning()
    
    # Save results for next steps
    clean_data.to_csv('../data/processed/test_cleaned_data.csv', index=False)
    logger.success("✅ Test completed! Clean data saved to ../data/processed/")