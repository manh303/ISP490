# -*- coding: utf-8 -*-
"""
Generate Sample Data for ML Pipeline Demo
Tạo dữ liệu sample để demo pipeline khi DWH tables chưa sẵn
"""

import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from utils.logger import get_logger

logger = get_logger("sample_data_generation")


def generate_demand_data(n_products: int = 100, n_days: int = 180) -> pd.DataFrame:
    """Generate sample demand data"""
    logger.info(f"Generating {n_products} products x {n_days} days = {n_products * n_days} records...")
    
    data = []
    base_date = datetime.now() - timedelta(days=n_days)
    
    np.random.seed(42)
    
    for product_id in range(1, n_products + 1):
        for day in range(n_days):
            date = base_date + timedelta(days=day)
            
            # Realistic demand pattern
            base_demand = np.random.normal(50, 20)  # Mean 50, std 20
            seasonal_factor = np.sin(day / 365 * 2 * np.pi) * 10 + 10  # Seasonal pattern
            trend = (day / n_days) * 20  # Upward trend
            noise = np.random.normal(0, 5)
            
            sold_count = max(0, base_demand + seasonal_factor + trend + noise)
            
            # Price features
            price_original = 500000 + np.random.normal(0, 100000)
            discount_pct = np.random.uniform(0, 50)
            price_current = price_original * (1 - discount_pct / 100)
            
            # Rating features
            rating_avg = np.random.normal(4.0, 0.5)
            rating_avg = np.clip(rating_avg, 1, 5)
            rating_count = np.random.poisson(100)
            review_count = int(rating_count * np.random.uniform(0.3, 0.8))
            
            data.append({
                'date_sk': 20000000 + day,
                'product_sk': product_id,
                'product_name': f'Product_{product_id}',
                'brand_sk': (product_id % 20) + 1,
                'category_sk': (product_id % 10) + 1,
                'category_name': f'Category_{(product_id % 10) + 1}',
                'date_value': date,
                'year': date.year,
                'month': date.month,
                'day': date.day,
                'day_of_week': date.weekday(),
                'price_current': price_current,
                'price_original': price_original,
                'discount_pct': discount_pct,
                'rating_avg': rating_avg,
                'rating_count': rating_count,
                'review_count': review_count,
                'sold_count': sold_count,
                'is_available': True
            })
    
    df = pd.DataFrame(data)
    logger.info(f"Generated {len(df)} demand records")
    
    return df


def generate_recommendation_data(n_products: int = 100) -> pd.DataFrame:
    """Generate sample recommendation data"""
    logger.info(f"Generating {n_products} products for recommendation...")
    
    np.random.seed(42)
    
    data = []
    for product_id in range(1, n_products + 1):
        category_id = (product_id % 10) + 1
        
        avg_rating = np.random.normal(4.0, 0.5)
        avg_rating = np.clip(avg_rating, 1, 5)
        
        total_reviews = np.random.poisson(500)
        total_review_count = int(total_reviews * np.random.uniform(0.7, 1.0))
        
        avg_sentiment = np.random.normal(0.3, 0.3)
        avg_sentiment = np.clip(avg_sentiment, -1, 1)
        
        positive_reviews = int(total_reviews * max(0.2, avg_sentiment + 0.5))
        negative_reviews = int(total_reviews * max(0, 0.5 - avg_sentiment))
        
        active_days = np.random.randint(30, 180)
        
        data.append({
            'product_sk': product_id,
            'product_name': f'Product_{product_id}',
            'brand_sk': (product_id % 20) + 1,
            'brand_name': f'Brand_{(product_id % 20) + 1}',
            'category_sk': category_id,
            'category_name': f'Category_{category_id}',
            'parent_category_sk': 1 if category_id <= 5 else 2,
            'avg_rating': avg_rating,
            'total_review_count': total_review_count,
            'total_reviews': total_reviews,
            'avg_sentiment': avg_sentiment,
            'positive_reviews': positive_reviews,
            'negative_reviews': negative_reviews,
            'active_days': active_days
        })
    
    df = pd.DataFrame(data)
    logger.info(f"Generated {len(df)} products for recommendation")
    
    return df


def main():
    """Generate sample data"""
    try:
        logger.info("[SAMPLE DATA GENERATION] Creating demo dataset")
        logger.info("="*60)
        
        # Generate demand data
        demand_df = generate_demand_data(n_products=100, n_days=180)
        
        # Generate recommendation data
        recommendation_df = generate_recommendation_data(n_products=100)
        
        # Save data
        demand_dir = Path('data/demand_prediction')
        demand_dir.mkdir(parents=True, exist_ok=True)
        
        recommendation_dir = Path('data/product_recommendation')
        recommendation_dir.mkdir(parents=True, exist_ok=True)
        
        demand_df.to_csv(demand_dir / 'raw_demand_data.csv', index=False)
        logger.info(f"✓ Saved to {demand_dir / 'raw_demand_data.csv'}")
        
        recommendation_df.to_csv(recommendation_dir / 'raw_recommendation_data.csv', index=False)
        logger.info(f"✓ Saved to {recommendation_dir / 'raw_recommendation_data.csv'}")
        
        logger.info("\n" + "="*60)
        logger.info("✓ SAMPLE DATA GENERATION COMPLETED")
        logger.info("="*60)
        logger.info("\nNow run pipeline:")
        logger.info("  python 2_data_preparation.py")
        logger.info("  python 3_model_training.py")
        logger.info("  python 4_model_evaluation.py")
        logger.info("  python 5_model_serving.py")
        
    except Exception as e:
        logger.error(f"\n✗ FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
