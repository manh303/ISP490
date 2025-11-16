# -*- coding: utf-8 -*-
"""
Step 1: Data Extraction from DWH
Lấy dữ liệu từ DWH cho Demand Prediction và Product Recommendation
"""

import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import os
import pandas as pd
from pathlib import Path
from utils.db_connector import DWHConnector
from utils.logger import get_logger
import yaml

logger = get_logger("data_extraction")

# Load config
with open('config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)


def extract_demand_data():
    """Extract data for demand prediction"""
    logger.info("\n" + "="*60)
    logger.info("EXTRACTING DEMAND PREDICTION DATA")
    logger.info("="*60)
    
    conn = DWHConnector()
    days = config['data_extraction']['demand']['lookback_days']
    
    sql = f"""
    SELECT 
        fac.agg_date,
        fac.source_platform_std,
        fac.category_lvl1,
        fac.category_lvl2,
        fac.category_lvl3,
        fac.category_std,
        fac.distinct_products,
        fac.avg_price,
        fac.min_price,
        fac.max_price,
        fac.total_review_count,
        EXTRACT(YEAR FROM fac.agg_date)::INT as year,
        EXTRACT(MONTH FROM fac.agg_date)::INT as month,
        EXTRACT(DAY FROM fac.agg_date)::INT as day,
        EXTRACT(DOW FROM fac.agg_date)::INT as day_of_week
    FROM dwh.fact_product_daily_agg fac
    WHERE fac.agg_date >= CURRENT_DATE - INTERVAL '{days} days'
    ORDER BY fac.agg_date DESC
    """
    
    try:
        df = conn.query(sql)
        logger.info(f"[OK] Total records: {len(df)}")
        logger.info(f"[OK] Date range: {df['agg_date'].min()} to {df['agg_date'].max()}")
        logger.info(f"[OK] Categories: {df['category_std'].nunique()}")
        
        # Data quality checks
        logger.info("\nData Quality Checks:")
        logger.info(f"  Missing avg_price: {df['avg_price'].isna().sum()}")
        logger.info(f"  Missing total_review_count: {df['total_review_count'].isna().sum()}")
        
        # Save raw data
        output_dir = Path(config['data_extraction']['demand']['output_dir'])
        output_dir.mkdir(parents=True, exist_ok=True)
        
        df.to_csv(output_dir / 'raw_demand_data.csv', index=False)
        logger.info(f"[OK] Saved to {output_dir / 'raw_demand_data.csv'}")
        
        return df
    
    except Exception as e:
        logger.error(f"[ERROR] Error extracting demand data: {e}")
        raise
    
    finally:
        conn.close()


def extract_recommendation_data():
    """Extract data for product recommendation"""
    logger.info("\n" + "="*60)
    logger.info("EXTRACTING PRODUCT RECOMMENDATION DATA")
    logger.info("="*60)
    
    conn = DWHConnector()
    days = config['data_extraction']['recommendation']['lookback_days']
    
    sql = f"""
    SELECT 
        fac.global_product_id,
        fac.source_platform_std,
        fac.total_reviews,
        fac.avg_rating,
        fac.avg_sentiment_score,
        fac.positive_reviews,
        fac.negative_reviews,
        fac.neutral_reviews,
        fac.positive_sentiment_pct,
        fac.negative_sentiment_pct,
        fac.review_quality_score,
        COUNT(DISTINCT fac.agg_date) as active_days
    FROM dwh.fact_review_daily_agg fac
    WHERE fac.agg_date >= CURRENT_DATE - INTERVAL '{days} days'
    AND fac.total_reviews > 0
    GROUP BY fac.global_product_id, fac.source_platform_std, 
             fac.total_reviews, fac.avg_rating, fac.avg_sentiment_score,
             fac.positive_reviews, fac.negative_reviews, fac.neutral_reviews,
             fac.positive_sentiment_pct, fac.negative_sentiment_pct,
             fac.review_quality_score
    HAVING SUM(fac.total_reviews) > 0
    ORDER BY SUM(fac.total_reviews) DESC
    """
    
    try:
        df = conn.query(sql)
        logger.info(f"[OK] Total products: {len(df)}")
        logger.info(f"[OK] Source platforms: {df['source_platform_std'].nunique()}")
        logger.info(f"[OK] Avg reviews per product: {df['total_reviews'].mean():.0f}")
        
        # Filter by min interactions
        min_interactions = config['data_extraction']['recommendation']['min_interactions']
        df = df[df['total_reviews'] >= min_interactions]
        
        logger.info(f"\nAfter filtering (min {min_interactions} reviews):")
        logger.info(f"  Products: {len(df)}")
        
        # Data quality checks
        logger.info("\nData Quality Checks:")
        logger.info(f"  Missing avg_rating: {df['avg_rating'].isna().sum()}")
        logger.info(f"  Missing avg_sentiment_score: {df['avg_sentiment_score'].isna().sum()}")
        
        # Save raw data
        output_dir = Path(config['data_extraction']['recommendation']['output_dir'])
        output_dir.mkdir(parents=True, exist_ok=True)
        
        df.to_csv(output_dir / 'raw_recommendation_data.csv', index=False)
        logger.info(f"[OK] Saved to {output_dir / 'raw_recommendation_data.csv'}")
        
        return df
    
    except Exception as e:
        logger.error(f"[ERROR] Error extracting recommendation data: {e}")
        raise
    
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        logger.info("[ML PIPELINE] Step 1: Data Extraction")
        
        # Extract both datasets
        demand_df = extract_demand_data()
        recommendation_df = extract_recommendation_data()
        
        logger.info("\n" + "="*60)
        logger.info("[OK] DATA EXTRACTION COMPLETED")
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"\n[FAILED] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
