#!/usr/bin/env python3
"""
Olist Data Integration - Join staging tables
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent / 'src'))

import pandas as pd
from loguru import logger
from utils.database import DatabaseManager
from datetime import datetime

def main():
    """Main integration function"""
    
    logger.info("Starting Olist Data Integration")
    logger.info("="*60)
    
    db_manager = DatabaseManager()
    
    # Try to load main tables
    tables_to_load = {
        'customers': 'olist_customers_staging',
        'orders': 'olist_orders_staging', 
        'products': 'olist_products_staging',
        'order_items': 'olist_order_items_staging'
    }
    
    loaded_tables = {}
    
    for table_key, table_name in tables_to_load.items():
        try:
            df = db_manager.load_from_postgres(f"SELECT * FROM {table_name}")
            loaded_tables[table_key] = df
            logger.info(f"Loaded {table_key}: {len(df)} rows")
        except Exception as e:
            logger.warning(f"Could not load {table_name}: {e}")
    
    if len(loaded_tables) < 2:
        logger.error("Not enough tables loaded for integration")
        return False
    
    # Start with orders as base
    if 'orders' in loaded_tables:
        df_integrated = loaded_tables['orders'].copy()
        logger.info(f"Base dataset: {len(df_integrated)} orders")
    else:
        logger.error("Orders table not available")
        return False
    
    # Join with customers
    if 'customers' in loaded_tables:
        try:
            before_join = len(df_integrated)
            df_integrated = df_integrated.merge(
                loaded_tables['customers'], 
                on='customer_id', 
                how='left'
            )
            logger.info(f"Joined customers: {before_join} -> {len(df_integrated)} rows")
        except Exception as e:
            logger.error(f"Failed to join customers: {e}")
    
    # Join with order items
    if 'order_items' in loaded_tables:
        try:
            before_join = len(df_integrated)
            df_integrated = df_integrated.merge(
                loaded_tables['order_items'], 
                on='order_id', 
                how='left'
            )
            logger.info(f"Joined order items: {before_join} -> {len(df_integrated)} rows")
        except Exception as e:
            logger.error(f"Failed to join order items: {e}")
    
    # Join with products
    if 'products' in loaded_tables and 'product_id' in df_integrated.columns:
        try:
            before_join = len(df_integrated)
            df_integrated = df_integrated.merge(
                loaded_tables['products'], 
                on='product_id', 
                how='left'
            )
            logger.info(f"Joined products: {before_join} -> {len(df_integrated)} rows")
        except Exception as e:
            logger.error(f"Failed to join products: {e}")
    
    # Add integration metadata
    df_integrated['integration_date'] = datetime.now()
    
    # Save integrated dataset
    try:
        db_manager.save_to_postgres(df_integrated, 'olist_integrated_dataset')
        logger.success("Integrated dataset saved to: olist_integrated_dataset")
        
        # Summary
        logger.info("="*60)
        logger.info("INTEGRATION COMPLETE")
        logger.info(f"Final dataset: {len(df_integrated):,} rows x {len(df_integrated.columns)} columns")
        
        if 'customer_id' in df_integrated.columns:
            logger.info(f"Unique customers: {df_integrated['customer_id'].nunique():,}")
        
        if 'product_id' in df_integrated.columns:
            logger.info(f"Unique products: {df_integrated['product_id'].nunique():,}")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to save integrated dataset: {e}")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)