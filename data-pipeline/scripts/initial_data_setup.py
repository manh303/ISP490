#!/usr/bin/env python3
"""
Initial Data Setup Script
Chạy script này để setup data ban đầu cho dự án
"""

import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / 'src'))

from collectors.kaggle_collector import KaggleCollector
from collectors.web_scraper import WebScraper
from processors.data_cleaner import DataCleaner
from utils.database import DatabaseManager
from loguru import logger

def main():
    logger.info("=== Starting Initial Data Setup ===")
    
    # Initialize components
    kaggle_collector = KaggleCollector()
    web_scraper = WebScraper()
    data_cleaner = DataCleaner()
    db_manager = DatabaseManager()
    
    # Step 1: Download Kaggle datasets
    logger.info("Step 1: Downloading Kaggle datasets")
    
    # Recommended datasets cho ecommerce
    datasets = [
        'carrie1/ecommerce-data',  # Classic ecommerce dataset
        'mkechinov/ecommerce-behavior-data-from-multi-category-store',
        'olistbr/brazilian-ecommerce'  # Large Brazilian ecommerce dataset
    ]
    
    downloaded_data = []
    for dataset in datasets:
        try:
            path = kaggle_collector.download_dataset(dataset)
            if path:
                files = kaggle_collector.list_files_in_dataset(path)
                for file in files:
                    if file.suffix == '.csv':
                        df = kaggle_collector.load_csv_file(file)
                        if df is not None:
                            downloaded_data.append({
                                'name': dataset.split('/')[-1],
                                'dataframe': df,
                                'source': 'kaggle'
                            })
        except Exception as e:
            logger.warning(f"Could not download {dataset}: {e}")
    
    # Step 2: Generate sample scraped data
    logger.info("Step 2: Generating sample scraped data")
    scraped_df = web_scraper.scrape_sample_ecommerce(num_pages=10)
    downloaded_data.append({
        'name': 'scraped_sample',
        'dataframe': scraped_df,
        'source': 'scraper'
    })
    
    # Step 3: Clean all data
    logger.info("Step 3: Cleaning data")
    cleaned_data = []
    for data_item in downloaded_data:
        logger.info(f"Cleaning dataset: {data_item['name']}")
        cleaned_df = data_cleaner.clean_ecommerce_data(data_item['dataframe'])
        cleaned_data.append({
            'name': data_item['name'],
            'dataframe': cleaned_df,
            'source': data_item['source']
        })
    
    # Step 4: Save to databases
    logger.info("Step 4: Saving to databases")
    for data_item in cleaned_data:
        # Save to PostgreSQL (structured data)
        db_manager.save_to_postgres(
            data_item['dataframe'], 
            f"raw_{data_item['name']}"
        )
        
        # Save to MongoDB (flexible schema)
        db_manager.save_to_mongo(
            data_item['dataframe'], 
            f"raw_{data_item['name']}"
        )
    
    logger.success("=== Initial Data Setup Completed ===")
    logger.info(f"Total datasets processed: {len(cleaned_data)}")

if __name__ == "__main__":
    main()