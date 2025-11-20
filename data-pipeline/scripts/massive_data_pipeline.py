# File: scripts/massive_data_pipeline.py
#!/usr/bin/env python3

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent / 'src'))

from generators.synthetic_data_generator import SyntheticEcommerceGenerator
from collectors.multi_source_scraper import MultiSourceScraper
from collectors.api_collectors import APICollector
from utils.database import DatabaseManager
from loguru import logger
import pandas as pd

class MassiveDataPipeline:
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.target_volume = {
            'customers': 1_000_000,      # 1M customers
            'products': 500_000,         # 500K products  
            'transactions': 10_000_000,  # 10M transactions
        }
    
    def generate_massive_dataset(self):
        """Generate massive synthetic dataset"""
        logger.info("Generating massive synthetic dataset...")
        
        generator = SyntheticEcommerceGenerator(scale_factor=1)
        
        # Generate customers
        logger.info(f"Generating {self.target_volume['customers']:,} customers...")
        customers_df = generator.generate_customers(self.target_volume['customers'])
        self.db_manager.save_to_postgres(customers_df, 'massive_customers')
        
        # Generate products  
        logger.info(f"Generating {self.target_volume['products']:,} products...")
        products_df = generator.generate_products(self.target_volume['products'])
        self.db_manager.save_to_postgres(products_df, 'massive_products')
        
        # Generate transactions (in batches to avoid memory issues)
        logger.info("Generating massive transaction dataset...")
        batch_size = 100_000  # Process customers in batches
        
        for batch_start in range(0, len(customers_df), batch_size):
            batch_end = min(batch_start + batch_size, len(customers_df))
            customer_batch = customers_df.iloc[batch_start:batch_end]
            
            logger.info(f"Processing customer batch {batch_start:,} to {batch_end:,}")
            
            # Sample products for this batch
            product_sample = products_df.sample(n=10000)  # Use subset for performance
            
            transactions_batch = generator.generate_transactions(
                customer_batch, product_sample, transactions_per_customer=10
            )
            
            # Save batch
            self.db_manager.save_to_postgres(
                transactions_batch, 
                'massive_transactions',
                if_exists='append' if batch_start > 0 else 'replace'
            )
            
            logger.info(f"Saved {len(transactions_batch):,} transactions")
        
        return True
    
    def collect_external_data(self):
        """Collect data from external sources"""
        scraper = MultiSourceScraper()
        api_collector = APICollector()
        
        # Web scraping
        fake_store_data = scraper.scrape_fake_store_api()
        if not fake_store_data.empty:
            self.db_manager.save_to_postgres(fake_store_data, 'external_products_fakestore')
        
        dummyjson_data = scraper.scrape_dummyjson_products()
        if not dummyjson_data.empty:
            self.db_manager.save_to_postgres(dummyjson_data, 'external_products_dummyjson')
        
        # API data
        currency_data = api_collector.get_currency_data()
        if not currency_data.empty:
            self.db_manager.save_to_postgres(currency_data, 'external_currency_rates')
        
        return True
    
    def run_massive_pipeline(self):
        """Run complete massive data generation pipeline"""
        logger.info("Starting Massive Data Pipeline")
        logger.info("Target volumes:")
        for entity, count in self.target_volume.items():
            logger.info(f"  {entity}: {count:,}")
        
        # Generate synthetic data
        self.generate_massive_dataset()
        
        # Collect external data
        self.collect_external_data()
        
        # Final statistics
        stats = {}
        tables = ['massive_customers', 'massive_products', 'massive_transactions']
        
        for table in tables:
            try:
                result = self.db_manager.load_from_postgres(f"SELECT COUNT(*) as count FROM {table}")
                stats[table] = result.iloc[0]['count']
            except:
                stats[table] = 0
        
        logger.info("Pipeline Complete - Final Statistics:")
        for table, count in stats.items():
            logger.info(f"  {table}: {count:,} records")
        
        total_records = sum(stats.values())
        logger.info(f"Total records generated: {total_records:,}")
        
        return total_records > 1_000_000  # Success if > 1M total records

def main():
    pipeline = MassiveDataPipeline()
    success = pipeline.run_massive_pipeline()
    
    if success:
        logger.success("Massive data pipeline completed successfully!")
        logger.info("You now have Big Data volumes for proper analysis")
    else:
        logger.error("Pipeline failed to generate sufficient data volume")
    
    return success

if __name__ == "__main__":
    main()