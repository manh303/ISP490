#!/usr/bin/env python3
"""
Optimized Batch Processing Module for E-commerce DSS
Handles large-scale data processing with memory-efficient techniques
"""

import os
import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Any, Optional
import multiprocessing as mp
from concurrent.futures import ThreadPoolExecutor, as_completed
import gc
import psutil

# Database connections
from sqlalchemy import create_engine, text
import pymongo
import redis

# Configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class OptimizedBatchProcessor:
    """
    Optimized batch processor with memory management and parallel processing
    """

    def __init__(self, batch_size: int = 50000, max_workers: int = None):
        self.batch_size = batch_size
        self.max_workers = max_workers or min(mp.cpu_count(), 8)
        self.memory_threshold = 0.8  # 80% memory usage threshold

        # Database connections
        self.postgres_engine = None
        self.mongo_client = None
        self.redis_client = None

        self._setup_connections()

    def _setup_connections(self):
        """Setup database connections"""
        try:
            # PostgreSQL
            db_url = os.getenv('DATABASE_URL', 'postgresql://admin:admin_password@postgres:5432/ecommerce_dss')
            self.postgres_engine = create_engine(db_url, pool_size=10, max_overflow=20)

            # MongoDB
            mongo_url = os.getenv('MONGODB_URI', 'mongodb://admin:admin_password@mongodb:27017/')
            self.mongo_client = pymongo.MongoClient(mongo_url)

            # Redis
            redis_url = os.getenv('REDIS_URL', 'redis://redis:6379/0')
            self.redis_client = redis.from_url(redis_url)

            logger.info("✅ Database connections established")
        except Exception as e:
            logger.error(f"❌ Failed to setup database connections: {e}")
            raise

    def check_memory_usage(self) -> float:
        """Check current memory usage"""
        return psutil.virtual_memory().percent / 100

    def process_csv_in_chunks(self, file_path: str, processor_func: callable) -> Dict[str, Any]:
        """
        Process large CSV files in memory-efficient chunks
        """
        logger.info(f"🔄 Processing {file_path} in chunks of {self.batch_size}")

        results = {
            'total_processed': 0,
            'chunks_processed': 0,
            'errors': []
        }

        try:
            # Get file size for progress tracking
            file_size = os.path.getsize(file_path)
            logger.info(f"📊 File size: {file_size / (1024**2):.2f} MB")

            # Process in chunks
            chunk_iter = pd.read_csv(file_path, chunksize=self.batch_size)

            for chunk_num, chunk in enumerate(chunk_iter):
                try:
                    # Memory check
                    if self.check_memory_usage() > self.memory_threshold:
                        logger.warning(f"⚠️ High memory usage: {self.check_memory_usage():.1%}")
                        gc.collect()

                    # Process chunk
                    processed_chunk = processor_func(chunk, chunk_num)

                    results['total_processed'] += len(processed_chunk)
                    results['chunks_processed'] += 1

                    # Clear memory
                    del chunk, processed_chunk
                    gc.collect()

                    if chunk_num % 10 == 0:
                        logger.info(f"📈 Processed {results['chunks_processed']} chunks, {results['total_processed']} records")

                except Exception as e:
                    logger.error(f"❌ Error processing chunk {chunk_num}: {e}")
                    results['errors'].append(f"Chunk {chunk_num}: {str(e)}")

            logger.info(f"✅ Completed processing: {results['total_processed']} records in {results['chunks_processed']} chunks")
            return results

        except Exception as e:
            logger.error(f"❌ Failed to process file {file_path}: {e}")
            raise

    def parallel_process_files(self, file_paths: List[str], processor_func: callable) -> Dict[str, Any]:
        """
        Process multiple files in parallel
        """
        logger.info(f"🚀 Processing {len(file_paths)} files in parallel with {self.max_workers} workers")

        results = {
            'files_processed': 0,
            'total_records': 0,
            'errors': [],
            'file_results': {}
        }

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit tasks
            future_to_file = {
                executor.submit(self.process_csv_in_chunks, file_path, processor_func): file_path
                for file_path in file_paths if Path(file_path).exists()
            }

            # Collect results
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    file_result = future.result()
                    results['file_results'][file_path] = file_result
                    results['files_processed'] += 1
                    results['total_records'] += file_result['total_processed']

                    logger.info(f"✅ Completed {Path(file_path).name}: {file_result['total_processed']} records")

                except Exception as e:
                    logger.error(f"❌ Failed to process {file_path}: {e}")
                    results['errors'].append(f"{file_path}: {str(e)}")

        logger.info(f"🎯 Parallel processing completed: {results['files_processed']} files, {results['total_records']} total records")
        return results

class DataProcessors:
    """
    Collection of optimized data processing functions
    """

    @staticmethod
    def process_products_chunk(chunk: pd.DataFrame, chunk_num: int) -> pd.DataFrame:
        """Process products data chunk"""
        try:
            # Data cleaning and transformation
            chunk_clean = chunk.copy()

            # Handle missing values
            chunk_clean['product_weight_g'] = chunk_clean['product_weight_g'].fillna(chunk_clean['product_weight_g'].median())
            chunk_clean['product_length_cm'] = chunk_clean['product_length_cm'].fillna(0)
            chunk_clean['product_height_cm'] = chunk_clean['product_height_cm'].fillna(0)
            chunk_clean['product_width_cm'] = chunk_clean['product_width_cm'].fillna(0)

            # Calculate product volume
            chunk_clean['product_volume_cm3'] = (
                chunk_clean['product_length_cm'] *
                chunk_clean['product_height_cm'] *
                chunk_clean['product_width_cm']
            )

            # Categorize by weight
            chunk_clean['weight_category'] = pd.cut(
                chunk_clean['product_weight_g'],
                bins=[0, 100, 500, 1000, 5000, float('inf')],
                labels=['very_light', 'light', 'medium', 'heavy', 'very_heavy']
            )

            # Add processing metadata
            chunk_clean['processed_at'] = datetime.now()
            chunk_clean['chunk_number'] = chunk_num
            chunk_clean['processing_batch'] = f"batch_{datetime.now().strftime('%Y%m%d_%H')}"

            return chunk_clean

        except Exception as e:
            logger.error(f"❌ Error processing products chunk {chunk_num}: {e}")
            return chunk  # Return original chunk if processing fails

    @staticmethod
    def process_customers_chunk(chunk: pd.DataFrame, chunk_num: int) -> pd.DataFrame:
        """Process customers data chunk"""
        try:
            chunk_clean = chunk.copy()

            # Clean zip codes
            chunk_clean['customer_zip_code_prefix'] = chunk_clean['customer_zip_code_prefix'].astype(str).str.zfill(5)

            # Standardize city names
            chunk_clean['customer_city'] = chunk_clean['customer_city'].str.title().str.strip()

            # Create customer segments
            state_mapping = {
                'SP': 'southeast', 'RJ': 'southeast', 'MG': 'southeast', 'ES': 'southeast',
                'PR': 'south', 'RS': 'south', 'SC': 'south',
                'GO': 'center_west', 'MT': 'center_west', 'MS': 'center_west', 'DF': 'center_west',
                'BA': 'northeast', 'PE': 'northeast', 'CE': 'northeast', 'MA': 'northeast',
                'AM': 'north', 'PA': 'north', 'RO': 'north', 'AC': 'north'
            }

            chunk_clean['customer_region'] = chunk_clean['customer_state'].map(state_mapping).fillna('other')

            # Add processing metadata
            chunk_clean['processed_at'] = datetime.now()
            chunk_clean['chunk_number'] = chunk_num
            chunk_clean['processing_batch'] = f"batch_{datetime.now().strftime('%Y%m%d_%H')}"

            return chunk_clean

        except Exception as e:
            logger.error(f"❌ Error processing customers chunk {chunk_num}: {e}")
            return chunk

    @staticmethod
    def process_orders_chunk(chunk: pd.DataFrame, chunk_num: int) -> pd.DataFrame:
        """Process orders data chunk"""
        try:
            chunk_clean = chunk.copy()

            # Convert date columns
            date_columns = ['order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date']
            for col in date_columns:
                if col in chunk_clean.columns:
                    chunk_clean[col] = pd.to_datetime(chunk_clean[col], errors='coerce')

            # Calculate delivery time metrics
            if 'order_purchase_timestamp' in chunk_clean.columns and 'order_delivered_customer_date' in chunk_clean.columns:
                chunk_clean['delivery_time_days'] = (
                    chunk_clean['order_delivered_customer_date'] - chunk_clean['order_purchase_timestamp']
                ).dt.days

            # Add processing metadata
            chunk_clean['processed_at'] = datetime.now()
            chunk_clean['chunk_number'] = chunk_num
            chunk_clean['processing_batch'] = f"batch_{datetime.now().strftime('%Y%m%d_%H')}"

            return chunk_clean

        except Exception as e:
            logger.error(f"❌ Error processing orders chunk {chunk_num}: {e}")
            return chunk

def main():
    """Main batch processing function"""
    logger.info("🚀 Starting optimized batch processing")

    # Initialize processor
    processor = OptimizedBatchProcessor(batch_size=50000, max_workers=4)

    # Define data sources
    data_sources = [
        {
            'path': '/app/data/raw/kaggle_datasets/data.csv',
            'processor': DataProcessors.process_products_chunk,
            'output_table': 'products_processed'
        },
        {
            'path': '/app/data/integrated_data/vietnamese_ecommerce_integrated.csv',
            'processor': DataProcessors.process_customers_chunk,
            'output_table': 'customers_processed'
        },
        {
            'path': '/app/data/real_datasets/kaggle_electronics_filtered.csv',
            'processor': DataProcessors.process_products_chunk,
            'output_table': 'electronics_processed'
        }
    ]

    # Process each data source
    for source in data_sources:
        if Path(source['path']).exists():
            logger.info(f"🔄 Processing {source['path']}")

            try:
                results = processor.process_csv_in_chunks(source['path'], source['processor'])
                logger.info(f"✅ Processed {source['path']}: {results['total_processed']} records")

                # Store results in Redis for monitoring
                processor.redis_client.setex(
                    f"batch_processing_result_{source['output_table']}",
                    timedelta(days=1),
                    str(results)
                )

            except Exception as e:
                logger.error(f"❌ Failed to process {source['path']}: {e}")
        else:
            logger.warning(f"⚠️ File not found: {source['path']}")

    logger.info("🎯 Batch processing completed")

if __name__ == "__main__":
    main()
