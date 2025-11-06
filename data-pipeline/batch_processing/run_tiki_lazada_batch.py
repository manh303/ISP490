#!/usr/bin/env python3
"""
Batch Runner for Tiki & Lazada Data Processing
Simple script to execute the batch pipeline with different configurations
"""

import os
import sys
import argparse
from datetime import datetime, timedelta
from tiki_lazada_batch_pipeline import TikiLazadaBatchPipeline
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'batch_processing_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def run_daily_batch(date_str=None):
    """Run daily batch processing for specific date or today"""
    if not date_str:
        date_str = datetime.now().strftime('%Y-%m-%d')

    logger.info(f"🚀 Starting daily batch processing for {date_str}")

    # Database configuration
    db_config = {
        'host': 'localhost',
        'port': 5433,
        'database': 'ecommerce_dss',
        'user': 'dss_user',
        'password': 'dss_password_123'
    }

    # Data directories
    data_directories = [
        'C:/DoAn_FPT_FALL2025/ecommerce-dss-project/data-collection/data',
        'C:/DoAn_FPT_FALL2025/ecommerce-dss-project/data-collection/crawlers/outputs'
    ]

    # Run pipeline
    pipeline = TikiLazadaBatchPipeline(db_config)

    if not pipeline.connect_database():
        logger.error("❌ Failed to connect to database")
        return False

    try:
        pipeline.run_batch_pipeline(data_directories, date_str)
        logger.info(f"✅ Daily batch completed successfully for {date_str}")
        return True

    except Exception as e:
        logger.error(f"❌ Daily batch failed for {date_str}: {e}")
        return False

    finally:
        pipeline.close_connection()

def run_backfill_batch(start_date, end_date):
    """Run backfill processing for date range"""
    logger.info(f"🔄 Starting backfill batch processing from {start_date} to {end_date}")

    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')

    success_count = 0
    fail_count = 0

    current_date = start_dt
    while current_date <= end_dt:
        date_str = current_date.strftime('%Y-%m-%d')

        if run_daily_batch(date_str):
            success_count += 1
        else:
            fail_count += 1

        current_date += timedelta(days=1)

    logger.info(f"🎯 Backfill completed: {success_count} success, {fail_count} failed")

def validate_environment():
    """Validate environment and dependencies"""
    logger.info("🔍 Validating environment...")

    # Check Python packages
    required_packages = ['psycopg2', 'pandas']
    for package in required_packages:
        try:
            __import__(package)
            logger.info(f"✅ {package} is available")
        except ImportError:
            logger.error(f"❌ {package} is not installed")
            return False

    # Check data directories
    data_dirs = [
        'C:/DoAn_FPT_FALL2025/ecommerce-dss-project/data-collection/data',
        'C:/DoAn_FPT_FALL2025/ecommerce-dss-project/data-collection/crawlers/outputs'
    ]

    for data_dir in data_dirs:
        if os.path.exists(data_dir):
            logger.info(f"✅ Data directory exists: {data_dir}")
        else:
            logger.warning(f"⚠️ Data directory not found: {data_dir}")

    return True

def main():
    """Main function with CLI interface"""
    parser = argparse.ArgumentParser(description='Tiki & Lazada Batch Processing Runner')
    parser.add_argument('--mode', choices=['daily', 'backfill', 'validate'],
                       default='daily', help='Processing mode')
    parser.add_argument('--date', help='Specific date (YYYY-MM-DD) for daily mode')
    parser.add_argument('--start-date', help='Start date (YYYY-MM-DD) for backfill mode')
    parser.add_argument('--end-date', help='End date (YYYY-MM-DD) for backfill mode')

    args = parser.parse_args()

    logger.info("🎯 Tiki & Lazada Batch Processing Runner")
    logger.info(f"Mode: {args.mode}")

    if args.mode == 'validate':
        if validate_environment():
            logger.info("✅ Environment validation passed")
        else:
            logger.error("❌ Environment validation failed")
            sys.exit(1)

    elif args.mode == 'daily':
        if not validate_environment():
            logger.error("❌ Environment validation failed")
            sys.exit(1)

        success = run_daily_batch(args.date)
        sys.exit(0 if success else 1)

    elif args.mode == 'backfill':
        if not args.start_date or not args.end_date:
            logger.error("❌ Backfill mode requires --start-date and --end-date")
            sys.exit(1)

        if not validate_environment():
            logger.error("❌ Environment validation failed")
            sys.exit(1)

        run_backfill_batch(args.start_date, args.end_date)

if __name__ == "__main__":
    main()