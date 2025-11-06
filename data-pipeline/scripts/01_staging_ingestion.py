#!/usr/bin/env python3
"""
Staging Data Ingestion Script for Tiki & Lazada
Simple script to load raw crawler data into staging tables
Can be called by Airflow or run standalone
"""

import os
import sys
import json
import logging
import psycopg2
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
import uuid

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StagingIngestion:
    """Simple staging data ingestion for Tiki and Lazada"""

    def __init__(self, db_config: Dict[str, str]):
        self.db_config = db_config
        self.conn = None
        self.platforms = ['tiki', 'lazada']

    def connect(self):
        """Connect to database"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.conn.autocommit = True
            logger.info("✅ Connected to database")
            return True
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            return False

    def ingest_products(self, data_directories: List[str], date_filter: str = None) -> Dict[str, int]:
        """
        Ingest product data from JSON files

        Args:
            data_directories: List of directories to scan for JSON files
            date_filter: Optional date filter (YYYY-MM-DD)

        Returns:
            Statistics dictionary
        """
        stats = {'tiki': 0, 'lazada': 0, 'total': 0, 'files_processed': 0}
        load_id = f"staging_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

        cursor = self.conn.cursor()

        try:
            for data_dir in data_directories:
                if not os.path.exists(data_dir):
                    logger.warning(f"⚠️ Directory not found: {data_dir}")
                    continue

                # Find JSON files
                for json_file in Path(data_dir).glob('**/*.json'):
                    platform = self._detect_platform(str(json_file))
                    if not platform:
                        continue

                    # Optional date filtering
                    if date_filter and not self._file_matches_date(json_file, date_filter):
                        continue

                    count = self._process_product_file(cursor, json_file, platform, load_id)
                    stats[platform] += count
                    stats['total'] += count
                    stats['files_processed'] += 1

                    logger.info(f"📁 Processed {json_file.name}: {count} products ({platform})")

            logger.info(f"🎯 Staging ingestion completed: {stats}")
            return stats

        except Exception as e:
            logger.error(f"❌ Ingestion failed: {e}")
            raise
        finally:
            cursor.close()

    def _detect_platform(self, file_path: str) -> Optional[str]:
        """Detect platform from file path/name"""
        file_path_lower = file_path.lower()

        if any(pattern in file_path_lower for pattern in ['tiki', 'working_tiki']):
            return 'tiki'
        elif any(pattern in file_path_lower for pattern in ['lazada', 'enhanced_lazada', 'working_lazada']):
            return 'lazada'

        return None

    def _file_matches_date(self, file_path: Path, date_filter: str) -> bool:
        """Check if file matches date filter"""
        # Check file modification time or filename patterns
        try:
            file_date = datetime.fromtimestamp(file_path.stat().st_mtime).strftime('%Y-%m-%d')
            return file_date == date_filter
        except:
            return True  # If can't determine date, include the file

    def _process_product_file(self, cursor, file_path: Path, platform: str, load_id: str) -> int:
        """Process a single JSON file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            if not isinstance(data, list):
                data = [data]

            count = 0
            for item in data:
                try:
                    # Extract basic info
                    url = item.get('url', '')
                    product_id = (item.get('product_id') or
                                item.get('id') or
                                item.get('platform_product_id') or
                                str(uuid.uuid4())[:8])

                    crawled_at = self._parse_timestamp(
                        item.get('crawl_date') or
                        item.get('crawled_at') or
                        datetime.now()
                    )

                    checksum = hashlib.md5(json.dumps(item, sort_keys=True).encode()).hexdigest()

                    # Insert into staging
                    cursor.execute("""
                        INSERT INTO stg_raw_products (
                            source_platform, url, platform_product_id,
                            crawled_at, raw_data, checksum, load_id
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (source_platform, platform_product_id, checksum) DO NOTHING
                    """, (platform, url, product_id, crawled_at, json.dumps(item), checksum, load_id))

                    if cursor.rowcount > 0:
                        count += 1

                except Exception as e:
                    logger.warning(f"⚠️ Failed to process item: {e}")
                    continue

            return count

        except Exception as e:
            logger.error(f"❌ Failed to process file {file_path}: {e}")
            return 0

    def _parse_timestamp(self, timestamp_val: Any) -> datetime:
        """Parse timestamp from various formats"""
        if isinstance(timestamp_val, datetime):
            return timestamp_val

        if not timestamp_val:
            return datetime.now()

        try:
            # Common timestamp formats
            formats = [
                '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d',
                '%d/%m/%Y %H:%M:%S',
                '%d/%m/%Y'
            ]

            for fmt in formats:
                try:
                    return datetime.strptime(str(timestamp_val), fmt)
                except ValueError:
                    continue

        except Exception:
            pass

        return datetime.now()

    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("🔒 Database connection closed")


def main():
    """Main function for standalone execution"""
    import argparse

    parser = argparse.ArgumentParser(description='Staging Data Ingestion for Tiki & Lazada')
    parser.add_argument('--date', help='Process files for specific date (YYYY-MM-DD)')
    parser.add_argument('--data-dirs', nargs='+',
                       default=[
                           'C:/DoAn_FPT_FALL2025/ecommerce-dss-project/data-collection/data',
                           'C:/DoAn_FPT_FALL2025/ecommerce-dss-project/data-collection/crawlers/outputs'
                       ],
                       help='Data directories to scan')

    args = parser.parse_args()

    # Database configuration
    db_config = {
        'host': 'localhost',
        'port': 5433,
        'database': 'ecommerce_dss',
        'user': 'dss_user',
        'password': 'dss_password_123'
    }

    # Run ingestion
    ingestion = StagingIngestion(db_config)

    if not ingestion.connect():
        logger.error("❌ Cannot connect to database")
        return False

    try:
        stats = ingestion.ingest_products(args.data_dirs, args.date)
        logger.info(f"✅ Staging ingestion completed successfully: {stats['total']} total products")
        return True

    except Exception as e:
        logger.error(f"❌ Staging ingestion failed: {e}")
        return False

    finally:
        ingestion.close()


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)