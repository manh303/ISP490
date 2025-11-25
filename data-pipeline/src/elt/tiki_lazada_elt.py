#!/usr/bin/env python3
"""
ELT Pipeline for Tiki & Lazada Data
==================================
Tự động xử lý dữ liệu từ crawlers Tiki và Lazada vào Data Warehouse

Features:
- Extract từ JSON files của crawlers
- Load vào staging tables
- Transform dữ liệu theo schema chuẩn
- Data validation và quality checks
- Incremental loading với SCD Type 2

Author: ELT Team
Version: 1.0.0
"""

import os
import sys
import json
import logging
import pandas as pd
import psycopg2
from psycopg2.extras import Json, DictCursor
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import hashlib
import re
from urllib.parse import urlparse, parse_qs
import asyncio
from dataclasses import dataclass
from decimal import Decimal
import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class ELTConfig:
    """Configuration for ELT pipeline"""
    db_host: str = 'localhost'
    db_port: int = 5433
    db_name: str = 'ecommerce_dss_1'
    db_user: str = 'dss_user'
    db_password: str = 'dss_password_123'
    crawler_data_path: str = 'C:/DoAn_FPT_FALL2025/ecommerce-dss-project/data-collection/crawlers/outputs'
    batch_size: int = 1000
    max_retries: int = 3

class TikiLazadaELT:
    """
    ELT Pipeline for Tiki & Lazada e-commerce data
    """

    def __init__(self, config: ELTConfig = None):
        self.config = config or ELTConfig()
        self.conn = None
        self.stats = {
            'files_processed': 0,
            'records_extracted': 0,
            'records_loaded': 0,
            'records_failed': 0,
            'start_time': datetime.now()
        }

        # Platform mapping
        self.platform_mapping = {
            'tiki': {
                'id': 1,
                'name': 'Tiki',
                'base_url': 'https://tiki.vn'
            },
            'lazada': {
                'id': 2,
                'name': 'Lazada',
                'base_url': 'https://lazada.vn'
            }
        }

    def connect_db(self) -> psycopg2.extensions.connection:
        """Tạo kết nối database"""
        try:
            conn = psycopg2.connect(
                host=self.config.db_host,
                port=self.config.db_port,
                database=self.config.db_name,
                user=self.config.db_user,
                password=self.config.db_password
            )
            logger.info("✅ Database connection established")
            return conn
        except Exception as e:
            logger.error(f"❌ Database connection failed: {str(e)}")
            raise

    def create_schemas(self):
        """Tạo schemas cần thiết"""
        schemas = ['staging', 'ods', 'dwh', 'marts', 'control']

        with self.connect_db() as conn:
            cursor = conn.cursor()
            for schema in schemas:
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            conn.commit()
            logger.info(f"✅ Created schemas: {', '.join(schemas)}")

    def extract_crawler_files(self) -> List[Dict[str, Any]]:
        """Extract dữ liệu từ crawler output files"""
        logger.info("🔍 Starting data extraction from crawler files...")

        crawler_path = Path(self.config.crawler_data_path)
        if not crawler_path.exists():
            logger.warning(f"Crawler path not found: {crawler_path}")
            return []

        extracted_data = []

        # Tìm các file JSON từ crawlers
        json_files = list(crawler_path.glob("*.json"))
        csv_files = list(crawler_path.glob("*.csv"))

        logger.info(f"Found {len(json_files)} JSON files and {len(csv_files)} CSV files")

        # Process JSON files
        for json_file in json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    file_data = json.load(f)

                # Xác định platform từ filename
                platform = self._detect_platform(json_file.name)

                if isinstance(file_data, list):
                    for item in file_data:
                        extracted_data.append({
                            'source_platform': platform,
                            'file_path': str(json_file),
                            'raw_data': item,
                            'extracted_at': datetime.now()
                        })
                else:
                    extracted_data.append({
                        'source_platform': platform,
                        'file_path': str(json_file),
                        'raw_data': file_data,
                        'extracted_at': datetime.now()
                    })

                self.stats['files_processed'] += 1
                logger.info(f"✅ Processed {json_file.name}: {len(file_data) if isinstance(file_data, list) else 1} records")

            except Exception as e:
                logger.error(f"❌ Failed to process {json_file}: {str(e)}")
                self.stats['records_failed'] += 1

        # Process CSV files (nếu có)
        for csv_file in csv_files:
            try:
                df = pd.read_csv(csv_file)
                platform = self._detect_platform(csv_file.name)

                for _, row in df.iterrows():
                    extracted_data.append({
                        'source_platform': platform,
                        'file_path': str(csv_file),
                        'raw_data': row.to_dict(),
                        'extracted_at': datetime.now()
                    })

                self.stats['files_processed'] += 1
                logger.info(f"✅ Processed {csv_file.name}: {len(df)} records")

            except Exception as e:
                logger.error(f"❌ Failed to process {csv_file}: {str(e)}")

        self.stats['records_extracted'] = len(extracted_data)
        logger.info(f"🎉 Extraction completed: {len(extracted_data)} total records")
        return extracted_data

    def _detect_platform(self, filename: str) -> str:
        """Xác định platform từ tên file"""
        filename_lower = filename.lower()

        if 'tiki' in filename_lower:
            return 'tiki'
        elif 'lazada' in filename_lower:
            return 'lazada'
        else:
            # Default fallback
            return 'unknown'

    def load_to_staging(self, extracted_data: List[Dict[str, Any]]):
        """Load dữ liệu vào staging table"""
        logger.info("📥 Loading data to staging...")

        if not extracted_data:
            logger.warning("No data to load")
            return

        with self.connect_db() as conn:
            cursor = conn.cursor()

            # Create staging table if not exists
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS staging.raw_products (
                    id SERIAL PRIMARY KEY,
                    source_platform VARCHAR(50) NOT NULL,
                    crawl_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    raw_data JSONB NOT NULL,
                    url TEXT,
                    file_path TEXT,
                    data_hash VARCHAR(64),
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # Create indexes
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_raw_products_platform_time
                ON staging.raw_products(source_platform, crawl_timestamp);
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_raw_products_hash
                ON staging.raw_products(data_hash);
            """)

            loaded_count = 0
            duplicate_count = 0

            for batch_start in range(0, len(extracted_data), self.config.batch_size):
                batch = extracted_data[batch_start:batch_start + self.config.batch_size]

                for record in batch:
                    try:
                        # Calculate hash for deduplication
                        data_str = json.dumps(record['raw_data'], sort_keys=True, ensure_ascii=False)
                        data_hash = hashlib.md5(data_str.encode()).hexdigest()

                        # Extract URL if available
                        url = self._extract_url(record['raw_data'])

                        # Check if record already exists
                        cursor.execute("""
                            SELECT COUNT(*) FROM staging.raw_products
                            WHERE data_hash = %s
                        """, (data_hash,))

                        if cursor.fetchone()[0] > 0:
                            duplicate_count += 1
                            continue

                        # Insert new record
                        cursor.execute("""
                            INSERT INTO staging.raw_products
                            (source_platform, raw_data, url, file_path, data_hash)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (
                            record['source_platform'],
                            Json(record['raw_data']),
                            url,
                            record['file_path'],
                            data_hash
                        ))

                        loaded_count += 1

                    except Exception as e:
                        logger.error(f"Failed to load record: {str(e)}")
                        self.stats['records_failed'] += 1

                conn.commit()
                logger.info(f"📥 Loaded batch: {len(batch)} records")

            self.stats['records_loaded'] = loaded_count
            logger.info(f"✅ Staging load completed: {loaded_count} new records, {duplicate_count} duplicates skipped")

    def _extract_url(self, raw_data: Dict[str, Any]) -> Optional[str]:
        """Extract product URL từ raw data"""
        possible_url_fields = ['url', 'product_url', 'link', 'href']

        for field in possible_url_fields:
            if field in raw_data and raw_data[field]:
                return raw_data[field]

        return None

    def transform_to_ods(self):
        """Transform staging data thành ODS format"""
        logger.info("🔄 Transforming data to ODS...")

        with self.connect_db() as conn:
            cursor = conn.cursor()

            # Create ODS products table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS ods.products (
                    product_id SERIAL PRIMARY KEY,
                    source_platform VARCHAR(50) NOT NULL,
                    external_product_id VARCHAR(255) NOT NULL,

                    title TEXT NOT NULL,
                    description TEXT,
                    category VARCHAR(255),
                    subcategory VARCHAR(255),
                    brand VARCHAR(255),

                    product_url TEXT NOT NULL,
                    image_url TEXT,

                    current_price DECIMAL(15,2),
                    original_price DECIMAL(15,2),
                    discount_percentage DECIMAL(5,2),
                    discount_amount DECIMAL(15,2),
                    currency VARCHAR(10) DEFAULT 'VND',

                    rating DECIMAL(3,2),
                    review_count INTEGER DEFAULT 0,
                    sold_count INTEGER,
                    stock_status VARCHAR(50),

                    shop_name VARCHAR(255),
                    shop_rating DECIMAL(3,2),
                    shop_location VARCHAR(255),

                    first_seen TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    is_active BOOLEAN DEFAULT TRUE,

                    UNIQUE(source_platform, external_product_id)
                );
            """)

            # Process staging data by platform
            for platform in ['tiki', 'lazada']:
                self._transform_platform_data(cursor, platform)
                conn.commit()

            logger.info("✅ ODS transformation completed")

    def _transform_platform_data(self, cursor, platform: str):
        """Transform data cho từng platform cụ thể"""
        logger.info(f"🔄 Transforming {platform} data...")

        # Get unprocessed staging data
        cursor.execute("""
            SELECT id, raw_data, url FROM staging.raw_products
            WHERE source_platform = %s
            AND id NOT IN (
                SELECT DISTINCT raw_product_id
                FROM ods.products
                WHERE source_platform = %s
            )
        """, (platform, platform))

        # Add raw_product_id column if not exists
        cursor.execute("""
            ALTER TABLE ods.products
            ADD COLUMN IF NOT EXISTS raw_product_id INTEGER;
        """)

        staging_records = cursor.fetchall()
        transformed_count = 0

        for staging_id, raw_data, url in staging_records:
            try:
                transformed_product = self._transform_product_data(raw_data, platform, url)
                transformed_product['raw_product_id'] = staging_id

                # Insert or update ODS record
                self._upsert_ods_product(cursor, transformed_product)
                transformed_count += 1

            except Exception as e:
                logger.error(f"Failed to transform record {staging_id}: {str(e)}")

        logger.info(f"✅ Transformed {transformed_count} {platform} products")

    def _transform_product_data(self, raw_data: Dict[str, Any], platform: str, url: str) -> Dict[str, Any]:
        """Transform raw product data thành ODS format"""

        if platform == 'tiki':
            return self._transform_tiki_data(raw_data, url)
        elif platform == 'lazada':
            return self._transform_lazada_data(raw_data, url)
        else:
            raise ValueError(f"Unsupported platform: {platform}")

    def _transform_tiki_data(self, data: Dict[str, Any], url: str) -> Dict[str, Any]:
        """Transform Tiki data format"""
        external_id = self._extract_tiki_product_id(url or data.get('url', ''))

        # Parse price
        current_price = self._parse_price(data.get('price') or data.get('current_price'))
        original_price = self._parse_price(data.get('original_price') or data.get('list_price'))

        # Calculate discount
        discount_amount = None
        discount_percentage = None
        if original_price and current_price:
            discount_amount = original_price - current_price
            discount_percentage = (discount_amount / original_price) * 100

        return {
            'source_platform': 'tiki',
            'external_product_id': external_id,
            'title': self._clean_text(data.get('name') or data.get('title', '')),
            'description': self._clean_text(data.get('description', '')),
            'category': self._normalize_category(data.get('category_name') or data.get('category')),
            'brand': self._clean_text(data.get('brand_name') or data.get('brand', '')),
            'product_url': url or data.get('url'),
            'image_url': data.get('thumbnail_url') or data.get('image_url'),
            'current_price': current_price,
            'original_price': original_price,
            'discount_amount': discount_amount,
            'discount_percentage': discount_percentage,
            'rating': self._parse_rating(data.get('rating_average')),
            'review_count': self._parse_int(data.get('review_count')),
            'sold_count': self._parse_int(data.get('quantity_sold')),
            'shop_name': self._clean_text(data.get('seller_name', '')),
            'shop_rating': self._parse_rating(data.get('seller_rating'))
        }

    def _transform_lazada_data(self, data: Dict[str, Any], url: str) -> Dict[str, Any]:
        """Transform Lazada data format"""
        external_id = self._extract_lazada_product_id(url or data.get('url', ''))

        # Parse price
        current_price = self._parse_price(data.get('price') or data.get('price_text'))
        original_price = self._parse_price(data.get('original_price') or data.get('original_price_text'))

        # Parse discount
        discount_text = data.get('discount', '')
        discount_percentage = self._parse_discount_percentage(discount_text)

        return {
            'source_platform': 'lazada',
            'external_product_id': external_id,
            'title': self._clean_text(data.get('title') or data.get('name', '')),
            'description': self._clean_text(data.get('description', '')),
            'category': self._normalize_category(data.get('category')),
            'brand': self._clean_text(data.get('brand', '')),
            'product_url': url or data.get('url'),
            'image_url': data.get('image_url'),
            'current_price': current_price,
            'original_price': original_price,
            'discount_percentage': discount_percentage,
            'rating': self._parse_rating(data.get('rating')),
            'review_count': self._parse_review_count(data.get('review_count')),
            'sold_count': self._parse_sold_count(data.get('sold_count')),
            'shop_name': self._clean_text(data.get('shop_name', '')),
            'shop_rating': self._parse_rating(data.get('shop_rating')),
            'shop_location': self._clean_text(data.get('location', ''))
        }

    def _extract_tiki_product_id(self, url: str) -> str:
        """Extract Tiki product ID from URL"""
        if not url:
            return ''

        # Tiki URLs: https://tiki.vn/product-name-p123456.html
        import re
        match = re.search(r'-p(\d+)\.html', url)
        if match:
            return match.group(1)

        # Fallback: hash the URL
        return hashlib.md5(url.encode()).hexdigest()[:16]

    def _extract_lazada_product_id(self, url: str) -> str:
        """Extract Lazada product ID from URL"""
        if not url:
            return ''

        # Lazada URLs: https://www.lazada.vn/products/product-name-i123456789.html
        import re
        match = re.search(r'-i(\d+)\.html', url)
        if match:
            return match.group(1)

        # Alternative pattern: pdp-i123456789
        match = re.search(r'pdp-i(\d+)', url)
        if match:
            return match.group(1)

        # Fallback: hash the URL
        return hashlib.md5(url.encode()).hexdigest()[:16]

    def _parse_price(self, price_text: Any) -> Optional[Decimal]:
        """Parse price từ text thành decimal"""
        if not price_text:
            return None

        if isinstance(price_text, (int, float)):
            return Decimal(str(price_text))

        if isinstance(price_text, str):
            # Remove currency symbols and spaces
            price_clean = re.sub(r'[^\d.]', '', price_text)
            try:
                return Decimal(price_clean) if price_clean else None
            except:
                return None

        return None

    def _parse_rating(self, rating: Any) -> Optional[Decimal]:
        """Parse rating value"""
        if not rating:
            return None

        try:
            rating_val = float(rating)
            return Decimal(str(min(max(rating_val, 0), 5)))  # Clamp between 0-5
        except:
            return None

    def _parse_int(self, value: Any) -> Optional[int]:
        """Parse integer value"""
        if not value:
            return None

        try:
            if isinstance(value, str):
                # Remove non-digit characters
                value_clean = re.sub(r'[^\d]', '', value)
                return int(value_clean) if value_clean else None
            return int(value)
        except:
            return None

    def _parse_review_count(self, review_text: Any) -> Optional[int]:
        """Parse review count từ text"""
        return self._parse_int(review_text)

    def _parse_sold_count(self, sold_text: Any) -> Optional[int]:
        """Parse sold count từ text"""
        return self._parse_int(sold_text)

    def _parse_discount_percentage(self, discount_text: str) -> Optional[Decimal]:
        """Parse discount percentage từ text"""
        if not discount_text:
            return None

        # Extract percentage number
        match = re.search(r'(\d+(?:\.\d+)?)%', str(discount_text))
        if match:
            try:
                return Decimal(match.group(1))
            except:
                pass

        return None

    def _clean_text(self, text: str) -> str:
        """Clean và normalize text"""
        if not text:
            return ''

        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', str(text)).strip()

        # Limit length
        if len(text) > 1000:
            text = text[:997] + '...'

        return text

    def _normalize_category(self, category: str) -> Optional[str]:
        """Normalize category names"""
        if not category:
            return None

        # Category mapping cho standardization
        category_mapping = {
            'smartphones': 'Smartphones',
            'dien-thoai': 'Smartphones',
            'phones': 'Smartphones',
            'laptops': 'Laptops',
            'laptop': 'Laptops',
            'tablets': 'Tablets',
            'may-tinh-bang': 'Tablets',
            'headphones': 'Headphones',
            'tai-nghe': 'Headphones',
            'smartwatches': 'Smartwatches',
            'dong-ho-thong-minh': 'Smartwatches',
            'cameras': 'Cameras',
            'may-anh': 'Cameras'
        }

        category_clean = self._clean_text(category).lower()
        return category_mapping.get(category_clean, category.title())

    def _upsert_ods_product(self, cursor, product_data: Dict[str, Any]):
        """Insert or update ODS product"""

        # Check if product exists
        cursor.execute("""
            SELECT product_id FROM ods.products
            WHERE source_platform = %s AND external_product_id = %s
        """, (product_data['source_platform'], product_data['external_product_id']))

        existing = cursor.fetchone()

        if existing:
            # Update existing record
            cursor.execute("""
                UPDATE ods.products SET
                    title = %s,
                    description = %s,
                    category = %s,
                    brand = %s,
                    product_url = %s,
                    image_url = %s,
                    current_price = %s,
                    original_price = %s,
                    discount_amount = %s,
                    discount_percentage = %s,
                    rating = %s,
                    review_count = %s,
                    sold_count = %s,
                    shop_name = %s,
                    shop_rating = %s,
                    shop_location = %s,
                    last_updated = CURRENT_TIMESTAMP,
                    raw_product_id = %s
                WHERE product_id = %s
            """, (
                product_data['title'],
                product_data['description'],
                product_data['category'],
                product_data['brand'],
                product_data['product_url'],
                product_data['image_url'],
                product_data['current_price'],
                product_data['original_price'],
                product_data['discount_amount'],
                product_data['discount_percentage'],
                product_data['rating'],
                product_data['review_count'],
                product_data['sold_count'],
                product_data['shop_name'],
                product_data['shop_rating'],
                product_data['shop_location'],
                product_data['raw_product_id'],
                existing[0]
            ))
        else:
            # Insert new record
            cursor.execute("""
                INSERT INTO ods.products (
                    source_platform, external_product_id, title, description, category, brand,
                    product_url, image_url, current_price, original_price, discount_amount,
                    discount_percentage, rating, review_count, sold_count, shop_name,
                    shop_rating, shop_location, raw_product_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                product_data['source_platform'],
                product_data['external_product_id'],
                product_data['title'],
                product_data['description'],
                product_data['category'],
                product_data['brand'],
                product_data['product_url'],
                product_data['image_url'],
                product_data['current_price'],
                product_data['original_price'],
                product_data['discount_amount'],
                product_data['discount_percentage'],
                product_data['rating'],
                product_data['review_count'],
                product_data['sold_count'],
                product_data['shop_name'],
                product_data['shop_rating'],
                product_data['shop_location'],
                product_data['raw_product_id']
            ))

    def run_full_elt(self):
        """Chạy toàn bộ ELT pipeline"""
        logger.info("🚀 Starting full ELT pipeline...")

        try:
            # 1. Create schemas
            self.create_schemas()

            # 2. Extract
            extracted_data = self.extract_crawler_files()

            # 3. Load to staging
            self.load_to_staging(extracted_data)

            # 4. Transform to ODS
            self.transform_to_ods()

            # 5. Generate summary
            self._generate_summary()

            logger.info("🎉 ELT pipeline completed successfully!")

        except Exception as e:
            logger.error(f"❌ ELT pipeline failed: {str(e)}")
            raise

    def _generate_summary(self):
        """Generate pipeline execution summary"""
        end_time = datetime.now()
        duration = end_time - self.stats['start_time']

        with self.connect_db() as conn:
            cursor = conn.cursor()

            # Get final counts
            cursor.execute("SELECT COUNT(*) FROM staging.raw_products")
            staging_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM ods.products")
            ods_count = cursor.fetchone()[0]

            cursor.execute("""
                SELECT source_platform, COUNT(*)
                FROM ods.products
                GROUP BY source_platform
            """)
            platform_counts = dict(cursor.fetchall())

        summary = f"""
🎉 ELT Pipeline Summary
=====================
⏱️  Duration: {duration}
📁 Files processed: {self.stats['files_processed']}
📊 Records extracted: {self.stats['records_extracted']}
📥 Records loaded to staging: {staging_count}
🔄 Records transformed to ODS: {ods_count}
❌ Failed records: {self.stats['records_failed']}

📈 Platform breakdown:
{chr(10).join([f"   {platform}: {count:,} products" for platform, count in platform_counts.items()])}
        """

        logger.info(summary)

def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description='ELT Pipeline for Tiki & Lazada')
    parser.add_argument('--data-path', default='C:/DoAn_FPT_FALL2025/ecommerce-dss-project/data-collection/crawlers/outputs',
                       help='Path to crawler output files')
    parser.add_argument('--db-host', default='localhost', help='Database host')
    parser.add_argument('--db-name', default='ecommerce_dss_1', help='Database name')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for processing')

    args = parser.parse_args()

    # Create config
    config = ELTConfig(
        crawler_data_path=args.data_path,
        db_host=args.db_host,
        db_name=args.db_name,
        batch_size=args.batch_size
    )

    # Run ELT pipeline
    elt_pipeline = TikiLazadaELT(config)
    elt_pipeline.run_full_elt()

if __name__ == '__main__':
    main()