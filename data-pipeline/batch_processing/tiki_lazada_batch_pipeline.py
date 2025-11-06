#!/usr/bin/env python3
"""
Tiki & Lazada Batch Processing Pipeline
Optimized batch processing for only Tiki and Lazada data following architecture diagram:
1. Staging Ingestion (Raw Data)
2. ODS Standardization (Data Cleaning + Standardization + Quality & Dedup)
3. DWH Transformation (Star Schema)
4. Data Mart Aggregation (Price Optimization)
"""

import os
import json
import logging
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import uuid
import hashlib
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TikiLazadaBatchPipeline:
    """
    Batch processing pipeline specifically for Tiki and Lazada data
    Following the Data Integration Framework → Data Standardization Tool → Data Warehouse → Data Mart flow
    """

    def __init__(self, db_config: Dict[str, str]):
        self.db_config = db_config
        self.conn = None

        # Only process these platforms
        self.platforms = ['tiki', 'lazada']

        # Batch processing configurations
        self.batch_size = 1000
        self.max_retries = 3

    def connect_database(self):
        """Connect to PostgreSQL database"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.conn.autocommit = False  # Use transactions for batch processing
            logger.info("✅ Connected to PostgreSQL database")
            return True
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            return False

    def run_batch_pipeline(self, data_directories: List[str], batch_date: str = None):
        """
        Run complete batch pipeline for Tiki and Lazada

        Args:
            data_directories: List of directories containing raw JSON files
            batch_date: Date for this batch run (YYYY-MM-DD), defaults to today
        """
        if not batch_date:
            batch_date = datetime.now().strftime('%Y-%m-%d')

        logger.info(f"🚀 Starting Tiki & Lazada batch processing for {batch_date}")

        try:
            # Step 1: Staging Ingestion
            staging_stats = self._run_staging_batch(data_directories, batch_date)

            # Step 2: ODS Standardization
            ods_stats = self._run_ods_batch(batch_date)

            # Step 3: DWH Transformation
            dwh_stats = self._run_dwh_batch(batch_date)

            # Step 4: Data Mart Aggregation
            mart_stats = self._run_datamart_batch(batch_date)

            # Log final statistics
            self._log_batch_summary(staging_stats, ods_stats, dwh_stats, mart_stats, batch_date)

            logger.info(f"✅ Batch processing completed successfully for {batch_date}")

        except Exception as e:
            logger.error(f"❌ Batch processing failed: {e}")
            self.conn.rollback()
            raise

    def _run_staging_batch(self, data_directories: List[str], batch_date: str) -> Dict[str, int]:
        """
        Step 1: Staging Ingestion - Load raw JSON data into stg_raw_products and stg_raw_reviews
        """
        logger.info("📥 Step 1: Running staging batch ingestion...")

        stats = {'products': 0, 'reviews': 0, 'files': 0}
        load_id = f"{batch_date}_{uuid.uuid4().hex[:8]}"

        cursor = self.conn.cursor()

        try:
            for data_dir in data_directories:
                if not os.path.exists(data_dir):
                    continue

                for platform in self.platforms:
                    # Process product files
                    product_files = self._find_platform_files(data_dir, platform, 'product')
                    for file_path in product_files:
                        count = self._ingest_product_file(cursor, file_path, platform, load_id)
                        stats['products'] += count
                        stats['files'] += 1

                    # Process review files (if any)
                    review_files = self._find_platform_files(data_dir, platform, 'review')
                    for file_path in review_files:
                        count = self._ingest_review_file(cursor, file_path, platform, load_id)
                        stats['reviews'] += count
                        stats['files'] += 1

            self.conn.commit()
            logger.info(f"✅ Staging completed: {stats['products']} products, {stats['reviews']} reviews")

        except Exception as e:
            self.conn.rollback()
            logger.error(f"❌ Staging batch failed: {e}")
            raise

        finally:
            cursor.close()

        return stats

    def _run_ods_batch(self, batch_date: str) -> Dict[str, int]:
        """
        Step 2: ODS Standardization - Data cleaning, standardization, quality check, deduplication
        """
        logger.info("🔧 Step 2: Running ODS standardization batch...")

        stats = {'platforms': 0, 'categories': 0, 'products': 0, 'prices': 0, 'ratings': 0}
        cursor = self.conn.cursor()

        try:
            # Initialize platform reference data
            self._initialize_platforms(cursor)
            stats['platforms'] = len(self.platforms)

            # Process and clean product data
            stats['products'] = self._standardize_products(cursor, batch_date)

            # Process price data
            stats['prices'] = self._standardize_prices(cursor, batch_date)

            # Process rating data
            stats['ratings'] = self._standardize_ratings(cursor, batch_date)

            # Category mapping
            stats['categories'] = self._map_categories(cursor)

            self.conn.commit()
            logger.info(f"✅ ODS standardization completed: {stats}")

        except Exception as e:
            self.conn.rollback()
            logger.error(f"❌ ODS batch failed: {e}")
            raise

        finally:
            cursor.close()

        return stats

    def _run_dwh_batch(self, batch_date: str) -> Dict[str, int]:
        """
        Step 3: DWH Transformation - Transform data into star schema (dimensions + facts)
        """
        logger.info("⭐ Step 3: Running DWH transformation batch...")

        stats = {'date_dim': 0, 'brand_dim': 0, 'category_dim': 0, 'product_dim': 0, 'fact_daily': 0, 'fact_reviews': 0}
        cursor = self.conn.cursor()

        try:
            # Build dimension tables
            stats['date_dim'] = self._build_date_dimension(cursor, batch_date)
            stats['brand_dim'] = self._build_brand_dimension(cursor)
            stats['category_dim'] = self._build_category_dimension(cursor)
            stats['product_dim'] = self._build_product_dimension(cursor, batch_date)

            # Build fact tables
            stats['fact_daily'] = self._build_daily_fact(cursor, batch_date)
            stats['fact_reviews'] = self._build_review_fact(cursor, batch_date)

            self.conn.commit()
            logger.info(f"✅ DWH transformation completed: {stats}")

        except Exception as e:
            self.conn.rollback()
            logger.error(f"❌ DWH batch failed: {e}")
            raise

        finally:
            cursor.close()

        return stats

    def _run_datamart_batch(self, batch_date: str) -> Dict[str, int]:
        """
        Step 4: Data Mart Aggregation - Create price optimization analytics
        """
        logger.info("📊 Step 4: Running Data Mart aggregation batch...")

        stats = {'price_analytics': 0}
        cursor = self.conn.cursor()

        try:
            # Build price optimization data mart
            stats['price_analytics'] = self._build_price_analytics(cursor, batch_date)

            self.conn.commit()
            logger.info(f"✅ Data Mart completed: {stats}")

        except Exception as e:
            self.conn.rollback()
            logger.error(f"❌ Data Mart batch failed: {e}")
            raise

        finally:
            cursor.close()

        return stats

    def _find_platform_files(self, data_dir: str, platform: str, data_type: str) -> List[str]:
        """Find JSON files for specific platform and data type"""
        files = []
        data_path = Path(data_dir)

        # Search patterns for each platform
        patterns = {
            'tiki': ['tiki', 'working_tiki'],
            'lazada': ['lazada', 'enhanced_lazada', 'working_lazada']
        }

        if platform not in patterns:
            return files

        for pattern in patterns[platform]:
            # Find files containing the pattern
            for json_file in data_path.glob(f'**/*{pattern}*.json'):
                if data_type == 'review' and 'review' in str(json_file).lower():
                    files.append(str(json_file))
                elif data_type == 'product' and 'review' not in str(json_file).lower():
                    files.append(str(json_file))

        return files

    def _ingest_product_file(self, cursor, file_path: str, platform: str, load_id: str) -> int:
        """Ingest products from a single JSON file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            if not isinstance(data, list):
                data = [data]

            count = 0
            for item in data:
                try:
                    # Extract key fields
                    url = item.get('url', '')
                    product_id = item.get('product_id') or item.get('id') or ''
                    crawled_at = self._parse_datetime(item.get('crawl_date') or item.get('crawled_at'))
                    checksum = hashlib.md5(json.dumps(item, sort_keys=True).encode()).hexdigest()

                    # Insert into staging
                    cursor.execute("""
                        INSERT INTO stg_raw_products (
                            source_platform, url, platform_product_id,
                            crawled_at, raw_data, checksum, load_id
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (source_platform, platform_product_id, checksum) DO NOTHING
                    """, (platform, url, product_id, crawled_at, json.dumps(item), checksum, load_id))

                    count += 1

                except Exception as e:
                    logger.warning(f"⚠️ Failed to process item in {file_path}: {e}")
                    continue

            return count

        except Exception as e:
            logger.error(f"❌ Failed to process file {file_path}: {e}")
            return 0

    def _ingest_review_file(self, cursor, file_path: str, platform: str, load_id: str) -> int:
        """Ingest reviews from a single JSON file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            count = 0
            # Handle different review data structures
            if isinstance(data, list):
                for product in data:
                    reviews = product.get('reviews', [])
                    product_id = product.get('product_id') or product.get('id', '')

                    for review in reviews:
                        try:
                            review['product_id'] = product_id
                            crawled_at = self._parse_datetime(review.get('review_time'))

                            cursor.execute("""
                                INSERT INTO stg_raw_reviews (
                                    source_platform, platform_product_id,
                                    crawled_at, raw_data, load_id
                                ) VALUES (%s, %s, %s, %s, %s)
                            """, (platform, product_id, crawled_at, json.dumps(review), load_id))

                            count += 1

                        except Exception as e:
                            logger.warning(f"⚠️ Failed to process review: {e}")
                            continue

            return count

        except Exception as e:
            logger.error(f"❌ Failed to process review file {file_path}: {e}")
            return 0

    def _parse_datetime(self, dt_str: Any) -> datetime:
        """Parse datetime from various string formats"""
        if isinstance(dt_str, datetime):
            return dt_str

        if not dt_str:
            return datetime.now()

        try:
            # Common formats
            formats = [
                '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d',
                '%d/%m/%Y'
            ]

            for fmt in formats:
                try:
                    return datetime.strptime(str(dt_str), fmt)
                except ValueError:
                    continue

        except Exception:
            pass

        return datetime.now()

    def _initialize_platforms(self, cursor):
        """Initialize platform reference data"""
        platforms_data = {
            'tiki': {'name': 'Tiki.vn', 'url': 'https://tiki.vn'},
            'lazada': {'name': 'Lazada Vietnam', 'url': 'https://lazada.vn'}
        }

        for code, info in platforms_data.items():
            cursor.execute("""
                INSERT INTO ods_platform_ref (platform_code, platform_name, website_url)
                VALUES (%s, %s, %s)
                ON CONFLICT (platform_code) DO UPDATE SET
                    platform_name = EXCLUDED.platform_name,
                    website_url = EXCLUDED.website_url
            """, (code, info['name'], info['url']))

    def _standardize_products(self, cursor, batch_date: str) -> int:
        """Standardize and clean product data"""
        cursor.execute("""
            WITH cleaned_products AS (
                SELECT DISTINCT
                    source_platform,
                    platform_product_id,
                    (raw_data->>'title')::text as product_name,
                    (raw_data->>'product_name')::text as alt_product_name,
                    COALESCE(
                        (raw_data->>'brand')::text,
                        (raw_data->>'brand_name')::text
                    ) as brand_name,
                    (raw_data->>'category')::text as category,
                    (raw_data->>'description')::text as description,
                    (raw_data->>'seller_name')::text as seller_name,
                    (raw_data->>'seller_type')::text as seller_type,
                    (raw_data->>'image_urls')::text[] as image_urls,
                    (raw_data->>'image_url')::text as single_image_url,
                    created_at
                FROM stg_raw_products
                WHERE source_platform IN ('tiki', 'lazada')
                    AND DATE(created_at) = %s
            ),
            platform_mapping AS (
                SELECT platform_sk, platform_code
                FROM ods_platform_ref
                WHERE platform_code IN ('tiki', 'lazada')
            )
            INSERT INTO ods_product_clean (
                global_product_id, product_name, brand_name,
                description, image_urls, seller_name, seller_type,
                first_seen, last_seen, is_active, data_quality_score
            )
            SELECT DISTINCT
                gen_random_uuid()::varchar(36),
                COALESCE(TRIM(cp.product_name), TRIM(cp.alt_product_name), 'Unknown Product'),
                TRIM(cp.brand_name),
                TRIM(cp.description),
                CASE
                    WHEN cp.image_urls IS NOT NULL THEN cp.image_urls
                    WHEN cp.single_image_url IS NOT NULL THEN ARRAY[cp.single_image_url]
                    ELSE ARRAY[]::text[]
                END,
                TRIM(cp.seller_name),
                TRIM(cp.seller_type),
                cp.created_at,
                cp.created_at,
                true,
                CASE
                    WHEN cp.product_name IS NOT NULL AND LENGTH(TRIM(cp.product_name)) > 10 THEN 1.0
                    ELSE 0.7
                END
            FROM cleaned_products cp
            WHERE cp.product_name IS NOT NULL
               OR cp.alt_product_name IS NOT NULL
            ON CONFLICT DO NOTHING;
        """, (batch_date,))

        return cursor.rowcount

    def _standardize_prices(self, cursor, batch_date: str) -> int:
        """Standardize price data"""
        cursor.execute("""
            WITH price_data AS (
                SELECT
                    p.platform_product_id,
                    pr.platform_sk,
                    pc.global_product_id,
                    CASE
                        WHEN (p.raw_data->>'price_current')::text ~ '^[0-9.]+$'
                        THEN (p.raw_data->>'price_current')::decimal
                        WHEN (p.raw_data->>'price')::text ~ '^[0-9.]+$'
                        THEN (p.raw_data->>'price')::decimal
                        ELSE NULL
                    END as price_current,
                    CASE
                        WHEN (p.raw_data->>'price_original')::text ~ '^[0-9.]+$'
                        THEN (p.raw_data->>'price_original')::decimal
                        ELSE NULL
                    END as price_original,
                    CASE
                        WHEN (p.raw_data->>'discount_percent')::text ~ '^[0-9.]+$'
                        THEN (p.raw_data->>'discount_percent')::decimal
                        ELSE NULL
                    END as discount_percent,
                    p.crawled_at
                FROM stg_raw_products p
                JOIN ods_platform_ref pr ON p.source_platform = pr.platform_code
                JOIN ods_product_clean pc ON pc.global_product_id IS NOT NULL
                WHERE p.source_platform IN ('tiki', 'lazada')
                    AND DATE(p.crawled_at) = %s
                    AND (
                        (p.raw_data->>'price_current') IS NOT NULL OR
                        (p.raw_data->>'price') IS NOT NULL
                    )
            )
            INSERT INTO ods_price_point (
                global_product_id, platform_sk, captured_at,
                price_current, price_original, discount_percent, is_available
            )
            SELECT DISTINCT
                pd.global_product_id,
                pd.platform_sk,
                pd.crawled_at,
                pd.price_current,
                pd.price_original,
                pd.discount_percent,
                true
            FROM price_data pd
            WHERE pd.price_current > 0
            ON CONFLICT DO NOTHING;
        """, (batch_date,))

        return cursor.rowcount

    def _standardize_ratings(self, cursor, batch_date: str) -> int:
        """Standardize rating and review data"""
        cursor.execute("""
            WITH rating_data AS (
                SELECT
                    p.platform_product_id,
                    pr.platform_sk,
                    pc.global_product_id,
                    CASE
                        WHEN (p.raw_data->>'rating')::text ~ '^[0-9.]+$'
                        THEN (p.raw_data->>'rating')::decimal(3,2)
                        WHEN (p.raw_data->>'rating_avg')::text ~ '^[0-9.]+$'
                        THEN (p.raw_data->>'rating_avg')::decimal(3,2)
                        ELSE NULL
                    END as rating_avg,
                    CASE
                        WHEN (p.raw_data->>'rating_count')::text ~ '^[0-9]+$'
                        THEN (p.raw_data->>'rating_count')::int
                        ELSE NULL
                    END as rating_count,
                    CASE
                        WHEN (p.raw_data->>'review_count')::text ~ '^[0-9]+$'
                        THEN (p.raw_data->>'review_count')::int
                        ELSE NULL
                    END as review_count,
                    CASE
                        WHEN (p.raw_data->>'sold_count')::text ~ '^[0-9]+$'
                        THEN (p.raw_data->>'sold_count')::int
                        ELSE NULL
                    END as sold_count,
                    p.crawled_at
                FROM stg_raw_products p
                JOIN ods_platform_ref pr ON p.source_platform = pr.platform_code
                JOIN ods_product_clean pc ON pc.global_product_id IS NOT NULL
                WHERE p.source_platform IN ('tiki', 'lazada')
                    AND DATE(p.crawled_at) = %s
            )
            INSERT INTO ods_rating_snapshot (
                global_product_id, platform_sk, captured_at,
                rating_avg, rating_count, review_count, sold_count
            )
            SELECT DISTINCT
                rd.global_product_id,
                rd.platform_sk,
                rd.crawled_at,
                rd.rating_avg,
                rd.rating_count,
                rd.review_count,
                rd.sold_count
            FROM rating_data rd
            ON CONFLICT DO NOTHING;
        """, (batch_date,))

        return cursor.rowcount

    def _map_categories(self, cursor) -> int:
        """Map and standardize categories"""
        # Basic category mapping for Tiki and Lazada
        categories = [
            ('smartphones', 'Smartphones', None, 1),
            ('electronics', 'Electronics', None, 1),
            ('laptops', 'Laptops', 'electronics', 2),
            ('tablets', 'Tablets', 'electronics', 2),
            ('headphones', 'Headphones', 'electronics', 2)
        ]

        count = 0
        for code, name, parent, level in categories:
            cursor.execute("""
                INSERT INTO ods_category_taxonomy (category_code, category_name, level)
                VALUES (%s, %s, %s)
                ON CONFLICT (category_code) DO NOTHING
            """, (code, name, level))
            count += cursor.rowcount

        return count

    def _build_date_dimension(self, cursor, batch_date: str) -> int:
        """Build date dimension for the batch date"""
        dt = datetime.strptime(batch_date, '%Y-%m-%d')
        date_sk = int(dt.strftime('%Y%m%d'))

        cursor.execute("""
            INSERT INTO dwh_dim_date (
                date_sk, date_value, day, month, quarter, year,
                week_of_year, is_weekend, day_name, month_name
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date_sk) DO NOTHING
        """, (
            date_sk, dt.date(), dt.day, dt.month,
            (dt.month - 1) // 3 + 1, dt.year, dt.isocalendar()[1],
            dt.weekday() >= 5, dt.strftime('%A'), dt.strftime('%B')
        ))

        return cursor.rowcount

    def _build_brand_dimension(self, cursor) -> int:
        """Build brand dimension from ODS data"""
        cursor.execute("""
            INSERT INTO dwh_dim_brand (brand_code, brand_name)
            SELECT DISTINCT
                LOWER(REPLACE(brand_name, ' ', '_')),
                brand_name
            FROM ods_product_clean
            WHERE brand_name IS NOT NULL AND TRIM(brand_name) != ''
            ON CONFLICT (brand_code) DO NOTHING
        """)

        return cursor.rowcount

    def _build_category_dimension(self, cursor) -> int:
        """Build category dimension"""
        cursor.execute("""
            INSERT INTO dwh_dim_category (
                category_code, category_name, category_level, category_path
            )
            SELECT
                category_code,
                category_name,
                level,
                category_code
            FROM ods_category_taxonomy
            ON CONFLICT (category_code) DO NOTHING
        """)

        return cursor.rowcount

    def _build_product_dimension(self, cursor, batch_date: str) -> int:
        """Build product dimension with SCD Type 2"""
        dt = datetime.strptime(batch_date, '%Y-%m-%d')

        cursor.execute("""
            INSERT INTO dwh_dim_product (
                global_product_id, product_name, brand_sk, category_sk,
                seller_name, seller_type, effective_from, effective_to, is_current
            )
            SELECT
                pc.global_product_id,
                pc.product_name,
                db.brand_sk,
                dc.category_sk,
                pc.seller_name,
                pc.seller_type,
                %s,
                '9999-12-31'::date,
                true
            FROM ods_product_clean pc
            LEFT JOIN dwh_dim_brand db ON LOWER(REPLACE(pc.brand_name, ' ', '_')) = db.brand_code
            LEFT JOIN dwh_dim_category dc ON dc.category_code = 'electronics'  -- Default category
            WHERE DATE(pc.first_seen) = %s
            ON CONFLICT (global_product_id, effective_from) DO NOTHING
        """, (dt.date(), batch_date))

        return cursor.rowcount

    def _build_daily_fact(self, cursor, batch_date: str) -> int:
        """Build daily product fact table"""
        dt = datetime.strptime(batch_date, '%Y-%m-%d')
        date_sk = int(dt.strftime('%Y%m%d'))

        cursor.execute("""
            INSERT INTO dwh_fact_product_daily (
                date_sk, product_sk, platform_sk, price_current, price_original,
                discount_pct, rating_avg, rating_count, review_count, sold_count, is_available
            )
            SELECT
                %s,
                dp.product_sk,
                dpl.platform_sk,
                pp.price_current,
                pp.price_original,
                pp.discount_percent,
                rs.rating_avg,
                rs.rating_count,
                rs.review_count,
                rs.sold_count,
                pp.is_available
            FROM ods_price_point pp
            JOIN dwh_dim_product dp ON pp.global_product_id = dp.global_product_id
            JOIN dwh_dim_platform dpl ON pp.platform_sk = dpl.platform_sk
            LEFT JOIN ods_rating_snapshot rs ON pp.global_product_id = rs.global_product_id
                AND pp.platform_sk = rs.platform_sk
                AND DATE(pp.captured_at) = DATE(rs.captured_at)
            WHERE DATE(pp.captured_at) = %s
            ON CONFLICT (date_sk, product_sk, platform_sk)
            DO UPDATE SET
                price_current = EXCLUDED.price_current,
                price_original = EXCLUDED.price_original,
                discount_pct = EXCLUDED.discount_pct,
                rating_avg = EXCLUDED.rating_avg,
                rating_count = EXCLUDED.rating_count,
                review_count = EXCLUDED.review_count,
                sold_count = EXCLUDED.sold_count,
                is_available = EXCLUDED.is_available
        """, (date_sk, batch_date))

        return cursor.rowcount

    def _build_review_fact(self, cursor, batch_date: str) -> int:
        """Build review summary fact table"""
        dt = datetime.strptime(batch_date, '%Y-%m-%d')
        date_sk = int(dt.strftime('%Y%m%d'))

        cursor.execute("""
            INSERT INTO dwh_fact_review_summary (
                date_sk, product_sk, platform_sk, total_reviews, avg_rating,
                positive_reviews, negative_reviews, neutral_reviews, sentiment_score
            )
            SELECT
                %s,
                dp.product_sk,
                dpl.platform_sk,
                rs.review_count,
                rs.rating_avg,
                CASE WHEN rs.rating_avg >= 4 THEN rs.review_count ELSE 0 END,
                CASE WHEN rs.rating_avg <= 2 THEN rs.review_count ELSE 0 END,
                CASE WHEN rs.rating_avg > 2 AND rs.rating_avg < 4 THEN rs.review_count ELSE 0 END,
                rs.rating_avg
            FROM ods_rating_snapshot rs
            JOIN dwh_dim_product dp ON rs.global_product_id = dp.global_product_id
            JOIN dwh_dim_platform dpl ON rs.platform_sk = dpl.platform_sk
            WHERE DATE(rs.captured_at) = %s AND rs.review_count > 0
            ON CONFLICT (date_sk, product_sk, platform_sk)
            DO UPDATE SET
                total_reviews = EXCLUDED.total_reviews,
                avg_rating = EXCLUDED.avg_rating,
                positive_reviews = EXCLUDED.positive_reviews,
                negative_reviews = EXCLUDED.negative_reviews,
                neutral_reviews = EXCLUDED.neutral_reviews,
                sentiment_score = EXCLUDED.sentiment_score
        """, (date_sk, batch_date))

        return cursor.rowcount

    def _build_price_analytics(self, cursor, batch_date: str) -> int:
        """Build price optimization data mart"""
        dt = datetime.strptime(batch_date, '%Y-%m-%d')
        date_sk = int(dt.strftime('%Y%m%d'))

        cursor.execute("""
            WITH price_comparison AS (
                SELECT
                    product_sk,
                    platform_sk,
                    price_current,
                    price_original,
                    discount_pct,
                    MIN(price_current) OVER (PARTITION BY product_sk) as min_price,
                    MAX(price_current) OVER (PARTITION BY product_sk) as max_price,
                    ROW_NUMBER() OVER (PARTITION BY product_sk ORDER BY price_current) as price_rank
                FROM dwh_fact_product_daily
                WHERE date_sk = %s AND price_current > 0
            )
            INSERT INTO dm_price_analytics (
                product_sk, platform_sk, date_sk, price_current, price_original,
                discount_pct, competitor_min_price, competitor_max_price, price_rank, price_trend
            )
            SELECT
                product_sk,
                platform_sk,
                %s,
                price_current,
                price_original,
                discount_pct,
                min_price,
                max_price,
                price_rank,
                CASE
                    WHEN price_current = min_price THEN 'lowest'
                    WHEN price_current = max_price THEN 'highest'
                    ELSE 'competitive'
                END
            FROM price_comparison
            ON CONFLICT (product_sk, platform_sk, date_sk)
            DO UPDATE SET
                price_current = EXCLUDED.price_current,
                price_original = EXCLUDED.price_original,
                discount_pct = EXCLUDED.discount_pct,
                competitor_min_price = EXCLUDED.competitor_min_price,
                competitor_max_price = EXCLUDED.competitor_max_price,
                price_rank = EXCLUDED.price_rank,
                price_trend = EXCLUDED.price_trend
        """, (date_sk, date_sk))

        return cursor.rowcount

    def _log_batch_summary(self, staging_stats: Dict, ods_stats: Dict,
                          dwh_stats: Dict, mart_stats: Dict, batch_date: str):
        """Log comprehensive batch processing summary"""
        logger.info("="*60)
        logger.info(f"🎯 BATCH PROCESSING SUMMARY - {batch_date}")
        logger.info("="*60)
        logger.info(f"📥 STAGING: {staging_stats['products']} products, {staging_stats['reviews']} reviews")
        logger.info(f"🔧 ODS: {ods_stats['products']} products, {ods_stats['prices']} prices, {ods_stats['ratings']} ratings")
        logger.info(f"⭐ DWH: {dwh_stats['product_dim']} product dims, {dwh_stats['fact_daily']} daily facts")
        logger.info(f"📊 MART: {mart_stats['price_analytics']} price analytics records")
        logger.info("="*60)

    def close_connection(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("🔒 Database connection closed")


def main():
    """Main execution function for batch processing"""
    # Database configuration
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'database': 'ecommerce_dss',
        'user': 'dss_user',
        'password': 'dss_password_123'
    }

    # Data directories (only Tiki and Lazada)
    data_directories = [
        'C:/DoAn_FPT_FALL2025/ecommerce-dss-project/data-collection/data',
        'C:/DoAn_FPT_FALL2025/ecommerce-dss-project/data-collection/crawlers/outputs'
    ]

    # Initialize pipeline
    pipeline = TikiLazadaBatchPipeline(db_config)

    if not pipeline.connect_database():
        logger.error("❌ Cannot proceed without database connection")
        return

    try:
        # Run batch processing for today's date
        batch_date = datetime.now().strftime('%Y-%m-%d')
        pipeline.run_batch_pipeline(data_directories, batch_date)

    except Exception as e:
        logger.error(f"❌ Batch processing failed: {e}")

    finally:
        pipeline.close_connection()


if __name__ == "__main__":
    main()