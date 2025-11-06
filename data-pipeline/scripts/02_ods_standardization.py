#!/usr/bin/env python3
"""
ODS Data Standardization Script for Tiki & Lazada
Transforms staging data into clean, standardized ODS tables
Can be called by Airflow or run standalone
"""

import os
import sys
import logging
import psycopg2
import uuid
from datetime import datetime
from typing import Dict, List, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ODSStandardization:
    """ODS data standardization for Tiki and Lazada"""

    def __init__(self, db_config: Dict[str, str]):
        self.db_config = db_config
        self.conn = None

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

    def standardize_data(self, process_date: str = None) -> Dict[str, int]:
        """
        Run ODS standardization process

        Args:
            process_date: Date to process (YYYY-MM-DD), defaults to today

        Returns:
            Statistics dictionary
        """
        if not process_date:
            process_date = datetime.now().strftime('%Y-%m-%d')

        logger.info(f"🔧 Starting ODS standardization for {process_date}")

        stats = {
            'platforms_initialized': 0,
            'products_standardized': 0,
            'prices_processed': 0,
            'ratings_processed': 0,
            'categories_mapped': 0
        }

        cursor = self.conn.cursor()

        try:
            # Step 1: Initialize platform data
            stats['platforms_initialized'] = self._initialize_platforms(cursor)

            # Step 2: Standardize products
            stats['products_standardized'] = self._standardize_products(cursor, process_date)

            # Step 3: Process prices
            stats['prices_processed'] = self._process_prices(cursor, process_date)

            # Step 4: Process ratings
            stats['ratings_processed'] = self._process_ratings(cursor, process_date)

            # Step 5: Map categories
            stats['categories_mapped'] = self._map_categories(cursor)

            logger.info(f"✅ ODS standardization completed: {stats}")
            return stats

        except Exception as e:
            logger.error(f"❌ ODS standardization failed: {e}")
            raise
        finally:
            cursor.close()

    def _initialize_platforms(self, cursor) -> int:
        """Initialize platform reference data"""
        platforms_data = [
            ('tiki', 'Tiki.vn', 'https://tiki.vn'),
            ('lazada', 'Lazada Vietnam', 'https://lazada.vn')
        ]

        count = 0
        for code, name, url in platforms_data:
            cursor.execute("""
                INSERT INTO ods_platform_ref (platform_code, platform_name, website_url)
                VALUES (%s, %s, %s)
                ON CONFLICT (platform_code) DO UPDATE SET
                    platform_name = EXCLUDED.platform_name,
                    website_url = EXCLUDED.website_url
            """, (code, name, url))
            count += 1

        logger.info(f"📊 Initialized {count} platforms")
        return count

    def _standardize_products(self, cursor, process_date: str) -> int:
        """Standardize and clean product data"""
        cursor.execute("""
            WITH new_products AS (
                SELECT DISTINCT
                    gen_random_uuid()::varchar(36) as global_product_id,
                    p.source_platform,
                    p.platform_product_id,
                    COALESCE(
                        TRIM((p.raw_data->>'product_name')::text),
                        TRIM((p.raw_data->>'title')::text),
                        'Unknown Product'
                    ) as product_name,
                    COALESCE(
                        TRIM((p.raw_data->>'brand')::text),
                        TRIM((p.raw_data->>'brand_name')::text)
                    ) as brand_name,
                    COALESCE(
                        TRIM((p.raw_data->>'category')::text),
                        'electronics'
                    ) as category,
                    TRIM((p.raw_data->>'description')::text) as description,
                    CASE
                        WHEN (p.raw_data->>'image_urls')::text IS NOT NULL
                        THEN string_to_array((p.raw_data->>'image_urls')::text, ',')
                        WHEN (p.raw_data->>'image_url')::text IS NOT NULL
                        THEN ARRAY[(p.raw_data->>'image_url')::text]
                        ELSE ARRAY[]::text[]
                    END as image_urls,
                    TRIM((p.raw_data->>'seller_name')::text) as seller_name,
                    TRIM((p.raw_data->>'seller_type')::text) as seller_type,
                    p.crawled_at as first_seen
                FROM stg_raw_products p
                WHERE p.source_platform IN ('tiki', 'lazada')
                    AND DATE(p.crawled_at) = %s
                    AND NOT EXISTS (
                        SELECT 1 FROM ods_product_clean opc
                        WHERE opc.product_name = COALESCE(
                            TRIM((p.raw_data->>'product_name')::text),
                            TRIM((p.raw_data->>'title')::text)
                        )
                        AND opc.seller_name = TRIM((p.raw_data->>'seller_name')::text)
                    )
            ),
            inserted_products AS (
                INSERT INTO ods_product_clean (
                    global_product_id, product_name, brand_name,
                    description, image_urls, seller_name, seller_type,
                    first_seen, last_seen, is_active, data_quality_score
                )
                SELECT
                    global_product_id,
                    product_name,
                    brand_name,
                    description,
                    image_urls,
                    seller_name,
                    seller_type,
                    first_seen,
                    first_seen,
                    true,
                    CASE
                        WHEN LENGTH(product_name) > 10 AND brand_name IS NOT NULL THEN 1.0
                        WHEN LENGTH(product_name) > 10 THEN 0.8
                        ELSE 0.6
                    END
                FROM new_products
                RETURNING global_product_id
            ),
            product_id_mapping AS (
                INSERT INTO ods_product_id_map (
                    platform_sk, platform_product_id, global_product_id
                )
                SELECT
                    pr.platform_sk,
                    sp.platform_product_id,
                    np.global_product_id
                FROM new_products np
                JOIN stg_raw_products sp ON sp.source_platform = np.source_platform
                    AND sp.platform_product_id = np.platform_product_id
                JOIN ods_platform_ref pr ON sp.source_platform = pr.platform_code
                WHERE DATE(sp.crawled_at) = %s
                ON CONFLICT (platform_sk, platform_product_id) DO NOTHING
                RETURNING global_product_id
            )
            SELECT COUNT(*) FROM inserted_products
        """, (process_date, process_date))

        count = cursor.fetchone()[0]
        logger.info(f"📦 Standardized {count} products")
        return count

    def _process_prices(self, cursor, process_date: str) -> int:
        """Process and standardize price data"""
        cursor.execute("""
            WITH price_data AS (
                SELECT DISTINCT
                    pim.global_product_id,
                    pr.platform_sk,
                    p.crawled_at as captured_at,
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
                    END as discount_percent
                FROM stg_raw_products p
                JOIN ods_platform_ref pr ON p.source_platform = pr.platform_code
                JOIN ods_product_id_map pim ON pr.platform_sk = pim.platform_sk
                    AND p.platform_product_id = pim.platform_product_id
                WHERE p.source_platform IN ('tiki', 'lazada')
                    AND DATE(p.crawled_at) = %s
                    AND (
                        (p.raw_data->>'price_current')::text ~ '^[0-9.]+$' OR
                        (p.raw_data->>'price')::text ~ '^[0-9.]+$'
                    )
            )
            INSERT INTO ods_price_point (
                global_product_id, platform_sk, captured_at,
                price_current, price_original, discount_percent, is_available
            )
            SELECT
                global_product_id,
                platform_sk,
                captured_at,
                price_current,
                price_original,
                discount_percent,
                CASE WHEN price_current > 0 THEN true ELSE false END
            FROM price_data
            WHERE price_current IS NOT NULL AND price_current > 0
            ON CONFLICT (global_product_id, platform_sk, captured_at)
            DO UPDATE SET
                price_current = EXCLUDED.price_current,
                price_original = EXCLUDED.price_original,
                discount_percent = EXCLUDED.discount_percent,
                is_available = EXCLUDED.is_available
        """, (process_date,))

        count = cursor.rowcount
        logger.info(f"💰 Processed {count} price records")
        return count

    def _process_ratings(self, cursor, process_date: str) -> int:
        """Process rating and review data"""
        cursor.execute("""
            WITH rating_data AS (
                SELECT DISTINCT
                    pim.global_product_id,
                    pr.platform_sk,
                    p.crawled_at as captured_at,
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
                        ELSE 0
                    END as rating_count,
                    CASE
                        WHEN (p.raw_data->>'review_count')::text ~ '^[0-9]+$'
                        THEN (p.raw_data->>'review_count')::int
                        ELSE 0
                    END as review_count,
                    CASE
                        WHEN (p.raw_data->>'sold_count')::text ~ '^[0-9]+$'
                        THEN (p.raw_data->>'sold_count')::int
                        ELSE 0
                    END as sold_count
                FROM stg_raw_products p
                JOIN ods_platform_ref pr ON p.source_platform = pr.platform_code
                JOIN ods_product_id_map pim ON pr.platform_sk = pim.platform_sk
                    AND p.platform_product_id = pim.platform_product_id
                WHERE p.source_platform IN ('tiki', 'lazada')
                    AND DATE(p.crawled_at) = %s
            )
            INSERT INTO ods_rating_snapshot (
                global_product_id, platform_sk, captured_at,
                rating_avg, rating_count, review_count, sold_count
            )
            SELECT
                global_product_id,
                platform_sk,
                captured_at,
                rating_avg,
                rating_count,
                review_count,
                sold_count
            FROM rating_data
            WHERE rating_avg IS NOT NULL OR rating_count > 0 OR review_count > 0
            ON CONFLICT (global_product_id, platform_sk, captured_at)
            DO UPDATE SET
                rating_avg = EXCLUDED.rating_avg,
                rating_count = EXCLUDED.rating_count,
                review_count = EXCLUDED.review_count,
                sold_count = EXCLUDED.sold_count
        """, (process_date,))

        count = cursor.rowcount
        logger.info(f"⭐ Processed {count} rating records")
        return count

    def _map_categories(self, cursor) -> int:
        """Initialize and map categories"""
        # Basic category taxonomy
        categories = [
            ('electronics', 'Electronics', None, 1),
            ('smartphones', 'Smartphones', 'electronics', 2),
            ('laptops', 'Laptops', 'electronics', 2),
            ('tablets', 'Tablets', 'electronics', 2),
            ('headphones', 'Headphones', 'electronics', 2),
            ('cameras', 'Cameras', 'electronics', 2)
        ]

        count = 0
        for code, name, parent, level in categories:
            cursor.execute("""
                INSERT INTO ods_category_taxonomy (category_code, category_name, level)
                VALUES (%s, %s, %s)
                ON CONFLICT (category_code) DO UPDATE SET
                    category_name = EXCLUDED.category_name,
                    level = EXCLUDED.level
            """, (code, name, level))
            count += 1

        # Map platform categories (basic mapping)
        cursor.execute("""
            INSERT INTO ods_platform_category_map (
                platform_sk, platform_category_id, category_sk, confidence_score
            )
            SELECT DISTINCT
                pr.platform_sk,
                'electronics',
                ct.category_sk,
                1.0
            FROM ods_platform_ref pr
            CROSS JOIN ods_category_taxonomy ct
            WHERE pr.platform_code IN ('tiki', 'lazada')
                AND ct.category_code = 'electronics'
            ON CONFLICT (platform_sk, platform_category_id) DO NOTHING
        """)

        logger.info(f"🗂️ Mapped {count} categories")
        return count

    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("🔒 Database connection closed")


def main():
    """Main function for standalone execution"""
    import argparse

    parser = argparse.ArgumentParser(description='ODS Data Standardization for Tiki & Lazada')
    parser.add_argument('--date', help='Process data for specific date (YYYY-MM-DD)')

    args = parser.parse_args()

    # Database configuration
    db_config = {
        'host': 'localhost',
        'port': 5433,
        'database': 'ecommerce_dss',
        'user': 'dss_user',
        'password': 'dss_password_123'
    }

    # Run standardization
    ods = ODSStandardization(db_config)

    if not ods.connect():
        logger.error("❌ Cannot connect to database")
        return False

    try:
        stats = ods.standardize_data(args.date)
        logger.info(f"✅ ODS standardization completed successfully")
        return True

    except Exception as e:
        logger.error(f"❌ ODS standardization failed: {e}")
        return False

    finally:
        ods.close()


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)