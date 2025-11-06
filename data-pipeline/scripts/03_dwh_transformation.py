#!/usr/bin/env python3
"""
DWH Data Transformation Script for Tiki & Lazada
Transforms ODS data into star schema (dimensions + facts)
Can be called by Airflow or run standalone
"""

import os
import sys
import logging
import psycopg2
from datetime import datetime
from typing import Dict, List, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DWHTransformation:
    """DWH transformation for star schema"""

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

    def transform_to_dwh(self, process_date: str = None) -> Dict[str, int]:
        """
        Transform ODS data to DWH star schema

        Args:
            process_date: Date to process (YYYY-MM-DD), defaults to today

        Returns:
            Statistics dictionary
        """
        if not process_date:
            process_date = datetime.now().strftime('%Y-%m-%d')

        logger.info(f"⭐ Starting DWH transformation for {process_date}")

        stats = {
            'date_dim': 0,
            'platform_dim': 0,
            'brand_dim': 0,
            'category_dim': 0,
            'product_dim': 0,
            'fact_product_daily': 0,
            'fact_review_summary': 0,
            'price_analytics': 0
        }

        cursor = self.conn.cursor()

        try:
            # Build dimensions
            stats['date_dim'] = self._build_date_dimension(cursor, process_date)
            stats['platform_dim'] = self._build_platform_dimension(cursor)
            stats['brand_dim'] = self._build_brand_dimension(cursor)
            stats['category_dim'] = self._build_category_dimension(cursor)
            stats['product_dim'] = self._build_product_dimension(cursor, process_date)

            # Build fact tables
            stats['fact_product_daily'] = self._build_product_fact(cursor, process_date)
            stats['fact_review_summary'] = self._build_review_fact(cursor, process_date)

            # Build data mart
            stats['price_analytics'] = self._build_price_analytics(cursor, process_date)

            logger.info(f"✅ DWH transformation completed: {stats}")
            return stats

        except Exception as e:
            logger.error(f"❌ DWH transformation failed: {e}")
            raise
        finally:
            cursor.close()

    def _build_date_dimension(self, cursor, process_date: str) -> int:
        """Build date dimension for processing date"""
        dt = datetime.strptime(process_date, '%Y-%m-%d')
        date_sk = int(dt.strftime('%Y%m%d'))

        cursor.execute("""
            INSERT INTO dwh_dim_date (
                date_sk, date_value, day, month, quarter, year,
                week_of_year, is_weekend, day_name, month_name
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date_sk) DO UPDATE SET
                day_name = EXCLUDED.day_name,
                month_name = EXCLUDED.month_name
        """, (
            date_sk, dt.date(), dt.day, dt.month,
            (dt.month - 1) // 3 + 1, dt.year, dt.isocalendar()[1],
            dt.weekday() >= 5, dt.strftime('%A'), dt.strftime('%B')
        ))

        count = cursor.rowcount
        logger.info(f"📅 Built date dimension: {count} records")
        return count

    def _build_platform_dimension(self, cursor) -> int:
        """Build platform dimension from ODS"""
        cursor.execute("""
            INSERT INTO dwh_dim_platform (
                platform_code, platform_name, website_url, country_code, is_active
            )
            SELECT
                platform_code, platform_name, website_url, country_code, is_active
            FROM ods_platform_ref
            WHERE platform_code IN ('tiki', 'lazada')
            ON CONFLICT (platform_code) DO UPDATE SET
                platform_name = EXCLUDED.platform_name,
                website_url = EXCLUDED.website_url,
                is_active = EXCLUDED.is_active
        """)

        count = cursor.rowcount
        logger.info(f"🏢 Built platform dimension: {count} records")
        return count

    def _build_brand_dimension(self, cursor) -> int:
        """Build brand dimension from ODS products"""
        cursor.execute("""
            INSERT INTO dwh_dim_brand (brand_code, brand_name)
            SELECT DISTINCT
                LOWER(REGEXP_REPLACE(brand_name, '[^a-zA-Z0-9]', '_', 'g')),
                brand_name
            FROM ods_product_clean
            WHERE brand_name IS NOT NULL
                AND TRIM(brand_name) != ''
                AND LENGTH(TRIM(brand_name)) > 1
            ON CONFLICT (brand_code) DO UPDATE SET
                brand_name = EXCLUDED.brand_name
        """)

        count = cursor.rowcount
        logger.info(f"🏷️ Built brand dimension: {count} records")
        return count

    def _build_category_dimension(self, cursor) -> int:
        """Build category dimension from ODS taxonomy"""
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
            ON CONFLICT (category_code) DO UPDATE SET
                category_name = EXCLUDED.category_name,
                category_level = EXCLUDED.category_level
        """)

        count = cursor.rowcount
        logger.info(f"📂 Built category dimension: {count} records")
        return count

    def _build_product_dimension(self, cursor, process_date: str) -> int:
        """Build product dimension with SCD Type 2"""
        dt = datetime.strptime(process_date, '%Y-%m-%d')

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
            LEFT JOIN dwh_dim_brand db ON LOWER(REGEXP_REPLACE(pc.brand_name, '[^a-zA-Z0-9]', '_', 'g')) = db.brand_code
            LEFT JOIN dwh_dim_category dc ON dc.category_code = 'electronics'  -- Default category
            WHERE DATE(pc.first_seen) = %s
                AND NOT EXISTS (
                    SELECT 1 FROM dwh_dim_product dp
                    WHERE dp.global_product_id = pc.global_product_id
                        AND dp.is_current = true
                )
            ON CONFLICT (global_product_id, effective_from) DO NOTHING
        """, (dt.date(), process_date))

        count = cursor.rowcount
        logger.info(f"📦 Built product dimension: {count} records")
        return count

    def _build_product_fact(self, cursor, process_date: str) -> int:
        """Build daily product fact table"""
        dt = datetime.strptime(process_date, '%Y-%m-%d')
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
            JOIN ods_product_clean pc ON pp.global_product_id = pc.global_product_id
            JOIN dwh_dim_product dp ON pc.global_product_id = dp.global_product_id AND dp.is_current = true
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
                is_available = EXCLUDED.is_available,
                captured_at = NOW()
        """, (date_sk, process_date))

        count = cursor.rowcount
        logger.info(f"📊 Built product fact: {count} records")
        return count

    def _build_review_fact(self, cursor, process_date: str) -> int:
        """Build review summary fact table"""
        dt = datetime.strptime(process_date, '%Y-%m-%d')
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
            JOIN ods_product_clean pc ON rs.global_product_id = pc.global_product_id
            JOIN dwh_dim_product dp ON pc.global_product_id = dp.global_product_id AND dp.is_current = true
            JOIN dwh_dim_platform dpl ON rs.platform_sk = dpl.platform_sk
            WHERE DATE(rs.captured_at) = %s
                AND rs.review_count > 0
            ON CONFLICT (date_sk, product_sk, platform_sk)
            DO UPDATE SET
                total_reviews = EXCLUDED.total_reviews,
                avg_rating = EXCLUDED.avg_rating,
                positive_reviews = EXCLUDED.positive_reviews,
                negative_reviews = EXCLUDED.negative_reviews,
                neutral_reviews = EXCLUDED.neutral_reviews,
                sentiment_score = EXCLUDED.sentiment_score
        """, (date_sk, process_date))

        count = cursor.rowcount
        logger.info(f"⭐ Built review fact: {count} records")
        return count

    def _build_price_analytics(self, cursor, process_date: str) -> int:
        """Build price optimization data mart"""
        dt = datetime.strptime(process_date, '%Y-%m-%d')
        date_sk = int(dt.strftime('%Y%m%d'))

        cursor.execute("""
            WITH price_comparison AS (
                SELECT
                    product_sk,
                    platform_sk,
                    price_current,
                    price_original,
                    discount_pct,
                    MIN(price_current) OVER (PARTITION BY product_sk) as competitor_min_price,
                    MAX(price_current) OVER (PARTITION BY product_sk) as competitor_max_price,
                    RANK() OVER (PARTITION BY product_sk ORDER BY price_current) as price_rank,
                    LAG(price_current) OVER (
                        PARTITION BY product_sk, platform_sk
                        ORDER BY date_sk
                    ) as previous_price
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
                competitor_min_price,
                competitor_max_price,
                price_rank,
                CASE
                    WHEN previous_price IS NULL THEN 'new'
                    WHEN price_current > previous_price THEN 'increasing'
                    WHEN price_current < previous_price THEN 'decreasing'
                    ELSE 'stable'
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

        count = cursor.rowcount
        logger.info(f"💰 Built price analytics: {count} records")
        return count

    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("🔒 Database connection closed")


def main():
    """Main function for standalone execution"""
    import argparse

    parser = argparse.ArgumentParser(description='DWH Data Transformation for Tiki & Lazada')
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

    # Run transformation
    dwh = DWHTransformation(db_config)

    if not dwh.connect():
        logger.error("❌ Cannot connect to database")
        return False

    try:
        stats = dwh.transform_to_dwh(args.date)
        logger.info(f"✅ DWH transformation completed successfully")
        return True

    except Exception as e:
        logger.error(f"❌ DWH transformation failed: {e}")
        return False

    finally:
        dwh.close()


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)