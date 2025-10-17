#!/usr/bin/env python3
"""
Process Warehouse Data
Move data from streaming_data to Bronze → Silver → Gold → DW_Core
"""

import psycopg2
import logging
from datetime import datetime
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_db_connection():
    """Get database connection"""
    return psycopg2.connect(
        host='localhost',
        port=5433,
        database='ecommerce_dss',
        user='dss_user',
        password='dss_password_123'
    )

def process_streaming_to_bronze():
    """Move data from streaming_data to Bronze layer"""
    logger.info("🥉 PROCESSING STREAMING DATA TO BRONZE LAYER")

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Insert streaming orders to Bronze
        cursor.execute("""
            INSERT INTO bronze.orders_raw (
                order_id, customer_id, product_id, quantity, total_amount,
                order_date, platform, payment_method, status, city, district,
                original_data, created_at
            )
            SELECT
                order_id,
                customer_id,
                product_id,
                quantity,
                total_amount,
                order_date,
                platform,
                payment_method,
                status,
                city,
                district,
                jsonb_build_object(
                    'source_topic', source_topic,
                    'processed_at', processed_at
                ) as original_data,
                NOW()
            FROM streaming_data.orders_stream
            WHERE order_id NOT IN (SELECT order_id FROM bronze.orders_raw WHERE order_id IS NOT NULL)
            LIMIT 10000;
        """)

        bronze_inserted = cursor.rowcount
        conn.commit()
        logger.info(f"✅ Inserted {bronze_inserted:,} orders into Bronze layer")

        cursor.close()
        conn.close()
        return bronze_inserted

    except Exception as e:
        logger.error(f"❌ Bronze processing failed: {e}")
        conn.rollback()
        cursor.close()
        conn.close()
        return 0

def process_bronze_to_silver():
    """Move data from Bronze to Silver layer"""
    logger.info("🥈 PROCESSING BRONZE TO SILVER LAYER")

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Clean and validate data for Silver layer
        cursor.execute("""
            INSERT INTO silver.orders_clean (
                order_id, customer_id, product_id, quantity, total_amount,
                order_date, platform, payment_method, status, city, district,
                data_quality_score, validation_flags, created_at
            )
            SELECT
                order_id,
                customer_id,
                product_id,
                quantity,
                CASE
                    WHEN total_amount <= 0 THEN NULL
                    ELSE total_amount
                END as total_amount,
                order_date,
                COALESCE(platform, 'Unknown') as platform,
                COALESCE(payment_method, 'Unknown') as payment_method,
                COALESCE(status, 'Unknown') as status,
                COALESCE(NULLIF(city, ''), 'Unknown') as city,
                COALESCE(NULLIF(district, ''), 'Unknown') as district,
                -- Data quality score (0.0 to 1.0)
                CASE
                    WHEN order_id IS NOT NULL
                     AND customer_id IS NOT NULL
                     AND product_id IS NOT NULL
                     AND quantity > 0
                     AND total_amount > 0
                    THEN 1.0
                    ELSE 0.8
                END as data_quality_score,
                jsonb_build_object(
                    'has_valid_amount', total_amount > 0,
                    'has_location', city IS NOT NULL AND city != '',
                    'has_payment_method', payment_method IS NOT NULL
                ) as validation_flags,
                NOW()
            FROM bronze.orders_raw
            WHERE order_id NOT IN (SELECT order_id FROM silver.orders_clean WHERE order_id IS NOT NULL)
            AND created_at > NOW() - INTERVAL '1 hour'
            LIMIT 5000;
        """)

        silver_inserted = cursor.rowcount
        conn.commit()
        logger.info(f"✅ Processed {silver_inserted:,} orders into Silver layer")

        cursor.close()
        conn.close()
        return silver_inserted

    except Exception as e:
        logger.error(f"❌ Silver processing failed: {e}")
        conn.rollback()
        cursor.close()
        conn.close()
        return 0

def process_silver_to_gold():
    """Create Gold layer aggregations"""
    logger.info("🥇 PROCESSING SILVER TO GOLD LAYER")

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Update daily sales summary
        cursor.execute("""
            INSERT INTO gold.daily_sales_summary (
                date_key, total_orders, total_revenue, avg_order_value,
                unique_customers, unique_products, top_payment_method,
                top_platform, top_city, orders_by_status, revenue_by_payment,
                created_at, updated_at
            )
            SELECT
                CURRENT_DATE as date_key,
                COUNT(*) as total_orders,
                SUM(total_amount) as total_revenue,
                AVG(total_amount) as avg_order_value,
                COUNT(DISTINCT customer_id) as unique_customers,
                COUNT(DISTINCT product_id) as unique_products,
                (SELECT payment_method FROM silver.orders_clean
                 WHERE order_date::date = CURRENT_DATE
                 GROUP BY payment_method
                 ORDER BY COUNT(*) DESC LIMIT 1) as top_payment_method,
                (SELECT platform FROM silver.orders_clean
                 WHERE order_date::date = CURRENT_DATE
                 GROUP BY platform
                 ORDER BY COUNT(*) DESC LIMIT 1) as top_platform,
                (SELECT city FROM silver.orders_clean
                 WHERE order_date::date = CURRENT_DATE AND city != 'Unknown'
                 GROUP BY city
                 ORDER BY COUNT(*) DESC LIMIT 1) as top_city,
                jsonb_object_agg(status, status_count) as orders_by_status,
                jsonb_object_agg(payment_method, payment_revenue) as revenue_by_payment,
                NOW(),
                NOW()
            FROM (
                SELECT
                    status,
                    payment_method,
                    COUNT(*) as status_count,
                    SUM(total_amount) as payment_revenue
                FROM silver.orders_clean
                WHERE order_date::date = CURRENT_DATE
                GROUP BY status, payment_method
            ) sub
            ON CONFLICT (date_key)
            DO UPDATE SET
                total_orders = EXCLUDED.total_orders,
                total_revenue = EXCLUDED.total_revenue,
                avg_order_value = EXCLUDED.avg_order_value,
                unique_customers = EXCLUDED.unique_customers,
                unique_products = EXCLUDED.unique_products,
                updated_at = NOW();
        """)

        # Create customer summaries
        cursor.execute("""
            INSERT INTO gold.customer_summary (
                customer_id, total_orders, total_spent, avg_order_value,
                first_order_date, last_order_date, favorite_payment_method,
                favorite_platform, primary_city, customer_segment,
                days_since_last_order, created_at, updated_at
            )
            SELECT
                customer_id,
                COUNT(*) as total_orders,
                SUM(total_amount) as total_spent,
                AVG(total_amount) as avg_order_value,
                MIN(order_date::date) as first_order_date,
                MAX(order_date::date) as last_order_date,
                (SELECT payment_method FROM silver.orders_clean sc2
                 WHERE sc2.customer_id = sc.customer_id
                 GROUP BY payment_method
                 ORDER BY COUNT(*) DESC LIMIT 1) as favorite_payment_method,
                (SELECT platform FROM silver.orders_clean sc2
                 WHERE sc2.customer_id = sc.customer_id
                 GROUP BY platform
                 ORDER BY COUNT(*) DESC LIMIT 1) as favorite_platform,
                (SELECT city FROM silver.orders_clean sc2
                 WHERE sc2.customer_id = sc.customer_id AND city != 'Unknown'
                 GROUP BY city
                 ORDER BY COUNT(*) DESC LIMIT 1) as primary_city,
                CASE
                    WHEN SUM(total_amount) > 50000000 THEN 'Premium'
                    WHEN SUM(total_amount) > 10000000 THEN 'Standard'
                    ELSE 'Basic'
                END as customer_segment,
                CURRENT_DATE - MAX(order_date::date) as days_since_last_order,
                NOW(),
                NOW()
            FROM silver.orders_clean sc
            WHERE order_date > CURRENT_DATE - INTERVAL '30 days'
            GROUP BY customer_id
            ON CONFLICT (customer_id)
            DO UPDATE SET
                total_orders = EXCLUDED.total_orders,
                total_spent = EXCLUDED.total_spent,
                avg_order_value = EXCLUDED.avg_order_value,
                last_order_date = EXCLUDED.last_order_date,
                days_since_last_order = EXCLUDED.days_since_last_order,
                updated_at = NOW();
        """)

        conn.commit()
        logger.info("✅ Gold layer aggregations updated successfully")

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        logger.error(f"❌ Gold processing failed: {e}")
        conn.rollback()
        cursor.close()
        conn.close()
        return False

def process_gold_to_dw_core():
    """Move data to DW_Core star schema"""
    logger.info("⭐ PROCESSING GOLD TO DW_CORE STAR SCHEMA")

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Insert into fact table
        cursor.execute("""
            INSERT INTO dw_core.fact_orders (
                order_id, customer_key, product_key, time_key,
                quantity, unit_price, total_amount, order_timestamp,
                payment_method, platform, status, created_at
            )
            SELECT
                sc.order_id,
                COALESCE(dc.customer_key, 0) as customer_key,
                COALESCE(dp.product_key, 0) as product_key,
                COALESCE(dt.date_key, 1) as time_key,
                sc.quantity,
                sc.total_amount / NULLIF(sc.quantity, 0) as unit_price,
                sc.total_amount,
                sc.order_date as order_timestamp,
                sc.payment_method,
                sc.platform,
                sc.status,
                NOW()
            FROM silver.orders_clean sc
            LEFT JOIN dw_core.dim_customers dc ON sc.customer_id = dc.customer_id
            LEFT JOIN dw_core.dim_products dp ON sc.product_id = dp.product_id
            LEFT JOIN dw_core.dim_time dt ON sc.order_date::date = dt.date_value
            WHERE sc.order_id NOT IN (SELECT order_id FROM dw_core.fact_orders WHERE order_id IS NOT NULL)
            AND sc.created_at > NOW() - INTERVAL '1 hour'
            LIMIT 1000;
        """)

        dw_inserted = cursor.rowcount
        conn.commit()
        logger.info(f"✅ Inserted {dw_inserted:,} records into DW_Core fact table")

        cursor.close()
        conn.close()
        return dw_inserted

    except Exception as e:
        logger.error(f"❌ DW_Core processing failed: {e}")
        conn.rollback()
        cursor.close()
        conn.close()
        return 0

def main():
    """Main processing function"""
    logger.info("🚀 STARTING COMPLETE WAREHOUSE DATA PROCESSING")
    logger.info("=" * 60)

    # Process through all layers
    bronze_count = process_streaming_to_bronze()
    if bronze_count > 0:
        silver_count = process_bronze_to_silver()
        if silver_count > 0:
            gold_success = process_silver_to_gold()
            if gold_success:
                dw_count = process_gold_to_dw_core()
                logger.info(f"✅ Complete pipeline: {bronze_count} → {silver_count} → Gold → {dw_count}")

    logger.info("🎉 WAREHOUSE PROCESSING COMPLETE!")

if __name__ == "__main__":
    main()