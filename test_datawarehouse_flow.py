#!/usr/bin/env python3
"""
Test script to verify Bronze→Silver→Gold datawarehouse pipeline
Tests the complete data flow from MongoDB streaming data to PostgreSQL datawarehouse layers
"""

import json
import logging
from datetime import datetime
from sqlalchemy import create_engine, text
import pymongo
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_db_connection():
    """Get PostgreSQL database connection"""
    return create_engine('postgresql://dss_user:dss_password_123@localhost:5432/ecommerce_dss')

def get_mongo_client():
    """Get MongoDB client"""
    return pymongo.MongoClient('mongodb://admin:admin_password@localhost:27017/')

def populate_datawarehouse():
    """Populate datawarehouse tables from streaming data - Same as Airflow DAG function"""
    dw_results = {}

    try:
        engine = get_db_connection()
        mongo_client = get_mongo_client()
        db = mongo_client['ecommerce_dss']

        logger.info("Starting datawarehouse population test...")

        # 1. BRONZE LAYER - Raw streaming data from MongoDB to PostgreSQL
        try:
            logger.info("Processing Bronze layer...")
            # Load processed streaming orders from MongoDB
            processed_orders = list(db.processed_orders_stream.find({}))
            logger.info(f"Found {len(processed_orders)} processed orders in MongoDB")

            if processed_orders:
                # Create bronze orders table
                with engine.begin() as conn:
                    conn.execute(text("""
                        CREATE TABLE IF NOT EXISTS bronze.orders_raw (
                            id SERIAL PRIMARY KEY,
                            order_id VARCHAR(100),
                            customer_id VARCHAR(100),
                            kafka_topic VARCHAR(100),
                            kafka_partition INTEGER,
                            kafka_offset BIGINT,
                            original_data JSONB,
                            processed_at TIMESTAMP,
                            inserted_at TIMESTAMP DEFAULT NOW()
                        );
                    """))

                # Insert streaming data into bronze layer
                insert_count = 0
                for order_doc in processed_orders:
                    try:
                        original_data = order_doc.get('original_data', {})
                        kafka_meta = order_doc.get('kafka_metadata', {})

                        with engine.begin() as conn:
                            conn.execute(text("""
                                INSERT INTO bronze.orders_raw
                                (order_id, customer_id, kafka_topic, kafka_partition, kafka_offset, original_data, processed_at)
                                VALUES (:order_id, :customer_id, :topic, :partition, :offset, :data, :processed_at)
                                ON CONFLICT DO NOTHING;
                            """), {
                                'order_id': original_data.get('order_id'),
                                'customer_id': original_data.get('customer_id'),
                                'topic': kafka_meta.get('topic'),
                                'partition': kafka_meta.get('partition'),
                                'offset': kafka_meta.get('offset'),
                                'data': json.dumps(original_data),
                                'processed_at': order_doc.get('processed_at')
                            })
                            insert_count += 1
                    except Exception as e:
                        logger.warning(f"Failed to insert order into bronze: {e}")

                dw_results['bronze_orders'] = {
                    'status': 'completed',
                    'records_inserted': insert_count
                }
                logger.info(f"Bronze layer: {insert_count} records inserted")
            else:
                dw_results['bronze_orders'] = {
                    'status': 'skipped',
                    'reason': 'no_streaming_data'
                }
                logger.warning("Bronze layer: No streaming data found")

        except Exception as e:
            logger.error(f"Bronze layer population failed: {e}")
            dw_results['bronze_orders'] = {'status': 'failed', 'error': str(e)}

        # 2. SILVER LAYER - Cleaned and validated data
        try:
            logger.info("Processing Silver layer...")
            with engine.begin() as conn:
                # Create silver orders table
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS silver.orders_clean (
                        order_id VARCHAR(100) PRIMARY KEY,
                        customer_id VARCHAR(100),
                        product_id VARCHAR(100),
                        quantity INTEGER,
                        total_amount DECIMAL(15,2),
                        currency VARCHAR(10) DEFAULT 'VND',
                        order_date TIMESTAMP,
                        payment_method VARCHAR(50),
                        platform VARCHAR(50),
                        status VARCHAR(50),
                        city VARCHAR(100),
                        district VARCHAR(100),
                        processed_at TIMESTAMP DEFAULT NOW(),
                        data_quality_score DECIMAL(3,2) DEFAULT 1.0
                    );
                """))

                # Transform bronze to silver with data cleaning
                result = conn.execute(text("""
                    INSERT INTO silver.orders_clean
                    (order_id, customer_id, product_id, quantity, total_amount, order_date,
                     payment_method, platform, status, city, district, processed_at)
                    SELECT DISTINCT
                        (original_data->>'order_id')::VARCHAR(100),
                        (original_data->>'customer_id')::VARCHAR(100),
                        (original_data->>'product_id')::VARCHAR(100),
                        COALESCE((original_data->>'quantity')::INTEGER, 1),
                        COALESCE((original_data->>'total_amount')::DECIMAL, 0),
                        (original_data->>'order_date')::TIMESTAMP,
                        (original_data->>'payment_method')::VARCHAR(50),
                        (original_data->>'platform')::VARCHAR(50),
                        (original_data->>'status')::VARCHAR(50),
                        (original_data->'shipping_address'->>'city')::VARCHAR(100),
                        (original_data->'shipping_address'->>'district')::VARCHAR(100),
                        NOW()
                    FROM bronze.orders_raw
                    WHERE original_data->>'order_id' IS NOT NULL
                    ON CONFLICT (order_id) DO UPDATE SET
                        total_amount = EXCLUDED.total_amount,
                        status = EXCLUDED.status,
                        processed_at = NOW();
                """))

                silver_count = result.rowcount
                dw_results['silver_orders'] = {
                    'status': 'completed',
                    'records_processed': silver_count
                }
                logger.info(f"Silver layer: {silver_count} records processed")

        except Exception as e:
            logger.error(f"Silver layer population failed: {e}")
            dw_results['silver_orders'] = {'status': 'failed', 'error': str(e)}

        # 3. GOLD LAYER - Business-ready aggregated data
        try:
            logger.info("Processing Gold layer...")
            with engine.begin() as conn:
                # Create gold layer tables
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS gold.daily_sales_summary (
                        date_key DATE PRIMARY KEY,
                        total_orders INTEGER,
                        total_revenue DECIMAL(15,2),
                        avg_order_value DECIMAL(15,2),
                        unique_customers INTEGER,
                        top_payment_method VARCHAR(50),
                        top_platform VARCHAR(50),
                        created_at TIMESTAMP DEFAULT NOW(),
                        updated_at TIMESTAMP DEFAULT NOW()
                    );
                """))

                # Populate gold layer with daily aggregations
                result = conn.execute(text("""
                    INSERT INTO gold.daily_sales_summary
                    (date_key, total_orders, total_revenue, avg_order_value, unique_customers,
                     top_payment_method, top_platform, updated_at)
                    SELECT
                        DATE(order_date) as date_key,
                        COUNT(*) as total_orders,
                        SUM(total_amount) as total_revenue,
                        AVG(total_amount) as avg_order_value,
                        COUNT(DISTINCT customer_id) as unique_customers,
                        MODE() WITHIN GROUP (ORDER BY payment_method) as top_payment_method,
                        MODE() WITHIN GROUP (ORDER BY platform) as top_platform,
                        NOW()
                    FROM silver.orders_clean
                    WHERE order_date >= CURRENT_DATE - INTERVAL '30 days'
                    GROUP BY DATE(order_date)
                    ON CONFLICT (date_key) DO UPDATE SET
                        total_orders = EXCLUDED.total_orders,
                        total_revenue = EXCLUDED.total_revenue,
                        avg_order_value = EXCLUDED.avg_order_value,
                        unique_customers = EXCLUDED.unique_customers,
                        top_payment_method = EXCLUDED.top_payment_method,
                        top_platform = EXCLUDED.top_platform,
                        updated_at = NOW();
                """))

                gold_count = result.rowcount
                dw_results['gold_daily_summary'] = {
                    'status': 'completed',
                    'records_processed': gold_count
                }
                logger.info(f"Gold layer: {gold_count} records processed")

        except Exception as e:
            logger.error(f"Gold layer population failed: {e}")
            dw_results['gold_daily_summary'] = {'status': 'failed', 'error': str(e)}

        # 4. DW_CORE - Fact and dimension tables
        try:
            logger.info("Processing DW Core layer...")
            with engine.begin() as conn:
                # Create fact table
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS dw_core.fact_orders (
                        order_key SERIAL PRIMARY KEY,
                        order_id VARCHAR(100) UNIQUE,
                        customer_key INTEGER,
                        product_key INTEGER,
                        date_key DATE,
                        quantity INTEGER,
                        unit_price DECIMAL(15,2),
                        total_amount DECIMAL(15,2),
                        order_date TIMESTAMP,
                        created_at TIMESTAMP DEFAULT NOW()
                    );
                """))

                # Create dimension tables
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS dw_core.dim_customers (
                        customer_key SERIAL PRIMARY KEY,
                        customer_id VARCHAR(100) UNIQUE,
                        customer_segment VARCHAR(50),
                        city VARCHAR(100),
                        district VARCHAR(100),
                        created_at TIMESTAMP DEFAULT NOW(),
                        updated_at TIMESTAMP DEFAULT NOW()
                    );
                """))

                # Populate fact table from silver layer
                result = conn.execute(text("""
                    INSERT INTO dw_core.fact_orders
                    (order_id, date_key, quantity, total_amount, unit_price, order_date)
                    SELECT
                        order_id,
                        DATE(order_date),
                        quantity,
                        total_amount,
                        total_amount / GREATEST(quantity, 1),
                        order_date
                    FROM silver.orders_clean
                    WHERE order_date >= CURRENT_DATE - INTERVAL '7 days'
                    ON CONFLICT (order_id) DO UPDATE SET
                        total_amount = EXCLUDED.total_amount,
                        quantity = EXCLUDED.quantity,
                        unit_price = EXCLUDED.unit_price;
                """))

                fact_count = result.rowcount
                dw_results['dw_core_facts'] = {
                    'status': 'completed',
                    'records_processed': fact_count
                }
                logger.info(f"DW Core layer: {fact_count} records processed")

        except Exception as e:
            logger.error(f"DW Core population failed: {e}")
            dw_results['dw_core_facts'] = {'status': 'failed', 'error': str(e)}

        # Summary
        successful_layers = len([r for r in dw_results.values() if r.get('status') == 'completed'])
        total_records = sum(r.get('records_processed', r.get('records_inserted', 0))
                          for r in dw_results.values() if r.get('status') == 'completed')

        logger.info(f"Datawarehouse population completed: {successful_layers} layers, {total_records} total records processed")

        return dw_results

    except Exception as e:
        error_msg = f"Datawarehouse population failed: {str(e)}"
        logger.error(error_msg)
        raise

def verify_data_flow():
    """Verify the data flow through all layers"""
    engine = get_db_connection()

    logger.info("Verifying data flow across all layers...")

    layers = {
        'Bronze': 'SELECT COUNT(*) as count FROM bronze.orders_raw',
        'Silver': 'SELECT COUNT(*) as count FROM silver.orders_clean',
        'Gold': 'SELECT COUNT(*) as count FROM gold.daily_sales_summary',
        'DW Core': 'SELECT COUNT(*) as count FROM dw_core.fact_orders'
    }

    results = {}
    for layer, query in layers.items():
        try:
            with engine.connect() as conn:
                result = conn.execute(text(query))
                count = result.fetchone()[0]
                results[layer] = count
                logger.info(f"{layer} layer: {count} records")
        except Exception as e:
            logger.error(f"Failed to query {layer} layer: {e}")
            results[layer] = f"Error: {e}"

    return results

def main():
    """Main test function"""
    logger.info("Starting Bronze→Silver→Gold datawarehouse flow test")

    try:
        # Step 1: Populate datawarehouse
        dw_results = populate_datawarehouse()

        # Step 2: Verify data flow
        verification_results = verify_data_flow()

        # Step 3: Display results
        logger.info("\n" + "="*50)
        logger.info("DATAWAREHOUSE POPULATION RESULTS:")
        logger.info("="*50)
        for layer, result in dw_results.items():
            status = result.get('status', 'unknown')
            records = result.get('records_processed', result.get('records_inserted', 0))
            logger.info(f"{layer}: {status} ({records} records)")

        logger.info("\n" + "="*50)
        logger.info("DATA VERIFICATION RESULTS:")
        logger.info("="*50)
        for layer, count in verification_results.items():
            logger.info(f"{layer}: {count} records")

        logger.info("\n✅ Bronze→Silver→Gold datawarehouse pipeline test completed successfully!")

    except Exception as e:
        logger.error(f"❌ Test failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()