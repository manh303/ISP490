#!/usr/bin/env python3
"""
Run Datawarehouse Pipeline từ MongoDB streaming data vào PostgreSQL
Bronze → Silver → Gold → DW_Core layers
"""

import json
import logging
import psycopg2
import pymongo
from datetime import datetime
import traceback

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DatawarehousePipeline:
    def __init__(self):
        # Database connections từ Docker containers
        self.postgres_config = {
            'host': 'postgres',  # Docker service name
            'port': 5432,
            'database': 'ecommerce_dss_1',
            'user': 'dss_user',
            'password': 'dss_password_123'
        }

        self.mongo_uri = 'mongodb://admin:admin_password@mongodb:27017/'  # Docker service name
        self.postgres_conn = None
        self.mongo_client = None

    def connect_databases(self):
        """Kết nối với PostgreSQL và MongoDB"""
        try:
            # Connect to PostgreSQL
            self.postgres_conn = psycopg2.connect(**self.postgres_config)
            self.postgres_conn.autocommit = True
            logger.info("✅ Connected to PostgreSQL")

            # Connect to MongoDB
            self.mongo_client = pymongo.MongoClient(self.mongo_uri)
            self.mongo_client.admin.command('ping')
            logger.info("✅ Connected to MongoDB")

            return True
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            return False

    def create_datawarehouse_schemas(self):
        """Tạo các schema cho datawarehouse"""
        try:
            cursor = self.postgres_conn.cursor()

            # Create schemas
            schemas = ['bronze', 'silver', 'gold', 'dw_core']
            for schema in schemas:
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
                logger.info(f"✅ Schema {schema} created/exists")

            cursor.close()
            return True
        except Exception as e:
            logger.error(f"❌ Failed to create schemas: {e}")
            return False

    def populate_bronze_layer(self):
        """BRONZE LAYER: Raw streaming data từ MongoDB"""
        logger.info("🔷 Processing Bronze Layer...")
        try:
            db = self.mongo_client['ecommerce_dss_1']

            # Lấy dữ liệu streaming từ MongoDB
            processed_orders = list(db.processed_orders_stream.find({}))
            logger.info(f"Found {len(processed_orders)} streaming orders in MongoDB")

            if not processed_orders:
                logger.warning("⚠️ No streaming data found in MongoDB")
                return 0

            cursor = self.postgres_conn.cursor()

            # Create bronze table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bronze.orders_raw (
                    id SERIAL PRIMARY KEY,
                    order_id VARCHAR(100),
                    customer_id VARCHAR(100),
                    kafka_topic VARCHAR(100),
                    kafka_partition INTEGER,
                    kafka_offset BIGINT,
                    original_data JSONB,
                    processed_at TIMESTAMP,
                    inserted_at TIMESTAMP DEFAULT NOW(),
                    source VARCHAR(50) DEFAULT 'streaming'
                );

                CREATE INDEX IF NOT EXISTS idx_bronze_orders_order_id ON bronze.orders_raw(order_id);
                CREATE INDEX IF NOT EXISTS idx_bronze_orders_processed_at ON bronze.orders_raw(processed_at);
            """)

            # Insert streaming data
            insert_count = 0
            for order_doc in processed_orders:
                try:
                    original_data = order_doc.get('original_data', {})
                    kafka_meta = order_doc.get('kafka_metadata', {})

                    cursor.execute("""
                        INSERT INTO bronze.orders_raw
                        (order_id, customer_id, kafka_topic, kafka_partition, kafka_offset, original_data, processed_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING;
                    """, (
                        original_data.get('order_id'),
                        original_data.get('customer_id'),
                        kafka_meta.get('topic', 'ecommerce.orders.stream'),
                        kafka_meta.get('partition', 0),
                        kafka_meta.get('offset', 0),
                        json.dumps(original_data),
                        order_doc.get('processed_at', datetime.now())
                    ))
                    insert_count += 1
                except Exception as e:
                    logger.warning(f"⚠️ Failed to insert order into bronze: {e}")

            cursor.close()
            logger.info(f"✅ Bronze layer: {insert_count} records inserted")
            return insert_count

        except Exception as e:
            logger.error(f"❌ Bronze layer failed: {e}")
            traceback.print_exc()
            return 0

    def populate_silver_layer(self):
        """SILVER LAYER: Cleaned and validated data"""
        logger.info("🔶 Processing Silver Layer...")
        try:
            cursor = self.postgres_conn.cursor()

            # Create silver table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS silver.orders_clean (
                    order_id VARCHAR(100) PRIMARY KEY,
                    customer_id VARCHAR(100),
                    product_id VARCHAR(100),
                    quantity INTEGER DEFAULT 1,
                    total_amount DECIMAL(15,2),
                    currency VARCHAR(10) DEFAULT 'VND',
                    order_date TIMESTAMP,
                    payment_method VARCHAR(50),
                    platform VARCHAR(50),
                    status VARCHAR(50),
                    city VARCHAR(100),
                    district VARCHAR(100),
                    processed_at TIMESTAMP DEFAULT NOW(),
                    data_quality_score DECIMAL(3,2) DEFAULT 1.0,
                    source VARCHAR(50) DEFAULT 'streaming'
                );

                CREATE INDEX IF NOT EXISTS idx_silver_orders_customer_id ON silver.orders_clean(customer_id);
                CREATE INDEX IF NOT EXISTS idx_silver_orders_order_date ON silver.orders_clean(order_date);
                CREATE INDEX IF NOT EXISTS idx_silver_orders_status ON silver.orders_clean(status);
            """)

            # Transform bronze to silver
            cursor.execute("""
                INSERT INTO silver.orders_clean
                (order_id, customer_id, product_id, quantity, total_amount, order_date,
                 payment_method, platform, status, city, district, processed_at)
                SELECT DISTINCT
                    (original_data->>'order_id')::VARCHAR(100),
                    (original_data->>'customer_id')::VARCHAR(100),
                    (original_data->>'product_id')::VARCHAR(100),
                    COALESCE((original_data->>'quantity')::INTEGER, 1),
                    COALESCE((original_data->>'total_amount')::DECIMAL, 0),
                    CASE
                        WHEN original_data->>'order_date' IS NOT NULL
                        THEN (original_data->>'order_date')::TIMESTAMP
                        ELSE NOW()
                    END,
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
            """)

            silver_count = cursor.rowcount
            cursor.close()
            logger.info(f"✅ Silver layer: {silver_count} records processed")
            return silver_count

        except Exception as e:
            logger.error(f"❌ Silver layer failed: {e}")
            traceback.print_exc()
            return 0

    def populate_gold_layer(self):
        """GOLD LAYER: Business aggregated data"""
        logger.info("🔸 Processing Gold Layer...")
        try:
            cursor = self.postgres_conn.cursor()

            # Create gold tables
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS gold.daily_sales_summary (
                    date_key DATE PRIMARY KEY,
                    total_orders INTEGER DEFAULT 0,
                    total_revenue DECIMAL(15,2) DEFAULT 0,
                    avg_order_value DECIMAL(15,2) DEFAULT 0,
                    unique_customers INTEGER DEFAULT 0,
                    top_payment_method VARCHAR(50),
                    top_platform VARCHAR(50),
                    top_city VARCHAR(100),
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS gold.customer_summary (
                    customer_id VARCHAR(100) PRIMARY KEY,
                    total_orders INTEGER DEFAULT 0,
                    total_spent DECIMAL(15,2) DEFAULT 0,
                    avg_order_value DECIMAL(15,2) DEFAULT 0,
                    first_order_date TIMESTAMP,
                    last_order_date TIMESTAMP,
                    favorite_payment_method VARCHAR(50),
                    favorite_platform VARCHAR(50),
                    city VARCHAR(100),
                    customer_segment VARCHAR(20),
                    updated_at TIMESTAMP DEFAULT NOW()
                );
            """)

            # Populate daily sales summary
            cursor.execute("""
                INSERT INTO gold.daily_sales_summary
                (date_key, total_orders, total_revenue, avg_order_value, unique_customers,
                 top_payment_method, top_platform, top_city, updated_at)
                SELECT
                    DATE(order_date) as date_key,
                    COUNT(*) as total_orders,
                    SUM(total_amount) as total_revenue,
                    AVG(total_amount) as avg_order_value,
                    COUNT(DISTINCT customer_id) as unique_customers,
                    MODE() WITHIN GROUP (ORDER BY payment_method) as top_payment_method,
                    MODE() WITHIN GROUP (ORDER BY platform) as top_platform,
                    MODE() WITHIN GROUP (ORDER BY city) as top_city,
                    NOW()
                FROM silver.orders_clean
                WHERE order_date >= CURRENT_DATE - INTERVAL '30 days'
                  AND total_amount > 0
                GROUP BY DATE(order_date)
                ON CONFLICT (date_key) DO UPDATE SET
                    total_orders = EXCLUDED.total_orders,
                    total_revenue = EXCLUDED.total_revenue,
                    avg_order_value = EXCLUDED.avg_order_value,
                    unique_customers = EXCLUDED.unique_customers,
                    top_payment_method = EXCLUDED.top_payment_method,
                    top_platform = EXCLUDED.top_platform,
                    top_city = EXCLUDED.top_city,
                    updated_at = NOW();
            """)

            daily_count = cursor.rowcount

            # Populate customer summary
            cursor.execute("""
                INSERT INTO gold.customer_summary
                (customer_id, total_orders, total_spent, avg_order_value,
                 first_order_date, last_order_date, favorite_payment_method,
                 favorite_platform, city, customer_segment, updated_at)
                SELECT
                    customer_id,
                    COUNT(*) as total_orders,
                    SUM(total_amount) as total_spent,
                    AVG(total_amount) as avg_order_value,
                    MIN(order_date) as first_order_date,
                    MAX(order_date) as last_order_date,
                    MODE() WITHIN GROUP (ORDER BY payment_method) as favorite_payment_method,
                    MODE() WITHIN GROUP (ORDER BY platform) as favorite_platform,
                    MODE() WITHIN GROUP (ORDER BY city) as city,
                    CASE
                        WHEN SUM(total_amount) > 5000000 THEN 'VIP'
                        WHEN SUM(total_amount) > 1000000 THEN 'Premium'
                        WHEN SUM(total_amount) > 100000 THEN 'Standard'
                        ELSE 'Basic'
                    END as customer_segment,
                    NOW()
                FROM silver.orders_clean
                WHERE total_amount > 0
                GROUP BY customer_id
                ON CONFLICT (customer_id) DO UPDATE SET
                    total_orders = EXCLUDED.total_orders,
                    total_spent = EXCLUDED.total_spent,
                    avg_order_value = EXCLUDED.avg_order_value,
                    last_order_date = EXCLUDED.last_order_date,
                    favorite_payment_method = EXCLUDED.favorite_payment_method,
                    favorite_platform = EXCLUDED.favorite_platform,
                    customer_segment = EXCLUDED.customer_segment,
                    updated_at = NOW();
            """)

            customer_count = cursor.rowcount
            cursor.close()

            logger.info(f"✅ Gold layer: {daily_count} daily summaries, {customer_count} customer summaries")
            return daily_count + customer_count

        except Exception as e:
            logger.error(f"❌ Gold layer failed: {e}")
            traceback.print_exc()
            return 0

    def populate_dw_core_layer(self):
        """DW_CORE LAYER: Star schema fact and dimension tables"""
        logger.info("💎 Processing DW Core Layer...")
        try:
            cursor = self.postgres_conn.cursor()

            # Create dimension and fact tables
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS dw_core.dim_customers (
                    customer_key SERIAL PRIMARY KEY,
                    customer_id VARCHAR(100) UNIQUE NOT NULL,
                    customer_segment VARCHAR(50),
                    city VARCHAR(100),
                    district VARCHAR(100),
                    total_lifetime_value DECIMAL(15,2) DEFAULT 0,
                    first_order_date DATE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS dw_core.dim_products (
                    product_key SERIAL PRIMARY KEY,
                    product_id VARCHAR(100) UNIQUE NOT NULL,
                    product_category VARCHAR(100),
                    created_at TIMESTAMP DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS dw_core.fact_orders (
                    order_key SERIAL PRIMARY KEY,
                    order_id VARCHAR(100) UNIQUE NOT NULL,
                    customer_key INTEGER REFERENCES dw_core.dim_customers(customer_key),
                    product_key INTEGER REFERENCES dw_core.dim_products(product_key),
                    date_key DATE,
                    quantity INTEGER DEFAULT 1,
                    unit_price DECIMAL(15,2) DEFAULT 0,
                    total_amount DECIMAL(15,2) DEFAULT 0,
                    order_date TIMESTAMP,
                    payment_method VARCHAR(50),
                    platform VARCHAR(50),
                    status VARCHAR(50),
                    created_at TIMESTAMP DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS idx_fact_orders_date_key ON dw_core.fact_orders(date_key);
                CREATE INDEX IF NOT EXISTS idx_fact_orders_customer_key ON dw_core.fact_orders(customer_key);
            """)

            # Populate dim_customers
            cursor.execute("""
                INSERT INTO dw_core.dim_customers
                (customer_id, customer_segment, city, district, total_lifetime_value, first_order_date)
                SELECT DISTINCT
                    customer_id,
                    'Standard' as customer_segment,
                    city,
                    district,
                    0 as total_lifetime_value,
                    DATE(MIN(order_date)) as first_order_date
                FROM silver.orders_clean
                WHERE customer_id IS NOT NULL
                GROUP BY customer_id, city, district
                ON CONFLICT (customer_id) DO UPDATE SET
                    city = EXCLUDED.city,
                    district = EXCLUDED.district,
                    updated_at = NOW();
            """)

            dim_customers_count = cursor.rowcount

            # Populate dim_products
            cursor.execute("""
                INSERT INTO dw_core.dim_products (product_id, product_category)
                SELECT DISTINCT
                    product_id,
                    'Electronics' as product_category
                FROM silver.orders_clean
                WHERE product_id IS NOT NULL
                ON CONFLICT (product_id) DO NOTHING;
            """)

            dim_products_count = cursor.rowcount

            # Populate fact_orders
            cursor.execute("""
                INSERT INTO dw_core.fact_orders
                (order_id, customer_key, product_key, date_key, quantity, unit_price,
                 total_amount, order_date, payment_method, platform, status)
                SELECT
                    s.order_id,
                    dc.customer_key,
                    dp.product_key,
                    DATE(s.order_date),
                    s.quantity,
                    s.total_amount / GREATEST(s.quantity, 1),
                    s.total_amount,
                    s.order_date,
                    s.payment_method,
                    s.platform,
                    s.status
                FROM silver.orders_clean s
                LEFT JOIN dw_core.dim_customers dc ON s.customer_id = dc.customer_id
                LEFT JOIN dw_core.dim_products dp ON s.product_id = dp.product_id
                WHERE s.order_date >= CURRENT_DATE - INTERVAL '7 days'
                ON CONFLICT (order_id) DO UPDATE SET
                    total_amount = EXCLUDED.total_amount,
                    quantity = EXCLUDED.quantity,
                    unit_price = EXCLUDED.unit_price,
                    status = EXCLUDED.status;
            """)

            fact_count = cursor.rowcount
            cursor.close()

            logger.info(f"✅ DW Core: {dim_customers_count} customers, {dim_products_count} products, {fact_count} facts")
            return fact_count

        except Exception as e:
            logger.error(f"❌ DW Core layer failed: {e}")
            traceback.print_exc()
            return 0

    def verify_pipeline_results(self):
        """Kiểm tra kết quả của pipeline"""
        logger.info("🔍 Verifying pipeline results...")
        try:
            cursor = self.postgres_conn.cursor()

            verification_queries = {
                'Bronze Orders': 'SELECT COUNT(*) FROM bronze.orders_raw',
                'Silver Orders': 'SELECT COUNT(*) FROM silver.orders_clean',
                'Gold Daily Summary': 'SELECT COUNT(*) FROM gold.daily_sales_summary',
                'Gold Customer Summary': 'SELECT COUNT(*) FROM gold.customer_summary',
                'DW Customers': 'SELECT COUNT(*) FROM dw_core.dim_customers',
                'DW Products': 'SELECT COUNT(*) FROM dw_core.dim_products',
                'DW Facts': 'SELECT COUNT(*) FROM dw_core.fact_orders'
            }

            results = {}
            for name, query in verification_queries.items():
                try:
                    cursor.execute(query)
                    count = cursor.fetchone()[0]
                    results[name] = count
                    logger.info(f"✅ {name}: {count:,} records")
                except Exception as e:
                    results[name] = f"Error: {e}"
                    logger.error(f"❌ {name}: {e}")

            cursor.close()
            return results

        except Exception as e:
            logger.error(f"❌ Verification failed: {e}")
            return {}

    def run_complete_pipeline(self):
        """Chạy toàn bộ pipeline Bronze → Silver → Gold → DW_Core"""
        logger.info("🚀 Starting Complete Datawarehouse Pipeline")
        logger.info("=" * 60)

        try:
            # Connect to databases
            if not self.connect_databases():
                return False

            # Create schemas
            if not self.create_datawarehouse_schemas():
                return False

            # Run pipeline layers
            bronze_count = self.populate_bronze_layer()
            if bronze_count == 0:
                logger.warning("⚠️ No data processed in Bronze layer")
                return False

            silver_count = self.populate_silver_layer()
            gold_count = self.populate_gold_layer()
            dw_count = self.populate_dw_core_layer()

            # Verify results
            results = self.verify_pipeline_results()

            logger.info("=" * 60)
            logger.info("🎯 PIPELINE COMPLETION SUMMARY")
            logger.info("=" * 60)
            logger.info(f"Bronze Layer: {bronze_count} records processed")
            logger.info(f"Silver Layer: {silver_count} records processed")
            logger.info(f"Gold Layer: {gold_count} records processed")
            logger.info(f"DW Core Layer: {dw_count} records processed")

            logger.info("\n📊 FINAL VERIFICATION RESULTS:")
            for layer, count in results.items():
                logger.info(f"  {layer}: {count}")

            logger.info("\n✅ Datawarehouse pipeline completed successfully!")
            logger.info("💾 Data is now available for analytics and dashboard")

            return True

        except Exception as e:
            logger.error(f"❌ Pipeline failed: {e}")
            traceback.print_exc()
            return False

        finally:
            # Cleanup connections
            if self.postgres_conn:
                self.postgres_conn.close()
            if self.mongo_client:
                self.mongo_client.close()

def main():
    """Main function"""
    pipeline = DatawarehousePipeline()
    pipeline.run_complete_pipeline()

if __name__ == "__main__":
    main()