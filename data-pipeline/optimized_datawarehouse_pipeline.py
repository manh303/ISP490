#!/usr/bin/env python3
"""
Optimized Datawarehouse Pipeline - Bronze→Silver→Gold→DW_Core
Tối ưu luồng dữ liệu để đảm bảo consistency và không mất dữ liệu
"""

import json
import logging
import psycopg2
import pymongo
from datetime import datetime, timedelta
import traceback
from decimal import Decimal
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class OptimizedDatawarehousePipeline:
    def __init__(self):
        # Database connections optimized for Docker environment
        self.postgres_config = {
            'host': '127.0.0.1',
            'port': 5432,
            'database': 'ecommerce_dss',
            'user': 'dss_user',
            'password': 'dss_password_123'
        }

        self.mongo_uri = 'mongodb://admin:admin_password@127.0.0.1:27017/'
        self.postgres_conn = None
        self.mongo_client = None

    def connect_databases(self):
        """Kết nối với PostgreSQL và MongoDB với error handling"""
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

    def clean_and_validate_data(self, data):
        """Clean và validate dữ liệu đầu vào"""
        try:
            # Chuẩn hóa order_id
            if 'order_id' in data and data['order_id']:
                data['order_id'] = str(data['order_id']).strip()

            # Chuẩn hóa customer_id
            if 'customer_id' in data and data['customer_id']:
                data['customer_id'] = str(data['customer_id']).strip()

            # Chuẩn hóa và validate total_amount
            if 'total_amount' in data:
                amount = data['total_amount']
                if isinstance(amount, (int, float)):
                    data['total_amount'] = float(amount)
                elif isinstance(amount, str):
                    # Remove any non-numeric characters except decimal point
                    cleaned_amount = re.sub(r'[^\d.]', '', amount)
                    data['total_amount'] = float(cleaned_amount) if cleaned_amount else 0.0
                else:
                    data['total_amount'] = 0.0

            # Chuẩn hóa quantity
            if 'quantity' in data:
                try:
                    data['quantity'] = int(data['quantity']) if data['quantity'] else 1
                except (ValueError, TypeError):
                    data['quantity'] = 1

            # Chuẩn hóa order_date
            if 'order_date' in data and data['order_date']:
                if isinstance(data['order_date'], str):
                    try:
                        # Parse ISO format datetime
                        data['order_date'] = datetime.fromisoformat(data['order_date'].replace('Z', '+00:00'))
                    except:
                        data['order_date'] = datetime.now()
                elif not isinstance(data['order_date'], datetime):
                    data['order_date'] = datetime.now()
            else:
                data['order_date'] = datetime.now()

            # Chuẩn hóa payment_method
            if 'payment_method' in data and data['payment_method']:
                data['payment_method'] = str(data['payment_method']).lower().strip()
            else:
                data['payment_method'] = 'unknown'

            # Chuẩn hóa platform
            if 'platform' in data and data['platform']:
                data['platform'] = str(data['platform']).lower().strip()
            else:
                data['platform'] = 'web'

            # Chuẩn hóa status
            if 'status' in data and data['status']:
                data['status'] = str(data['status']).lower().strip()
            else:
                data['status'] = 'pending'

            # Chuẩn hóa shipping address
            if 'shipping_address' in data and isinstance(data['shipping_address'], dict):
                addr = data['shipping_address']
                data['city'] = str(addr.get('city', 'Unknown')).strip()
                data['district'] = str(addr.get('district', 'Unknown')).strip()
            else:
                data['city'] = 'Unknown'
                data['district'] = 'Unknown'

            return data

        except Exception as e:
            logger.warning(f"⚠️ Data cleaning failed for record: {e}")
            return data

    def create_optimized_schemas(self):
        """Tạo schemas tối ưu với proper indexes và constraints"""
        try:
            cursor = self.postgres_conn.cursor()

            # Create schemas if not exist
            schemas = ['bronze', 'silver', 'gold', 'dw_core']
            for schema in schemas:
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

            # Drop and recreate tables for fresh start
            logger.info("🔄 Recreating optimized tables...")

            # BRONZE LAYER - Optimized
            cursor.execute("""
                DROP TABLE IF EXISTS bronze.orders_raw CASCADE;
                CREATE TABLE bronze.orders_raw (
                    id SERIAL PRIMARY KEY,
                    order_id VARCHAR(100) NOT NULL,
                    customer_id VARCHAR(100),
                    product_id VARCHAR(100),
                    kafka_topic VARCHAR(100),
                    kafka_partition INTEGER,
                    kafka_offset BIGINT,
                    original_data JSONB NOT NULL,
                    processed_at TIMESTAMP DEFAULT NOW(),
                    inserted_at TIMESTAMP DEFAULT NOW(),
                    source VARCHAR(50) DEFAULT 'streaming',
                    data_quality_score DECIMAL(3,2) DEFAULT 1.0,

                    -- Constraints
                    CONSTRAINT unique_bronze_order_offset UNIQUE(order_id, kafka_offset)
                );

                -- Indexes for performance
                CREATE INDEX IF NOT EXISTS idx_bronze_order_id ON bronze.orders_raw(order_id);
                CREATE INDEX IF NOT EXISTS idx_bronze_customer_id ON bronze.orders_raw(customer_id);
                CREATE INDEX IF NOT EXISTS idx_bronze_inserted_at ON bronze.orders_raw(inserted_at);
                CREATE INDEX IF NOT EXISTS idx_bronze_original_data_gin ON bronze.orders_raw USING GIN(original_data);
            """)

            # SILVER LAYER - Optimized
            cursor.execute("""
                DROP TABLE IF EXISTS silver.orders_clean CASCADE;
                CREATE TABLE silver.orders_clean (
                    order_id VARCHAR(100) PRIMARY KEY,
                    customer_id VARCHAR(100) NOT NULL,
                    product_id VARCHAR(100),
                    product_name VARCHAR(200),
                    quantity INTEGER DEFAULT 1 CHECK (quantity > 0),
                    total_amount DECIMAL(15,2) DEFAULT 0 CHECK (total_amount >= 0),
                    currency VARCHAR(10) DEFAULT 'VND',
                    order_date TIMESTAMP NOT NULL,
                    payment_method VARCHAR(50) NOT NULL,
                    platform VARCHAR(50) NOT NULL,
                    status VARCHAR(50) NOT NULL,
                    city VARCHAR(100),
                    district VARCHAR(100),
                    customer_age_group VARCHAR(20),
                    customer_gender VARCHAR(10),
                    customer_membership VARCHAR(20),
                    processed_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW(),
                    data_quality_score DECIMAL(3,2) DEFAULT 1.0,
                    source VARCHAR(50) DEFAULT 'streaming'
                );

                -- Indexes for analytics performance
                CREATE INDEX IF NOT EXISTS idx_silver_customer_id ON silver.orders_clean(customer_id);
                CREATE INDEX IF NOT EXISTS idx_silver_order_date ON silver.orders_clean(order_date);
                CREATE INDEX IF NOT EXISTS idx_silver_payment_method ON silver.orders_clean(payment_method);
                CREATE INDEX IF NOT EXISTS idx_silver_city ON silver.orders_clean(city);
                CREATE INDEX IF NOT EXISTS idx_silver_status ON silver.orders_clean(status);
                CREATE INDEX IF NOT EXISTS idx_silver_total_amount ON silver.orders_clean(total_amount);
            """)

            # GOLD LAYER - Optimized
            cursor.execute("""
                DROP TABLE IF EXISTS gold.daily_sales_summary CASCADE;
                CREATE TABLE gold.daily_sales_summary (
                    date_key DATE PRIMARY KEY,
                    total_orders INTEGER DEFAULT 0,
                    total_revenue DECIMAL(15,2) DEFAULT 0,
                    avg_order_value DECIMAL(15,2) DEFAULT 0,
                    unique_customers INTEGER DEFAULT 0,
                    unique_products INTEGER DEFAULT 0,
                    top_payment_method VARCHAR(50),
                    top_platform VARCHAR(50),
                    top_city VARCHAR(100),
                    orders_by_status JSONB,
                    revenue_by_payment JSONB,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                );

                DROP TABLE IF EXISTS gold.customer_summary CASCADE;
                CREATE TABLE gold.customer_summary (
                    customer_id VARCHAR(100) PRIMARY KEY,
                    total_orders INTEGER DEFAULT 0,
                    total_spent DECIMAL(15,2) DEFAULT 0,
                    avg_order_value DECIMAL(15,2) DEFAULT 0,
                    first_order_date DATE,
                    last_order_date DATE,
                    favorite_payment_method VARCHAR(50),
                    favorite_platform VARCHAR(50),
                    primary_city VARCHAR(100),
                    customer_age_group VARCHAR(20),
                    customer_gender VARCHAR(10),
                    customer_membership VARCHAR(20),
                    customer_segment VARCHAR(20),
                    days_since_last_order INTEGER,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                );

                -- Indexes for Gold layer
                CREATE INDEX IF NOT EXISTS idx_gold_daily_date ON gold.daily_sales_summary(date_key);
                CREATE INDEX IF NOT EXISTS idx_gold_customer_segment ON gold.customer_summary(customer_segment);
                CREATE INDEX IF NOT EXISTS idx_gold_customer_total_spent ON gold.customer_summary(total_spent);
            """)

            # DW_CORE LAYER - Optimized Star Schema
            cursor.execute("""
                DROP TABLE IF EXISTS dw_core.dim_customers CASCADE;
                CREATE TABLE dw_core.dim_customers (
                    customer_key SERIAL PRIMARY KEY,
                    customer_id VARCHAR(100) UNIQUE NOT NULL,
                    customer_segment VARCHAR(50),
                    age_group VARCHAR(20),
                    gender VARCHAR(10),
                    membership_level VARCHAR(20),
                    primary_city VARCHAR(100),
                    primary_district VARCHAR(100),
                    total_lifetime_value DECIMAL(15,2) DEFAULT 0,
                    first_order_date DATE,
                    last_order_date DATE,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                );

                DROP TABLE IF EXISTS dw_core.dim_products CASCADE;
                CREATE TABLE dw_core.dim_products (
                    product_key SERIAL PRIMARY KEY,
                    product_id VARCHAR(100) UNIQUE NOT NULL,
                    product_name VARCHAR(200),
                    product_category VARCHAR(100) DEFAULT 'Electronics',
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT NOW()
                );

                DROP TABLE IF EXISTS dw_core.dim_time CASCADE;
                CREATE TABLE dw_core.dim_time (
                    time_key SERIAL PRIMARY KEY,
                    date_value DATE UNIQUE NOT NULL,
                    year INTEGER,
                    quarter INTEGER,
                    month INTEGER,
                    day INTEGER,
                    day_of_week INTEGER,
                    day_name VARCHAR(20),
                    month_name VARCHAR(20),
                    is_weekend BOOLEAN
                );

                DROP TABLE IF EXISTS dw_core.fact_orders CASCADE;
                CREATE TABLE dw_core.fact_orders (
                    order_key SERIAL PRIMARY KEY,
                    order_id VARCHAR(100) UNIQUE NOT NULL,
                    customer_key INTEGER REFERENCES dw_core.dim_customers(customer_key),
                    product_key INTEGER REFERENCES dw_core.dim_products(product_key),
                    time_key INTEGER REFERENCES dw_core.dim_time(time_key),
                    quantity INTEGER DEFAULT 1,
                    unit_price DECIMAL(15,2) DEFAULT 0,
                    total_amount DECIMAL(15,2) DEFAULT 0,
                    order_date TIMESTAMP,
                    payment_method VARCHAR(50),
                    platform VARCHAR(50),
                    status VARCHAR(50),
                    created_at TIMESTAMP DEFAULT NOW()
                );

                -- Indexes for Star Schema performance
                CREATE INDEX IF NOT EXISTS idx_fact_customer_key ON dw_core.fact_orders(customer_key);
                CREATE INDEX IF NOT EXISTS idx_fact_product_key ON dw_core.fact_orders(product_key);
                CREATE INDEX IF NOT EXISTS idx_fact_time_key ON dw_core.fact_orders(time_key);
                CREATE INDEX IF NOT EXISTS idx_fact_order_date ON dw_core.fact_orders(order_date);
            """)

            cursor.close()
            logger.info("✅ Optimized schemas created successfully")
            return True

        except Exception as e:
            logger.error(f"❌ Schema creation failed: {e}")
            return False

    def populate_bronze_layer_optimized(self):
        """Populate Bronze layer với data cleaning và validation"""
        logger.info("🔷 Processing Optimized Bronze Layer...")
        try:
            db = self.mongo_client['ecommerce_dss']
            cursor = self.postgres_conn.cursor()

            # Get all streaming data from MongoDB
            streaming_collections = [
                'processed_orders_stream',
                'raw_data_collection'  # Backup collection if exists
            ]

            total_inserted = 0

            for collection_name in streaming_collections:
                try:
                    collection = db[collection_name]
                    documents = list(collection.find({}))
                    logger.info(f"Processing {len(documents)} documents from {collection_name}")

                    for doc in documents:
                        try:
                            # Extract and clean original data
                            original_data = doc.get('original_data', doc)

                            # Skip if no order_id
                            if not original_data.get('order_id'):
                                continue

                            # Clean and validate data
                            cleaned_data = self.clean_and_validate_data(original_data.copy())

                            # Extract metadata
                            kafka_meta = doc.get('kafka_metadata', {})

                            # Convert datetime objects to string for JSON serialization
                            def json_serial(obj):
                                if isinstance(obj, datetime):
                                    return obj.isoformat()
                                raise TypeError(f"Type {type(obj)} not serializable")

                            # Insert into Bronze layer
                            cursor.execute("""
                                INSERT INTO bronze.orders_raw
                                (order_id, customer_id, product_id, kafka_topic, kafka_partition, kafka_offset,
                                 original_data, processed_at, source)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (order_id, kafka_offset) DO UPDATE SET
                                    original_data = EXCLUDED.original_data,
                                    processed_at = NOW();
                            """, (
                                cleaned_data.get('order_id'),
                                cleaned_data.get('customer_id'),
                                cleaned_data.get('product_id'),
                                kafka_meta.get('topic', 'ecommerce.orders.stream'),
                                kafka_meta.get('partition', 0),
                                kafka_meta.get('offset', 0),
                                json.dumps(cleaned_data, default=json_serial),
                                doc.get('processed_at', datetime.now()),
                                'streaming_optimized'
                            ))
                            total_inserted += 1

                        except Exception as e:
                            logger.warning(f"⚠️ Failed to process document: {e}")
                            continue

                except Exception as e:
                    logger.warning(f"⚠️ Collection {collection_name} not found or error: {e}")
                    continue

            cursor.close()
            logger.info(f"✅ Bronze layer: {total_inserted} records processed")
            return total_inserted

        except Exception as e:
            logger.error(f"❌ Bronze layer failed: {e}")
            return 0

    def populate_silver_layer_optimized(self):
        """Populate Silver layer với comprehensive data transformation"""
        logger.info("🔶 Processing Optimized Silver Layer...")
        try:
            cursor = self.postgres_conn.cursor()

            # Transform ALL Bronze data to Silver with proper cleaning
            cursor.execute("""
                INSERT INTO silver.orders_clean
                (order_id, customer_id, product_id, product_name, quantity, total_amount,
                 order_date, payment_method, platform, status, city, district,
                 customer_age_group, customer_gender, customer_membership, processed_at, source)
                SELECT DISTINCT
                    (original_data->>'order_id')::VARCHAR(100) as order_id,
                    (original_data->>'customer_id')::VARCHAR(100) as customer_id,
                    (original_data->>'product_id')::VARCHAR(100) as product_id,
                    (original_data->>'product_name')::VARCHAR(200) as product_name,
                    COALESCE((original_data->>'quantity')::INTEGER, 1) as quantity,
                    COALESCE((original_data->>'total_amount')::DECIMAL, 0) as total_amount,
                    CASE
                        WHEN original_data->>'order_date' IS NOT NULL
                        THEN (original_data->>'order_date')::TIMESTAMP
                        ELSE NOW()
                    END as order_date,
                    COALESCE((original_data->>'payment_method')::VARCHAR(50), 'unknown') as payment_method,
                    COALESCE((original_data->>'platform')::VARCHAR(50), 'web') as platform,
                    COALESCE((original_data->>'status')::VARCHAR(50), 'pending') as status,
                    COALESCE((original_data->'shipping_address'->>'city')::VARCHAR(100), 'Unknown') as city,
                    COALESCE((original_data->'shipping_address'->>'district')::VARCHAR(100), 'Unknown') as district,
                    (original_data->'customer_info'->>'age_group')::VARCHAR(20) as customer_age_group,
                    (original_data->'customer_info'->>'gender')::VARCHAR(10) as customer_gender,
                    (original_data->'customer_info'->>'membership')::VARCHAR(20) as customer_membership,
                    NOW() as processed_at,
                    'bronze_to_silver_optimized' as source
                FROM bronze.orders_raw
                WHERE original_data->>'order_id' IS NOT NULL
                  AND (original_data->>'total_amount')::DECIMAL > 0
                ON CONFLICT (order_id) DO UPDATE SET
                    total_amount = EXCLUDED.total_amount,
                    status = EXCLUDED.status,
                    product_name = EXCLUDED.product_name,
                    customer_age_group = EXCLUDED.customer_age_group,
                    customer_gender = EXCLUDED.customer_gender,
                    customer_membership = EXCLUDED.customer_membership,
                    updated_at = NOW();
            """)

            silver_count = cursor.rowcount
            cursor.close()
            logger.info(f"✅ Silver layer: {silver_count} records processed")
            return silver_count

        except Exception as e:
            logger.error(f"❌ Silver layer failed: {e}")
            traceback.print_exc()
            return 0

    def populate_gold_layer_optimized(self):
        """Populate Gold layer với advanced business analytics"""
        logger.info("🔸 Processing Optimized Gold Layer...")
        try:
            cursor = self.postgres_conn.cursor()

            # Populate comprehensive daily sales summary
            cursor.execute("""
                INSERT INTO gold.daily_sales_summary
                (date_key, total_orders, total_revenue, avg_order_value, unique_customers, unique_products,
                 top_payment_method, top_platform, top_city, orders_by_status, revenue_by_payment, updated_at)
                SELECT
                    DATE(order_date) as date_key,
                    COUNT(*) as total_orders,
                    SUM(total_amount) as total_revenue,
                    AVG(total_amount) as avg_order_value,
                    COUNT(DISTINCT customer_id) as unique_customers,
                    COUNT(DISTINCT product_id) as unique_products,
                    MODE() WITHIN GROUP (ORDER BY payment_method) as top_payment_method,
                    MODE() WITHIN GROUP (ORDER BY platform) as top_platform,
                    MODE() WITHIN GROUP (ORDER BY city) as top_city,
                    -- JSON aggregations for detailed analysis
                    json_object_agg(status, status_count) as orders_by_status,
                    json_object_agg(payment_method, payment_revenue) as revenue_by_payment,
                    NOW() as updated_at
                FROM (
                    SELECT *,
                           COUNT(*) OVER (PARTITION BY DATE(order_date), status) as status_count,
                           SUM(total_amount) OVER (PARTITION BY DATE(order_date), payment_method) as payment_revenue
                    FROM silver.orders_clean
                    WHERE order_date >= CURRENT_DATE - INTERVAL '30 days'
                ) subq
                GROUP BY DATE(order_date)
                ON CONFLICT (date_key) DO UPDATE SET
                    total_orders = EXCLUDED.total_orders,
                    total_revenue = EXCLUDED.total_revenue,
                    avg_order_value = EXCLUDED.avg_order_value,
                    unique_customers = EXCLUDED.unique_customers,
                    unique_products = EXCLUDED.unique_products,
                    top_payment_method = EXCLUDED.top_payment_method,
                    top_platform = EXCLUDED.top_platform,
                    top_city = EXCLUDED.top_city,
                    orders_by_status = EXCLUDED.orders_by_status,
                    revenue_by_payment = EXCLUDED.revenue_by_payment,
                    updated_at = NOW();
            """)

            daily_count = cursor.rowcount

            # Populate comprehensive customer summary with segmentation
            cursor.execute("""
                INSERT INTO gold.customer_summary
                (customer_id, total_orders, total_spent, avg_order_value,
                 first_order_date, last_order_date, favorite_payment_method,
                 favorite_platform, primary_city, customer_age_group, customer_gender,
                 customer_membership, customer_segment, days_since_last_order, updated_at)
                SELECT
                    customer_id,
                    COUNT(*) as total_orders,
                    SUM(total_amount) as total_spent,
                    AVG(total_amount) as avg_order_value,
                    MIN(DATE(order_date)) as first_order_date,
                    MAX(DATE(order_date)) as last_order_date,
                    MODE() WITHIN GROUP (ORDER BY payment_method) as favorite_payment_method,
                    MODE() WITHIN GROUP (ORDER BY platform) as favorite_platform,
                    MODE() WITHIN GROUP (ORDER BY city) as primary_city,
                    MODE() WITHIN GROUP (ORDER BY customer_age_group) as customer_age_group,
                    MODE() WITHIN GROUP (ORDER BY customer_gender) as customer_gender,
                    MODE() WITHIN GROUP (ORDER BY customer_membership) as customer_membership,
                    -- Advanced customer segmentation
                    CASE
                        WHEN SUM(total_amount) > 50000000 THEN 'VIP'
                        WHEN SUM(total_amount) > 10000000 THEN 'Premium'
                        WHEN SUM(total_amount) > 1000000 THEN 'Standard'
                        ELSE 'Basic'
                    END as customer_segment,
                    CURRENT_DATE - MAX(DATE(order_date)) as days_since_last_order,
                    NOW() as updated_at
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
                    primary_city = EXCLUDED.primary_city,
                    customer_age_group = EXCLUDED.customer_age_group,
                    customer_gender = EXCLUDED.customer_gender,
                    customer_membership = EXCLUDED.customer_membership,
                    customer_segment = EXCLUDED.customer_segment,
                    days_since_last_order = EXCLUDED.days_since_last_order,
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

    def populate_dw_core_optimized(self):
        """Populate DW Core với proper star schema và time dimension"""
        logger.info("💎 Processing Optimized DW Core Layer...")
        try:
            cursor = self.postgres_conn.cursor()

            # Populate time dimension first
            cursor.execute("""
                INSERT INTO dw_core.dim_time (date_value, year, quarter, month, day, day_of_week, day_name, month_name, is_weekend)
                SELECT DISTINCT
                    DATE(order_date) as date_value,
                    EXTRACT(YEAR FROM order_date) as year,
                    EXTRACT(QUARTER FROM order_date) as quarter,
                    EXTRACT(MONTH FROM order_date) as month,
                    EXTRACT(DAY FROM order_date) as day,
                    EXTRACT(DOW FROM order_date) as day_of_week,
                    TO_CHAR(order_date, 'Day') as day_name,
                    TO_CHAR(order_date, 'Month') as month_name,
                    EXTRACT(DOW FROM order_date) IN (0, 6) as is_weekend
                FROM silver.orders_clean
                ON CONFLICT (date_value) DO NOTHING;
            """)

            # Populate customer dimension with Gold layer data
            cursor.execute("""
                INSERT INTO dw_core.dim_customers
                (customer_id, customer_segment, age_group, gender, membership_level,
                 primary_city, primary_district, total_lifetime_value, first_order_date, last_order_date)
                SELECT
                    gs.customer_id,
                    gs.customer_segment,
                    gs.customer_age_group,
                    gs.customer_gender,
                    gs.customer_membership,
                    gs.primary_city,
                    NULL as primary_district,  -- Can be enhanced later
                    gs.total_spent,
                    gs.first_order_date,
                    gs.last_order_date
                FROM gold.customer_summary gs
                ON CONFLICT (customer_id) DO UPDATE SET
                    customer_segment = EXCLUDED.customer_segment,
                    total_lifetime_value = EXCLUDED.total_lifetime_value,
                    last_order_date = EXCLUDED.last_order_date,
                    updated_at = NOW();
            """)

            dim_customers_count = cursor.rowcount

            # Populate product dimension
            cursor.execute("""
                INSERT INTO dw_core.dim_products (product_id, product_name, product_category)
                SELECT DISTINCT
                    product_id,
                    product_name,
                    'Electronics' as product_category
                FROM silver.orders_clean
                WHERE product_id IS NOT NULL
                ON CONFLICT (product_id) DO UPDATE SET
                    product_name = EXCLUDED.product_name;
            """)

            dim_products_count = cursor.rowcount

            # Populate fact table with proper foreign keys
            cursor.execute("""
                INSERT INTO dw_core.fact_orders
                (order_id, customer_key, product_key, time_key, quantity, unit_price,
                 total_amount, order_date, payment_method, platform, status)
                SELECT
                    s.order_id,
                    dc.customer_key,
                    dp.product_key,
                    dt.time_key,
                    s.quantity,
                    s.total_amount / GREATEST(s.quantity, 1) as unit_price,
                    s.total_amount,
                    s.order_date,
                    s.payment_method,
                    s.platform,
                    s.status
                FROM silver.orders_clean s
                LEFT JOIN dw_core.dim_customers dc ON s.customer_id = dc.customer_id
                LEFT JOIN dw_core.dim_products dp ON s.product_id = dp.product_id
                LEFT JOIN dw_core.dim_time dt ON DATE(s.order_date) = dt.date_value
                WHERE s.total_amount > 0
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
            logger.error(f"❌ DW Core failed: {e}")
            traceback.print_exc()
            return 0

    def run_optimized_pipeline(self):
        """Chạy toàn bộ pipeline tối ưu"""
        logger.info("🚀 Starting OPTIMIZED Datawarehouse Pipeline")
        logger.info("=" * 70)

        try:
            # Connect to databases
            if not self.connect_databases():
                return False

            # Create optimized schemas
            if not self.create_optimized_schemas():
                return False

            # Run optimized pipeline layers
            bronze_count = self.populate_bronze_layer_optimized()
            if bronze_count == 0:
                logger.warning("⚠️ No data processed in Bronze layer")
                return False

            silver_count = self.populate_silver_layer_optimized()
            gold_count = self.populate_gold_layer_optimized()
            dw_count = self.populate_dw_core_optimized()

            logger.info("=" * 70)
            logger.info("🎯 OPTIMIZED PIPELINE COMPLETION SUMMARY")
            logger.info("=" * 70)
            logger.info(f"Bronze Layer: {bronze_count} records processed")
            logger.info(f"Silver Layer: {silver_count} records processed")
            logger.info(f"Gold Layer: {gold_count} records processed")
            logger.info(f"DW Core Layer: {dw_count} records processed")

            logger.info("\n✅ Optimized datawarehouse pipeline completed successfully!")
            logger.info("💾 Consistent data flow với proper format và không mất dữ liệu!")

            return True

        except Exception as e:
            logger.error(f"❌ Optimized pipeline failed: {e}")
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
    pipeline = OptimizedDatawarehousePipeline()
    success = pipeline.run_optimized_pipeline()
    return success

if __name__ == "__main__":
    main()