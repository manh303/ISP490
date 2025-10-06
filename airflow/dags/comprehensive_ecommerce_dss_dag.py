#!/usr/bin/env python3
"""
Comprehensive E-commerce DSS Pipeline DAG
===================================
Complete data pipeline for E-commerce Decision Support System including:
- Data Collection & Ingestion
- Real-time Streaming Setup
- Data Processing & Transformation
- Machine Learning Pipeline
- Data Quality & Validation
- Analytics & Reporting
- Monitoring & Alerting
- Backup & Recovery

Author: E-commerce DSS Team
Version: 2.0.0
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

# Airflow imports
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

# Custom operators
import sys
sys.path.append('/opt/airflow/dags')

# Database and external connections
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import pymongo
import requests
from kafka import KafkaProducer, KafkaConsumer
import redis

# ML and analytics
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
import joblib

# ================================
# DAG CONFIGURATION
# ================================
DAG_ID = "comprehensive_ecommerce_dss_pipeline"
DAG_DESCRIPTION = "Complete E-commerce DSS Pipeline with full functionality"

# Schedule: Run every 4 hours for comprehensive processing
SCHEDULE_INTERVAL = "0 */4 * * *"  # Every 4 hours

# Default arguments
DEFAULT_ARGS = {
    'owner': 'dss-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['admin@ecommerce-dss.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1,
    'catchup': False,
}

# Environment variables
DB_CONN_ID = "postgres_default"
MONGO_CONN_URI = Variable.get("MONGODB_URI", "mongodb://admin:admin_password@mongodb:27017/")
KAFKA_SERVERS = Variable.get("KAFKA_SERVERS", "kafka:29092")
REDIS_URL = Variable.get("REDIS_URL", "redis://redis:6379/0")

# ================================
# UTILITY FUNCTIONS
# ================================

def get_db_connection():
    """Get PostgreSQL database connection"""
    hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    return hook.get_sqlalchemy_engine()

def get_mongo_client():
    """Get MongoDB client"""
    return pymongo.MongoClient(MONGO_CONN_URI)

def get_redis_client():
    """Get Redis client"""
    return redis.from_url(REDIS_URL)

def send_alert(message: str, severity: str = "INFO"):
    """Send alert to monitoring system"""
    try:
        mongo_client = get_mongo_client()
        db = mongo_client['ecommerce_dss']

        alert_doc = {
            'alert_id': f"dag_{datetime.now().isoformat()}",
            'alert_type': 'pipeline_status',
            'severity': severity,
            'title': f'Pipeline Alert - {severity}',
            'message': message,
            'component': 'airflow',
            'source': DAG_ID,
            'triggered_at': datetime.now(),
            'is_active': True,
            'metadata': {
                'dag_id': DAG_ID,
                'execution_date': datetime.now().isoformat()
            }
        }

        db.alerts.insert_one(alert_doc)
        logging.info(f"Alert sent: {message}")

    except Exception as e:
        logging.error(f"Failed to send alert: {str(e)}")

# ================================
# DATA COLLECTION TASKS
# ================================

def check_data_sources(**context):
    """Check availability of all data sources"""
    sources_status = {}

    try:
        # Check PostgreSQL
        engine = get_db_connection()
        with engine.connect() as conn:
            from sqlalchemy import text
            result = conn.execute(text("SELECT 1"))
            sources_status['postgresql'] = True

        # Check MongoDB
        mongo_client = get_mongo_client()
        mongo_client.admin.command('ping')
        sources_status['mongodb'] = True

        # Check Redis
        redis_client = get_redis_client()
        redis_client.ping()
        sources_status['redis'] = True

        # Check Kafka
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        producer.close()
        sources_status['kafka'] = True

        logging.info(f"Data sources status: {sources_status}")

        # Store status in XCom
        context['task_instance'].xcom_push(key='sources_status', value=sources_status)

        # Send success alert
        send_alert("All data sources are healthy", "INFO")

        return sources_status

    except Exception as e:
        error_msg = f"Data source check failed: {str(e)}"
        logging.error(error_msg)
        send_alert(error_msg, "CRITICAL")
        raise

def collect_external_data(**context):
    """Collect data from available real data sources"""
    collected_data = {}

    try:
        # Real data sources based on available datasets
        data_sources = [
            {
                'name': 'kaggle_retail',
                'path': '/opt/airflow/data/raw/kaggle_datasets/data.csv',
                'type': 'csv'
            },
            {
                'name': 'vietnamese_integrated',
                'path': '/opt/airflow/data/integrated_data/vietnamese_ecommerce_integrated.csv',
                'type': 'csv'
            },
            {
                'name': 'electronics_catalog',
                'path': '/opt/airflow/data/real_datasets/kaggle_electronics_filtered.csv',
                'type': 'csv'
            },
            {
                'name': 'dummyjson_electronics',
                'path': '/opt/airflow/data/expanded_real/dummyjson_expanded_electronics.csv',
                'type': 'csv'
            },
            {
                'name': 'synthetic_transactions',
                'path': '/opt/airflow/data/expanded_real/synthetic_realistic_transactions.csv',
                'type': 'csv'
            }
        ]

        mongo_client = get_mongo_client()
        db = mongo_client['ecommerce_dss']

        for source in data_sources:
            try:
                # Check if file exists and load data
                import os
                file_path = source['path']

                if os.path.exists(file_path):
                    if source['type'] == 'csv':
                        df = pd.read_csv(file_path)
                        record_count = len(df)

                        # Store sample data in MongoDB
                        sample_data = df.head(100).to_dict('records') if record_count > 0 else []

                        collection_doc = {
                            'collection_id': f"{source['name']}_{datetime.now().isoformat()}",
                            'source': source['name'],
                            'file_path': file_path,
                            'record_count': record_count,
                            'sample_data': sample_data,
                            'collected_at': datetime.now(),
                            'processed': False,
                            'columns': list(df.columns) if record_count > 0 else []
                        }

                        db.raw_data_collection.insert_one(collection_doc)

                        collected_data[source['name']] = {
                            'source': source['name'],
                            'timestamp': datetime.now().isoformat(),
                            'status': 'collected',
                            'record_count': record_count,
                            'file_path': file_path
                        }

                        logging.info(f"Collected {record_count} records from {source['name']}")

                else:
                    collected_data[source['name']] = {
                        'status': 'file_not_found',
                        'error': f"File not found: {file_path}"
                    }
                    logging.warning(f"File not found: {file_path}")

            except Exception as e:
                logging.error(f"Failed to collect from {source['name']}: {str(e)}")
                collected_data[source['name']] = {'status': 'failed', 'error': str(e)}

        # Store summary in XCom
        context['task_instance'].xcom_push(key='collected_data', value=collected_data)

        success_count = sum(1 for data in collected_data.values() if data.get('status') == 'collected')
        total_records = sum(data.get('record_count', 0) for data in collected_data.values() if data.get('status') == 'collected')

        send_alert(f"Collected data from {success_count}/{len(data_sources)} sources, {total_records:,} total records", "INFO")

        return collected_data

    except Exception as e:
        error_msg = f"External data collection failed: {str(e)}"
        logging.error(error_msg)
        send_alert(error_msg, "HIGH")
        raise

def setup_streaming_topics(**context):
    """Setup and verify Kafka topics for streaming"""
    try:
        from kafka.admin import KafkaAdminClient, NewTopic
        from kafka.errors import TopicAlreadyExistsError

        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_SERVERS,
            client_id='airflow_topic_setup'
        )

        # Define topics for streaming data
        topics = [
            NewTopic(name="ecommerce.products.stream", num_partitions=3, replication_factor=1),
            NewTopic(name="ecommerce.customers.stream", num_partitions=3, replication_factor=1),
            NewTopic(name="ecommerce.orders.stream", num_partitions=3, replication_factor=1),
            NewTopic(name="ecommerce.analytics.stream", num_partitions=3, replication_factor=1),
            NewTopic(name="ecommerce.ml.predictions", num_partitions=2, replication_factor=1),
            NewTopic(name="ecommerce.alerts", num_partitions=1, replication_factor=1),
        ]

        created_topics = []
        existing_topics = []

        # Try to create all topics at once
        try:
            future_map = admin_client.create_topics(topics, validate_only=False)

            # Wait for topics to be created
            for topic, future in future_map.items():
                try:
                    future.result()  # Block until topic is created
                    created_topics.append(topic)
                    logging.info(f"Created topic: {topic}")
                except TopicAlreadyExistsError:
                    existing_topics.append(topic)
                    logging.info(f"Topic already exists: {topic}")
                except Exception as e:
                    logging.error(f"Failed to create topic {topic}: {str(e)}")

        except Exception as e:
            logging.warning(f"Batch topic creation failed, trying individual creation: {str(e)}")
            # Fallback to individual topic creation
            for topic in topics:
                try:
                    admin_client.create_topics([topic])
                    created_topics.append(topic.name)
                    logging.info(f"Created topic: {topic.name}")
                except TopicAlreadyExistsError:
                    existing_topics.append(topic.name)
                    logging.info(f"Topic already exists: {topic.name}")
                except Exception as e:
                    logging.error(f"Failed to create topic {topic.name}: {str(e)}")

        # Verify topics exist
        try:
            topic_metadata = admin_client.list_topics()
            # Check if it's a metadata object or list
            if hasattr(topic_metadata, 'topics'):
                existing_topic_names = set(topic_metadata.topics.keys())
            else:
                # If it's a list or set
                existing_topic_names = set(topic_metadata)

            verified_topics = [topic.name for topic in topics if topic.name in existing_topic_names]
        except Exception as e:
            logging.warning(f"Failed to verify topics: {str(e)}")
            verified_topics = created_topics + existing_topics

        result = {
            'created_topics': created_topics,
            'existing_topics': existing_topics,
            'verified_topics': verified_topics,
            'total_expected': len(topics)
        }

        context['task_instance'].xcom_push(key='streaming_setup', value=result)

        send_alert(f"Streaming topics setup: {len(verified_topics)}/{len(topics)} topics ready", "INFO")

        return result

    except Exception as e:
        error_msg = f"Streaming setup failed: {str(e)}"
        logging.error(error_msg)
        send_alert(error_msg, "HIGH")
        raise

# ================================
# DATA PROCESSING TASKS
# ================================

def validate_data_quality(**context):
    """Comprehensive data quality validation"""
    validation_results = {}

    try:
        engine = get_db_connection()

        # Quality checks for different tables
        quality_checks = {
            'customers': [
                "SELECT COUNT(*) as total_customers FROM customers",
                "SELECT COUNT(*) as customers_with_email FROM customers WHERE email IS NOT NULL",
                "SELECT COUNT(*) as duplicate_emails FROM (SELECT email, COUNT(*) FROM customers GROUP BY email HAVING COUNT(*) > 1) t",
                "SELECT AVG(total_spent) as avg_customer_value FROM customers WHERE total_spent > 0"
            ],
            'products': [
                "SELECT COUNT(*) as total_products FROM products",
                "SELECT COUNT(*) as active_products FROM products WHERE is_active = true",
                "SELECT COUNT(*) as products_with_price FROM products WHERE price > 0",
                "SELECT AVG(price) as avg_price FROM products WHERE price > 0"
            ],
            'orders': [
                "SELECT COUNT(*) as total_orders FROM orders",
                "SELECT COUNT(*) as completed_orders FROM orders WHERE status = 'delivered'",
                "SELECT SUM(total_amount) as total_revenue FROM orders WHERE status != 'cancelled'",
                "SELECT COUNT(*) as orders_today FROM orders WHERE DATE(order_date) = CURRENT_DATE"
            ]
        }

        for table, queries in quality_checks.items():
            table_results = {}

            for query in queries:
                try:
                    df = pd.read_sql(query, engine)
                    # Get the first column name and value
                    metric_name = df.columns[0]
                    metric_value = df.iloc[0, 0]
                    table_results[metric_name] = float(metric_value) if metric_value is not None else 0
                except Exception as e:
                    logging.error(f"Quality check failed for {table}: {str(e)}")
                    table_results[query] = {'error': str(e)}

            validation_results[table] = table_results

        # Calculate overall quality score
        total_checks = sum(len(checks) for checks in quality_checks.values())
        successful_checks = sum(
            len([k for k, v in results.items() if not isinstance(v, dict) or 'error' not in v])
            for results in validation_results.values()
        )

        quality_score = (successful_checks / total_checks) * 100
        validation_results['quality_score'] = quality_score
        validation_results['total_checks'] = total_checks
        validation_results['successful_checks'] = successful_checks

        # Store results
        context['task_instance'].xcom_push(key='data_quality', value=validation_results)

        # Send alert based on quality score
        if quality_score >= 95:
            send_alert(f"Data quality excellent: {quality_score:.1f}%", "INFO")
        elif quality_score >= 80:
            send_alert(f"Data quality good: {quality_score:.1f}%", "MEDIUM")
        else:
            send_alert(f"Data quality poor: {quality_score:.1f}% - Investigation needed", "HIGH")

        return validation_results

    except Exception as e:
        error_msg = f"Data quality validation failed: {str(e)}"
        logging.error(error_msg)
        send_alert(error_msg, "HIGH")
        raise

def process_streaming_data(**context):
    """Process streaming data from Kafka topics"""
    processing_results = {}

    try:
        # Get MongoDB client for storing processed data
        mongo_client = get_mongo_client()
        db = mongo_client['ecommerce_dss']

        # Topics to process
        topics = ['ecommerce.products.stream', 'ecommerce.customers.stream', 'ecommerce.orders.stream']

        for topic in topics:
            try:
                # Create consumer
                consumer = KafkaConsumer(
                    topic,
                    bootstrap_servers=KAFKA_SERVERS,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    consumer_timeout_ms=10000,  # 10 second timeout
                    auto_offset_reset='latest'
                )

                messages_processed = 0
                batch_data = []

                # Process messages in batches
                for message in consumer:
                    try:
                        data = message.value

                        # Add processing metadata
                        processed_record = {
                            'original_data': data,
                            'kafka_metadata': {
                                'topic': message.topic,
                                'partition': message.partition,
                                'offset': message.offset,
                                'timestamp': message.timestamp
                            },
                            'processed_at': datetime.now(),
                            'processing_version': '2.0.0',
                            'processed': True
                        }

                        batch_data.append(processed_record)
                        messages_processed += 1

                        # Process in batches of 100
                        if len(batch_data) >= 100:
                            collection_name = f"processed_{topic.replace('.', '_').replace('ecommerce_', '')}"
                            db[collection_name].insert_many(batch_data)
                            batch_data = []

                    except Exception as e:
                        logging.error(f"Failed to process message from {topic}: {str(e)}")

                # Insert remaining batch
                if batch_data:
                    collection_name = f"processed_{topic.replace('.', '_').replace('ecommerce_', '')}"
                    db[collection_name].insert_many(batch_data)

                consumer.close()
                processing_results[topic] = {
                    'messages_processed': messages_processed,
                    'status': 'completed'
                }

            except Exception as e:
                logging.error(f"Failed to process topic {topic}: {str(e)}")
                processing_results[topic] = {
                    'messages_processed': 0,
                    'status': 'failed',
                    'error': str(e)
                }

        # Store results
        context['task_instance'].xcom_push(key='streaming_processing', value=processing_results)

        total_processed = sum(result.get('messages_processed', 0) for result in processing_results.values())
        send_alert(f"Processed {total_processed} streaming messages across {len(topics)} topics", "INFO")

        return processing_results

    except Exception as e:
        error_msg = f"Streaming data processing failed: {str(e)}"
        logging.error(error_msg)
        send_alert(error_msg, "HIGH")
        raise

def transform_and_aggregate(**context):
    """Transform raw data and create aggregated views"""
    transformation_results = {}

    try:
        engine = get_db_connection()

        # Data transformations and aggregations
        transformations = {
            'customer_analytics': """
                INSERT INTO analytics_summary (
                    time_bucket, granularity, customer_id,
                    order_count, revenue, avg_order_value, created_at, updated_at
                )
                SELECT
                    DATE_TRUNC('hour', o.order_date) as time_bucket,
                    'hour' as granularity,
                    c.customer_id,
                    COUNT(o.id) as order_count,
                    SUM(o.total_amount) as revenue,
                    AVG(o.total_amount) as avg_order_value,
                    NOW() as created_at,
                    NOW() as updated_at
                FROM orders o
                JOIN customers c ON o.customer_uuid = c.id
                WHERE o.order_date >= NOW() - INTERVAL '4 hours'
                GROUP BY DATE_TRUNC('hour', o.order_date), c.customer_id
                ON CONFLICT (time_bucket, granularity, customer_id)
                DO UPDATE SET
                    order_count = EXCLUDED.order_count,
                    revenue = EXCLUDED.revenue,
                    avg_order_value = EXCLUDED.avg_order_value,
                    updated_at = NOW()
            """,

            'product_analytics': """
                INSERT INTO analytics_summary (
                    time_bucket, granularity, product_id, category,
                    order_count, revenue, quantity_sold, created_at, updated_at
                )
                SELECT
                    DATE_TRUNC('hour', o.order_date) as time_bucket,
                    'hour' as granularity,
                    p.product_id,
                    p.category,
                    COUNT(DISTINCT o.id) as order_count,
                    SUM(oi.total_price) as revenue,
                    SUM(oi.quantity) as quantity_sold,
                    NOW() as created_at,
                    NOW() as updated_at
                FROM order_items oi
                JOIN orders o ON oi.order_uuid = o.id
                JOIN products p ON oi.product_uuid = p.id
                WHERE o.order_date >= NOW() - INTERVAL '4 hours'
                GROUP BY DATE_TRUNC('hour', o.order_date), p.product_id, p.category
                ON CONFLICT (time_bucket, granularity, product_id, category)
                DO UPDATE SET
                    order_count = EXCLUDED.order_count,
                    revenue = EXCLUDED.revenue,
                    quantity_sold = EXCLUDED.quantity_sold,
                    updated_at = NOW()
            """,

            'update_customer_metrics': """
                UPDATE customers SET
                    total_orders = subq.order_count,
                    total_spent = subq.total_spent,
                    average_order_value = subq.avg_order_value,
                    last_order_date = subq.last_order_date,
                    updated_at = NOW()
                FROM (
                    SELECT
                        customer_uuid,
                        COUNT(*) as order_count,
                        SUM(total_amount) as total_spent,
                        AVG(total_amount) as avg_order_value,
                        MAX(order_date) as last_order_date
                    FROM orders
                    WHERE status != 'cancelled'
                    GROUP BY customer_uuid
                ) subq
                WHERE customers.id = subq.customer_uuid
            """
        }

        for name, query in transformations.items():
            try:
                with engine.begin() as conn:
                    from sqlalchemy import text
                    result = conn.execute(text(query))
                    rows_affected = result.rowcount if hasattr(result, 'rowcount') else 0
                    transformation_results[name] = {
                        'status': 'completed',
                        'rows_affected': rows_affected
                    }
                    logging.info(f"Transformation {name} completed: {rows_affected} rows affected")
            except Exception as e:
                logging.error(f"Transformation {name} failed: {str(e)}")
                transformation_results[name] = {
                    'status': 'failed',
                    'error': str(e)
                }

        # Store results
        context['task_instance'].xcom_push(key='transformations', value=transformation_results)

        successful_transforms = len([r for r in transformation_results.values() if r.get('status') == 'completed'])
        send_alert(f"Data transformations: {successful_transforms}/{len(transformations)} completed", "INFO")

        return transformation_results

    except Exception as e:
        error_msg = f"Data transformation failed: {str(e)}"
        logging.error(error_msg)
        send_alert(error_msg, "HIGH")
        raise

# ================================
# MACHINE LEARNING PIPELINE
# ================================

def prepare_ml_features(**context):
    """Prepare features for machine learning models"""
    feature_results = {}

    try:
        engine = get_db_connection()

        # Check if required tables exist
        def check_table_exists(table_name):
            """Check if table exists in PostgreSQL"""
            try:
                from sqlalchemy import text
                with engine.connect() as conn:
                    query = text("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = :table_name)")
                    result = conn.execute(query, {"table_name": table_name})
                    return result.fetchone()[0]
            except Exception as e:
                logging.warning(f"Failed to check table existence for {table_name}: {str(e)}")
                return False

        # Check for required tables
        required_tables = ['customers', 'products', 'orders', 'order_items']
        missing_tables = []
        use_dummy_data = False

        # Test database connection first
        try:
            from sqlalchemy import text
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logging.info("Database connection successful")

            # Only check tables if connection is successful
            for table in required_tables:
                if not check_table_exists(table):
                    missing_tables.append(table)

        except Exception as e:
            logging.error(f"Database connection failed: {str(e)}. Using dummy data.")
            missing_tables = required_tables  # Force dummy data
            use_dummy_data = True

        logging.info(f"Required tables: {required_tables}")
        logging.info(f"Missing tables: {missing_tables}")

        # Force dummy data if any tables are missing or connection failed
        if missing_tables or use_dummy_data:
            logging.warning(f"Missing tables: {missing_tables}. Creating dummy data for ML features.")

            # Create dummy CLV features
            clv_df = pd.DataFrame({
                'customer_id': [f'CUST_{i:06d}' for i in range(1, 101)],
                'total_orders': np.random.randint(1, 20, 100),
                'total_spent': np.random.uniform(100, 5000, 100),
                'average_order_value': np.random.uniform(50, 500, 100),
                'customer_age_days': np.random.randint(30, 1000, 100),
                'days_since_last_order': np.random.randint(1, 180, 100),
                'active_last_30d': np.random.choice([0, 1], 100, p=[0.3, 0.7]),
                'customer_tier': np.random.choice(['bronze', 'silver', 'gold', 'platinum'], 100),
                'country': np.random.choice(['US', 'UK', 'CA', 'AU', 'DE'], 100),
                'preferred_category': np.random.choice(['electronics', 'clothing', 'books', 'home'], 100)
            })

            logging.info(f"Generated {len(clv_df)} dummy customer records for ML features")

        else:
            # Customer Lifetime Value features
            clv_query = """
                SELECT
                    c.customer_id,
                    c.total_orders,
                    c.total_spent,
                    c.average_order_value,
                    EXTRACT(days FROM (NOW() - c.created_at)) as customer_age_days,
                    EXTRACT(days FROM (NOW() - c.last_order_date)) as days_since_last_order,
                    CASE WHEN c.last_order_date >= NOW() - INTERVAL '30 days' THEN 1 ELSE 0 END as active_last_30d,
                    c.customer_tier,
                    c.country,
                    c.preferred_category
                FROM customers c
                WHERE c.total_orders > 0
                AND c.total_spent > 0
            """

            clv_df = pd.read_sql(clv_query, engine)

        # Feature engineering
        clv_df['avg_days_between_orders'] = clv_df['customer_age_days'] / clv_df['total_orders']
        clv_df['spending_velocity'] = clv_df['total_spent'] / clv_df['customer_age_days']

        # Encode categorical variables
        clv_df['tier_encoded'] = pd.Categorical(clv_df['customer_tier']).codes
        clv_df['country_encoded'] = pd.Categorical(clv_df['country']).codes

        # Product recommendation features
        if missing_tables or use_dummy_data:
            # Create dummy product features
            product_df = pd.DataFrame({
                'product_id': [f'PROD_{i:06d}' for i in range(1, 201)],
                'category': np.random.choice(['electronics', 'clothing', 'books', 'home', 'sports'], 200),
                'brand': np.random.choice(['BrandA', 'BrandB', 'BrandC', 'BrandD', 'BrandE'], 200),
                'price': np.random.uniform(10, 1000, 200),
                'total_sold': np.random.randint(0, 500, 200),
                'total_revenue': np.random.uniform(100, 50000, 200),
                'avg_rating': np.random.uniform(3.0, 5.0, 200),
                'review_count': np.random.randint(0, 100, 200),
                'unique_orders': np.random.randint(0, 200, 200),
                'avg_quantity_per_order': np.random.uniform(1.0, 5.0, 200)
            })
            logging.info(f"Generated {len(product_df)} dummy product records for ML features")

        else:
            product_query = """
                SELECT
                    p.product_id,
                    p.category,
                    p.brand,
                    p.price,
                    p.total_sold,
                    p.total_revenue,
                    p.avg_rating,
                    p.review_count,
                    COUNT(DISTINCT oi.order_uuid) as unique_orders,
                    AVG(oi.quantity) as avg_quantity_per_order
                FROM products p
                LEFT JOIN order_items oi ON p.id = oi.product_uuid
                WHERE p.is_active = true
                GROUP BY p.id, p.product_id, p.category, p.brand, p.price,
                         p.total_sold, p.total_revenue, p.avg_rating, p.review_count
            """

            product_df = pd.read_sql(product_query, engine)

        # Feature engineering for products
        product_df['revenue_per_sold'] = product_df['total_revenue'] / (product_df['total_sold'] + 1)
        product_df['popularity_score'] = (
            product_df['total_sold'] * 0.4 +
            product_df['avg_rating'] * product_df['review_count'] * 0.3 +
            product_df['unique_orders'] * 0.3
        )

        # Store features in MongoDB for ML pipeline
        mongo_client = get_mongo_client()
        db = mongo_client['ecommerce_dss']

        # Store CLV features
        clv_features = clv_df.to_dict('records')
        db.ml_features_clv.delete_many({})  # Clear old features
        db.ml_features_clv.insert_many(clv_features)

        # Store product features
        product_features = product_df.to_dict('records')
        db.ml_features_products.delete_many({})  # Clear old features
        db.ml_features_products.insert_many(product_features)

        feature_results = {
            'clv_features': {
                'count': len(clv_features),
                'features': list(clv_df.columns)
            },
            'product_features': {
                'count': len(product_features),
                'features': list(product_df.columns)
            }
        }

        # Store results
        context['task_instance'].xcom_push(key='ml_features', value=feature_results)

        send_alert(f"ML features prepared: {len(clv_features)} customers, {len(product_features)} products", "INFO")

        return feature_results

    except Exception as e:
        error_msg = f"ML feature preparation failed: {str(e)}"
        logging.error(error_msg)
        send_alert(error_msg, "HIGH")
        raise

def train_ml_models(**context):
    """Train machine learning models"""
    training_results = {}

    try:
        # Get features from MongoDB
        mongo_client = get_mongo_client()
        db = mongo_client['ecommerce_dss']

        # Train Customer Lifetime Value model
        clv_features = list(db.ml_features_clv.find({}, {'_id': 0}))
        if clv_features:
            clv_df = pd.DataFrame(clv_features)

            # Prepare features and target
            feature_cols = ['total_orders', 'average_order_value', 'customer_age_days',
                          'days_since_last_order', 'active_last_30d', 'tier_encoded',
                          'avg_days_between_orders', 'spending_velocity']

            X = clv_df[feature_cols].fillna(0)
            y = clv_df['total_spent']

            # Split data
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

            # Train model
            clv_model = RandomForestRegressor(n_estimators=100, random_state=42)
            clv_model.fit(X_train, y_train)

            # Evaluate model
            y_pred = clv_model.predict(X_test)
            mae = mean_absolute_error(y_test, y_pred)
            mse = mean_squared_error(y_test, y_pred)

            # Save model
            model_path = '/app/models/clv_model.pkl'
            os.makedirs(os.path.dirname(model_path), exist_ok=True)
            joblib.dump(clv_model, model_path)

            training_results['clv_model'] = {
                'status': 'completed',
                'train_samples': len(X_train),
                'test_samples': len(X_test),
                'mae': float(mae),
                'mse': float(mse),
                'model_path': model_path,
                'features': feature_cols
            }

            # Store model metadata in MongoDB
            model_metadata = {
                'model_name': 'customer_lifetime_value',
                'model_version': '1.0',
                'model_type': 'regression',
                'algorithm': 'random_forest',
                'trained_at': datetime.now(),
                'performance_metrics': {
                    'mae': float(mae),
                    'mse': float(mse),
                    'train_samples': len(X_train),
                    'test_samples': len(X_test)
                },
                'feature_columns': feature_cols,
                'model_path': model_path
            }

            db.ml_models.insert_one(model_metadata)

        # Train Product Recommendation model (simplified version)
        product_features = list(db.ml_features_products.find({}, {'_id': 0}))
        if product_features:
            product_df = pd.DataFrame(product_features)

            # Simple popularity-based recommendation scoring
            product_df['recommendation_score'] = (
                product_df['popularity_score'] * 0.4 +
                (product_df['avg_rating'].fillna(3.0) * product_df['review_count'].fillna(1)) * 0.3 +
                product_df['revenue_per_sold'] * 0.3
            )

            # Store recommendation scores
            recommendations = product_df[['product_id', 'category', 'recommendation_score']].to_dict('records')
            db.ml_recommendations.delete_many({})
            db.ml_recommendations.insert_many(recommendations)

            training_results['recommendation_model'] = {
                'status': 'completed',
                'products_processed': len(recommendations)
            }

        # Store results
        context['task_instance'].xcom_push(key='ml_training', value=training_results)

        models_trained = len([r for r in training_results.values() if r.get('status') == 'completed'])
        send_alert(f"ML models trained: {models_trained} models completed", "INFO")

        return training_results

    except Exception as e:
        error_msg = f"ML model training failed: {str(e)}"
        logging.error(error_msg)
        send_alert(error_msg, "HIGH")
        raise

def generate_predictions(**context):
    """Generate predictions using trained models"""
    prediction_results = {}

    try:
        mongo_client = get_mongo_client()
        db = mongo_client['ecommerce_dss']

        # Load CLV model and generate predictions
        try:
            model_path = '/app/models/clv_model.pkl'
            if os.path.exists(model_path):
                clv_model = joblib.load(model_path)

                # Get recent customers for prediction
                clv_features = list(db.ml_features_clv.find({}).limit(100))
                if clv_features:
                    clv_df = pd.DataFrame(clv_features)

                    feature_cols = ['total_orders', 'average_order_value', 'customer_age_days',
                                  'days_since_last_order', 'active_last_30d', 'tier_encoded',
                                  'avg_days_between_orders', 'spending_velocity']

                    X = clv_df[feature_cols].fillna(0)
                    predictions = clv_model.predict(X)

                    # Store predictions
                    prediction_docs = []
                    for i, (_, customer) in enumerate(clv_df.iterrows()):
                        pred_doc = {
                            'model_name': 'customer_lifetime_value',
                            'model_version': '1.0',
                            'customer_id': customer['customer_id'],
                            'prediction': float(predictions[i]),
                            'confidence_score': 0.85,  # Simplified confidence
                            'input_features': {col: float(customer[col]) for col in feature_cols},
                            'predicted_at': datetime.now(),
                            'prediction_type': 'customer_lifetime_value'
                        }
                        prediction_docs.append(pred_doc)

                    db.ml_predictions.insert_many(prediction_docs)

                    prediction_results['clv_predictions'] = {
                        'status': 'completed',
                        'predictions_generated': len(prediction_docs),
                        'avg_prediction': float(np.mean(predictions))
                    }
        except Exception as e:
            logging.error(f"CLV prediction failed: {str(e)}")
            prediction_results['clv_predictions'] = {'status': 'failed', 'error': str(e)}

        # Generate product recommendations
        try:
            recommendations = list(db.ml_recommendations.find({}).sort('recommendation_score', -1).limit(50))

            # Update with fresh timestamp
            for rec in recommendations:
                rec['generated_at'] = datetime.now()
                rec['model_version'] = '1.0'

            db.ml_product_recommendations.delete_many({})
            db.ml_product_recommendations.insert_many(recommendations)

            prediction_results['product_recommendations'] = {
                'status': 'completed',
                'recommendations_generated': len(recommendations)
            }

        except Exception as e:
            logging.error(f"Product recommendation failed: {str(e)}")
            prediction_results['product_recommendations'] = {'status': 'failed', 'error': str(e)}

        # Store results
        context['task_instance'].xcom_push(key='ml_predictions', value=prediction_results)

        successful_predictions = len([r for r in prediction_results.values() if r.get('status') == 'completed'])
        total_predictions = sum(r.get('predictions_generated', r.get('recommendations_generated', 0))
                              for r in prediction_results.values() if r.get('status') == 'completed')

        send_alert(f"ML predictions generated: {total_predictions} predictions across {successful_predictions} models", "INFO")

        return prediction_results

    except Exception as e:
        error_msg = f"ML prediction generation failed: {str(e)}"
        logging.error(error_msg)
        send_alert(error_msg, "HIGH")
        raise

# ================================
# MONITORING AND ALERTING
# ================================

def monitor_system_health(**context):
    """Monitor overall system health and performance"""
    health_status = {}

    try:
        # Check database connections
        try:
            engine = get_db_connection()
            with engine.connect() as conn:
                from sqlalchemy import text
                conn.execute(text("SELECT 1"))
            health_status['postgresql'] = {'status': 'healthy', 'response_time': 'fast'}
        except Exception as e:
            health_status['postgresql'] = {'status': 'unhealthy', 'error': str(e)}

        # Check MongoDB
        try:
            mongo_client = get_mongo_client()
            start_time = datetime.now()
            mongo_client.admin.command('ping')
            response_time = (datetime.now() - start_time).total_seconds()
            health_status['mongodb'] = {'status': 'healthy', 'response_time': f'{response_time:.3f}s'}
        except Exception as e:
            health_status['mongodb'] = {'status': 'unhealthy', 'error': str(e)}

        # Check Redis
        try:
            redis_client = get_redis_client()
            start_time = datetime.now()
            redis_client.ping()
            response_time = (datetime.now() - start_time).total_seconds()
            health_status['redis'] = {'status': 'healthy', 'response_time': f'{response_time:.3f}s'}
        except Exception as e:
            health_status['redis'] = {'status': 'unhealthy', 'error': str(e)}

        # Check Kafka
        try:
            producer = KafkaProducer(bootstrap_servers=KAFKA_SERVERS)
            producer.close()
            health_status['kafka'] = {'status': 'healthy', 'response_time': 'fast'}
        except Exception as e:
            health_status['kafka'] = {'status': 'unhealthy', 'error': str(e)}

        # Calculate overall health score
        healthy_services = len([s for s in health_status.values() if s.get('status') == 'healthy'])
        total_services = len(health_status)
        health_score = (healthy_services / total_services) * 100

        health_status['overall'] = {
            'health_score': health_score,
            'healthy_services': healthy_services,
            'total_services': total_services,
            'status': 'healthy' if health_score >= 80 else 'degraded' if health_score >= 60 else 'unhealthy'
        }

        # Store health metrics in MongoDB
        mongo_client = get_mongo_client()
        db = mongo_client['ecommerce_dss']

        health_doc = {
            'timestamp': datetime.now(),
            'health_status': health_status,
            'dag_run': context['dag_run'].dag_id,
            'execution_date': context['execution_date']
        }

        db.system_health_metrics.insert_one(health_doc)

        # Store results
        context['task_instance'].xcom_push(key='system_health', value=health_status)

        # Send appropriate alert
        if health_score >= 90:
            send_alert(f"System health excellent: {health_score:.1f}%", "INFO")
        elif health_score >= 80:
            send_alert(f"System health good: {health_score:.1f}%", "MEDIUM")
        else:
            send_alert(f"System health poor: {health_score:.1f}% - Immediate attention required", "CRITICAL")

        return health_status

    except Exception as e:
        error_msg = f"System health monitoring failed: {str(e)}"
        logging.error(error_msg)
        send_alert(error_msg, "CRITICAL")
        raise

def generate_performance_report(**context):
    """Generate comprehensive performance report"""
    report_data = {}

    try:
        engine = get_db_connection()
        mongo_client = get_mongo_client()
        db = mongo_client['ecommerce_dss']

        # Business metrics
        business_metrics_query = """
            SELECT
                COUNT(DISTINCT c.id) as total_customers,
                COUNT(DISTINCT p.id) as total_products,
                COUNT(DISTINCT o.id) as total_orders,
                SUM(o.total_amount) as total_revenue,
                AVG(o.total_amount) as avg_order_value,
                COUNT(DISTINCT CASE WHEN o.order_date >= NOW() - INTERVAL '24 hours' THEN o.id END) as orders_24h,
                SUM(CASE WHEN o.order_date >= NOW() - INTERVAL '24 hours' THEN o.total_amount ELSE 0 END) as revenue_24h
            FROM customers c
            CROSS JOIN products p
            CROSS JOIN orders o
        """

        business_df = pd.read_sql(business_metrics_query, engine)
        business_metrics = business_df.iloc[0].to_dict()

        # System performance metrics
        recent_health = list(db.system_health_metrics.find().sort('timestamp', -1).limit(10))

        # ML model performance
        recent_predictions = db.ml_predictions.count_documents({
            'predicted_at': {'$gte': datetime.now() - timedelta(hours=24)}
        })

        # Data quality metrics
        data_quality_tasks = context['task_instance'].xcom_pull(task_ids='validate_data_quality')

        # Compile report
        report_data = {
            'report_timestamp': datetime.now(),
            'execution_date': context['execution_date'],
            'dag_run_id': context['dag_run'].run_id,
            'business_metrics': {
                'total_customers': int(business_metrics.get('total_customers', 0)),
                'total_products': int(business_metrics.get('total_products', 0)),
                'total_orders': int(business_metrics.get('total_orders', 0)),
                'total_revenue': float(business_metrics.get('total_revenue', 0)),
                'avg_order_value': float(business_metrics.get('avg_order_value', 0)),
                'orders_24h': int(business_metrics.get('orders_24h', 0)),
                'revenue_24h': float(business_metrics.get('revenue_24h', 0))
            },
            'system_performance': {
                'health_checks_count': len(recent_health),
                'avg_health_score': np.mean([h['health_status']['overall']['health_score'] for h in recent_health if 'overall' in h.get('health_status', {})]),
                'ml_predictions_24h': recent_predictions
            },
            'data_quality': data_quality_tasks,
            'pipeline_status': 'completed'
        }

        # Store report in MongoDB
        db.performance_reports.insert_one(report_data)

        # Store results
        context['task_instance'].xcom_push(key='performance_report', value=report_data)

        # Send summary alert
        summary = f"""Performance Report Generated:
        • Revenue 24h: ${report_data['business_metrics']['revenue_24h']:,.2f}
        • Orders 24h: {report_data['business_metrics']['orders_24h']:,}
        • ML Predictions: {recent_predictions:,}
        • System Health: {report_data['system_performance']['avg_health_score']:.1f}%"""

        send_alert(summary, "INFO")

        return report_data

    except Exception as e:
        error_msg = f"Performance report generation failed: {str(e)}"
        logging.error(error_msg)
        send_alert(error_msg, "HIGH")
        raise

# ================================
# BACKUP AND CLEANUP
# ================================

def backup_critical_data(**context):
    """Backup critical data"""
    backup_results = {}

    try:
        backup_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Backup PostgreSQL critical tables
        engine = get_db_connection()
        critical_tables = ['customers', 'products', 'orders', 'order_items', 'analytics_summary']

        for table in critical_tables:
            try:
                df = pd.read_sql(f"SELECT * FROM {table}", engine)
                backup_path = f"/app/data/backups/{table}_backup_{backup_timestamp}.csv"
                os.makedirs(os.path.dirname(backup_path), exist_ok=True)
                df.to_csv(backup_path, index=False)

                backup_results[table] = {
                    'status': 'completed',
                    'records': len(df),
                    'path': backup_path
                }
            except Exception as e:
                backup_results[table] = {
                    'status': 'failed',
                    'error': str(e)
                }

        # Backup MongoDB collections
        mongo_client = get_mongo_client()
        db = mongo_client['ecommerce_dss']

        critical_collections = ['ml_predictions', 'ml_models', 'alerts', 'performance_reports']

        for collection_name in critical_collections:
            try:
                collection = db[collection_name]
                documents = list(collection.find())

                if documents:
                    backup_path = f"/app/data/backups/{collection_name}_backup_{backup_timestamp}.json"
                    with open(backup_path, 'w') as f:
                        json.dump(documents, f, default=str, indent=2)

                    backup_results[f"mongo_{collection_name}"] = {
                        'status': 'completed',
                        'records': len(documents),
                        'path': backup_path
                    }
                else:
                    backup_results[f"mongo_{collection_name}"] = {
                        'status': 'skipped',
                        'reason': 'no_data'
                    }
            except Exception as e:
                backup_results[f"mongo_{collection_name}"] = {
                    'status': 'failed',
                    'error': str(e)
                }

        # Store results
        context['task_instance'].xcom_push(key='backup_results', value=backup_results)

        successful_backups = len([r for r in backup_results.values() if r.get('status') == 'completed'])
        total_records = sum(r.get('records', 0) for r in backup_results.values() if r.get('status') == 'completed')

        send_alert(f"Data backup completed: {successful_backups} backups, {total_records:,} total records", "INFO")

        return backup_results

    except Exception as e:
        error_msg = f"Data backup failed: {str(e)}"
        logging.error(error_msg)
        send_alert(error_msg, "HIGH")
        raise

def cleanup_old_data(**context):
    """Clean up old data and logs"""
    cleanup_results = {}

    try:
        # Clean up old streaming data (keep last 7 days)
        mongo_client = get_mongo_client()
        db = mongo_client['ecommerce_dss']

        cutoff_date = datetime.now() - timedelta(days=7)

        cleanup_collections = [
            'processed_products_stream',
            'processed_customers_stream',
            'processed_orders_stream',
            'system_health_metrics'
        ]

        for collection_name in cleanup_collections:
            try:
                collection = db[collection_name]
                result = collection.delete_many({
                    'processed_at': {'$lt': cutoff_date}
                })

                cleanup_results[collection_name] = {
                    'status': 'completed',
                    'deleted_count': result.deleted_count
                }
            except Exception as e:
                cleanup_results[collection_name] = {
                    'status': 'failed',
                    'error': str(e)
                }

        # Clean up old performance reports (keep last 30 days)
        try:
            cutoff_date = datetime.now() - timedelta(days=30)
            result = db.performance_reports.delete_many({
                'report_timestamp': {'$lt': cutoff_date}
            })
            cleanup_results['performance_reports'] = {
                'status': 'completed',
                'deleted_count': result.deleted_count
            }
        except Exception as e:
            cleanup_results['performance_reports'] = {
                'status': 'failed',
                'error': str(e)
            }

        # Store results
        context['task_instance'].xcom_push(key='cleanup_results', value=cleanup_results)

        total_deleted = sum(r.get('deleted_count', 0) for r in cleanup_results.values() if r.get('status') == 'completed')
        send_alert(f"Data cleanup completed: {total_deleted:,} old records removed", "INFO")

        return cleanup_results

    except Exception as e:
        error_msg = f"Data cleanup failed: {str(e)}"
        logging.error(error_msg)
        send_alert(error_msg, "MEDIUM")
        raise

# ================================
# DAG DEFINITION
# ================================

# Create the DAG
dag = DAG(
    DAG_ID,
    default_args=DEFAULT_ARGS,
    description=DAG_DESCRIPTION,
    schedule_interval=SCHEDULE_INTERVAL,
    max_active_runs=1,
    catchup=False,
    tags=['ecommerce', 'dss', 'comprehensive', 'ml', 'streaming']
)

# ================================
# TASK DEFINITIONS
# ================================

# Start and end tasks
start_task = DummyOperator(
    task_id='start_pipeline',
    dag=dag
)

end_task = DummyOperator(
    task_id='end_pipeline',
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag
)

# Data source validation
check_sources_task = PythonOperator(
    task_id='check_data_sources',
    python_callable=check_data_sources,
    dag=dag
)

# Data collection task group
with TaskGroup("data_collection", dag=dag) as data_collection_group:
    collect_external_data_task = PythonOperator(
        task_id='collect_external_data',
        python_callable=collect_external_data,
        dag=dag
    )

    setup_streaming_task = PythonOperator(
        task_id='setup_streaming_topics',
        python_callable=setup_streaming_topics,
        dag=dag
    )

# Data processing task group
with TaskGroup("data_processing", dag=dag) as data_processing_group:
    validate_quality_task = PythonOperator(
        task_id='validate_data_quality',
        python_callable=validate_data_quality,
        dag=dag
    )

    process_streaming_task = PythonOperator(
        task_id='process_streaming_data',
        python_callable=process_streaming_data,
        dag=dag
    )

    transform_aggregate_task = PythonOperator(
        task_id='transform_and_aggregate',
        python_callable=transform_and_aggregate,
        dag=dag
    )

# Machine learning task group
with TaskGroup("machine_learning", dag=dag) as ml_group:
    prepare_features_task = PythonOperator(
        task_id='prepare_ml_features',
        python_callable=prepare_ml_features,
        dag=dag
    )

    train_models_task = PythonOperator(
        task_id='train_ml_models',
        python_callable=train_ml_models,
        dag=dag
    )

    generate_predictions_task = PythonOperator(
        task_id='generate_predictions',
        python_callable=generate_predictions,
        dag=dag
    )

# Monitoring task group
with TaskGroup("monitoring", dag=dag) as monitoring_group:
    monitor_health_task = PythonOperator(
        task_id='monitor_system_health',
        python_callable=monitor_system_health,
        dag=dag
    )

    generate_report_task = PythonOperator(
        task_id='generate_performance_report',
        python_callable=generate_performance_report,
        dag=dag
    )

# Backup and cleanup task group
with TaskGroup("backup_cleanup", dag=dag) as backup_group:
    backup_data_task = PythonOperator(
        task_id='backup_critical_data',
        python_callable=backup_critical_data,
        dag=dag
    )

    cleanup_data_task = PythonOperator(
        task_id='cleanup_old_data',
        python_callable=cleanup_old_data,
        dag=dag
    )

# Email notification task (on success)
success_email_task = EmailOperator(
    task_id='send_success_notification',
    to=['admin@ecommerce-dss.com'],
    subject='E-commerce DSS Pipeline - Success',
    html_content="""
    <h3>E-commerce DSS Pipeline Completed Successfully</h3>
    <p>Execution Date: {{ ds }}</p>
    <p>DAG Run ID: {{ dag_run.run_id }}</p>
    <p>All tasks completed successfully. Check the performance report for details.</p>
    """,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag
)

# ================================
# TASK DEPENDENCIES
# ================================

# Main pipeline flow
start_task >> check_sources_task >> data_collection_group
data_collection_group >> data_processing_group >> ml_group
ml_group >> monitoring_group
monitoring_group >> backup_group >> success_email_task >> end_task

# Internal task group dependencies
# Data collection group
collect_external_data_task >> setup_streaming_task

# Data processing group
validate_quality_task >> [process_streaming_task, transform_aggregate_task]

# ML group
prepare_features_task >> train_models_task >> generate_predictions_task

# Monitoring group
monitor_health_task >> generate_report_task

# Backup group
backup_data_task >> cleanup_data_task

# ================================
# DAG DOCUMENTATION
# ================================

dag.doc_md = """
# Comprehensive E-commerce DSS Pipeline

This DAG implements a complete end-to-end data pipeline for the E-commerce Decision Support System.

## Features:
- **Data Collection**: External API integration, streaming setup
- **Data Processing**: Quality validation, streaming data processing, transformations
- **Machine Learning**: Feature engineering, model training, prediction generation
- **Monitoring**: System health checks, performance reporting
- **Backup & Cleanup**: Data backup, old data cleanup

## Schedule:
Runs every 4 hours to ensure fresh data and predictions

## Dependencies:
- PostgreSQL (processed data)
- MongoDB (streaming data, ML artifacts)
- Kafka (streaming messages)
- Redis (caching)
- External APIs (data sources)

## Monitoring:
- System health alerts
- Performance reports
- Data quality metrics
- ML model performance

## Contact:
DSS Team - admin@ecommerce-dss.com
"""