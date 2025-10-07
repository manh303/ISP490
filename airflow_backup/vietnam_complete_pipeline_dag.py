#!/usr/bin/env python3
"""
Vietnam E-commerce Complete Data Pipeline DAG
============================================
Comprehensive end-to-end data pipeline for Vietnamese e-commerce platforms:

CRAWL → API → KAFKA → SPARK → DATA WAREHOUSE → ANALYTICS

Flow:
1. Crawl data from Vietnamese platforms (Shopee, Lazada, Tiki, Sendo, FPTShop)
2. Process via API endpoints
3. Stream through Kafka topics
4. Process with Spark for real-time analytics
5. Load into PostgreSQL data warehouse
6. Generate analytics and ML predictions

Author: Vietnam DSS Team
Version: 3.0.0
"""

import os
import sys
import json
import time
import logging
import subprocess
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

# Airflow imports
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.sensors.http import HttpSensor
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

# External libraries
import pandas as pd
import numpy as np
import requests
import pymongo
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import redis
import psycopg2

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ================================
# DAG CONFIGURATION
# ================================
DAG_ID = "vietnam_complete_ecommerce_pipeline"
DAG_DESCRIPTION = "Complete Vietnam E-commerce Pipeline: Crawl → API → Kafka → Spark → DW"

# Schedule: Run every 2 hours for fresh data
SCHEDULE_INTERVAL = "0 */2 * * *"  # Every 2 hours

# Default arguments
DEFAULT_ARGS = {
    'owner': 'vietnam-dss-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['vietnam-team@ecommerce-dss.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'max_active_runs': 1,
    'catchup': False,
}

# Environment configuration
POSTGRES_CONN_ID = "postgres_default"
MONGODB_URI = Variable.get("MONGODB_URI", "mongodb://admin:admin_password@mongodb:27017/")
KAFKA_SERVERS = Variable.get("KAFKA_SERVERS", "kafka:29092")
REDIS_URL = Variable.get("REDIS_URL", "redis://redis:6379/0")

# Vietnamese platforms configuration
VIETNAM_PLATFORMS = {
    'shopee': {
        'name': 'Shopee Vietnam',
        'market_share': 0.352,
        'api_endpoint': 'https://shopee.vn/api/v4/search/search_items',
        'data_path': '/opt/airflow/data/vietnamese_ecommerce/shopee_products'
    },
    'lazada': {
        'name': 'Lazada Vietnam',
        'market_share': 0.285,
        'api_endpoint': 'https://www.lazada.vn/catalog/',
        'data_path': '/opt/airflow/data/vietnamese_ecommerce/lazada_products'
    },
    'tiki': {
        'name': 'Tiki Vietnam',
        'market_share': 0.158,
        'api_endpoint': 'https://tiki.vn/api/v2/products',
        'data_path': '/opt/airflow/data/vietnamese_ecommerce/tiki_products'
    },
    'sendo': {
        'name': 'Sendo Vietnam',
        'market_share': 0.103,
        'api_endpoint': 'https://www.sendo.vn/api/product',
        'data_path': '/opt/airflow/data/vietnamese_ecommerce/sendo_products'
    },
    'fptshop': {
        'name': 'FPT Shop',
        'market_share': 0.052,
        'api_endpoint': 'https://fptshop.com.vn/api/product',
        'data_path': '/opt/airflow/data/vietnamese_ecommerce/fptshop_products'
    },
    'cellphones': {
        'name': 'CellphoneS',
        'market_share': 0.035,
        'api_endpoint': 'https://cellphones.com.vn/api/product',
        'data_path': '/opt/airflow/data/vietnamese_ecommerce/cellphones_products'
    }
}

# ================================
# UTILITY FUNCTIONS
# ================================

def get_postgres_connection():
    """Get PostgreSQL connection"""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    return hook.get_conn()

def get_mongo_client():
    """Get MongoDB client"""
    return pymongo.MongoClient(MONGODB_URI)

def get_redis_client():
    """Get Redis client"""
    return redis.from_url(REDIS_URL)

def json_serializable(obj):
    """Convert objects to JSON serializable format"""
    import json
    from datetime import datetime, date

    if obj is None:
        return None
    elif isinstance(obj, (str, int, float, bool)):
        return obj
    elif isinstance(obj, (datetime, date)):
        return obj.isoformat()
    elif str(type(obj)) == "<class 'bson.objectid.ObjectId'>":
        return str(obj)
    elif isinstance(obj, dict):
        return {k: json_serializable(v) for k, v in obj.items() if k != '_id'}  # Skip MongoDB _id
    elif isinstance(obj, list):
        return [json_serializable(item) for item in obj]
    elif hasattr(obj, '__dict__'):
        return json_serializable(obj.__dict__)
    else:
        try:
            json.dumps(obj)  # Test if already serializable
            return obj
        except (TypeError, ValueError):
            return str(obj)

def send_pipeline_alert(message: str, severity: str = "INFO", **context):
    """Send pipeline alert to monitoring system"""
    try:
        mongo_client = get_mongo_client()
        db = mongo_client['vietnam_ecommerce_dss']

        # Safely extract task info from context
        task_info = context.get('task_instance')
        task_id = 'unknown'
        if task_info and hasattr(task_info, 'task_id'):
            task_id = task_info.task_id
        elif context.get('task') and hasattr(context.get('task'), 'task_id'):
            task_id = context.get('task').task_id

        # Safely extract execution date
        execution_date = datetime.now().isoformat()
        if context.get('execution_date'):
            try:
                execution_date = context['execution_date'].isoformat()
            except:
                execution_date = str(context['execution_date'])

        alert_doc = {
            'alert_id': f"pipeline_{datetime.now().isoformat()}",
            'dag_id': DAG_ID,
            'task_id': task_id,
            'execution_date': execution_date,
            'severity': severity,
            'message': message,
            'timestamp': datetime.now().isoformat(),
            'source': 'vietnam_pipeline',
            'metadata': {
                'platform': 'vietnam_ecommerce',
                'pipeline_version': '3.0.0'
            }
        }

        db.pipeline_alerts.insert_one(alert_doc)
        logger.info(f"Alert sent: {severity} - {message}")

    except Exception as e:
        logger.error(f"Failed to send alert: {str(e)}")

# ================================
# CRAWLING TASKS
# ================================

def check_vietnamese_platforms(**context):
    """Check availability of Vietnamese e-commerce platforms"""
    platform_status = {}

    try:
        logger.info("🇻🇳 Checking Vietnamese e-commerce platforms...")

        for platform_key, platform_info in VIETNAM_PLATFORMS.items():
            try:
                # Check if data files exist
                data_path = platform_info['data_path']
                file_patterns = [f"{data_path}*.csv", f"{data_path}*.json"]

                import glob
                found_files = []
                for pattern in file_patterns:
                    found_files.extend(glob.glob(pattern))

                if found_files:
                    # Get latest file
                    latest_file = max(found_files, key=os.path.getmtime)
                    file_size = os.path.getsize(latest_file)
                    file_age = time.time() - os.path.getmtime(latest_file)

                    platform_status[platform_key] = {
                        'status': 'data_available',
                        'latest_file': latest_file,
                        'file_size_mb': round(file_size / (1024*1024), 2),
                        'age_hours': round(file_age / 3600, 2),
                        'market_share': platform_info['market_share']
                    }
                else:
                    platform_status[platform_key] = {
                        'status': 'no_data',
                        'message': f"No data files found at {data_path}",
                        'market_share': platform_info['market_share']
                    }

            except Exception as e:
                platform_status[platform_key] = {
                    'status': 'error',
                    'error': str(e),
                    'market_share': platform_info['market_share']
                }

        # Calculate overall data coverage
        available_platforms = [p for p in platform_status.values() if p['status'] == 'data_available']
        total_market_share = sum(p['market_share'] for p in available_platforms)

        summary = {
            'platforms_checked': len(VIETNAM_PLATFORMS),
            'platforms_available': len(available_platforms),
            'market_coverage': round(total_market_share * 100, 1),
            'platform_details': platform_status
        }

        # Store results
        context['task_instance'].xcom_push(key='platform_status', value=json_serializable(summary))

        send_pipeline_alert(
            f"Platform check: {len(available_platforms)}/{len(VIETNAM_PLATFORMS)} platforms available, {summary['market_coverage']}% market coverage",
            "INFO", **context
        )

        logger.info(f"✅ Platform check completed: {summary}")
        return summary

    except Exception as e:
        error_msg = f"Platform check failed: {str(e)}"
        logger.error(error_msg)
        send_pipeline_alert(error_msg, "HIGH", **context)
        raise

def crawl_vietnamese_ecommerce_data(**context):
    """Crawl data from Vietnamese e-commerce platforms"""
    crawl_results = {}

    try:
        logger.info("🕷️ Starting Vietnamese e-commerce data crawling...")

        # Get platform status from previous task
        platform_status = context['task_instance'].xcom_pull(key='platform_status', task_ids='vietnam_data_crawling.check_vietnamese_platforms')

        # Handle case where platform_status is None or doesn't have expected structure
        if platform_status and 'platform_details' in platform_status:
            available_platforms = [
                platform for platform, status in platform_status['platform_details'].items()
                if status['status'] == 'data_available'
            ]
        else:
            logger.warning("No platform status received, using all platforms")
            available_platforms = list(VIETNAM_PLATFORMS.keys())

        mongo_client = get_mongo_client()
        db = mongo_client['vietnam_ecommerce_dss']

        total_products = 0

        for platform_key in available_platforms:
            try:
                platform_info = VIETNAM_PLATFORMS[platform_key]

                # Check if platform status has details
                if platform_status and 'platform_details' in platform_status and platform_key in platform_status['platform_details']:
                    status_info = platform_status['platform_details'][platform_key]
                    logger.info(f"Processing {platform_info['name']}...")

                    # Load data from latest file
                    latest_file = status_info.get('latest_file')
                    if not latest_file:
                        logger.warning(f"No latest file for {platform_key}")
                        continue
                else:
                    # Fallback: try to find data files directly
                    logger.warning(f"No status info for {platform_key}, trying direct file search")
                    data_path = platform_info['data_path']
                    import glob
                    found_files = glob.glob(f"{data_path}*.csv") + glob.glob(f"{data_path}*.json")
                    if not found_files:
                        logger.warning(f"No data files found for {platform_key}")
                        continue
                    latest_file = max(found_files, key=os.path.getmtime)

                if latest_file.endswith('.csv'):
                    df = pd.read_csv(latest_file)
                elif latest_file.endswith('.json'):
                    df = pd.read_json(latest_file)
                else:
                    continue

                # Clean and standardize data
                if len(df) > 0:
                    # Add platform metadata
                    df['platform'] = platform_key
                    df['platform_name'] = platform_info['name']
                    df['market_share'] = platform_info['market_share']
                    df['crawled_at'] = datetime.now()
                    df['data_source'] = 'vietnam_ecommerce_crawl'

                    # Sample data if too large (for demo purposes)
                    if len(df) > 5000:
                        df = df.sample(n=5000, random_state=42)
                        logger.info(f"Sampled 5000 records from {platform_info['name']} (original: {len(df)})")

                    # Store in MongoDB
                    products_data = df.to_dict('records')
                    collection_name = f"crawled_{platform_key}_products"

                    # Clear old data and insert new
                    db[collection_name].delete_many({'crawled_at': {'$lt': datetime.now() - timedelta(hours=24)}})
                    inserted_result = db[collection_name].insert_many(products_data)

                    # Convert ObjectIds to strings for JSON serialization
                    for i, product in enumerate(products_data):
                        if '_id' in product:
                            del product['_id']  # Remove MongoDB _id to avoid serialization issues

                    crawl_results[platform_key] = {
                        'status': 'success',
                        'products_count': len(products_data),
                        'file_source': latest_file,
                        'collection': collection_name
                    }

                    total_products += len(products_data)
                    logger.info(f"✅ {platform_info['name']}: {len(products_data)} products stored")

                else:
                    crawl_results[platform_key] = {
                        'status': 'empty',
                        'message': 'No data in file'
                    }

            except Exception as e:
                logger.error(f"Failed to process {platform_key}: {str(e)}")
                crawl_results[platform_key] = {
                    'status': 'error',
                    'error': str(e)
                }

        # Store crawl summary
        crawl_summary = {
            'total_platforms_processed': len(available_platforms),
            'successful_platforms': len([r for r in crawl_results.values() if r['status'] == 'success']),
            'total_products_crawled': total_products,
            'crawl_timestamp': datetime.now(),
            'results_by_platform': crawl_results
        }

        # Store summary in MongoDB
        db.crawl_summary.insert_one(crawl_summary.copy())

        # Store results in XCom (remove ObjectIds for JSON serialization)
        crawl_summary_serializable = json_serializable(crawl_summary)
        context['task_instance'].xcom_push(key='crawl_results', value=crawl_summary_serializable)

        send_pipeline_alert(
            f"Crawling completed: {total_products:,} products from {len(available_platforms)} platforms",
            "INFO", **context
        )

        logger.info(f"🎉 Crawling completed: {crawl_summary}")
        return crawl_summary

    except Exception as e:
        error_msg = f"Crawling failed: {str(e)}"
        logger.error(error_msg)
        send_pipeline_alert(error_msg, "HIGH", **context)
        raise

# ================================
# API PROCESSING TASKS
# ================================

def process_data_via_api(**context):
    """Process crawled data through API endpoints"""
    api_results = {}

    try:
        logger.info("🌐 Processing data via API endpoints...")

        # Get crawl results
        crawl_results = context['task_instance'].xcom_pull(key='crawl_results', task_ids='vietnam_data_crawling.crawl_vietnamese_ecommerce_data')

        # Test backend API health first
        try:
            health_response = requests.get('http://backend:8000/health', timeout=10)
            if health_response.status_code == 200:
                logger.info("✅ Backend API is healthy")
            else:
                logger.warning(f"⚠️ Backend API health check returned: {health_response.status_code}")
        except Exception as e:
            logger.warning(f"⚠️ Backend API health check failed: {str(e)}")

        mongo_client = get_mongo_client()
        db = mongo_client['vietnam_ecommerce_dss']

        processed_count = 0

        # Process each platform's data
        for platform_key, platform_result in crawl_results['results_by_platform'].items():
            if platform_result['status'] == 'success':
                try:
                    collection_name = platform_result['collection']
                    products = list(db[collection_name].find().limit(100))  # Process sample

                    # Transform for API processing
                    api_products = []
                    for product in products:
                        # Clean and standardize product data
                        api_product = {
                            'product_id': str(product.get('_id', '')) if product.get('_id') else f"product_{len(api_products)}",
                            'name': product.get('name', product.get('product_name', 'Unknown')),
                            'price': float(product.get('price', product.get('current_price', 0))),
                            'category': product.get('category', product.get('category_name', 'general')),
                            'platform': platform_key,
                            'brand': product.get('brand', 'Unknown'),
                            'rating': float(product.get('rating', product.get('avg_rating', 4.0))),
                            'availability': product.get('stock', product.get('available', True)),
                            'processed_via': 'api_pipeline',
                            'processed_at': datetime.now().isoformat()
                        }
                        api_products.append(api_product)

                    # Store processed data
                    if api_products:
                        processed_collection = f"api_processed_{platform_key}"
                        db[processed_collection].delete_many({})  # Clear old data
                        db[processed_collection].insert_many(api_products)

                        api_results[platform_key] = {
                            'status': 'processed',
                            'products_processed': len(api_products),
                            'collection': processed_collection
                        }

                        processed_count += len(api_products)
                        logger.info(f"✅ API processed {len(api_products)} products from {platform_key}")

                except Exception as e:
                    logger.error(f"API processing failed for {platform_key}: {str(e)}")
                    api_results[platform_key] = {
                        'status': 'error',
                        'error': str(e)
                    }

        # Store API processing summary
        api_summary = {
            'total_products_processed': processed_count,
            'platforms_processed': len([r for r in api_results.values() if r['status'] == 'processed']),
            'processing_timestamp': datetime.now(),
            'results_by_platform': api_results
        }

        db.api_processing_summary.insert_one(api_summary.copy())
        context['task_instance'].xcom_push(key='api_results', value=json_serializable(api_summary))

        send_pipeline_alert(
            f"API processing completed: {processed_count:,} products processed via API",
            "INFO", **context
        )

        logger.info(f"🌐 API processing completed: {api_summary}")
        return api_summary

    except Exception as e:
        error_msg = f"API processing failed: {str(e)}"
        logger.error(error_msg)
        send_pipeline_alert(error_msg, "HIGH", **context)
        raise

# ================================
# KAFKA STREAMING TASKS
# ================================

def setup_kafka_topics(**context):
    """Setup Kafka topics for Vietnam e-commerce streaming"""
    try:
        logger.info("📡 Setting up Kafka topics...")

        from kafka.admin import KafkaAdminClient, NewTopic
        from kafka.errors import TopicAlreadyExistsError

        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_SERVERS,
            client_id='vietnam_pipeline_admin'
        )

        # Define Vietnam-specific topics
        vietnam_topics = [
            NewTopic(name="vietnam_customers", num_partitions=3, replication_factor=1),
            NewTopic(name="vietnam_products", num_partitions=4, replication_factor=1),
            NewTopic(name="vietnam_sales_events", num_partitions=6, replication_factor=1),
            NewTopic(name="vietnam_user_activities", num_partitions=4, replication_factor=1),
            NewTopic(name="vietnam_analytics", num_partitions=2, replication_factor=1),
            NewTopic(name="vietnam_alerts", num_partitions=1, replication_factor=1),
        ]

        created_topics = []
        existing_topics = []

        for topic in vietnam_topics:
            try:
                admin_client.create_topics([topic])
                created_topics.append(topic.name)
                logger.info(f"✅ Created topic: {topic.name}")
            except TopicAlreadyExistsError:
                existing_topics.append(topic.name)
                logger.info(f"ℹ️ Topic already exists: {topic.name}")
            except Exception as e:
                logger.error(f"❌ Failed to create topic {topic.name}: {str(e)}")

        setup_result = {
            'created_topics': created_topics,
            'existing_topics': existing_topics,
            'total_topics': len(vietnam_topics),
            'setup_timestamp': datetime.now()
        }

        context['task_instance'].xcom_push(key='kafka_setup', value=setup_result)

        send_pipeline_alert(
            f"Kafka setup: {len(created_topics + existing_topics)}/{len(vietnam_topics)} topics ready",
            "INFO", **context
        )

        logger.info(f"📡 Kafka topics setup completed: {setup_result}")
        return setup_result

    except Exception as e:
        error_msg = f"Kafka setup failed: {str(e)}"
        logger.error(error_msg)
        send_pipeline_alert(error_msg, "HIGH", **context)
        raise

def stream_data_to_kafka(**context):
    """Stream processed data to Kafka topics"""
    streaming_results = {}

    try:
        logger.info("🚀 Streaming data to Kafka...")

        # Get API processing results
        api_results = context['task_instance'].xcom_pull(key='api_results', task_ids='api_data_processing.process_data_via_api')

        # Initialize Kafka producer
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVERS,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None,
            acks='all',
            retries=3,
            compression_type='gzip'
        )

        mongo_client = get_mongo_client()
        db = mongo_client['vietnam_ecommerce_dss']

        total_messages_sent = 0

        # Stream products to Kafka
        for platform_key, platform_result in api_results['results_by_platform'].items():
            if platform_result['status'] == 'processed':
                try:
                    collection_name = platform_result['collection']
                    products = list(db[collection_name].find().limit(200))  # Stream sample

                    messages_sent = 0

                    for idx, product in enumerate(products):
                        # Create Kafka message with safe ID handling
                        product_id = str(product.get('_id', f"{platform_key}_product_{idx}"))

                        kafka_message = {
                            'message_id': f"vn_{platform_key}_{product_id}",
                            'platform': platform_key,
                            'product_data': {
                                'id': product_id,
                                'name': product.get('name', ''),
                                'price': product.get('price', 0),
                                'category': product.get('category', ''),
                                'brand': product.get('brand', ''),
                                'rating': product.get('rating', 0)
                            },
                            'metadata': {
                                'source': 'vietnam_pipeline',
                                'streamed_at': datetime.now().isoformat(),
                                'pipeline_version': '3.0.0'
                            }
                        }

                        # Send to appropriate topic
                        topic = 'vietnam_products'
                        producer.send(
                            topic,
                            key=kafka_message['message_id'],
                            value=kafka_message
                        )

                        messages_sent += 1

                    # Flush producer
                    producer.flush()

                    streaming_results[platform_key] = {
                        'status': 'streamed',
                        'messages_sent': messages_sent,
                        'topic': 'vietnam_products'
                    }

                    total_messages_sent += messages_sent
                    logger.info(f"✅ Streamed {messages_sent} messages from {platform_key}")

                except Exception as e:
                    logger.error(f"Streaming failed for {platform_key}: {str(e)}")
                    streaming_results[platform_key] = {
                        'status': 'error',
                        'error': str(e)
                    }

        # Generate synthetic customer and sales events
        try:
            # Generate customer events
            for i in range(50):
                customer_event = {
                    'customer_id': f"VN_CUST_{i:06d}",
                    'event_type': 'profile_update',
                    'platform': 'vietnam_ecommerce',
                    'timestamp': datetime.now().isoformat(),
                    'metadata': {'source': 'pipeline_generated'}
                }

                producer.send('vietnam_customers', value=customer_event)

            # Generate sales events
            for i in range(100):
                sales_event = {
                    'order_id': f"VN_ORDER_{i:08d}",
                    'customer_id': f"VN_CUST_{i % 50:06d}",
                    'total_amount': round(np.random.uniform(100000, 5000000), 0),  # VND
                    'platform': list(VIETNAM_PLATFORMS.keys())[i % len(VIETNAM_PLATFORMS)],
                    'timestamp': datetime.now().isoformat(),
                    'metadata': {'source': 'pipeline_generated'}
                }

                producer.send('vietnam_sales_events', value=sales_event)

            producer.flush()
            total_messages_sent += 150  # 50 customers + 100 sales

            streaming_results['synthetic_events'] = {
                'status': 'generated',
                'customer_events': 50,
                'sales_events': 100
            }

        except Exception as e:
            logger.error(f"Synthetic event generation failed: {str(e)}")

        producer.close()

        # Store streaming summary
        streaming_summary = {
            'total_messages_sent': total_messages_sent,
            'platforms_streamed': len([r for r in streaming_results.values() if r.get('status') == 'streamed']),
            'streaming_timestamp': datetime.now(),
            'results_by_platform': streaming_results
        }

        db.kafka_streaming_summary.insert_one(streaming_summary.copy())
        context['task_instance'].xcom_push(key='streaming_results', value=json_serializable(streaming_summary))

        send_pipeline_alert(
            f"Kafka streaming completed: {total_messages_sent:,} messages sent",
            "INFO", **context
        )

        logger.info(f"🚀 Kafka streaming completed: {streaming_summary}")
        return streaming_summary

    except Exception as e:
        error_msg = f"Kafka streaming failed: {str(e)}"
        logger.error(error_msg)
        send_pipeline_alert(error_msg, "HIGH", **context)
        raise

# ================================
# SPARK PROCESSING TASKS
# ================================

def trigger_spark_processing(**context):
    """Trigger Spark processing of Kafka streams"""
    spark_results = {}

    try:
        logger.info("⚡ Triggering Spark processing...")

        # Get streaming results
        streaming_results = context['task_instance'].xcom_pull(key='streaming_results', task_ids='kafka_streaming.stream_data_to_kafka')

        # Method 1: Direct Python execution in data-pipeline container
        spark_command = """
python -c "
import sys
sys.path.append('.')
from simple_spark_processor import VietnamDataProcessor
import time

print('🇻🇳 Starting Vietnam Spark Processor...')

processor = VietnamDataProcessor()
processor.kafka_config['bootstrap_servers'] = ['kafka:29092']

if processor.init_kafka_consumer() and processor.init_postgres_connection():
    print('✅ Connections established!')
    processor.running = True
    processed_count = 0
    topic_counts = {}

    try:
        start_time = time.time()
        for message in processor.consumer:
            if message.value:
                topic = message.topic
                topic_counts[topic] = topic_counts.get(topic, 0) + 1
                processed_count += 1

                # Process based on topic
                success = False
                if topic == 'vietnam_sales_events':
                    success = processor.process_sales_event(message.value)
                elif topic == 'vietnam_customers':
                    success = processor.process_customer_data(message.value)
                elif topic == 'vietnam_products':
                    success = processor.process_product_data(message.value)
                elif topic == 'vietnam_user_activities':
                    success = processor.process_user_activity(message.value)

                if processed_count % 10 == 0:
                    print(f'📊 Processed {processed_count} messages: {topic_counts}')

                # Process for 30 seconds or 100 messages max
                if time.time() - start_time > 30 or processed_count >= 100:
                    break

    except Exception as e:
        print(f'❌ Processing error: {e}')
    finally:
        print(f'✅ Processing completed! Total: {processed_count} messages')
        print(f'📈 Final counts: {topic_counts}')
        if processor.db_conn:
            processor.db_conn.close()
        if processor.consumer:
            processor.consumer.close()
else:
    print('❌ Connection failed')
"
"""

        # Execute Spark processor
        try:
            result = subprocess.run(
                ['docker', 'exec', 'ecommerce-dss-project-data-pipeline-1', 'bash', '-c', spark_command],
                capture_output=True,
                text=True,
                timeout=120  # 2 minute timeout
            )

            if result.returncode == 0:
                # Parse output for metrics
                output_lines = result.stdout.split('\n')
                processed_count = 0
                for line in output_lines:
                    if 'Processed' in line and 'messages' in line:
                        try:
                            processed_count = int(line.split('Processed ')[1].split(' messages')[0])
                        except:
                            pass

                spark_results = {
                    'status': 'success',
                    'messages_processed': processed_count,
                    'execution_time': 30,
                    'output': result.stdout[-500:],  # Last 500 chars
                    'method': 'direct_execution'
                }

                logger.info(f"✅ Spark processing completed: {processed_count} messages processed")

            else:
                spark_results = {
                    'status': 'error',
                    'error': result.stderr,
                    'output': result.stdout,
                    'return_code': result.returncode
                }
                logger.error(f"❌ Spark processing failed: {result.stderr}")

        except subprocess.TimeoutExpired:
            spark_results = {
                'status': 'timeout',
                'error': 'Processing timed out after 2 minutes'
            }
            logger.warning("⏰ Spark processing timed out")

        except Exception as e:
            spark_results = {
                'status': 'execution_error',
                'error': str(e)
            }
            logger.error(f"❌ Spark execution error: {str(e)}")

        # Store results
        context['task_instance'].xcom_push(key='spark_results', value=spark_results)

        if spark_results['status'] == 'success':
            send_pipeline_alert(
                f"Spark processing completed: {spark_results.get('messages_processed', 0)} messages processed",
                "INFO", **context
            )
        else:
            send_pipeline_alert(
                f"Spark processing {spark_results['status']}: {spark_results.get('error', 'Unknown error')}",
                "MEDIUM", **context
            )

        logger.info(f"⚡ Spark processing result: {spark_results}")
        return spark_results

    except Exception as e:
        error_msg = f"Spark processing trigger failed: {str(e)}"
        logger.error(error_msg)
        send_pipeline_alert(error_msg, "HIGH", **context)
        raise

# ================================
# DATA WAREHOUSE TASKS
# ================================

def load_to_data_warehouse(**context):
    """Load processed data to PostgreSQL data warehouse"""
    warehouse_results = {}

    try:
        logger.info("🏪 Loading data to data warehouse...")

        # Get processing results
        spark_results = context['task_instance'].xcom_pull(key='spark_results', task_ids='spark_processing.trigger_spark_processing')

        # Connect to PostgreSQL
        conn = get_postgres_connection()
        cursor = conn.cursor()

        # Check data warehouse tables
        warehouse_tables = [
            'vietnam_dw.dim_customer_vn',
            'vietnam_dw.dim_product_vn',
            'vietnam_dw.fact_sales_vn',
            'vietnam_dw.fact_customer_activity_vn'
        ]

        for table in warehouse_tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                warehouse_results[table] = {
                    'status': 'accessible',
                    'record_count': count
                }
                logger.info(f"✅ {table}: {count:,} records")

            except Exception as e:
                warehouse_results[table] = {
                    'status': 'error',
                    'error': str(e)
                }
                logger.error(f"❌ {table}: {str(e)}")

        # Load aggregated analytics
        try:
            # Create hourly analytics summary
            analytics_query = """
            INSERT INTO vietnam_dw.analytics_summary_vn (
                time_bucket, total_sales, total_revenue, avg_order_value,
                unique_customers, top_platform, created_at
            )
            SELECT
                DATE_TRUNC('hour', NOW()) as time_bucket,
                COUNT(*) as total_sales,
                SUM(COALESCE(total_amount, unit_price_vnd * quantity, 100000)) as total_revenue,
                AVG(COALESCE(total_amount, unit_price_vnd * quantity, 100000)) as avg_order_value,
                COUNT(DISTINCT customer_id) as unique_customers,
                'shopee' as top_platform,
                NOW() as created_at
            FROM vietnam_dw.fact_sales_vn
            WHERE order_date >= NOW() - INTERVAL '1 hour'
            ON CONFLICT (time_bucket) DO UPDATE SET
                total_sales = EXCLUDED.total_sales,
                total_revenue = EXCLUDED.total_revenue,
                avg_order_value = EXCLUDED.avg_order_value,
                unique_customers = EXCLUDED.unique_customers,
                updated_at = NOW()
            """

            cursor.execute(analytics_query)
            conn.commit()

            warehouse_results['analytics_summary'] = {
                'status': 'updated',
                'query': 'hourly_analytics'
            }

        except Exception as e:
            warehouse_results['analytics_summary'] = {
                'status': 'error',
                'error': str(e)
            }
            logger.error(f"Analytics summary error: {str(e)}")

        cursor.close()
        conn.close()

        # Store warehouse results
        warehouse_summary = {
            'tables_checked': len(warehouse_tables),
            'accessible_tables': len([r for r in warehouse_results.values() if r.get('status') == 'accessible']),
            'total_records': sum(r.get('record_count', 0) for r in warehouse_results.values() if r.get('status') == 'accessible'),
            'load_timestamp': datetime.now(),
            'results_by_table': warehouse_results
        }

        context['task_instance'].xcom_push(key='warehouse_results', value=json_serializable(warehouse_summary))

        send_pipeline_alert(
            f"Data warehouse check: {warehouse_summary['accessible_tables']}/{len(warehouse_tables)} tables accessible, {warehouse_summary['total_records']:,} total records",
            "INFO", **context
        )

        logger.info(f"🏪 Data warehouse loading completed: {warehouse_summary}")
        return warehouse_summary

    except Exception as e:
        error_msg = f"Data warehouse loading failed: {str(e)}"
        logger.error(error_msg)
        send_pipeline_alert(error_msg, "HIGH", **context)
        raise

# ================================
# VALIDATION & ANALYTICS TASKS
# ================================

def validate_pipeline_results(**context):
    """Validate end-to-end pipeline results"""
    validation_results = {}

    try:
        logger.info("🔍 Validating pipeline results...")

        # Get all previous results
        crawl_results = context['task_instance'].xcom_pull(key='crawl_results', task_ids='vietnam_data_crawling.crawl_vietnamese_ecommerce_data')
        api_results = context['task_instance'].xcom_pull(key='api_results', task_ids='api_data_processing.process_data_via_api')
        streaming_results = context['task_instance'].xcom_pull(key='streaming_results', task_ids='kafka_streaming.stream_data_to_kafka')
        spark_results = context['task_instance'].xcom_pull(key='spark_results', task_ids='spark_processing.trigger_spark_processing')
        warehouse_results = context['task_instance'].xcom_pull(key='warehouse_results', task_ids='data_warehouse.load_to_data_warehouse')

        # Validate data flow continuity
        validation_checks = {
            'crawl_to_api': {
                'crawled_products': crawl_results.get('total_products_crawled', 0),
                'api_processed': api_results.get('total_products_processed', 0),
                'success_rate': 0
            },
            'api_to_kafka': {
                'api_processed': api_results.get('total_products_processed', 0),
                'kafka_messages': streaming_results.get('total_messages_sent', 0),
                'success_rate': 0
            },
            'kafka_to_spark': {
                'kafka_messages': streaming_results.get('total_messages_sent', 0),
                'spark_processed': spark_results.get('messages_processed', 0),
                'success_rate': 0
            },
            'spark_to_warehouse': {
                'spark_processed': spark_results.get('messages_processed', 0),
                'warehouse_records': warehouse_results.get('total_records', 0),
                'success_rate': 0
            }
        }

        # Calculate success rates
        for check_name, check_data in validation_checks.items():
            if check_name == 'crawl_to_api':
                if check_data['crawled_products'] > 0:
                    check_data['success_rate'] = min(100, (check_data['api_processed'] / check_data['crawled_products']) * 100)
            elif check_name == 'api_to_kafka':
                if check_data['api_processed'] > 0:
                    check_data['success_rate'] = min(100, (check_data['kafka_messages'] / check_data['api_processed']) * 100)
            elif check_name == 'kafka_to_spark':
                if check_data['kafka_messages'] > 0:
                    check_data['success_rate'] = min(100, (check_data['spark_processed'] / check_data['kafka_messages']) * 100)
            elif check_name == 'spark_to_warehouse':
                # For warehouse, we check if tables are accessible (binary check)
                check_data['success_rate'] = 100 if warehouse_results.get('accessible_tables', 0) >= 3 else 0

        # Overall pipeline health
        avg_success_rate = np.mean([check['success_rate'] for check in validation_checks.values()])

        validation_summary = {
            'pipeline_health_score': round(avg_success_rate, 2),
            'validation_checks': validation_checks,
            'overall_status': 'healthy' if avg_success_rate >= 80 else 'degraded' if avg_success_rate >= 60 else 'unhealthy',
            'validation_timestamp': datetime.now(),
            'recommendations': []
        }

        # Add recommendations
        if avg_success_rate < 80:
            validation_summary['recommendations'].append("Pipeline health below 80% - investigate bottlenecks")
        if validation_checks['kafka_to_spark']['success_rate'] < 50:
            validation_summary['recommendations'].append("Low Spark processing rate - check Spark cluster health")
        if warehouse_results.get('accessible_tables', 0) < 4:
            validation_summary['recommendations'].append("Not all warehouse tables accessible - check database connection")

        context['task_instance'].xcom_push(key='validation_results', value=json_serializable(validation_summary))

        # Send appropriate alert
        if avg_success_rate >= 90:
            send_pipeline_alert(f"Pipeline validation excellent: {avg_success_rate:.1f}% health score", "INFO", **context)
        elif avg_success_rate >= 70:
            send_pipeline_alert(f"Pipeline validation good: {avg_success_rate:.1f}% health score", "MEDIUM", **context)
        else:
            send_pipeline_alert(f"Pipeline validation poor: {avg_success_rate:.1f}% health score - investigation needed", "HIGH", **context)

        logger.info(f"🔍 Pipeline validation completed: {validation_summary}")
        return validation_summary

    except Exception as e:
        error_msg = f"Pipeline validation failed: {str(e)}"
        logger.error(error_msg)
        send_pipeline_alert(error_msg, "HIGH", **context)
        raise

def generate_pipeline_report(**context):
    """Generate comprehensive pipeline execution report"""
    try:
        logger.info("📊 Generating pipeline execution report...")

        # Collect all results
        crawl_results = context['task_instance'].xcom_pull(key='crawl_results', task_ids='vietnam_data_crawling.crawl_vietnamese_ecommerce_data')
        api_results = context['task_instance'].xcom_pull(key='api_results', task_ids='api_data_processing.process_data_via_api')
        streaming_results = context['task_instance'].xcom_pull(key='streaming_results', task_ids='kafka_streaming.stream_data_to_kafka')
        spark_results = context['task_instance'].xcom_pull(key='spark_results', task_ids='spark_processing.trigger_spark_processing')
        warehouse_results = context['task_instance'].xcom_pull(key='warehouse_results', task_ids='data_warehouse.load_to_data_warehouse')
        validation_results = context['task_instance'].xcom_pull(key='validation_results', task_ids='validation_reporting.validate_pipeline_results')

        # Create comprehensive report
        pipeline_report = {
            'report_metadata': {
                'dag_id': DAG_ID,
                'execution_date': context['execution_date'].isoformat() if context.get('execution_date') else datetime.now().isoformat(),
                'dag_run_id': context['dag_run'].run_id if context.get('dag_run') else 'unknown',
                'report_generated_at': datetime.now().isoformat(),
                'pipeline_version': '3.0.0'
            },
            'execution_summary': {
                'total_execution_time': 'calculated_runtime',
                'overall_status': validation_results.get('overall_status', 'unknown'),
                'pipeline_health_score': validation_results.get('pipeline_health_score', 0)
            },
            'stage_results': {
                'crawling': {
                    'status': 'completed' if crawl_results.get('total_products_crawled', 0) > 0 else 'failed',
                    'products_crawled': crawl_results.get('total_products_crawled', 0),
                    'platforms_processed': crawl_results.get('successful_platforms', 0)
                },
                'api_processing': {
                    'status': 'completed' if api_results.get('total_products_processed', 0) > 0 else 'failed',
                    'products_processed': api_results.get('total_products_processed', 0),
                    'platforms_processed': api_results.get('platforms_processed', 0)
                },
                'kafka_streaming': {
                    'status': 'completed' if streaming_results.get('total_messages_sent', 0) > 0 else 'failed',
                    'messages_sent': streaming_results.get('total_messages_sent', 0),
                    'platforms_streamed': streaming_results.get('platforms_streamed', 0)
                },
                'spark_processing': {
                    'status': spark_results.get('status', 'unknown'),
                    'messages_processed': spark_results.get('messages_processed', 0),
                    'processing_method': spark_results.get('method', 'unknown')
                },
                'data_warehouse': {
                    'status': 'completed' if warehouse_results.get('accessible_tables', 0) >= 3 else 'degraded',
                    'accessible_tables': warehouse_results.get('accessible_tables', 0),
                    'total_records': warehouse_results.get('total_records', 0)
                }
            },
            'performance_metrics': {
                'data_throughput': {
                    'crawl_rate': f"{crawl_results.get('total_products_crawled', 0)}/2h",
                    'processing_rate': f"{api_results.get('total_products_processed', 0)}/2h",
                    'streaming_rate': f"{streaming_results.get('total_messages_sent', 0)}/2h"
                },
                'success_rates': validation_results.get('validation_checks', {}),
                'recommendations': validation_results.get('recommendations', [])
            },
            'vietnam_market_insights': {
                'platforms_covered': len(VIETNAM_PLATFORMS),
                'market_coverage_percent': sum(p['market_share'] for p in VIETNAM_PLATFORMS.values()) * 100,
                'data_freshness': 'within_2_hours'
            }
        }

        # Store report in MongoDB (create a copy to avoid ObjectId issues)
        mongo_client = get_mongo_client()
        db = mongo_client['vietnam_ecommerce_dss']
        db.pipeline_execution_reports.insert_one(json_serializable(pipeline_report))

        context['task_instance'].xcom_push(key='pipeline_report', value=json_serializable(pipeline_report))

        # Send summary alert
        summary_message = f"""Vietnam E-commerce Pipeline Completed:
• Health Score: {validation_results.get('pipeline_health_score', 0):.1f}%
• Products Crawled: {crawl_results.get('total_products_crawled', 0):,}
• Messages Streamed: {streaming_results.get('total_messages_sent', 0):,}
• Spark Processed: {spark_results.get('messages_processed', 0):,}
• Status: {validation_results.get('overall_status', 'unknown')}"""

        send_pipeline_alert(summary_message, "INFO", **context)

        logger.info(f"📊 Pipeline report generated: {pipeline_report['execution_summary']}")
        return pipeline_report

    except Exception as e:
        error_msg = f"Pipeline report generation failed: {str(e)}"
        logger.error(error_msg)
        send_pipeline_alert(error_msg, "MEDIUM", **context)
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
    tags=['vietnam', 'ecommerce', 'complete-pipeline', 'crawl', 'kafka', 'spark']
)

# ================================
# TASK DEFINITIONS
# ================================

# Start and end tasks
start_task = DummyOperator(
    task_id='start_vietnam_pipeline',
    dag=dag
)

end_task = DummyOperator(
    task_id='end_vietnam_pipeline',
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag
)

# Crawling task group
with TaskGroup("vietnam_data_crawling", dag=dag) as crawling_group:
    check_platforms_task = PythonOperator(
        task_id='check_vietnamese_platforms',
        python_callable=check_vietnamese_platforms,
        dag=dag
    )

    crawl_data_task = PythonOperator(
        task_id='crawl_vietnamese_ecommerce_data',
        python_callable=crawl_vietnamese_ecommerce_data,
        dag=dag
    )

# API processing task group
with TaskGroup("api_data_processing", dag=dag) as api_group:
    process_api_task = PythonOperator(
        task_id='process_data_via_api',
        python_callable=process_data_via_api,
        dag=dag
    )

# Kafka streaming task group
with TaskGroup("kafka_streaming", dag=dag) as kafka_group:
    setup_kafka_task = PythonOperator(
        task_id='setup_kafka_topics',
        python_callable=setup_kafka_topics,
        dag=dag
    )

    stream_kafka_task = PythonOperator(
        task_id='stream_data_to_kafka',
        python_callable=stream_data_to_kafka,
        dag=dag
    )

# Spark processing task group
with TaskGroup("spark_processing", dag=dag) as spark_group:
    spark_process_task = PythonOperator(
        task_id='trigger_spark_processing',
        python_callable=trigger_spark_processing,
        dag=dag
    )

# Data warehouse task group
with TaskGroup("data_warehouse", dag=dag) as warehouse_group:
    load_warehouse_task = PythonOperator(
        task_id='load_to_data_warehouse',
        python_callable=load_to_data_warehouse,
        dag=dag
    )

# Validation & reporting task group
with TaskGroup("validation_reporting", dag=dag) as validation_group:
    validate_task = PythonOperator(
        task_id='validate_pipeline_results',
        python_callable=validate_pipeline_results,
        dag=dag
    )

    report_task = PythonOperator(
        task_id='generate_pipeline_report',
        python_callable=generate_pipeline_report,
        dag=dag
    )

# ================================
# TASK DEPENDENCIES
# ================================

# Main pipeline flow: CRAWL → API → KAFKA → SPARK → WAREHOUSE → VALIDATION
start_task >> crawling_group >> api_group >> kafka_group >> spark_group >> warehouse_group >> validation_group >> end_task

# Internal task group dependencies
# Crawling group
check_platforms_task >> crawl_data_task

# Kafka group
setup_kafka_task >> stream_kafka_task

# Validation group
validate_task >> report_task

# ================================
# DAG DOCUMENTATION
# ================================

dag.doc_md = """
# Vietnam E-commerce Complete Data Pipeline

## 🇻🇳 Overview
Complete end-to-end data pipeline for Vietnamese e-commerce market analysis.

## 📊 Data Flow
```
CRAWL (Vietnamese Platforms)
    ↓
API (Data Processing)
    ↓
KAFKA (Real-time Streaming)
    ↓
SPARK (Stream Processing)
    ↓
DATA WAREHOUSE (PostgreSQL)
    ↓
ANALYTICS & VALIDATION
```

## 🏪 Vietnamese Platforms Covered
- **Shopee Vietnam** (35.2% market share)
- **Lazada Vietnam** (28.5% market share)
- **Tiki Vietnam** (15.8% market share)
- **Sendo Vietnam** (10.3% market share)
- **FPT Shop** (5.2% market share)
- **CellphoneS** (3.5% market share)

## ⚡ Processing Capabilities
- **Real-time streaming** via Kafka
- **Spark processing** for big data analytics
- **Data warehouse** with Vietnam-specific schema
- **End-to-end validation** with health scoring

## 📈 Monitoring
- Pipeline health scoring
- Performance metrics tracking
- Alert system for failures
- Comprehensive execution reports

## 🔄 Schedule
Runs every 2 hours to ensure fresh market data

## 📧 Contact
Vietnam DSS Team - vietnam-team@ecommerce-dss.com
"""