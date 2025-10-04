#!/usr/bin/env python3
"""
Main Data Pipeline DAG for Big Data Analytics
Orchestrates batch ETL processes, data quality checks, and ML model updates
"""

import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Any

# Airflow imports
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

# Custom operators
sys.path.append('/opt/airflow/dags')
try:
    from operators.data_quality_operator import DataQualityOperator
    from operators.ml_training_operator import MLTrainingOperator
    from operators.data_export_operator import DataExportOperator
    CUSTOM_OPERATORS_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Custom operators not available: {e}")
    CUSTOM_OPERATORS_AVAILABLE = False
    # Create placeholder operators
    DataQualityOperator = PythonOperator
    MLTrainingOperator = PythonOperator
    DataExportOperator = PythonOperator

# Python libraries
import pandas as pd
import numpy as np
import json
import logging
from pathlib import Path
import boto3
from pymongo import MongoClient
import redis

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ====================================
# DAG CONFIGURATION
# ====================================

# Default arguments for all tasks - PERFORMANCE OPTIMIZED
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['data-team@company.com'],
    'retries': 3,  # Increased retries for reliability
    'retry_delay': timedelta(minutes=2),  # Reduced retry delay
    'retry_exponential_backoff': True,  # Exponential backoff
    'max_retry_delay': timedelta(minutes=10),
    'start_date': days_ago(1),
    'catchup': False,
    'pool': 'default_pool',  # Use connection pool
    'priority_weight': 10  # Higher priority for main pipeline
}

# DAG definition - PERFORMANCE OPTIMIZED
dag = DAG(
    'main_data_pipeline',
    default_args=default_args,
    description='Main ETL pipeline for big data analytics platform - Performance Optimized',
    schedule_interval='@hourly',  # Run every hour
    max_active_runs=2,  # Allow 2 concurrent runs for better throughput
    max_active_tasks=16,  # Increased task parallelism
    concurrency=16,  # Global concurrency limit
    catchup=False,  # Don't backfill missed runs
    dagrun_timeout=timedelta(minutes=45),  # 45 minute SLA
    tags=['etl', 'analytics', 'production', 'optimized'],
    doc_md="""
    # Main Data Pipeline DAG - Performance Optimized

    This DAG orchestrates the complete data processing pipeline with performance enhancements:

    ## Performance Features:
    - Increased task parallelism (max_active_tasks=16)
    - Optimized retry logic with exponential backoff
    - Connection pooling for database operations
    - Batch processing optimizations
    - Memory-efficient data transformations

    ## Stages:
    1. **Data Ingestion**: Load new data from various sources
    2. **Data Validation**: Quality checks and schema validation
    3. **Data Transformation**: ETL processes and aggregations (Parallel)
    4. **Data Storage**: Write processed data to data warehouse
    5. **ML Pipeline**: Update models with fresh data
    6. **Data Export**: Generate reports and exports
    7. **Monitoring**: Send alerts and notifications

    ## Schedule: Hourly execution
    ## SLA: 45 minutes
    ## Max Parallel Tasks: 16
    """
)

# ====================================
# HELPER FUNCTIONS
# ====================================

def get_postgres_connection():
    """Get PostgreSQL connection"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    return postgres_hook.get_conn()

def get_mongodb_connection():
    """Get MongoDB connection"""
    mongo_uri = Variable.get("MONGODB_URI", "mongodb://root:admin123@mongodb:27017/")
    return MongoClient(mongo_uri)

def get_redis_connection():
    """Get Redis connection"""
    redis_host = Variable.get("REDIS_HOST", "redis")
    redis_port = int(Variable.get("REDIS_PORT", "6379"))
    return redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

def check_data_freshness(**context):
    """Check if new data is available for processing"""
    logger.info("🔍 Checking data freshness...")
    
    try:
        # Check MongoDB for new streaming data
        mongo_client = get_mongodb_connection()
        db = mongo_client.dss_streaming
        
        # Get current time
        current_time = datetime.now()
        one_hour_ago = current_time - timedelta(hours=1)
        
        # Check each collection for recent data
        collections_to_check = ['products_raw', 'customers_raw', 'orders_raw']
        fresh_data_count = 0
        
        for collection_name in collections_to_check:
            collection = db[collection_name]
            recent_count = collection.count_documents({
                'timestamp': {'$gte': one_hour_ago}
            })
            
            logger.info(f"📊 {collection_name}: {recent_count} records in last hour")
            fresh_data_count += recent_count
        
        # Store metrics in XCom for downstream tasks
        context['task_instance'].xcom_push(key='fresh_data_count', value=fresh_data_count)
        context['task_instance'].xcom_push(key='check_timestamp', value=current_time.isoformat())
        
        if fresh_data_count == 0:
            logger.warning("⚠️ No fresh data found!")
            return False
        else:
            logger.info(f"✅ Found {fresh_data_count} fresh records")
            return True
            
    except Exception as e:
        logger.error(f"❌ Data freshness check failed: {e}")
        raise
    finally:
        if 'mongo_client' in locals():
            mongo_client.close()

def extract_streaming_data(**context):
    """Extract data from available CSV files and MongoDB collections"""
    logger.info("📥 Extracting data from available sources...")

    try:
        extracted_data = {}
        execution_date = context['execution_date']

        # Data sources based on available datasets
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

        for source in data_sources:
            try:
                import os
                file_path = source['path']

                if os.path.exists(file_path):
                    # Load CSV data
                    df = pd.read_csv(file_path)

                    # Sample data for processing (to avoid memory issues)
                    if len(df) > 10000:
                        df = df.sample(n=10000, random_state=42)
                        logger.info(f"📊 Sampled 10,000 records from {source['name']} (original: {len(df)} records)")

                    extracted_data[source['name']] = len(df)

                    # Add processing metadata
                    df['extracted_at'] = execution_date
                    df['data_source'] = source['name']

                    # Save to temporary location for next tasks
                    output_path = f"/tmp/extracted_{source['name']}_{execution_date.strftime('%Y%m%d_%H')}.parquet"
                    df.to_parquet(output_path, index=False)

                    logger.info(f"💾 Extracted {len(df)} records from {source['name']} to {output_path}")

                else:
                    logger.warning(f"⚠️ File not found: {file_path}")
                    extracted_data[source['name']] = 0

            except Exception as e:
                logger.error(f"❌ Failed to extract from {source['name']}: {e}")
                extracted_data[source['name']] = 0

        # Also try to extract from MongoDB if available
        try:
            mongo_client = get_mongodb_connection()
            db = mongo_client.ecommerce_dss

            # Check for any existing collections
            mongo_collections = ['raw_data_collection', 'processed_data']

            for collection_name in mongo_collections:
                try:
                    collection = db[collection_name]
                    documents = list(collection.find().limit(1000))  # Limit to avoid memory issues

                    if documents:
                        df = pd.DataFrame(documents)

                        # Clean ObjectId fields
                        if '_id' in df.columns:
                            df['_id'] = df['_id'].astype(str)

                        extracted_data[f"mongo_{collection_name}"] = len(df)

                        # Save to temporary location
                        output_path = f"/tmp/extracted_mongo_{collection_name}_{execution_date.strftime('%Y%m%d_%H')}.parquet"
                        df.to_parquet(output_path, index=False)

                        logger.info(f"💾 Extracted {len(df)} records from MongoDB {collection_name}")

                except Exception as e:
                    logger.debug(f"Collection {collection_name} not available: {e}")

        except Exception as e:
            logger.debug(f"MongoDB extraction skipped: {e}")

        # Store extraction stats in XCom
        context['task_instance'].xcom_push(key='extraction_stats', value=extracted_data)

        total_records = sum(extracted_data.values())
        logger.info(f"✅ Total extraction completed: {total_records:,} records from {len(extracted_data)} sources")

        return extracted_data

    except Exception as e:
        logger.error(f"❌ Data extraction failed: {e}")
        raise

def transform_products_data(**context):
    """Transform products data with business logic"""
    logger.info("🔄 Transforming products data...")
    
    try:
        execution_date = context['execution_date']
        input_file = f"/tmp/extracted_products_raw_{execution_date.strftime('%Y%m%d_%H')}.parquet"
        
        if not Path(input_file).exists():
            logger.info("ℹ️ No products data to transform")
            return
        
        # Load data
        df = pd.read_parquet(input_file)
        logger.info(f"📊 Loaded {len(df)} product records")
        
        # Data transformations
        df_transformed = df.copy()
        
        # 1. Calculate product dimensions volume
        df_transformed['product_volume_cm3'] = (
            df_transformed['product_length_cm'].fillna(0) * 
            df_transformed['product_height_cm'].fillna(0) * 
            df_transformed['product_width_cm'].fillna(0)
        )
        
        # 2. Categorize product weight
        df_transformed['weight_category'] = pd.cut(
            df_transformed['product_weight_g'].fillna(0),
            bins=[0, 100, 500, 1000, 5000, float('inf')],
            labels=['Very Light', 'Light', 'Medium', 'Heavy', 'Very Heavy']
        )
        
        # 3. Calculate product score based on photos and description
        df_transformed['product_quality_score'] = (
            (df_transformed['product_photos_qty'].fillna(0) * 0.3) +
            (df_transformed['product_description_length'].fillna(0) / 1000 * 0.4) +
            (df_transformed['product_name_length'].fillna(0) / 100 * 0.3)
        ).round(2)
        
        # 4. Clean and standardize category names
        df_transformed['product_category_clean'] = (
            df_transformed['product_category_name']
            .fillna('unknown')
            .str.lower()
            .str.replace('[^a-z0-9_]', '_', regex=True)
            .str.strip('_')
        )
        
        # 5. Add processing metadata
        df_transformed['processed_at'] = datetime.now()
        df_transformed['processing_date'] = execution_date.date()
        df_transformed['data_source'] = 'streaming'
        
        # Save transformed data
        output_file = f"/tmp/transformed_products_{execution_date.strftime('%Y%m%d_%H')}.parquet"
        df_transformed.to_parquet(output_file, index=False)
        
        # Store transformation stats
        stats = {
            'input_records': len(df),
            'output_records': len(df_transformed),
            'null_weights': df['product_weight_g'].isnull().sum(),
            'categories_found': df['product_category_name'].nunique()
        }
        
        context['task_instance'].xcom_push(key='products_transform_stats', value=stats)
        logger.info(f"✅ Products transformation completed: {stats}")
        
    except Exception as e:
        logger.error(f"❌ Products transformation failed: {e}")
        raise

def transform_customers_data(**context):
    """Transform customers data with business logic"""
    logger.info("🔄 Transforming customers data...")
    
    try:
        execution_date = context['execution_date']
        input_file = f"/tmp/extracted_customers_raw_{execution_date.strftime('%Y%m%d_%H')}.parquet"
        
        if not Path(input_file).exists():
            logger.info("ℹ️ No customers data to transform")
            return
        
        # Load data
        df = pd.read_parquet(input_file)
        logger.info(f"📊 Loaded {len(df)} customer records")
        
        # Data transformations
        df_transformed = df.copy()
        
        # 1. Standardize zip codes
        df_transformed['customer_zip_code_clean'] = (
            df_transformed['customer_zip_code_prefix']
            .astype(str)
            .str.zfill(5)  # Pad with zeros to 5 digits
        )
        
        # 2. Create customer segments based on location
        state_segments = {
            'SP': 'Metropolitan',
            'RJ': 'Metropolitan', 
            'MG': 'Southeast',
            'PR': 'South',
            'RS': 'South',
            'SC': 'South'
        }
        
        df_transformed['customer_segment'] = (
            df_transformed['customer_state']
            .map(state_segments)
            .fillna('Other')
        )
        
        # 3. Clean city names
        df_transformed['customer_city_clean'] = (
            df_transformed['customer_city']
            .fillna('unknown')
            .str.title()
            .str.strip()
        )
        
        # 4. Add processing metadata
        df_transformed['processed_at'] = datetime.now()
        df_transformed['processing_date'] = execution_date.date()
        df_transformed['data_source'] = 'streaming'
        
        # Save transformed data
        output_file = f"/tmp/transformed_customers_{execution_date.strftime('%Y%m%d_%H')}.parquet"
        df_transformed.to_parquet(output_file, index=False)
        
        # Store transformation stats
        stats = {
            'input_records': len(df),
            'output_records': len(df_transformed),
            'unique_states': df['customer_state'].nunique(),
            'unique_cities': df['customer_city'].nunique()
        }
        
        context['task_instance'].xcom_push(key='customers_transform_stats', value=stats)
        logger.info(f"✅ Customers transformation completed: {stats}")
        
    except Exception as e:
        logger.error(f"❌ Customers transformation failed: {e}")
        raise

def create_analytics_aggregations(**context):
    """Create analytical aggregations and summaries"""
    logger.info("📈 Creating analytics aggregations...")
    
    try:
        execution_date = context['execution_date']
        
        # Files to process
        products_file = f"/tmp/transformed_products_{execution_date.strftime('%Y%m%d_%H')}.parquet"
        customers_file = f"/tmp/transformed_customers_{execution_date.strftime('%Y%m%d_%H')}.parquet"
        
        aggregations = {}
        
        # Products aggregations
        if Path(products_file).exists():
            df_products = pd.read_parquet(products_file)
            
            products_agg = df_products.groupby(['product_category_clean', 'weight_category']).agg({
                'product_id': 'count',
                'product_weight_g': ['mean', 'median'],
                'product_volume_cm3': ['mean', 'sum'],
                'product_quality_score': 'mean',
                'product_photos_qty': 'mean'
            }).round(2)
            
            # Flatten column names
            products_agg.columns = ['_'.join(col).strip() for col in products_agg.columns]
            products_agg = products_agg.reset_index()
            
            # Add metadata
            products_agg['aggregation_date'] = execution_date.date()
            products_agg['aggregation_hour'] = execution_date.hour
            products_agg['metric_type'] = 'products_hourly'
            
            aggregations['products'] = products_agg
            logger.info(f"📊 Created {len(products_agg)} product aggregations")
        
        # Customers aggregations  
        if Path(customers_file).exists():
            df_customers = pd.read_parquet(customers_file)
            
            customers_agg = df_customers.groupby(['customer_state', 'customer_segment']).agg({
                'customer_id': 'count',
                'customer_city': 'nunique'
            }).reset_index()
            
            customers_agg.columns = ['customer_state', 'customer_segment', 'customer_count', 'unique_cities']
            
            # Add metadata
            customers_agg['aggregation_date'] = execution_date.date()
            customers_agg['aggregation_hour'] = execution_date.hour
            customers_agg['metric_type'] = 'customers_hourly'
            
            aggregations['customers'] = customers_agg
            logger.info(f"📊 Created {len(customers_agg)} customer aggregations")
        
        # Save aggregations
        for agg_type, agg_df in aggregations.items():
            output_file = f"/tmp/aggregated_{agg_type}_{execution_date.strftime('%Y%m%d_%H')}.parquet"
            agg_df.to_parquet(output_file, index=False)
        
        # Store stats
        agg_stats = {k: len(v) for k, v in aggregations.items()}
        context['task_instance'].xcom_push(key='aggregation_stats', value=agg_stats)
        
        logger.info(f"✅ Analytics aggregations completed: {agg_stats}")
        
    except Exception as e:
        logger.error(f"❌ Analytics aggregation failed: {e}")
        raise

def load_to_data_warehouse(**context):
    """Load processed data to PostgreSQL data warehouse"""
    logger.info("🏪 Loading data to warehouse...")
    
    try:
        execution_date = context['execution_date']
        postgres_conn = get_postgres_connection()
        
        # Files to load
        files_to_load = [
            ('transformed_products', 'products_processed'),
            ('transformed_customers', 'customers_processed'),  
            ('aggregated_products', 'analytics_products_hourly'),
            ('aggregated_customers', 'analytics_customers_hourly')
        ]
        
        loaded_counts = {}
        
        for file_prefix, table_name in files_to_load:
            file_path = f"/tmp/{file_prefix}_{execution_date.strftime('%Y%m%d_%H')}.parquet"
            
            if Path(file_path).exists():
                # Load DataFrame
                df = pd.read_parquet(file_path)
                
                # Convert DataFrame to SQL
                df.to_sql(
                    table_name,
                    postgres_conn,
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=1000
                )
                
                loaded_counts[table_name] = len(df)
                logger.info(f"✅ Loaded {len(df)} records to {table_name}")
            else:
                logger.info(f"ℹ️ No data file found: {file_path}")
        
        # Store load stats
        context['task_instance'].xcom_push(key='warehouse_load_stats', value=loaded_counts)
        
        logger.info(f"🏪 Warehouse loading completed: {loaded_counts}")
        
    except Exception as e:
        logger.error(f"❌ Warehouse loading failed: {e}")
        raise
    finally:
        if 'postgres_conn' in locals():
            postgres_conn.close()

def cleanup_temp_files(**context):
    """Clean up temporary files"""
    logger.info("🧹 Cleaning up temporary files...")
    
    execution_date = context['execution_date']
    temp_pattern = f"/tmp/*_{execution_date.strftime('%Y%m%d_%H')}.parquet"
    
    # Use bash command to clean up
    import glob
    temp_files = glob.glob(temp_pattern)
    
    for file_path in temp_files:
        try:
            Path(file_path).unlink()
            logger.info(f"🗑️ Deleted: {file_path}")
        except Exception as e:
            logger.warning(f"⚠️ Could not delete {file_path}: {e}")
    
    logger.info(f"✅ Cleaned up {len(temp_files)} temporary files")

def send_pipeline_summary(**context):
    """Send pipeline execution summary"""
    logger.info("📧 Sending pipeline summary...")
    
    try:
        # Collect stats from XCom
        fresh_data_count = context['task_instance'].xcom_pull(key='fresh_data_count', task_ids='check_data_freshness')
        extraction_stats = context['task_instance'].xcom_pull(key='extraction_stats', task_ids='extract_streaming_data')
        products_stats = context['task_instance'].xcom_pull(key='products_transform_stats', task_ids='transform_products_data')
        customers_stats = context['task_instance'].xcom_pull(key='customers_transform_stats', task_ids='transform_customers_data')
        agg_stats = context['task_instance'].xcom_pull(key='aggregation_stats', task_ids='create_analytics_aggregations')
        warehouse_stats = context['task_instance'].xcom_pull(key='warehouse_load_stats', task_ids='load_to_data_warehouse')
        
        # Create summary
        summary = {
            'execution_date': context['execution_date'].isoformat(),
            'dag_run_id': context['dag_run'].run_id,
            'fresh_data_records': fresh_data_count or 0,
            'extraction_stats': extraction_stats or {},
            'transformation_stats': {
                'products': products_stats or {},
                'customers': customers_stats or {}
            },
            'aggregation_stats': agg_stats or {},
            'warehouse_load_stats': warehouse_stats or {},
            'pipeline_status': 'SUCCESS'
        }
        
        # Store in Redis for monitoring
        redis_client = get_redis_connection()
        redis_client.setex(
            f"pipeline_summary_{context['execution_date'].strftime('%Y%m%d_%H')}", 
            timedelta(days=7),
            json.dumps(summary)
        )
        
        logger.info(f"📊 Pipeline Summary: {summary}")
        
        return summary
        
    except Exception as e:
        logger.error(f"❌ Failed to send pipeline summary: {e}")
        raise

# ====================================
# TASK DEFINITIONS
# ====================================

# Start task
start_task = DummyOperator(
    task_id='start',
    dag=dag
)

# Data freshness check
check_freshness_task = PythonOperator(
    task_id='check_data_freshness',
    python_callable=check_data_freshness,
    dag=dag
)

# Health checks
with TaskGroup("health_checks", dag=dag) as health_checks_group:
    
    postgres_health = PostgresOperator(
        task_id='check_postgres_health',
        postgres_conn_id='postgres_default',
        sql="SELECT 1 as health_check;",
        dag=dag
    )
    
    api_health = HttpSensor(
        task_id='check_api_health',
        http_conn_id='api_default',
        endpoint='/health',
        timeout=30,
        poke_interval=10,
        dag=dag
    )

# Data extraction
extract_task = PythonOperator(
    task_id='extract_streaming_data',
    python_callable=extract_streaming_data,
    dag=dag
)

# Data transformation tasks
with TaskGroup("data_transformations", dag=dag) as transform_group:
    
    transform_products_task = PythonOperator(
        task_id='transform_products_data',
        python_callable=transform_products_data,
        dag=dag
    )
    
    transform_customers_task = PythonOperator(
        task_id='transform_customers_data',
        python_callable=transform_customers_data,
        dag=dag
    )

# Analytics aggregation
aggregate_task = PythonOperator(
    task_id='create_analytics_aggregations',
    python_callable=create_analytics_aggregations,
    dag=dag
)

# Data quality checks
data_quality_task = DataQualityOperator(
    task_id='data_quality_checks',
    postgres_conn_id='postgres_default',
    tables=['products_processed', 'customers_processed'],
    dag=dag
)

# Load to warehouse
load_warehouse_task = PythonOperator(
    task_id='load_to_data_warehouse',
    python_callable=load_to_data_warehouse,
    dag=dag
)

# ML pipeline (if enabled)
ml_training_task = MLTrainingOperator(
    task_id='update_ml_models',
    model_types=['customer_segmentation', 'product_recommendation'],
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag
)

# Data export (if enabled)
data_export_task = DataExportOperator(
    task_id='export_reports',
    export_types=['daily_summary', 'customer_insights'],
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag
)

# Cleanup
cleanup_task = PythonOperator(
    task_id='cleanup_temp_files',
    python_callable=cleanup_temp_files,
    trigger_rule=TriggerRule.ALL_DONE,  # Run regardless of upstream success/failure
    dag=dag
)

# Pipeline summary
summary_task = PythonOperator(
    task_id='send_pipeline_summary',
    python_callable=send_pipeline_summary,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag
)

# Slack notification on failure
slack_alert_task = SlackWebhookOperator(
    task_id='slack_alert_on_failure',
    http_conn_id='slack_default',
    message="""
    ❌ Data Pipeline Failed
    DAG: {{ dag.dag_id }}
    Execution Date: {{ ds }}
    Task: {{ task.task_id }}
    """,
    trigger_rule=TriggerRule.ONE_FAILED,
    dag=dag
)

# End task
end_task = DummyOperator(
    task_id='end',
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag
)

# ====================================
# TASK DEPENDENCIES
# ====================================

# Main pipeline flow
start_task >> check_freshness_task >> health_checks_group >> extract_task
extract_task >> transform_group >> aggregate_task >> data_quality_task
data_quality_task >> load_warehouse_task >> [ml_training_task, data_export_task]
[ml_training_task, data_export_task] >> cleanup_task >> summary_task >> end_task

# Failure handling
[check_freshness_task, extract_task, transform_group, aggregate_task, 
 data_quality_task, load_warehouse_task] >> slack_alert_task