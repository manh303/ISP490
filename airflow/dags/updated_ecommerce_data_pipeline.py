#!/usr/bin/env python3
"""
Updated E-commerce Data Pipeline DAG
=====================================
Cập nhật pipeline để xử lý dữ liệu từ các nguồn có sẵn:
- Kaggle UK Retail Dataset (541K records)
- Vietnamese Integrated Data (36K records)
- Expanded Real Datasets (5K+ records)
- Electronics Product Catalogs
- Customer Data (100K+ records)

Version: 3.0.0
Updated: October 2025
"""

import os
import json
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from pathlib import Path

# Airflow imports
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

# Database connections
from sqlalchemy import create_engine
import pymongo

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ====================================
# DAG CONFIGURATION
# ====================================

# DAG default arguments
default_args = {
    'owner': 'ecommerce-dss-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# DAG definition
dag = DAG(
    'updated_ecommerce_data_pipeline',
    default_args=default_args,
    description='Updated E-commerce Data Pipeline for Available Datasets',
    schedule_interval='@daily',
    max_active_runs=1,
    tags=['ecommerce', 'data-pipeline', 'updated', 'real-data']
)

# ====================================
# DATA PATHS CONFIGURATION
# ====================================

DATA_PATHS = {
    'kaggle_retail': '/opt/airflow/data/raw/kaggle_datasets/data.csv',
    'vietnamese_integrated': {
        'products': '/opt/airflow/data/integrated/vietnamese_products_integrated_*.csv',
        'customers': '/opt/airflow/data/integrated/vietnamese_customers_integrated_*.csv',
        'orders': '/opt/airflow/data/integrated/vietnamese_orders_integrated_*.csv',
        'transactions': '/opt/airflow/data/integrated/vietnamese_transactions_integrated_*.csv'
    },
    'electronics': '/opt/airflow/data/electronics/electronics_products_en_*.csv',
    'expanded_real': {
        'dummyjson': '/opt/airflow/data/expanded_real/dummyjson_expanded_electronics.csv',
        'platzi': '/opt/airflow/data/expanded_real/platzi_electronics.csv',
        'transactions': '/opt/airflow/data/expanded_real/synthetic_realistic_transactions.csv',
        'customers': '/opt/airflow/data/expanded_real/jsonplaceholder_customers.csv'
    },
    'customers': '/opt/airflow/data/customers.csv',
    'output': '/opt/airflow/data/processed'
}

# ====================================
# PYTHON FUNCTIONS
# ====================================

def validate_data_sources(**context):
    """Validate that all required data sources are available"""
    logger.info("🔍 Validating data sources...")

    validation_results = {
        'kaggle_retail': False,
        'vietnamese_data': False,
        'electronics_data': False,
        'customer_data': False,
        'expanded_data': False
    }

    try:
        # Check Kaggle retail data
        kaggle_path = Path(DATA_PATHS['kaggle_retail'])
        if kaggle_path.exists():
            df_sample = pd.read_csv(kaggle_path, nrows=100)
            if len(df_sample) > 0:
                validation_results['kaggle_retail'] = True
                logger.info(f"✅ Kaggle retail data: {len(df_sample)} sample rows")

        # Check Vietnamese integrated data
        vietnamese_dir = Path('/opt/airflow/data/integrated')
        if vietnamese_dir.exists():
            files = list(vietnamese_dir.glob('vietnamese_*.csv'))
            if len(files) >= 3:  # Products, customers, orders
                validation_results['vietnamese_data'] = True
                logger.info(f"✅ Vietnamese data: {len(files)} files found")

        # Check electronics data
        electronics_dir = Path('/opt/airflow/data/electronics')
        if electronics_dir.exists():
            files = list(electronics_dir.glob('electronics_*.csv'))
            if len(files) > 0:
                validation_results['electronics_data'] = True
                logger.info(f"✅ Electronics data: {len(files)} files found")

        # Check customer data
        customer_path = Path(DATA_PATHS['customers'])
        if customer_path.exists():
            validation_results['customer_data'] = True
            logger.info("✅ Customer data available")

        # Check expanded real data
        expanded_dir = Path('/opt/airflow/data/expanded_real')
        if expanded_dir.exists():
            files = list(expanded_dir.glob('*.csv'))
            if len(files) > 0:
                validation_results['expanded_data'] = True
                logger.info(f"✅ Expanded data: {len(files)} files found")

        # Summary
        total_sources = sum(validation_results.values())
        logger.info(f"📊 Data validation complete: {total_sources}/5 sources available")

        # Push results to XCom
        context['task_instance'].xcom_push(key='validation_results', value=validation_results)

        return validation_results

    except Exception as e:
        logger.error(f"❌ Data validation failed: {e}")
        raise

def process_kaggle_retail_data(**context):
    """Process Kaggle UK Retail dataset for DSS analytics"""
    logger.info("🇬🇧 Processing Kaggle UK Retail data...")

    try:
        # Load data in chunks to handle large file
        chunk_size = 10000
        processed_chunks = []

        kaggle_path = DATA_PATHS['kaggle_retail']

        for chunk in pd.read_csv(kaggle_path, chunksize=chunk_size):
            # Data cleaning and processing
            chunk = chunk.dropna(subset=['CustomerID'])  # Remove records without customer
            chunk = chunk[chunk['Quantity'] > 0]  # Only positive quantities
            chunk = chunk[chunk['UnitPrice'] > 0]  # Only positive prices

            # Add calculated fields
            chunk['TotalAmount'] = chunk['Quantity'] * chunk['UnitPrice']
            chunk['TransactionDate'] = pd.to_datetime(chunk['InvoiceDate'])
            chunk['Year'] = chunk['TransactionDate'].dt.year
            chunk['Month'] = chunk['TransactionDate'].dt.month
            chunk['DayOfWeek'] = chunk['TransactionDate'].dt.dayofweek

            # Add product categorization
            chunk['ProductCategory'] = chunk['Description'].apply(categorize_product)

            processed_chunks.append(chunk)

            if len(processed_chunks) >= 5:  # Process 5 chunks at a time
                break

        # Combine processed chunks
        processed_data = pd.concat(processed_chunks, ignore_index=True)

        # Save processed data
        output_path = f"{DATA_PATHS['output']}/kaggle_retail_processed.csv"
        Path(DATA_PATHS['output']).mkdir(parents=True, exist_ok=True)
        processed_data.to_csv(output_path, index=False)

        # Generate summary
        summary = {
            'total_records': len(processed_data),
            'unique_customers': processed_data['CustomerID'].nunique(),
            'unique_products': processed_data['StockCode'].nunique(),
            'total_revenue': processed_data['TotalAmount'].sum(),
            'date_range': [
                processed_data['TransactionDate'].min().isoformat(),
                processed_data['TransactionDate'].max().isoformat()
            ]
        }

        logger.info(f"✅ Kaggle retail processed: {summary}")

        # Push to XCom
        context['task_instance'].xcom_push(key='kaggle_summary', value=summary)

        return summary

    except Exception as e:
        logger.error(f"❌ Kaggle retail processing failed: {e}")
        raise

def process_vietnamese_data(**context):
    """Process Vietnamese integrated datasets"""
    logger.info("🇻🇳 Processing Vietnamese integrated data...")

    try:
        vietnamese_dir = Path('/opt/airflow/data/integrated')

        # Load all Vietnamese datasets
        datasets = {}

        for pattern in ['products', 'customers', 'orders', 'transactions']:
            files = list(vietnamese_dir.glob(f'vietnamese_{pattern}_*.csv'))
            if files:
                # Load the most recent file
                latest_file = max(files, key=lambda x: x.stat().st_mtime)
                df = pd.read_csv(latest_file)
                datasets[pattern] = df
                logger.info(f"✅ Loaded {pattern}: {len(df)} records from {latest_file.name}")

        # Process each dataset
        processed_summary = {}

        # Products processing
        if 'products' in datasets:
            products = datasets['products']
            products['price_usd_clean'] = pd.to_numeric(products['price_usd'], errors='coerce')
            products['rating_clean'] = pd.to_numeric(products['rating'], errors='coerce')

            # Save processed products
            products.to_csv(f"{DATA_PATHS['output']}/vietnamese_products_processed.csv", index=False)

            processed_summary['products'] = {
                'total_products': len(products),
                'avg_price_usd': products['price_usd_clean'].mean(),
                'categories': products['category'].value_counts().to_dict(),
                'top_brands': products['brand'].value_counts().head(5).to_dict()
            }

        # Customers processing
        if 'customers' in datasets:
            customers = datasets['customers']
            customers['age_clean'] = pd.to_numeric(customers['age'], errors='coerce')

            customers.to_csv(f"{DATA_PATHS['output']}/vietnamese_customers_processed.csv", index=False)

            processed_summary['customers'] = {
                'total_customers': len(customers),
                'avg_age': customers['age_clean'].mean(),
                'segments': customers['customer_segment'].value_counts().to_dict(),
                'regions': customers['region'].value_counts().to_dict()
            }

        # Orders processing
        if 'orders' in datasets:
            orders = datasets['orders']
            orders['order_date_clean'] = pd.to_datetime(orders['order_date'], errors='coerce')
            orders['total_amount_usd_clean'] = pd.to_numeric(orders['total_amount_usd'], errors='coerce')

            orders.to_csv(f"{DATA_PATHS['output']}/vietnamese_orders_processed.csv", index=False)

            processed_summary['orders'] = {
                'total_orders': len(orders),
                'total_revenue_usd': orders['total_amount_usd_clean'].sum(),
                'avg_order_value': orders['total_amount_usd_clean'].mean(),
                'platforms': orders['platform'].value_counts().to_dict()
            }

        # Transactions processing
        if 'transactions' in datasets:
            transactions = datasets['transactions']
            transactions['amount_usd_clean'] = pd.to_numeric(transactions['amount_usd'], errors='coerce')

            transactions.to_csv(f"{DATA_PATHS['output']}/vietnamese_transactions_processed.csv", index=False)

            processed_summary['transactions'] = {
                'total_transactions': len(transactions),
                'total_volume_usd': transactions['amount_usd_clean'].sum(),
                'payment_methods': transactions['payment_method'].value_counts().to_dict()
            }

        logger.info(f"✅ Vietnamese data processed: {processed_summary}")

        # Push to XCom
        context['task_instance'].xcom_push(key='vietnamese_summary', value=processed_summary)

        return processed_summary

    except Exception as e:
        logger.error(f"❌ Vietnamese data processing failed: {e}")
        raise

def process_electronics_data(**context):
    """Process electronics product datasets"""
    logger.info("📱 Processing electronics data...")

    try:
        electronics_dir = Path('/opt/airflow/data/electronics')

        # Load main electronics file
        electronics_files = list(electronics_dir.glob('electronics_products_en_*.csv'))

        if not electronics_files:
            logger.warning("⚠️ No electronics files found")
            return {}

        # Load the most recent file
        latest_file = max(electronics_files, key=lambda x: x.stat().st_mtime)
        electronics = pd.read_csv(latest_file)

        # Data processing
        electronics['price_usd_clean'] = pd.to_numeric(electronics['price_usd'], errors='coerce')
        electronics['rating_clean'] = pd.to_numeric(electronics['rating'], errors='coerce')
        electronics['review_count_clean'] = pd.to_numeric(electronics['review_count'], errors='coerce')

        # Categorization
        electronics['price_tier'] = pd.cut(
            electronics['price_usd_clean'],
            bins=[0, 100, 500, 1000, float('inf')],
            labels=['Budget', 'Mid-Range', 'Premium', 'Luxury']
        )

        # Save processed data
        electronics.to_csv(f"{DATA_PATHS['output']}/electronics_processed.csv", index=False)

        # Generate summary
        summary = {
            'total_products': len(electronics),
            'price_stats': {
                'min': electronics['price_usd_clean'].min(),
                'max': electronics['price_usd_clean'].max(),
                'avg': electronics['price_usd_clean'].mean(),
                'median': electronics['price_usd_clean'].median()
            },
            'categories': electronics['main_category'].value_counts().to_dict(),
            'brands': electronics['brand'].value_counts().head(10).to_dict(),
            'price_tiers': electronics['price_tier'].value_counts().to_dict(),
            'avg_rating': electronics['rating_clean'].mean()
        }

        logger.info(f"✅ Electronics processed: {summary}")

        # Push to XCom
        context['task_instance'].xcom_push(key='electronics_summary', value=summary)

        return summary

    except Exception as e:
        logger.error(f"❌ Electronics processing failed: {e}")
        raise

def process_expanded_real_data(**context):
    """Process expanded real datasets from APIs"""
    logger.info("🌐 Processing expanded real data...")

    try:
        expanded_dir = Path('/opt/airflow/data/expanded_real')

        datasets = {}

        # Load each dataset
        dataset_files = {
            'dummyjson_electronics': 'dummyjson_expanded_electronics.csv',
            'platzi_electronics': 'platzi_electronics.csv',
            'synthetic_transactions': 'synthetic_realistic_transactions.csv',
            'api_customers': 'jsonplaceholder_customers.csv'
        }

        for dataset_name, filename in dataset_files.items():
            file_path = expanded_dir / filename
            if file_path.exists():
                df = pd.read_csv(file_path)
                datasets[dataset_name] = df
                logger.info(f"✅ Loaded {dataset_name}: {len(df)} records")

        processed_summary = {}

        # Process DummyJSON electronics
        if 'dummyjson_electronics' in datasets:
            dummyjson = datasets['dummyjson_electronics']
            dummyjson['price_clean'] = pd.to_numeric(dummyjson['price'], errors='coerce')
            dummyjson['rating_clean'] = pd.to_numeric(dummyjson['rating'], errors='coerce')

            dummyjson.to_csv(f"{DATA_PATHS['output']}/dummyjson_electronics_processed.csv", index=False)

            processed_summary['dummyjson'] = {
                'total_products': len(dummyjson),
                'categories': dummyjson['category'].value_counts().to_dict(),
                'brands': dummyjson['brand'].value_counts().to_dict(),
                'avg_price': dummyjson['price_clean'].mean()
            }

        # Process synthetic transactions
        if 'synthetic_transactions' in datasets:
            transactions = datasets['synthetic_transactions']
            transactions['total_amount_clean'] = pd.to_numeric(transactions['total_amount'], errors='coerce')
            transactions['transaction_date_clean'] = pd.to_datetime(transactions['transaction_date'], errors='coerce')

            transactions.to_csv(f"{DATA_PATHS['output']}/synthetic_transactions_processed.csv", index=False)

            processed_summary['transactions'] = {
                'total_transactions': len(transactions),
                'total_revenue': transactions['total_amount_clean'].sum(),
                'avg_order_value': transactions['total_amount_clean'].mean(),
                'payment_methods': transactions['payment_method'].value_counts().to_dict(),
                'categories': transactions['product_category'].value_counts().to_dict()
            }

        logger.info(f"✅ Expanded real data processed: {processed_summary}")

        # Push to XCom
        context['task_instance'].xcom_push(key='expanded_summary', value=processed_summary)

        return processed_summary

    except Exception as e:
        logger.error(f"❌ Expanded real data processing failed: {e}")
        raise

def consolidate_all_data(**context):
    """Consolidate all processed datasets into unified analytics tables"""
    logger.info("🔄 Consolidating all processed data...")

    try:
        # Get summaries from previous tasks
        kaggle_summary = context['task_instance'].xcom_pull(task_ids='process_kaggle_data', key='kaggle_summary') or {}
        vietnamese_summary = context['task_instance'].xcom_pull(task_ids='process_vietnamese_data', key='vietnamese_summary') or {}
        electronics_summary = context['task_instance'].xcom_pull(task_ids='process_electronics_data', key='electronics_summary') or {}
        expanded_summary = context['task_instance'].xcom_pull(task_ids='process_expanded_data', key='expanded_summary') or {}

        # Create consolidated summary
        consolidated_summary = {
            'consolidation_timestamp': datetime.now().isoformat(),
            'data_sources': {
                'kaggle_retail': kaggle_summary,
                'vietnamese_integrated': vietnamese_summary,
                'electronics_catalog': electronics_summary,
                'expanded_real': expanded_summary
            },
            'totals': {
                'total_transaction_records': 0,
                'total_product_records': 0,
                'total_customer_records': 0,
                'total_revenue_usd': 0
            }
        }

        # Calculate totals
        if kaggle_summary:
            consolidated_summary['totals']['total_transaction_records'] += kaggle_summary.get('total_records', 0)
            consolidated_summary['totals']['total_revenue_usd'] += kaggle_summary.get('total_revenue', 0)

        if vietnamese_summary:
            for key, data in vietnamese_summary.items():
                if key == 'products':
                    consolidated_summary['totals']['total_product_records'] += data.get('total_products', 0)
                elif key == 'customers':
                    consolidated_summary['totals']['total_customer_records'] += data.get('total_customers', 0)
                elif key == 'orders':
                    consolidated_summary['totals']['total_revenue_usd'] += data.get('total_revenue_usd', 0)

        if electronics_summary:
            consolidated_summary['totals']['total_product_records'] += electronics_summary.get('total_products', 0)

        # Save consolidated summary
        summary_path = f"{DATA_PATHS['output']}/consolidated_data_summary.json"
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(consolidated_summary, f, ensure_ascii=False, indent=2)

        logger.info(f"✅ Data consolidation complete: {consolidated_summary['totals']}")

        # Push to XCom
        context['task_instance'].xcom_push(key='consolidated_summary', value=consolidated_summary)

        return consolidated_summary

    except Exception as e:
        logger.error(f"❌ Data consolidation failed: {e}")
        raise

def generate_data_quality_report(**context):
    """Generate comprehensive data quality report"""
    logger.info("📊 Generating data quality report...")

    try:
        # Get consolidated summary
        consolidated_summary = context['task_instance'].xcom_pull(task_ids='consolidate_data', key='consolidated_summary') or {}

        # Data quality checks
        quality_report = {
            'report_timestamp': datetime.now().isoformat(),
            'data_sources_status': {},
            'quality_metrics': {},
            'recommendations': []
        }

        # Check each data source
        processed_dir = Path(DATA_PATHS['output'])

        expected_files = [
            'kaggle_retail_processed.csv',
            'vietnamese_products_processed.csv',
            'vietnamese_customers_processed.csv',
            'vietnamese_orders_processed.csv',
            'electronics_processed.csv',
            'synthetic_transactions_processed.csv'
        ]

        for filename in expected_files:
            file_path = processed_dir / filename
            if file_path.exists():
                try:
                    df = pd.read_csv(file_path, nrows=1000)  # Sample for quality check
                    quality_report['data_sources_status'][filename] = {
                        'status': 'available',
                        'sample_rows': len(df),
                        'columns': len(df.columns),
                        'null_percentage': (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100
                    }
                except Exception as e:
                    quality_report['data_sources_status'][filename] = {
                        'status': 'error',
                        'error': str(e)
                    }
            else:
                quality_report['data_sources_status'][filename] = {
                    'status': 'missing'
                }

        # Calculate overall quality metrics
        available_sources = sum(1 for status in quality_report['data_sources_status'].values() if status['status'] == 'available')
        total_sources = len(expected_files)

        quality_report['quality_metrics'] = {
            'data_completeness': (available_sources / total_sources) * 100,
            'sources_available': available_sources,
            'sources_total': total_sources,
            'consolidated_totals': consolidated_summary.get('totals', {})
        }

        # Generate recommendations
        if quality_report['quality_metrics']['data_completeness'] < 80:
            quality_report['recommendations'].append("Data completeness is below 80%. Review missing data sources.")

        if quality_report['quality_metrics']['data_completeness'] >= 90:
            quality_report['recommendations'].append("Excellent data completeness. Ready for analytics and reporting.")

        # Save quality report
        report_path = f"{DATA_PATHS['output']}/data_quality_report.json"
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(quality_report, f, ensure_ascii=False, indent=2)

        logger.info(f"✅ Data quality report generated: {quality_report['quality_metrics']}")

        return quality_report

    except Exception as e:
        logger.error(f"❌ Data quality report generation failed: {e}")
        raise

def categorize_product(description):
    """Categorize product based on description"""
    if pd.isna(description):
        return 'Unknown'

    desc_lower = str(description).lower()

    if any(term in desc_lower for term in ['phone', 'mobile', 'smartphone']):
        return 'Electronics'
    elif any(term in desc_lower for term in ['laptop', 'computer', 'tablet']):
        return 'Electronics'
    elif any(term in desc_lower for term in ['headphone', 'speaker', 'audio']):
        return 'Electronics'
    elif any(term in desc_lower for term in ['camera', 'photo']):
        return 'Electronics'
    elif any(term in desc_lower for term in ['book', 'magazine']):
        return 'Books'
    elif any(term in desc_lower for term in ['clothing', 'shirt', 'dress']):
        return 'Fashion'
    elif any(term in desc_lower for term in ['home', 'kitchen', 'furniture']):
        return 'Home'
    else:
        return 'General'

# ====================================
# TASK DEFINITIONS
# ====================================

# Start task
start_task = DummyOperator(
    task_id='start_pipeline',
    dag=dag
)

# Data validation task
validate_data_task = PythonOperator(
    task_id='validate_data_sources',
    python_callable=validate_data_sources,
    dag=dag
)

# Data processing tasks
process_kaggle_task = PythonOperator(
    task_id='process_kaggle_data',
    python_callable=process_kaggle_retail_data,
    dag=dag
)

process_vietnamese_task = PythonOperator(
    task_id='process_vietnamese_data',
    python_callable=process_vietnamese_data,
    dag=dag
)

process_electronics_task = PythonOperator(
    task_id='process_electronics_data',
    python_callable=process_electronics_data,
    dag=dag
)

process_expanded_task = PythonOperator(
    task_id='process_expanded_data',
    python_callable=process_expanded_real_data,
    dag=dag
)

# Data consolidation task
consolidate_task = PythonOperator(
    task_id='consolidate_data',
    python_callable=consolidate_all_data,
    dag=dag
)

# Data quality task
quality_report_task = PythonOperator(
    task_id='generate_quality_report',
    python_callable=generate_data_quality_report,
    dag=dag
)

# End task
end_task = DummyOperator(
    task_id='pipeline_complete',
    dag=dag,
    trigger_rule=TriggerRule.ALL_DONE
)

# ====================================
# TASK DEPENDENCIES
# ====================================

# Linear pipeline with parallel processing
start_task >> validate_data_task

validate_data_task >> [
    process_kaggle_task,
    process_vietnamese_task,
    process_electronics_task,
    process_expanded_task
]

[
    process_kaggle_task,
    process_vietnamese_task,
    process_electronics_task,
    process_expanded_task
] >> consolidate_task

consolidate_task >> quality_report_task >> end_task