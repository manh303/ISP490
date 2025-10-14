#!/usr/bin/env python3
"""
Custom Operators for Vietnam E-commerce Data Pipeline
===================================================
Specialized operators for Vietnamese e-commerce data processing

Author: Vietnam DSS Team
Version: 1.0.0
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import pymongo
from kafka import KafkaProducer
import requests

logger = logging.getLogger(__name__)

class VietnameseDataCrawlOperator(BaseOperator):
    """
    Operator for crawling Vietnamese e-commerce platforms
    """

    @apply_defaults
    def __init__(
        self,
        platforms: List[str] = None,
        data_limit: int = 1000,
        mongo_conn_id: str = 'mongo_default',
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.platforms = platforms or ['shopee', 'lazada', 'tiki', 'sendo']
        self.data_limit = data_limit
        self.mongo_conn_id = mongo_conn_id

    def execute(self, context):
        """Execute crawling operation"""
        logger.info(f"🇻🇳 Starting Vietnamese e-commerce crawling for platforms: {self.platforms}")

        results = {}
        total_crawled = 0

        # Platform configurations
        platform_configs = {
            'shopee': {
                'data_path': '/opt/airflow/data/vietnamese_ecommerce/shopee_products*.csv',
                'market_share': 0.352
            },
            'lazada': {
                'data_path': '/opt/airflow/data/vietnamese_ecommerce/lazada_products*.csv',
                'market_share': 0.285
            },
            'tiki': {
                'data_path': '/opt/airflow/data/vietnamese_ecommerce/tiki_products*.csv',
                'market_share': 0.158
            },
            'sendo': {
                'data_path': '/opt/airflow/data/vietnamese_ecommerce/sendo_products*.csv',
                'market_share': 0.103
            }
        }

        for platform in self.platforms:
            try:
                config = platform_configs.get(platform, {})
                data_path = config.get('data_path', '')

                # Find data files
                import glob
                files = glob.glob(data_path)

                if files:
                    latest_file = max(files, key=os.path.getmtime)

                    # Load and process data
                    if latest_file.endswith('.csv'):
                        df = pd.read_csv(latest_file)
                    elif latest_file.endswith('.json'):
                        df = pd.read_json(latest_file)
                    else:
                        continue

                    # Limit data size
                    if len(df) > self.data_limit:
                        df = df.sample(n=self.data_limit, random_state=42)

                    # Add metadata
                    df['platform'] = platform
                    df['market_share'] = config.get('market_share', 0)
                    df['crawled_at'] = datetime.now()
                    df['source_file'] = latest_file

                    results[platform] = {
                        'status': 'success',
                        'records': len(df),
                        'file': latest_file,
                        'data': df.to_dict('records')
                    }

                    total_crawled += len(df)
                    logger.info(f"✅ {platform}: {len(df)} records crawled")

                else:
                    results[platform] = {
                        'status': 'no_data',
                        'message': f"No data files found for {platform}"
                    }
                    logger.warning(f"⚠️ {platform}: No data files found")

            except Exception as e:
                results[platform] = {
                    'status': 'error',
                    'error': str(e)
                }
                logger.error(f"❌ {platform}: {str(e)}")

        # Summary
        crawl_summary = {
            'total_platforms': len(self.platforms),
            'successful_platforms': len([r for r in results.values() if r['status'] == 'success']),
            'total_records': total_crawled,
            'crawl_timestamp': datetime.now(),
            'platform_results': results
        }

        logger.info(f"🎉 Crawling completed: {total_crawled:,} total records")
        return crawl_summary

class KafkaStreamingOperator(BaseOperator):
    """
    Operator for streaming data to Kafka topics
    """

    @apply_defaults
    def __init__(
        self,
        kafka_servers: str = 'kafka:29092',
        topic_prefix: str = 'vietnam_',
        batch_size: int = 100,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.kafka_servers = kafka_servers
        self.topic_prefix = topic_prefix
        self.batch_size = batch_size

    def execute(self, context):
        """Execute Kafka streaming operation"""
        logger.info(f"🚀 Starting Kafka streaming to {self.kafka_servers}")

        # Get data from previous task
        upstream_data = context['task_instance'].xcom_pull(task_ids=self.upstream_task_ids[0])

        if not upstream_data or 'platform_results' not in upstream_data:
            logger.warning("No data to stream")
            return {'status': 'no_data'}

        # Initialize Kafka producer
        producer = KafkaProducer(
            bootstrap_servers=self.kafka_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None,
            acks='all',
            retries=3
        )

        streaming_results = {}
        total_messages = 0

        try:
            for platform, platform_data in upstream_data['platform_results'].items():
                if platform_data['status'] == 'success':
                    data_records = platform_data['data']

                    # Stream products
                    topic = f"{self.topic_prefix}products"
                    messages_sent = 0

                    for record in data_records:
                        # Create Kafka message
                        kafka_message = {
                            'message_id': f"{platform}_{record.get('id', messages_sent)}",
                            'platform': platform,
                            'data': record,
                            'metadata': {
                                'streamed_at': datetime.now().isoformat(),
                                'source': 'vietnam_pipeline'
                            }
                        }

                        producer.send(topic, value=kafka_message, key=kafka_message['message_id'])
                        messages_sent += 1

                        if messages_sent % self.batch_size == 0:
                            producer.flush()

                    producer.flush()

                    streaming_results[platform] = {
                        'status': 'streamed',
                        'messages_sent': messages_sent,
                        'topic': topic
                    }

                    total_messages += messages_sent
                    logger.info(f"✅ Streamed {messages_sent} messages from {platform}")

        finally:
            producer.close()

        streaming_summary = {
            'total_messages_sent': total_messages,
            'platforms_streamed': len([r for r in streaming_results.values() if r['status'] == 'streamed']),
            'streaming_timestamp': datetime.now(),
            'platform_results': streaming_results
        }

        logger.info(f"🚀 Kafka streaming completed: {total_messages:,} messages sent")
        return streaming_summary

class VietnameseDataValidationOperator(BaseOperator):
    """
    Operator for validating Vietnamese e-commerce data quality
    """

    @apply_defaults
    def __init__(
        self,
        postgres_conn_id: str = 'postgres_default',
        validation_rules: Dict = None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.validation_rules = validation_rules or {}

    def execute(self, context):
        """Execute data validation"""
        logger.info("🔍 Starting Vietnamese e-commerce data validation")

        # Default validation rules
        default_rules = {
            'vietnam_dw.dim_customer_vn': {
                'min_records': 100,
                'required_fields': ['customer_id', 'customer_name'],
                'checks': [
                    "SELECT COUNT(*) FROM vietnam_dw.dim_customer_vn",
                    "SELECT COUNT(*) FROM vietnam_dw.dim_customer_vn WHERE customer_name IS NOT NULL"
                ]
            },
            'vietnam_dw.dim_product_vn': {
                'min_records': 50,
                'required_fields': ['product_id', 'product_name'],
                'checks': [
                    "SELECT COUNT(*) FROM vietnam_dw.dim_product_vn",
                    "SELECT COUNT(*) FROM vietnam_dw.dim_product_vn WHERE price > 0"
                ]
            },
            'vietnam_dw.fact_sales_vn': {
                'min_records': 0,
                'required_fields': ['order_id', 'customer_id'],
                'checks': [
                    "SELECT COUNT(*) FROM vietnam_dw.fact_sales_vn",
                    "SELECT COUNT(*) FROM vietnam_dw.fact_sales_vn WHERE order_date >= NOW() - INTERVAL '24 hours'"
                ]
            }
        }

        validation_rules = {**default_rules, **self.validation_rules}

        # Get PostgreSQL connection
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        validation_results = {}
        overall_score = 0
        total_checks = 0
        passed_checks = 0

        for table, rules in validation_rules.items():
            table_results = {
                'checks_passed': 0,
                'total_checks': 0,
                'check_details': []
            }

            try:
                # Run validation checks
                for check_sql in rules.get('checks', []):
                    try:
                        result = postgres_hook.get_first(check_sql)
                        value = result[0] if result else 0

                        check_result = {
                            'sql': check_sql,
                            'value': value,
                            'status': 'passed'
                        }

                        table_results['check_details'].append(check_result)
                        table_results['checks_passed'] += 1
                        passed_checks += 1

                    except Exception as e:
                        check_result = {
                            'sql': check_sql,
                            'error': str(e),
                            'status': 'failed'
                        }
                        table_results['check_details'].append(check_result)

                    table_results['total_checks'] += 1
                    total_checks += 1

                # Calculate table score
                if table_results['total_checks'] > 0:
                    table_score = (table_results['checks_passed'] / table_results['total_checks']) * 100
                    table_results['score'] = round(table_score, 2)
                else:
                    table_results['score'] = 0

                validation_results[table] = table_results
                logger.info(f"✅ {table}: {table_results['score']}% ({table_results['checks_passed']}/{table_results['total_checks']})")

            except Exception as e:
                validation_results[table] = {
                    'error': str(e),
                    'score': 0
                }
                logger.error(f"❌ {table}: {str(e)}")

        # Calculate overall score
        if total_checks > 0:
            overall_score = (passed_checks / total_checks) * 100

        validation_summary = {
            'overall_score': round(overall_score, 2),
            'total_checks': total_checks,
            'passed_checks': passed_checks,
            'validation_timestamp': datetime.now(),
            'table_results': validation_results,
            'status': 'healthy' if overall_score >= 80 else 'degraded' if overall_score >= 60 else 'unhealthy'
        }

        logger.info(f"🔍 Validation completed: {overall_score:.1f}% overall score")
        return validation_summary

class VietnameseAnalyticsOperator(BaseOperator):
    """
    Operator for generating Vietnamese e-commerce analytics
    """

    @apply_defaults
    def __init__(
        self,
        postgres_conn_id: str = 'postgres_default',
        analytics_type: str = 'summary',
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.analytics_type = analytics_type

    def execute(self, context):
        """Execute analytics generation"""
        logger.info(f"📊 Generating Vietnamese e-commerce analytics: {self.analytics_type}")

        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        if self.analytics_type == 'summary':
            return self._generate_summary_analytics(postgres_hook)
        elif self.analytics_type == 'trends':
            return self._generate_trend_analytics(postgres_hook)
        elif self.analytics_type == 'platform':
            return self._generate_platform_analytics(postgres_hook)
        else:
            raise ValueError(f"Unknown analytics type: {self.analytics_type}")

    def _generate_summary_analytics(self, postgres_hook):
        """Generate summary analytics"""
        analytics_queries = {
            'total_customers': "SELECT COUNT(*) FROM vietnam_dw.dim_customer_vn",
            'total_products': "SELECT COUNT(*) FROM vietnam_dw.dim_product_vn",
            'total_sales': "SELECT COUNT(*) FROM vietnam_dw.fact_sales_vn",
            'total_revenue': "SELECT SUM(COALESCE(total_amount, unit_price_vnd * quantity, 0)) FROM vietnam_dw.fact_sales_vn",
            'avg_order_value': "SELECT AVG(COALESCE(total_amount, unit_price_vnd * quantity, 0)) FROM vietnam_dw.fact_sales_vn WHERE COALESCE(total_amount, unit_price_vnd * quantity, 0) > 0",
            'recent_sales_24h': "SELECT COUNT(*) FROM vietnam_dw.fact_sales_vn WHERE order_date >= NOW() - INTERVAL '24 hours'"
        }

        analytics_results = {}

        for metric, query in analytics_queries.items():
            try:
                result = postgres_hook.get_first(query)
                value = result[0] if result and result[0] is not None else 0
                analytics_results[metric] = float(value)
            except Exception as e:
                logger.error(f"Analytics query failed for {metric}: {str(e)}")
                analytics_results[metric] = 0

        # Calculate derived metrics
        analytics_results['revenue_per_customer'] = (
            analytics_results['total_revenue'] / analytics_results['total_customers']
            if analytics_results['total_customers'] > 0 else 0
        )

        analytics_results['products_per_customer'] = (
            analytics_results['total_products'] / analytics_results['total_customers']
            if analytics_results['total_customers'] > 0 else 0
        )

        analytics_summary = {
            'analytics_type': 'summary',
            'metrics': analytics_results,
            'generated_at': datetime.now(),
            'market': 'vietnam_ecommerce'
        }

        logger.info(f"📊 Summary analytics generated: {len(analytics_results)} metrics")
        return analytics_summary

    def _generate_trend_analytics(self, postgres_hook):
        """Generate trend analytics"""
        # Simplified trend analysis
        trend_query = """
        SELECT
            DATE(order_date) as date,
            COUNT(*) as daily_orders,
            SUM(COALESCE(total_amount, unit_price_vnd * quantity, 0)) as daily_revenue,
            COUNT(DISTINCT customer_id) as unique_customers
        FROM vietnam_dw.fact_sales_vn
        WHERE order_date >= NOW() - INTERVAL '7 days'
        GROUP BY DATE(order_date)
        ORDER BY date DESC
        """

        try:
            results = postgres_hook.get_records(trend_query)

            trend_data = []
            for row in results:
                trend_data.append({
                    'date': str(row[0]),
                    'daily_orders': int(row[1]),
                    'daily_revenue': float(row[2]),
                    'unique_customers': int(row[3])
                })

            analytics_summary = {
                'analytics_type': 'trends',
                'trend_data': trend_data,
                'period': '7_days',
                'generated_at': datetime.now()
            }

        except Exception as e:
            logger.error(f"Trend analytics failed: {str(e)}")
            analytics_summary = {
                'analytics_type': 'trends',
                'error': str(e),
                'generated_at': datetime.now()
            }

        logger.info(f"📈 Trend analytics generated: {len(trend_data) if 'trend_data' in analytics_summary else 0} data points")
        return analytics_summary

    def _generate_platform_analytics(self, postgres_hook):
        """Generate platform-specific analytics"""
        # This would require platform information in the sales data
        platform_query = """
        SELECT
            'vietnam_ecommerce' as platform,
            COUNT(*) as total_orders,
            SUM(COALESCE(total_amount, unit_price_vnd * quantity, 0)) as total_revenue,
            AVG(COALESCE(total_amount, unit_price_vnd * quantity, 0)) as avg_order_value
        FROM vietnam_dw.fact_sales_vn
        """

        try:
            result = postgres_hook.get_first(platform_query)

            if result:
                platform_data = {
                    'platform': result[0],
                    'total_orders': int(result[1]),
                    'total_revenue': float(result[2]),
                    'avg_order_value': float(result[3])
                }
            else:
                platform_data = {}

            analytics_summary = {
                'analytics_type': 'platform',
                'platform_data': platform_data,
                'generated_at': datetime.now()
            }

        except Exception as e:
            logger.error(f"Platform analytics failed: {str(e)}")
            analytics_summary = {
                'analytics_type': 'platform',
                'error': str(e),
                'generated_at': datetime.now()
            }

        logger.info(f"🏪 Platform analytics generated")
        return analytics_summary