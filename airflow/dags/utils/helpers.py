#!/usr/bin/env python3
"""
Utility Functions and Helpers for E-commerce DSS Pipeline
======================================================
Common functions, data transformations, and helper utilities
"""

import os
import json
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union, Tuple
import hashlib
import uuid

# Database connections
import pymongo
import redis
from sqlalchemy import create_engine, text
from kafka import KafkaProducer, KafkaConsumer

# Data processing
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import mean_absolute_error, mean_squared_error, accuracy_score

# ================================
# CONFIGURATION HELPERS
# ================================

class ConfigManager:
    """Configuration management utility"""

    def __init__(self, config_path: str = None):
        self.config_path = config_path or "/opt/airflow/config/dss_config.json"
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from file"""
        try:
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r') as f:
                    return json.load(f)
            else:
                return self._get_default_config()
        except Exception as e:
            logging.warning(f"Failed to load config from {self.config_path}: {str(e)}")
            return self._get_default_config()

    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration"""
        return {
            'databases': {
                'postgres': {
                    'host': 'postgres',
                    'port': 5432,
                    'database': 'ecommerce_dss',
                    'user': 'dss_user',
                    'password': 'dss_password_123'
                },
                'mongodb': {
                    'uri': 'mongodb://admin:admin_password@mongodb:27017/',
                    'database': 'ecommerce_dss'
                },
                'redis': {
                    'url': 'redis://redis:6379/0'
                }
            },
            'kafka': {
                'bootstrap_servers': 'kafka:29092',
                'topics': {
                    'products': 'ecommerce.products.stream',
                    'customers': 'ecommerce.customers.stream',
                    'orders': 'ecommerce.orders.stream',
                    'analytics': 'ecommerce.analytics.stream',
                    'predictions': 'ecommerce.ml.predictions',
                    'alerts': 'ecommerce.alerts'
                }
            },
            'ml': {
                'models_path': '/app/models',
                'feature_store': {
                    'type': 'mongodb',
                    'collection': 'ml_features'
                },
                'model_registry': {
                    'type': 'mongodb',
                    'collection': 'ml_models'
                }
            },
            'data_quality': {
                'default_threshold': 0.8,
                'alert_threshold': 0.6
            },
            'monitoring': {
                'health_check_interval': 300,  # seconds
                'metric_retention_days': 30,
                'alert_retention_days': 90
            }
        }

    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value"""
        keys = key.split('.')
        value = self.config

        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default

        return value

    def set(self, key: str, value: Any):
        """Set configuration value"""
        keys = key.split('.')
        config = self.config

        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]

        config[keys[-1]] = value

    def save(self):
        """Save configuration to file"""
        try:
            os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
            with open(self.config_path, 'w') as f:
                json.dump(self.config, f, indent=2)
        except Exception as e:
            logging.error(f"Failed to save config: {str(e)}")

# Global config instance
config = ConfigManager()

# ================================
# DATABASE CONNECTION HELPERS
# ================================

def get_postgres_engine():
    """Get PostgreSQL SQLAlchemy engine"""
    db_config = config.get('databases.postgres')
    connection_string = (
        f"postgresql://{db_config['user']}:{db_config['password']}"
        f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    )
    return create_engine(connection_string)

def get_mongo_client():
    """Get MongoDB client"""
    mongo_config = config.get('databases.mongodb')
    return pymongo.MongoClient(mongo_config['uri'])

def get_redis_client():
    """Get Redis client"""
    redis_config = config.get('databases.redis')
    return redis.from_url(redis_config['url'])

def get_mongo_collection(collection_name: str):
    """Get MongoDB collection"""
    client = get_mongo_client()
    db_name = config.get('databases.mongodb.database')
    return client[db_name][collection_name]

# ================================
# DATA PROCESSING HELPERS
# ================================

def clean_dataframe(df: pd.DataFrame, config: Dict[str, Any] = None) -> pd.DataFrame:
    """Clean and standardize DataFrame"""
    if config is None:
        config = {}

    df_clean = df.copy()

    # Remove duplicates
    if config.get('remove_duplicates', True):
        df_clean = df_clean.drop_duplicates()

    # Handle missing values
    missing_strategy = config.get('missing_strategy', 'fillna')
    if missing_strategy == 'fillna':
        # Fill numeric columns with median
        numeric_cols = df_clean.select_dtypes(include=[np.number]).columns
        df_clean[numeric_cols] = df_clean[numeric_cols].fillna(df_clean[numeric_cols].median())

        # Fill categorical columns with mode
        categorical_cols = df_clean.select_dtypes(include=['object']).columns
        for col in categorical_cols:
            mode_value = df_clean[col].mode()
            if len(mode_value) > 0:
                df_clean[col] = df_clean[col].fillna(mode_value[0])
            else:
                df_clean[col] = df_clean[col].fillna('Unknown')

    elif missing_strategy == 'drop':
        df_clean = df_clean.dropna()

    # Standardize column names
    if config.get('standardize_columns', True):
        df_clean.columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df_clean.columns]

    # Remove outliers (using IQR method)
    if config.get('remove_outliers', False):
        numeric_cols = df_clean.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            Q1 = df_clean[col].quantile(0.25)
            Q3 = df_clean[col].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            df_clean = df_clean[(df_clean[col] >= lower_bound) & (df_clean[col] <= upper_bound)]

    return df_clean

def encode_categorical_features(df: pd.DataFrame, columns: List[str] = None) -> Tuple[pd.DataFrame, Dict[str, LabelEncoder]]:
    """Encode categorical features"""
    if columns is None:
        columns = df.select_dtypes(include=['object']).columns

    df_encoded = df.copy()
    encoders = {}

    for col in columns:
        if col in df_encoded.columns:
            encoder = LabelEncoder()
            df_encoded[f"{col}_encoded"] = encoder.fit_transform(df_encoded[col].astype(str))
            encoders[col] = encoder

    return df_encoded, encoders

def calculate_rfm_features(df: pd.DataFrame, customer_col: str, date_col: str, amount_col: str) -> pd.DataFrame:
    """Calculate RFM (Recency, Frequency, Monetary) features"""
    current_date = df[date_col].max()

    rfm_df = df.groupby(customer_col).agg({
        date_col: lambda x: (current_date - x.max()).days,  # Recency
        amount_col: ['count', 'sum', 'mean']  # Frequency and Monetary
    }).round(2)

    # Flatten column names
    rfm_df.columns = ['recency', 'frequency', 'monetary_total', 'monetary_avg']
    rfm_df.reset_index(inplace=True)

    # Calculate RFM scores (1-5 scale)
    rfm_df['recency_score'] = pd.qcut(rfm_df['recency'].rank(method='first'), 5, labels=[5, 4, 3, 2, 1])
    rfm_df['frequency_score'] = pd.qcut(rfm_df['frequency'].rank(method='first'), 5, labels=[1, 2, 3, 4, 5])
    rfm_df['monetary_score'] = pd.qcut(rfm_df['monetary_total'].rank(method='first'), 5, labels=[1, 2, 3, 4, 5])

    # Calculate overall RFM score
    rfm_df['rfm_score'] = (
        rfm_df['recency_score'].astype(int) * 100 +
        rfm_df['frequency_score'].astype(int) * 10 +
        rfm_df['monetary_score'].astype(int)
    )

    return rfm_df

def create_time_features(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    """Create time-based features from datetime column"""
    df_time = df.copy()

    if date_col not in df_time.columns:
        return df_time

    # Ensure datetime type
    df_time[date_col] = pd.to_datetime(df_time[date_col])

    # Extract time features
    df_time[f"{date_col}_year"] = df_time[date_col].dt.year
    df_time[f"{date_col}_month"] = df_time[date_col].dt.month
    df_time[f"{date_col}_day"] = df_time[date_col].dt.day
    df_time[f"{date_col}_dayofweek"] = df_time[date_col].dt.dayofweek
    df_time[f"{date_col}_hour"] = df_time[date_col].dt.hour
    df_time[f"{date_col}_quarter"] = df_time[date_col].dt.quarter

    # Add categorical time features
    df_time[f"{date_col}_is_weekend"] = (df_time[f"{date_col}_dayofweek"] >= 5).astype(int)
    df_time[f"{date_col}_season"] = df_time[f"{date_col}_month"].map({
        12: 'Winter', 1: 'Winter', 2: 'Winter',
        3: 'Spring', 4: 'Spring', 5: 'Spring',
        6: 'Summer', 7: 'Summer', 8: 'Summer',
        9: 'Fall', 10: 'Fall', 11: 'Fall'
    })

    return df_time

# ================================
# ML HELPERS
# ================================

def evaluate_model_performance(y_true: np.ndarray, y_pred: np.ndarray, task_type: str = 'regression') -> Dict[str, float]:
    """Evaluate model performance with comprehensive metrics"""
    metrics = {}

    if task_type == 'regression':
        metrics['mae'] = float(mean_absolute_error(y_true, y_pred))
        metrics['mse'] = float(mean_squared_error(y_true, y_pred))
        metrics['rmse'] = float(np.sqrt(mean_squared_error(y_true, y_pred)))

        # R-squared
        ss_res = np.sum((y_true - y_pred) ** 2)
        ss_tot = np.sum((y_true - np.mean(y_true)) ** 2)
        metrics['r2'] = float(1 - (ss_res / ss_tot)) if ss_tot != 0 else 0.0

        # Mean Absolute Percentage Error
        mape = np.mean(np.abs((y_true - y_pred) / y_true)) * 100
        metrics['mape'] = float(mape) if not np.isnan(mape) else 0.0

    elif task_type == 'classification':
        from sklearn.metrics import precision_score, recall_score, f1_score, classification_report

        metrics['accuracy'] = float(accuracy_score(y_true, y_pred))
        metrics['precision'] = float(precision_score(y_true, y_pred, average='weighted'))
        metrics['recall'] = float(recall_score(y_true, y_pred, average='weighted'))
        metrics['f1'] = float(f1_score(y_true, y_pred, average='weighted'))

    return metrics

def create_feature_importance_report(model, feature_names: List[str], top_n: int = 20) -> Dict[str, Any]:
    """Create feature importance report"""
    if not hasattr(model, 'feature_importances_'):
        return {'error': 'Model does not support feature importance'}

    importances = model.feature_importances_
    feature_importance_df = pd.DataFrame({
        'feature': feature_names,
        'importance': importances
    }).sort_values('importance', ascending=False)

    top_features = feature_importance_df.head(top_n)

    return {
        'total_features': len(feature_names),
        'top_features': top_features.to_dict('records'),
        'importance_sum': float(importances.sum()),
        'top_10_importance_sum': float(top_features.head(10)['importance'].sum())
    }

def detect_data_drift(reference_data: pd.DataFrame, current_data: pd.DataFrame, threshold: float = 0.1) -> Dict[str, Any]:
    """Detect data drift between reference and current datasets"""
    drift_results = {}

    # Compare statistical properties
    for column in reference_data.select_dtypes(include=[np.number]).columns:
        if column in current_data.columns:
            ref_mean = reference_data[column].mean()
            curr_mean = current_data[column].mean()

            ref_std = reference_data[column].std()
            curr_std = current_data[column].std()

            # Calculate relative changes
            mean_change = abs(curr_mean - ref_mean) / abs(ref_mean) if ref_mean != 0 else 0
            std_change = abs(curr_std - ref_std) / abs(ref_std) if ref_std != 0 else 0

            drift_detected = mean_change > threshold or std_change > threshold

            drift_results[column] = {
                'drift_detected': drift_detected,
                'mean_change': float(mean_change),
                'std_change': float(std_change),
                'reference_mean': float(ref_mean),
                'current_mean': float(curr_mean),
                'reference_std': float(ref_std),
                'current_std': float(curr_std)
            }

    # Overall drift summary
    total_features = len(drift_results)
    drifted_features = len([r for r in drift_results.values() if r['drift_detected']])

    drift_results['summary'] = {
        'total_features_checked': total_features,
        'drifted_features': drifted_features,
        'drift_percentage': (drifted_features / total_features) * 100 if total_features > 0 else 0,
        'overall_drift_detected': drifted_features > 0,
        'threshold_used': threshold
    }

    return drift_results

# ================================
# STREAMING HELPERS
# ================================

def create_kafka_producer(config_overrides: Dict[str, Any] = None) -> KafkaProducer:
    """Create configured Kafka producer"""
    producer_config = {
        'bootstrap_servers': config.get('kafka.bootstrap_servers'),
        'value_serializer': lambda v: json.dumps(v, default=str).encode('utf-8'),
        'key_serializer': lambda k: k.encode('utf-8') if isinstance(k, str) else k,
        'acks': 'all',
        'retries': 3,
        'batch_size': 16384,
        'linger_ms': 10,
        'compression_type': 'snappy'
    }

    if config_overrides:
        producer_config.update(config_overrides)

    return KafkaProducer(**producer_config)

def create_kafka_consumer(topics: List[str], group_id: str, config_overrides: Dict[str, Any] = None) -> KafkaConsumer:
    """Create configured Kafka consumer"""
    consumer_config = {
        'bootstrap_servers': config.get('kafka.bootstrap_servers'),
        'group_id': group_id,
        'value_deserializer': lambda m: json.loads(m.decode('utf-8')),
        'key_deserializer': lambda k: k.decode('utf-8') if k else None,
        'auto_offset_reset': 'latest',
        'enable_auto_commit': True,
        'consumer_timeout_ms': 10000
    }

    if config_overrides:
        consumer_config.update(config_overrides)

    return KafkaConsumer(*topics, **consumer_config)

def send_to_kafka(topic: str, data: Dict[str, Any], key: str = None) -> bool:
    """Send data to Kafka topic"""
    try:
        producer = create_kafka_producer()

        # Add metadata
        enriched_data = {
            'timestamp': datetime.now().isoformat(),
            'source': 'airflow_pipeline',
            'data': data
        }

        future = producer.send(topic, key=key, value=enriched_data)
        producer.flush()
        producer.close()

        # Wait for result
        record_metadata = future.get(timeout=10)
        logging.info(f"Sent message to {topic}: partition={record_metadata.partition}, offset={record_metadata.offset}")

        return True

    except Exception as e:
        logging.error(f"Failed to send message to Kafka topic {topic}: {str(e)}")
        return False

def process_streaming_batch(messages: List[Dict[str, Any]], processor_function, batch_size: int = 100) -> List[Dict[str, Any]]:
    """Process streaming messages in batches"""
    processed_messages = []

    for i in range(0, len(messages), batch_size):
        batch = messages[i:i + batch_size]

        try:
            # Process batch
            batch_results = processor_function(batch)

            if isinstance(batch_results, list):
                processed_messages.extend(batch_results)
            else:
                processed_messages.append(batch_results)

        except Exception as e:
            logging.error(f"Failed to process batch {i//batch_size + 1}: {str(e)}")
            # Add error info to messages
            for msg in batch:
                msg['processing_error'] = str(e)
                msg['processed_at'] = datetime.now().isoformat()
                processed_messages.append(msg)

    return processed_messages

# ================================
# MONITORING HELPERS
# ================================

def create_system_health_check() -> Dict[str, Any]:
    """Perform comprehensive system health check"""
    health_status = {
        'timestamp': datetime.now().isoformat(),
        'overall_status': 'healthy',
        'services': {}
    }

    # Check PostgreSQL
    try:
        engine = get_postgres_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        health_status['services']['postgresql'] = {
            'status': 'healthy',
            'response_time': 'fast',
            'last_check': datetime.now().isoformat()
        }
    except Exception as e:
        health_status['services']['postgresql'] = {
            'status': 'unhealthy',
            'error': str(e),
            'last_check': datetime.now().isoformat()
        }
        health_status['overall_status'] = 'degraded'

    # Check MongoDB
    try:
        client = get_mongo_client()
        client.admin.command('ping')
        health_status['services']['mongodb'] = {
            'status': 'healthy',
            'response_time': 'fast',
            'last_check': datetime.now().isoformat()
        }
    except Exception as e:
        health_status['services']['mongodb'] = {
            'status': 'unhealthy',
            'error': str(e),
            'last_check': datetime.now().isoformat()
        }
        health_status['overall_status'] = 'degraded'

    # Check Redis
    try:
        redis_client = get_redis_client()
        redis_client.ping()
        health_status['services']['redis'] = {
            'status': 'healthy',
            'response_time': 'fast',
            'last_check': datetime.now().isoformat()
        }
    except Exception as e:
        health_status['services']['redis'] = {
            'status': 'unhealthy',
            'error': str(e),
            'last_check': datetime.now().isoformat()
        }
        health_status['overall_status'] = 'degraded'

    # Check Kafka
    try:
        producer = create_kafka_producer()
        producer.close()
        health_status['services']['kafka'] = {
            'status': 'healthy',
            'response_time': 'fast',
            'last_check': datetime.now().isoformat()
        }
    except Exception as e:
        health_status['services']['kafka'] = {
            'status': 'unhealthy',
            'error': str(e),
            'last_check': datetime.now().isoformat()
        }
        health_status['overall_status'] = 'degraded'

    # Calculate health score
    healthy_services = len([s for s in health_status['services'].values() if s['status'] == 'healthy'])
    total_services = len(health_status['services'])
    health_score = (healthy_services / total_services) * 100 if total_services > 0 else 0

    health_status['health_score'] = health_score

    if health_score < 50:
        health_status['overall_status'] = 'critical'
    elif health_score < 80:
        health_status['overall_status'] = 'degraded'

    return health_status

def send_alert_to_monitoring(alert_type: str, severity: str, message: str, metadata: Dict[str, Any] = None):
    """Send alert to monitoring system"""
    try:
        collection = get_mongo_collection('alerts')

        alert_doc = {
            'alert_id': str(uuid.uuid4()),
            'alert_type': alert_type,
            'severity': severity,
            'title': f'{alert_type} - {severity}',
            'message': message,
            'component': 'airflow_pipeline',
            'source': 'helpers',
            'triggered_at': datetime.now(),
            'is_active': True,
            'metadata': metadata or {}
        }

        collection.insert_one(alert_doc)

        # Also send to Kafka alerts topic
        kafka_topic = config.get('kafka.topics.alerts')
        if kafka_topic:
            send_to_kafka(kafka_topic, alert_doc)

        logging.info(f"Alert sent: {alert_type} - {severity}")

    except Exception as e:
        logging.error(f"Failed to send alert: {str(e)}")

def calculate_pipeline_metrics(dag_run_context: Dict[str, Any]) -> Dict[str, Any]:
    """Calculate pipeline performance metrics"""
    try:
        # Get task instances from context
        dag_run = dag_run_context.get('dag_run')
        if not dag_run:
            return {}

        task_instances = dag_run.get_task_instances()

        # Calculate metrics
        total_tasks = len(task_instances)
        successful_tasks = len([ti for ti in task_instances if ti.state == 'success'])
        failed_tasks = len([ti for ti in task_instances if ti.state == 'failed'])
        running_tasks = len([ti for ti in task_instances if ti.state == 'running'])

        # Calculate durations
        completed_tasks = [ti for ti in task_instances if ti.end_date and ti.start_date]
        task_durations = [(ti.end_date - ti.start_date).total_seconds() for ti in completed_tasks]

        metrics = {
            'dag_id': dag_run.dag_id,
            'run_id': dag_run.run_id,
            'execution_date': dag_run.execution_date.isoformat(),
            'total_tasks': total_tasks,
            'successful_tasks': successful_tasks,
            'failed_tasks': failed_tasks,
            'running_tasks': running_tasks,
            'success_rate': (successful_tasks / total_tasks) * 100 if total_tasks > 0 else 0,
            'avg_task_duration': np.mean(task_durations) if task_durations else 0,
            'total_pipeline_duration': sum(task_durations) if task_durations else 0,
            'calculated_at': datetime.now().isoformat()
        }

        return metrics

    except Exception as e:
        logging.error(f"Failed to calculate pipeline metrics: {str(e)}")
        return {}

# ================================
# UTILITY FUNCTIONS
# ================================

def generate_unique_id(prefix: str = "") -> str:
    """Generate unique ID"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    random_part = str(uuid.uuid4())[:8]
    return f"{prefix}{timestamp}_{random_part}" if prefix else f"{timestamp}_{random_part}"

def hash_data(data: Union[str, Dict, List]) -> str:
    """Generate hash for data"""
    if isinstance(data, (dict, list)):
        data_str = json.dumps(data, sort_keys=True, default=str)
    else:
        data_str = str(data)

    return hashlib.sha256(data_str.encode()).hexdigest()

def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """Safe division that handles zero division"""
    try:
        return float(numerator / denominator) if denominator != 0 else default
    except (TypeError, ValueError):
        return default

def format_bytes(bytes_value: int) -> str:
    """Format bytes to human readable format"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_value < 1024:
            return f"{bytes_value:.2f} {unit}"
        bytes_value /= 1024
    return f"{bytes_value:.2f} PB"

def retry_operation(operation, max_retries: int = 3, delay: float = 1.0, backoff_factor: float = 2.0):
    """Retry operation with exponential backoff"""
    import time

    for attempt in range(max_retries):
        try:
            return operation()
        except Exception as e:
            if attempt == max_retries - 1:
                raise e

            wait_time = delay * (backoff_factor ** attempt)
            logging.warning(f"Operation failed (attempt {attempt + 1}/{max_retries}), retrying in {wait_time:.2f}s: {str(e)}")
            time.sleep(wait_time)

def validate_data_schema(data: pd.DataFrame, expected_schema: Dict[str, str]) -> Dict[str, Any]:
    """Validate DataFrame against expected schema"""
    validation_results = {
        'is_valid': True,
        'errors': [],
        'warnings': []
    }

    # Check required columns
    missing_columns = set(expected_schema.keys()) - set(data.columns)
    if missing_columns:
        validation_results['is_valid'] = False
        validation_results['errors'].append(f"Missing required columns: {list(missing_columns)}")

    # Check data types
    for column, expected_type in expected_schema.items():
        if column in data.columns:
            actual_type = str(data[column].dtype)

            if expected_type == 'datetime' and not pd.api.types.is_datetime64_any_dtype(data[column]):
                validation_results['warnings'].append(f"Column '{column}' expected datetime, got {actual_type}")
            elif expected_type == 'numeric' and not pd.api.types.is_numeric_dtype(data[column]):
                validation_results['warnings'].append(f"Column '{column}' expected numeric, got {actual_type}")
            elif expected_type == 'string' and not pd.api.types.is_string_dtype(data[column]):
                validation_results['warnings'].append(f"Column '{column}' expected string, got {actual_type}")

    return validation_results

# ================================
# LOGGING HELPERS
# ================================

def setup_task_logger(task_id: str, log_level: str = "INFO") -> logging.Logger:
    """Setup logger for specific task"""
    logger = logging.getLogger(f"dss_pipeline.{task_id}")
    logger.setLevel(getattr(logging, log_level.upper()))

    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Add file handler if not exists
    if not logger.handlers:
        log_file = f"/opt/airflow/logs/dss_pipeline/{task_id}.log"
        os.makedirs(os.path.dirname(log_file), exist_ok=True)

        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Also add console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger

def log_task_metrics(task_id: str, metrics: Dict[str, Any]):
    """Log task performance metrics"""
    logger = setup_task_logger(task_id)

    logger.info(f"Task Metrics for {task_id}:")
    for key, value in metrics.items():
        logger.info(f"  {key}: {value}")

    # Also store in monitoring collection
    try:
        collection = get_mongo_collection('task_metrics')
        metric_doc = {
            'task_id': task_id,
            'timestamp': datetime.now(),
            'metrics': metrics
        }
        collection.insert_one(metric_doc)
    except Exception as e:
        logger.warning(f"Failed to store task metrics: {str(e)}")