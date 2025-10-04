#!/usr/bin/env python3
"""
Constants and Configuration for E-commerce DSS Pipeline
===================================================
Centralized constants, enums, and configuration values
"""

from enum import Enum
from datetime import timedelta

# ================================
# PIPELINE CONSTANTS
# ================================

# DAG Configuration
DAG_CONFIG = {
    'DEFAULT_RETRIES': 2,
    'DEFAULT_RETRY_DELAY': timedelta(minutes=5),
    'DEFAULT_EMAIL_ON_FAILURE': True,
    'DEFAULT_EMAIL_ON_RETRY': False,
    'MAX_ACTIVE_RUNS': 1,
    'CATCHUP': False,
    'DEPENDS_ON_PAST': False
}

# Schedule Intervals
SCHEDULE_INTERVALS = {
    'COMPREHENSIVE_PIPELINE': '0 */4 * * *',  # Every 4 hours
    'STREAMING_MONITOR': '*/15 * * * *',       # Every 15 minutes
    'DATA_QUALITY_CHECK': '0 */2 * * *',       # Every 2 hours
    'ML_TRAINING': '0 2 * * 0',                # Weekly on Sunday at 2 AM
    'BACKUP_PIPELINE': '0 3 * * *',            # Daily at 3 AM
    'CLEANUP_PIPELINE': '0 4 * * 0',           # Weekly on Sunday at 4 AM
    'HEALTH_CHECK': '*/5 * * * *',             # Every 5 minutes
    'REPORT_GENERATION': '0 6 * * *'           # Daily at 6 AM
}

# Task Timeouts (in seconds)
TASK_TIMEOUTS = {
    'DATA_COLLECTION': 1800,      # 30 minutes
    'DATA_PROCESSING': 3600,      # 1 hour
    'ML_TRAINING': 7200,          # 2 hours
    'ML_PREDICTION': 1800,        # 30 minutes
    'DATA_QUALITY': 900,          # 15 minutes
    'BACKUP': 3600,               # 1 hour
    'CLEANUP': 1800,              # 30 minutes
    'MONITORING': 300,            # 5 minutes
    'REPORTING': 1800             # 30 minutes
}

# ================================
# DATABASE CONSTANTS
# ================================

# Connection IDs
CONNECTION_IDS = {
    'POSTGRES': 'postgres_default',
    'MONGODB': 'mongo_default',
    'REDIS': 'redis_default',
    'KAFKA': 'kafka_default',
    'SLACK': 'slack_default',
    'EMAIL': 'email_default'
}

# Database Configuration
DB_CONFIG = {
    'POSTGRES': {
        'HOST': 'postgres',
        'PORT': 5432,
        'DATABASE': 'ecommerce_dss',
        'SCHEMA': 'public',
        'POOL_SIZE': 20,
        'MAX_OVERFLOW': 30,
        'POOL_TIMEOUT': 30
    },
    'MONGODB': {
        'HOST': 'mongodb',
        'PORT': 27017,
        'DATABASE': 'ecommerce_dss',
        'MAX_POOL_SIZE': 100,
        'SERVER_SELECTION_TIMEOUT': 30000
    },
    'REDIS': {
        'HOST': 'redis',
        'PORT': 6379,
        'DATABASE': 0,
        'MAX_CONNECTIONS': 20
    }
}

# ================================
# KAFKA CONSTANTS
# ================================

# Kafka Configuration
KAFKA_CONFIG = {
    'BOOTSTRAP_SERVERS': 'kafka:29092',
    'CLIENT_ID': 'airflow_dss_pipeline',
    'REQUEST_TIMEOUT_MS': 30000,
    'RETRY_BACKOFF_MS': 1000,
    'RECONNECT_BACKOFF_MS': 50,
    'MAX_POLL_RECORDS': 500,
    'FETCH_MIN_BYTES': 1024,
    'BATCH_SIZE': 16384,
    'LINGER_MS': 10,
    'COMPRESSION_TYPE': 'snappy',
    'ACKS': 'all',
    'RETRIES': 3
}

# Kafka Topics
KAFKA_TOPICS = {
    'PRODUCTS_STREAM': 'ecommerce.products.stream',
    'CUSTOMERS_STREAM': 'ecommerce.customers.stream',
    'ORDERS_STREAM': 'ecommerce.orders.stream',
    'ANALYTICS_STREAM': 'ecommerce.analytics.stream',
    'ML_PREDICTIONS': 'ecommerce.ml.predictions',
    'ALERTS': 'ecommerce.alerts',
    'SYSTEM_METRICS': 'ecommerce.system.metrics',
    'DATA_QUALITY': 'ecommerce.data.quality',
    'PIPELINE_STATUS': 'ecommerce.pipeline.status'
}

# Topic Configurations
TOPIC_CONFIGS = {
    'DEFAULT_PARTITIONS': 3,
    'DEFAULT_REPLICATION_FACTOR': 1,
    'RETENTION_MS': 604800000,  # 7 days
    'CLEANUP_POLICY': 'delete'
}

# ================================
# ML CONSTANTS
# ================================

# Model Configurations
ML_MODELS = {
    'CUSTOMER_LIFETIME_VALUE': {
        'TYPE': 'random_forest',
        'TASK': 'regression',
        'PARAMETERS': {
            'n_estimators': 100,
            'random_state': 42,
            'max_depth': 10,
            'min_samples_split': 5,
            'min_samples_leaf': 2
        },
        'FEATURES': [
            'total_orders', 'average_order_value', 'customer_age_days',
            'days_since_last_order', 'active_last_30d', 'tier_encoded',
            'avg_days_between_orders', 'spending_velocity'
        ],
        'TARGET': 'total_spent'
    },
    'CHURN_PREDICTION': {
        'TYPE': 'gradient_boosting',
        'TASK': 'classification',
        'PARAMETERS': {
            'n_estimators': 100,
            'learning_rate': 0.1,
            'max_depth': 6,
            'random_state': 42
        },
        'FEATURES': [
            'recency', 'frequency', 'monetary_total',
            'days_since_last_order', 'avg_order_value', 'total_orders'
        ],
        'TARGET': 'is_churned'
    },
    'PRODUCT_RECOMMENDATION': {
        'TYPE': 'collaborative_filtering',
        'TASK': 'recommendation',
        'PARAMETERS': {
            'n_factors': 50,
            'n_epochs': 20,
            'lr_all': 0.005,
            'reg_all': 0.02
        }
    }
}

# Feature Engineering
FEATURE_ENGINEERING = {
    'TIME_WINDOWS': {
        'SHORT': timedelta(days=7),
        'MEDIUM': timedelta(days=30),
        'LONG': timedelta(days=90)
    },
    'AGGREGATION_FUNCTIONS': ['sum', 'mean', 'count', 'min', 'max', 'std'],
    'CATEGORICAL_ENCODING': 'label',  # label, onehot, target
    'SCALING_METHOD': 'standard',     # standard, minmax, robust
    'MISSING_VALUE_STRATEGY': 'median'  # mean, median, mode, drop
}

# Model Performance Thresholds
ML_PERFORMANCE_THRESHOLDS = {
    'REGRESSION': {
        'MIN_R2': 0.6,
        'MAX_RMSE': 1000,
        'MAX_MAE': 500
    },
    'CLASSIFICATION': {
        'MIN_ACCURACY': 0.8,
        'MIN_PRECISION': 0.75,
        'MIN_RECALL': 0.75,
        'MIN_F1': 0.75
    },
    'RECOMMENDATION': {
        'MIN_PRECISION_AT_K': 0.1,
        'MIN_RECALL_AT_K': 0.05
    }
}

# ================================
# DATA QUALITY CONSTANTS
# ================================

# Data Quality Thresholds
DATA_QUALITY_THRESHOLDS = {
    'OVERALL_QUALITY_THRESHOLD': 0.8,
    'CRITICAL_QUALITY_THRESHOLD': 0.6,
    'COMPLETENESS_THRESHOLD': 0.9,
    'UNIQUENESS_THRESHOLD': 0.95,
    'VALIDITY_THRESHOLD': 0.95,
    'CONSISTENCY_THRESHOLD': 0.9
}

# Data Quality Checks
DATA_QUALITY_CHECKS = {
    'COMPLETENESS': {
        'NULL_PERCENTAGE': 0.05,      # Max 5% nulls
        'EMPTY_STRING_PERCENTAGE': 0.02  # Max 2% empty strings
    },
    'UNIQUENESS': {
        'DUPLICATE_PERCENTAGE': 0.01   # Max 1% duplicates
    },
    'VALIDITY': {
        'EMAIL_FORMAT': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
        'PHONE_FORMAT': r'^\+?[\d\s\-\(\)]+$',
        'DATE_FORMAT': '%Y-%m-%d',
        'POSITIVE_VALUES': ['price', 'quantity', 'amount']
    },
    'CONSISTENCY': {
        'CROSS_TABLE_REFERENCES': {
            'orders.customer_id': 'customers.customer_id',
            'order_items.order_id': 'orders.order_id',
            'order_items.product_id': 'products.product_id'
        }
    }
}

# ================================
# MONITORING CONSTANTS
# ================================

# Alert Severities
class AlertSeverity(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

# Alert Types
class AlertType(Enum):
    SYSTEM_HEALTH = "system_health"
    DATA_QUALITY = "data_quality"
    PIPELINE_FAILURE = "pipeline_failure"
    ML_PERFORMANCE = "ml_performance"
    RESOURCE_USAGE = "resource_usage"
    SECURITY = "security"
    BUSINESS_METRIC = "business_metric"

# Monitoring Thresholds
MONITORING_THRESHOLDS = {
    'SYSTEM_HEALTH': {
        'RESPONSE_TIME_MS': 5000,
        'ERROR_RATE_PERCENTAGE': 5,
        'CPU_USAGE_PERCENTAGE': 80,
        'MEMORY_USAGE_PERCENTAGE': 85,
        'DISK_USAGE_PERCENTAGE': 90
    },
    'PIPELINE': {
        'TASK_FAILURE_RATE': 0.1,
        'PIPELINE_DURATION_HOURS': 6,
        'DATA_PROCESSING_LAG_HOURS': 2
    },
    'BUSINESS_METRICS': {
        'REVENUE_DROP_PERCENTAGE': 20,
        'ORDER_COUNT_DROP_PERCENTAGE': 30,
        'CUSTOMER_ACQUISITION_DROP_PERCENTAGE': 40
    }
}

# Notification Channels
NOTIFICATION_CHANNELS = {
    'EMAIL': {
        'ENABLED': True,
        'RECIPIENTS': ['admin@ecommerce-dss.com', 'alerts@ecommerce-dss.com'],
        'SENDER': 'noreply@ecommerce-dss.com'
    },
    'SLACK': {
        'ENABLED': True,
        'CHANNELS': {
            'GENERAL': '#dss-alerts',
            'CRITICAL': '#dss-critical',
            'DATA_QUALITY': '#dss-data-quality',
            'ML_ALERTS': '#dss-ml-alerts'
        }
    },
    'WEBHOOKS': {
        'ENABLED': True,
        'ENDPOINTS': [
            'http://monitoring.internal/webhooks/dss-alerts'
        ]
    }
}

# ================================
# FILE PATHS AND DIRECTORIES
# ================================

# Directory Paths
DIRECTORIES = {
    'DATA_ROOT': '/app/data',
    'MODELS': '/app/models',
    'LOGS': '/opt/airflow/logs',
    'CONFIG': '/opt/airflow/config',
    'BACKUPS': '/app/data/backups',
    'TEMP': '/tmp/dss_pipeline',
    'PROCESSED_DATA': '/app/data/processed',
    'RAW_DATA': '/app/data/raw',
    'FEATURES': '/app/data/features',
    'REPORTS': '/app/data/reports'
}

# File Patterns
FILE_PATTERNS = {
    'MODEL_FILES': '*.pkl',
    'DATA_FILES': '*.csv',
    'CONFIG_FILES': '*.json',
    'LOG_FILES': '*.log',
    'BACKUP_FILES': '*_backup_*.{csv,json}'
}

# ================================
# BUSINESS LOGIC CONSTANTS
# ================================

# Customer Segmentation
CUSTOMER_SEGMENTS = {
    'VIP': {
        'MIN_TOTAL_SPENT': 10000,
        'MIN_ORDERS': 20,
        'MAX_DAYS_SINCE_LAST_ORDER': 30
    },
    'LOYAL': {
        'MIN_TOTAL_SPENT': 5000,
        'MIN_ORDERS': 10,
        'MAX_DAYS_SINCE_LAST_ORDER': 60
    },
    'REGULAR': {
        'MIN_TOTAL_SPENT': 1000,
        'MIN_ORDERS': 3,
        'MAX_DAYS_SINCE_LAST_ORDER': 90
    },
    'NEW': {
        'MAX_DAYS_SINCE_REGISTRATION': 30,
        'MAX_ORDERS': 2
    },
    'AT_RISK': {
        'MIN_DAYS_SINCE_LAST_ORDER': 90,
        'MIN_HISTORICAL_ORDERS': 5
    },
    'CHURNED': {
        'MIN_DAYS_SINCE_LAST_ORDER': 180
    }
}

# Product Categories
PRODUCT_CATEGORIES = [
    'Electronics', 'Clothing', 'Home & Garden', 'Sports & Outdoors',
    'Books', 'Beauty & Health', 'Toys & Games', 'Automotive',
    'Food & Beverages', 'Office Supplies', 'Pet Supplies', 'Other'
]

# Order Statuses
class OrderStatus(Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    SHIPPED = "shipped"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"
    REFUNDED = "refunded"

# Payment Methods
PAYMENT_METHODS = [
    'credit_card', 'debit_card', 'paypal', 'bank_transfer',
    'cash_on_delivery', 'digital_wallet', 'crypto', 'other'
]

# ================================
# PERFORMANCE CONSTANTS
# ================================

# Batch Sizes
BATCH_SIZES = {
    'SMALL': 100,
    'MEDIUM': 1000,
    'LARGE': 10000,
    'EXTRA_LARGE': 100000
}

# Memory Limits (in MB)
MEMORY_LIMITS = {
    'SMALL_TASK': 512,
    'MEDIUM_TASK': 1024,
    'LARGE_TASK': 2048,
    'ML_TASK': 4096,
    'DATA_PROCESSING_TASK': 8192
}

# Parallelism Settings
PARALLELISM = {
    'MAX_WORKER_PROCESSES': 4,
    'MAX_CONCURRENT_TASKS': 8,
    'THREAD_POOL_SIZE': 16
}

# ================================
# API CONSTANTS
# ================================

# HTTP Status Codes
HTTP_STATUS = {
    'SUCCESS': 200,
    'CREATED': 201,
    'NO_CONTENT': 204,
    'BAD_REQUEST': 400,
    'UNAUTHORIZED': 401,
    'FORBIDDEN': 403,
    'NOT_FOUND': 404,
    'INTERNAL_ERROR': 500,
    'SERVICE_UNAVAILABLE': 503
}

# API Rate Limits
API_RATE_LIMITS = {
    'REQUESTS_PER_MINUTE': 1000,
    'REQUESTS_PER_HOUR': 10000,
    'BURST_LIMIT': 100
}

# Request Timeouts (in seconds)
REQUEST_TIMEOUTS = {
    'CONNECTION': 10,
    'READ': 30,
    'TOTAL': 60
}

# ================================
# SECURITY CONSTANTS
# ================================

# Encryption Settings
ENCRYPTION = {
    'ALGORITHM': 'AES-256-GCM',
    'KEY_LENGTH': 32,
    'IV_LENGTH': 16
}

# Password Requirements
PASSWORD_POLICY = {
    'MIN_LENGTH': 12,
    'REQUIRE_UPPERCASE': True,
    'REQUIRE_LOWERCASE': True,
    'REQUIRE_NUMBERS': True,
    'REQUIRE_SYMBOLS': True,
    'MAX_AGE_DAYS': 90
}

# Session Settings
SESSION_CONFIG = {
    'TIMEOUT_MINUTES': 30,
    'CLEANUP_INTERVAL_MINUTES': 60,
    'MAX_SESSIONS_PER_USER': 5
}

# ================================
# DEPLOYMENT CONSTANTS
# ================================

# Environment Types
class Environment(Enum):
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    TESTING = "testing"

# Resource Quotas
RESOURCE_QUOTAS = {
    'DEVELOPMENT': {
        'MAX_CPU': '2000m',
        'MAX_MEMORY': '4Gi',
        'MAX_STORAGE': '50Gi'
    },
    'STAGING': {
        'MAX_CPU': '4000m',
        'MAX_MEMORY': '8Gi',
        'MAX_STORAGE': '100Gi'
    },
    'PRODUCTION': {
        'MAX_CPU': '8000m',
        'MAX_MEMORY': '16Gi',
        'MAX_STORAGE': '500Gi'
    }
}

# Health Check Settings
HEALTH_CHECK = {
    'INTERVAL_SECONDS': 30,
    'TIMEOUT_SECONDS': 10,
    'RETRIES': 3,
    'START_PERIOD_SECONDS': 60
}

# ================================
# LOGGING CONSTANTS
# ================================

# Log Levels
class LogLevel(Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

# Log Formats
LOG_FORMATS = {
    'STANDARD': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'DETAILED': '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s - %(message)s',
    'JSON': '{"timestamp": "%(asctime)s", "logger": "%(name)s", "level": "%(levelname)s", "message": "%(message)s"}'
}

# Log Retention
LOG_RETENTION = {
    'DAYS': 30,
    'MAX_SIZE_MB': 100,
    'BACKUP_COUNT': 5
}

# ================================
# VALIDATION CONSTANTS
# ================================

# Data Validation Rules
VALIDATION_RULES = {
    'EMAIL_REGEX': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
    'PHONE_REGEX': r'^\+?[\d\s\-\(\)]{10,}$',
    'URL_REGEX': r'^https?://[^\s/$.?#].[^\s]*$',
    'UUID_REGEX': r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
    'MIN_STRING_LENGTH': 1,
    'MAX_STRING_LENGTH': 255,
    'MIN_PRICE': 0.01,
    'MAX_PRICE': 999999.99,
    'MIN_QUANTITY': 1,
    'MAX_QUANTITY': 10000
}

# ================================
# METADATA CONSTANTS
# ================================

# Pipeline Metadata
PIPELINE_METADATA = {
    'VERSION': '2.0.0',
    'AUTHOR': 'E-commerce DSS Team',
    'DESCRIPTION': 'Comprehensive E-commerce Decision Support System Pipeline',
    'LICENSE': 'MIT',
    'CREATED_DATE': '2024-01-01',
    'LAST_UPDATED': '2024-12-24',
    'DOCUMENTATION_URL': 'https://docs.ecommerce-dss.com',
    'SUPPORT_EMAIL': 'support@ecommerce-dss.com'
}

# Tags for DAGs
DAG_TAGS = {
    'COMPREHENSIVE': ['ecommerce', 'dss', 'comprehensive', 'ml', 'streaming'],
    'DATA_COLLECTION': ['ecommerce', 'data-collection', 'etl'],
    'DATA_PROCESSING': ['ecommerce', 'data-processing', 'transformation'],
    'ML_PIPELINE': ['ecommerce', 'machine-learning', 'ml', 'prediction'],
    'MONITORING': ['ecommerce', 'monitoring', 'health-check', 'alerts'],
    'BACKUP': ['ecommerce', 'backup', 'maintenance'],
    'STREAMING': ['ecommerce', 'streaming', 'real-time', 'kafka']
}