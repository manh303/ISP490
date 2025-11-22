# -*- coding: utf-8 -*-
"""
ML Training Pipeline DAG
Tự động train 3 mô hình ML hàng ngày:
1. Sentiment Classification Model
2. Product Clustering Model  
3. Demand Prediction Model (tuỳ chọn)
"""

import os
import sys
import json
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.utils.task_group import TaskGroup
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator

import logging

logger = logging.getLogger(__name__)

# ===========================
# DAG Configuration
# ===========================

default_args = {
    'owner': 'ml_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['admin@ecommerce.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=3),
}

dag = DAG(
    'ml_training_pipeline',
    default_args=default_args,
    description='Automated ML Model Training Pipeline - Daily',
    schedule_interval='0 1 * * *',  # Run at 1:00 AM daily
    catchup=False,
    tags=['ml', 'training', 'daily'],
    max_active_runs=1,
)

# ===========================
# Configuration
# ===========================

# Use /opt/airflow for directories that Airflow user needs to write to
# /app/ml is read-only for Airflow container
ML_PROJECT_PATH = '/app/ml'  # Path to ML project (Airflow writable)
MODELS_OUTPUT_DIR = '/app/ml/models/ml-models'
DATA_DIR = '/app/ml/data'
LOGS_DIR = '/app/ml/logs'

def setup_directories(**context):
    """Create necessary directories at runtime"""
    os.makedirs(MODELS_OUTPUT_DIR, exist_ok=True)
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(LOGS_DIR, exist_ok=True)
    logger.info("✅ Directories created successfully")

# ===========================
# Python Functions
# ===========================

def check_data_extraction(**context):
    """Check if data extraction was successful"""
    logger.info("🔍 Checking data extraction status...")
    
    try:
        # Check if data files exist
        sentiment_data = Path(DATA_DIR) / 'sentiment_analysis' / 'raw_sentiment_data.csv'
        clustering_data = Path(DATA_DIR) / 'product_clustering' / 'raw_clustering_data.csv'
        
        if not sentiment_data.exists():
            logger.error(f"❌ Sentiment data not found: {sentiment_data}")
            return False
        
        if not clustering_data.exists():
            logger.error(f"❌ Clustering data not found: {clustering_data}")
            return False
        
        logger.info("✅ All data files exist")
        return True
    
    except Exception as e:
        logger.error(f"❌ Data check failed: {e}")
        raise AirflowException(f"Data extraction check failed: {e}")


def validate_trained_models(**context):
    """Validate all trained models"""
    logger.info("🔍 Validating trained models...")
    
    try:
        required_models = [
            'sentiment_classifier.pkl',
            'sentiment_tfidf_vectorizer.pkl',
            'sentiment_label_encoder.pkl',
            'recommendation_kmeans.pkl',
            'clustering_scaler.pkl',
        ]
        
        models_dir = Path(MODELS_OUTPUT_DIR)
        missing_models = []
        
        for model_file in required_models:
            model_path = models_dir / model_file
            if not model_path.exists():
                missing_models.append(model_file)
                logger.warning(f"⚠️  Model not found: {model_file}")
            else:
                size_mb = model_path.stat().st_size / (1024 * 1024)
                logger.info(f"✅ {model_file} ({size_mb:.2f} MB)")
        
        if missing_models:
            logger.error(f"❌ Missing models: {missing_models}")
            return False
        
        logger.info("✅ All models validated successfully")
        return True
    
    except Exception as e:
        logger.error(f"❌ Model validation failed: {e}")
        raise AirflowException(f"Model validation failed: {e}")


def get_model_metrics(**context):
    """Get metrics from trained models"""
    logger.info("📊 Retrieving model metrics...")
    
    try:
        metrics_dir = Path(LOGS_DIR) / 'metrics'
        metrics_data = {}
        
        # Read sentiment metrics
        sentiment_metrics_file = metrics_dir / 'sentiment_metrics.json'
        if sentiment_metrics_file.exists():
            with open(sentiment_metrics_file, 'r') as f:
                metrics_data['sentiment'] = json.load(f)
                logger.info(f"✅ Sentiment metrics: {metrics_data['sentiment']}")
        
        # Read clustering metrics
        clustering_metrics_file = metrics_dir / 'clustering_metrics.json'
        if clustering_metrics_file.exists():
            with open(clustering_metrics_file, 'r') as f:
                metrics_data['clustering'] = json.load(f)
                logger.info(f"✅ Clustering metrics: {metrics_data['clustering']}")
        
        # Push metrics to XCom for downstream tasks
        context['task_instance'].xcom_push(key='model_metrics', value=metrics_data)
        
        logger.info(f"✅ Metrics retrieved: {metrics_data}")
        return metrics_data
    
    except Exception as e:
        logger.error(f"❌ Failed to get metrics: {e}")
        raise AirflowException(f"Metrics retrieval failed: {e}")


def notify_completion(**context):
    """Send completion notification"""
    logger.info("📧 Sending completion notification...")
    
    try:
        task_instance = context['task_instance']
        dag_run = context['dag_run']
        
        metrics = task_instance.xcom_pull(task_ids='get_model_metrics', key='model_metrics')
        
        notification = {
            'status': 'success',
            'dag_run_id': dag_run.run_id,
            'execution_date': context['execution_date'].isoformat(),
            'models_trained': 2,
            'metrics': metrics,
            'timestamp': datetime.now().isoformat()
        }
        
        # Save notification
        notification_file = Path(LOGS_DIR) / f"training_complete_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(notification_file, 'w') as f:
            json.dump(notification, f, indent=2)
        
        logger.info(f"✅ Completion notification saved: {notification_file}")
        
        # In production, send email/slack notification
        # send_email(recipients=['admin@ecommerce.com'], subject=f"ML Training Complete - {dag_run.run_id}", ...)
        
        return notification
    
    except Exception as e:
        logger.error(f"❌ Failed to send notification: {e}")
        # Don't fail the DAG for notification errors
        return {'status': 'notification_failed', 'error': str(e)}


def handle_failure(**context):
    """Handle pipeline failure"""
    logger.error("❌ ML Training Pipeline Failed!")
    
    try:
        dag_run = context['dag_run']
        exception = context.get('exception')
        
        error_log = {
            'status': 'failed',
            'dag_run_id': dag_run.run_id,
            'execution_date': context['execution_date'].isoformat(),
            'error': str(exception),
            'timestamp': datetime.now().isoformat()
        }
        
        # Save error log
        error_file = Path(LOGS_DIR) / f"training_error_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(error_file, 'w') as f:
            json.dump(error_log, f, indent=2)
        
        logger.error(f"Error log saved: {error_file}")
        
    except Exception as e:
        logger.error(f"Failed to handle error: {e}")


# ===========================
# DAG Tasks
# ===========================

# Start task
start = DummyOperator(
    task_id='start',
    dag=dag,
)

# Task 0: Setup directories
setup_task = PythonOperator(
    task_id='setup_directories',
    python_callable=setup_directories,
    dag=dag,
)

# Task 1: Data Extraction
extract_data = BashOperator(
    task_id='extract_data',
    bash_command=f'cd {ML_PROJECT_PATH} && python 1_data_extraction.py',
    env={
        'PYTHONPATH': ML_PROJECT_PATH,
    },
    dag=dag,
)

# Task 2: Check data extraction
check_data = PythonOperator(
    task_id='check_data',
    python_callable=check_data_extraction,
    dag=dag,
)

# Task 3: Train Sentiment Classifier
train_sentiment = BashOperator(
    task_id='train_sentiment_classifier',
    bash_command=f'cd {ML_PROJECT_PATH} && python train_sentiment_classifier.py',
    env={
        'PYTHONPATH': ML_PROJECT_PATH,
    },
    dag=dag,
)

# Task 4: Train Product Clustering
train_clustering = BashOperator(
    task_id='train_product_clustering',
    bash_command=f'cd {ML_PROJECT_PATH} && python train_product_clustering.py',
    env={
        'PYTHONPATH': ML_PROJECT_PATH,
    },
    dag=dag,
)

# Task 5: Test Models
test_models = BashOperator(
    task_id='test_models',
    bash_command=f'cd {ML_PROJECT_PATH} && python test_models.py',
    env={
        'PYTHONPATH': ML_PROJECT_PATH,
    },
    dag=dag,
)

# Task 6: Validate Models
validate_models = PythonOperator(
    task_id='validate_models',
    python_callable=validate_trained_models,
    dag=dag,
)

# Task 7: Get Model Metrics
get_metrics = PythonOperator(
    task_id='get_model_metrics',
    python_callable=get_model_metrics,
    dag=dag,
)

# Task 8: Notify Completion
notify = PythonOperator(
    task_id='notify_completion',
    python_callable=notify_completion,
    on_failure_callback=handle_failure,
    dag=dag,
)

# End task
end = DummyOperator(
    task_id='end',
    dag=dag,
)

# ===========================
# DAG Dependencies
# ===========================

# Linear pipeline: sequential execution
start >> setup_task >> extract_data >> check_data

# Parallel training: both models train simultaneously
check_data >> [train_sentiment, train_clustering]

# After both models are trained
[train_sentiment, train_clustering] >> test_models >> validate_models

# Get metrics and notify
validate_models >> get_metrics >> notify >> end

# ===========================
# DAG Documentation
# ===========================

dag.doc_md = """
## ML Training Pipeline DAG

### Overview
Automated daily training pipeline for ML models.

### Models Trained
1. **Sentiment Classification** - Classify review sentiment (Positive/Negative/Neutral)
   - Algorithm: Random Forest Classifier
   - Data: Review text from DWH
   - Output: sentiment_classifier.pkl

2. **Product Clustering** - Segment products into clusters
   - Algorithm: KMeans
   - Data: Product features from DWH
   - Output: recommendation_kmeans.pkl

### Schedule
- **Frequency**: Daily at 1:00 AM (UTC)
- **Catchup**: Disabled
- **Max Active Runs**: 1

### Tasks
1. **extract_data** - Extract training data from DWH
2. **check_data** - Verify data files exist
3. **train_sentiment_classifier** - Train sentiment model (parallel)
4. **train_product_clustering** - Train clustering model (parallel)
5. **test_models** - Test trained models
6. **validate_models** - Validate model files
7. **get_model_metrics** - Retrieve performance metrics
8. **notify_completion** - Send completion notification

### Output Locations
- **Models**: `/app/ml/models/ml-models/`
- **Metrics**: `/app/ml/logs/metrics/`
- **Logs**: `/app/ml/logs/ml_pipeline.log`

### Monitoring
- Check logs: `tail -f /app/ml/logs/ml_pipeline.log`
- Monitor Airflow UI: http://localhost:8080
- Check model status: `GET /api/v1/ml/models`

### Troubleshooting
- If data extraction fails: Check DWH connection in config.yaml
- If training fails: Check model hyperparameters in config.yaml
- If validation fails: Verify model files in output directory

### References
- Training Guide: `/app/ml/TRAINING_GUIDE.md`
- API Documentation: `/app/backend/ML_API_DOCUMENTATION.md`
"""

# ===========================
# DAG Configuration
# ===========================

if __name__ == "__main__":
    dag.cli()
