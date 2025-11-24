# -*- coding: utf-8 -*-
"""
ML Training & Inference Pipeline DAG
Tự động train và run inference cho 3 mô hình ML:
1. Sentiment Analysis Model (TF-IDF + Logistic Regression)
2. Product Recommendation Model (Content-based TF-IDF)
3. Price Prediction Model (Random Forest Regressor)

Flow:
- Phase 1: Train 3 models song song
- Phase 2: Run batch inference/predictions song song
- Phase 3: Validate và thông báo
"""

import os
import json
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowException

import logging

logger = logging.getLogger(__name__)

# ===========================
# DAG Configuration
# ===========================

default_args = {
    'owner': 'ml_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=4),
}

# ===========================
# Path Configuration
# ===========================

ML_PROJECT_PATH = '/app/ml'
MODELS_OUTPUT_DIR = '/app/ml/models'
LOGS_DIR = '/app/ml/logs'

# ===========================
# Python Helper Functions
# ===========================

def setup_directories(**context):
    """Create necessary directories at runtime"""
    os.makedirs(MODELS_OUTPUT_DIR, exist_ok=True)
    os.makedirs(LOGS_DIR, exist_ok=True)
    logger.info("✅ Directories created successfully")


def check_dwh_data(**context):
    """Check if DWH has sufficient data for training"""
    logger.info("🔍 Checking DWH data availability...")
    
    try:
        import psycopg2
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            raise AirflowException("DATABASE_URL not set")
        
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        
        # Check fact_review for sentiment training
        cur.execute("SELECT COUNT(*) FROM dwh.fact_review WHERE review_body IS NOT NULL")
        review_count = cur.fetchone()[0]
        logger.info(f"  Reviews available: {review_count}")
        
        # Check dim_product for recommendations
        cur.execute("SELECT COUNT(*) FROM dwh.dim_product WHERE product_key IS NOT NULL")
        product_count = cur.fetchone()[0]
        logger.info(f"  Products available: {product_count}")
        
        # Check fact_product_daily for price training
        cur.execute("SELECT COUNT(*) FROM dwh.fact_product_daily WHERE avg_price IS NOT NULL")
        price_records = cur.fetchone()[0]
        logger.info(f"  Price records available: {price_records}")
        
        cur.close()
        conn.close()
        
        # Minimum thresholds
        if review_count < 100:
            raise AirflowException(f"Insufficient reviews: {review_count} < 100")
        if product_count < 50:
            raise AirflowException(f"Insufficient products: {product_count} < 50")
        if price_records < 100:
            raise AirflowException(f"Insufficient price records: {price_records} < 100")
        
        logger.info("✅ DWH data check passed")
        return True
        
    except Exception as e:
        logger.error(f"❌ DWH data check failed: {e}")
        raise


def validate_trained_models(**context):
    """Validate all trained models exist"""
    logger.info("🔍 Validating trained models...")
    
    try:
        required_models = [
            'sentiment_tfidf_logreg_v1.0.pkl',
            'content_recommender_tfidf_v1.0.pkl',
            'price_forecast_rf_v1.0.pkl',
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
            raise AirflowException(f"Missing models: {missing_models}")
        
        logger.info("✅ All models validated successfully")
        return True
    
    except Exception as e:
        logger.error(f"❌ Model validation failed: {e}")
        raise


def validate_ml_results(**context):
    """Validate ML results in database"""
    logger.info("🔍 Validating ML results in database...")
    
    try:
        import psycopg2
        db_url = os.getenv("DATABASE_URL")
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        
        # Check dim_ml_model
        cur.execute("SELECT COUNT(*) FROM ml.dim_ml_model")
        model_count = cur.fetchone()[0]
        logger.info(f"  Models registered: {model_count}")
        
        # Check fact_review_sentiment
        cur.execute("SELECT COUNT(*) FROM ml.fact_review_sentiment")
        sentiment_count = cur.fetchone()[0]
        logger.info(f"  Sentiment predictions: {sentiment_count}")
        
        # Check fact_product_recommendation
        cur.execute("SELECT COUNT(*) FROM ml.fact_product_recommendation")
        rec_count = cur.fetchone()[0]
        logger.info(f"  Recommendations: {rec_count}")
        
        # Check fact_price_prediction
        cur.execute("SELECT COUNT(*) FROM ml.fact_price_prediction")
        price_count = cur.fetchone()[0]
        logger.info(f"  Price predictions: {price_count}")
        
        cur.close()
        conn.close()
        
        if model_count < 3:
            logger.warning(f"⚠️  Expected 3 models, found {model_count}")
        
        logger.info("✅ ML results validation completed")
        return True
        
    except Exception as e:
        logger.error(f"❌ ML results validation failed: {e}")
        raise


def notify_completion(**context):
    """Send completion notification"""
    logger.info("📧 Sending completion notification...")
    
    try:
        dag_run = context['dag_run']
        
        notification = {
            'status': 'success',
            'dag_run_id': dag_run.run_id,
            'execution_date': context['execution_date'].isoformat(),
            'models_trained': 3,
            'timestamp': datetime.now().isoformat()
        }
        
        # Save notification
        os.makedirs(LOGS_DIR, exist_ok=True)
        notification_file = Path(LOGS_DIR) / f"ml_pipeline_complete_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(notification_file, 'w') as f:
            json.dump(notification, f, indent=2)
        
        logger.info(f"✅ Notification saved: {notification_file}")
        return notification
    
    except Exception as e:
        logger.error(f"⚠️  Notification failed: {e}")
        return {'status': 'notification_failed', 'error': str(e)}


# ===========================
# DAG Definition
# ===========================

with DAG(
    dag_id='ml_training_inference_pipeline',
    default_args=default_args,
    description='ML Training & Inference Pipeline - 3 Models (Sentiment, Recommendation, Price)',
    schedule_interval='0 3 * * *',  # Run at 3:00 AM daily (after DWH pipeline at 2:00 AM)
    catchup=False,
    tags=['ml', 'training', 'inference', 'daily'],
    max_active_runs=1,
) as dag:

    # ===========================
    # PHASE 0: Setup & Validation
    # ===========================
    
    start = EmptyOperator(task_id='start')
    
    setup_task = PythonOperator(
        task_id='setup_directories',
        python_callable=setup_directories,
    )
    
    check_dwh = PythonOperator(
        task_id='check_dwh_data',
        python_callable=check_dwh_data,
    )
    
    # ===========================
    # PHASE 1: Model Training (Parallel)
    # ===========================
    
    train_sentiment = BashOperator(
        task_id='train_sentiment_model',
        bash_command=f'cd {ML_PROJECT_PATH} && python train_sentiment_model.py',
        env={
            'PYTHONPATH': ML_PROJECT_PATH,
            'ML_MODEL_DIR': MODELS_OUTPUT_DIR,
            'DATABASE_URL': os.getenv('DATABASE_URL', ''),
        },
    )
    
    train_recommender = BashOperator(
        task_id='train_recommender_model',
        bash_command=f'cd {ML_PROJECT_PATH} && python train_recommender.py',
        env={
            'PYTHONPATH': ML_PROJECT_PATH,
            'ML_MODEL_DIR': MODELS_OUTPUT_DIR,
            'DATABASE_URL': os.getenv('DATABASE_URL', ''),
        },
    )
    
    train_price = BashOperator(
        task_id='train_price_model',
        bash_command=f'cd {ML_PROJECT_PATH} && python train_price_model.py',
        env={
            'PYTHONPATH': ML_PROJECT_PATH,
            'ML_MODEL_DIR': MODELS_OUTPUT_DIR,
            'DATABASE_URL': os.getenv('DATABASE_URL', ''),
        },
    )
    
    validate_models = PythonOperator(
        task_id='validate_trained_models',
        python_callable=validate_trained_models,
    )
    
    # ===========================
    # PHASE 2: Batch Inference (Parallel)
    # ===========================
    
    run_sentiment_batch = BashOperator(
        task_id='run_sentiment_batch',
        bash_command=f'cd {ML_PROJECT_PATH} && python run_sentiment_batch.py',
        env={
            'PYTHONPATH': ML_PROJECT_PATH,
            'ML_MODEL_DIR': MODELS_OUTPUT_DIR,
            'DATABASE_URL': os.getenv('DATABASE_URL', ''),
        },
        execution_timeout=timedelta(minutes=30),  # Allow 30 min for sentiment scoring
    )
    
    run_recommendations = BashOperator(
        task_id='run_recommendations',
        bash_command=f'cd {ML_PROJECT_PATH} && python run_recommendations.py',
        env={
            'PYTHONPATH': ML_PROJECT_PATH,
            'ML_MODEL_DIR': MODELS_OUTPUT_DIR,
            'DATABASE_URL': os.getenv('DATABASE_URL', ''),
        },
        execution_timeout=timedelta(hours=1),  # Allow 1 hour for recommendations (CPU intensive with n_jobs=1)
    )
    
    run_price_predictions = BashOperator(
        task_id='run_price_predictions',
        bash_command=f'cd {ML_PROJECT_PATH} && python run_price_predictions.py',
        env={
            'PYTHONPATH': ML_PROJECT_PATH,
            'ML_MODEL_DIR': MODELS_OUTPUT_DIR,
            'DATABASE_URL': os.getenv('DATABASE_URL', ''),
        },
        execution_timeout=timedelta(minutes=20),  # Allow 20 min for price predictions
    )
    
    # ===========================
    # PHASE 3: Validation & Notification
    # ===========================
    
    validate_results = PythonOperator(
        task_id='validate_ml_results',
        python_callable=validate_ml_results,
    )
    
    notify = PythonOperator(
        task_id='notify_completion',
        python_callable=notify_completion,
        trigger_rule='all_done',
    )
    
    end = EmptyOperator(task_id='end')
    
    # ===========================
    # DAG Flow
    # ===========================
    
    # Phase 0: Setup & validation
    start >> setup_task >> check_dwh
    
    # Phase 1: Train 3 models in parallel
    check_dwh >> [train_sentiment, train_recommender, train_price]
    
    # Validate all models trained
    [train_sentiment, train_recommender, train_price] >> validate_models
    
    # Phase 2: Run batch inference in parallel
    validate_models >> [run_sentiment_batch, run_recommendations, run_price_predictions]
    
    # Phase 3: Validate results and notify
    [run_sentiment_batch, run_recommendations, run_price_predictions] >> validate_results >> notify >> end

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
