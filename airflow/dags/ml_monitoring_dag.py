# -*- coding: utf-8 -*-
"""
ML Model Monitoring & Alerting DAG
Monitor trained models performance and alert on issues
"""

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.exceptions import AirflowException
from airflow.models import Variable

logger = logging.getLogger(__name__)

# ===========================
# DAG Configuration
# ===========================

default_args = {
    'owner': 'ml_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email': ['admin@ecommerce.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ml_model_monitoring',
    default_args=default_args,
    description='ML Model Performance Monitoring & Alerting',
    schedule_interval='0 2 * * *',  # Run at 2:00 AM daily (after training)
    catchup=False,
    tags=['ml', 'monitoring', 'daily'],
    max_active_runs=1,
)

# ===========================
# Configuration
# ===========================

LOGS_DIR = '/app/ml/logs'
MODELS_DIR = '/app/ml/models/ml-models'
METRICS_THRESHOLDS = {
    'sentiment': {
        'accuracy_min': 0.80,
        'f1_score_min': 0.80,
        'precision_min': 0.75,
    },
    'clustering': {
        'silhouette_score_min': 0.40,
    }
}

# ===========================
# Python Functions
# ===========================

def check_model_performance(**context):
    """Check model performance against thresholds"""
    logger.info("📊 Checking model performance metrics...")
    
    try:
        metrics_dir = Path(LOGS_DIR) / 'metrics'
        alerts = []
        performance_report = {}
        
        # Check Sentiment Model Metrics
        sentiment_metrics_file = metrics_dir / 'sentiment_metrics.json'
        if sentiment_metrics_file.exists():
            with open(sentiment_metrics_file, 'r') as f:
                metrics = json.load(f)
            
            logger.info(f"📈 Sentiment Model Metrics: {metrics}")
            performance_report['sentiment'] = metrics
            
            # Check thresholds
            thresholds = METRICS_THRESHOLDS['sentiment']
            
            if metrics.get('accuracy', 0) < thresholds['accuracy_min']:
                alerts.append({
                    'model': 'sentiment',
                    'metric': 'accuracy',
                    'value': metrics['accuracy'],
                    'threshold': thresholds['accuracy_min'],
                    'severity': 'warning'
                })
            
            if metrics.get('f1_score', 0) < thresholds['f1_score_min']:
                alerts.append({
                    'model': 'sentiment',
                    'metric': 'f1_score',
                    'value': metrics['f1_score'],
                    'threshold': thresholds['f1_score_min'],
                    'severity': 'warning'
                })
        
        # Check Clustering Model Metrics
        clustering_metrics_file = metrics_dir / 'clustering_metrics.json'
        if clustering_metrics_file.exists():
            with open(clustering_metrics_file, 'r') as f:
                metrics = json.load(f)
            
            logger.info(f"📈 Clustering Model Metrics: {metrics}")
            performance_report['clustering'] = metrics
            
            # Check thresholds
            thresholds = METRICS_THRESHOLDS['clustering']
            
            if metrics.get('silhouette_score', 0) < thresholds['silhouette_score_min']:
                alerts.append({
                    'model': 'clustering',
                    'metric': 'silhouette_score',
                    'value': metrics['silhouette_score'],
                    'threshold': thresholds['silhouette_score_min'],
                    'severity': 'warning'
                })
        
        # Push to XCom
        context['task_instance'].xcom_push(key='performance_report', value=performance_report)
        context['task_instance'].xcom_push(key='alerts', value=alerts)
        
        if alerts:
            logger.warning(f"⚠️  {len(alerts)} alerts detected:")
            for alert in alerts:
                logger.warning(f"  - {alert['model']}: {alert['metric']} = {alert['value']:.4f} (threshold: {alert['threshold']:.4f})")
        else:
            logger.info("✅ All metrics within acceptable thresholds")
        
        return {'alerts_count': len(alerts), 'alerts': alerts, 'report': performance_report}
    
    except Exception as e:
        logger.error(f"❌ Performance check failed: {e}")
        raise AirflowException(f"Performance check failed: {e}")


def check_model_files(**context):
    """Verify model files are present and up-to-date"""
    logger.info("🔍 Checking model files...")
    
    try:
        models_dir = Path(MODELS_DIR)
        required_models = [
            'sentiment_classifier.pkl',
            'sentiment_tfidf_vectorizer.pkl',
            'sentiment_label_encoder.pkl',
            'recommendation_kmeans.pkl',
            'clustering_scaler.pkl',
        ]
        
        file_status = {}
        missing_files = []
        stale_files = []
        
        current_time = datetime.now()
        max_age_hours = 36  # Alert if model is older than 36 hours
        
        for model_file in required_models:
            model_path = models_dir / model_file
            
            if not model_path.exists():
                missing_files.append(model_file)
                file_status[model_file] = 'missing'
                logger.error(f"❌ Model file missing: {model_file}")
            else:
                # Check file age
                file_age_hours = (current_time - datetime.fromtimestamp(model_path.stat().st_mtime)).total_seconds() / 3600
                size_mb = model_path.stat().st_size / (1024 * 1024)
                
                if file_age_hours > max_age_hours:
                    stale_files.append(model_file)
                    file_status[model_file] = 'stale'
                    logger.warning(f"⚠️  Model file is stale ({file_age_hours:.1f} hours old): {model_file}")
                else:
                    file_status[model_file] = 'ok'
                    logger.info(f"✅ {model_file} ({size_mb:.2f} MB, {file_age_hours:.1f} hours old)")
        
        # Push to XCom
        context['task_instance'].xcom_push(key='file_status', value=file_status)
        
        if missing_files or stale_files:
            warning = f"File issues detected: {len(missing_files)} missing, {len(stale_files)} stale"
            logger.warning(f"⚠️  {warning}")
            return {'status': 'warning', 'missing': missing_files, 'stale': stale_files}
        else:
            logger.info("✅ All model files OK")
            return {'status': 'ok'}
    
    except Exception as e:
        logger.error(f"❌ File check failed: {e}")
        raise AirflowException(f"File check failed: {e}")


def compare_with_baseline(**context):
    """Compare current model performance with baseline"""
    logger.info("📊 Comparing with baseline performance...")
    
    try:
        task_instance = context['task_instance']
        performance_report = task_instance.xcom_pull(task_ids='check_performance', key='performance_report')
        
        # Define baseline metrics (from initial training)
        baseline = {
            'sentiment': {
                'accuracy': 0.865,
                'f1_score': 0.865,
                'precision': 0.858,
            },
            'clustering': {
                'silhouette_score': 0.65,
                'davies_bouldin_index': 1.2,
            }
        }
        
        degradation_alerts = []
        max_degradation = 0.05  # Alert if performance degraded by >5%
        
        for model_type, metrics in performance_report.items():
            if model_type in baseline:
                baseline_metrics = baseline[model_type]
                
                for metric_name, current_value in metrics.items():
                    if metric_name in baseline_metrics:
                        baseline_value = baseline_metrics[metric_name]
                        
                        # Calculate degradation percentage
                        if baseline_value > 0:
                            degradation = (baseline_value - current_value) / baseline_value
                            
                            if degradation > max_degradation:
                                degradation_alerts.append({
                                    'model': model_type,
                                    'metric': metric_name,
                                    'baseline': baseline_value,
                                    'current': current_value,
                                    'degradation_pct': degradation * 100,
                                    'severity': 'critical' if degradation > 0.10 else 'warning'
                                })
                            
                            logger.info(f"  {model_type}/{metric_name}: {current_value:.4f} (baseline: {baseline_value:.4f}, change: {degradation*100:+.1f}%)")
        
        # Push to XCom
        context['task_instance'].xcom_push(key='degradation_alerts', value=degradation_alerts)
        
        if degradation_alerts:
            logger.warning(f"⚠️  {len(degradation_alerts)} degradation alerts:")
            for alert in degradation_alerts:
                logger.warning(f"  - {alert['model']}/{alert['metric']}: degraded {alert['degradation_pct']:.1f}%")
        else:
            logger.info("✅ No significant performance degradation detected")
        
        return {'degradation_count': len(degradation_alerts), 'alerts': degradation_alerts}
    
    except Exception as e:
        logger.error(f"❌ Baseline comparison failed: {e}")
        # Don't fail the DAG for comparison errors
        return {'error': str(e)}


def generate_monitoring_report(**context):
    """Generate comprehensive monitoring report"""
    logger.info("📋 Generating monitoring report...")
    
    try:
        task_instance = context['task_instance']
        
        # Gather all monitoring data
        performance_report = task_instance.xcom_pull(task_ids='check_performance', key='performance_report')
        alerts = task_instance.xcom_pull(task_ids='check_performance', key='alerts')
        file_status = task_instance.xcom_pull(task_ids='check_files', key='file_status')
        degradation_alerts = task_instance.xcom_pull(task_ids='compare_baseline', key='degradation_alerts')
        
        # Create comprehensive report
        report = {
            'timestamp': datetime.now().isoformat(),
            'execution_date': context['execution_date'].isoformat(),
            'monitoring': {
                'performance': performance_report or {},
                'threshold_alerts': alerts or [],
                'file_status': file_status or {},
                'degradation_alerts': degradation_alerts or [],
            },
            'summary': {
                'total_alerts': len(alerts or []) + len(degradation_alerts or []),
                'file_issues': sum(1 for s in (file_status or {}).values() if s != 'ok'),
                'status': 'ok' if (not alerts and not degradation_alerts and all(s == 'ok' for s in (file_status or {}).values())) else 'warning'
            }
        }
        
        # Save report
        report_dir = Path(LOGS_DIR) / 'monitoring_reports'
        report_dir.mkdir(parents=True, exist_ok=True)
        
        report_file = report_dir / f"monitoring_{context['execution_date'].strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        logger.info(f"✅ Monitoring report saved: {report_file}")
        
        # Push to XCom for notification
        context['task_instance'].xcom_push(key='monitoring_report', value=report)
        
        return report
    
    except Exception as e:
        logger.error(f"❌ Report generation failed: {e}")
        # Don't fail the DAG for reporting errors
        return {'error': str(e)}


def send_alerts(**context):
    """Send alerts if issues detected"""
    logger.info("📧 Checking for alerts to send...")
    
    try:
        task_instance = context['task_instance']
        monitoring_report = task_instance.xcom_pull(task_ids='generate_report', key='monitoring_report')
        
        if monitoring_report.get('summary', {}).get('status') != 'ok':
            total_alerts = monitoring_report.get('summary', {}).get('total_alerts', 0)
            
            logger.warning(f"⚠️  {total_alerts} issues detected, would send alert notification")
            
            # In production, send email/slack/pagerduty alerts
            # Example:
            # send_email(
            #     to=['admin@ecommerce.com', 'ml-team@ecommerce.com'],
            #     subject=f"⚠️ ML Model Monitoring Alert - {total_alerts} issues",
            #     html_content=json.dumps(monitoring_report, indent=2)
            # )
            
            return {'alerts_sent': True, 'alert_count': total_alerts}
        else:
            logger.info("✅ No alerts to send")
            return {'alerts_sent': False}
    
    except Exception as e:
        logger.error(f"❌ Alert sending failed: {e}")
        # Don't fail the DAG for alert sending errors
        return {'error': str(e)}


# ===========================
# DAG Tasks
# ===========================

start = DummyOperator(task_id='start', dag=dag)

check_performance = PythonOperator(
    task_id='check_performance',
    python_callable=check_model_performance,
    dag=dag,
)

check_files = PythonOperator(
    task_id='check_files',
    python_callable=check_model_files,
    dag=dag,
)

compare_baseline = PythonOperator(
    task_id='compare_baseline',
    python_callable=compare_with_baseline,
    dag=dag,
)

generate_report = PythonOperator(
    task_id='generate_report',
    python_callable=generate_monitoring_report,
    dag=dag,
)

send_alerts_task = PythonOperator(
    task_id='send_alerts',
    python_callable=send_alerts,
    dag=dag,
)

end = DummyOperator(task_id='end', dag=dag)

# ===========================
# DAG Dependencies
# ===========================

start >> [check_performance, check_files] >> compare_baseline >> generate_report >> send_alerts_task >> end

# ===========================
# DAG Documentation
# ===========================

dag.doc_md = """
## ML Model Monitoring & Alerting DAG

### Overview
Daily monitoring of ML model performance and file integrity.

### Tasks
1. **check_performance** - Verify metrics are above thresholds
2. **check_files** - Verify model files exist and are up-to-date
3. **compare_baseline** - Compare with baseline performance
4. **generate_report** - Create comprehensive monitoring report
5. **send_alerts** - Send alerts for detected issues

### Monitoring Thresholds
- Sentiment Accuracy: >= 80%
- Sentiment F1-Score: >= 80%
- Clustering Silhouette Score: >= 0.4

### Alerting
- Threshold violations
- Model file staleness (> 36 hours old)
- Performance degradation (> 5% from baseline)

### Output
- Reports: `/app/ml/logs/monitoring_reports/`
- Alerts: Email to admin@ecommerce.com

### References
- ML Training DAG: ml_training_pipeline
"""

if __name__ == "__main__":
    dag.cli()
