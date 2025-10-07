#!/usr/bin/env python3
"""
Optimized Vietnam Electronics Pipeline DAG
DAG tối ưu duy nhất cho xử lý dữ liệu điện tử TMĐT Việt Nam
"""

import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Configuration
DAG_ID = "vietnam_electronics_optimized"
DESCRIPTION = "Optimized pipeline for Vietnam Electronics E-commerce data processing"

# Default args
default_args = {
    'owner': 'vietnam-electronics-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    DAG_ID,
    default_args=default_args,
    description=DESCRIPTION,
    schedule_interval='0 */6 * * *',  # Every 6 hours
    catchup=False,
    max_active_runs=1,
    tags=['vietnam', 'electronics', 'optimized']
)

def collect_vietnam_electronics(**context):
    """Thu thập dữ liệu điện tử VN từ collector"""
    import subprocess
    import logging

    logger = logging.getLogger(__name__)
    logger.info("🇻🇳 Bắt đầu thu thập dữ liệu điện tử Việt Nam...")

    try:
        # Run the collector script
        result = subprocess.run(
            ['python', '/opt/airflow/dags/../vietnam_electronics_collector.py'],
            capture_output=True,
            text=True,
            timeout=1800  # 30 minutes timeout
        )

        if result.returncode == 0:
            logger.info("✅ Thu thập thành công")
            logger.info(result.stdout)
            return {"status": "success", "message": "Data collected successfully"}
        else:
            logger.error("❌ Thu thập thất bại")
            logger.error(result.stderr)
            raise Exception(f"Collection failed: {result.stderr}")

    except Exception as e:
        logger.error(f"❌ Lỗi thu thập: {e}")
        raise

def process_electronics_data(**context):
    """Xử lý và làm sạch dữ liệu điện tử"""
    import pandas as pd
    import logging
    from pathlib import Path

    logger = logging.getLogger(__name__)
    logger.info("🔄 Bắt đầu xử lý dữ liệu điện tử...")

    try:
        # Find latest fresh data
        fresh_dir = Path("/opt/airflow/data/vietnam_electronics_fresh")
        if not fresh_dir.exists():
            raise Exception("No fresh data directory found")

        # Get latest file
        csv_files = list(fresh_dir.glob("vietnam_electronics_fresh_*.csv"))
        if not csv_files:
            raise Exception("No fresh data files found")

        latest_file = max(csv_files, key=lambda x: x.stat().st_mtime)
        logger.info(f"📊 Processing file: {latest_file}")

        # Load and basic processing
        df = pd.read_csv(latest_file)

        # Basic cleaning
        initial_count = len(df)
        df = df.dropna(subset=['name', 'price_vnd'])
        df = df.drop_duplicates(subset=['name', 'brand'])
        final_count = len(df)

        # Save processed data
        output_file = fresh_dir / f"processed_electronics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(output_file, index=False)

        logger.info(f"✅ Xử lý hoàn tất: {initial_count} → {final_count} records")
        logger.info(f"💾 Saved to: {output_file}")

        return {
            "status": "success",
            "input_records": initial_count,
            "output_records": final_count,
            "output_file": str(output_file)
        }

    except Exception as e:
        logger.error(f"❌ Lỗi xử lý: {e}")
        raise

def generate_electronics_report(**context):
    """Tạo báo cáo dữ liệu điện tử"""
    import json
    import logging

    logger = logging.getLogger(__name__)
    logger.info("📊 Tạo báo cáo dữ liệu điện tử...")

    try:
        # Get results from previous tasks
        collection_result = context['task_instance'].xcom_pull(task_ids='collect_electronics_data')
        processing_result = context['task_instance'].xcom_pull(task_ids='process_electronics_data')

        # Create report
        report = {
            'pipeline_run': context['dag_run'].run_id,
            'execution_date': context['execution_date'].isoformat(),
            'collection_status': collection_result.get('status', 'unknown') if collection_result else 'failed',
            'processing_status': processing_result.get('status', 'unknown') if processing_result else 'failed',
            'records_processed': processing_result.get('output_records', 0) if processing_result else 0,
            'data_quality_score': 85.0,  # Placeholder
            'platforms_covered': ['Tiki', 'DummyJSON', 'FakeStore', 'Synthetic'],
            'report_generated_at': datetime.now().isoformat()
        }

        logger.info(f"📋 Report: {json.dumps(report, indent=2)}")

        return report

    except Exception as e:
        logger.error(f"❌ Lỗi tạo báo cáo: {e}")
        raise

# Task definitions
collect_task = PythonOperator(
    task_id='collect_electronics_data',
    python_callable=collect_vietnam_electronics,
    dag=dag
)

process_task = PythonOperator(
    task_id='process_electronics_data',
    python_callable=process_electronics_data,
    dag=dag
)

report_task = PythonOperator(
    task_id='generate_electronics_report',
    python_callable=generate_electronics_report,
    dag=dag
)

# Task dependencies
collect_task >> process_task >> report_task

# DAG documentation
dag.doc_md = """
# Vietnam Electronics Optimized Pipeline

## Mục đích
Pipeline tối ưu duy nhất để xử lý dữ liệu sản phẩm điện tử từ các platform TMĐT Việt Nam.

## Luồng xử lý
1. **Collect**: Thu thập dữ liệu từ Tiki, APIs và synthetic data
2. **Process**: Làm sạch và chuẩn hóa dữ liệu
3. **Report**: Tạo báo cáo chất lượng dữ liệu

## Lịch chạy
- Mỗi 6 giờ
- Không chạy backfill
- Tối đa 1 run concurrent

## Platforms hỗ trợ
- Tiki Vietnam API
- DummyJSON Electronics
- FakeStore API
- Vietnam Synthetic Data Generator
"""
