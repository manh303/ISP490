#!/bin/bash
# airflow/config/airflow_env_optimization.sh
# Script để set các environment variables tối ưu cho Airflow

echo "🚀 Setting Airflow optimization environment variables..."

# ============================================================
# SCHEDULER OPTIMIZATION
# ============================================================
export AIRFLOW__SCHEDULER__JOB_HEARTBEAT_SEC=30
export AIRFLOW__SCHEDULER__SCHEDULER_HEARTBEAT_SEC=30
export AIRFLOW__SCHEDULER__MAX_ACTIVE_TASKS_PER_DAG=16
export AIRFLOW__SCHEDULER__MAX_ACTIVE_RUNS_PER_DAG=2
export AIRFLOW__SCHEDULER__PARSING_PROCESSES=2

echo "✅ Scheduler configuration set"

# ============================================================
# DATABASE CONNECTION POOL
# ============================================================
export AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE=10
export AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_OVERFLOW=20
export AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_RECYCLE=1800
export AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_PRE_PING=True

echo "✅ Database connection pool configuration set"

# ============================================================
# CORE SETTINGS
# ============================================================
export AIRFLOW__CORE__PARALLELISM=32
export AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=16
export AIRFLOW__CORE__DAG_CONCURRENCY=16
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False

echo "✅ Core configuration set"

# ============================================================
# LOGGING
# ============================================================
export AIRFLOW__LOGGING__LOGGING_LEVEL=INFO
export AIRFLOW__LOGGING__FAB_LOGGING_LEVEL=WARN

echo "✅ Logging configuration set"

# ============================================================
# WEBSERVER (Optional)
# ============================================================
export AIRFLOW__WEBSERVER__WORKER_REFRESH_INTERVAL=30
export AIRFLOW__WEBSERVER__WEB_SERVER_WORKER_TIMEOUT=120

echo "✅ Webserver configuration set"

echo "
================================================================================
✅ Airflow optimization environment variables have been set!

To apply these settings:
1. Source this file before starting Airflow:
   source airflow/config/airflow_env_optimization.sh

2. Or add to your .bashrc / .zshrc:
   echo 'source /path/to/airflow/config/airflow_env_optimization.sh' >> ~/.bashrc

3. Or add to docker-compose.yml environment section

4. Restart Airflow services:
   docker-compose restart airflow-scheduler airflow-webserver
================================================================================
"

