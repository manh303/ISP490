# ====================================
# AIRFLOW CONTAINER - FIXED VERSION
# ====================================

FROM apache/airflow:2.8.1-python3.9

# Switch to root for system packages
USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    postgresql-client \
    curl \
    vim \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy requirements first for better caching
COPY requirements.txt /opt/airflow/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Create necessary directories
RUN mkdir -p /opt/airflow/dags /opt/airflow/plugins /opt/airflow/config

# Copy project files (these will be mounted as volumes in docker-compose)
# COPY dags/ /opt/airflow/dags/
# COPY plugins/ /opt/airflow/plugins/
# COPY config/ /opt/airflow/config/

# Set environment variables
ENV PYTHONPATH="/opt/airflow/dags:/app/src:$PYTHONPATH"
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD airflow jobs check --job-type SchedulerJob --hostname "$(hostname)" || exit 1