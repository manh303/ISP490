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
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy requirements first for better caching
COPY requirements.txt /requirements.txt

# Install Python dependencies with no-deps for critical packages to avoid conflicts
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt

# Copy project files
COPY dags/ /opt/airflow/dags/
COPY plugins/ /opt/airflow/plugins/
COPY config/ /opt/airflow/config/

# Set environment variables
ENV PYTHONPATH="/opt/airflow/dags:/app/src:$PYTHONPATH"
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True