# Data Pipeline Container
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/src

# Set work directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    wget \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Install Java (required for Spark)
RUN apt-get update && apt-get install -y \
    default-jdk \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Create necessary directories
RUN mkdir -p /app/data/{raw,processed,models,exports} && \
    mkdir -p /app/logs && \
    chmod -R 755 /app

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash dss_user && \
    mkdir -p /home/dss_user/kaggle_datasets && \
    chown -R dss_user:dss_user /home/dss_user
# Set working directory and permissions
WORKDIR /app
RUN chown -R dss_user:dss_user /app
USER dss_user

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD python -c "import pandas; print('Data pipeline healthy')" || exit 1

# Default command
CMD ["python", "scripts/initial_data_setup.py"]