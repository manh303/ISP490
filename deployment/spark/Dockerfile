# deployment/spark/Dockerfile
FROM bitnami/spark:3.4

USER root

# Thêm PostgreSQL dev headers và build tools
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    procps \
    vim \
    build-essential \
    libpq-dev \
    postgresql-client \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip
RUN pip3 install --upgrade pip setuptools wheel

# Copy và install requirements
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# Tạo directories
RUN mkdir -p /app/{data,streaming,logs,checkpoints}

# Copy configs
COPY spark-defaults.conf /opt/bitnami/spark/conf/
COPY healthcheck.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/healthcheck.sh

# Set permissions
RUN chown -R 1001:1001 /app /opt/bitnami/spark/conf/

USER 1001

WORKDIR /app
CMD ["/opt/bitnami/scripts/spark/run.sh"]