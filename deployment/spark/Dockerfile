# deployment/spark/Dockerfile
FROM apache/spark:3.5.0

USER root

# Install dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    procps \
    vim \
    netcat-openbsd \
    build-essential \
    libpq-dev \
    postgresql-client \
    python3-dev \
    python3-pip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip
RUN pip3 install --upgrade pip setuptools wheel

# Copy và install requirements
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# Tạo directories
RUN mkdir -p /app/{data,streaming,logs,checkpoints}

# Copy configs and scripts
RUN mkdir -p /opt/spark/conf
COPY spark-defaults.conf /opt/spark/conf/
COPY healthcheck.sh /usr/local/bin/
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/healthcheck.sh /usr/local/bin/docker-entrypoint.sh

# Create and set permissions for Ivy cache
RUN mkdir -p /home/spark/.ivy2 && \
    chown -R spark:spark /app /opt/spark/conf/ /home/spark/.ivy2 && \
    chmod -R 755 /home/spark/.ivy2

USER spark

WORKDIR /app

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]