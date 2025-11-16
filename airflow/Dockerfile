# ====================================
# AIRFLOW CONTAINER - FIXED VERSION
# ====================================

FROM apache/airflow:2.8.1-python3.9

# -------------------------------
# 1) Switch to root for system pkgs
# -------------------------------
USER root

# Install system dependencies + Chromium + Chromedriver
# - Thêm libs chrome hay cần: fonts-liberation, libnss3, libxss1, libasound2, libatk-bridge2.0-0, libgtk-3-0
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        postgresql-client \
        curl \
        vim \
        netcat-openbsd \
        ca-certificates \
        fonts-liberation \
        libnss3 \
        libxss1 \
        libasound2 \
        libatk-bridge2.0-0 \
        libgtk-3-0; \
    # Cài Chromium & Chromedriver (tên gói khác nhau tùy distro)
    if apt-cache show chromium >/dev/null 2>&1; then \
        apt-get install -y --no-install-recommends chromium chromium-driver; \
        ln -sf /usr/bin/chromium /usr/bin/chromium-browser || true; \
    else \
        apt-get install -y --no-install-recommends chromium-browser chromium-chromedriver || true; \
    fi; \
    rm -rf /var/lib/apt/lists/*

# Env cho Selenium/Chrome
ENV CHROME_BIN=/usr/bin/chromium-browser
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver
ENV DISPLAY=:99

# Allow pip to run as root for base image (Airflow requirement)
ENV PIP_USER=false

# -------------------------------
# 2) Python requirements as airflow user (Airflow requirement)
# -------------------------------
USER airflow

COPY requirements.txt /opt/airflow/requirements.txt

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Install playwright browsers (needs root access for system libs)
USER root
RUN python -m playwright install --with-deps chromium

# Back to airflow user for runtime
USER airflow

# -------------------------------
# 4) Folders (dags/plugins/config)
# -------------------------------
RUN mkdir -p /opt/airflow/dags /opt/airflow/plugins /opt/airflow/config

# COPY nếu bạn muốn đóng gói sẵn thay vì mount volumes
# COPY dags/ /opt/airflow/dags/
# COPY plugins/ /opt/airflow/plugins/
# COPY config/ /opt/airflow/config/

# -------------------------------
# 5) Airflow env
# -------------------------------
ENV PYTHONPATH="/opt/airflow/dags:/app/src:${PYTHONPATH}"
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True

# -------------------------------
# 6) Healthcheck
# -------------------------------
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD airflow jobs check --job-type SchedulerJob --hostname "$(hostname)" || exit 1
