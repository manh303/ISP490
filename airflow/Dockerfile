# ====================================
# AIRFLOW CONTAINER - FIXED VERSION
# ====================================

FROM apache/airflow:2.8.1-python3.9

# -------------------------------
# 1) Switch to root for system pkgs
# -------------------------------
USER root

# Install system dependencies + Chromium + Chromedriver
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
    # Install Chromium & Chromedriver
    if apt-cache show chromium >/dev/null 2>&1; then \
        apt-get install -y --no-install-recommends chromium chromium-driver; \
        ln -sf /usr/bin/chromium /usr/bin/chromium-browser || true; \
    else \
        apt-get install -y --no-install-recommends chromium-browser chromium-chromedriver || true; \
    fi; \
    rm -rf /var/lib/apt/lists/*

# Env for Selenium/Chrome
ENV CHROME_BIN=/usr/bin/chromium-browser
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver
ENV DISPLAY=:99

# -------------------------------
# 2) Python requirements as airflow user
# -------------------------------
USER airflow

COPY requirements.txt /opt/airflow/requirements.txt

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /opt/airflow/requirements.txt

# -------------------------------
# 3) Playwright (optional, if needed by your code)
# -------------------------------
# Install playwright browsers
# Note: We install playwright as airflow user (via pip above).
# We skip --with-deps because we already installed system deps in step 1.
RUN python -m playwright install chromium

# -------------------------------
# 4) Folders (dags/plugins/config)
# -------------------------------
RUN mkdir -p /opt/airflow/dags /opt/airflow/plugins /opt/airflow/config

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
