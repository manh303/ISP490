# ====================================
# AIRFLOW CONTAINER - FIXED VERSION (giữ 2.8.1 như bạn)
# ====================================
FROM apache/airflow:2.8.1-python3.9

# -------------------------------
# 1) ROOT: System packages & Chromium/Chromedriver
# -------------------------------
USER root

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        gcc g++ \
        postgresql-client \
        curl wget \
        vim \
        netcat-openbsd \
        ca-certificates \
        # libs cho chromium
        fonts-liberation \
        libnss3 \
        libxss1 \
        libasound2 \
        libatk-bridge2.0-0 \
        libgtk-3-0 \
        libxkbcommon0 \
        libxcomposite1 \
        libxdamage1 \
        libxrandr2 \
        libxext6 \
        libxfixes3 \
        libxshmfence1 \
        libdrm2 \
        libgbm1 \
    ; \
    # Cài Chromium & Chromedriver (tên gói tùy base)
    if apt-cache show chromium >/dev/null 2>&1; then \
        apt-get install -y --no-install-recommends chromium chromium-driver; \
    else \
        apt-get install -y --no-install-recommends chromium-browser chromium-chromedriver || true; \
    fi; \
    # Đồng bộ symlink để đường dẫn chuẩn đoán chắc chắn
    if [ -x /usr/bin/chromium ]; then ln -sf /usr/bin/chromium /usr/bin/chromium-browser; fi; \
    if [ -x /usr/lib/chromium/chromedriver ]; then ln -sf /usr/lib/chromium/chromedriver /usr/bin/chromedriver; fi; \
    rm -rf /var/lib/apt/lists/*

# Env cho Selenium/Chrome
ENV CHROME_BIN=/usr/bin/chromium-browser
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver
ENV DISPLAY=:99
ENV TZ=Asia/Bangkok

# -------------------------------
# 2) AIRFLOW: Python deps
# -------------------------------
USER airflow

# Nếu bạn có requirements riêng, giữ nguyên để cache tốt
COPY requirements.txt /opt/airflow/requirements.txt

RUN pip install --no-cache-dir --upgrade pip && \
    # Cài gói dự án của bạn
    pip install --no-cache-dir -r /opt/airflow/requirements.txt && \
    # Gói crawler/phổ biến (nếu đã có trong requirements.txt thì pip sẽ skip)
    pip install --no-cache-dir \
        undetected-chromedriver \
        selenium \
        beautifulsoup4==4.12.2 \
        lxml==4.9.3 && \
    # Providers cần cho Celery/DB
    pip install --no-cache-dir \
        apache-airflow-providers-postgres \
        apache-airflow-providers-redis

# (Tuỳ chọn) Nếu bạn dùng Playwright
ENV PLAYWRIGHT_BROWSERS_PATH=/opt/airflow/.cache/ms-playwright
RUN mkdir -p "${PLAYWRIGHT_BROWSERS_PATH}"

USER root
# Cài OS deps cho Playwright Chromium (không lỗi nếu không dùng)
RUN python -m playwright install-deps chromium || true
USER airflow
RUN python -m playwright install chromium || true

# -------------------------------
# 3) Thư mục DAGS/PLUGINS/CONFIG
# -------------------------------
RUN mkdir -p /opt/airflow/dags /opt/airflow/plugins /opt/airflow/config

# COPY dags/ /opt/airflow/dags/
# COPY plugins/ /opt/airflow/plugins/
# COPY config/ /opt/airflow/config/

# -------------------------------
# 4) Airflow ENV
# -------------------------------
ENV PYTHONPATH="/opt/airflow/dags:/app/src:${PYTHONPATH}"
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True

# -------------------------------
# 5) Healthcheck
# -------------------------------
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD airflow jobs check --job-type SchedulerJob --hostname "$(hostname)" || exit 1
