# ====================================
# AIRFLOW CONTAINER - FIXED VERSION
# ====================================
FROM apache/airflow:2.8.1-python3.9

# -------------------------------
# 1) ROOT: System packages & Chromium/Chromedriver
# -------------------------------
USER root

# System libs cần cho Chromium/Headless + tiện ích
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
    # Cài Chromium & Chromedriver (tên gói có thể khác tùy base)
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

# -------------------------------
# 2) AIRFLOW: Python deps
# -------------------------------
USER airflow

# Copy trước requirements để cache tốt hơn
COPY requirements.txt /opt/airflow/requirements.txt

# Cài deps của project + các gói cần thiết cho crawler
# (Nếu requirements.txt của bạn đã có những gói này, pip sẽ skip)
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /opt/airflow/requirements.txt && \
    pip install --no-cache-dir \
        playwright==1.47.0 \
        undetected-chromedriver \
        selenium \
        beautifulsoup4==4.12.2 \
        lxml==4.9.3

# Cache browsers cho Playwright ở nơi dễ quản lý/ghi
ENV PLAYWRIGHT_BROWSERS_PATH=/opt/airflow/.cache/ms-playwright
RUN mkdir -p "${PLAYWRIGHT_BROWSERS_PATH}"

# -------------------------------
# 3) ROOT: Cài OS deps cho Playwright Chromium
#    (cần quyền root để apt install-deps)
# -------------------------------
USER root
RUN python -m playwright install-deps chromium || true

# -------------------------------
# 4) AIRFLOW: Tải Chromium cho Playwright vào cache của user airflow
# -------------------------------
USER airflow
RUN python -m playwright install chromium

# -------------------------------
# 5) Thư mục DAGS/PLUGINS/CONFIG
# -------------------------------
RUN mkdir -p /opt/airflow/dags /opt/airflow/plugins /opt/airflow/config

# (Tuỳ chọn) Nếu muốn bake sẵn:
# COPY dags/ /opt/airflow/dags/
# COPY plugins/ /opt/airflow/plugins/
# COPY config/ /opt/airflow/config/

# -------------------------------
# 6) Airflow ENV
# -------------------------------
ENV PYTHONPATH="/opt/airflow/dags:/app/src:${PYTHONPATH}"
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True

# -------------------------------
# 7) Healthcheck
# -------------------------------
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD airflow jobs check --job-type SchedulerJob --hostname "$(hostname)" || exit 1
