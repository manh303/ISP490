# Lazada Crawler Setup Guide

## Bước 1: Generate Cookies (Trên máy local Windows)

```bash
cd c:\DoAn_FPT_FALL2025\ecommerce-dss-project\data-collection\crawlers\lazada\runners

# Cài playwright nếu chưa có
pip install playwright
playwright install chromium

# Chạy cookie generator
python lazada_cookie_generator.py
```

**Trong browser:**
1. Đăng nhập Lazada với tài khoản: manh027382@gmail.com
2. Sau khi login xong, quay lại terminal
3. Nhấn Enter
4. File `lazada_cookies.json` sẽ được tạo

## Bước 2: Copy Cookies vào Docker

```bash
# Tạo thư mục
docker exec airflow-webserver mkdir -p /tmp/profiles/lazada

# Copy cookies
docker cp lazada_cookies.json airflow-webserver:/tmp/profiles/lazada/cookies.json

# Verify
docker exec airflow-webserver ls -la /tmp/profiles/lazada/
```

## Bước 3: Test Crawler

```bash
docker exec -it airflow-webserver bash

# Cài playwright trong container
pip install playwright
playwright install chromium

# Test crawler
cd /app/crawlers/lazada/runners
python lazada_with_cookies.py
```

## Bước 4: Chạy trong Airflow

DAG đã được cấu hình tự động:
- Trigger DAG: `tiki_lazada_pipeline`
- Crawler sẽ chạy với cookies đã lưu
- Crawl 3 pages mỗi category
- Tổng ~600 products (5 categories × 40 products × 3 pages)

## Lưu ý

- Cookies có thời hạn, cần refresh định kỳ (mỗi 1-2 tuần)
- Nếu crawler fail, check cookies còn valid không
- Re-generate cookies bằng cách chạy lại bước 1-2
