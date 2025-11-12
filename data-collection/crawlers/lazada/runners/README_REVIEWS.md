# Lazada Reviews Crawler - Hướng dẫn

## Vấn đề đã giải quyết

### 1. Timeout khi crawl Lazada
**Vấn đề cũ:**
- Timeout 60s quá ngắn
- Sử dụng `networkidle` chờ lâu
- Playwright không tìm thấy products (selector sai)

**Giải pháp:**
- Tăng timeout lên 90-120s
- Đổi sang `domcontentloaded` (nhanh hơn)
- **ĐỔI SANG SELENIUM** với selectors đã verify
- Sử dụng undetected-chromedriver để tránh anti-bot

### 2. Thiếu rating trong dữ liệu
**Vấn đề cũ:**
- Không extract được rating từ listing page
- Review count không được lưu đầy đủ

**Giải pháp:**
- Đảm bảo field `review` luôn tồn tại (default '0')
- Extract rating từ stars element
- Lưu cả rating_avg và review_count

### 3. Thiếu luồng crawl reviews
**Vấn đề cũ:**
- Chỉ có product crawler
- Không có reviews chi tiết

**Giải pháp mới:**
- Tạo `lazada_reviews_crawler_airflow.py` với Selenium
- Sử dụng selectors từ `working_lazada_reviews_crawler.py`
- Thêm task `crawl_lazada_reviews` trong Airflow DAG
- Thêm sensor `wait_reviews_ready` để đợi reviews data

## Cấu trúc Airflow DAG mới

```
start
  ├─> crawl_lazada ──> crawl_lazada_reviews ──> wait_reviews_ready ─┐
  └─> crawl_tiki ────────────────────────────> wait_raw_ready ──────┤
                                                                      ├─> spark_etl
                                                                      │
                                                                      └─> dwh_ddl -> ...
```

## Cách sử dụng

### 1. Test local (không dùng Airflow)

```bash
cd data-collection/crawlers/lazada/runners

# Cài dependencies
pip install undetected-chromedriver selenium

# Quick test (3 products only)
python quick_test_reviews.py

# Full test
python lazada_reviews_crawler_airflow.py
```

### 2. Chạy trên Airflow

```bash
# Restart Airflow để load DAG mới
docker-compose restart airflow-webserver airflow-scheduler

# Trigger DAG
docker exec -it airflow-webserver airflow dags trigger tiki_lazada_pipeline
```

### 3. Kiểm tra kết quả

```bash
# Xem logs
docker logs airflow-scheduler -f

# Kiểm tra output
ls -la /app/data/outputs/lazada/date=2025-11-11/
ls -la /app/data/outputs/lazada_reviews/date=2025-11-11/
```

## Output Structure

### Products (lazada_with_cookies.py)
```
/app/data/outputs/lazada/date=YYYY-MM-DD/
  └── lazada_smartphones_YYYYMMDD_HHMMSS.jsonl
```

### Reviews (lazada_reviews_crawler_airflow.py)
```
/app/data/outputs/lazada_reviews/date=YYYY-MM-DD/
  └── lazada_reviews_smartphones_YYYYMMDD_HHMMSS.jsonl
```

## Cấu hình

### Environment Variables

```bash
# Trong docker-compose.yml hoặc .env
CRAWLER_OUTPUT_DIR=/app/data/outputs

# Không cần cookies nữa vì dùng undetected-chromedriver
```

### Timeout Settings

Trong `lazada_with_cookies.py`:
- `timeout=90000` (90s) cho domcontentloaded
- `timeout=120000` (120s) cho retry với load

Trong `lazada_reviews_crawler_airflow.py`:
- `timeout=90000` (90s) cho tất cả page loads
- `max_reviews_per_product=10` (giới hạn reviews)
- `max_products_per_category=20` (giới hạn products)

## Troubleshooting

### Lỗi: "Timeout 60000ms exceeded"
✅ **Đã fix:** Tăng timeout lên 90-120s và đổi wait strategy

### Lỗi: "Found 0 products"
✅ **Đã fix:** Đổi sang Selenium với selectors đã verify
- Đảm bảo Chrome/Chromium đã cài
- Chạy `quick_test_reviews.py` để test nhanh

### Lỗi: "No reviews found"
- Bình thường vì nhiều sản phẩm không có reviews
- Crawler sẽ skip và tiếp tục với sản phẩm khác

### Lỗi: "Task failed"
```bash
# Xem logs chi tiết
docker logs airflow-scheduler | grep "lazada_reviews"

# Kiểm tra file script tồn tại
docker exec airflow-scheduler ls -la /app/crawlers/lazada/runners/
```

## Performance

### Thời gian crawl ước tính

- **Products crawler**: ~5-10 phút (3 categories, 2 pages mỗi category)
- **Reviews crawler**: ~10-15 phút (3 categories, 5 products/category, 10 reviews/product)
- **Tổng**: ~15-25 phút cho toàn bộ pipeline

### Tối ưu hóa

Để crawl nhanh hơn, giảm các giá trị trong `lazada_reviews_crawler_airflow.py`:

```python
self.max_reviews_per_product = 5  # Giảm từ 10
self.max_products_per_category = 10  # Giảm từ 20
```

Hoặc trong DAG, giảm `max_pages`:

```python
crawler.run(max_pages=1)  # Chỉ crawl 1 page
```

## Notes

- Reviews crawler chạy SONG SONG với Tiki crawler
- Reviews crawler chạy SAU products crawler (dependency)
- Spark ETL chỉ chạy khi CẢ products VÀ reviews đều ready
- Cookies cần được refresh định kỳ (mỗi 7-14 ngày)
