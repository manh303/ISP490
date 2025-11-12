# Hướng dẫn cài đặt và test Reviews Crawler

## Bước 1: Cài đặt dependencies

```bash
pip install undetected-chromedriver selenium pandas
```

## Bước 2: Test nhanh (3 products, 3 reviews mỗi product)

```bash
cd data-collection/crawlers/lazada/runners
python quick_test_reviews.py
```

**Kết quả mong đợi:**
```
Testing Lazada Reviews Crawler...
============================================================
[Lazada-Reviews] Starting reviews crawler...
[Lazada-Reviews] Driver ready
[Lazada-Reviews] Crawling: smartphones
[Lazada-Reviews] Page 1
[Lazada-Reviews] Found 40 products
[Lazada-Reviews] Extracted X reviews
[Lazada-Reviews] Category 'smartphones': X reviews
[Lazada-Reviews] Saved X reviews to ...
============================================================
Test completed!
```

## Bước 3: Test đầy đủ

```bash
python lazada_reviews_crawler_airflow.py
```

## Bước 4: Kiểm tra output

```bash
# Windows
dir ..\..\..\..\..\data\outputs\lazada_reviews\

# Linux/Mac
ls -la ../../../../../data/outputs/lazada_reviews/
```

## Troubleshooting

### Lỗi: "Driver setup failed"

**Giải pháp:**
```bash
# Cài Chrome/Chromium
# Windows: Download từ google.com/chrome
# Linux: sudo apt install chromium-browser
# Mac: brew install chromium
```

### Lỗi: "Found 0 products"

**Nguyên nhân:** Lazada có thể đang block

**Giải pháp:**
1. Chạy không headless để xem:
   - Sửa trong `lazada_reviews_crawler_airflow.py`:
   ```python
   # Dòng 42: Xóa "--headless=new"
   options.add_argument("--headless=new")  # <-- Xóa dòng này
   ```

2. Thử URL khác:
   ```python
   crawler.categories = {
       "smartphones": "https://www.lazada.vn/dien-thoai-di-dong/"
   }
   ```

### Lỗi: "No reviews found"

**Bình thường!** Nhiều sản phẩm không có reviews. Crawler sẽ:
- Skip sản phẩm không có reviews
- Tiếp tục với sản phẩm khác
- Lưu những reviews tìm được

## Cấu trúc dữ liệu Reviews

```json
{
  "review_id": "lazada_rev_abc123",
  "product_id": "123456789",
  "product_name": "iPhone 15 Pro Max",
  "product_url": "https://www.lazada.vn/products/...",
  "review_text": "Sản phẩm rất tốt, giao hàng nhanh...",
  "rating": 5,
  "reviewer_name": "Nguyễn Văn A",
  "review_date": null,
  "helpful_count": 0,
  "category": "smartphones",
  "page_number": 1,
  "crawl_timestamp": "2025-11-11T00:00:00"
}
```

## Tối ưu hóa

### Crawl nhanh hơn (ít dữ liệu)

Sửa trong `lazada_reviews_crawler_airflow.py`:

```python
# Dòng 36-37
self.max_reviews_per_product = 3  # Giảm từ 5
self.max_products_per_category = 5  # Giảm từ 10
```

### Crawl nhiều hơn (nhiều dữ liệu)

```python
# Dòng 36-37
self.max_reviews_per_product = 10  # Tăng lên
self.max_products_per_category = 20  # Tăng lên

# Khi chạy
crawler.run(max_pages=2)  # Crawl 2 pages thay vì 1
```

## Deploy lên Airflow

Sau khi test local thành công:

```bash
# Copy file vào container
docker cp lazada_reviews_crawler_airflow.py airflow-scheduler:/app/crawlers/lazada/runners/

# Restart Airflow
docker-compose restart airflow-scheduler airflow-webserver

# Trigger DAG
docker exec -it airflow-webserver airflow dags trigger tiki_lazada_pipeline

# Xem logs
docker logs airflow-scheduler -f | grep "Lazada-Reviews"
```

## Kiểm tra trong Airflow

1. Mở Airflow UI: http://localhost:8080
2. Tìm DAG: `tiki_lazada_pipeline`
3. Xem task: `crawl_lazada_reviews`
4. Kiểm tra logs và output files

## Support

Nếu gặp vấn đề:
1. Chạy `quick_test_reviews.py` để test nhanh
2. Kiểm tra Chrome/Chromium đã cài chưa
3. Xem logs chi tiết
4. Thử giảm số lượng products/reviews để test
