# Lazada Reviews Crawler - Final Working Version

## ✅ Đã giải quyết

### Vấn đề
- Playwright/Selenium không tìm thấy products trên category pages
- Lazada render nội dung bằng JavaScript, cần đợi lâu
- Selectors không đúng với cấu trúc thực tế

### Giải pháp
- **Crawl reviews trực tiếp từ product URLs** thay vì category pages
- Sử dụng selectors từ HTML thực tế (`product_detail.html`)
- Pipeline 2 bước: Lấy URLs → Crawl reviews

## 📁 Files

1. **lazada_reviews_final.py** - Crawler reviews từ product URLs
2. **lazada_full_pipeline.py** - Pipeline đầy đủ (URLs + Reviews)
3. **test_reviews_final.py** - Test nhanh
4. **lazada_reviews_crawler_airflow.py** - Version cho Airflow (copy của final)

## 🚀 Cách sử dụng

### Test nhanh (1 product)

```bash
cd data-collection/crawlers/lazada/runners
pip install undetected-chromedriver selenium

python test_reviews_final.py
```

### Crawl đầy đủ (Products + Reviews)

```bash
python lazada_full_pipeline.py
```

### Chỉ crawl reviews từ URLs có sẵn

```python
from lazada_reviews_final import LazadaReviewsCrawler

urls = [
    "https://www.lazada.vn/products/i2974492628.html",
    "https://www.lazada.vn/products/i1234567890.html",
]

crawler = LazadaReviewsCrawler()
crawler.run(urls)
```

## 📊 Cấu trúc dữ liệu Reviews

```json
{
  "review_id": "lazada_rev_abc123",
  "product_id": "2974492628",
  "product_name": "Xiaomi Redmi Note 14 Pro",
  "product_url": "https://www.lazada.vn/products/i2974492628.html",
  "reviewer_name": "0***2",
  "review_date": "2 weeks ago",
  "rating": 5,
  "review_text": "note 14 pro 🥹🥹🥹",
  "sku_info": "Màu sắc: Xanh",
  "helpful_count": 0,
  "crawl_timestamp": "2025-11-11T00:00:00"
}
```

## 🎯 Selectors sử dụng (từ HTML thực tế)

```python
# Review items
'div.mod-reviews div.item'

# Reviewer name
'span.reviewer'

# Review date
'span.time'

# Rating (count stars)
'div.container-star img.star'

# Review text
'div.item-content-main-content-reviews-item span'

# SKU info (color, variant)
'div.skuInfo-item'

# Helpful count
'span.item-content-like-content-text'
```

## ⚙️ Cấu hình

### Trong code

```python
# lazada_reviews_final.py
self.max_reviews_per_product = 10  # Số reviews tối đa mỗi product

# lazada_full_pipeline.py
max_products=5  # Số products lấy từ mỗi category
```

### Environment variables

```bash
CRAWLER_OUTPUT_DIR=/app/data/outputs  # Thư mục output
```

## 🔄 Tích hợp Airflow

### Cách 1: Sử dụng product URLs có sẵn

Nếu bạn đã có danh sách product URLs từ crawler khác:

```python
# Trong Airflow DAG
crawl_reviews = BashOperator(
    task_id="crawl_lazada_reviews",
    bash_command="""
    cd /app/crawlers/lazada/runners
    python lazada_reviews_crawler_airflow.py
    """
)
```

### Cách 2: Pipeline đầy đủ

```python
# Trong Airflow DAG
crawl_full_pipeline = BashOperator(
    task_id="crawl_lazada_full",
    bash_command="""
    cd /app/crawlers/lazada/runners
    python lazada_full_pipeline.py
    """
)
```

## 📈 Performance

### Thời gian ước tính

- **1 product**: ~15 giây (load page + extract reviews)
- **10 products**: ~3-4 phút
- **50 products**: ~15-20 phút

### Tối ưu hóa

```python
# Giảm wait time (rủi ro: có thể miss reviews)
time.sleep(5)  # Thay vì 10

# Giảm số reviews
self.max_reviews_per_product = 5  # Thay vì 10

# Headless mode (nhanh hơn)
options.add_argument("--headless=new")
```

## 🐛 Troubleshooting

### Lỗi: "Found 0 review items"

**Nguyên nhân:** Page chưa load xong

**Giải pháp:**
```python
# Tăng wait time trong extract_reviews_from_product
time.sleep(15)  # Thay vì 10
```

### Lỗi: "Driver setup failed"

**Giải pháp:**
```bash
# Cài Chrome/Chromium
# Windows: Download từ google.com/chrome
# Linux: sudo apt install chromium-browser

# Hoặc dùng regular Chrome
pip install selenium
```

### Không có reviews

**Bình thường!** Nhiều sản phẩm không có reviews. Crawler sẽ:
- Return empty list
- Log "Extracted 0 reviews"
- Tiếp tục với product khác

## 📝 Notes

- ✅ Crawler hoạt động với HTML structure thực tế
- ✅ Không cần cookies
- ✅ Sử dụng undetected-chromedriver để tránh anti-bot
- ✅ Extract đầy đủ: reviewer, date, rating, text, SKU, helpful count
- ⚠️ Cần đợi 10s để page load (JavaScript rendering)
- ⚠️ Lazada có thể thay đổi HTML structure → cần update selectors

## 🔗 Workflow đề xuất

### Cho Airflow

```
1. crawl_lazada_products (existing)
   ↓
2. extract_product_urls (new task)
   ↓
3. crawl_lazada_reviews (new task)
   ↓
4. spark_etl (existing)
```

### Script extract URLs từ products data

```python
# extract_urls.py
import json

with open('lazada_products.jsonl', 'r') as f:
    for line in f:
        product = json.loads(line)
        print(product['url'])
```

## ✨ Kết luận

Crawler này:
- ✅ Hoạt động với cấu trúc HTML thực tế
- ✅ Extract đầy đủ thông tin reviews
- ✅ Dễ tích hợp vào Airflow
- ✅ Có thể scale (crawl nhiều products)

Hãy test với `test_reviews_final.py` trước khi deploy!
