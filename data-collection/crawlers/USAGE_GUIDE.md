# Hướng dẫn sử dụng E-commerce Crawlers

## ✅ Trạng thái hiện tại

- **Base Crawler**: ✅ Hoạt động tốt
- **7 Platform Crawlers**: ✅ Đã được tạo
- **Multi-platform Orchestrator**: ✅ Sẵn sàng
- **Requirements**: ✅ Đã định nghĩa

## 🚀 Cài đặt nhanh

```bash
# 1. Chuyển vào thư mục crawlers
cd data-collection/crawlers

# 2. Cài đặt dependencies
pip install -r requirements.txt

# 3. Test crawler cơ bản
python simple_test.py
```

## 🎯 Sử dụng cơ bản

### Crawl từng platform riêng lẻ:

```bash
# Lazada
python lazada_crawler.py

# Tiki
python tiki_crawler.py

# Sendo
python sendo_crawler.py

# CellphoneS
python cellphones_crawler.py

# FPTShop
python fptshop_crawler.py

# Thế Giới Di Động
python thegioididong_crawler.py

# Hoàng Hà Mobile
python hoanghamobile_crawler.py
```

### Crawl nhiều platform cùng lúc:

```bash
# Crawl tất cả cho điện thoại (mode an toàn)
python multi_platform_orchestrator.py --categories dien-thoai --mode sequential

# Crawl platforms cụ thể
python multi_platform_orchestrator.py --platforms lazada tiki --categories dien-thoai laptop

# Crawl với tùy chỉnh
python multi_platform_orchestrator.py --categories dien-thoai --max-pages 3 --max-products 30
```

## 📊 Danh mục hỗ trợ

- `dien-thoai`: Điện thoại di động
- `laptop`: Laptop, máy tính xách tay
- `tai-nghe`: Tai nghe
- `dong-ho-thong-minh`: Đồng hồ thông minh
- `phu-kien`: Phụ kiện công nghệ

## 📁 Cấu trúc output

Dữ liệu được lưu trong:
- `../data/`: Chứa file JSONL với dữ liệu sản phẩm
- `../logs/`: Chứa log files từng platform

Format dữ liệu:
```json
{
  "source": "lazada",
  "product_id": "123456",
  "url": "https://...",
  "crawl_date": "2025-10-24T...",
  "product_name": "iPhone 15 128GB",
  "brand": "Apple",
  "category": "Điện thoại",
  "description": "...",
  "image_urls": ["https://..."],
  "price_current": 25000000,
  "price_original": 30000000,
  "discount_percent": 16.67,
  "rating_avg": 4.8,
  "rating_count": 150,
  "sold_count": 50,
  "seller_name": "Apple Store",
  "seller_type": "Official Store"
}
```

## ⚙️ Tùy chỉnh crawler

### Thay đổi settings cơ bản:

Trong từng file crawler, bạn có thể tùy chỉnh:

```python
# Thay đổi delay time (giây)
delay_range=(2, 5)  # Min 2s, Max 5s

# Thay đổi số trang crawl
max_pages=3

# Thay đổi số sản phẩm tối đa
max_products=50
```

### Thêm selectors mới:

Nếu website thay đổi cấu trúc, cập nhật selectors trong methods `_extract_*`:

```python
def _extract_product_name(self, driver) -> str:
    selectors = [
        'h1.new-selector',  # Thêm selector mới
        'h1.old-selector',  # Giữ selector cũ
        # ...
    ]
```

## 🛡️ Best Practices

### 1. Tuân thủ robots.txt
```python
# Crawler tự động kiểm tra robots.txt
if not self.can_crawl(url):
    self.logger.warning("Robots.txt disallows crawling")
    continue
```

### 2. Rate limiting
```python
# Random delay giữa requests
self.random_delay()  # 2-7 giây ngẫu nhiên
```

### 3. Error handling
```python
try:
    product_data = crawler.extract_product_data(url)
    if product_data:
        products_data.append(product_data)
except Exception as e:
    logger.error(f"Error: {e}")
    continue  # Tiếp tục với sản phẩm khác
```

## 🐛 Troubleshooting

### 1. ChromeDriver issues
```bash
# Cập nhật webdriver-manager
pip install --upgrade webdriver-manager

# Hoặc tải Chrome driver thủ công
# https://chromedriver.chromium.org/
```

### 2. Anti-bot detection
- Tăng delay time: `delay_range=(5, 10)`
- Sử dụng mode sequential thay vì parallel
- Giảm số lượng products: `max_products=10`

### 3. Selectors outdated
- Kiểm tra HTML structure của website
- Cập nhật selectors trong method `_extract_*`
- Test với `simple_test.py` trước

### 4. Memory issues
```bash
# Giảm số lượng products
python crawler.py --max-products 20

# Chạy từng platform riêng
python lazada_crawler.py
```

## 📝 Logs và monitoring

### Xem logs:
```bash
# Logs của từng platform
tail -f ../logs/lazada_crawler.log
tail -f ../logs/tiki_crawler.log
```

### Log levels:
- `INFO`: Hoạt động bình thường
- `WARNING`: Không tìm thấy element (bình thường)
- `ERROR`: Lỗi nghiêm trọng cần kiểm tra

## 🔄 Update và maintenance

### 1. Cập nhật selectors định kỳ
Websites thường thay đổi cấu trúc → cần cập nhật selectors

### 2. Monitor robots.txt
Kiểm tra thay đổi rules crawling của từng site

### 3. Performance tuning
- Tối ưu delay times
- Cập nhật user agents
- Kiểm tra anti-bot measures mới

## 📞 Support

Nếu gặp vấn đề:

1. **Kiểm tra logs** trong `../logs/`
2. **Test base crawler** với `simple_test.py`
3. **Cập nhật dependencies** `pip install -r requirements.txt --upgrade`
4. **Kiểm tra selectors** bằng cách inspect element trên website

---

## 🎉 Kết luận

Bạn đã có:
- ✅ **7 crawler riêng biệt** cho từng platform
- ✅ **Base class chung** để dễ dàng mở rộng
- ✅ **Orchestrator** để chạy nhiều platform
- ✅ **Rate limiting** và tuân thủ robots.txt
- ✅ **Error handling** và logging chi tiết
- ✅ **Data format chuẩn** JSONL

**Crawler system hoàn chỉnh và sẵn sàng sử dụng!** 🚀