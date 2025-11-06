# Enhanced Lazada Crawler

## 🎯 Tính năng mới

### ✅ **Đã sửa lỗi shop_name**
- Trước: Lấy nhầm từ sold_count/review_count trên listing page
- Sau: Lấy chính xác từ product detail page (`.seller-name-v2__detail-name`)

### ✅ **Lấy thông tin shop chi tiết**
- **Shop name**: Tên chính xác từ product page
- **Shop rating**: Seller Ratings X%
- **Shop badges**: Top Seller, Preferred Seller, etc.

### ✅ **Lấy reviews từ product detail**
- **Reviewer**: Tên người review (ẩn danh)
- **Rating**: Số sao (1-5)
- **Time**: Thời gian review
- **Content**: Nội dung review
- **SKU info**: Màu sắc, kích thước đã mua
- **Helpful count**: Số người thấy hữu ích

### ✅ **Chỉ lấy image URL**
- Không lấy base64 nữa
- Chỉ lấy links từ Lazada CDN

### ✅ **Tách riêng review_count và sold_count**
- `review_count`: Số reviews thực tế
- `sold_count`: Số lượng đã bán

## 📊 Schema dữ liệu mới

```json
{
  "category": "smartphones",
  "title": "iPhone 15 Pro Max 256GB",
  "url": "https://www.lazada.vn/products/...",
  "image_url": "https://img.lazcdn.com/g/p/...",
  "price_text": "35.000.000 ₫",
  "price": 35000000,
  "rating": 4.5,
  "review_count": "123",
  "sold_count": "1.2k",
  "location": "Hà Nội",

  "shop_name": "Xiaomi Store1",
  "shop_rating": "96%",
  "shop_badges": ["Top18 Seller for Smartphones", "Preferred Seller"],

  "reviews": [
    {
      "reviewer": "0***2",
      "rating": 5,
      "time": "2 weeks ago",
      "content": "note 14 pro 🥹🥹🥹",
      "sku_info": "Màu sắc: Xanh",
      "helpful_count": 0
    }
  ],

  "review_summary": {
    "total_reviews": 10,
    "average_rating": 4.3
  }
}
```

## 🚀 Cách sử dụng

### 1. **Chạy test với 1 category**
```bash
cd data-collection/crawlers/lazada/runners/
python enhanced_lazada_crawler.py
```

### 2. **Chạy với tùy chọn**
```bash
# Crawl smartphones với details
python run_enhanced_crawler.py --category smartphones --pages 3 --products-per-page 10

# Crawl laptops nhanh (không lấy details)
python run_enhanced_crawler.py --category laptops --pages 5 --products-per-page 20 --no-details

# Crawl tất cả categories
python run_enhanced_crawler.py --all-categories --pages 2 --products-per-page 5

# Custom output filename
python run_enhanced_crawler.py --category smartphones --output-prefix "my_lazada_data"
```

### 3. **Available categories**
- `smartphones` - Điện thoại
- `laptops` - Laptop
- `tablets` - Máy tính bảng
- `smartwatches` - Đồng hồ thông minh
- `tvs` - Tivi
- `headphones` - Tai nghe
- `cameras` - Máy ảnh
- `monitors` - Màn hình
- `destops-computers` - Máy tính để bàn

## 📁 Output files

### **1. Complete JSON** - `enhanced_lazada_complete_TIMESTAMP.json`
Dữ liệu đầy đủ với tất cả thông tin shop và reviews

### **2. Main CSV** - `enhanced_lazada_main_TIMESTAMP.csv`
Các trường chính cho phân tích:
- category, title, url, image_url
- price_text, price, rating
- review_count, sold_count, location
- shop_name, shop_rating, discount

### **3. Reviews JSON** - `enhanced_lazada_reviews_TIMESTAMP.json`
Reviews riêng biệt với thông tin sản phẩm:
- product_title, product_url, shop_name
- reviewer, rating, time, content, sku_info

## ⚡ Performance & Settings

### **Tốc độ crawling**
- **Với details**: ~10-15 giây/sản phẩm (do phải vào từng product page)
- **Không details**: ~1-2 giây/sản phẩm (chỉ listing page)

### **Recommended settings**
```bash
# Test nhanh
--pages 1 --products-per-page 5

# Crawl vừa phải
--pages 3 --products-per-page 10

# Crawl đầy đủ (chậm)
--pages 10 --products-per-page 20
```

### **Anti-detection**
- Random delays between requests (1-6 seconds)
- Real Chrome user agent
- Disable automation flags
- Scroll simulation

## 🔧 Troubleshooting

### **Lỗi thường gặp:**

1. **ChromeDriver not found**
   ```bash
   # Download ChromeDriver và đặt vào PATH
   # Hoặc cài đặt qua pip
   pip install webdriver-manager
   ```

2. **Timeout errors**
   - Tăng timeout trong WebDriverWait
   - Giảm số products per page
   - Kiểm tra internet connection

3. **Captcha/blocking**
   - Tăng random delays
   - Đổi user agent
   - Restart crawler sau một thời gian

4. **Memory issues**
   - Giảm số pages/products
   - Restart browser định kỳ

### **Debug mode**
Thêm print statements trong code để debug:
```python
print(f"🔍 Current URL: {self.driver.current_url}")
print(f"📊 Page title: {self.driver.title}")
```

## 📈 So sánh với crawler cũ

| Feature | Old Crawler | Enhanced Crawler |
|---------|-------------|------------------|
| Shop name | ❌ Sai (lấy từ listing) | ✅ Đúng (từ detail page) |
| Shop info | ❌ Không có | ✅ Rating, badges |
| Reviews | ❌ Không có | ✅ Chi tiết đầy đủ |
| Image | ❌ Base64 lớn | ✅ URL link |
| Data quality | ⚠️ Trung bình | ✅ Cao |
| Speed | ✅ Nhanh | ⚠️ Chậm hơn (do details) |

## 🎯 Next Steps

1. **Integration với Data Pipeline**
   - Chạy enhanced crawler
   - Feed vào Spark standardization pipeline
   - Process shop và review data

2. **Thêm features**
   - Product specifications
   - Seller verification status
   - Price history tracking
   - More review metadata

3. **Optimization**
   - Parallel processing
   - Database caching
   - Incremental updates