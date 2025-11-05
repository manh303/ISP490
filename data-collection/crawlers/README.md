# Multi-Platform E-commerce Crawlers

Bộ crawler tự động thu thập dữ liệu sản phẩm từ các sàn thương mại điện tử và cửa hàng bán lẻ điện tử tại Việt Nam.

## 📋 Danh sách Platform được hỗ trợ

### Sàn TMĐT:
- **Lazada** (`lazada_crawler.py`)
- **Tiki** (`tiki_crawler.py`)
- **Sendo** (`sendo_crawler.py`)

### Nhà bán lẻ điện tử:
- **CellphoneS** (`cellphones_crawler.py`)
- **FPTShop** (`fptshop_crawler.py`)
- **Thế Giới Di Động** (`thegioididong_crawler.py`)
- **Hoàng Hà Mobile** (`hoanghamobile_crawler.py`)

## 🏗️ Cấu trúc thư mục

```
crawlers/
├── __init__.py
├── base_crawler.py              # Lớp cơ sở chung
├── lazada_crawler.py            # Crawler cho Lazada
├── tiki_crawler.py              # Crawler cho Tiki
├── sendo_crawler.py             # Crawler cho Sendo
├── cellphones_crawler.py        # Crawler cho CellphoneS
├── fptshop_crawler.py           # Crawler cho FPTShop
├── thegioididong_crawler.py     # Crawler cho Thế Giới Di Động
├── hoanghamobile_crawler.py     # Crawler cho Hoàng Hà Mobile
├── multi_platform_orchestrator.py # Orchestrator chạy nhiều platform
├── requirements.txt             # Dependencies
└── README.md                   # Tài liệu này
```

## 🚀 Cài đặt và sử dụng

### 1. Cài đặt dependencies

```bash
cd data-collection/crawlers
pip install -r requirements.txt
```

### 2. Cài đặt ChromeDriver

Crawlers sử dụng Selenium với Chrome WebDriver. Đảm bảo đã cài đặt:
- Google Chrome browser
- ChromeDriver (có thể tự động tải qua webdriver-manager)

### 3. Chạy crawler đơn lẻ

```bash
# Crawl Lazada điện thoại
python lazada_crawler.py

# Crawl Tiki laptop
python tiki_crawler.py
```

### 4. Chạy multi-platform crawler

```bash
# Crawl tất cả platforms cho điện thoại (mode tuần tự)
python multi_platform_orchestrator.py --categories dien-thoai --mode sequential

# Crawl một số platforms cụ thể
python multi_platform_orchestrator.py --platforms lazada tiki sendo --categories dien-thoai laptop

# Crawl song song (nhanh hơn nhưng có thể bị chặn)
python multi_platform_orchestrator.py --categories dien-thoai --mode parallel

# Tùy chỉnh số trang và sản phẩm
python multi_platform_orchestrator.py --categories dien-thoai --max-pages 3 --max-products 50
```

## 📊 Danh mục sản phẩm được hỗ trợ

- `dien-thoai`: Điện thoại di động
- `laptop`: Laptop, máy tính xách tay
- `tai-nghe`: Tai nghe
- `dong-ho-thong-minh`: Đồng hồ thông minh
- `phu-kien`: Phụ kiện công nghệ

## 🎯 Dữ liệu thu thập được

Mỗi sản phẩm sẽ có các trường thông tin sau:

```json
{
  "source": "tiki",
  "product_id": "123456",
  "url": "https://tiki.vn/dien-thoai-xyz.html",
  "crawl_date": "2025-10-24T09:00:00Z",
  "product_name": "Điện thoại XYZ 128GB",
  "brand": "BrandA",
  "category": "Điện thoại di động",
  "description": "Màn hình lớn, camera rõ nét, pin 5000mAh.",
  "image_urls": ["https://img.tiki.vn/xyz1.jpg"],
  "price_current": 8990000,
  "price_original": 10990000,
  "discount_percent": 18.2,
  "rating_avg": 4.6,
  "rating_count": 1520,
  "sold_count": 3200,
  "favorite_count": 500,
  "seller_name": "BrandA Official",
  "seller_type": "Official Store"
}
```

## ⚙️ Tính năng chính

### 🛡️ Tuân thủ robots.txt
- Tự động kiểm tra và tuân thủ robots.txt của từng website
- Tôn trọng quy định về crawling của các platform

### 🚦 Rate Limiting thông minh
- Delay ngẫu nhiên giữa các request (2-7 giây)
- User-agent rotation
- Tránh overload server

### 🎭 Anti-bot evasion
- Headless browser simulation
- Random delays
- Multiple user agents
- Realistic browsing patterns

### 📁 Lưu trữ dữ liệu
- Format JSONL (mỗi dòng là một JSON object)
- Tự động tạo thư mục `data-collection/data/`
- Timestamp trong tên file

### 📝 Logging chi tiết
- Log file riêng cho từng platform
- Theo dõi tiến trình crawling
- Error handling và retry logic

## 🎛️ Tùy chọn Orchestrator

```bash
python multi_platform_orchestrator.py [options]

Options:
  --platforms PLATFORM [PLATFORM ...]
                        Platforms to crawl: lazada, tiki, sendo, cellphones,
                        fptshop, thegioididong, hoanghamobile, all
  --categories CATEGORY [CATEGORY ...]
                        Categories to crawl: dien-thoai, laptop, tai-nghe,
                        dong-ho-thong-minh, phu-kien, all
  --max-pages N         Maximum pages per category (default: 2)
  --max-products N      Maximum products per category (default: 20)
  --mode {sequential,parallel}
                        Crawling mode (default: sequential)
  --output FILENAME     Output filename
```

## 📋 Ví dụ sử dụng

### Crawl cơ bản
```bash
# Crawl điện thoại từ tất cả platforms
python multi_platform_orchestrator.py --categories dien-thoai

# Crawl laptop từ 3 sàn chính
python multi_platform_orchestrator.py --platforms lazada tiki sendo --categories laptop
```

### Crawl nâng cao
```bash
# Crawl nhiều danh mục với nhiều trang
python multi_platform_orchestrator.py --categories dien-thoai laptop tai-nghe --max-pages 3 --max-products 30

# Crawl song song (nhanh hơn)
python multi_platform_orchestrator.py --categories dien-thoai --mode parallel --max-workers 2
```

## ⚠️ Lưu ý quan trọng

### Tuân thủ pháp luật
- Chỉ crawl dữ liệu công khai
- Tuân thủ robots.txt và terms of service
- Không overload server
- Sử dụng với mục đích nghiên cứu/phân tích

### Hiệu suất
- Mode `sequential` an toàn hơn, ít bị chặn
- Mode `parallel` nhanh hơn nhưng có rủi ro
- Điều chỉnh delay phù hợp với từng platform

### Xử lý lỗi
- Tự động retry khi gặp lỗi tạm thời
- Skip sản phẩm lỗi, tiếp tục crawl
- Log chi tiết để debug

## 🔧 Tùy chỉnh crawler

Để thêm platform mới hoặc tùy chỉnh:

1. Tạo class kế thừa từ `BaseCrawler`
2. Override các method cần thiết
3. Thêm vào `multi_platform_orchestrator.py`

```python
from base_crawler import BaseCrawler

class NewPlatformCrawler(BaseCrawler):
    def __init__(self):
        super().__init__(
            source_name="newplatform",
            base_url="https://newplatform.com",
            delay_range=(2, 4)
        )
```

## 📊 Output

Dữ liệu được lưu trong thư mục `data-collection/data/`:
- Mỗi platform/category tạo file riêng
- File tổng hợp khi dùng orchestrator
- Format JSONL để dễ dàng import vào database

## 🐛 Troubleshooting

### ChromeDriver issues
```bash
# Cập nhật webdriver-manager
pip install --upgrade webdriver-manager
```

### Anti-bot detection
- Tăng delay time
- Sử dụng mode sequential
- Kiểm tra user-agent

### Memory issues
- Giảm max_products
- Chạy từng platform riêng lẻ