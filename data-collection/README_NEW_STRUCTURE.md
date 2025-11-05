# Data Collection - Cấu Trúc Mới

## Tổng Quan Cấu Trúc

```
data-collection/
├─ crawlers/
│  ├─ lazada/
│  │  ├─ configs/           # *.yaml: cấu hình seed, rate-limit, headers
│  │  ├─ schemas/           # schema snapshot *.json (raw fields)
│  │  ├─ seeds/             # danh mục, từ khóa, URL khởi tạo
│  │  ├─ runners/           # entrypoints: run_category.py, run_detail.py
│  │  └─ utils/             # url_canonical.py, hashing.py, manifest.py
│  ├─ fptshop/             # cấu trúc tương tự
│  ├─ cellphones/          # cấu trúc tương tự
│  ├─ tiki/                # cấu trúc tương tự (chuẩn bị sẵn)
│  └─ hoanghamobile/       # cấu trúc tương tự
├─ manifests/              # manifest *.json sinh trong run (bản local)
├─ logs/                   # file log theo run/source
├─ checkpoints/            # state chống crawl trùng (last_page, cursors)
└─ outputs/                # buffer tạm trước khi upload S3/MinIO
```

## Chi Tiết Từng Thư Mục

### crawlers/{platform}/
Mỗi platform có cấu trúc riêng biệt:

#### configs/
- `default.yaml`: Cấu hình mặc định (rate limit, headers, timeouts)
- `production.yaml`: Cấu hình production
- `debug.yaml`: Cấu hình debug

#### schemas/
- `product.json`: JSON schema cho product data
- `category.json`: JSON schema cho category data
- `review.json`: JSON schema cho review data

#### seeds/
- `categories.json`: Danh sách categories và URLs
- `keywords.json`: Keywords tìm kiếm
- `test_urls.json`: URLs test

#### runners/
- `run_category.py`: Crawler cho categories
- `run_detail.py`: Crawler cho product details
- `run_search.py`: Crawler cho search
- Các file crawler được di chuyển từ cấu trúc cũ

#### utils/
- `url_canonical.py`: Chuẩn hóa URLs
- `hashing.py`: Hash cho deduplication
- `manifest.py`: Quản lý manifest files

### manifests/
Chứa manifest files cho mỗi run:
- `{platform}_{type}_{timestamp}.json`
- Tracking stats, URLs crawled, errors

### logs/
File logs theo run và source:
- `{run_id}.log`
- `{platform}_{date}.log`

### checkpoints/
State files để tránh crawl trùng:
- `{platform}_last_page.json`
- `{platform}_cursors.json`

### outputs/
Buffer tạm trước khi upload:
- `{run_id}_products.jsonl`
- `{run_id}_categories.json`

## Cách Sử Dụng

### Chạy Crawler
```bash
# Lazada categories
python data-collection/crawlers/lazada/runners/run_category.py

# FPTShop products
python data-collection/crawlers/fptshop/runners/run_detail.py

# Với config tùy chỉnh
python data-collection/crawlers/lazada/runners/run_category.py --config custom.yaml
```

### Import Utils
```python
from crawlers.lazada.utils.manifest import CrawlManifest
from crawlers.lazada.utils.hashing import hash_product
from crawlers.lazada.utils.url_canonical import canonicalize_product_url
```

## Lợi Ích Cấu Trúc Mới

1. **Tách biệt rõ ràng**: Mỗi platform có thư mục riêng
2. **Tái sử dụng code**: Utils có thể share giữa các platform
3. **Cấu hình linh hoạt**: YAML configs cho từng môi trường
4. **Tracking tốt hơn**: Manifests và checkpoints chi tiết
5. **Mở rộng dễ dàng**: Thêm platform mới chỉ cần copy structure
6. **Debug hiệu quả**: Logs và outputs tách biệt rõ ràng