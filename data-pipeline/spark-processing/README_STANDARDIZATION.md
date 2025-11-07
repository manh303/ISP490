# E-commerce DSS Data Standardization Pipeline

## Tổng quan

Pipeline xử lý dữ liệu chuẩn hóa cho hệ thống DSS E-commerce, thực hiện theo đúng quy trình trong kiến trúc của bạn:

```
Data Sources → Data Cleaning → Data Standardization → Data Quality & Dedup →
Synchronize Identifier → Category Mapping → Technical Metadata → Data Warehouse
```

## Tính năng chính

### 1. Data Cleaning (Làm sạch dữ liệu)
- Loại bỏ ký tự đặc biệt và khoảng trắng thừa
- Chuẩn hóa encoding cho tiếng Việt
- Xử lý giá trị null và dữ liệu không hợp lệ
- Làm sạch trường giá và định dạng text

### 2. Data Standardization (Chuẩn hóa dữ liệu)
- Chuẩn hóa đơn vị giá về VND
- Chuẩn hóa tên địa điểm (Hà Nội, Hồ Chí Minh, ...)
- Chuẩn hóa format title và description
- Xử lý các trường dữ liệu khác nhau từ các nguồn

### 3. Data Quality & Dedup (Kiểm tra chất lượng & loại bỏ trùng lặp)
- Áp dụng ngưỡng chất lượng:
  - Giá tối thiểu: 1,000 VND
  - Giá tối đa: 500,000,000 VND
  - Title tối thiểu: 10 ký tự
- Loại bỏ sản phẩm trùng lặp dựa trên title + giá + nguồn

### 4. Synchronize Identifier (Tạo định danh duy nhất)
- Tạo product_id duy nhất cho mỗi sản phẩm
- Sử dụng hash MD5 từ title + price + source + url
- Format: `PROD_XXXXXXXXXXXXXXXX`

### 5. Category Mapping (Phân loại sản phẩm)
- Tự động phân loại sản phẩm dựa trên title
- Các category chính:
  - Điện thoại di động
  - Laptop
  - Máy tính bảng
  - Tai nghe
  - Phụ kiện
  - Và nhiều category khác

### 6. Technical Metadata (Thêm metadata kỹ thuật)
- Timestamp xử lý
- Version pipeline
- Quality score (60-100)
- Processing information

## Cách sử dụng

### Yêu cầu
- Docker và Docker Compose
- Spark cluster đang chạy (master + workers)
- Dữ liệu crawled ở `/data-collection/outputs/`

### Chạy Pipeline

#### Trên Windows:
```cmd
cd data-pipeline\spark-processing
run_standardization.bat
```

#### Trên Linux/Mac:
```bash
cd data-pipeline/spark-processing
chmod +x run_standardization.sh
./run_standardization.sh
```

#### Chạy thủ công:
```bash
# Copy script vào Spark container
docker cp standardization_pipeline.py spark-master:/opt/spark/apps/

# Chạy trong Spark container
docker exec spark-master python /opt/spark/apps/standardization_pipeline.py
```

## Đầu vào và Đầu ra

### Đầu vào (Input):
- `/opt/spark/data/raw/fixed_lazada_products.json` - Dữ liệu Lazada
- `/opt/spark/data/raw/fptshop_*.json` - Dữ liệu FPTShop
- `/opt/spark/data/raw/cellphones_*.jsonl` - Dữ liệu CellphoneS

### Đầu ra (Output):
- `/opt/spark/data/processed/standardized_products_YYYYMMDD_HHMMSS.parquet` - Dữ liệu chính (Parquet)
- `/opt/spark/data/processed/standardized_products_YYYYMMDD_HHMMSS.json` - Dữ liệu JSON
- `/opt/spark/data/processed/sample_standardized_products_YYYYMMDD_HHMMSS.json` - Mẫu dữ liệu
- `/opt/spark/data/processed/processing_report_YYYYMMDD_HHMMSS.json` - Báo cáo xử lý

## Schema đầu ra

```json
{
  "product_id": "PROD_A1B2C3D4E5F6G7H8",
  "title_standardized": "iPhone 15 Pro Max 256GB",
  "price_standardized": 35000000,
  "location_standardized": "Hà Nội",
  "category": "Điện thoại di động",
  "category_level_1": "Điện tử",
  "source": "lazada",
  "url": "https://...",
  "processed_at": "2024-11-05T10:30:00Z",
  "quality_score": 100,
  "processing_version": "1.0.0"
}
```

## Monitoring

### Logs
```bash
# Xem logs của Spark Master
docker logs spark-master

# Xem logs realtime
docker logs -f spark-master
```

### Spark UI
- Master UI: http://localhost:8081
- Worker 1 UI: http://localhost:8082
- Worker 2 UI: http://localhost:8083
- Application UI: http://localhost:4040 (khi chạy)

### Kiểm tra kết quả
```bash
# Xem files đã tạo
docker exec spark-master ls -la /opt/spark/data/processed/

# Xem sample data
docker exec spark-master head -20 /opt/spark/data/processed/sample_standardized_products_*.json

# Xem báo cáo
docker exec spark-master cat /opt/spark/data/processed/processing_report_*.json
```

## Troubleshooting

### Lỗi thường gặp:

1. **Spark cluster not accessible**
   ```bash
   docker-compose up -d
   ```

2. **Out of memory**
   - Tăng memory cho workers trong docker-compose.yml
   - Giảm số records xử lý trong một batch

3. **Data not found**
   - Kiểm tra đường dẫn data mounting trong docker-compose.yml
   - Đảm bảo files crawled tồn tại

4. **Permission denied**
   ```bash
   chmod +x run_standardization.sh
   ```

## Customization

### Thay đổi category mapping:
Chỉnh sửa `category_mapping` trong `standardization_pipeline.py`:

```python
self.category_mapping = {
    r'(điện thoại|smartphone)': 'Điện thoại di động',
    r'(laptop|macbook)': 'Laptop',
    # Thêm pattern mới
    r'(camera|máy ảnh)': 'Camera',
}
```

### Thay đổi quality thresholds:
```python
self.quality_thresholds = {
    'min_price': 1000,        # Giá tối thiểu
    'max_price': 500000000,   # Giá tối đa
    'min_title_length': 10,   # Độ dài title tối thiểu
}
```

## Performance

- Xử lý 100k records: ~5-10 phút
- Sử dụng 2 workers, mỗi worker 2GB RAM
- Tự động partition và optimize
- Hỗ trợ scale horizontal

## Next Steps

1. **Integration với Data Warehouse**: Kết nối output với PostgreSQL/ClickHouse
2. **Real-time Processing**: Chuyển sang Kafka Streaming
3. **ML Pipeline**: Thêm feature engineering cho machine learning
4. **Data Quality Dashboard**: Monitor quality metrics realtime