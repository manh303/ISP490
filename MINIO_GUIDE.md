# Hướng dẫn sử dụng MinIO

## 1. Start MinIO
```bash
docker-compose up -d minio
```

## 2. Truy cập MinIO Console
- URL: http://localhost:9001
- Username: `minioadmin`
- Password: `minioadmin123`

## 3. Upload dữ liệu từ Python

```python
from minio import Minio
import json, io

# Kết nối
client = Minio("localhost:9000", "minioadmin", "minioadmin123", secure=False)

# Tạo bucket
if not client.bucket_exists("crawler-data"):
    client.make_bucket("crawler-data")

# Upload JSONL
data = [{"id": 1, "name": "Product 1"}]
jsonl = "\n".join([json.dumps(d) for d in data])

client.put_object(
    "crawler-data",
    "tiki/date=2025-11-14/products.jsonl",
    io.BytesIO(jsonl.encode('utf-8')),
    len(jsonl.encode('utf-8'))
)
```

## 4. Đọc từ Spark

```python
# Cấu hình Spark
spark.conf.set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
spark.conf.set("spark.hadoop.fs.s3a.access.key", "minioadmin")
spark.conf.set("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# Đọc data
df = spark.read.json("s3a://crawler-data/tiki/date=2025-11-14/*.jsonl")
df.show()
```

## 5. Cấu trúc thư mục khuyến nghị

```
crawler-data/
├── tiki/
│   ├── date=2025-11-14/
│   │   ├── laptop_150320.jsonl
│   │   └── dien_thoai_150430.jsonl
│   └── date=2025-11-15/
└── lazada/
    └── date=2025-11-14/
        └── products_160520.jsonl
```

## 6. Test upload
```bash
python test_minio_upload.py
```

## 7. Xem dữ liệu trong Console
1. Vào http://localhost:9001
2. Login với minioadmin/minioadmin123
3. Click **Buckets** → **crawler-data**
4. Browse files theo date partition
