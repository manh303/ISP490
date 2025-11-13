from minio import Minio
from datetime import datetime
import json
import io

# Kết nối MinIO
client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin123",
    secure=False
)

# Tạo bucket
bucket = "crawler-data"
if not client.bucket_exists(bucket):
    client.make_bucket(bucket)
    print(f" Created bucket: {bucket}")
else:
    print(f" Bucket exists: {bucket}")

# Sample data
products = [
    {"product_id": "1", "name": "Laptop Dell", "price": 15000000},
    {"product_id": "2", "name": "iPhone 15", "price": 25000000},
    {"product_id": "3", "name": "Samsung TV", "price": 12000000}
]

# Convert to JSONL
jsonl = "\n".join([json.dumps(p, ensure_ascii=False) for p in products])

# Upload
date = datetime.now().strftime("%Y-%m-%d")
object_name = f"tiki/date={date}/test_products.jsonl"

client.put_object(
    bucket, object_name,
    io.BytesIO(jsonl.encode('utf-8')),
    len(jsonl.encode('utf-8')),
    content_type='application/x-ndjson'
)

print(f" Uploaded: s3a://{bucket}/{object_name}")
print(f"\n View in MinIO Console: http://localhost:9001")
print(f"   Username: minioadmin")
print(f"   Password: minioadmin123")
