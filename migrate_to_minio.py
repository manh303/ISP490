from minio import Minio
from pathlib import Path
import sys

sys.stdout.reconfigure(encoding='utf-8')

client = Minio("localhost:9000", "minioadmin", "minioadmin123", secure=False)
bucket = "crawler-data"

if not client.bucket_exists(bucket):
    client.make_bucket(bucket)
    print(f"Created bucket: {bucket}")

# Thư mục trong Docker container
docker_output_dir = Path("c:/DoAn_FPT_FALL2025/ecommerce-dss-project/data/outputs")

if not docker_output_dir.exists():
    print(f"ERROR: {docker_output_dir} not found")
    sys.exit(1)

uploaded = 0
for jsonl_file in docker_output_dir.rglob("*.jsonl"):
    # Extract: outputs/tiki/date=2025-11-14/file.jsonl -> tiki/date=2025-11-14/file.jsonl
    relative_path = jsonl_file.relative_to(docker_output_dir)
    object_name = str(relative_path).replace("\\", "/")
    
    client.fput_object(bucket, object_name, str(jsonl_file))
    print(f"OK {object_name}")
    uploaded += 1

print(f"\nTotal uploaded: {uploaded} files")
print(f"View at: http://localhost:9001")
