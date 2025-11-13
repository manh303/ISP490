import os

STORAGE_TYPE = os.getenv("STORAGE_TYPE", "minio")  # minio or s3

if STORAGE_TYPE == "s3":
    # Production: AWS S3
    ENDPOINT = None  # Use default AWS endpoint
    ACCESS_KEY = os.getenv("AKIAYZLPH4I7BTUCL6YS")
    SECRET_KEY = os.getenv("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")
    BUCKET = "ecommerce-crawler-prod"
    SECURE = True
else:
    # Development: MinIO
    ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
    ACCESS_KEY = "minioadmin"
    SECRET_KEY = "minioadmin123"
    BUCKET = "crawler-data"
    SECURE = False
