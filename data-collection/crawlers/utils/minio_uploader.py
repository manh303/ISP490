from minio import Minio
from datetime import datetime
import json
import io
from .storage_config import ENDPOINT, ACCESS_KEY, SECRET_KEY, BUCKET, SECURE

class MinIOUploader:
    def __init__(self):
        if ENDPOINT:
            self.client = Minio(ENDPOINT, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=SECURE)
        else:
            # AWS S3
            self.client = Minio("s3.amazonaws.com", access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=True)
        self.bucket = BUCKET
        self._ensure_bucket()
    
    def _ensure_bucket(self):
        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket)
    
    def upload_jsonl(self, data, platform, category, date=None):
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")
        
        jsonl = "\n".join([json.dumps(item, ensure_ascii=False) for item in data])
        timestamp = datetime.now().strftime("%H%M%S")
        object_name = f"{platform}/date={date}/{category}_{timestamp}.jsonl"
        
        self.client.put_object(
            self.bucket, object_name,
            io.BytesIO(jsonl.encode('utf-8')),
            len(jsonl.encode('utf-8')),
            content_type='application/x-ndjson'
        )
        
        return f"s3a://{self.bucket}/{object_name}"
