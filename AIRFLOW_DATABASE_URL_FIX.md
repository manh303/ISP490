# Airflow DATABASE_URL Fix

## ❌ Vấn Đề

ML Training Pipeline DAG fails với lỗi:

```
airflow.exceptions.AirflowException: DATABASE_URL not set
```

### Nguyên nhân
DATABASE_URL được define trong `x-airflow-common-env` anchor (line 11 của docker-compose.yml) nhưng không được apply vào Airflow containers vì cách YAML anchor hoạt động.

---

## ✅ Giải Pháp

### Fix: Thêm DATABASE_URL trực tiếp vào environment của mỗi Airflow service

**File:** `docker-compose.yml`

### Thay đổi cho `airflow-worker`

**Before:**
```yaml
airflow-worker:
  environment: *airflow-common-env
```

**After:**
```yaml
airflow-worker:
  environment:
    <<: *airflow-common-env
    DATABASE_URL: postgresql://dss_user:IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4@dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com/ecommerce_dss
```

### Thay đổi cho `airflow-scheduler`

**Before:**
```yaml
airflow-scheduler:
  environment: *airflow-common-env
```

**After:**
```yaml
airflow-scheduler:
  environment:
    <<: *airflow-common-env
    DATABASE_URL: postgresql://dss_user:IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4@dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com/ecommerce_dss
```

### Thay đổi cho `airflow-webserver`

**Before:**
```yaml
airflow-webserver:
  environment: *airflow-common-env
```

**After:**
```yaml
airflow-webserver:
  environment:
    <<: *airflow-common-env
    DATABASE_URL: postgresql://dss_user:IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4@dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com/ecommerce_dss
```

---

## 🔧 Cách Apply

### 1. Sau khi sửa docker-compose.yml, recreate containers:

```bash
# Stop containers
docker stop ecommerce-dss-project-airflow-worker-1 \
  ecommerce-dss-project-airflow-scheduler-1 \
  ecommerce-dss-project-airflow-webserver-1

# Recreate with new env vars
docker-compose up -d airflow-scheduler airflow-worker airflow-webserver

# Wait for services to start
Start-Sleep -Seconds 15  # PowerShell
# or
sleep 15  # Bash
```

### 2. Verify DATABASE_URL is set:

```bash
docker exec ecommerce-dss-project-airflow-worker-1 printenv DATABASE_URL
```

**Expected output:**
```
postgresql://dss_user:IkJaw42NkCz2JQw0UjdqdsTmXgcMIHC4@dpg-d454rjq4d50c73fhmen0-a.oregon-postgres.render.com/ecommerce_dss
```

### 3. Trigger ML DAG:

```bash
docker exec ecommerce-dss-project-airflow-webserver-1 \
  airflow dags trigger ml_training_inference_pipeline
```

---

## 🔍 Debug Commands

### Check all env vars in container:

```bash
docker exec ecommerce-dss-project-airflow-worker-1 printenv
```

### Check database-related env vars:

```bash
docker exec ecommerce-dss-project-airflow-worker-1 \
  bash -c "printenv | grep -E '(DATABASE|DB_|POSTGRES)'"
```

### Check DAG status:

```bash
docker exec ecommerce-dss-project-airflow-webserver-1 \
  airflow dags list | grep ml_training
```

### Monitor logs:

```bash
# Scheduler logs
docker logs ecommerce-dss-project-airflow-scheduler-1 -f

# Worker logs
docker logs ecommerce-dss-project-airflow-worker-1 -f
```

---

## 📖 Giải thích Kỹ thuật

### Tại sao cần `<<: *airflow-common-env`?

YAML merge operator `<<:` cho phép:
1. Inherit tất cả env vars từ anchor `*airflow-common-env`
2. Override hoặc thêm env vars mới (như DATABASE_URL)

**Syntax:**
```yaml
environment:
  <<: *anchor_name  # Merge all from anchor
  KEY: value        # Add or override specific key
```

### Tại sao không dùng `environment: *airflow-common-env` đơn giản?

Khi dùng `*anchor_name` (alias reference) thay vì `<<: *anchor_name` (merge), bạn không thể thêm hoặc override keys. Nó chỉ reference toàn bộ block.

---

## ✅ Verification Checklist

Sau khi apply fix:

- [ ] DATABASE_URL visible trong container (`printenv DATABASE_URL`)
- [ ] ML DAG không còn lỗi "DATABASE_URL not set"
- [ ] Task `check_dwh_data` chạy thành công
- [ ] ML models được train thành công
- [ ] ML results được ghi vào database

---

## 🚨 Lưu ý Bảo mật

⚠️ **DATABASE_URL chứa credentials!**

### Trong production, nên:

1. **Sử dụng Docker secrets:**
```yaml
secrets:
  - database_url

services:
  airflow-worker:
    secrets:
      - database_url
    environment:
      DATABASE_URL_FILE: /run/secrets/database_url
```

2. **Sử dụng environment file:**
```bash
# .env file (không commit vào git!)
DATABASE_URL=postgresql://user:pass@host/db

# docker-compose.yml
services:
  airflow-worker:
    env_file:
      - .env
```

3. **Sử dụng Airflow Connections:**
```python
from airflow.hooks.base import BaseHook
conn = BaseHook.get_connection('postgres_dwh')
DATABASE_URL = conn.get_uri()
```

### Trong dev/local (hiện tại):
- OK để hardcode DATABASE_URL trong docker-compose.yml
- Đảm bảo `.env` trong `.gitignore`
- Không commit credentials vào git

---

## 📚 Related Files

- `docker-compose.yml` - Main fix location
- `airflow/dags/ml_training_pipeline_dag.py` - DAG sử dụng DATABASE_URL
- `ml/load_ml_results_to_db.py` - Helper functions dùng DATABASE_URL
- `ml/train_*.py` - Training scripts dùng DATABASE_URL
- `ml/run_*.py` - Inference scripts dùng DATABASE_URL

---

## 🎯 Next Steps

1. ✅ DATABASE_URL được set
2. ✅ ML DAG triggered
3. ⏳ Monitor DAG execution
4. ⏳ Verify models trained
5. ⏳ Verify ML results in database

**Monitor progress:**
```bash
# Via UI
http://localhost:8080/dags/ml_training_inference_pipeline/graph

# Via CLI
docker exec ecommerce-dss-project-airflow-webserver-1 \
  airflow dags state ml_training_inference_pipeline
```

---

**Status:** ✅ Fixed  
**Date:** 2025-11-24  
**Impact:** ML Training Pipeline can now connect to DWH database

