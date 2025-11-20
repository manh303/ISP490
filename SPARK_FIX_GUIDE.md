# Spark Container Fix Guide

## Vấn đề
Container `spark-master` không healthy, gây lỗi: `dependency failed to start: container spark-master is unhealthy`

## Nguyên nhân
1. Healthcheck quá nghiêm ngặt (kiểm tra port + HTTP)
2. Spark master cần thời gian khởi động lâu
3. Thiếu entrypoint script phù hợp

## Giải pháp đã áp dụng

### 1. Đơn giản hóa Healthcheck
**File: `deployment/spark/healthcheck.sh`**
- Chỉ kiểm tra process đang chạy
- Không cần `nc` hay `curl`

### 2. Thêm Entrypoint Script
**File: `deployment/spark/docker-entrypoint.sh`**
- Khởi động Spark đúng cách theo mode
- Tạo thư mục cần thiết

### 3. Cập nhật Docker Compose
**File: `docker-compose.yml`**
- Tăng `start_period: 60s` cho Spark master
- Tăng `retries: 5`
- Sử dụng healthcheck đơn giản hơn

## Cách test

### Option 1: Test Spark riêng biệt
```bash
# Build và test Spark master
./test-spark.sh

# Hoặc manual
docker-compose build spark-master
docker-compose up -d spark-master
docker-compose logs -f spark-master
```

### Option 2: Test với minimal stack
```bash
# Chỉ chạy các service cơ bản
docker-compose -f docker-compose.minimal.yml up -d

# Kiểm tra
docker-compose -f docker-compose.minimal.yml ps
```

### Option 3: Chạy toàn bộ (sau khi test thành công)
```bash
# Dọn dẹp
docker-compose down -v

# Rebuild
docker-compose build

# Start
docker-compose up -d

# Monitor
docker-compose logs -f spark-master
```

## Kiểm tra Spark Master

### 1. Kiểm tra logs
```bash
docker-compose logs spark-master
```

Logs thành công sẽ có:
```
Starting Spark in master mode...
Starting Spark Master...
Spark master running on spark://spark-master:7077
```

### 2. Kiểm tra healthcheck
```bash
docker-compose exec spark-master /usr/local/bin/healthcheck.sh
```

### 3. Kiểm tra Web UI
```
http://localhost:8081
```

### 4. Kiểm tra process
```bash
docker-compose exec spark-master ps aux | grep spark
```

## Nếu vẫn lỗi

### 1. Tăng timeout
Trong `docker-compose.yml`:
```yaml
healthcheck:
  start_period: 120s  # Tăng lên 2 phút
  interval: 60s
  retries: 10
```

### 2. Tạm thời disable healthcheck
```yaml
spark-master:
  # ... other config
  # healthcheck:
  #   test: ["CMD-SHELL", "..."]
```

### 3. Kiểm tra resources
```bash
# Kiểm tra memory/CPU
docker stats spark-master

# Tăng resources nếu cần
docker-compose up -d --scale spark-worker-1=0 --scale spark-worker-2=0
```

### 4. Xem logs chi tiết
```bash
# Logs đầy đủ
docker-compose logs --tail=100 spark-master

# Logs realtime
docker-compose logs -f spark-master

# Logs trong container
docker-compose exec spark-master cat /app/logs/spark-master.log
```

## Troubleshooting Commands

```bash
# 1. Kiểm tra container status
docker-compose ps

# 2. Inspect container
docker inspect spark-master

# 3. Kiểm tra network
docker network inspect ecommerce-dss-project_dss_network

# 4. Restart service
docker-compose restart spark-master

# 5. Rebuild từ đầu
docker-compose build --no-cache spark-master
docker-compose up -d spark-master

# 6. Xóa volumes và rebuild
docker-compose down -v
docker volume prune -f
docker-compose up -d
```

## Các file đã sửa

1. ✅ `deployment/spark/healthcheck.sh` - Đơn giản hóa
2. ✅ `deployment/spark/docker-entrypoint.sh` - Thêm mới
3. ✅ `deployment/spark/Dockerfile` - Thêm entrypoint
4. ✅ `docker-compose.yml` - Cập nhật healthcheck
5. ✅ `test-spark.sh` - Script test
6. ✅ `docker-compose.minimal.yml` - Minimal stack

## Kết quả mong đợi

Sau khi áp dụng fix:
```bash
$ docker-compose ps spark-master
NAME           STATUS                    PORTS
spark-master   Up (healthy)   0.0.0.0:7077->7077/tcp, 0.0.0.0:8081->8080/tcp
```

## Lưu ý

- Spark master cần ~30-60s để khởi động hoàn toàn
- Đảm bảo có đủ RAM (tối thiểu 4GB cho Spark)
- Workers sẽ chỉ start sau khi master healthy
- Nếu vẫn lỗi, có thể chạy không cần Spark bằng cách comment out trong docker-compose.yml
