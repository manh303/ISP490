# Docker Setup cho React Admin Dashboard

## Cách chạy ứng dụng với Docker

### 1. Build và chạy với docker-compose (Khuyến nghị)

```bash
# Build và chạy ứng dụng
docker-compose up --build

# Chạy ở background
docker-compose up -d --build

# Dừng ứng dụng
docker-compose down
```

### 2. Build và chạy với Docker thông thường

```bash
# Build image
docker build -t react-admin-dashboard .

# Chạy container
docker run -p 3000:80 react-admin-dashboard
```

### Truy cập ứng dụng

Sau khi chạy thành công, truy cập: http://localhost:3000

## Cấu trúc Docker

- **Dockerfile**: Multi-stage build với Node.js (build) và Nginx (production)
- **docker-compose.yml**: Orchestration cho development/production
- **nginx.conf**: Cấu hình Nginx cho SPA routing
- **.dockerignore**: Loại trừ các file không cần thiết

## Lưu ý

- Port mặc định: 3000
- Nginx được cấu hình để handle React Router
- Static assets được cache 1 năm
- Gzip compression được bật

## Troubleshooting

Nếu gặp lỗi port đã được sử dụng:
```bash
# Thay đổi port trong docker-compose.yml
ports:
  - "8080:80"  # Thay vì 3000:80
```