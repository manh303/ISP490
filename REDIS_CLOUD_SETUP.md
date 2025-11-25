# Hướng dẫn kết nối Redis Cloud với Render

## Bước 1: Lấy Redis URL từ Cloud Service

### Nếu dùng Redis Cloud (redis.com):
1. Đăng nhập vào [Redis Cloud Console](https://redis.com/try-free/redis-enterprise-cloud/)
2. Chọn database của bạn
3. Vào tab **"Configuration"** hoặc **"Connect"**
4. Copy **"Public endpoint"** hoặc **"Redis URL"**
5. Format thường là: `redis://default:password@host:port`

### Nếu dùng Upstash:
1. Đăng nhập vào [Upstash Console](https://console.upstash.com/)
2. Chọn Redis database
3. Vào tab **"Details"**
4. Copy **"REST URL"** hoặc **"Redis URL"**
5. Format thường là: `rediss://default:password@host:port` (lưu ý: `rediss` với SSL)

### Nếu dùng Redis Labs hoặc dịch vụ khác:
- Tìm trong dashboard phần **"Connection String"** hoặc **"Endpoint"**
- Format thường là: `redis://:password@host:port` hoặc `redis://username:password@host:port`

## Bước 2: Cập nhật render.yaml

Mở file `render.yaml` và thay thế dòng:

```yaml
- key: REDIS_URL
  value: redis://default:YOUR_PASSWORD@YOUR_REDIS_HOST:YOUR_REDIS_PORT
```

Bằng Redis URL thực tế của bạn, ví dụ:

```yaml
- key: REDIS_URL
  value: redis://default:abc123xyz@redis-12345.c1.us-east-1-1.ec2.cloud.redislabs.com:12345
```

**Lưu ý quan trọng:**
- Nếu Redis URL có SSL/TLS, dùng `rediss://` thay vì `redis://`
- Đảm bảo password không có ký tự đặc biệt cần encode (như `@`, `:`, `/`)
- Nếu password có ký tự đặc biệt, URL encode nó (ví dụ: `@` → `%40`)

## Bước 3: Cách 2 - Thêm trực tiếp trên Render Dashboard (Khuyến nghị)

Nếu bạn không muốn commit Redis URL vào git (bảo mật hơn):

1. Vào [Render Dashboard](https://dashboard.render.com/)
2. Chọn service **ecommerce-dss-backend**
3. Vào tab **"Environment"**
4. Tìm hoặc thêm biến môi trường:
   - **Key:** `REDIS_URL`
   - **Value:** Redis URL của bạn (ví dụ: `redis://default:password@host:port`)
5. Click **"Save Changes"**
6. Service sẽ tự động redeploy

**Ưu điểm:** Không cần commit password vào git, bảo mật hơn.

## Bước 4: Kiểm tra kết nối

Sau khi deploy, kiểm tra logs:

```bash
# Trên Render Dashboard, vào tab "Logs"
# Tìm dòng:
✅ Redis cache enabled (url=redis://...)
```

Nếu thấy warning:
```
⚠️  Redis not available (...), using in-memory cache
```

Kiểm tra:
1. Redis URL đúng format chưa?
2. Password có đúng không?
3. Firewall/Security Group có cho phép kết nối từ Render IP không?
4. Redis có hỗ trợ public access không?

## Bước 5: Test Redis hoạt động

Sau khi deploy thành công, test API:

```bash
# Gọi API bất kỳ (sẽ cache kết quả)
curl https://your-backend.onrender.com/api/v1/dss/health

# Gọi lại lần 2 (sẽ lấy từ cache - nhanh hơn)
curl https://your-backend.onrender.com/api/v1/dss/health
```

## Troubleshooting

### Lỗi: "Connection refused"
- Kiểm tra Redis URL có đúng không
- Kiểm tra port có đúng không (thường là 6379 hoặc 12345)
- Kiểm tra firewall có block không

### Lỗi: "Authentication failed"
- Kiểm tra password có đúng không
- Kiểm tra username có đúng không (thường là `default`)

### Lỗi: "SSL/TLS required"
- Đổi `redis://` thành `rediss://` (thêm chữ 's')

### Redis vẫn dùng in-memory cache
- Kiểm tra biến môi trường `REDIS_URL` có được set đúng không
- Xem logs để biết lỗi cụ thể
- Thử test kết nối Redis bằng `redis-cli` hoặc tool online

## Ví dụ Redis URL theo từng dịch vụ

### Redis Cloud:
```
redis://default:MyPassword123@redis-12345.c1.us-east-1-1.ec2.cloud.redislabs.com:12345
```

### Upstash:
```
rediss://default:MyPassword123@usw1-xxx.upstash.io:6379
```

### Redis Labs:
```
redis://:MyPassword123@redis-12345.c1.us-east-1-1.ec2.cloud.redislabs.com:12345
```

### Custom Redis Server:
```
redis://username:password@your-redis-host.com:6379
```

---

**Sau khi hoàn thành:** Commit và push code, Render sẽ tự động deploy với Redis mới!

