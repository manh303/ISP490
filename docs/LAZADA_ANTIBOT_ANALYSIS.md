# Lazada Anti-Bot Analysis

## Vấn đề
Lazada.vn có hệ thống anti-bot cực kỳ mạnh, chặn mọi phương pháp crawling phổ biến.

## Các phương pháp đã thử

### 1. Selenium với Chrome headless
- **Kết quả**: Bị chặn, trả về 0 products
- **Lý do**: Lazada detect automation flags

### 2. Undetected ChromeDriver
- **Kết quả**: Vẫn bị chặn
- **Lý do**: Anti-bot vẫn detect được

### 3. Lazada Search API (`/catalog/`)
- **Kết quả**: Trả về HTML thay vì JSON
- **Lý do**: API endpoint yêu cầu authentication/token

### 4. Lazada Mobile API (`/api/search`)
- **Kết quả**: HTTP 403/404
- **Lý do**: Endpoint không public hoặc cần token

### 5. HTML Parsing với window.pageData
- **Kết quả**: Encoding error với tiếng Việt
- **Lý do**: Requests library encoding issue với Vietnamese characters

## Kết luận

Lazada có các lớp bảo vệ:
1. **CloudFlare/WAF** - Chặn requests không có proper headers
2. **Bot Detection** - Detect Selenium/automation tools
3. **API Authentication** - API endpoints cần token/signature
4. **Rate Limiting** - Chặn requests quá nhanh
5. **Encoding Protection** - Trả về response với encoding đặc biệt

## Giải pháp

**Disable Lazada crawler, chỉ sử dụng Tiki**

### Lý do:
- Tiki API hoạt động tốt (240 products/run)
- Tiki có public API endpoint ổn định
- Không cần phức tạp hóa hệ thống
- Focus vào data quality thay vì quantity

### Thay thế:
Nếu cần thêm data sources:
- **Shopee**: Có API dễ crawl hơn
- **Sendo**: Ít anti-bot hơn
- **FPT Shop**: Website đơn giản
- **Thế Giới Di Động**: Có sitemap XML

## Pipeline hiện tại

```
Tiki API → JSONL → Spark ETL → PostgreSQL → Analytics
```

**Status**: ✅ Hoạt động tốt với 240 products mỗi lần chạy
