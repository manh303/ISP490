# Vietnamese E-commerce Crawling Specifications

## 📋 Tổng Quan
Tài liệu chuyên biệt cho crawl dữ liệu e-commerce từ các sàn thương mại điện tử phổ biến tại Việt Nam.

## 🇻🇳 Các Sàn E-commerce Chính Tại Việt Nam

### 1. **Shopee Vietnam** 🛒
- **URL**: https://shopee.vn
- **Market Share**: ~45% thị trường Việt Nam
- **Khả năng crawl**: ⚠️ Hạn chế cao
- **Rate Limit**: 10 requests/phút (estimate)
- **Data/ngày**: ~500-1,000 sản phẩm
- **Hạn chế**:
  - SPA architecture, cần JavaScript
  - Anti-bot SDK mạnh
  - Dynamic loading content
  - CAPTCHA protection

### 2. **Tiki.vn** 📚
- **URL**: https://tiki.vn
- **Market Share**: ~20% thị trường Việt Nam
- **Khả năng crawl**: ✅ Khả thi
- **Rate Limit**: 30 requests/phút
- **Data/ngày**: ~2,000-5,000 sản phẩm
- **Ưu điểm**:
  - API endpoints công khai
  - Less aggressive anti-bot
  - Better structured data

### 3. **Lazada Vietnam** 🏪
- **URL**: https://lazada.vn
- **Market Share**: ~15% thị trường Việt Nam
- **Khả năng crawl**: ⚠️ Trung bình
- **Rate Limit**: 20 requests/phút
- **Data/ngày**: ~1,500-3,000 sản phẩm
- **Hạn chế**:
  - Alibaba anti-crawler system
  - Region blocking
  - Rate limiting

### 4. **FPTShop.com.vn** 💻
- **URL**: https://fptshop.com.vn
- **Market Share**: ~8% (electronics focus)
- **Khả năng crawl**: ✅ Tốt
- **Rate Limit**: 50 requests/phút
- **Data/ngày**: ~3,000-8,000 sản phẩm
- **Ưu điểm**:
  - Chuyên điện tử
  - Ít protection
  - Clear structure

### 5. **Sendo.vn** 🌟
- **URL**: https://sendo.vn
- **Market Share**: ~5% thị trường Việt Nam
- **Khả năng crawl**: ✅ Khả thi
- **Rate Limit**: 40 requests/phút
- **Data/ngày**: ~2,500-5,000 sản phẩm

### 6. **CellphoneS.com.vn** 📱
- **URL**: https://cellphones.com.vn
- **Market Share**: ~3% (mobile focus)
- **Khả năng crawl**: ✅ Tốt
- **Rate Limit**: 60 requests/phút
- **Data/ngày**: ~4,000-10,000 sản phẩm

## 📊 Khả Năng Crawl Tổng Hợp Hàng Ngày

| **Platform** | **Products/Day** | **Categories** | **Difficulty** | **Success Rate** |
|--------------|------------------|----------------|----------------|------------------|
| **FPTShop** | 8,000 | Electronics | Easy | 90% |
| **CellphoneS** | 10,000 | Mobile/Tech | Easy | 95% |
| **Sendo** | 5,000 | General | Medium | 80% |
| **Tiki** | 5,000 | Books/General | Medium | 75% |
| **Lazada** | 3,000 | General | Hard | 60% |
| **Shopee** | 1,000 | General | Very Hard | 30% |

**🎯 Tổng Cộng**: ~32,000 sản phẩm/ngày từ crawling thực tế

## 🛠️ Cấu Hình Kỹ Thuật

### Rate Limiting Chuẩn
```python
RATE_LIMITS = {
    'fptshop': {'requests_per_minute': 50, 'concurrent': 5},
    'cellphones': {'requests_per_minute': 60, 'concurrent': 6},
    'sendo': {'requests_per_minute': 40, 'concurrent': 4},
    'tiki': {'requests_per_minute': 30, 'concurrent': 3},
    'lazada': {'requests_per_minute': 20, 'concurrent': 2},
    'shopee': {'requests_per_minute': 10, 'concurrent': 1}
}
```

### Headers Chuẩn
```python
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1'
}
```

## 📋 Data Schema Chuẩn

### Product Schema
```json
{
    "product_id": "string",
    "name": "string",
    "price": "number",
    "original_price": "number",
    "discount_percent": "number",
    "category": "string",
    "brand": "string",
    "rating": "number",
    "review_count": "number",
    "seller": "string",
    "description": "string",
    "images": ["array"],
    "specifications": {"object"},
    "availability": "boolean",
    "shipping_info": {"object"},
    "platform": "string",
    "crawl_timestamp": "datetime",
    "url": "string"
}
```

### Seller Schema
```json
{
    "seller_id": "string",
    "name": "string",
    "rating": "number",
    "follower_count": "number",
    "product_count": "number",
    "response_rate": "number",
    "location": "string",
    "platform": "string"
}
```

## 🎯 Chiến Lược Crawling

### 1. **Ưu Tiên Cao** (60% effort)
- **FPTShop**: Electronics focus, ít protection
- **CellphoneS**: Mobile/tech, high success rate
- **Sendo**: General marketplace, reasonable limits

### 2. **Ưu Tiên Trung Bình** (30% effort)
- **Tiki**: Books + general, có API
- **Lazada**: Large data but protected

### 3. **Ưu Tiên Thấp** (10% effort)
- **Shopee**: Khó crawl, dùng synthetic data

## ⏰ Schedule Crawling

### Optimal Times (GMT+7)
- **Peak**: 09:00-11:00, 14:00-16:00, 20:00-22:00
- **Off-peak**: 02:00-06:00 (lower protection)

### Frequency
- **High-value sites**: Every 6 hours
- **Medium sites**: Every 12 hours
- **Protected sites**: Every 24 hours

## 🔧 Technical Implementation

### Concurrent Processing
```python
MAX_CONCURRENT = {
    'fptshop': 5,
    'cellphones': 6,
    'sendo': 4,
    'tiki': 3,
    'lazada': 2,
    'shopee': 1
}
```

### Error Handling
- **Timeout**: 30 seconds per request
- **Retries**: 3 attempts with exponential backoff
- **Circuit breaker**: Stop after 10 consecutive failures

## 📈 Expected Daily Output

### Realistic Estimates (Conservative)
- **Products**: 25,000-30,000/day
- **Categories**: 50-100 categories
- **Brands**: 500-1,000 brands
- **Sellers**: 1,000-2,000 sellers

### Peak Performance (Optimized)
- **Products**: 35,000-45,000/day
- **Real-time updates**: Every 4-6 hours
- **Data freshness**: <8 hours average

## 🚫 Ethical Considerations

### Compliance
- ✅ Respect robots.txt
- ✅ Reasonable rate limiting
- ✅ No personal data collection
- ✅ Public data only
- ❌ No bulk downloading
- ❌ No reselling of data

### Best Practices
- Use official APIs when available
- Cache data to reduce requests
- Monitor server load
- Implement graceful degradation

## 🔮 Future Enhancements

1. **API Integration**: Direct partnerships với platforms
2. **ML-based detection**: Anti-bot evasion
3. **Proxy rotation**: Geographic distribution
4. **Real-time streaming**: Webhook integrations
5. **Data validation**: Quality assurance pipelines

---

**🎯 Mục Tiêu**: Thu thập 30,000+ sản phẩm e-commerce Việt Nam/ngày từ 6 platforms chính với tỷ lệ thành công >80%