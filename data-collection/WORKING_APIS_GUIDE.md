# 🎯 APIs Thực Tế Hoạt Động - Hướng Dẫn Sử Dụng

## 📊 APIs Miễn Phí 100% (Không Cần Đăng Ký)

### **1. 📦 FakeStore API - E-commerce Product Data**
```bash
Base URL: https://fakestoreapi.com
Endpoints:
  • /products - Tất cả sản phẩm
  • /products/categories - Danh mục
  • /products/category/{category} - Sản phẩm theo danh mục
  • /users - Thông tin người dùng

Rate Limit: Không giới hạn
Auth: Không cần
```

**Sample Request:**
```bash
curl "https://fakestoreapi.com/products?limit=5"
```

**Sample Response:**
```json
[
  {
    "id": 1,
    "title": "Fjallraven - Foldsack No. 1 Backpack, Fits 15 Laptops",
    "price": 109.95,
    "description": "Your perfect pack for everyday use...",
    "category": "men's clothing",
    "image": "https://fakestoreapi.com/img/81fPKd-2AYL._AC_SL1500_.jpg",
    "rating": {"rate": 3.9, "count": 120}
  }
]
```

### **2. 👥 JSONPlaceholder - Mock Social/User Data**
```bash
Base URL: https://jsonplaceholder.typicode.com
Endpoints:
  • /users - User profiles
  • /posts - Social media posts
  • /comments - Comments on posts
  • /albums - Photo albums

Rate Limit: Không giới hạn
Auth: Không cần
```

**Sample Request:**
```bash
curl "https://jsonplaceholder.typicode.com/users"
```

### **3. ₿ CoinGecko API - Cryptocurrency Data**
```bash
Base URL: https://api.coingecko.com/api/v3
Endpoints:
  • /simple/price - Current prices
  • /coins/markets - Market data
  • /global - Global market stats

Rate Limit: 10-30 calls/minute (free tier)
Auth: Không cần cho basic endpoints
```

**Sample Request:**
```bash
curl "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd"
```

### **4. 📰 Hacker News API - Tech News/Discussions**
```bash
Base URL: https://hacker-news.firebaseio.com/v0
Endpoints:
  • /topstories.json - Top story IDs
  • /item/{id}.json - Story details
  • /user/{username}.json - User info

Rate Limit: Không giới hạn
Auth: Không cần
```

**Sample Request:**
```bash
curl "https://hacker-news.firebaseio.com/v0/topstories.json"
```

### **5. 💱 Exchange Rate API - Currency Data**
```bash
Base URL: https://api.exchangerate-api.com/v4/latest
Endpoints:
  • /{base_currency} - Exchange rates

Rate Limit: 1500 requests/month (free)
Auth: Không cần
```

**Sample Request:**
```bash
curl "https://api.exchangerate-api.com/v4/latest/USD"
```

### **6. 🧪 ReqRes API - Sample API Data**
```bash
Base URL: https://reqres.in/api
Endpoints:
  • /users - User data
  • /unknown - Product-like data

Rate Limit: Không giới hạn
Auth: Không cần
```

## 🚀 Quick Test - Kiểm Tra APIs

```bash
# Test tất cả APIs
python test_free_apis.py

# Kết quả mong đợi:
# ✅ FakeStore API - Products: SUCCESS (200ms)
# ✅ JSONPlaceholder - Users: SUCCESS (150ms)
# ✅ CoinGecko - Simple Price: SUCCESS (300ms)
# ✅ Hacker News - Top Stories: SUCCESS (100ms)
# ✅ Exchange Rate API: SUCCESS (250ms)
```

## 📊 Sử Dụng Trong Data Collection

### **1. Chạy Free API Collector**
```bash
# Collect data từ tất cả free APIs
python apis/free_api_collector.py

# Kết quả:
# 📦 Collected 20 products from FakeStore API
# 👥 Collected 10 users from JSONPlaceholder API
# ₿ Collected 20 crypto prices from CoinGecko API
# 📰 Collected 20 stories from Hacker News API
# 💱 Collected exchange rates for 168 currencies
```

### **2. Data Flow vào System**
```
Free APIs → Kafka Topics → MongoDB Raw Data → Spark Processing → PostgreSQL Analytics
```

### **3. Kafka Topics được tạo:**
- `products_raw` - FakeStore products
- `customers_raw` - JSONPlaceholder users
- `market_data` - CoinGecko crypto + Exchange rates
- `news_trends` - Hacker News stories

## 🎯 APIs Cần Đăng Ký Miễn Phí (Optional)

### **1. 🌤️ OpenWeatherMap API**
```bash
Signup: https://openweathermap.org/api
Free Tier: 60 calls/minute, 1M calls/month
Use Case: Weather data affecting shopping patterns
```

### **2. 📰 NewsAPI**
```bash
Signup: https://newsapi.org/register
Free Tier: 100 requests/day
Use Case: E-commerce news and trends
```

### **3. 🐦 Twitter API v2**
```bash
Signup: https://developer.twitter.com
Free Tier: 500K tweets/month
Use Case: Brand mentions và sentiment analysis
```

## ⚙️ Configuration Update

Cập nhật `config/data_sources.json` với APIs thực tế:

```json
{
  "apis": {
    "free_public_apis": {
      "fakestore": {
        "base_url": "https://fakestoreapi.com",
        "auth_required": false,
        "rate_limit": "unlimited",
        "status": "✅ WORKING"
      },
      "jsonplaceholder": {
        "base_url": "https://jsonplaceholder.typicode.com",
        "auth_required": false,
        "rate_limit": "unlimited",
        "status": "✅ WORKING"
      },
      "coingecko": {
        "base_url": "https://api.coingecko.com/api/v3",
        "auth_required": false,
        "rate_limit": "10-30/minute",
        "status": "✅ WORKING"
      }
    }
  }
}
```

## 📈 Expected Data Volume từ Free APIs

| API Source | Records/Hour | Data Type | Size/Record |
|------------|--------------|-----------|-------------|
| FakeStore | 20 products | E-commerce | ~500 bytes |
| JSONPlaceholder | 10 users | Customer | ~300 bytes |
| CoinGecko | 20 crypto prices | Financial | ~200 bytes |
| Hacker News | 20 stories | News/Social | ~400 bytes |
| Exchange Rates | 168 rates | Financial | ~50 bytes |

**Total: ~270 records/hour = 2.3M records/year** từ free APIs alone!

## 🔧 Troubleshooting

### **Common Issues:**

1. **CoinGecko Rate Limit**
```bash
# Error: 429 Too Many Requests
# Solution: Add delays between requests
await asyncio.sleep(2)  # 2 second delay
```

2. **Network Timeout**
```bash
# Error: TimeoutError
# Solution: Increase timeout
async with session.get(url, timeout=30) as response:
```

3. **JSON Decode Error**
```bash
# Error: json.JSONDecodeError
# Solution: Check response status first
if response.status == 200:
    data = await response.json()
```

## 🎉 Benefits của Free APIs

✅ **Immediate Access** - Không cần approval process
✅ **No Cost** - Hoàn toàn miễn phí
✅ **Reliable Data** - Stable, well-maintained APIs
✅ **Good Volume** - Đủ data cho testing và development
✅ **Diverse Data Types** - Products, users, financial, news
✅ **Real-time Updates** - Crypto prices cập nhật real-time

## 🚀 Production Scaling

Khi cần scale lên production:

1. **Upgrade to Paid Tiers** của working APIs
2. **Add Authentication** cho premium endpoints
3. **Implement Caching** để optimize rate limits
4. **Use Multiple API Keys** để increase limits
5. **Add Real E-commerce APIs** khi có budget

**Current Free Setup** cung cấp solid foundation cho big data analytics development!