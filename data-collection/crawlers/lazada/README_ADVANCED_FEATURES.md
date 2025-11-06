# Advanced Lazada Data Processing System

## 🎯 Tổng quan hệ thống

Hệ thống xử lý dữ liệu Lazada nâng cao với đầy đủ tính năng từ crawling đến analysis và enrichment:

```
🕷️ Enhanced Crawler → 📊 Advanced Processor → 🔧 Data Enrichment → 📈 Analytics
```

## 🚀 Tính năng chính

### 1. **Enhanced Crawler** (Đã sửa lỗi shop_name)
✅ Lấy shop_name chính xác từ product detail page
✅ Lấy reviews chi tiết với sentiment
✅ Image URLs (không base64)
✅ Tách riêng review_count và sold_count

### 2. **Advanced Data Processor**
✅ Làm sạch và chuẩn hóa dữ liệu
✅ Phân tích giá trị và xu hướng
✅ Tính toán chất lượng dữ liệu
✅ Insights và recommendations

### 3. **Data Enrichment Engine**
✅ Brand detection và categorization
✅ Specification extraction (RAM, storage, etc.)
✅ Sentiment analysis nâng cao
✅ Competitive analysis
✅ Market positioning

## 📁 Files trong hệ thống

### **Core Components:**
- `enhanced_lazada_crawler.py` - Crawler chính (fixed)
- `advanced_data_processor.py` - Xử lý và phân tích dữ liệu
- `data_enrichment_engine.py` - Làm giàu dữ liệu với AI/ML
- `run_complete_pipeline.py` - Chạy toàn bộ pipeline

### **Utilities:**
- `run_enhanced_crawler.py` - Chạy crawler với options
- `requirements_enhanced.txt` - Dependencies

## 🎯 Cách sử dụng

### **1. Setup Environment**
```bash
cd data-collection/crawlers/lazada/runners/
pip install -r requirements_enhanced.txt
```

### **2. Chạy Complete Pipeline**
```bash
# Pipeline đầy đủ: Crawl → Process → Enrich → Analyze
python run_complete_pipeline.py --category smartphones --pages 2 --products-per-page 10

# Pipeline nhanh (skip details)
python run_complete_pipeline.py --category smartphones --no-details --pages 3

# Chỉ process data có sẵn (skip crawling)
python run_complete_pipeline.py --skip-crawling
```

### **3. Chạy từng component riêng**

#### **A. Enhanced Crawler**
```bash
# Crawl với đầy đủ shop details và reviews
python enhanced_lazada_crawler.py

# Crawl with options
python run_enhanced_crawler.py --category laptops --pages 2 --products-per-page 15
```

#### **B. Advanced Data Processor**
```bash
# Process existing crawled data
python advanced_data_processor.py
```

#### **C. Data Enrichment Engine**
```bash
# Enrich data with AI/ML features
python data_enrichment_engine.py
```

## 📊 Output Structure

### **1. Crawler Output**
```
outputs/
├── enhanced_lazada_complete_TIMESTAMP.json    # Dữ liệu đầy đủ
├── enhanced_lazada_main_TIMESTAMP.csv         # CSV chính
└── enhanced_lazada_reviews_TIMESTAMP.json     # Reviews riêng
```

### **2. Analysis Output**
```
analysis_output_CATEGORY/
├── analytics_report_TIMESTAMP.json            # Phân tích chi tiết
├── insights_report_TIMESTAMP.json             # Insights & recommendations
├── cleaned_data_TIMESTAMP.json                # Dữ liệu đã làm sạch
└── summary_report_TIMESTAMP.csv               # Tóm tắt CSV
```

### **3. Enrichment Output**
```
enriched_output_CATEGORY/
├── enriched_CATEGORY_enriched_TIMESTAMP.json  # Dữ liệu enriched
├── enriched_CATEGORY_enrichment_summary_TIMESTAMP.json  # Summary
└── enriched_CATEGORY_analysis_TIMESTAMP.csv   # Analysis CSV
```

## 🔧 Advanced Features Detail

### **1. Data Cleaning & Standardization**
- **Price normalization**: Tất cả về VND integer
- **Location standardization**: "hà nội" → "Hà Nội"
- **Text cleaning**: Loại bỏ ký tự đặc biệt, normalize encoding
- **Quality scoring**: 0-100 điểm cho mỗi sản phẩm

### **2. Enhanced Categorization**
```python
{
  "detailed_category": "smartphones",
  "subcategory": "flagship",           # flagship/mid_range/budget
  "product_type": "android_phone",
  "category_confidence": 0.95
}
```

### **3. Brand Detection**
```python
{
  "brand": "xiaomi",
  "brand_confidence": 0.89,
  "brand_tier": "mainstream"           # premium/mainstream/budget/luxury
}
```

### **4. Specification Extraction**
```python
{
  "specifications": {
    "memory": {
      "ram": "8",                      # GB
      "storage": "256"                 # GB
    },
    "display": {
      "screen_size": "6.7",            # inch
      "resolution": "1080x2400"
    },
    "camera": {
      "main_camera": "108",            # MP
      "camera_count": "3"
    },
    "battery": {
      "capacity": "5000",              # mAh
      "charging": "67W"
    }
  },
  "spec_completeness": 0.8             # 0-1
}
```

### **5. Sentiment Analysis**
```python
{
  "sentiment_analysis": {
    "overall_sentiment": "positive",   # positive/negative/neutral
    "sentiment_score": 0.7,           # -1 to 1
    "positive_aspects": ["quality", "performance"],
    "negative_aspects": ["shipping"],
    "keyword_sentiment": {
      "quality": 0.8,
      "performance": 0.6,
      "design": 0.4
    }
  }
}
```

### **6. Price Analysis**
```python
{
  "price_analysis": {
    "price_tier": "mid_range",         # budget/mid_range/premium/flagship
    "value_score": 0.75,              # 0-1
    "price_per_spec": {
      "price_per_gb_ram": 875000,     # VND per GB
      "price_per_mp": 156250           # VND per MP
    }
  }
}
```

### **7. Competitive Analysis**
```python
{
  "competitive_analysis": {
    "market_position": "strong",
    "competitive_advantages": ["rich_specifications", "high_rating"],
    "competitive_disadvantages": [],
    "uniqueness_score": 0.85
  }
}
```

### **8. Quality Indicators**
```python
{
  "quality_indicators": {
    "data_completeness": 0.9,          # 0-1
    "information_richness": 0.8,       # 0-1
    "credibility_score": 0.85,         # 0-1
    "engagement_score": 0.7            # 0-1
  }
}
```

## 📈 Analytics & Insights

### **Automated Insights Generated:**

#### **Market Insights**
- Market size và distribution
- Top categories và brands
- Coverage analysis

#### **Price Insights**
- Price distributions và ranges
- Value propositions
- Price tier analysis

#### **Quality Insights**
- Data quality metrics
- Missing data analysis
- Recommendations for improvement

#### **Shop Insights**
- Top performers
- Shop ratings distribution
- Market concentration

## ⚡ Performance & Optimization

### **Speed Comparison:**
| Mode | Speed | Data Quality | Use Case |
|------|--------|--------------|----------|
| Basic Crawler | 🚀 Fast | ⚠️ Medium | Quick data collection |
| Enhanced + Details | 🐌 Slow | ✅ High | Complete analysis |
| Enhanced No Details | 🏃 Medium | ✅ Good | Balance quality/speed |

### **Recommended Settings:**

#### **For Testing (Fast)**
```bash
--pages 1 --products-per-page 5 --no-details
```

#### **For Production (Quality)**
```bash
--pages 10 --products-per-page 20
```

#### **For Complete Analysis**
```bash
--pages 5 --products-per-page 15
# Includes: crawling + processing + enrichment + analysis
```

## 🔗 Integration với Spark Pipeline

Data output từ system này có thể integrate trực tiếp với Spark standardization pipeline:

```bash
# 1. Chạy complete pipeline
python run_complete_pipeline.py --category smartphones

# 2. Copy output đến Spark input
cp enriched_output_smartphones/enriched_smartphones_enriched_*.json /spark/data/raw/

# 3. Chạy Spark standardization
docker exec spark-master python /opt/spark/apps/standardization_pipeline.py
```

## 🎯 Use Cases

### **1. Market Research**
- Analyze competitor products
- Price benchmarking
- Feature comparison

### **2. Product Management**
- Identify market gaps
- Feature prioritization
- Competitive positioning

### **3. Business Intelligence**
- Market trends analysis
- Customer sentiment tracking
- Shop performance monitoring

### **4. Data Science**
- ML model training data
- Recommendation systems
- Price prediction models

## 🔧 Customization

### **Add New Categories**
Edit `enhanced_lazada_crawler.py`:
```python
self.categories = {
    "new_category": "https://www.lazada.vn/tag/new/?q=new",
    # ... existing categories
}
```

### **Custom Brand Detection**
Edit `data_enrichment_engine.py`:
```python
self.brand_patterns = {
    'new_brand': ['new_brand', 'pattern1', 'pattern2'],
    # ... existing brands
}
```

### **Custom Sentiment Keywords**
```python
positive_keywords = {
    'quality': ['tốt', 'good', 'excellent', 'your_keywords'],
    # ... existing keywords
}
```

## 🐛 Troubleshooting

### **Common Issues:**

1. **ChromeDriver issues**
   ```bash
   pip install webdriver-manager
   ```

2. **Memory issues with large datasets**
   - Reduce `products_per_page`
   - Process in smaller batches
   - Use `--no-details` for faster processing

3. **Timeout errors**
   - Increase delays in crawler
   - Check internet connection
   - Reduce concurrent requests

4. **Missing dependencies**
   ```bash
   pip install -r requirements_enhanced.txt
   ```

## 🚀 Next Steps

1. **Real-time Processing**: Stream data processing với Kafka
2. **Advanced ML**: Deep learning cho NLP và computer vision
3. **API Integration**: RESTful API cho real-time queries
4. **Dashboard**: Interactive analytics dashboard
5. **Automation**: Scheduled crawling và processing

Hệ thống này cung cấp foundation mạnh mẽ cho E-commerce data analysis và có thể scale để handle production workloads! 🎉