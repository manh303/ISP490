# Big Data Architecture - Quick Start

## 🚀 Start Big Data Stack

```bash
# Start all services
docker-compose -f docker-compose.bigdata.yml up -d

# Check status
docker-compose -f docker-compose.bigdata.yml ps
```

## 📊 Access Points

| Service | URL | Credentials |
|---------|-----|-------------|
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin123 |
| Spark Master | http://localhost:8081 | - |
| Trino | http://localhost:8085 | - |
| Airflow | http://localhost:8080 | admin / admin123 |
| Grafana | http://localhost:3001 | admin / admin123 |
| Prometheus | http://localhost:9090 | - |

## 💾 Data Flow

```
Crawlers → MinIO (Parquet) → Spark → MinIO (Delta Lake) → Trino/PostgreSQL
```

## 📝 Usage Examples

### 1. Write to Data Lake (Parquet)
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Write partitioned Parquet
df.write \
    .partitionBy("date", "platform") \
    .mode("append") \
    .parquet("s3a://datalake/products/")
```

### 2. Read from Data Lake
```python
# Read specific partition
df = spark.read.parquet("s3a://datalake/products/date=2025-11-13/platform=lazada/")

# Read all data
df = spark.read.parquet("s3a://datalake/products/")
```

### 3. Query with Trino
```sql
-- Connect to Trino at localhost:8085
SELECT 
    platform,
    category,
    COUNT(*) as product_count,
    AVG(price) as avg_price
FROM minio.default.products
WHERE date = '2025-11-13'
GROUP BY platform, category;
```

### 4. Aggregate to PostgreSQL
```python
# Aggregate and save summary
summary = df.groupBy("category", "platform") \
    .agg(
        count("*").alias("count"),
        avg("price").alias("avg_price"),
        max("rating").alias("max_rating")
    )

summary.write \
    .jdbc(
        url="jdbc:postgresql://postgres:5432/ecommerce_dss_1",
        table="product_summary",
        mode="overwrite"
    )
```

## 🎯 Performance Comparison

### Small Data (< 1M records)
- **Current**: PostgreSQL only
- **Time**: 5-8 hours
- **Cost**: $50/month

### Big Data (10M+ records)
- **New**: MinIO + Spark + Parquet
- **Time**: 30-60 minutes
- **Cost**: $10/month

## 📦 Architecture Components

### Storage Layer
- **MinIO**: S3-compatible object storage (unlimited)
- **Format**: Parquet (10x compression, 100x faster)
- **Partition**: By date/platform/category

### Processing Layer
- **Spark Cluster**: 4 workers (16 cores, 16GB RAM)
- **Parallel**: Process multiple partitions simultaneously
- **Scalable**: Add more workers as needed

### Query Layer
- **Trino**: SQL query engine for data lake
- **ClickHouse**: OLAP for analytics
- **PostgreSQL**: Aggregated data only

### Serving Layer
- **Redis**: Hot cache (< 1ms)
- **PostgreSQL**: Summary tables (< 100ms)
- **Trino**: Ad-hoc queries (< 1s)

## 🔧 Configuration

### Increase Spark Workers
```yaml
# Add to docker-compose.bigdata.yml
spark-worker-4:
  <<: *spark-common
  container_name: spark-worker-4
  environment:
    - SPARK_WORKER_CORES=4
    - SPARK_WORKER_MEMORY=4g
```

### Tune Spark Performance
```python
spark = SparkSession.builder \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.default.parallelism", "100") \
    .getOrCreate()
```

## 📈 Scaling Strategy

### Phase 1: Current (Week 1)
- Start with 4 Spark workers
- Store raw data in MinIO
- Aggregate to PostgreSQL

### Phase 2: Scale (Month 1)
- Add 6 more workers (10 total)
- Implement Delta Lake
- Add Trino for queries

### Phase 3: Production (Month 2)
- 20+ workers
- Multi-region MinIO
- Real-time streaming with Kafka

## 🐛 Troubleshooting

### MinIO not accessible
```bash
docker logs minio
# Check http://localhost:9001
```

### Spark workers not connecting
```bash
docker logs spark-worker-1
# Check SPARK_MASTER_URL
```

### Out of memory
```yaml
# Increase worker memory
SPARK_WORKER_MEMORY=8g
```

## 📚 Next Steps

1. Migrate existing data to MinIO
2. Update ETL to write Parquet
3. Test query performance
4. Scale workers as needed
5. Implement Delta Lake for ACID
