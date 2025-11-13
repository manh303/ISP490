# Big Data Scaling Plan

## Current Architecture (Small Scale - 100K-1M records)
```
Crawlers → PostgreSQL → Spark (local) → PostgreSQL
```
**Bottlenecks:**
- PostgreSQL không scale cho billions records
- Spark local mode chậm
- Không có distributed storage

---

## Big Data Architecture (10M-1B+ records)

### Phase 1: Data Lake (Immediate - 1 week)
```
Crawlers → S3/MinIO (Parquet) → Spark → S3 → PostgreSQL (aggregated only)
```

**Changes:**
1. **Raw data → S3/MinIO** (không lưu PostgreSQL)
2. **Format: Parquet** (nén 10x, query nhanh 100x)
3. **Partition: date/platform/category**
4. **PostgreSQL: Chỉ aggregated data** (summary tables)

**Benefits:**
- Storage: Unlimited (S3)
- Cost: $0.023/GB/month
- Query: 100x faster với Parquet
- Scale: Billions records

---

### Phase 2: Distributed Processing (2 weeks)
```
S3 → Spark Cluster (10+ workers) → Delta Lake → Presto/Trino
```

**Changes:**
1. **Spark Cluster**: 10-50 workers thay vì 2
2. **Delta Lake**: ACID transactions trên S3
3. **Presto/Trino**: Query engine cho data lake
4. **Iceberg**: Table format cho big data

**Benefits:**
- Process: 10TB+ data
- Speed: 10-100x faster
- Cost: Pay per use

---

### Phase 3: Real-time Streaming (3 weeks)
```
Kafka → Flink/Spark Streaming → Delta Lake → Real-time Dashboard
```

**Changes:**
1. **Kafka**: 1000+ events/sec
2. **Flink**: Real-time processing
3. **Materialized Views**: Pre-aggregated
4. **Redis**: Hot data cache

**Benefits:**
- Latency: <1 second
- Throughput: 100K events/sec
- Real-time analytics

---

## Implementation Priority

### 🔥 Quick Wins (This Week)
1. **Switch to Parquet format**
2. **Use S3/MinIO for raw data**
3. **Partition by date**
4. **Aggregate to PostgreSQL**

### 📊 Medium Term (1 Month)
1. **Add more Spark workers**
2. **Implement Delta Lake**
3. **Add Presto for queries**
4. **Columnar storage**

### 🚀 Long Term (3 Months)
1. **Kafka streaming**
2. **Flink processing**
3. **Data mesh architecture**
4. **Multi-region replication**

---

## Cost Comparison

### Current (PostgreSQL only)
- 100M records: $200-500/month
- 1B records: $2000-5000/month ❌ Too expensive

### Big Data (S3 + Spark)
- 100M records: $20-50/month
- 1B records: $200-500/month ✅ 10x cheaper
- 10B records: $2000-5000/month ✅ Scalable

---

## Technology Stack

### Storage Layer
- **S3/MinIO**: Object storage (unlimited)
- **Delta Lake**: ACID on S3
- **Iceberg**: Table format
- **Parquet**: Columnar format

### Processing Layer
- **Spark**: Batch processing
- **Flink**: Stream processing
- **Presto/Trino**: Query engine
- **dbt**: Data transformation

### Serving Layer
- **PostgreSQL**: Aggregated data only
- **Redis**: Hot cache
- **Elasticsearch**: Full-text search
- **ClickHouse**: OLAP queries

### Orchestration
- **Airflow**: Workflow
- **Kubernetes**: Container orchestration
- **ArgoCD**: GitOps deployment

---

## Migration Steps

### Step 1: Add MinIO (S3-compatible)
```yaml
# docker-compose.yml
minio:
  image: minio/minio
  ports:
    - "9000:9000"
  volumes:
    - minio_data:/data
  command: server /data
```

### Step 2: Write to Parquet
```python
# Instead of PostgreSQL
df.write.partitionBy("date", "platform") \
  .mode("append") \
  .parquet("s3://bucket/products/")
```

### Step 3: Query from S3
```python
# Read from S3, aggregate, write to PostgreSQL
df = spark.read.parquet("s3://bucket/products/date=2025-11-13/")
summary = df.groupBy("category").agg(...)
summary.write.jdbc(postgres_url, "product_summary")
```

### Step 4: Scale Spark
```yaml
# Add more workers
spark-worker-3:
  ...
spark-worker-10:
  ...
```

---

## Performance Targets

### Current
- Crawl: 10K products/hour
- Process: 100K records/hour
- Query: 5-10 seconds
- Storage: 100GB

### After Phase 1 (Data Lake)
- Crawl: 50K products/hour
- Process: 1M records/hour
- Query: 1-2 seconds
- Storage: 10TB

### After Phase 2 (Distributed)
- Crawl: 200K products/hour
- Process: 10M records/hour
- Query: <1 second
- Storage: 100TB

### After Phase 3 (Streaming)
- Crawl: 1M products/hour
- Process: 100M records/hour
- Query: <100ms
- Storage: Unlimited

---

## Next Steps

1. **This week**: Implement MinIO + Parquet
2. **Next week**: Add 5 more Spark workers
3. **Month 1**: Delta Lake + Presto
4. **Month 2**: Kafka + Flink
5. **Month 3**: Production deployment

---

## References
- [Delta Lake](https://delta.io/)
- [Apache Iceberg](https://iceberg.apache.org/)
- [Presto](https://prestodb.io/)
- [Data Lake Best Practices](https://aws.amazon.com/big-data/datalakes-and-analytics/)
