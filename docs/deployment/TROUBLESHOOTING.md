# 🔧 TROUBLESHOOTING GUIDE

## 🚨 Common Issues & Solutions

### **Quick Diagnostics**
```bash
# Check all services status
docker-compose ps

# Check system resources
docker stats

# View logs for specific service
docker-compose logs [service_name]

# Test network connectivity
docker-compose exec kafka nc -zv kafka 29092
```

---

## 🗄️ **DATABASE ISSUES**

### **PostgreSQL Connection Problems**

**❌ Error:** `FATAL: password authentication failed`
```bash
# Solution 1: Reset PostgreSQL container
docker-compose stop postgres
docker-compose rm -f postgres
docker volume rm ecommerce-dss_postgres_data
docker-compose up -d postgres

# Solution 2: Check environment variables
grep -E "DB_USER|DB_PASSWORD" .env
```

**❌ Error:** `PostgreSQL is starting up`
```bash
# Wait for PostgreSQL to initialize (up to 2 minutes)
docker-compose exec postgres pg_isready -U dss_user

# Check initialization logs
docker-compose logs postgres | grep "database system is ready"
```

**❌ Error:** `Connection refused`
```bash
# Check if PostgreSQL container is running
docker-compose ps postgres

# Verify port mapping
docker-compose port postgres 5432

# Test connection from host
psql -h localhost -p 5432 -U dss_user -d ecommerce_dss
```

### **MongoDB Connection Problems**

**❌ Error:** `MongoNetworkError: connect ECONNREFUSED`
```bash
# Check MongoDB status
docker-compose exec mongodb mongosh --eval "db.adminCommand('ping')"

# Restart MongoDB
docker-compose restart mongodb

# Check MongoDB logs
docker-compose logs mongodb | tail -50
```

**❌ Error:** `Authentication failed`
```bash
# Connect without authentication for debugging
docker-compose exec mongodb mongosh ecommerce_dss

# Reset MongoDB with clean data
docker-compose stop mongodb
docker volume rm ecommerce-dss_mongo_data
docker-compose up -d mongodb
```

---

## 📡 **KAFKA ISSUES**

### **Kafka Connectivity Problems**

**❌ Error:** `Broker may not be available`
```bash
# Check if Kafka is ready
docker-compose exec kafka kafka-broker-api-versions --bootstrap-server kafka:29092

# Verify Zookeeper connection
docker-compose exec zookeeper zkCli.sh ls /

# Check Kafka logs for errors
docker-compose logs kafka | grep -i error
```

**❌ Error:** `Topic not found`
```bash
# List existing topics
docker-compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list

# Create missing topic manually
docker-compose exec kafka kafka-topics --bootstrap-server kafka:29092 \
  --create --topic products_stream --partitions 3 --replication-factor 1

# Enable auto-topic creation
echo "KAFKA_AUTO_CREATE_TOPICS_ENABLE=true" >> .env
docker-compose restart kafka
```

**❌ Error:** `Consumer lag too high`
```bash
# Check consumer group status
docker-compose exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 \
  --describe --group dss_consumers

# Reset consumer group offset
docker-compose exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 \
  --group dss_consumers --reset-offsets --to-latest --all-topics --execute

# Scale up consumers
docker-compose up -d --scale stream-consumer=3
```

### **Kafka Performance Issues**

**❌ Issue:** Slow message processing
```bash
# Increase partition count
docker-compose exec kafka kafka-topics --bootstrap-server kafka:29092 \
  --alter --topic products_stream --partitions 6

# Tune producer settings
export KAFKA_PRODUCER_BATCH_SIZE=32768
export KAFKA_PRODUCER_LINGER_MS=5
docker-compose restart stream-producer

# Monitor Kafka metrics
curl http://localhost:8090/api/clusters
```

---

## ⚡ **SPARK ISSUES**

### **Spark Job Failures**

**❌ Error:** `Application failed with exit code 1`
```bash
# Check Spark master logs
docker-compose logs spark-master

# Check worker logs
docker-compose logs spark-worker-1 spark-worker-2

# Access Spark UI for detailed error
open http://localhost:8081

# Check for memory issues
docker stats spark-worker-1 spark-worker-2
```

**❌ Error:** `OutOfMemoryError`
```bash
# Increase worker memory
docker-compose down
# Edit docker-compose.yml:
#   SPARK_WORKER_MEMORY=4g
#   SPARK_EXECUTOR_MEMORY=2g
docker-compose up -d

# Enable dynamic allocation
export SPARK_DYNAMIC_ALLOCATION_ENABLED=true
docker-compose restart spark-master
```

**❌ Error:** `Connection refused to Spark master`
```bash
# Verify Spark master is running
docker-compose exec spark-master curl http://localhost:8080

# Check network connectivity
docker-compose exec spark-worker-1 nc -zv spark-master 7077

# Restart Spark cluster
docker-compose restart spark-master spark-worker-1 spark-worker-2
```

### **Spark Streaming Issues**

**❌ Error:** `StreamingQueryException`
```bash
# Check streaming query status
docker-compose exec spark-master spark-sql \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1

# Monitor streaming queries
curl http://localhost:8081/api/v1/applications

# Check checkpoint directory
docker-compose exec stream-processor ls -la /app/checkpoints

# Clear corrupted checkpoints
docker-compose exec stream-processor rm -rf /app/checkpoints/*
docker-compose restart stream-processor
```

---

## 🌐 **APPLICATION ISSUES**

### **Backend API Problems**

**❌ Error:** `Connection refused on port 8000`
```bash
# Check backend container status
docker-compose ps backend

# View backend logs
docker-compose logs backend | tail -100

# Test API health endpoint
curl http://localhost:8000/health

# Restart backend service
docker-compose restart backend
```

**❌ Error:** `Internal Server Error 500`
```bash
# Check detailed backend logs
docker-compose logs backend | grep -i error

# Test database connectivity from backend
docker-compose exec backend python -c "
from sqlalchemy import create_engine
engine = create_engine('postgresql://dss_user:dss_password_123@postgres:5432/ecommerce_dss')
print(engine.execute('SELECT 1').scalar())
"

# Check environment variables
docker-compose exec backend env | grep -E "DB_|MONGO_"
```

### **Frontend Issues**

**❌ Error:** `Cannot connect to backend API`
```bash
# Check if backend is accessible from frontend
docker-compose exec frontend curl http://backend:8000/health

# Verify API URL configuration
docker-compose exec frontend env | grep REACT_APP_API_URL

# Check nginx proxy configuration
docker-compose exec nginx nginx -t

# View nginx logs
docker-compose logs nginx
```

**❌ Error:** `Frontend not loading`
```bash
# Check if frontend container is running
docker-compose ps frontend

# Test direct frontend access
curl http://localhost:3000

# Check for JavaScript errors
docker-compose logs frontend | grep -i error

# Rebuild frontend
docker-compose build frontend
docker-compose up -d frontend
```

---

## 📊 **MONITORING ISSUES**

### **Prometheus Problems**

**❌ Error:** `Prometheus not scraping targets`
```bash
# Check Prometheus configuration
docker-compose exec prometheus cat /etc/prometheus/prometheus.yml

# Verify target endpoints
curl http://localhost:9090/api/v1/targets

# Check Prometheus logs
docker-compose logs prometheus

# Restart Prometheus
docker-compose restart prometheus
```

### **Grafana Issues**

**❌ Error:** `Cannot connect to data source`
```bash
# Test Prometheus connectivity from Grafana
docker-compose exec grafana curl http://prometheus:9090/api/v1/status/config

# Check Grafana logs
docker-compose logs grafana

# Reset Grafana admin password
docker-compose exec grafana grafana-cli admin reset-admin-password admin123

# Access Grafana directly
open http://localhost:3001
```

---

## 🚀 **PERFORMANCE ISSUES**

### **High Memory Usage**

```bash
# Check memory usage by container
docker stats --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}"

# Identify memory-heavy containers
docker stats --no-stream --format "table {{.Container}}\t{{.MemUsage}}" | sort -k2 -h

# Increase Docker memory limits
# Edit docker-compose.yml and add:
#   deploy:
#     resources:
#       limits:
#         memory: 4G

# Restart services with new limits
docker-compose up -d
```

### **High CPU Usage**

```bash
# Monitor CPU usage
docker stats --format "table {{.Container}}\t{{.CPUPerc}}"

# Check for CPU-intensive processes
docker-compose exec [container] top

# Scale horizontally
docker-compose up -d --scale spark-worker=4

# Limit CPU usage
# Add to docker-compose.yml:
#   deploy:
#     resources:
#       limits:
#         cpus: '2.0'
```

### **Slow Performance**

```bash
# Check disk I/O
docker stats --format "table {{.Container}}\t{{.BlockIO}}"

# Monitor network I/O
docker stats --format "table {{.Container}}\t{{.NetIO}}"

# Check for disk space issues
df -h
docker system df

# Clean up unused resources
docker system prune -f
docker volume prune -f
```

---

## 🔧 **SYSTEM-LEVEL FIXES**

### **Docker Issues**

**❌ Error:** `Docker daemon not responding`
```bash
# Restart Docker service (Linux)
sudo systemctl restart docker

# Restart Docker Desktop (Windows/Mac)
# Use Docker Desktop interface

# Check Docker daemon logs
sudo journalctl -u docker.service -f

# Reset Docker to factory defaults (last resort)
# Use Docker Desktop: Settings > Troubleshoot > Reset to factory defaults
```

**❌ Error:** `No space left on device`
```bash
# Clean up Docker resources
docker system prune -a -f
docker volume prune -f
docker network prune -f

# Remove old containers and images
docker container prune -f
docker image prune -a -f

# Check disk usage
docker system df
df -h
```

### **Network Issues**

**❌ Error:** `Port already in use`
```bash
# Find process using port
lsof -i :8000  # Replace with your port
netstat -tulpn | grep 8000

# Kill process using port
sudo kill -9 [PID]

# Use different ports in docker-compose.yml
# Change: "8000:8000" to "8001:8000"

# Restart with new configuration
docker-compose down
docker-compose up -d
```

**❌ Error:** `Network timeout`
```bash
# Check Docker network
docker network ls
docker network inspect ecommerce-dss_dss_network

# Test container connectivity
docker-compose exec kafka nc -zv zookeeper 2181

# Reset Docker networks
docker-compose down
docker network prune -f
docker-compose up -d
```

---

## 🆘 **EMERGENCY PROCEDURES**

### **Complete System Reset**

```bash
# ⚠️ WARNING: This will delete all data
# Stop all services
docker-compose down -v

# Remove all containers, networks, volumes
docker system prune -a -f
docker volume prune -f

# Remove project-specific volumes
docker volume ls | grep ecommerce-dss | awk '{print $2}' | xargs docker volume rm

# Restart from scratch
./quick_start.sh
```

### **Backup and Restore**

```bash
# Backup databases
mkdir -p backups/$(date +%Y%m%d)

# Backup PostgreSQL
docker-compose exec postgres pg_dump -U dss_user ecommerce_dss > backups/$(date +%Y%m%d)/postgres_backup.sql

# Backup MongoDB
docker-compose exec mongodb mongodump --db ecommerce_dss --out /backups/$(date +%Y%m%d)

# Restore PostgreSQL
docker-compose exec -T postgres psql -U dss_user ecommerce_dss < backups/[date]/postgres_backup.sql

# Restore MongoDB
docker-compose exec mongodb mongorestore /backups/[date]/ecommerce_dss
```

---

## 📋 **Diagnostic Commands Cheat Sheet**

```bash
# General Health Check
docker-compose ps
docker-compose logs --tail=50
docker stats --no-stream

# Service-Specific Checks
docker-compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list
docker-compose exec postgres pg_isready -U dss_user
docker-compose exec mongodb mongosh --eval "db.adminCommand('ping')"
docker-compose exec redis redis-cli ping
docker-compose exec spark-master curl http://localhost:8080

# Network Diagnostics
docker network ls
docker-compose exec [service] nc -zv [target_service] [port]

# Resource Usage
docker stats --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}\t{{.BlockIO}}"

# Cleanup Commands
docker-compose down -v
docker system prune -f
docker volume prune -f
```

---

## 🎯 **Getting Help**

### **Log Collection for Support**
```bash
# Collect all logs
mkdir -p debug_logs/$(date +%Y%m%d_%H%M%S)
docker-compose logs > debug_logs/$(date +%Y%m%d_%H%M%S)/all_services.log
docker-compose ps > debug_logs/$(date +%Y%m%d_%H%M%S)/container_status.txt
docker stats --no-stream > debug_logs/$(date +%Y%m%d_%H%M%S)/resource_usage.txt
docker system df > debug_logs/$(date +%Y%m%d_%H%M%S)/disk_usage.txt

# Package for support
tar -czf debug_logs_$(date +%Y%m%d_%H%M%S).tar.gz debug_logs/
```

### **Community Support**
- 📖 **Documentation**: Check STREAMING_ARCHITECTURE_GUIDE.md
- 🐛 **Issues**: Create GitHub issue with debug logs
- 💬 **Discussions**: Stack Overflow with tag `big-data-streaming`
- 📧 **Enterprise**: Contact support team

---

## 🔄 **Prevention Tips**

1. **Regular Monitoring**
   - Set up alerts in Grafana
   - Monitor disk space and memory usage
   - Check service health endpoints regularly

2. **Maintenance Schedule**
   - Weekly: Check logs for errors
   - Monthly: Clean up Docker resources
   - Quarterly: Update Docker images

3. **Backup Strategy**
   - Daily: Automated database backups
   - Weekly: Full system backup
   - Monthly: Backup validation

4. **Capacity Planning**
   - Monitor growth trends
   - Plan for 3x current capacity
   - Scale before hitting limits

---

**💡 Remember**: Most issues can be resolved by restarting the specific service or checking logs for detailed error messages. The streaming architecture is designed to be resilient and self-healing in most scenarios.