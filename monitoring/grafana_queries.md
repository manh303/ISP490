# 📊 Grafana Dashboard Queries - E-commerce DSS

## 🏥 System Health Panels

### 1. Services Status Table
- **Title**: 🏥 Services Health Status
- **Type**: Table
- **Query**: `up`
- **Options**: Enable "Instant"

### 2. Services Count
- **Title**: 📊 Services Summary
- **Type**: Stat
- **Queries**:
  - UP: `count(up == 1)`
  - DOWN: `count(up == 0)`
  - Total: `count(up)`

## 💻 System Resources

### 3. CPU Usage
- **Title**: 💻 CPU Usage %
- **Type**: Time series
- **Query**: `100 - (avg(irate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)`
- **Unit**: Percent (0-100)

### 4. Memory Usage
- **Title**: 💾 Memory Usage %
- **Type**: Time series
- **Query**: `(1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)) * 100`
- **Unit**: Percent (0-100)

### 5. Disk Usage
- **Title**: 💽 Disk Usage %
- **Type**: Time series
- **Query**: `(1 - (node_filesystem_avail_bytes{fstype!="tmpfs"} / node_filesystem_size_bytes{fstype!="tmpfs"})) * 100`
- **Unit**: Percent (0-100)

### 6. Load Average
- **Title**: ⚖️ System Load
- **Type**: Time series
- **Queries**:
  - 1m: `node_load1`
  - 5m: `node_load5`
  - 15m: `node_load15`

## 🌐 Network

### 7. Network Traffic
- **Title**: 🌐 Network Traffic
- **Type**: Time series
- **Queries**:
  - RX: `rate(node_network_receive_bytes_total{device!="lo"}[5m]) / 1024 / 1024`
  - TX: `rate(node_network_transmit_bytes_total{device!="lo"}[5m]) / 1024 / 1024`
- **Unit**: MB/s

### 8. Network Packets
- **Title**: 📦 Network Packets
- **Type**: Time series
- **Queries**:
  - RX Packets: `rate(node_network_receive_packets_total{device!="lo"}[5m])`
  - TX Packets: `rate(node_network_transmit_packets_total{device!="lo"}[5m])`

## 🗄️ Database Metrics

### 9. PostgreSQL Connections
- **Title**: 🗄️ PostgreSQL Connections
- **Type**: Time series
- **Query**: `pg_stat_database_numbackends`
- **Legend**: `Connections - {{datname}}`

### 10. PostgreSQL Transactions
- **Title**: 🔄 PostgreSQL Transactions
- **Type**: Time series
- **Queries**:
  - Commits: `rate(pg_stat_database_xact_commit[5m])`
  - Rollbacks: `rate(pg_stat_database_xact_rollback[5m])`

### 11. PostgreSQL Locks
- **Title**: 🔒 PostgreSQL Locks
- **Type**: Time series
- **Query**: `pg_locks_count`

### 12. Database Size
- **Title**: 📊 Database Sizes
- **Type**: Stat
- **Query**: `pg_database_size_bytes / 1024 / 1024`
- **Unit**: MB

## 📮 Redis Metrics

### 13. Redis Connections
- **Title**: 📮 Redis Connected Clients
- **Type**: Time series
- **Query**: `redis_connected_clients`

### 14. Redis Memory
- **Title**: 💾 Redis Memory Usage
- **Type**: Time series
- **Queries**:
  - Used: `redis_memory_used_bytes / 1024 / 1024`
  - Max: `redis_memory_max_bytes / 1024 / 1024`
- **Unit**: MB

### 15. Redis Commands
- **Title**: ⚡ Redis Commands/sec
- **Type**: Time series
- **Query**: `rate(redis_commands_processed_total[5m])`

### 16. Redis Hit Rate
- **Title**: 🎯 Redis Hit Rate %
- **Type**: Stat
- **Query**: `redis_keyspace_hits_total / (redis_keyspace_hits_total + redis_keyspace_misses_total) * 100`
- **Unit**: Percent (0-100)

## 📊 MongoDB Metrics

### 17. MongoDB Connections
- **Title**: 🍃 MongoDB Connections
- **Type**: Time series
- **Query**: `mongodb_connections`

### 18. MongoDB Operations
- **Title**: 🔄 MongoDB Operations
- **Type**: Time series
- **Queries**:
  - Inserts: `rate(mongodb_op_counters_total{type="insert"}[5m])`
  - Queries: `rate(mongodb_op_counters_total{type="query"}[5m])`
  - Updates: `rate(mongodb_op_counters_total{type="update"}[5m])`

## 📈 Application Metrics

### 19. Grafana Stats
- **Title**: 📊 Grafana Internal
- **Type**: Time series
- **Queries**:
  - Users: `grafana_stat_total_users`
  - Dashboards: `grafana_stat_totals_dashboard`
  - API Requests: `rate(grafana_api_response_status_total[5m])`

### 20. Kafka UI Metrics
- **Title**: 🔄 Kafka UI
- **Type**: Time series
- **Queries**:
  - JVM Memory: `jvm_memory_used_bytes / 1024 / 1024`
  - HTTP Requests: `rate(http_server_requests_seconds_count[5m])`

## 🚨 Alerting Panels

### 21. Active Alerts
- **Title**: 🚨 Active Alerts
- **Type**: Table
- **Query**: `ALERTS{alertstate="firing"}`

### 22. Alert Summary
- **Title**: ⚠️ Alert Summary
- **Type**: Stat
- **Queries**:
  - Firing: `count(ALERTS{alertstate="firing"})`
  - Pending: `count(ALERTS{alertstate="pending"})`

## 🔍 Advanced Queries

### 23. Top Memory Processes
- **Title**: 🔝 Top Memory Usage
- **Type**: Table
- **Query**: `topk(10, node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes)`

### 24. Prometheus Metrics Count
- **Title**: 📊 Total Metrics Collected
- **Type**: Stat
- **Query**: `count(count by (__name__)({__name__=~".+"}))`

### 25. Scrape Duration
- **Title**: ⏱️ Scrape Duration
- **Type**: Time series
- **Query**: `prometheus_target_scrape_duration_seconds`

## 🎨 Panel Settings Tips

### Time Series Panels:
- **Y-Axis**: Set appropriate min/max values
- **Unit**: Choose correct unit (bytes, percent, etc.)
- **Legend**: Use meaningful names with `{{label}}`
- **Thresholds**: Set warning/critical levels

### Stat Panels:
- **Value mappings**: Map 0→DOWN, 1→UP
- **Color mode**: Use thresholds for colors
- **Instant**: Enable for latest values

### Table Panels:
- **Transform**: Use "Organize fields" to rename columns
- **Cell display mode**: Use "Color background" for status
- **Instant**: Enable for latest values

## 📱 Dashboard Settings

### General:
- **Title**: E-commerce DSS - Complete Monitoring
- **Tags**: ecommerce, dss, monitoring, production
- **Timezone**: Browser

### Time Options:
- **Refresh**: 30s, 1m, 5m
- **Time range**: Last 1 hour (default)
- **Hide time picker**: No

### Variables (Optional):
- **Instance**: `label_values(up, instance)`
- **Job**: `label_values(up, job)`

Use these queries để tạo dashboard đầy đủ tính năng!