# 📊 E-commerce DSS - Grafana Dashboard Setup Guide

## 🎯 Tổng Quan

Hướng dẫn chi tiết để setup và sử dụng Grafana Dashboard cho hệ thống E-commerce DSS monitoring.

## 🚀 Quick Setup (Tự Động)

### Cách 1: Sử dụng Script Tự Động

```bash
cd monitoring/
python import_dashboard.py
```

Script sẽ tự động:
- ✅ Chờ Grafana khởi động
- ✅ Cấu hình Prometheus datasource
- ✅ Import dashboard E-commerce DSS
- ✅ Tạo alert rules cơ bản

## 🔧 Manual Setup (Thủ Công)

### Bước 1: Truy Cập Grafana

1. Mở browser: `http://localhost:3001`
2. Đăng nhập:
   - Username: `admin`
   - Password: `admin`

### Bước 2: Thêm Prometheus Datasource

1. **Vào Settings → Data Sources**
2. **Click "Add data source"**
3. **Chọn "Prometheus"**
4. **Cấu hình:**
   - Name: `Prometheus`
   - URL: `http://prometheus:9090`
   - Access: `Server (default)`
5. **Click "Save & Test"**

### Bước 3: Import Dashboard

1. **Vào "+" → Import**
2. **Chọn "Upload JSON file"**
3. **Upload file:** `monitoring/ecommerce_dss_dashboard.json`
4. **Chọn Prometheus datasource**
5. **Click "Import"**

## 📊 Dashboard Panels Có Sẵn

### 🏥 System Health
- **Service Status**: Trạng thái tất cả services (UP/DOWN)
- **Health Summary**: Tổng quan health score
- **Service Discovery**: Danh sách targets Prometheus

### 💻 System Resources
- **CPU Usage**: % CPU đang sử dụng
- **Memory Usage**: % RAM đang sử dụng
- **Disk Usage**: % Disk space đã dùng
- **Network Traffic**: Bandwidth in/out

### 🗄️ Database Monitoring
- **PostgreSQL Connections**: Số connections active
- **PostgreSQL Transactions**: Commits/Rollbacks per second
- **Redis Metrics**: Connected clients, memory usage
- **Redis Commands**: Commands/sec, hit rate %
- **Database Sizes**: Kích thước databases

### 📈 Application Metrics
- **Backend Metrics**: Requests, connections, uptime (khi có)
- **Grafana Internal**: API responses, users
- **Performance Alerts**: Active alerts

### 📊 Monitoring Coverage
- **Services UP/DOWN**: Thống kê services
- **Total Metrics**: Số metrics đang được thu thập

## 🎛️ Dashboard Features

### 🔄 Auto Refresh
- Default: 30 seconds
- Options: 5s, 10s, 30s, 1m, 5m, 15m

### ⏰ Time Range
- Default: Last 1 hour
- Options: 5m, 15m, 1h, 6h, 12h, 24h, 7d

### 📱 Responsive Design
- Optimized cho desktop và mobile
- Auto-scaling panels

## 🚨 Alerts Setup

### Tự Động (Script)
Script đã tạo sẵn alert rules cơ bản:
- High CPU Usage (>80%)
- High Memory Usage (>85%)
- Service Down alerts

### Thủ Công
1. **Vào Alerting → Alert Rules**
2. **Click "New rule"**
3. **Cấu hình query và conditions**
4. **Set notification channels**

## 🎯 Sử Dụng Dashboard

### 💡 Tips Monitoring
1. **Monitor Service Status**: Luôn check panel "Service Health Status"
2. **Watch Resource Usage**: CPU, Memory, Disk không nên >80%
3. **Database Health**: PostgreSQL connections nên <50
4. **Redis Performance**: Hit rate nên >80%

### 🔍 Troubleshooting Panels
- **High CPU**: Check CPU panel → identify processes
- **Memory Issues**: Memory panel → check for leaks
- **Database Slow**: PostgreSQL panels → check connections
- **Cache Issues**: Redis panels → check hit rates

## 📈 Advanced Features

### 🎨 Customization
```json
// Thêm panel mới
{
  "title": "Custom Metric",
  "type": "graph",
  "targets": [
    {
      "expr": "your_custom_metric",
      "legendFormat": "Custom Legend"
    }
  ]
}
```

### 🔧 Variables Setup
1. **Dashboard Settings → Variables**
2. **Add variable:**
   - Name: `instance`
   - Type: `Query`
   - Query: `label_values(up, instance)`

### 📊 Panel Types Available
- **Graph**: Time series data
- **Stat**: Single value display
- **Table**: Tabular data
- **Logs**: Log entries
- **Heatmap**: Data density
- **Gauge**: Progress indicators

## 🌐 URLs & Access

- **Grafana**: http://localhost:3001
- **Prometheus**: http://localhost:9090
- **Dashboard**: http://localhost:3001/d/ecommerce-dss

## 🛠️ Maintenance

### 📊 Dashboard Updates
1. Export current dashboard
2. Modify JSON file
3. Re-import with overwrite

### 🔄 Backup Dashboard
```bash
# Export dashboard
curl -u admin:admin http://localhost:3001/api/dashboards/uid/YOUR_UID > backup.json
```

### 🗑️ Reset Dashboard
```bash
# Delete and re-import
python import_dashboard.py
```

## ❓ FAQ

**Q: Dashboard không hiển thị data?**
A: Check Prometheus datasource connection và đảm bảo metrics đang được scraped.

**Q: Alerts không hoạt động?**
A: Kiểm tra notification channels và alert rule conditions.

**Q: Panel hiển thị "No data"?**
A: Check metric names và ensure services đang expose metrics.

**Q: Dashboard load chậm?**
A: Reduce time range hoặc increase refresh interval.

## 🎉 Kết Luận

Dashboard E-commerce DSS cung cấp monitoring toàn diện cho:
- ✅ System resources (CPU, Memory, Disk)
- ✅ Database performance (PostgreSQL, Redis, MongoDB)
- ✅ Application metrics (Backend, Grafana)
- ✅ Service health và availability
- ✅ Real-time alerts và notifications

**Dashboard đã sẵn sàng cho production monitoring!** 🚀