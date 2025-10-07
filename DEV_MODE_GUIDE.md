# 🚀 HƯỚNG DẪN CHẠY DEVELOPMENT MODE

**Vấn đề:** Localhost:3000 chỉ hiển thị static HTML thay vì React dev server với hot reload
**Giải pháp:** Sử dụng Development Docker Compose để chạy Vite dev server

---

## 🎯 **HAI CHẾ ĐỘ DEPLOYMENT:**

### 📦 **1. PRODUCTION MODE (Hiện tại - Static Files):**
```bash
# Chạy production build với Nginx
docker-compose up -d

# Kết quả:
# - Build React thành static files
# - Serve qua Nginx
# - Không có hot reload
# - Hiển thị index.html tĩnh
```

### 🔥 **2. DEVELOPMENT MODE (React Dev Server):**
```bash
# Chạy development mode với Vite dev server
docker-compose -f docker-compose.yml -f docker-compose.dev.yml up -d

# Kết quả:
# - Vite dev server chạy trực tiếp
# - Hot reload enabled
# - React router hoạt động đầy đủ
# - Development tools available
```

---

## 🛠️ **CÁCH CHUYỂN SANG DEVELOPMENT MODE:**

### **Bước 1: Stop container hiện tại**
```bash
cd C:\DoAn_FPT_FALL2025\ecommerce-dss-project
docker-compose stop frontend
```

### **Bước 2: Build development image**
```bash
docker-compose -f docker-compose.yml -f docker-compose.dev.yml build frontend
```

### **Bước 3: Start với development mode**
```bash
docker-compose -f docker-compose.yml -f docker-compose.dev.yml up -d frontend
```

### **Bước 4: Verify Vite dev server**
```bash
# Check logs để confirm Vite đang chạy
docker-compose logs frontend

# Bạn sẽ thấy:
# VITE v6.1.0  ready in 218 ms
# ➜  Local:   http://localhost:3000/
# ➜  Network: http://172.18.0.x:3000/
```

---

## 🎯 **SO SÁNH HAI CHÊẾ ĐỘ:**

| Tính năng | Production Mode | Development Mode |
|-----------|----------------|------------------|
| **Build process** | Static build → Nginx | Vite dev server |
| **Hot reload** | ❌ Không | ✅ Có |
| **React Router** | ⚠️ Hạn chế | ✅ Đầy đủ |
| **Dev tools** | ❌ Không | ✅ Có |
| **Performance** | ⚡ Nhanh | 🐌 Chậm hơn |
| **Debugging** | ❌ Khó | ✅ Dễ |
| **File watching** | ❌ Không | ✅ Có |

---

## 🔥 **DEVELOPMENT MODE FEATURES:**

### ✅ **Hot Reload:**
- Code changes tự động refresh browser
- Không cần restart container
- State preservation trong React

### ✅ **React Router đầy đủ:**
- Client-side routing hoạt động
- SPA navigation smooth
- No more index.html tĩnh

### ✅ **Development Tools:**
- React DevTools support
- Source maps available
- Better error messages

### ✅ **File Mounting:**
- Source code mounted từ host
- Changes reflect ngay lập tức
- No need to rebuild container

---

## 🛠️ **FILES ĐÃ TẠO:**

### 📁 **`frontend/Dockerfile.dev`:**
```dockerfile
# Development Dockerfile for React with Hot Reload
FROM node:18-alpine

WORKDIR /app

# Copy package files and install dependencies
COPY package*.json ./
RUN npm install

# Copy source code
COPY src ./src
COPY public ./public
COPY index.html ./

# Expose port and start Vite dev server
EXPOSE 3000
ENV NODE_ENV=development
CMD ["npm", "run", "dev", "--", "--host", "0.0.0.0", "--port", "3000"]
```

### 📁 **`docker-compose.dev.yml`:**
```yaml
# Development Docker Compose Override
services:
  frontend:
    build:
      context: ./frontend
      dockerfile: Dockerfile.dev
    environment:
      - NODE_ENV=development
      - CHOKIDAR_USEPOLLING=true  # Enable hot reload on Windows/WSL
    volumes:
      # Mount source code for hot reload
      - ./frontend/src:/app/src
      - ./frontend/public:/app/public
      - /app/node_modules  # Exclude node_modules
    command: ["npm", "run", "dev", "--", "--host", "0.0.0.0", "--port", "3000"]
```

---

## 🎯 **QUICK COMMANDS:**

### 🔄 **Switch to Development Mode:**
```bash
# One-liner to switch to dev mode
docker-compose stop frontend && \
docker-compose -f docker-compose.yml -f docker-compose.dev.yml up -d frontend
```

### 📊 **Check Status:**
```bash
# Check if Vite dev server is running
docker-compose logs frontend | grep "VITE.*ready"

# Check React app accessibility
curl http://localhost:3000
```

### 🔄 **Switch back to Production Mode:**
```bash
# Return to production static build
docker-compose stop frontend && \
docker-compose up -d frontend
```

---

## 🌐 **ACCESS URLs:**

### **Development Mode:**
- **React App:** http://localhost:3000 (Vite dev server)
- **Hot Reload:** ✅ Enabled
- **React Router:** ✅ Full SPA experience

### **Production Mode:**
- **Static App:** http://localhost:3000 (Nginx)
- **Hot Reload:** ❌ Disabled
- **React Router:** ⚠️ Limited (index.html fallback)

---

## 🔧 **TROUBLESHOOTING:**

### ❌ **Vẫn thấy static HTML:**
```bash
# Confirm container đang dùng dev image
docker-compose ps frontend

# Rebuild dev image
docker-compose -f docker-compose.yml -f docker-compose.dev.yml build --no-cache frontend

# Restart container
docker-compose -f docker-compose.yml -f docker-compose.dev.yml up -d frontend
```

### ❌ **Hot reload không hoạt động:**
```bash
# Check file mounting
docker-compose exec frontend ls -la /app/src

# Verify polling enabled
docker-compose logs frontend | grep CHOKIDAR
```

### ❌ **Port conflict:**
```bash
# Check if port 3000 is in use
netstat -an | grep :3000

# Kill conflicting processes
taskkill /F /IM node.exe
```

---

## 🎉 **SUCCESS VERIFICATION:**

### ✅ **Development mode thành công khi:**

1. **Vite logs hiển thị:**
   ```
   VITE v6.1.0  ready in 218 ms
   ➜  Local:   http://localhost:3000/
   ```

2. **React app loads:** http://localhost:3000 shows full React application

3. **Hot reload works:** Edit file trong `frontend/src/` → browser auto-refresh

4. **React Router works:** Navigation between pages smooth, no page refresh

5. **DevTools available:** React DevTools extension works in browser

---

**🎯 Bây giờ bạn có React development server với hot reload hoàn chỉnh!**

**Development URL:** http://localhost:3000 (Vite dev server)
**Features:** Hot reload + React Router + Dev tools + Source maps

**🔥 Perfect cho development workflow!**