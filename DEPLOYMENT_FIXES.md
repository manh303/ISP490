# 🛠️ Deployment Fixes - Render & Vercel

## ❌ Lỗi đã gặp và ✅ Cách sửa

### 🔧 **Issue 1: Render "unknown type 'postgres'"**

**Lỗi:**
```
unknown type "postgres" khi deploy render.yaml
```

**Nguyên nhân:**
Render Blueprint không support trực tiếp PostgreSQL service type trong yaml. Database phải tạo manual.

**✅ Giải pháp:**

#### **Bước 1: Deploy backend trước**
```yaml
# render.yaml đã được fix - chỉ có backend service
services:
  - type: web
    name: ecommerce-dss-backend
    env: python
    plan: free
    # ... rest of config
```

#### **Bước 2: Tạo PostgreSQL database manual**
1. Vào Render Dashboard
2. Click "New" → "PostgreSQL"
3. Đặt tên: `ecommerce-dss-db`
4. Plan: Free
5. Copy Internal Database URL

#### **Bước 3: Add DATABASE_URL vào backend**
1. Vào backend service
2. Environment → Add Environment Variable
3. Key: `DATABASE_URL`
4. Value: `postgresql://...` (từ database bạn vừa tạo)

### 🔧 **Issue 2: Vercel Frontend 404 NOT_FOUND**

**Lỗi:**
```
404: NOT_FOUND
Code: NOT_FOUND
ID: hkg1::4nsrb-1760709326876-e127ab991440
```

**Nguyên nhân:**
Vercel không biết cách route SPA (Single Page Application). Tất cả routes cần redirect về `index.html`.

**✅ Giải pháp:**

#### **File `vercel.json` đã được tạo:**
```json
{
  "rewrites": [
    {
      "source": "/(.*)",
      "destination": "/index.html"
    }
  ]
}
```

#### **Deploy lại Vercel:**
```bash
# Push changes
git add frontend/vercel.json
git commit -m "Fix Vercel SPA routing"
git push

# Vercel sẽ auto-redeploy
```

## 🚀 **Deployment Process mới (Fixed)**

### **1. Render Backend Deployment**

```bash
# 1. Push code với render.yaml đã fix
git add render.yaml
git commit -m "Fix Render deployment config"
git push origin main

# 2. Deploy trên Render
- New → Blueprint
- Select repository
- Apply (chỉ backend service sẽ được tạo)

# 3. Tạo PostgreSQL database manually
- New → PostgreSQL
- Name: ecommerce-dss-db
- Plan: Free

# 4. Connect database to backend
- Backend service → Environment
- Add: DATABASE_URL = "postgres://..."
- Redeploy backend
```

### **2. Vercel Frontend Deployment**

```bash
# 1. Ensure vercel.json exists
cat frontend/vercel.json

# 2. Deploy to Vercel
- Import từ GitHub
- Framework: Vite/React
- Root Directory: frontend
- Build Command: npm run build
- Output Directory: dist

# 3. Add environment variables
VITE_API_BASE_URL=https://ecommerce-dss-backend.onrender.com
```

## 🧪 **Testing Sau Khi Fix**

### **Backend (Render):**
```bash
# Health check
curl https://ecommerce-dss-backend.onrender.com/health

# API Status
curl https://ecommerce-dss-backend.onrender.com/api/v1/status

# Swagger UI
# Visit: https://ecommerce-dss-backend.onrender.com/docs
```

### **Frontend (Vercel):**
```bash
# Test routing
curl https://ecommerce-dss-frontend.vercel.app/
curl https://ecommerce-dss-frontend.vercel.app/dashboard
curl https://ecommerce-dss-frontend.vercel.app/any-route

# All should return 200 OK (not 404)
```

## 📋 **Checklist After Fixes**

### **Render Backend:**
- [ ] Backend service deployed successfully
- [ ] PostgreSQL database created manually
- [ ] DATABASE_URL environment variable added
- [ ] Health endpoint returns 200
- [ ] Swagger UI accessible
- [ ] API endpoints working

### **Vercel Frontend:**
- [ ] vercel.json file exists
- [ ] SPA routing works (no 404s)
- [ ] API connection to Render backend works
- [ ] All routes load correctly
- [ ] Build process successful

## 🔍 **Monitoring & Logs**

### **Render:**
```bash
# View logs
render logs ecommerce-dss-backend --follow

# Check metrics
render metrics ecommerce-dss-backend
```

### **Vercel:**
```bash
# View deployment logs
vercel logs ecommerce-dss-frontend

# Check analytics
vercel analytics
```

## 🛡️ **Security Updates After Fix**

### **CORS Configuration:**
Backend đã được cập nhật để accept Vercel domain:
```python
CORS_ORIGINS=https://ecommerce-dss-frontend.vercel.app,http://localhost:3000
```

### **Environment Variables:**
```env
# Render Backend
ENVIRONMENT=production
DEBUG=false
SECRET_KEY=auto-generated
JWT_SECRET_KEY=auto-generated
DATABASE_URL=manual-input-required

# Vercel Frontend
VITE_API_BASE_URL=https://ecommerce-dss-backend.onrender.com
```

## 🎯 **Final URLs After Fix**

- **Backend API**: `https://ecommerce-dss-backend.onrender.com`
- **Swagger Docs**: `https://ecommerce-dss-backend.onrender.com/docs`
- **Frontend App**: `https://ecommerce-dss-frontend.vercel.app`

**Test credentials:**
```json
{
  "username": "demo",
  "password": "demo123"
}
```

---

## 💡 **Pro Tips**

1. **Render PostgreSQL**: Free tier có giới hạn 1GB, monitor usage
2. **Vercel SPA**: Always test routing với direct URL access
3. **CORS**: Update CORS_ORIGINS khi thay đổi domain
4. **Environment Variables**: Never commit secrets, use platform environment variables

Deployment issues đã được resolved! 🎉