# Manual Deployment Guide

## Nếu gặp lỗi GitHub SSO, thử các cách này:

### 1. Railway CLI Deploy
```bash
# Install Railway CLI
npm install -g @railway/cli

# Deploy backend
cd backend
railway login  # Đăng nhập bằng email
railway init
railway deploy
```

### 2. Vercel CLI Deploy
```bash
# Install Vercel CLI
npm i -g vercel

# Deploy frontend
cd frontend
vercel login  # Đăng nhập bằng email
vercel
vercel --prod
```

### 3. Alternative Free Platforms:
- **Render**: render.com (thay Railway)
- **Netlify**: netlify.com (thay Vercel)
- **Heroku**: heroku.com
- **PlanetScale**: planetscale.com (thay Supabase)

### 4. Docker Deployment:
```bash
# Build và push lên Docker Hub
docker build -t your-username/ecommerce-backend ./backend
docker push your-username/ecommerce-backend

# Deploy trên bất kỳ platform nào hỗ trợ Docker
```

### 5. GitHub Codespaces:
- Tạo Codespace từ repository
- Deploy trực tiếp từ Codespace
- Có thể bypass một số lỗi SSO

## Giải pháp đơn giản nhất:
1. Đăng ký các dịch vụ bằng EMAIL thay vì GitHub
2. Sau đó connect repository manually