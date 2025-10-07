# Hệ thống Login/Logout cho E-commerce DSS Project

## Tổng quan

Đã bổ sung hệ thống authentication đơn giản với dữ liệu cố định để test. Hệ thống bao gồm:

### Backend (FastAPI)
- **Simple Authentication API** với JWT tokens
- **Dữ liệu người dùng cố định** để test
- **Role-based access control** (admin, manager, analyst, user)

### Frontend (React + TypeScript)
- **AuthContext** để quản lý state authentication
- **AuthService** để gọi API
- **Protected Routes** để bảo vệ trang
- **Login/Logout UI** hoàn chỉnh

## Dữ liệu Test Accounts

| Username | Password | Role | Full Name | Email |
|----------|----------|------|-----------|-------|
| admin | admin123 | admin | System Administrator | admin@ecommerce-dss.com |
| user1 | user123 | user | Test User One | user1@ecommerce-dss.com |
| manager | manager123 | manager | System Manager | manager@ecommerce-dss.com |
| analyst | analyst123 | analyst | Data Analyst | analyst@ecommerce-dss.com |

## Cách chạy dự án

### 1. Backend Setup

```bash
cd backend
pip install -r requirements.txt
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### 2. Frontend Setup

```bash
cd frontend
npm install
npm run dev
```

### 3. Truy cập ứng dụng

- Frontend: http://localhost:5173
- Backend API: http://localhost:8000
- API Documentation: http://localhost:8000/docs

## Tính năng đã thêm

### Backend Endpoints

**Authentication API** (`/api/v1/simple-auth/`):

- `POST /login` - Đăng nhập với username/password
- `POST /logout` - Đăng xuất (xóa token client-side)
- `GET /me` - Lấy thông tin user hiện tại
- `GET /validate` - Validate JWT token
- `GET /users` - Xem tất cả users (chỉ admin)
- `GET /test-credentials` - Lấy danh sách test accounts
- `GET /admin-only` - Example admin-only endpoint
- `GET /manager-or-admin` - Example role-based endpoint

### Frontend Components

**Authentication Service** (`src/services/authService.ts`):
- Quản lý API calls
- JWT token storage
- Auto-logout khi token hết hạn

**Auth Context** (`src/contexts/AuthContext.tsx`):
- Global state management
- Protected Route component
- Authentication hooks

**Updated Components**:
- `SignInForm` - Form đăng nhập với test credentials
- `UserDropdown` - Hiển thị thông tin user, logout button
- `App.tsx` - Protected routes setup

## Cách sử dụng

### 1. Đăng nhập

1. Truy cập http://localhost:5173
2. Click vào "🧪 Test Credentials" để xem các tài khoản test
3. Click vào bất kỳ tài khoản nào để auto-fill form
4. Click "Login"

### 2. Sau khi đăng nhập

- Truy cập được tất cả protected routes
- Xem thông tin user ở header (UserDropdown)
- Role badge hiển thị quyền của user

### 3. Đăng xuất

- Click vào avatar ở header
- Click "Sign out"

## Protected Routes

Tất cả routes trong dashboard đều được bảo vệ:
- `/dashboard`
- `/profile`
- `/calendar`
- `/form-elements`
- `/basic-tables`
- Và tất cả UI elements, charts...

## Role-based Access

Có thể mở rộng để kiểm tra quyền theo role:

```tsx
<ProtectedRoute requiredRole="admin">
  <AdminOnlyComponent />
</ProtectedRoute>
```

## Environment Variables

Frontend `.env` file:
```
VITE_API_URL=http://localhost:8000
```

## API Testing

Có thể test API bằng curl:

```bash
# Login
curl -X POST "http://localhost:8000/api/v1/simple-auth/login" \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "admin123"}'

# Get user info (cần token)
curl -X GET "http://localhost:8000/api/v1/simple-auth/me" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN"
```

## Lưu ý quan trọng

1. **Đây là hệ thống test** - Passwords được lưu plain text
2. **Production setup** sẽ cần:
   - Hash passwords
   - Secure JWT secrets
   - Database connection
   - Environment variables cho sensitive data

3. **Token persistence** - Tokens được lưu trong localStorage và sẽ persist qua browser sessions

4. **Auto-redirect** - Users đã đăng nhập sẽ tự động redirect từ login page

## Troubleshooting

### Backend không chạy được
- Kiểm tra port 8000 có bị occupied không
- Đảm bảo đã install requirements.txt
- Kiểm tra Python version compatibility

### Frontend không connect được backend
- Kiểm tra VITE_API_URL trong .env
- Đảm bảo backend đang chạy trên đúng port
- Check CORS settings nếu có lỗi cross-origin

### Login không work
- Check browser console cho errors
- Verify API endpoints trong Network tab
- Đảm bảo backend có include simple_auth router

### Không redirect sau login
- Check AuthContext implementation
- Verify protected route setup
- Check navigation logic trong SignInForm

## Files đã thêm/sửa

### Backend
- ✅ `backend/app/simple_auth.py` - Simple auth endpoints
- ✅ `backend/app/main.py` - Include simple auth router

### Frontend
- ✅ `frontend/src/services/authService.ts` - Auth service
- ✅ `frontend/src/contexts/AuthContext.tsx` - Auth context
- ✅ `frontend/src/components/auth/SignInForm.tsx` - Updated login form
- ✅ `frontend/src/components/header/UserDropdown.tsx` - Updated with auth
- ✅ `frontend/src/App.tsx` - Protected routes setup
- ✅ `frontend/.env` - Environment config

Hệ thống đã sẵn sàng sử dụng! 🚀