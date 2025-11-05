# E-commerce DSS Authentication System

## Tổng quan
Hệ thống authentication cho E-commerce Decision Support System sử dụng JWT tokens và FastAPI.

## Cài đặt

1. **Cài đặt dependencies:**
```bash
pip install -r requirements.txt
```

2. **Tạo file .env:**
```bash
cp .env.example .env
```

3. **Chạy database migrations:**
```bash
alembic upgrade head
```

4. **Tạo admin user:**
```bash
python scripts/create_admin.py
```

5. **Chạy server:**
```bash
uvicorn app.main:app --reload
```

## API Endpoints

### Authentication
- `POST /api/v1/auth/register` - Đăng ký user mới
- `POST /api/v1/auth/login` - Đăng nhập (form data)
- `POST /api/v1/auth/login-json` - Đăng nhập (JSON)
- `GET /api/v1/auth/me` - Lấy thông tin user hiện tại

### Sử dụng
1. Đăng ký hoặc đăng nhập để lấy access token
2. Sử dụng token trong header: `Authorization: Bearer <token>`

## Cấu trúc Database

### User Table
- id: Primary key
- username: Unique username
- email: Unique email
- hashed_password: Mật khẩu đã hash
- full_name: Tên đầy đủ
- is_active: Trạng thái active
- is_admin: Quyền admin
- created_at: Thời gian tạo
- updated_at: Thời gian cập nhật

## Security Features
- Password hashing với bcrypt
- JWT token authentication
- Token expiration
- Role-based access control (admin/user)
- CORS middleware

## Admin User
- Username: admin
- Password: admin123
- Email: admin@ecommerce-dss.com