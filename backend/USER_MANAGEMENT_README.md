# User Management System

## Tổng quan
Hệ thống quản lý tài khoản với chức năng xóa mềm và xóa cứng, sử dụng database hiện có mà không thay đổi cấu trúc.

## Logic xóa tài khoản
1. **Xóa lần 1 (Soft Delete)**: Chuyển `status` từ `active` → `disabled` (vào "list xóa tài khoản")
2. **Xóa lần 2 (Hard Delete)**: Xóa vĩnh viễn khỏi database (yêu cầu xác nhận)

## API Endpoints

### 1. Quản lý tài khoản cơ bản

#### Danh sách tài khoản active
```http
GET /api/v1/admin/users?page=1&limit=20
Authorization: Bearer <admin_token>
```

#### Chi tiết tài khoản
```http
GET /api/v1/admin/users/{user_id}
Authorization: Bearer <admin_token>
```

#### Thêm tài khoản mới
```http
POST /api/v1/admin/users
Authorization: Bearer <admin_token>
Content-Type: application/json

{
  "email": "user@example.com",
  "password": "password123",
  "full_name": "Nguyen Van A",
  "phone": "0123456789",
  "role_code": "CUSTOMER"
}
```

#### Cập nhật thông tin tài khoản
```http
PUT /api/v1/admin/users/{user_id}
Authorization: Bearer <admin_token>
Content-Type: application/json

{
  "full_name": "Nguyen Van B",
  "phone": "0987654321",
  "role_code": "ANALYST"
}
```

#### Đổi mật khẩu
```http
PUT /api/v1/admin/users/{user_id}/password
Authorization: Bearer <admin_token>
Content-Type: application/json

{
  "new_password": "newpassword123"
}
```

### 2. Quản lý xóa tài khoản

#### Xóa mềm (chuyển vào list xóa)
```http
PUT /api/v1/admin/users/{user_id}/disable
Authorization: Bearer <admin_token>
```

#### Danh sách tài khoản đã xóa
```http
GET /api/v1/admin/users/deleted?page=1&limit=20
Authorization: Bearer <admin_token>
```

#### Khôi phục tài khoản
```http
PUT /api/v1/admin/users/{user_id}/restore
Authorization: Bearer <admin_token>
```

#### Xóa vĩnh viễn (yêu cầu xác nhận)
```http
DELETE /api/v1/admin/users/{user_id}/permanent?confirm=true
Authorization: Bearer <admin_token>
```

## Quyền truy cập
- Chỉ user có role `ADMIN` mới có thể truy cập các endpoint này
- Token JWT phải hợp lệ và chứa role `ADMIN`

## Cấu trúc file đã tạo

```
backend/app/
├── api/v1/admin.py              # API endpoints
├── models/admin.py              # Pydantic schemas
├── services/user_management_service.py  # Business logic
└── utils/admin_helpers.py       # Helper functions
```

## Test với Postman/curl

### 1. Đăng nhập admin
```bash
curl -X POST "http://localhost:8000/api/v1/auth/signin" \
  -H "Content-Type: application/json" \
  -d '{
    "email": "admin@dss.com",
    "password": "admin123"
  }'
```

### 2. Sử dụng token để quản lý user
```bash
curl -X GET "http://localhost:8000/api/v1/admin/users" \
  -H "Authorization: Bearer <your_admin_token>"
```

### 3. Tạo user mới
```bash
curl -X POST "http://localhost:8000/api/v1/admin/users" \
  -H "Authorization: Bearer <your_admin_token>" \
  -H "Content-Type: application/json" \
  -d '{
    "email": "newuser@example.com",
    "password": "password123",
    "full_name": "New User",
    "role_code": "CUSTOMER"
  }'
```

### 4. Xóa mềm user
```bash
curl -X PUT "http://localhost:8000/api/v1/admin/users/4/disable" \
  -H "Authorization: Bearer <your_admin_token>"
```

### 5. Xem list đã xóa
```bash
curl -X GET "http://localhost:8000/api/v1/admin/users/deleted" \
  -H "Authorization: Bearer <your_admin_token>"
```

### 6. Xóa vĩnh viễn (cần xác nhận)
```bash
curl -X DELETE "http://localhost:8000/api/v1/admin/users/4/permanent?confirm=true" \
  -H "Authorization: Bearer <your_admin_token>"
```

## Lưu ý
- Tài khoản admin mặc định: `admin@dss.com` / `admin123`
- Hệ thống sử dụng database hiện có, không thay đổi cấu trúc
- Xóa vĩnh viễn sẽ xóa tất cả dữ liệu liên quan (sessions, roles, etc.)
- Có validation và error handling đầy đủ