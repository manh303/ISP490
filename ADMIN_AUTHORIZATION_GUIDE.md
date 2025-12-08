# 🔐 Admin Authorization & Permission System

## 📋 Tóm Tắt Phân Quyền

### 1. Role Hierarchy (Cấp Độ Quyền)

```
ADMIN (Level 10) - Quyền cao nhất
  ├─ DATA_ENGINEER (Level 5)
  ├─ ML (Level 5)
  └─ ANALYST (Level 3) - Quyền thấp nhất
```

**File**: `backend/app/constants/roles.py`

### 2. ADMIN Role - Quyền Toàn Bộ

#### 📌 Modules (Mô-đun Được Truy Cập)
```python
"modules": [
    "Dashboard", 
    "User Management", 
    "Activity Logs", 
    "System Settings", 
    "Data Management"
]
```

#### 📌 Permissions (Quyền Chi Tiết)
```python
"permissions": [
    "system.admin",      # Quản lý hệ thống
    "user.manage",       # Quản lý người dùng
    "data.write",        # Ghi dữ liệu
    "analytics.view",    # Xem phân tích
    "dss.dashboard"      # Truy cập DSS Dashboard
]
```

#### 📌 Actions (Hành Động Cho Phép)
```python
"actions": [
    "view",           # Xem
    "create",         # Tạo
    "update",         # Cập nhật
    "delete",         # Xóa
    "manage_users",   # Quản lý người dùng
    "view_logs"       # Xem logs
]
```

#### 📌 Admin Features (Tính Năng Admin)
```python
"admin_features": {
    "user_management": True,           # ✅ Quản lý người dùng
    "activity_logs": True,             # ✅ Xem hoạt động
    "system_settings": True,           # ✅ Cài đặt hệ thống
    "user_creation": True,             # ✅ Tạo người dùng
    "user_deletion": True,             # ✅ Xóa người dùng
    "can_access_admin_panel": True     # ✅ Truy cập bảng điều khiển admin
}
```

---

## 🔑 Cách Hoạt Động Phân Quyền

### File: `backend/app/api/dependencies.py`

```python
def require_role(*role_codes: str):
    """Require user to have one of the specified roles"""
    async def dependency(
        current_user: Dict[str, Any] = Depends(get_current_user),
    ):
        user_roles = current_user.get("roles", [])
        # Kiểm tra nếu user có một trong các role được yêu cầu
        if not any(r in user_roles for r in role_codes):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Required roles: {role_codes}",
            )
        return current_user
    return dependency
```

**Cách sử dụng**:
```python
@router.get("/endpoint")
async def my_endpoint(
    current_user: dict = Depends(require_role("ADMIN"))  # ← Chỉ ADMIN
):
    ...

@router.get("/endpoint2")
async def my_endpoint2(
    current_user: dict = Depends(require_role("ADMIN", "ANALYST"))  # ← ADMIN hoặc ANALYST
):
    ...
```

---

## 📊 Bảng So Sánh Quyền Của Các Role

| Feature | ADMIN | DATA_ENGINEER | ML | ANALYST |
|---------|-------|---------------|----|---------| 
| User Management | ✅ | ❌ | ❌ | ❌ |
| Activity Logs | ✅ | ✅ | ❌ | ❌ |
| System Settings | ✅ | ❌ | ❌ | ❌ |
| Data Pipeline | ✅ | ✅ | ❌ | ❌ |
| ML Models | ✅ | ❌ | ✅ | ❌ |
| Analytics | ✅ | ✅ | ✅ | ✅ |
| Reports | ✅ | ✅ | ❌ | ✅ |
| DSS Dashboard | ✅ | ✅ | ✅ | ✅ |

---

## 🛡️ Cách Admin Sử Dụng Tất Cả API

### 1️⃣ Đăng Nhập Như Admin

```bash
POST /api/v1/auth/signin
Content-Type: application/json

{
  "email": "admin@dss.com",
  "password": "admin123"
}
```

**Response**:
```json
{
  "success": true,
  "message": "Welcome back, System Administrator!",
  "access_token": "eyJ0eXAiOiJKV1QiLCJhbGc...",
  "user": {
    "user_id": 1,
    "email": "admin@dss.com",
    "full_name": "System Administrator",
    "role": "ADMIN",
    "menu": {
      "modules": [...],
      "admin_features": {
        "user_management": true,
        "activity_logs": true,
        ...
      }
    }
  }
}
```

### 2️⃣ Sử Dụng Token Để Truy Cập API

Tất cả API endpoints bây giờ sẽ chấp nhận admin:

```bash
# ✅ Admin có thể truy cập tất cả endpoints này:
GET /api/v1/admin/users
GET /api/v1/admin/users/deleted
GET /api/v1/admin/activity-logs
GET /api/v1/admin/activity-stats

GET /api/v1/roles
GET /api/v1/roles/{role_id}

GET /api/v1/profile
PUT /api/v1/profile

GET /api/v1/analytics/dashboard
GET /api/v1/analytics/trends

GET /api/v1/ml/models
POST /api/v1/ml/predict

GET /api/v1/data-engineer/pipelines
GET /api/v1/business-metadata/catalog/datasets
GET /api/v1/dss/dashboard
```

**Header cần thiết**:
```
Authorization: Bearer {access_token}
```

---

## 🔄 Permission Hierarchy - Cách Kiểm Tra

### File: `backend/app/constants/roles.py`

```python
def has_permission(user_role: str, required_role: str) -> bool:
    """Check if user role has permission for required role"""
    user_level = ROLE_HIERARCHY.get(user_role, 0)
    required_level = ROLE_HIERARCHY.get(required_role, 0)
    return user_level >= required_level

# Ví dụ:
has_permission("ADMIN", "ANALYST")       # ✅ True  (10 >= 3)
has_permission("ADMIN", "DATA_ENGINEER") # ✅ True  (10 >= 5)
has_permission("ANALYST", "ADMIN")       # ❌ False (3 >= 10)
has_permission("DATA_ENGINEER", "ML")    # ✅ True  (5 >= 5)
```

---

## 📌 API Endpoints Được Bảo Vệ Bằng Phân Quyền

### Admin-Only Endpoints

| Endpoint | Method | Required Role |
|----------|--------|---------------|
| `/api/v1/admin/users` | GET, POST | ADMIN |
| `/api/v1/admin/users/{user_id}` | GET, PUT, DELETE | ADMIN |
| `/api/v1/admin/users/roles/{user_id}` | PUT | ADMIN |
| `/api/v1/admin/activity-logs` | GET | ADMIN |
| `/api/v1/roles` | GET, POST | ADMIN |
| `/api/v1/roles/{role_id}` | GET, PUT, DELETE | ADMIN |

### Multi-Role Endpoints

| Endpoint | Method | Required Roles |
|----------|--------|----------------|
| `/api/v1/profile` | GET, PUT | Any authenticated user |
| `/api/v1/analytics/*` | GET | ADMIN, DATA_ENGINEER, ANALYST |
| `/api/v1/dss/dashboard` | GET | ADMIN, DATA_ENGINEER, ML, ANALYST |
| `/api/v1/business-metadata/*` | GET | Any authenticated user |

---

## 🎯 Test Admin Access

### Script Test (Python)

```python
import requests

# 1. Đăng nhập
login_response = requests.post(
    "http://localhost:8000/api/v1/auth/signin",
    json={
        "email": "admin@dss.com",
        "password": "admin123"
    }
)

if login_response.status_code == 200:
    token = login_response.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}
    
    # 2. Test các endpoints khác nhau
    endpoints = [
        "/api/v1/admin/users",
        "/api/v1/roles",
        "/api/v1/profile",
        "/api/v1/dss/dashboard",
        "/api/v1/analytics/dashboard",
    ]
    
    for endpoint in endpoints:
        response = requests.get(
            f"http://localhost:8000{endpoint}",
            headers=headers
        )
        print(f"{endpoint}: {response.status_code}")
```

---

## 🔒 Bảo Mật

### Điểm Quan Trọng

1. **JWT Token**: Admin token chứa `role: "ADMIN"` và `roles: ["ADMIN"]`
2. **Kiểm Tra Server-Side**: Mọi kiểm tra quyền được thực hiện ở server
3. **Không Tin Tưởng Client**: Client không thể giả mạo quyền
4. **Token Expiration**: Token hết hạn sau 24 giờ (có thể cấu hình)

### Token Structure

```json
{
  "user_id": 1,
  "email": "admin@dss.com",
  "full_name": "System Administrator",
  "role": "ADMIN",
  "roles": ["ADMIN"],
  "permissions": ["system.admin", "user.manage", ...],
  "exp": 1704067200
}
```

---

## 📝 Cấu Hình Phân Quyền

Tất cả các role định nghĩa trong:
- **File**: `backend/app/constants/roles.py`
- **Sửa đổi**: Thêm/xóa permissions, modules, actions ở đây
- **Automatic Sync**: Tự động cập nhật menu khi người dùng đăng nhập

---

**Tóm tắt**: Admin có quyền truy cập **tất cả API** vì có level cao nhất (10). Các role khác bị hạn chế dựa trên level của chúng.
