# 🗑️ HƯỚNG DẪN XÓA VĨNH VIỄN TÀI KHOẢN NGƯỜI DÙNG

## ⚠️ CẢNH BÁO QUAN TRỌNG

**XÓA VĨNH VIỄN = KHÔNG THỂ HOÀN TÁC!**

Khi xóa vĩnh viễn một tài khoản, hệ thống sẽ xóa **TẤT CẢ** dữ liệu liên quan:
- ✅ Thông tin tài khoản (email, mật khẩu, tên, số điện thoại)
- ✅ Vai trò (roles) được gán
- ✅ Phiên đăng nhập (sessions)
- ✅ Lịch sử hoạt động (activity logs)
- ✅ Token xác thực email
- ✅ Token reset mật khẩu

---

## 📋 QUY TRÌNH XÓA AN TOÀN (KHUYẾN NGHỊ)

### Bước 1: Xóa Mềm (Soft Delete) - KHUYẾN NGHỊ
**Endpoint**: `PUT /api/v1/admin/users/{user_id}/disable`

```bash
# Ví dụ: Xóa mềm user có ID = 5
PUT http://localhost:8000/api/v1/admin/users/5/disable
```

**Kết quả**:
- User chuyển từ danh sách "Active" sang "Deleted"
- Status đổi từ `active` → `disabled`
- **CÓ THỂ KHÔI PHỤC** bằng endpoint `/restore`

---

### Bước 2: Khôi Phục (Nếu Xóa Nhầm)
**Endpoint**: `PUT /api/v1/admin/users/{user_id}/restore`

```bash
# Ví dụ: Khôi phục user có ID = 5
PUT http://localhost:8000/api/v1/admin/users/5/restore
```

**Kết quả**:
- User quay lại danh sách "Active"
- Status đổi từ `disabled` → `active`

---

## 🔥 XÓA VĨNH VIỄN (KHÔNG THỂ HOÀN TÁC)

### Khi Nào Nên Xóa Vĩnh Viễn?
- ✅ User vi phạm nghiêm trọng chính sách
- ✅ Dữ liệu giả/spam cần loại bỏ hoàn toàn
- ✅ Yêu cầu xóa dữ liệu từ chính user (GDPR/Privacy)
- ✅ User đã ở trạng thái "deleted" lâu ngày (>30 ngày)

### Cách Xóa Vĩnh Viễn

**Endpoint**: `DELETE /api/v1/admin/users/{user_id}?confirm=true`

**⚠️ BẮT BUỘC**: Phải thêm `?confirm=true` để xác nhận

#### Ví Dụ 1: Xóa bằng Swagger UI
1. Mở: http://localhost:8000/docs
2. Tìm endpoint: `DELETE /api/v1/admin/users/{user_id}`
3. Click "Try it out"
4. Nhập `user_id` (ví dụ: 5)
5. **Quan trọng**: Tick checkbox `confirm` = `true`
6. Click "Execute"

#### Ví Dụ 2: Xóa bằng cURL
```bash
curl -X DELETE "http://localhost:8000/api/v1/admin/users/5?confirm=true"
```

#### Ví Dụ 3: Xóa bằng Postman
```
DELETE http://localhost:8000/api/v1/admin/users/5?confirm=true
```

#### Ví Dụ 4: Xóa bằng JavaScript/Axios
```javascript
axios.delete('http://localhost:8000/api/v1/admin/users/5', {
  params: { confirm: true }
})
.then(response => {
  console.log('✅ Đã xóa:', response.data.message);
})
.catch(error => {
  console.error('❌ Lỗi:', error.response.data.detail);
});
```

---

## 📊 RESPONSE MESSAGES

### Thành Công (200 OK)
```json
{
  "success": true,
  "message": "✅ Đã xóa vĩnh viễn tài khoản user@example.com và tất cả dữ liệu liên quan",
  "user_id": 5
}
```

### Lỗi: Thiếu Xác Nhận (400 Bad Request)
```json
{
  "detail": "⚠️ Xóa vĩnh viễn cần xác nhận. Vui lòng thêm ?confirm=true vào URL"
}
```

### Lỗi: User Không Tồn Tại (404 Not Found)
```json
{
  "detail": "Không tìm thấy người dùng"
}
```

### Lỗi: Lỗi Server (500 Internal Server Error)
```json
{
  "detail": "Không thể xóa người dùng: [Chi tiết lỗi]"
}
```

---

## 🔍 KIỂM TRA SAU KHI XÓA

### Cách 1: Kiểm tra qua API
```bash
# Thử lấy thông tin user đã xóa
GET http://localhost:8000/api/v1/admin/users/5
# Kết quả: 404 Not Found (nếu đã xóa thành công)
```

### Cách 2: Kiểm tra trong database
```sql
-- Kiểm tra user đã bị xóa chưa
SELECT * FROM iam_user WHERE user_id = 5;
-- Kết quả: 0 rows (nếu đã xóa thành công)

-- Kiểm tra roles đã bị xóa chưa
SELECT * FROM iam_user_role WHERE user_id = 5;
-- Kết quả: 0 rows

-- Kiểm tra activity logs đã bị xóa chưa
SELECT * FROM user_activity_logs WHERE user_id = 5;
-- Kết quả: 0 rows
```

---

## 🛡️ BẢO MẬT & PHÒNG NGỪA

### 1. Không Cho Phép Xóa Admin Cuối Cùng
Nếu cần, thêm kiểm tra:
```python
# Đếm số admin còn lại
admin_count = await db.execute_query(
    "SELECT COUNT(*) FROM iam_user u JOIN iam_user_role ur ON u.user_id = ur.user_id WHERE ur.role_code = 'ADMIN'"
)
if admin_count <= 1:
    raise HTTPException(400, "Không thể xóa admin cuối cùng!")
```

### 2. Yêu Cầu Xác Thực Admin
Trong production, nên thêm middleware kiểm tra quyền ADMIN.

### 3. Lưu Log Audit
Hệ thống đã tự động log các thao tác xóa:
```
INFO: 🗑️ Attempting to permanently delete user_id: 5
INFO: 🔍 Found user to delete: user@example.com (ID: 5)
INFO: ✅ Successfully deleted user: user@example.com (ID: 5)
```

---

## 📝 LƯU Ý QUAN TRỌNG

1. **LUÔN XÓA MỀM TRƯỚC**: Sử dụng `/disable` trước, chỉ xóa vĩnh viễn khi chắc chắn 100%

2. **BACKUP TRƯỚC KHI XÓA**: Nếu xóa hàng loạt, nên backup database trước

3. **KIỂM TRA KỸ USER_ID**: Đảm bảo đúng user_id trước khi xóa

4. **KHÔNG XÓA USER ĐANG HOẠT ĐỘNG**: Nên disable trước, quan sát vài ngày, rồi mới xóa vĩnh viễn

5. **THÔNG BÁO CHO USER**: Nếu user yêu cầu xóa, nên thông báo trước 30 ngày

---

## 🆘 XỬ LÝ LỖI THƯỜNG GẶP

### Lỗi: "Foreign key constraint violation"
**Nguyên nhân**: Còn dữ liệu liên quan chưa xóa
**Giải pháp**: Code đã tự động xóa theo thứ tự:
1. User roles
2. User sessions
3. Activity logs
4. Password reset tokens
5. Email verification tokens
6. User record

### Lỗi: "User not found"
**Nguyên nhân**: User đã bị xóa trước đó hoặc user_id không tồn tại
**Giải pháp**: Kiểm tra lại user_id

### Lỗi: "confirm=true required"
**Nguyên nhân**: Quên thêm tham số confirm
**Giải pháp**: Thêm `?confirm=true` vào URL

---

## 📞 HỖ TRỢ

Nếu gặp vấn đề khi xóa user, kiểm tra:
1. **Server logs**: Xem chi tiết lỗi trong console
2. **Database logs**: Kiểm tra PostgreSQL logs
3. **Network**: Đảm bảo kết nối database ổn định

**Liên hệ**: Mở issue trên GitHub hoặc liên hệ admin hệ thống.

---

**Cập nhật**: November 15, 2025
**Phiên bản**: 2.0.0
