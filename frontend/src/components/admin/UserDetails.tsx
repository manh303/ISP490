import React, { useEffect, useState } from "react";
import { userApi } from "../../services/userApi";
interface User {
  user_id: number;
  full_name: string;
  email: string;
  phone: string;
  role_code: string;
  role_name: string;
  status: string;
}

interface UserDetailsProps {
  userId: number;
  onBack: () => void;
}

export default function UserDetails({ userId, onBack }: UserDetailsProps) {
  const [user, setUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [editMode, setEditMode] = useState(false);
  const [form, setForm] = useState({ full_name: "", phone: "", role_code: "CUSTOMER" });
  const [password, setPassword] = useState("");

  useEffect(() => {
  setLoading(true);
  setError(null);
  userApi.getUser(userId)
    .then((user) => {
      setUser(user);
      setForm({
        full_name: user.full_name || "",
        phone: user.phone || "",
        role_code: user.role_code || "CUSTOMER"
      });
    })
    .catch((err) => setError( err?.response?.data?.detail ||"Lỗi không xác định" ))
    .finally(() => setLoading(false));
}, [userId]);

  const handleUpdate = async () => {
    setLoading(true);
    try {
      await userApi.updateUser(userId, {
        full_name: form.full_name,
        phone: form.phone,
        role_code: form.role_code
      });
      setEditMode(false);
   } catch (err: any) {
       setError( err?.response?.data?.detail || "Khôi phục thất bại");
    } finally {
      setLoading(false);
    }
  };

  const handleUpdatePassword = async () => {
    setLoading(true);
    try {
      await userApi.updateUserPassword(userId, password);
      setPassword("");
  } catch (err: any) {
       setError( err?.response?.data?.detail || "Khôi phục thất bại");
    } finally {
      setLoading(false);
    }
  };

  const handleDisable = async () => {
    setLoading(true);
    try {
      await userApi.disableUser(userId);
      onBack();
    } catch (err: any) {
       setError( err?.response?.data?.detail || "Khôi phục thất bại");
    } finally {
      setLoading(false);
    }
  };

  const handleRestore = async () => {
    setLoading(true);
    try {
      await userApi.restoreUser(userId);
      onBack();
    } catch (err: any) {
       setError( err?.response?.data?.detail || "Khôi phục thất bại");
    } finally {
      setLoading(false);
    }
  };

  const handlePermanentDelete = async () => {
    setLoading(true);
    try {
      await userApi.permanentDeleteUser(userId);
      onBack();
    } catch (err) {
      setError("Xóa vĩnh viễn thất bại");
    } finally {
      setLoading(false);
    }
  };

  if (loading) return <p>Đang tải...</p>;
  if (error) return <p className="text-red-500">{error}</p>;
  if (!user) return null;

  return (
    <div className="max-w-md mx-auto p-4 border rounded">
      <button className="mb-4 px-3 py-1 bg-gray-300 rounded" onClick={onBack}>Quay lại</button>
      <h2 className="text-xl font-semibold mb-4">Chi tiết người dùng</h2>
      {editMode ? (
        <div>
          <div className="mb-2">
            <label>Họ tên</label>
            <input className="w-full border p-2" value={form.full_name} onChange={e => setForm(f => ({ ...f, full_name: e.target.value }))} />
          </div>
          <div className="mb-2">
            <label>Số điện thoại</label>
            <input className="w-full border p-2" value={form.phone} onChange={e => setForm(f => ({ ...f, phone: e.target.value }))} />
          </div>
          <div className="mb-2">
            <label>Vai trò</label>
            <select className="w-full border p-2" value={form.role_code} onChange={e => setForm(f => ({ ...f, role_code: e.target.value }))}>
              <option value="ADMIN">Admin</option>
              <option value="ANALYST">Analyst</option>
              <option value="CUSTOMER">Customer</option>
              <option value="MANAGER">Manager</option>
            </select>
          </div>
          <button className="px-4 py-2 bg-blue-500 text-white rounded" onClick={handleUpdate}>Lưu thay đổi</button>
        </div>
      ) : (
        <div>
          <p><strong>Họ tên:</strong> {user.full_name}</p>
          <p><strong>Email:</strong> {user.email}</p>
          <p><strong>Số điện thoại:</strong> {user.phone}</p>
          <p><strong>Vai trò:</strong> {user.role_name}</p>
          <p><strong>Trạng thái:</strong> {user.status}</p>
          <button className="px-4 py-2 bg-yellow-500 text-white rounded mt-2" onClick={() => setEditMode(true)}>Chỉnh sửa</button>
        </div>
      )}
      <div className="mt-4">
        <h3 className="font-semibold mb-2">Đổi mật khẩu</h3>
        <input className="w-full border p-2 mb-2" type="password" value={password} onChange={e => setPassword(e.target.value)} placeholder="Mật khẩu mới" />
        <button className="px-4 py-2 bg-green-500 text-white rounded" onClick={handleUpdatePassword}>Cập nhật mật khẩu</button>
      </div>
      <div className="mt-4 flex gap-2">
        <button className="px-4 py-2 bg-red-500 text-white rounded" onClick={handleDisable}>Vô hiệu hóa</button>
        <button className="px-4 py-2 bg-green-500 text-white rounded" onClick={handleRestore}>Khôi phục</button>
        <button className="px-4 py-2 bg-red-700 text-white rounded" onClick={handlePermanentDelete}>Xóa vĩnh viễn</button>
      </div>
    </div>
  );
}
