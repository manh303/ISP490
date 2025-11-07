import React, { useEffect, useState } from "react";
import { userApi } from "../../services/userApi";
import { Button } from '../../components/ui/figma/button';
import { Badge } from '../../components/ui/figma/badge';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '../../components/ui/figma/select';

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

const ROLE_OPTIONS = [
  { code: "ADMIN", name: "Admin" },
  { code: "ANALYST", name: "Analyst" },
  { code: "CUSTOMER", name: "Customer" },
  { code: "MANAGER", name: "Manager" },
];

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
      .catch((err) => setError(err?.response?.data?.detail || "Lỗi không xác định"))
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
      setError(err?.response?.data?.detail || "Cập nhật thất bại");
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
      setError(err?.response?.data?.detail || "Cập nhật mật khẩu thất bại");
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
      setError(err?.response?.data?.detail || "Vô hiệu hóa thất bại");
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
      setError(err?.response?.data?.detail || "Khôi phục thất bại");
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

  if (loading) return <div className="text-gray-500">Đang tải...</div>;
  if (error) return <div className="text-red-500">{error}</div>;
  if (!user) return null;

  return (
    <div className="bg-white rounded-lg shadow border border-gray-200 max-w-md mx-auto p-6">
      <Button variant="outline" className="mb-4" onClick={onBack}>Quay lại</Button>
      <h2 className="text-xl font-semibold mb-4">Chi tiết người dùng</h2>
      {editMode ? (
        <div className="space-y-4">
          <div>
            <label className="block text-gray-700 mb-1">Họ tên</label>
            <input className="w-full border border-gray-300 rounded px-3 py-2 focus:outline-none focus:ring" value={form.full_name} onChange={e => setForm(f => ({ ...f, full_name: e.target.value }))} />
          </div>
          <div>
            <label className="block text-gray-700 mb-1">Số điện thoại</label>
            <input className="w-full border border-gray-300 rounded px-3 py-2 focus:outline-none focus:ring" value={form.phone} onChange={e => setForm(f => ({ ...f, phone: e.target.value }))} />
          </div>
          <div>
            <label className="block text-gray-700 mb-1">Vai trò</label>
            <Select value={form.role_code} onValueChange={val => setForm(f => ({ ...f, role_code: val }))}>
              <SelectTrigger className="w-full border border-gray-300 bg-white">
                <SelectValue placeholder="Chọn vai trò" />
              </SelectTrigger>
              <SelectContent>
                {ROLE_OPTIONS.map(opt => (
                  <SelectItem key={opt.code} value={opt.code}>{opt.name}</SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <Button className="w-full" onClick={handleUpdate} disabled={loading}>Lưu thay đổi</Button>
        </div>
      ) : (
        <div className="space-y-2">
          <div className="flex items-center gap-2">
            <span className="font-semibold">Họ tên:</span> <span>{user.full_name}</span>
          </div>
          <div className="flex items-center gap-2">
            <span className="font-semibold">Email:</span> <span>{user.email}</span>
          </div>
          <div className="flex items-center gap-2">
            <span className="font-semibold">Số điện thoại:</span> <span>{user.phone}</span>
          </div>
          <div className="flex items-center gap-2">
            <span className="font-semibold">Vai trò:</span>
            <Badge variant={user.role_name === 'Admin' ? 'default' : 'secondary'}>{user.role_name}</Badge>
          </div>
          <div className="flex items-center gap-2">
            <span className="font-semibold">Trạng thái:</span>
            <Badge variant={user.status === 'Active' ? 'default' : 'destructive'}>{user.status}</Badge>
          </div>
          <Button className="w-full mt-2" variant="outline" onClick={() => setEditMode(true)}>Chỉnh sửa</Button>
        </div>
      )}
      <div className="mt-6">
        <h3 className="font-semibold mb-2">Đổi mật khẩu</h3>
        <div className="flex gap-2">
          <input className="flex-1 border border-gray-300 rounded px-3 py-2 focus:outline-none focus:ring" type="password" value={password} onChange={e => setPassword(e.target.value)} placeholder="Mật khẩu mới" />
          <Button variant="outline" onClick={handleUpdatePassword} disabled={loading}>Cập nhật</Button>
        </div>
      </div>
      <div className="mt-6 flex flex-col gap-2">
        <Button variant="destructive" onClick={handleDisable} disabled={loading}>Vô hiệu hóa</Button>
        <Button variant="outline" onClick={handleRestore} disabled={loading}>Khôi phục</Button>
        <Button variant="destructive" className="bg-red-700 hover:bg-red-800" onClick={handlePermanentDelete} disabled={loading}>Xóa vĩnh viễn</Button>
      </div>
    </div>
  );
}
