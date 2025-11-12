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
  editMode?: boolean;
}

const ROLE_OPTIONS = [
  { code: "ADMIN", name: "Admin" },
  { code: "ANALYST", name: "Analyst" },
  { code: "CUSTOMER", name: "Customer" },
  { code: "MANAGER", name: "Manager" },
];

export default function UserDetails(props: UserDetailsProps) {
  const { userId, onBack, editMode } = props;
  const [user, setUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [editModeState, setEditModeState] = useState<boolean>(!!editMode);
  const [form, setForm] = useState({ full_name: "", phone: "", role_code: "CUSTOMER" });
  const [password, setPassword] = useState("");
  const [showRoleDropdown, setShowRoleDropdown] = useState(false);

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
  setEditModeState(false);
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

  // const handlePermanentDelete = async () => {
  //   setLoading(true);
  //   try {
  //     await userApi.permanentDeleteUser(userId);
  //     onBack();
  //   } catch (err) {
  //     setError("Xóa vĩnh viễn thất bại");
  //   } finally {
  //     setLoading(false);
  //   }
  // };

  if (loading) return <div className="text-gray-500 text-center py-8">Đang tải dữ liệu người dùng...</div>;
  if (error) return <div className="text-red-500 text-center py-8">{error}</div>;
  if (!user) return null;

  return (
    <div className="bg-white rounded-xl shadow-lg border border-gray-200 max-w-lg mx-auto p-8">
      <Button variant="outline" className="mb-6" onClick={onBack}>
        ← Quay lại danh sách
      </Button>
      <h2 className="text-2xl font-bold mb-6 text-gray-800">Chi tiết người dùng</h2>

      {/* Thông tin người dùng */}
      <div className="mb-8">
        {editModeState ? (
          <div className="space-y-5">
            <div>
              <label className="block text-base font-medium text-gray-700 mb-2">Họ tên</label>
              <input className="w-full border border-gray-300 rounded-lg px-4 py-2 focus:outline-none focus:ring focus:border-brand-500 text-gray-900 text-base" value={form.full_name} onChange={e => setForm(f => ({ ...f, full_name: e.target.value }))} placeholder="Nhập họ tên" />
            </div>
            <div>
              <label className="block text-base font-medium text-gray-700 mb-2">Số điện thoại</label>
              <input className="w-full border border-gray-300 rounded-lg px-4 py-2 focus:outline-none focus:ring focus:border-brand-500 text-gray-900 text-base" value={form.phone} onChange={e => setForm(f => ({ ...f, phone: e.target.value }))} placeholder="Nhập số điện thoại" />
            </div>
            <div>
              <label className="block text-base font-medium text-gray-700 mb-2">Vai trò</label>
              <Select value={form.role_code} onValueChange={val => setForm(f => ({ ...f, role_code: val }))}>
                <div className="relative w-full">
                  <button
                    type="button"
                    className="w-full border border-gray-300 bg-white rounded-lg px-4 py-2 text-left flex justify-between items-center focus:outline-none"
                    onClick={() => setShowRoleDropdown(v => !v)}
                  >
                    {ROLE_OPTIONS.find(opt => opt.code === form.role_code)?.name || "Chọn vai trò"}
                    <span className="ml-2 text-gray-400">▼</span>
                  </button>
                  {showRoleDropdown && (
                    <ul className="absolute z-10 w-full bg-white border border-gray-200 rounded-lg mt-1 shadow-lg max-h-60 overflow-auto">
                      {ROLE_OPTIONS.map(opt => (
                        <li
                          key={opt.code}
                          className={`px-4 py-3 text-base cursor-pointer hover:bg-blue-50 ${form.role_code === opt.code ? 'bg-blue-100 font-semibold' : ''}`}
                          onClick={() => { setForm(f => ({ ...f, role_code: opt.code })); setShowRoleDropdown(false); }}
                        >
                          {opt.name}
                        </li>
                      ))}
                    </ul>
                  )}
                </div>
              </Select>
            </div>
            <div className="flex gap-3 mt-2">
              <Button className="flex-1" onClick={handleUpdate} disabled={loading} variant="default">Lưu thay đổi</Button>
              <Button className="flex-1" variant="outline" onClick={() => setEditModeState(false)}>Hủy</Button>
            </div>
          </div>
        ) : (
          <div className="space-y-3">
            <div className="flex items-center gap-2 text-base">
              <span className="font-semibold text-gray-700">Họ tên:</span> <span className="text-gray-900">{user.full_name}</span>
            </div>
            <div className="flex items-center gap-2 text-base">
              <span className="font-semibold text-gray-700">Email:</span> <span className="text-gray-900">{user.email}</span>
            </div>
            <div className="flex items-center gap-2 text-base">
              <span className="font-semibold text-gray-700">Số điện thoại:</span> <span className="text-gray-900">{user.phone}</span>
            </div>
            <div className="flex items-center gap-2 text-base">
              <span className="font-semibold text-gray-700">Vai trò:</span>
              <Badge variant={user.role_name === 'Admin' ? 'default' : 'secondary'}>{user.role_name}</Badge>
            </div>
            <div className="flex items-center gap-2 text-base">
              <span className="font-semibold text-gray-700">Trạng thái:</span>
              {user.status === 'active' ? (
                <Badge variant="default" className="bg-green-500 text-white">Hoạt động</Badge>
              ) : (
                <Badge variant="destructive" className="bg-gray-500 text-white">Vô hiệu hóa</Badge>
              )}
            </div>
            <Button className="w-full mt-4" variant="outline" onClick={() => setEditModeState(true)}>
              <span className="font-semibold">Chỉnh sửa thông tin</span>
            </Button>
          </div>
        )}
      </div>

      Đổi mật khẩu
      <div className="mb-8">
        <h3 className="font-semibold text-lg mb-3 text-gray-800">Đổi mật khẩu</h3>
        <div className="flex gap-3">
          <input className="flex-1 border border-gray-300 rounded-lg px-4 py-2 focus:outline-none focus:ring text-base" type="password" value={password} onChange={e => setPassword(e.target.value)} placeholder="Nhập mật khẩu mới" />
          <Button variant="default" onClick={handleUpdatePassword} disabled={loading}>Cập nhật</Button>
        </div>
      </div>

      {/* Các thao tác quản trị */}
      <div className="flex flex-col gap-3">
        {user.status === 'active' ? (
          <Button
            variant="destructive"
            onClick={handleDisable}
            disabled={loading}
            className={`font-semibold py-2 bg-red-600 hover:bg-red-700 text-white border-none ${loading ? 'opacity-60 cursor-not-allowed' : ''}`}
          >
            Vô hiệu hóa tài khoản
          </Button>
        ) : (
          <Button
            variant="outline"
            onClick={handleRestore}
            disabled={loading}
            className={`font-semibold py-2 ${loading ? 'bg-gray-200 text-gray-500 border border-gray-300 cursor-not-allowed' : ''}`}
          >
            Khôi phục tài khoản
          </Button>
        )}
      </div>
    </div>
  );
}
