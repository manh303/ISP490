import React, { useEffect, useState } from "react";
import { userApi } from "../../services/userApiTest";
import { Button } from '../../components/ui/figma/button';
import { Badge } from '../../components/ui/figma/badge';
import { useToast } from "../../contexts/ToastContext";

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
  const { showToast } = useToast();
  const [user, setUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [editModeState, setEditModeState] = useState<boolean>(!!editMode);
  const [form, setForm] = useState({ full_name: "", phone: "", role_code: "CUSTOMER" });
  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [passwordError, setPasswordError] = useState<string>("");
  const [showRoleDropdown, setShowRoleDropdown] = useState(false);
  const [showPasswordForm, setShowPasswordForm] = useState(false);

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
      showToast('✓ Cập nhật thông tin người dùng thành công!', 'success');
    } catch (err: any) {
      const errorMsg = err?.response?.data?.detail || "Cập nhật thất bại";
      setError(errorMsg);
      showToast(errorMsg, 'error');
    } finally {
      setLoading(false);
    }
  };

  const handleUpdatePassword = async () => {
    // Reset error
    setPasswordError("");
    
    // Validation
    if (!password || !confirmPassword) {
      setPasswordError("Vui lòng nhập đầy đủ mật khẩu");
      return;
    }
    
    if (password.length < 6) {
      setPasswordError("Mật khẩu phải có ít nhất 6 ký tự");
      return;
    }
    
    if (password !== confirmPassword) {
      setPasswordError("Mật khẩu xác nhận không khớp");
      return;
    }
    
    setLoading(true);
    try {
      await userApi.updateUserPassword(userId, password);
      setPassword("");
      setConfirmPassword("");
      setPasswordError("");
      setShowPasswordForm(false);
      showToast('✓ Cập nhật mật khẩu thành công!', 'success');
    } catch (err: any) {
      const errorMsg = err?.response?.data?.detail || "Cập nhật mật khẩu thất bại";
      setPasswordError(errorMsg);
      showToast(errorMsg, 'error');
    } finally {
      setLoading(false);
    }
  };

  const handleClosePasswordModal = () => {
    setShowPasswordForm(false);
    setPassword("");
    setConfirmPassword("");
    setPasswordError("");
  };

  const handleDisable = async () => {
    setLoading(true);
    try {
      await userApi.disableUser(userId);
      showToast('✓ Vô hiệu hóa tài khoản thành công!', 'success');
      onBack();
    } catch (err: any) {
      const errorMsg = err?.response?.data?.detail || "Vô hiệu hóa thất bại";
      setError(errorMsg);
      showToast(errorMsg, 'error');
    } finally {
      setLoading(false);
    }
  };

  const handleRestore = async () => {
    setLoading(true);
    try {
      await userApi.restoreUser(userId);
      showToast('✓ Khôi phục tài khoản thành công!', 'success');
      onBack();
    } catch (err: any) {
      const errorMsg = err?.response?.data?.detail || "Khôi phục thất bại";
      setError(errorMsg);
      showToast(errorMsg, 'error');
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

  // Show password change form
  if (showPasswordForm) {
    return (
      <div className="bg-white rounded-xl shadow-lg border border-gray-200 max-w-lg mx-auto p-8">
        <Button variant="outline" className="mb-6" onClick={handleClosePasswordModal}>
          ← Quay lại
        </Button>
        <h2 className="text-2xl font-bold mb-6 text-gray-800">Đổi mật khẩu</h2>
        
        <div className="space-y-4">
          {/* User Info Display */}
          <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 mb-6">
            <div className="flex items-center gap-3">
              <svg className="w-8 h-8 text-blue-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z" />
              </svg>
              <div>
                <p className="font-semibold text-gray-900">{user.full_name}</p>
                <p className="text-sm text-gray-600">{user.email}</p>
              </div>
            </div>
          </div>

          {/* Password input */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Mật khẩu mới <span className="text-red-500">*</span>
            </label>
            <input 
              className={`w-full border ${passwordError ? 'border-red-400 focus:ring-red-300' : 'border-gray-300 focus:ring-blue-300'} rounded-lg px-4 py-2.5 focus:outline-none focus:ring-2 text-base transition-all`}
              type="password" 
              value={password} 
              onChange={e => {
                setPassword(e.target.value);
                setPasswordError("");
              }}
              placeholder="Nhập mật khẩu mới (tối thiểu 6 ký tự)" 
            />
          </div>

          {/* Confirm Password input */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Xác nhận mật khẩu <span className="text-red-500">*</span>
            </label>
            <input 
              className={`w-full border ${passwordError ? 'border-red-400 focus:ring-red-300' : 'border-gray-300 focus:ring-blue-300'} rounded-lg px-4 py-2.5 focus:outline-none focus:ring-2 text-base transition-all`}
              type="password" 
              value={confirmPassword} 
              onChange={e => {
                setConfirmPassword(e.target.value);
                setPasswordError("");
              }}
              placeholder="Nhập lại mật khẩu mới" 
            />
          </div>

          {/* Error message */}
          {passwordError && (
            <div className="flex items-center gap-2 p-3 bg-red-50 border border-red-200 rounded-lg">
              <span className="text-red-500">⚠️</span>
              <span className="text-sm text-red-600 font-medium">{passwordError}</span>
            </div>
          )}

          {/* Success indicator when passwords match */}
          {password && confirmPassword && password === confirmPassword && password.length >= 6 && (
            <div className="flex items-center gap-2 p-3 bg-green-50 border border-green-200 rounded-lg">
              <span className="text-green-500">✓</span>
              <span className="text-sm text-green-600 font-medium">Mật khẩu khớp nhau</span>
            </div>
          )}

          {/* Action buttons */}
          <div className="flex gap-3 mt-6">
            <Button 
              variant="outline" 
              onClick={handleClosePasswordModal}
              className="flex-1"
              disabled={loading}
            >
              Hủy
            </Button>
            <Button 
              variant="default" 
              onClick={handleUpdatePassword} 
              disabled={loading || !password || !confirmPassword}
              className="flex-1 bg-blue-600 hover:bg-blue-700 text-white font-semibold disabled:bg-gray-300 disabled:cursor-not-allowed"
            >
              {loading ? 'Đang cập nhật...' : 'Cập nhật mật khẩu'}
            </Button>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="bg-white rounded-xl shadow-lg border border-gray-200 max-w-lg mx-auto p-8">
      <Button variant="outline" className="mb-6" onClick={onBack}>
        ← Quay lại danh sách
      </Button>
      <h2 className="text-2xl font-bold mb-6 text-gray-800">
        {editModeState ? 'Chỉnh sửa người dùng' : 'Chi tiết người dùng'}
      </h2>

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
              <div className="relative w-full">
                <button
                  type="button"
                  className="w-full border border-gray-300 bg-white rounded-lg px-4 py-2 text-left flex justify-between items-center focus:outline-none focus:ring-2 focus:ring-blue-500"
                  onClick={() => setShowRoleDropdown(v => !v)}
                >
                  <span className="text-gray-900">
                    {ROLE_OPTIONS.find(opt => opt.code === form.role_code)?.name || "Chọn vai trò"}
                  </span>
                  <span className={`ml-2 text-gray-400 transition-transform ${showRoleDropdown ? 'rotate-180' : ''}`}>▼</span>
                </button>
                {showRoleDropdown && (
                  <ul className="absolute z-10 w-full bg-white border border-gray-200 rounded-lg mt-1 shadow-lg max-h-60 overflow-auto">
                    {ROLE_OPTIONS.map(opt => (
                      <li
                        key={opt.code}
                        className={`px-4 py-3 text-base cursor-pointer hover:bg-blue-50 transition-colors ${
                          form.role_code === opt.code ? 'bg-blue-100 font-semibold text-blue-700' : 'text-gray-700'
                        }`}
                        onClick={() => { 
                          setForm(f => ({ ...f, role_code: opt.code })); 
                          setShowRoleDropdown(false); 
                        }}
                      >
                        {opt.name}
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            </div>
            <div className="flex gap-3 mt-2">
              <Button className="flex-1 bg-green-600 hover:bg-green-700 text-white px-6 py-3 rounded-lg font-semibold transition-colors" onClick={handleUpdate} disabled={loading} variant="default">Lưu thay đổi</Button>
              <Button className="flex-1" variant="outline" onClick={() => setEditModeState(false)}>Hủy</Button>
            </div>
          </div>
        ) : (
          <div className="space-y-4">
            <div className="bg-gray-50 rounded-lg p-4 space-y-3">
              <div className="flex items-center gap-2 text-base">
                <span className="font-semibold text-gray-700 min-w-[120px]">Họ tên:</span> 
                <span className="text-gray-900">{user.full_name}</span>
              </div>
              <div className="flex items-center gap-2 text-base">
                <span className="font-semibold text-gray-700 min-w-[120px]">Email:</span> 
                <span className="text-gray-900">{user.email}</span>
              </div>
              <div className="flex items-center gap-2 text-base">
                <span className="font-semibold text-gray-700 min-w-[120px]">Số điện thoại:</span> 
                <span className="text-gray-900">{user.phone || 'Chưa cập nhật'}</span>
              </div>
              <div className="flex items-center gap-2 text-base">
                <span className="font-semibold text-gray-700 min-w-[120px]">Vai trò:</span>
                <Badge variant={user.role_name === 'Admin' ? 'default' : 'secondary'} className="text-sm">
                  {user.role_name}
                </Badge>
              </div>
              <div className="flex items-center gap-2 text-base">
                <span className="font-semibold text-gray-700 min-w-[120px]">Trạng thái:</span>
                {user.status === 'active' ? (
                  <Badge variant="default" className="bg-green-500 text-white hover:bg-green-600">
                    ✓ Hoạt động
                  </Badge>
                ) : (
                  <Badge variant="destructive" className="bg-gray-500 text-white">
                    ✗ Vô hiệu hóa
                  </Badge>
                )}
              </div>
            </div>
            
            {/* Always show action buttons in view mode */}
            <div className="flex gap-3 mt-4">
              <Button className="flex-1" variant="outline" onClick={() => setEditModeState(true)}>
                <span className="font-semibold">✏️ Chỉnh sửa thông tin</span>
              </Button>
              <Button className="flex-1 bg-blue-600 hover:bg-blue-700 text-white px-6 py-3 rounded-lg font-semibold transition-colors" variant="default" onClick={() => setShowPasswordForm(true)}>
                <span className="font-semibold">🔒 Đổi mật khẩu</span>
              </Button>
            </div>
          </div>
        )}
      </div>

      {/* Đổi mật khẩu - Only show in edit mode */}
      {editModeState && editMode !== false && (
        <Button 
          className="w-full mb-8 bg-blue-600 hover:bg-blue-700 text-white font-semibold px-6 py-3 rounded-lg transition-colors" 
          onClick={() => setShowPasswordForm(true)}
        >
           Đổi mật khẩu
        </Button>
      )}

      {/* Các thao tác quản trị */}
      <div className="flex flex-col gap-3">
        {user.status === 'active' ? (
          <Button
            variant="destructive"
            onClick={handleDisable}
            disabled={loading}
            className={`font-semibold py-2 bg-red-600 hover:bg-red-700 text-white border-none ${loading ? 'opacity-60 cursor-not-allowed' : ''}`}
          >
            🚫 Vô hiệu hóa tài khoản
          </Button>
        ) : (
          <Button
            variant="outline"
            onClick={handleRestore}
            disabled={loading}
            className={`font-semibold py-2 ${loading ? 'bg-gray-200 text-gray-500 border border-gray-300 cursor-not-allowed' : ''}`}
          >
            ✓ Khôi phục tài khoản
          </Button>
        )}
      </div>
    </div>
  );
}
