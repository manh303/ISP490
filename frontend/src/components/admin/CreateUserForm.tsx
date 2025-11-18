
import React, { useState, useEffect } from "react";
import { userApi } from "../../services/userApi";
import { getAllRoles } from "../../services/roleApi";
import { Button } from '../../components/ui/figma/button';
import { useToast } from "../../contexts/ToastContext";
interface CreateUserFormProps {
  onCreated: () => void;
}

export default function CreateUserForm({ onCreated }: CreateUserFormProps) {
  const [fullName, setFullName] = useState("");
  const [email, setEmail] = useState("");
  const [roleCode, setRoleCode] = useState("");
  const [password, setPassword] = useState("");
  const [phone, setPhone] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [showRoleDropdown, setShowRoleDropdown] = useState(false);
  const [availableRoles, setAvailableRoles] = useState<Array<{ role_code: string; role_name: string }>>([]);
  const { showToast } = useToast();

  useEffect(() => {
    // Fetch roles
    getAllRoles({ page: 1, limit: 100 })
      .then((rolesData) => {
        if (rolesData.success) {
          setAvailableRoles(rolesData.data.map(role => ({
            role_code: role.role_code,
            role_name: role.role_name
          })));
          if (rolesData.data.length > 0) {
            setRoleCode(rolesData.data[0].role_code);
          }
        }
      })
      .catch((err) => {
        console.error("Error fetching roles:", err);
      });
  }, []);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setError(null);
    setSuccess(null);
    try {
      const data = await userApi.createUser({
        email,
        password,
        full_name: fullName,
        phone,
        role_code: roleCode,
      });
      if (!data.success) throw new Error(data.message || "Không thể tạo người dùng mới");
      setSuccess("Tạo người dùng thành công!");
      showToast("Tạo người dùng thành công!", 'success');
      setFullName(""); setEmail(""); setRoleCode(availableRoles.length > 0 ? availableRoles[0].role_code : ""); setPassword(""); setPhone("");
      onCreated();
    } catch (err: any) {
      const detail = err?.response?.data?.detail;
      let errorMsg = "Không thể tạo người dùng mới";
      if (typeof detail === 'string') {
        errorMsg = detail;
      } else if (Array.isArray(detail) && detail.length > 0 && detail[0]?.msg) {
        errorMsg = detail[0].msg;
      } else if (detail?.msg) {
        errorMsg = detail.msg;
      } else if (err.message) {
        errorMsg = err.message;
      }
      setError(errorMsg);
      showToast(errorMsg, 'error');
    } finally {
      setLoading(false);
    }
  };

  return (
    <form className="bg-white rounded-lg shadow border border-gray-200 max-w-md mx-auto p-6" onSubmit={handleSubmit}>
      <h2 className="text-xl font-semibold mb-4">Tạo người dùng mới</h2>
      {error && <div className="text-red-500 mb-2">{error}</div>}
      {success && <div className="text-green-500 mb-2">{success}</div>}
      <div className="mb-4">
        <label className="block text-gray-700 mb-1">Họ tên</label>
        <input className="w-full border border-gray-300 rounded px-3 py-2 focus:outline-none focus:ring" value={fullName} onChange={e => setFullName(e.target.value)} required />
      </div>
      <div className="mb-4">
        <label className="block text-gray-700 mb-1">Email</label>
        <input className="w-full border border-gray-300 rounded px-3 py-2 focus:outline-none focus:ring" type="email" value={email} onChange={e => setEmail(e.target.value)} required />
      </div>
      <div className="mb-4">
        <label className="block text-gray-700 mb-1">Số điện thoại</label>
        <input className="w-full border border-gray-300 rounded px-3 py-2 focus:outline-none focus:ring" value={phone} onChange={e => setPhone(e.target.value)} required />
      </div>
      <div className="mb-4">
        <label className="block text-base font-medium text-gray-700 mb-2">Vai trò</label>
        <div className="relative w-full">
          <button
            type="button"
            className="w-full border border-gray-300 bg-white rounded-lg px-4 py-2 text-left flex justify-between items-center focus:outline-none focus:ring-2 focus:ring-blue-500"
            onClick={() => setShowRoleDropdown(v => !v)}
          >
            <span className="text-gray-900">
              {availableRoles.find(opt => opt.role_code === roleCode)?.role_name || "Chọn vai trò"}
            </span>
            <span className={`ml-2 text-gray-400 transition-transform ${showRoleDropdown ? 'rotate-180' : ''}`}>▼</span>
          </button>
          {showRoleDropdown && (
            <ul className="absolute z-10 w-full bg-white border border-gray-200 rounded-lg mt-1 shadow-lg max-h-60 overflow-auto">
              {availableRoles.map(opt => (
                <li
                  key={opt.role_code}
                  className={`px-4 py-3 text-base cursor-pointer hover:bg-blue-50 transition-colors ${
                    roleCode === opt.role_code ? 'bg-blue-100 font-semibold text-blue-700' : 'text-gray-700'
                  }`}
                  onClick={() => { 
                    setRoleCode(opt.role_code); 
                    setShowRoleDropdown(false); 
                  }}
                >
                  {opt.role_name}
                </li>
              ))}
            </ul>
          )}
        </div>
      </div>
      <div className="mb-6">
        <label className="block text-gray-700 mb-1">Mật khẩu</label>
        <input className="w-full border border-gray-300 rounded px-3 py-2 focus:outline-none focus:ring" type="password" value={password} onChange={e => setPassword(e.target.value)} required />
      </div>
      <Button className="w-full bg-green-600 hover:bg-green-700 text-white px-6 py-3 rounded-lg font-semibold transition-colors" type="submit" disabled={loading}>
        {loading ? "Đang tạo..." : "Tạo người dùng"}
      </Button>
    </form>
  );
}
