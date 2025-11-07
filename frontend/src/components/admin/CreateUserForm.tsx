
import React, { useState } from "react";
import { userApi } from "../../services/userApi";
import { Button } from '../../components/ui/figma/button';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '../../components/ui/figma/select';

interface CreateUserFormProps {
  onCreated: () => void;
}

const ROLE_OPTIONS = [
  { code: "ADMIN", name: "Admin" },
  { code: "ANALYST", name: "Analyst" },
  { code: "CUSTOMER", name: "Customer" },
  { code: "MANAGER", name: "Manager" },
];

export default function CreateUserForm({ onCreated }: CreateUserFormProps) {
  const [fullName, setFullName] = useState("");
  const [email, setEmail] = useState("");
  const [roleCode, setRoleCode] = useState(ROLE_OPTIONS[0].code);
  const [password, setPassword] = useState("");
  const [phone, setPhone] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);

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
      setFullName(""); setEmail(""); setRoleCode(ROLE_OPTIONS[0].code); setPassword(""); setPhone("");
      onCreated();
    } catch (err: any) {
      setError(err.message);
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
        <label className="block text-gray-700 mb-1">Vai trò</label>
        <Select value={roleCode} onValueChange={setRoleCode}>
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
      <div className="mb-6">
        <label className="block text-gray-700 mb-1">Mật khẩu</label>
        <input className="w-full border border-gray-300 rounded px-3 py-2 focus:outline-none focus:ring" type="password" value={password} onChange={e => setPassword(e.target.value)} required />
      </div>
      <Button className="w-full" type="submit" disabled={loading}>
        {loading ? "Đang tạo..." : "Tạo người dùng"}
      </Button>
    </form>
  );
}
