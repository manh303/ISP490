
import React, { useState } from "react";
import { userApi } from "../../services/userApi";
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
    <form className="max-w-md mx-auto p-4 border rounded" onSubmit={handleSubmit}>
      <h2 className="text-xl font-semibold mb-4">Tạo người dùng mới</h2>
      {error && <p className="text-red-500 mb-2">{error}</p>}
      {success && <p className="text-green-500 mb-2">{success}</p>}
      <div className="mb-2">
        <label>Họ tên</label>
        <input className="w-full border p-2" value={fullName} onChange={e => setFullName(e.target.value)} required />
      </div>
      <div className="mb-2">
        <label>Email</label>
        <input className="w-full border p-2" type="email" value={email} onChange={e => setEmail(e.target.value)} required />
      </div>
      <div className="mb-2">
        <label>Số điện thoại</label>
        <input className="w-full border p-2" value={phone} onChange={e => setPhone(e.target.value)} required />
      </div>
      <div className="mb-2">
        <label>Vai trò</label>
        <select className="w-full border p-2" value={roleCode} onChange={e => setRoleCode(e.target.value)} required>
          {ROLE_OPTIONS.map(opt => (
            <option key={opt.code} value={opt.code}>{opt.name}</option>
          ))}
        </select>
      </div>
      <div className="mb-2">
        <label>Mật khẩu</label>
        <input className="w-full border p-2" type="password" value={password} onChange={e => setPassword(e.target.value)} required />
      </div>
      <button className="px-4 py-2 bg-green-500 text-white rounded" type="submit" disabled={loading}>
        {loading ? "Đang tạo..." : "Tạo người dùng"}
      </button>
    </form>
  );
}
