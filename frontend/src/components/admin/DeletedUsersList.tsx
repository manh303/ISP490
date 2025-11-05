import React, { useEffect, useState } from "react";
import { userApi } from "../../services/userApi";
interface User {
  user_id: number;
  full_name: string;
  email: string;
  role_name: string;
  status: string;
}

interface DeletedUsersListProps {
  onSelectUser: (id: number) => void;
}

export default function DeletedUsersList({ onSelectUser }: DeletedUsersListProps) {
  const [users, setUsers] = useState<User[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [page, setPage] = useState(1);
  const [limit] = useState(20);
  const [total, setTotal] = useState(0);

  useEffect(() => {
    setLoading(true);
    setError(null);
    userApi.getDeletedUsers(page, limit)
      .then((data) => {
        if (data.success) {
          setUsers(data.data);
          setTotal(data.total);
        } else if (Array.isArray(data)) {
          setUsers(data);
        } else if (data.detail) {
          setError(data.detail);
        } else {
          throw new Error("API trả về lỗi");
        }
      })
      .catch((err) => setError(err?.response?.data?.detail || "Lỗi không xác định"))
      .finally(() => setLoading(false));
  }, [page, limit]);

  const handleRestore = async (id: number) => {
    try {
      const res = await userApi.restoreUser(id);
      if (res && res.detail) {
        setError(res.detail);
      } else {
        setUsers(users.filter(u => u.user_id !== id));
      }
    } catch (err: any) {
      setError(err?.response?.data?.detail || "Khôi phục thất bại");
    }
  };

  const handlePermanentDelete = async (id: number) => {
    const confirmed = window.confirm("Bạn có chắc chắn muốn xóa vĩnh viễn người dùng này? Hành động này không thể hoàn tác!");
    if (!confirmed) return;
    try {
      const res = await userApi.permanentDeleteUser(id);
      if (res && res.detail) {
        setError(res.detail);
      } else {
        setUsers(users.filter(u => u.user_id !== id));
      }
    } catch (err: any) {
      setError(err?.response?.data?.detail || "Xóa vĩnh viễn thất bại");
    }
  };

  const totalPages = Math.ceil(total / limit);

  return (
    <div>
      <h2 className="text-xl font-semibold mb-4">Người dùng đã xóa</h2>
      {loading && <p>Đang tải...</p>}
      {error && <p className="text-red-500">{error}</p>}
      <table className="min-w-full border">
        <thead>
          <tr className="bg-gray-100">
            <th className="p-2 border">Tên</th>
            <th className="p-2 border">Email</th>
            <th className="p-2 border">Vai trò</th>
            <th className="p-2 border">Trạng thái</th>
            <th className="p-2 border">Hành động</th>
          </tr>
        </thead>
        <tbody>
          {users.map((user) => (
            <tr key={user.user_id} className="border-b">
              <td className="p-2 border">{user.full_name}</td>
              <td className="p-2 border">{user.email}</td>
              <td className="p-2 border">{user.role_name || "Chưa xác định"}</td>
              <td className="p-2 border">{user.status}</td>
              <td className="p-2 border flex gap-2">
                <button
                  className="px-3 py-1 bg-green-500 text-white rounded hover:bg-green-600"
                  onClick={() => handleRestore(user.user_id)}
                >
                  Khôi phục
                </button>
                <button
                  className="px-3 py-1 bg-red-500 text-white rounded hover:bg-red-600"
                  onClick={() => handlePermanentDelete(user.user_id)}
                >
                  Xóa vĩnh viễn
                </button>
                <button
                  className="px-3 py-1 bg-blue-500 text-white rounded hover:bg-blue-600"
                  onClick={() => onSelectUser(user.user_id)}
                >
                  Xem chi tiết
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {/* Pagination Controls */}
      <div className="flex justify-center items-center gap-2 mt-4">
        <button
          className="px-2 py-1 border rounded"
          disabled={page === 1}
          onClick={() => setPage(page - 1)}
        >
          Trang trước
        </button>
        <span>
          Trang {page} / {totalPages || 1}
        </span>
        <button
          className="px-2 py-1 border rounded"
          disabled={page === totalPages || totalPages === 0}
          onClick={() => setPage(page + 1)}
        >
          Trang sau
        </button>
      </div>
    </div>
  );
}
