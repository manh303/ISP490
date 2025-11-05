
import React, { useEffect, useState } from "react";
import { userApi } from "../../services/userApi";
interface User {
    user_id: number;
    email: string;
    full_name: string;
    phone: string;
    status: string;
    role_code: string;
    role_name: string;
    last_login_at: string;
    created_at: string;
    updated_at: string;
}

interface ActiveUsersListProps {
    onSelectUser: (id: number) => void;
}

export default function ActiveUsersList({ onSelectUser }: ActiveUsersListProps) {
    const [users, setUsers] = useState<User[]>([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [page, setPage] = useState(1);
    const [limit] = useState(20);
    const [total, setTotal] = useState(0);

    useEffect(() => {
        setLoading(true);
        setError(null);
        userApi.getActiveUsers(page, limit)
            .then((data) => {
                // Nếu API trả về { success, data, total }
                if (data.success) {
                    setUsers(data.data);
                    setTotal(data.total);
                } else {
                    throw new Error("API trả về lỗi");
                }
            })
            .catch((err) => {
                // console.error(" Lỗi khi tải danh sách người dùng:", err);
                setError(err?.response?.data?.detail || "Bạn không có quyền truy cập chức năng này");
            })
            .finally(() => setLoading(false));
    }, [page, limit]);

    const totalPages = Math.ceil(total / limit);

    return (
        <div>
            <h2 className="text-xl font-semibold mb-4">Người dùng đang hoạt động</h2>
            {loading && <p>Đang tải...</p>}
            {error && <p className="text-red-500">{error}</p>}
            <table className="min-w-full border">
                <thead>
                    <tr className="bg-gray-100">
                        <th className="p-2 border">Tên</th>
                        <th className="p-2 border">Email</th>
                        <th className="p-2 border">Số điện thoại</th>
                        <th className="p-2 border">Vai trò</th>
                        <th className="p-2 border">Trạng thái</th>
                        <th className="p-2 border">Lần đăng nhập cuối</th>
                        <th className="p-2 border">Hành động</th>
                    </tr>
                </thead>
                <tbody>
                    {users.map((user) => (
                        <tr key={user.user_id} className="border-b">
                            <td className="p-2 border">{user.full_name}</td>
                            <td className="p-2 border">{user.email}</td>
                            <td className="p-2 border">{user.phone}</td>
                            <td className="p-2 border">{user.role_name}</td>
                            <td className="p-2 border">{user.status}</td>
                            <td className="p-2 border">{user.last_login_at ? new Date(user.last_login_at).toLocaleString() : "-"}</td>
                            <td className="p-2 border">
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
