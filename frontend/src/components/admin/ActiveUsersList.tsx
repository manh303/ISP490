
import React, { useEffect, useState } from "react";
import { userApi } from "../../services/userApi";
import { Button } from '../../components/ui/figma/button';
import { Badge } from '../../components/ui/figma/badge';
import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from '../../components/ui/figma/table';
import { Edit, Eye, Trash2 } from 'lucide-react';

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
    onSelectUser: (id: number, editMode?: boolean) => void;
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
                if (data.success) {
                    setUsers(data.data);
                    setTotal(data.total);
                } else {
                    throw new Error("API trả về lỗi");
                }
            })
            .catch((err) => {
                setError(err?.response?.data?.detail || "Bạn không có quyền truy cập chức năng này");
            })
            .finally(() => setLoading(false));
    }, [page, limit]);

    const totalPages = Math.ceil(total / limit);

    return (
        <div className="bg-white rounded-lg shadow border border-gray-200 p-4">
            <div className="flex items-center justify-between mb-4">
                <h2 className="text-xl font-semibold">Người dùng đang hoạt động</h2>
                <div className="text-gray-500 text-sm">Tổng: {total}</div>
            </div>
            {loading && <div className="text-gray-500">Đang tải...</div>}
            {error && <div className="text-red-500 mb-2">{error}</div>}
            <div className="overflow-x-auto">
                <Table>
                    <TableHeader>
                        <TableRow>
                            <TableHead>STT</TableHead>
                            <TableHead>Tên</TableHead>
                            <TableHead>Email</TableHead>
                            <TableHead>Số điện thoại</TableHead>
                            <TableHead>Vai trò</TableHead>
                            <TableHead>Trạng thái</TableHead>
                            <TableHead>Lần đăng nhập cuối</TableHead>
                            <TableHead>Hành động</TableHead>
                        </TableRow>
                    </TableHeader>
                    <TableBody>
                        {users.map((user, idx) => (
                            <TableRow key={user.user_id}>
                                <TableCell>{(page - 1) * limit + idx + 1}</TableCell>
                                <TableCell>{user.full_name}</TableCell>
                                <TableCell>{user.email}</TableCell>
                                <TableCell>{user.phone}</TableCell>
                                <TableCell>
                                    <Badge variant={user.role_name === 'Admin' ? 'default' : 'secondary'}>
                                        {user.role_name}
                                    </Badge>
                                </TableCell>
                                <TableCell>
                                    {user.status === 'active' ? (
                                        <Badge variant="default" className="bg-green-500 text-white">Hoạt động</Badge>
                                    ) : (
                                        <Badge variant="destructive" className="bg-gray-500 text-white">Vô hiệu hóa</Badge>
                                    )}
                                </TableCell>
                                <TableCell>{user.last_login_at ? new Date(user.last_login_at).toLocaleString() : '-'}</TableCell>
                                <TableCell>
                                    <div className="flex gap-2">
                                        <Button size="sm" variant="outline" onClick={() => onSelectUser(user.user_id, false)} title="Xem chi tiết">
                                            <Eye className="h-4 w-4" />
                                        </Button>

                                        <Button size="sm" variant="outline" onClick={() => onSelectUser(user.user_id, true)} title="Sửa">
                                            <Edit className="h-4 w-4" />
                                        </Button>
                                        <Button size="sm" variant="destructive" title="Xóa">
                                            <Trash2 className="h-4 w-4" />
                                        </Button>
                                    </div>
                                </TableCell>
                            </TableRow>
                        ))}
                    </TableBody>
                </Table>
            </div>
            {/* Pagination Controls */}
            <div className="flex justify-between items-center mt-4">
                <div className="text-gray-600 text-sm">
                    Trang {page} / {totalPages || 1}
                </div>
                <div className="flex gap-2">
                    <Button size="sm" variant="outline" disabled={page === 1} onClick={() => setPage(page - 1)}>
                        Trang trước
                    </Button>
                    <Button size="sm" variant="outline" disabled={page === totalPages || totalPages === 0} onClick={() => setPage(page + 1)}>
                        Trang sau
                    </Button>
                </div>
            </div>
        </div>
    );
}
