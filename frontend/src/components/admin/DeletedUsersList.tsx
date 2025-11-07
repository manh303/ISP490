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
import { Eye, RotateCcw, Trash2 } from 'lucide-react';

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
    <div className="bg-white rounded-lg shadow border border-gray-200 p-4">
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-xl font-semibold">Người dùng đã xóa</h2>
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
              <TableHead>Vai trò</TableHead>
              <TableHead>Trạng thái</TableHead>
              <TableHead>Hành động</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {users.map((user, idx) => (
              <TableRow key={user.user_id}>
                <TableCell>{(page - 1) * limit + idx + 1}</TableCell>
                <TableCell>{user.full_name}</TableCell>
                <TableCell>{user.email}</TableCell>
                <TableCell>
                  <Badge variant={user.role_name === 'Admin' ? 'default' : 'secondary'}>
                    {user.role_name || "Chưa xác định"}
                  </Badge>
                </TableCell>
                <TableCell>
                  <Badge variant={user.status === 'Active' ? 'default' : 'destructive'}>
                    {user.status}
                  </Badge>
                </TableCell>
                <TableCell>
                  <div className="flex gap-2">
                    <Button size="sm" variant="outline" onClick={() => handleRestore(user.user_id)} title="Khôi phục">
                      <RotateCcw className="h-4 w-4" />
                    </Button>
                    <Button size="sm" variant="destructive" onClick={() => handlePermanentDelete(user.user_id)} title="Xóa vĩnh viễn">
                      <Trash2 className="h-4 w-4" />
                    </Button>
                    <Button size="sm" variant="outline" onClick={() => onSelectUser(user.user_id)} title="Xem chi tiết">
                      <Eye className="h-4 w-4" />
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
