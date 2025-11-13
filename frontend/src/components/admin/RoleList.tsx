import React, { useEffect, useState } from "react";
import { getAllRoles, deleteRole, deactivateRole, activateRole } from "../../services/roleApi";
import { Button } from '../ui/figma/button';
import { Badge } from '../ui/figma/badge';
import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from '../ui/figma/table';
import { Edit, Eye, Trash2, Ban, CheckCircle, Users } from 'lucide-react';
import { useToast } from "../../contexts/ToastContext";

interface Role {
    role_id: number;
    role_code: string;
    role_name: string;
    description: string;
    is_active: boolean;
}

interface RoleListProps {
    onSelectRole: (id: number, mode: 'view' | 'edit') => void;
    onViewUsers: (id: number) => void;
    refreshTrigger?: number;
}

export default function RoleList({ onSelectRole, onViewUsers, refreshTrigger }: RoleListProps) {
    const [roles, setRoles] = useState<Role[]>([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [page, setPage] = useState(1);
    const [limit] = useState(20);
    const [total, setTotal] = useState(0);
    const [activeOnly, setActiveOnly] = useState(false);
    const { showToast } = useToast();

    const fetchRoles = async () => {
        setLoading(true);
        setError(null);
        try {
            const data = await getAllRoles({ page, limit, active_only: activeOnly });
            if (data.success) {
                setRoles(data.data);
                setTotal(data.total);
            } else {
                throw new Error("API trả về lỗi");
            }
        } catch (err: any) {
            setError(err?.response?.data?.detail || "Không thể tải danh sách vai trò");
            showToast(err?.response?.data?.detail || "Không thể tải danh sách vai trò", "error");
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchRoles();
    }, [page, limit, activeOnly, refreshTrigger]);

    const handleDelete = async (roleId: number, roleName: string) => {
        if (!confirm(`Bạn có chắc chắn muốn xóa vai trò "${roleName}"?\nLưu ý: Chỉ xóa được nếu không có người dùng nào được gán vai trò này.`)) {
            return;
        }

        try {
            const response = await deleteRole(roleId);
            showToast(response.message || "Xóa vai trò thành công!", "success");
            fetchRoles();
        } catch (err: any) {
            showToast(err?.response?.data?.detail || "Không thể xóa vai trò", "error");
        }
    };

    const handleToggleStatus = async (roleId: number, roleName: string, isActive: boolean) => {
        const action = isActive ? "vô hiệu hóa" : "kích hoạt";
        if (!confirm(`Bạn có chắc chắn muốn ${action} vai trò "${roleName}"?`)) {
            return;
        }

        try {
            const response = isActive 
                ? await deactivateRole(roleId)
                : await activateRole(roleId);
            showToast(response.message || `${action.charAt(0).toUpperCase() + action.slice(1)} vai trò thành công!`, "success");
            fetchRoles();
        } catch (err: any) {
            showToast(err?.response?.data?.detail || `Không thể ${action} vai trò`, "error");
        }
    };

    const totalPages = Math.ceil(total / limit);

    return (
        <div className="bg-white rounded-lg shadow border border-gray-200 p-4">
            <div className="flex items-center justify-between mb-4">
                <h2 className="text-xl font-semibold">Danh sách vai trò</h2>
                <div className="flex items-center gap-4">
                    <label className="flex items-center gap-2 text-sm">
                        <input
                            type="checkbox"
                            checked={activeOnly}
                            onChange={(e) => {
                                setActiveOnly(e.target.checked);
                                setPage(1);
                            }}
                            className="rounded"
                        />
                        Chỉ hiển thị vai trò đang hoạt động
                    </label>
                    <div className="text-gray-500 text-sm">Tổng: {total}</div>
                </div>
            </div>

            {loading && <div className="text-gray-500">Đang tải...</div>}
            {error && <div className="text-red-500 mb-2">{error}</div>}

            <div className="overflow-x-auto">
                <Table>
                    <TableHeader>
                        <TableRow>
                            <TableHead>STT</TableHead>
                            <TableHead>Mã vai trò</TableHead>
                            <TableHead>Tên vai trò</TableHead>
                            <TableHead>Mô tả</TableHead>
                            <TableHead>Trạng thái</TableHead>
                            <TableHead>Hành động</TableHead>
                        </TableRow>
                    </TableHeader>
                    <TableBody>
                        {roles.map((role, idx) => (
                            <TableRow key={role.role_id}>
                                <TableCell>{(page - 1) * limit + idx + 1}</TableCell>
                                <TableCell>
                                    <Badge variant="outline" className="font-mono">
                                        {role.role_code}
                                    </Badge>
                                </TableCell>
                                <TableCell className="font-medium">{role.role_name}</TableCell>
                                <TableCell className="max-w-md truncate" title={role.description}>
                                    {role.description}
                                </TableCell>
                                <TableCell>
                                    {role.is_active ? (
                                        <Badge variant="default" className="bg-green-500 text-white">
                                            Hoạt động
                                        </Badge>
                                    ) : (
                                        <Badge variant="destructive" className="bg-gray-500 text-white">
                                            Vô hiệu hóa
                                        </Badge>
                                    )}
                                </TableCell>
                                <TableCell>
                                    <div className="flex gap-2">
                                        <Button 
                                            size="sm" 
                                            variant="outline" 
                                            onClick={() => onSelectRole(role.role_id, 'view')} 
                                            title="Xem chi tiết"
                                        >
                                            <Eye className="h-4 w-4" />
                                        </Button>

                                        <Button 
                                            size="sm" 
                                            variant="outline" 
                                            onClick={() => onSelectRole(role.role_id, 'edit')} 
                                            title="Sửa"
                                        >
                                            <Edit className="h-4 w-4" />
                                        </Button>

                                        <Button 
                                            size="sm" 
                                            variant="outline"
                                            onClick={() => onViewUsers(role.role_id)} 
                                            title="Xem người dùng"
                                        >
                                            <Users className="h-4 w-4" />
                                        </Button>

                                        <Button 
                                            size="sm" 
                                            variant={role.is_active ? "outline" : "default"}
                                            onClick={() => handleToggleStatus(role.role_id, role.role_name, role.is_active)}
                                            title={role.is_active ? "Vô hiệu hóa" : "Kích hoạt"}
                                        >
                                            {role.is_active ? (
                                                <Ban className="h-4 w-4" />
                                            ) : (
                                                <CheckCircle className="h-4 w-4" />
                                            )}
                                        </Button>

                                        <Button 
                                            size="sm" 
                                            variant="destructive" 
                                            onClick={() => handleDelete(role.role_id, role.role_name)}
                                            title="Xóa"
                                        >
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
                    <Button 
                        size="sm" 
                        variant="outline" 
                        disabled={page === 1} 
                        onClick={() => setPage(page - 1)}
                    >
                        Trang trước
                    </Button>
                    <Button 
                        size="sm" 
                        variant="outline" 
                        disabled={page === totalPages || totalPages === 0} 
                        onClick={() => setPage(page + 1)}
                    >
                        Trang sau
                    </Button>
                </div>
            </div>
        </div>
    );
}
