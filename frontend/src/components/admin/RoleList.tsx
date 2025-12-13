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
                throw new Error("API returned an error");
            }
        } catch (err: any) {
            const detail = err?.response?.data?.detail;
            let errorMsg = "Failed to fetch roles";
            if (typeof detail === 'string') {
                errorMsg = detail;
            } else if (Array.isArray(detail) && detail.length > 0 && detail[0]?.msg) {
                errorMsg = detail[0].msg;
            } else if (detail?.msg) {
                errorMsg = detail.msg;
            }
            setError(errorMsg);
            showToast(errorMsg, "error");
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchRoles();
    }, [page, limit, activeOnly, refreshTrigger]);

    const handleDelete = async (roleId: number, roleName: string) => {
        if (!confirm(`Are you sure you want to delete role "${roleName}"?\nNote: Role can only be deleted if no users are assigned to it.`)) {
            return;
        }

        try {
            const response = await deleteRole(roleId);
            showToast(response.message || "Role deleted successfully!", "success");
            fetchRoles();
        } catch (err: any) {
            showToast(err?.response?.data?.detail || "Failed to delete role", "error");
        }
    };

    const handleToggleStatus = async (roleId: number, roleName: string, isActive: boolean) => {
        const action = isActive ? "deactivate" : "activate";
        const actionText = isActive ? "deactivate" : "activate";
        if (!confirm(`Are you sure you want to ${actionText} role "${roleName}"?`)) {
            return;
        }

        try {
            const response = isActive
                ? await deactivateRole(roleId)
                : await activateRole(roleId);
            showToast(response.message || `Role ${action}d successfully!`, "success");
            fetchRoles();
        } catch (err: any) {
            showToast(err?.response?.data?.detail || `Failed to ${action} role`, "error");
        }
    };

    const totalPages = Math.ceil(total / limit);

    return (
        <div className="bg-white rounded-lg shadow border border-gray-200 p-4">
            <div className="flex items-center justify-between mb-4">
                <h2 className="text-xl font-semibold">Role List</h2>
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
                        Show active roles only
                    </label>
                    <div className="text-gray-500 text-sm">Total: {total}</div>
                </div>
            </div>

            {loading && <div className="text-gray-500">Loading...</div>}
            {error && <div className="text-red-500 mb-2">{error}</div>}

            <div className="overflow-x-auto">
                <Table>
                    <TableHeader>
                        <TableRow>
                            <TableHead>#</TableHead>
                            <TableHead>Role Code</TableHead>
                            <TableHead>Role Name</TableHead>
                            <TableHead>Description</TableHead>
                            <TableHead>Status</TableHead>
                            <TableHead>Actions</TableHead>
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
                                            Active
                                        </Badge>
                                    ) : (
                                        <Badge variant="destructive" className="bg-gray-500 text-white">
                                            Inactive
                                        </Badge>
                                    )}
                                </TableCell>
                                <TableCell>
                                    <div className="flex gap-2">
                                        <Button
                                            size="sm"
                                            variant="outline"
                                            onClick={() => onSelectRole(role.role_id, 'view')}
                                            title="View Details"
                                        >
                                            <Eye className="h-4 w-4" />
                                        </Button>

                                        <Button
                                            size="sm"
                                            variant="outline"
                                            onClick={() => onSelectRole(role.role_id, 'edit')}
                                            title="Edit"
                                        >
                                            <Edit className="h-4 w-4" />
                                        </Button>

                                        <Button
                                            size="sm"
                                            variant="outline"
                                            onClick={() => onViewUsers(role.role_id)}
                                            title="View Users"
                                        >
                                            <Users className="h-4 w-4" />
                                        </Button>

                                        <Button
                                            size="sm"
                                            variant={role.is_active ? "outline" : "default"}
                                            onClick={() => handleToggleStatus(role.role_id, role.role_name, role.is_active)}
                                            title={role.is_active ? "Deactivate" : "Activate"}
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
                                            title="Delete"
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
                    Page {page} / {totalPages || 1}
                </div>
                <div className="flex gap-2">
                    <Button
                        size="sm"
                        variant="outline"
                        disabled={page === 1}
                        onClick={() => setPage(page - 1)}
                    >
                        Previous
                    </Button>
                    <Button
                        size="sm"
                        variant="outline"
                        disabled={page === totalPages || totalPages === 0}
                        onClick={() => setPage(page + 1)}
                    >
                        Next
                    </Button>
                </div>
            </div>
        </div>
    );
}
