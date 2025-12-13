import React, { useEffect, useState } from "react";
import { getRoleUsers, getRoleDetails } from "../../services/roleApi";
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
import { X, Users as UsersIcon } from 'lucide-react';
import { useToast } from "../../contexts/ToastContext";

interface RoleUsersModalProps {
    roleId: number;
    onClose: () => void;
}

export default function RoleUsersModal({ roleId, onClose }: RoleUsersModalProps) {
    const [roleName, setRoleName] = useState<string>('');
    const [users, setUsers] = useState<any[]>([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const [page, setPage] = useState(1);
    const [limit] = useState(10);
    const [total, setTotal] = useState(0);
    const { showToast } = useToast();

    useEffect(() => {
        const fetchRoleDetails = async () => {
            try {
                const roleData = await getRoleDetails(roleId);
                setRoleName(roleData.role_name);
            } catch (err: any) {
                console.error("Failed to fetch role details:", err);
            }
        };

        fetchRoleDetails();
    }, [roleId]);

    useEffect(() => {
        const fetchUsers = async () => {
            setLoading(true);
            setError(null);
            try {
                const data = await getRoleUsers(roleId, { page, limit });

                // Handle different response structures
                if (typeof data === 'string') {
                    // If API returns a string, parse it or handle accordingly
                    setUsers([]);
                    setTotal(0);
                } else if (data.success && Array.isArray(data.data)) {

                    setUsers(data.users);
                    setTotal(data.total || data.data.length);
                } else if (Array.isArray(data)) {
                    console.log("2")
                    setUsers(data);
                    setTotal(data.length);
                } else {

                    setUsers(data.users);
                    setTotal(data.total);
                }
            } catch (err: any) {
                const detail = err?.response?.data?.detail;
                let errorMsg = "Failed to fetch users";
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

        fetchUsers();
    }, [roleId, page, limit]);

    const totalPages = Math.ceil(total / limit);

    return (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black bg-opacity-50">
            <div className="bg-white rounded-lg shadow-2xl p-6 max-w-4xl w-full mx-4 max-h-[90vh] overflow-y-auto">
                {/* Header */}
                <div className="flex justify-between items-start mb-6 pb-4 border-b">
                    <div>
                        <h2 className="text-2xl font-bold text-gray-800 mb-2">
                            Users with role: {roleName}
                        </h2>
                        <div className="flex items-center gap-2 text-gray-600">
                            <UsersIcon className="h-5 w-5" />
                            <span className="text-sm">Total users: {total}</span>
                        </div>
                    </div>
                    <Button variant="ghost" size="sm" onClick={onClose}>
                        <X className="h-5 w-5" />
                    </Button>
                </div>

                {/* Loading State */}
                {loading && (
                    <div className="flex justify-center items-center h-64">
                        <div className="text-gray-500">Loading...</div>
                    </div>
                )}

                {/* Error State */}
                {error && !loading && (
                    <div className="text-red-500 mb-4 p-4 bg-red-50 rounded-lg">
                        {error}
                    </div>
                )}

                {/* Empty State */}
                {!loading && !error && users.length === 0 && (
                    <div className="flex flex-col items-center justify-center h-64 text-gray-500">
                        <UsersIcon className="h-16 w-16 mb-4 text-gray-300" />
                        <p className="text-lg font-medium">No users found</p>
                        <p className="text-sm">No users assigned to this role yet</p>
                    </div>
                )}

                {/* Users Table */}
                {!loading && !error && users.length > 0 && (
                    <>
                        <div className="overflow-x-auto">
                            <Table>
                                <TableHeader>
                                    <TableRow>
                                        <TableHead>#</TableHead>
                                        <TableHead>Name</TableHead>
                                        <TableHead>Email</TableHead>
                                        <TableHead>Phone</TableHead>
                                        <TableHead>Status</TableHead>
                                        <TableHead>Last Login</TableHead>
                                    </TableRow>
                                </TableHeader>
                                <TableBody>
                                    {users.map((user, idx) => (
                                        <TableRow key={user.user_id || idx}>
                                            <TableCell>{(page - 1) * limit + idx + 1}</TableCell>
                                            <TableCell className="font-medium">
                                                {user.full_name || user.name || '-'}
                                            </TableCell>
                                            <TableCell>{user.email || '-'}</TableCell>
                                            <TableCell>{user.phone || '-'}</TableCell>
                                            <TableCell>
                                                {user.status === 'active' ? (
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
                                                {user.last_login_at
                                                    ? new Date(user.last_login_at).toLocaleString('vi-VN')
                                                    : '-'
                                                }
                                            </TableCell>
                                        </TableRow>
                                    ))}
                                </TableBody>
                            </Table>
                        </div>

                        {/* Pagination */}
                        {totalPages > 1 && (
                            <div className="flex justify-between items-center mt-4 pt-4 border-t">
                                <div className="text-gray-600 text-sm">
                                    Page {page} / {totalPages}
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
                                        disabled={page === totalPages}
                                        onClick={() => setPage(page + 1)}
                                    >
                                        Next
                                    </Button>
                                </div>
                            </div>
                        )}
                    </>
                )}

                {/* Close Button */}
                <div className="flex justify-end mt-6 pt-4 border-t">
                    <Button variant="outline" onClick={onClose}>
                        Close
                    </Button>
                </div>
            </div>
        </div>
    );
}
