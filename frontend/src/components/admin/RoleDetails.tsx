import React, { useEffect, useState } from "react";
import { getRoleDetails, type RoleDetails as RoleDetailsType } from "../../services/roleApi";
import { Button } from '../ui/figma/button';
import { Badge } from '../ui/figma/badge';
import { X, Users, Shield, Layers, Zap } from 'lucide-react';
import { useToast } from "../../contexts/ToastContext";

interface RoleDetailsProps {
    roleId: number;
    onClose: () => void;
    onEdit?: () => void;
}

export default function RoleDetails({ roleId, onClose, onEdit }: RoleDetailsProps) {
    const [role, setRole] = useState<RoleDetailsType | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const { showToast } = useToast();

    useEffect(() => {
        const fetchRoleDetails = async () => {
            setLoading(true);
            setError(null);
            try {
                const data = await getRoleDetails(roleId);
                setRole(data);
            } catch (err: any) {
                const errorMsg = err?.response?.data?.detail || "Không thể tải thông tin vai trò";
                setError(errorMsg);
                showToast(errorMsg, "error");
            } finally {
                setLoading(false);
            }
        };

        fetchRoleDetails();
    }, [roleId]);

    if (loading) {
        return (
            <div className="bg-white rounded-lg shadow-lg p-6">
                <div className="flex justify-center items-center h-64">
                    <div className="text-gray-500">Đang tải...</div>
                </div>
            </div>
        );
    }

    if (error || !role) {
        return (
            <div className="bg-white rounded-lg shadow-lg p-6">
                <div className="flex justify-between items-center mb-4">
                    <h2 className="text-2xl font-bold text-red-600">Lỗi</h2>
                    <Button variant="ghost" size="sm" onClick={onClose}>
                        <X className="h-5 w-5" />
                    </Button>
                </div>
                <div className="text-red-500">{error}</div>
            </div>
        );
    }

    return (
        <div className="bg-white rounded-lg shadow-lg p-6">
            {/* Header */}
            <div className="flex justify-between items-start mb-6">
                <div>
                    <h2 className="text-2xl font-bold text-gray-800 mb-2">{role.role_name}</h2>
                    <Badge variant="outline" className="font-mono text-sm">
                        {role.role_code}
                    </Badge>
                </div>
                <div className="flex gap-2">
                    {onEdit && (
                        <Button variant="outline" size="sm" onClick={onEdit}>
                            Chỉnh sửa
                        </Button>
                    )}
                    <Button variant="ghost" size="sm" onClick={onClose}>
                        <X className="h-5 w-5" />
                    </Button>
                </div>
            </div>

            {/* Status Badge */}
            <div className="mb-6">
                {role.is_active ? (
                    <Badge variant="default" className="bg-green-500 text-white">
                        Đang hoạt động
                    </Badge>
                ) : (
                    <Badge variant="destructive" className="bg-gray-500 text-white">
                        Đã vô hiệu hóa
                    </Badge>
                )}
            </div>

            {/* Description */}
            <div className="mb-6">
                <h3 className="text-lg font-semibold text-gray-700 mb-2">Mô tả</h3>
                <p className="text-gray-600 leading-relaxed">{role.description}</p>
            </div>

            {/* User Count */}
            {role.user_count !== undefined && (
                <div className="mb-6 p-4 bg-blue-50 rounded-lg">
                    <div className="flex items-center gap-2">
                        <Users className="h-5 w-5 text-blue-600" />
                        <span className="text-gray-700 font-medium">Số người dùng:</span>
                        <span className="text-2xl font-bold text-blue-600">{role.user_count}</span>
                    </div>
                </div>
            )}

            {/* Permissions */}
            {role.permissions && role.permissions.length > 0 && (
                <div className="mb-6">
                    <div className="flex items-center gap-2 mb-3">
                        <Shield className="h-5 w-5 text-purple-600" />
                        <h3 className="text-lg font-semibold text-gray-700">Quyền hạn</h3>
                    </div>
                    <div className="flex flex-wrap gap-2">
                        {role.permissions.map((permission, idx) => (
                            <Badge key={idx} variant="secondary" className="bg-purple-100 text-purple-700">
                                {permission}
                            </Badge>
                        ))}
                    </div>
                </div>
            )}

            {/* Modules */}
            {role.modules && role.modules.length > 0 && (
                <div className="mb-6">
                    <div className="flex items-center gap-2 mb-3">
                        <Layers className="h-5 w-5 text-indigo-600" />
                        <h3 className="text-lg font-semibold text-gray-700">Modules</h3>
                    </div>
                    <div className="flex flex-wrap gap-2">
                        {role.modules.map((module, idx) => (
                            <Badge key={idx} variant="secondary" className="bg-indigo-100 text-indigo-700">
                                {module}
                            </Badge>
                        ))}
                    </div>
                </div>
            )}

            {/* Actions */}
            {role.actions && role.actions.length > 0 && (
                <div className="mb-6">
                    <div className="flex items-center gap-2 mb-3">
                        <Zap className="h-5 w-5 text-yellow-600" />
                        <h3 className="text-lg font-semibold text-gray-700">Hành động</h3>
                    </div>
                    <div className="flex flex-wrap gap-2">
                        {role.actions.map((action, idx) => (
                            <Badge key={idx} variant="secondary" className="bg-yellow-100 text-yellow-700">
                                {action}
                            </Badge>
                        ))}
                    </div>
                </div>
            )}

            {/* Admin Features */}
            {role.admin_features && Object.keys(role.admin_features).length > 0 && (
                <div className="mb-6">
                    <h3 className="text-lg font-semibold text-gray-700 mb-3">Tính năng quản trị</h3>
                    <div className="grid grid-cols-2 gap-3">
                        {Object.entries(role.admin_features).map(([feature, enabled]) => (
                            <div key={feature} className="flex items-center gap-2 p-3 bg-gray-50 rounded">
                                <div className={`w-3 h-3 rounded-full ${enabled ? 'bg-green-500' : 'bg-gray-300'}`} />
                                <span className="text-sm text-gray-700">{feature}</span>
                            </div>
                        ))}
                    </div>
                </div>
            )}

            {/* Action Buttons */}
            <div className="flex justify-end gap-2 mt-6 pt-6 border-t">
                <Button variant="outline" onClick={onClose}>
                    Đóng
                </Button>
                {onEdit && (
                    <Button onClick={onEdit}>
                        Chỉnh sửa vai trò
                    </Button>
                )}
            </div>
        </div>
    );
}
