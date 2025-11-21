import React, { useState, useEffect } from "react";
import { getRoleDetails, updateRole, type UpdateRoleData, type RoleDetails } from "../../services/roleApi";
import { Button } from '../ui/figma/button';
import { Input } from '../ui/figma/input';
import { Label } from '../ui/figma/label';
import { Textarea } from '../ui/figma/textarea';
import { Badge } from '../ui/figma/badge';
import { X } from 'lucide-react';
import { useToast } from "../../contexts/ToastContext";

interface EditRoleFormProps {
    roleId: number;
    onClose: () => void;
    onSuccess: () => void;
}

export default function EditRoleForm({ roleId, onClose, onSuccess }: EditRoleFormProps) {
    const [role, setRole] = useState<RoleDetails | null>(null);
    const [formData, setFormData] = useState<UpdateRoleData>({
        role_name: '',
        description: ''
    });
    const [errors, setErrors] = useState<Partial<Record<keyof UpdateRoleData, string>>>({});
    const [isLoading, setIsLoading] = useState(true);
    const [isSubmitting, setIsSubmitting] = useState(false);
    const { showToast } = useToast();

    useEffect(() => {
        const fetchRole = async () => {
            setIsLoading(true);
            try {
                const data = await getRoleDetails(roleId);
                setRole(data);
                setFormData({
                    role_name: data.role_name,
                    description: data.description
                });
            } catch (err: any) {
                const detail = err?.response?.data?.detail;
                let errorMsg = "Không thể tải thông tin vai trò";
                if (typeof detail === 'string') {
                    errorMsg = detail;
                } else if (Array.isArray(detail) && detail.length > 0 && detail[0]?.msg) {
                    errorMsg = detail[0].msg;
                } else if (detail?.msg) {
                    errorMsg = detail.msg;
                }
                showToast(errorMsg, "error");
            } finally {
                setIsLoading(false);
            }
        };

        fetchRole();
    }, [roleId]);

    const validateForm = (): boolean => {
        const newErrors: Partial<Record<keyof UpdateRoleData, string>> = {};

        if (!formData.role_name?.trim()) {
            newErrors.role_name = "Tên vai trò không được để trống";
        }

        if (!formData.description?.trim()) {
            newErrors.description = "Mô tả không được để trống";
        }

        setErrors(newErrors);
        return Object.keys(newErrors).length === 0;
    };

    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault();

        if (!validateForm()) {
            return;
        }

        setIsSubmitting(true);
        try {
            const response = await updateRole(roleId, formData);
            showToast(response.message || "Cập nhật vai trò thành công!", "success");
            onSuccess();
        } catch (err: any) {
            const detail = err?.response?.data?.detail;
            let errorMsg = "Không thể cập nhật vai trò";
            if (typeof detail === 'string') {
                errorMsg = detail;
            } else if (Array.isArray(detail) && detail.length > 0 && detail[0]?.msg) {
                errorMsg = detail[0].msg;
            } else if (detail?.msg) {
                errorMsg = detail.msg;
            }
            showToast(errorMsg, "error");
        } finally {
            setIsSubmitting(false);
        }
    };

    const handleChange = (field: keyof UpdateRoleData, value: string) => {
        setFormData(prev => ({ ...prev, [field]: value }));
        // Clear error when user starts typing
        if (errors[field]) {
            setErrors(prev => ({ ...prev, [field]: undefined }));
        }
    };

    if (isLoading) {
        return (
            <div className="bg-white rounded-lg shadow-lg p-6">
                <div className="flex justify-center items-center h-64">
                    <div className="text-gray-500">Đang tải...</div>
                </div>
            </div>
        );
    }

    if (!role) {
        return (
            <div className="bg-white rounded-lg shadow-lg p-6">
                <div className="flex justify-between items-center mb-4">
                    <h2 className="text-2xl font-bold text-red-600">Lỗi</h2>
                    <Button variant="ghost" size="sm" onClick={onClose}>
                        <X className="h-5 w-5" />
                    </Button>
                </div>
                <div className="text-red-500">Không thể tải thông tin vai trò</div>
            </div>
        );
    }

    return (
        <div className="bg-white rounded-lg shadow-lg p-6">
            {/* Header */}
            <div className="flex justify-between items-start mb-6">
                <div>
                    <h2 className="text-2xl font-bold text-gray-800 mb-2">Chỉnh sửa vai trò</h2>
                    <Badge variant="outline" className="font-mono text-sm">
                        {role.role_code}
                    </Badge>
                </div>
                <Button variant="ghost" size="sm" onClick={onClose}>
                    <X className="h-5 w-5" />
                </Button>
            </div>

            {/* Info Note */}
            <div className="mb-6 p-4 bg-blue-50 rounded-lg border border-blue-200">
                <p className="text-sm text-blue-800">
                    <strong>Lưu ý:</strong> Mã vai trò (role_code) không thể thay đổi sau khi đã tạo.
                </p>
            </div>

            {/* Form */}
            <form onSubmit={handleSubmit} className="space-y-6">
                {/* Role Code (Read-only) */}
                <div className="space-y-2">
                    <Label htmlFor="role_code">Mã vai trò</Label>
                    <Input
                        id="role_code"
                        type="text"
                        value={role.role_code}
                        disabled
                        className="bg-gray-100"
                    />
                    <p className="text-xs text-gray-500">
                        Mã vai trò không thể thay đổi
                    </p>
                </div>

                {/* Role Name */}
                <div className="space-y-2">
                    <Label htmlFor="role_name">
                        Tên vai trò <span className="text-red-500">*</span>
                    </Label>
                    <Input
                        id="role_name"
                        type="text"
                        value={formData.role_name}
                        onChange={(e) => handleChange('role_name', e.target.value)}
                        placeholder="VD: Admin, Data Engineer"
                        className={errors.role_name ? 'border-red-500' : ''}
                        disabled={isSubmitting}
                    />
                    {errors.role_name && (
                        <p className="text-sm text-red-500">{errors.role_name}</p>
                    )}
                </div>

                {/* Description */}
                <div className="space-y-2">
                    <Label htmlFor="description">
                        Mô tả <span className="text-red-500">*</span>
                    </Label>
                    <Textarea
                        id="description"
                        value={formData.description}
                        onChange={(e) => handleChange('description', e.target.value)}
                        placeholder="Nhập mô tả chi tiết về vai trò này..."
                        rows={4}
                        className={errors.description ? 'border-red-500' : ''}
                        disabled={isSubmitting}
                    />
                    {errors.description && (
                        <p className="text-sm text-red-500">{errors.description}</p>
                    )}
                </div>

                {/* Status Display */}
                <div className="space-y-2">
                    <Label>Trạng thái</Label>
                    <div>
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
                    <p className="text-xs text-gray-500">
                        Để thay đổi trạng thái, sử dụng nút Kích hoạt/Vô hiệu hóa trong danh sách
                    </p>
                </div>

                {/* User Count Info */}
                {role.user_count !== undefined && (
                    <div className="p-4 bg-gray-50 rounded-lg">
                        <p className="text-sm text-gray-700">
                            <strong>Số người dùng hiện tại:</strong> {role.user_count} người
                        </p>
                    </div>
                )}

                {/* Action Buttons */}
                <div className="flex justify-end gap-3 pt-4 border-t">
                    <Button 
                        type="button" 
                        variant="outline" 
                        onClick={onClose}
                        disabled={isSubmitting}
                    >
                        Hủy
                    </Button>
                    <Button 
                        type="submit" 
                        disabled={isSubmitting}
                    >
                        {isSubmitting ? 'Đang cập nhật...' : 'Cập nhật vai trò'}
                    </Button>
                </div>
            </form>
        </div>
    );
}
