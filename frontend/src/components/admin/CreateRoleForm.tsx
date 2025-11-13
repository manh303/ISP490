import React, { useState } from "react";
import { createRole, type CreateRoleData } from "../../services/roleApi";
import { Button } from '../ui/figma/button';
import { Input } from '../ui/figma/input';
import { Label } from '../ui/figma/label';
import { Textarea } from '../ui/figma/textarea';
import { X } from 'lucide-react';
import { useToast } from "../../contexts/ToastContext";

interface CreateRoleFormProps {
    onClose: () => void;
    onSuccess: () => void;
}

export default function CreateRoleForm({ onClose, onSuccess }: CreateRoleFormProps) {
    const [formData, setFormData] = useState<CreateRoleData>({
        role_code: '',
        role_name: '',
        description: ''
    });
    const [errors, setErrors] = useState<Partial<Record<keyof CreateRoleData, string>>>({});
    const [isSubmitting, setIsSubmitting] = useState(false);
    const { showToast } = useToast();

    const validateForm = (): boolean => {
        const newErrors: Partial<Record<keyof CreateRoleData, string>> = {};

        if (!formData.role_code.trim()) {
            newErrors.role_code = "Mã vai trò không được để trống";
        } else if (!/^[A-Z_]+$/.test(formData.role_code)) {
            newErrors.role_code = "Mã vai trò chỉ được chứa chữ in hoa và dấu gạch dưới";
        }

        if (!formData.role_name.trim()) {
            newErrors.role_name = "Tên vai trò không được để trống";
        }

        if (!formData.description.trim()) {
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
            const response = await createRole(formData);
            showToast(response.message || "Tạo vai trò thành công!", "success");
            onSuccess();
        } catch (err: any) {
            const errorMsg = err?.response?.data?.detail || "Không thể tạo vai trò";
            showToast(errorMsg, "error");
        } finally {
            setIsSubmitting(false);
        }
    };

    const handleChange = (field: keyof CreateRoleData, value: string) => {
        setFormData(prev => ({ ...prev, [field]: value }));
        // Clear error when user starts typing
        if (errors[field]) {
            setErrors(prev => ({ ...prev, [field]: undefined }));
        }
    };

    return (
        <div className="bg-white rounded-lg shadow-lg p-6">
            {/* Header */}
            <div className="flex justify-between items-center mb-6">
                <h2 className="text-2xl font-bold text-gray-800">Tạo vai trò mới</h2>
                <Button variant="ghost" size="sm" onClick={onClose}>
                    <X className="h-5 w-5" />
                </Button>
            </div>

            {/* Form */}
            <form onSubmit={handleSubmit} className="space-y-6">
                {/* Role Code */}
                <div className="space-y-2">
                    <Label htmlFor="role_code">
                        Mã vai trò <span className="text-red-500">*</span>
                    </Label>
                    <Input
                        id="role_code"
                        type="text"
                        value={formData.role_code}
                        onChange={(e) => handleChange('role_code', e.target.value.toUpperCase())}
                        placeholder="VD: ADMIN, DATA_ENGINEER"
                        className={errors.role_code ? 'border-red-500' : ''}
                        disabled={isSubmitting}
                    />
                    {errors.role_code && (
                        <p className="text-sm text-red-500">{errors.role_code}</p>
                    )}
                    <p className="text-xs text-gray-500">
                        Mã vai trò chỉ được chứa chữ in hoa và dấu gạch dưới (_)
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
                        {isSubmitting ? 'Đang tạo...' : 'Tạo vai trò'}
                    </Button>
                </div>
            </form>
        </div>
    );
}
