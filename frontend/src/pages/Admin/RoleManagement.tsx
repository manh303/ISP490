import React, { useState } from "react";
import RoleList from "../../components/admin/RoleList";
import RoleDetails from "../../components/admin/RoleDetails";
import CreateRoleForm from "../../components/admin/CreateRoleForm";
import EditRoleForm from "../../components/admin/EditRoleForm";
import RoleUsersModal from "../../components/admin/RoleUsersModal";
import { Button } from '../../components/ui/figma/button';
import { Plus, Shield } from 'lucide-react';

type ViewMode = 'list' | 'create' | 'view' | 'edit';

export default function RoleManagement() {
    const [viewMode, setViewMode] = useState<ViewMode>('list');
    const [selectedRoleId, setSelectedRoleId] = useState<number | null>(null);
    const [showUsersModal, setShowUsersModal] = useState(false);
    const [usersModalRoleId, setUsersModalRoleId] = useState<number | null>(null);
    const [refreshTrigger, setRefreshTrigger] = useState(0);

    const handleSelectRole = (roleId: number, mode: 'view' | 'edit') => {
        setSelectedRoleId(roleId);
        setViewMode(mode);
    };

    const handleViewUsers = (roleId: number) => {
        setUsersModalRoleId(roleId);
        setShowUsersModal(true);
    };

    const handleBackToList = () => {
        setViewMode('list');
        setSelectedRoleId(null);
        setRefreshTrigger(prev => prev + 1);
    };

    const handleSuccess = () => {
        handleBackToList();
    };

    return (
        <div className="container mx-auto p-6">
            {/* Header */}
            <div className="mb-6">
                <div className="flex items-center justify-between">
                    <div className="flex items-center gap-3">
                        <div className="p-3 bg-purple-100 rounded-lg">
                            <Shield className="h-8 w-8 text-purple-600" />
                        </div>
                        <div>
                            <h1 className="text-3xl font-bold text-gray-800">Quản lý vai trò</h1>
                            <p className="text-gray-600 mt-1">
                                Quản lý vai trò người dùng và phân quyền trong hệ thống
                            </p>
                        </div>
                    </div>
                    {viewMode === 'list' && (
                        <Button 
                            onClick={() => setViewMode('create')}
                            className="flex items-center gap-2"
                        >
                            <Plus className="h-5 w-5" />
                            Tạo vai trò mới
                        </Button>
                    )}
                    {viewMode !== 'list' && (
                        <Button 
                            variant="outline"
                            onClick={handleBackToList}
                        >
                            ← Quay lại danh sách
                        </Button>
                    )}
                </div>
            </div>

            {/* Breadcrumb */}
            <div className="mb-6 text-sm text-gray-600">
                <span className="hover:text-gray-800 cursor-pointer" onClick={handleBackToList}>
                    Danh sách vai trò
                </span>
                {viewMode === 'create' && <span> / Tạo vai trò mới</span>}
                {viewMode === 'view' && <span> / Xem chi tiết</span>}
                {viewMode === 'edit' && <span> / Chỉnh sửa</span>}
            </div>

            {/* Main Content */}
            <div className="bg-gray-50 rounded-lg p-6">
                {viewMode === 'list' && (
                    <RoleList
                        onSelectRole={handleSelectRole}
                        onViewUsers={handleViewUsers}
                        refreshTrigger={refreshTrigger}
                    />
                )}

                {viewMode === 'create' && (
                    <CreateRoleForm
                        onClose={handleBackToList}
                        onSuccess={handleSuccess}
                    />
                )}

                {viewMode === 'view' && selectedRoleId && (
                    <RoleDetails
                        roleId={selectedRoleId}
                        onClose={handleBackToList}
                        onEdit={() => setViewMode('edit')}
                    />
                )}

                {viewMode === 'edit' && selectedRoleId && (
                    <EditRoleForm
                        roleId={selectedRoleId}
                        onClose={handleBackToList}
                        onSuccess={handleSuccess}
                    />
                )}
            </div>

            {/* Users Modal */}
            {showUsersModal && usersModalRoleId && (
                <RoleUsersModal
                    roleId={usersModalRoleId}
                    onClose={() => {
                        setShowUsersModal(false);
                        setUsersModalRoleId(null);
                    }}
                />
            )}
        </div>
    );
}
