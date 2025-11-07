import React, { useState } from "react";
import { Button } from '../../components/ui/figma/button';
import { Users, Trash2, UserPlus } from 'lucide-react';
import ActiveUsersList from "../../components/admin/ActiveUsersList";
import CreateUserForm from "../../components/admin/CreateUserForm";
import DeletedUsersList from "../../components/admin/DeletedUsersList";
import UserDetails from "../../components/admin/UserDetails";

export default function AdminUserManagement() {
  const [selectedUserId, setSelectedUserId] = useState<number | null>(null);
  const [view, setView] = useState<"active" | "deleted" | "create" | "details">("active");

  return (
    <div className="admin-user-management p-6">
      <h1 className="text-2xl font-bold mb-4">Quản lý người dùng</h1>
      <div className="flex gap-4 mb-6">
        <Button
          variant={view === "active" ? "default" : "outline"}
          onClick={() => setView("active")}
          className="flex items-center gap-2"
        >
          <Users className="h-4 w-4" /> Người dùng đang hoạt động
        </Button>
        <Button
          variant={view === "deleted" ? "default" : "outline"}
          onClick={() => setView("deleted")}
          className="flex items-center gap-2"
        >
          <Trash2 className="h-4 w-4" /> Người dùng đã xóa
        </Button>
        <Button
          variant={view === "create" ? "default" : "outline"}
          onClick={() => setView("create")}
          className="flex items-center gap-2"
        >
          <UserPlus className="h-4 w-4" /> Tạo người dùng mới
        </Button>
      </div>
      {view === "active" && (
        <ActiveUsersList onSelectUser={id => { setSelectedUserId(id); setView("details"); }} />
      )}
      {view === "deleted" && (
        <DeletedUsersList onSelectUser={id => { setSelectedUserId(id); setView("details"); }} />
      )}
      {view === "create" && (
        <CreateUserForm onCreated={() => setView("active")} />
      )}
      {view === "details" && selectedUserId && (
        <UserDetails userId={selectedUserId} onBack={() => setView("active")} />
      )}
    </div>
  );
}
