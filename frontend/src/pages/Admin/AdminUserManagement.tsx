import React, { useState } from "react";
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
        <button onClick={() => setView("active")}>Người dùng đang hoạt động</button>
        <button onClick={() => setView("deleted")}>Người dùng đã xóa</button>
        <button onClick={() => setView("create")}>Tạo người dùng mới</button>
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
