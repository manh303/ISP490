import React, { useMemo } from 'react';
import { useAuth } from '../../contexts/AuthContext';
import AdminDashboard from './roles/AdminDashboard';
import AnalystDashboard from './roles/AnalystDashboard';
import CustomerDashboard from './roles/CustomerDashboard';

/** Chuẩn hoá role về: 'admin' | 'analyst' | 'customer' */
function normalizeRole(user: any): 'admin' | 'analyst' | 'customer' {
  if (!user) return 'customer';

  // 1) Ưu tiên role dạng string
  const r1 = (user.role || '').toString().toLowerCase();
  if (r1) return alias(r1);

  // 2) Fallback sang mảng roles[]
  const first = Array.isArray(user.roles) && user.roles.length ? user.roles[0] : null;
  const r2 = (
    first?.role_code ||
    first?.role ||
    first?.code ||
    ''
  ).toString().toLowerCase();

  return alias(r2 || 'customer');
}

/** Gộp các biến thể/viết hoa-thường về 3 nhóm chính */
function alias(role: string): 'admin' | 'analyst' | 'customer' {
  switch (role) {
    case 'super_admin':
    case 'superadmin':
    case 'admin':
    case 'manager':
      return 'admin';
    case 'analyst':
      return 'analyst';
    case 'customer':
    case 'viewer':
    case 'user':
    default:
      return 'customer';
  }
}

export default function RoleBasedDashboard() {
  const { user, loading } = useAuth(); // AuthContext mới luôn trả object hợp lệ

  const role = useMemo(() => normalizeRole(user), [user]);

  if (loading) {
    return (
      <div className="p-6">
        <div className="w-8 h-8 border-4 border-blue-200 border-t-blue-600 rounded-full animate-spin" />
        <p className="text-gray-600 mt-3">Loading dashboard…</p>
      </div>
    );
  }

  if (import.meta?.env?.DEV) {
    console.debug('[RoleBasedDashboard] user =', user, '→ role =', role);
  }

  switch (role) {
    case 'admin':    return <AdminDashboard />;
    case 'analyst':  return <AnalystDashboard />;
    case 'customer': return <CustomerDashboard />;
    default:         return <CustomerDashboard />;
  }
}
