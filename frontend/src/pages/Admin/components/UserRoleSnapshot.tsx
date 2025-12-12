import React from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../../../components/ui/figma/card';
import { Badge } from '../../../components/ui/figma/badge';
import { Button } from '../../../components/ui/figma/button';
import { Users, Settings, UserCheck, Clock, ArrowRight } from 'lucide-react';
import { Link } from 'react-router-dom';
import {
    PieChart,
    Pie,
    Cell,
    ResponsiveContainer,
    Tooltip,
    Legend,
} from 'recharts';

interface User {
    user_id: number;
    email: string;
    full_name: string;
    role_name: string;
    status: string;
    last_login_at: string | null;
}

interface RoleDistribution {
    role_name: string;
    count: number;
}

interface UserRoleSnapshotProps {
    roleDistribution: RoleDistribution[];
    recentUsers: User[];
    selectedRoleFilter?: string;
    onRoleFilterChange?: (role: string) => void;
    isLoading?: boolean;
}

const COLORS = [
    '#3B82F6', // blue
    '#8B5CF6', // purple
    '#10B981', // green
    '#F59E0B', // amber
    '#EF4444', // red
    '#06B6D4', // cyan
    '#EC4899', // pink
];

const getStatusBadge = (status: string) => {
    const statusLower = status?.toLowerCase();
    if (statusLower === 'active') {
        return <Badge variant="default" className="bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300">Active</Badge>;
    } else if (statusLower === 'inactive') {
        return <Badge variant="secondary" className="bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300">Inactive</Badge>;
    } else if (statusLower === 'locked') {
        return <Badge variant="destructive">Locked</Badge>;
    }
    return <Badge variant="outline">{status}</Badge>;
};

const formatTimeAgo = (dateString: string | null) => {
    if (!dateString) return 'Never';
    const date = new Date(dateString);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));

    if (diffDays === 0) return 'Today';
    if (diffDays === 1) return 'Yesterday';
    if (diffDays < 7) return `${diffDays} days ago`;
    if (diffDays < 30) return `${Math.floor(diffDays / 7)} weeks ago`;
    return date.toLocaleDateString('vi-VN');
};

export default function UserRoleSnapshot({
    roleDistribution,
    recentUsers,
    selectedRoleFilter = 'all',
    onRoleFilterChange,
    isLoading = false,
}: UserRoleSnapshotProps) {
    if (isLoading) {
        return (
            <div className="space-y-4">
                <h2 className="text-xl font-bold text-gray-900 dark:text-white flex items-center gap-2">
                    <Users className="w-5 h-5 text-purple-600" />
                    User & Role Management
                </h2>
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    <Card className="animate-pulse">
                        <CardContent className="p-6">
                            <div className="h-64 bg-gray-200 dark:bg-gray-700 rounded"></div>
                        </CardContent>
                    </Card>
                    <Card className="animate-pulse">
                        <CardContent className="p-6">
                            <div className="h-64 bg-gray-200 dark:bg-gray-700 rounded"></div>
                        </CardContent>
                    </Card>
                </div>
            </div>
        );
    }

    const totalUsers = roleDistribution.reduce((sum, r) => sum + r.count, 0);

    return (
        <div className="space-y-4">
            <div className="flex items-center justify-between">
                <h2 className="text-xl font-bold text-gray-900 dark:text-white flex items-center gap-2">
                    <Users className="w-5 h-5 text-purple-600" />
                    User & Role Management
                </h2>
                <div className="flex gap-2">
                    <Link to="/admin/users">
                        <Button variant="outline" size="sm">
                            <UserCheck className="w-4 h-4 mr-2" />
                            Manage Users
                        </Button>
                    </Link>
                    <Link to="/admin/roles">
                        <Button variant="outline" size="sm">
                            <Settings className="w-4 h-4 mr-2" />
                            Manage Roles
                        </Button>
                    </Link>
                </div>
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Role Distribution Chart */}
                <Card>
                    <CardHeader>
                        <CardTitle className="text-base">User Distribution by Role</CardTitle>
                    </CardHeader>
                    <CardContent>
                        <div className="h-64">
                            <ResponsiveContainer width="100%" height="100%">
                                <PieChart>
                                    <Pie
                                        data={roleDistribution as any[]}
                                        dataKey="count"
                                        nameKey="role_name"
                                        cx="50%"
                                        cy="50%"
                                        outerRadius={80}
                                        innerRadius={50}
                                        paddingAngle={2}
                                        label={({ name, value }) => `${name}: ${value}`}
                                        labelLine={false}
                                    >
                                        {roleDistribution.map((entry, index) => (
                                            <Cell key={entry.role_name} fill={COLORS[index % COLORS.length]} />
                                        ))}
                                    </Pie>
                                    <Tooltip
                                        formatter={(value: number, name: string) => [`${value} users`, name]}
                                    />
                                    <Legend />
                                </PieChart>
                            </ResponsiveContainer>
                        </div>
                        <div className="text-center text-sm text-gray-500 mt-2">
                            Total: {totalUsers} users
                        </div>
                    </CardContent>
                </Card>

                {/* Recent Users Table */}
                <Card>
                    <CardHeader className="flex flex-row items-center justify-between">
                        <CardTitle className="text-base">Recent Users</CardTitle>
                        {onRoleFilterChange && (
                            <select
                                value={selectedRoleFilter}
                                onChange={(e) => onRoleFilterChange(e.target.value)}
                                className="text-sm border rounded px-2 py-1 dark:bg-gray-800 dark:border-gray-700"
                            >
                                <option value="all">All Roles</option>
                                {roleDistribution.map((r) => (
                                    <option key={r.role_name} value={r.role_name}>{r.role_name}</option>
                                ))}
                            </select>
                        )}
                    </CardHeader>
                    <CardContent>
                        <div className="space-y-3 max-h-64 overflow-y-auto">
                            {recentUsers.length === 0 ? (
                                <div className="text-center text-gray-500 py-8">No users found</div>
                            ) : (
                                recentUsers.slice(0, 10).map((user) => (
                                    <Link
                                        to={`/admin/users/${user.user_id}`}
                                        key={user.user_id}
                                        className="flex items-center justify-between p-3 rounded-lg hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors group"
                                    >
                                        <div className="flex items-center gap-3">
                                            <div className="w-8 h-8 rounded-full bg-gradient-to-br from-blue-500 to-purple-500 flex items-center justify-center text-white text-sm font-medium">
                                                {user.full_name?.[0]?.toUpperCase() || user.email?.[0]?.toUpperCase() || 'U'}
                                            </div>
                                            <div>
                                                <div className="font-medium text-gray-900 dark:text-white text-sm">
                                                    {user.full_name || user.email}
                                                </div>
                                                <div className="text-xs text-gray-500">{user.email}</div>
                                            </div>
                                        </div>
                                        <div className="flex items-center gap-3">
                                            <Badge variant="outline" className="text-xs">{user.role_name}</Badge>
                                            {getStatusBadge(user.status)}
                                            <div className="flex items-center text-xs text-gray-500">
                                                <Clock className="w-3 h-3 mr-1" />
                                                {formatTimeAgo(user.last_login_at)}
                                            </div>
                                            <ArrowRight className="w-4 h-4 text-gray-400 opacity-0 group-hover:opacity-100 transition-opacity" />
                                        </div>
                                    </Link>
                                ))
                            )}
                        </div>
                    </CardContent>
                </Card>
            </div>
        </div>
    );
}
