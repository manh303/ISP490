import React, { useState } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../../../components/ui/figma/card';
import { Badge } from '../../../components/ui/figma/badge';
import { Button } from '../../../components/ui/figma/button';
import {
    Shield,
    Clock,
    User,
    Activity,
    AlertTriangle,
    CheckCircle,
    XCircle,
    Download,
    Eye,
    Lock,
    FileDown,
    Settings
} from 'lucide-react';

interface ActivityLog {
    log_id: number;
    created_at: string;
    email: string;
    full_name?: string;
    role: string;
    action: string;
    module: string;
    resource?: string;
    status: 'SUCCESS' | 'FAILED';
    latency_ms?: number;
    is_high_risk?: boolean;
}

interface SecurityAuditLogProps {
    activityLogs: ActivityLog[];
    totalLogs: number;

    // Filters
    selectedTimeRange?: string;
    selectedUser?: string;
    selectedAction?: string;
    selectedStatus?: string;

    onTimeRangeChange?: (range: string) => void;
    onUserChange?: (user: string) => void;
    onActionChange?: (action: string) => void;
    onStatusChange?: (status: string) => void;
    onViewDetail?: (logId: number) => void;
    onExportCSV?: () => void;

    availableUsers?: string[];
    availableActions?: string[];

    isLoading?: boolean;
}

const HIGH_RISK_ACTIONS = [
    'GRANT_ADMIN_ROLE',
    'CHANGE_ROLE',
    'DELETE_USER',
    'EXPORT_DATA',
    'CHANGE_DATASET_ACCESS',
    'UPDATE_PERMISSIONS',
    'CREATE_ADMIN',
];

const getActionIcon = (action: string) => {
    if (action.includes('LOGIN')) return User;
    if (action.includes('EXPORT')) return FileDown;
    if (action.includes('ROLE') || action.includes('PERMISSION')) return Lock;
    if (action.includes('SETTINGS') || action.includes('CONFIG')) return Settings;
    return Activity;
};

const getStatusBadge = (status: string) => {
    if (status === 'SUCCESS') {
        return <Badge className="bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300">Success</Badge>;
    }
    return <Badge variant="destructive">Failed</Badge>;
};

const formatLatency = (ms?: number) => {
    if (!ms) return '-';
    if (ms < 1000) return `${ms}ms`;
    return `${(ms / 1000).toFixed(1)}s`;
};

export default function SecurityAuditLog({
    activityLogs,
    totalLogs,
    selectedTimeRange = '24h',
    selectedUser = 'all',
    selectedAction = 'all',
    selectedStatus = 'all',
    onTimeRangeChange,
    onUserChange,
    onActionChange,
    onStatusChange,
    onViewDetail,
    onExportCSV,
    availableUsers = [],
    availableActions = [],
    isLoading = false,
}: SecurityAuditLogProps) {
    if (isLoading) {
        return (
            <div className="space-y-4">
                <h2 className="text-xl font-bold text-gray-900 dark:text-white flex items-center gap-2">
                    <Shield className="w-5 h-5 text-red-600" />
                    Security & Audit Log
                </h2>
                <div className="animate-pulse">
                    <div className="h-96 bg-gray-200 dark:bg-gray-700 rounded"></div>
                </div>
            </div>
        );
    }

    const highRiskCount = activityLogs.filter(log =>
        HIGH_RISK_ACTIONS.some(action => log.action.toUpperCase().includes(action)) || log.is_high_risk
    ).length;

    return (
        <div className="space-y-4">
            <div className="flex items-center justify-between flex-wrap gap-4">
                <h2 className="text-xl font-bold text-gray-900 dark:text-white flex items-center gap-2">
                    <Shield className="w-5 h-5 text-red-600" />
                    Security & Audit Log
                    {highRiskCount > 0 && (
                        <Badge className="bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-300 ml-2">
                            {highRiskCount} high-risk actions
                        </Badge>
                    )}
                </h2>
                <Button variant="outline" size="sm" onClick={onExportCSV}>
                    <Download className="w-4 h-4 mr-2" />
                    Export CSV
                </Button>
            </div>

            {/* Filters */}
            <Card>
                <CardContent className="p-4">
                    <div className="flex flex-wrap gap-4 items-center">
                        <div className="flex items-center gap-2">
                            <Clock className="w-4 h-4 text-gray-500" />
                            <select
                                value={selectedTimeRange}
                                onChange={(e) => onTimeRangeChange?.(e.target.value)}
                                className="text-sm border rounded px-2 py-1 dark:bg-gray-800 dark:border-gray-700"
                            >
                                <option value="1h">Last 1 hour</option>
                                <option value="24h">Last 24 hours</option>
                                <option value="7d">Last 7 days</option>
                                <option value="30d">Last 30 days</option>
                            </select>
                        </div>

                        <div className="flex items-center gap-2">
                            <User className="w-4 h-4 text-gray-500" />
                            <select
                                value={selectedUser}
                                onChange={(e) => onUserChange?.(e.target.value)}
                                className="text-sm border rounded px-2 py-1 dark:bg-gray-800 dark:border-gray-700 min-w-32"
                            >
                                <option value="all">All Users</option>
                                {availableUsers.map(user => (
                                    <option key={user} value={user}>{user}</option>
                                ))}
                            </select>
                        </div>

                        <div className="flex items-center gap-2">
                            <Activity className="w-4 h-4 text-gray-500" />
                            <select
                                value={selectedAction}
                                onChange={(e) => onActionChange?.(e.target.value)}
                                className="text-sm border rounded px-2 py-1 dark:bg-gray-800 dark:border-gray-700 min-w-32"
                            >
                                <option value="all">All Actions</option>
                                {availableActions.map(action => (
                                    <option key={action} value={action}>{action.replace(/_/g, ' ')}</option>
                                ))}
                            </select>
                        </div>

                        <div className="flex items-center gap-2">
                            <select
                                value={selectedStatus}
                                onChange={(e) => onStatusChange?.(e.target.value)}
                                className="text-sm border rounded px-2 py-1 dark:bg-gray-800 dark:border-gray-700"
                            >
                                <option value="all">All Results</option>
                                <option value="SUCCESS">Success</option>
                                <option value="FAILED">Failed</option>
                            </select>
                        </div>

                        <div className="text-sm text-gray-500 ml-auto">
                            Showing {activityLogs.length} of {totalLogs} logs
                        </div>
                    </div>
                </CardContent>
            </Card>

            {/* Activity Timeline */}
            <Card>
                <CardHeader>
                    <CardTitle className="text-base">Recent Activities</CardTitle>
                </CardHeader>
                <CardContent>
                    <div className="space-y-1 max-h-96 overflow-y-auto">
                        {activityLogs.length === 0 ? (
                            <div className="text-center text-gray-500 py-8">No activity logs found</div>
                        ) : (
                            activityLogs.map((log) => {
                                const isHighRisk = HIGH_RISK_ACTIONS.some(action =>
                                    log.action.toUpperCase().includes(action)
                                ) || log.is_high_risk;
                                const ActionIcon = getActionIcon(log.action);

                                return (
                                    <div
                                        key={log.log_id}
                                        onClick={() => onViewDetail?.(log.log_id)}
                                        className={`flex items-center justify-between p-3 rounded-lg border transition-colors cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-750 ${isHighRisk
                                                ? 'bg-red-50 border-red-200 dark:bg-red-950 dark:border-red-800'
                                                : log.status === 'FAILED'
                                                    ? 'bg-orange-50 border-orange-200 dark:bg-orange-950 dark:border-orange-800'
                                                    : 'bg-white border-gray-200 dark:bg-gray-800 dark:border-gray-700'
                                            }`}
                                    >
                                        <div className="flex items-center gap-3">
                                            <div className={`p-2 rounded-full ${isHighRisk ? 'bg-red-100 dark:bg-red-900' : 'bg-gray-100 dark:bg-gray-700'}`}>
                                                {isHighRisk ? (
                                                    <Lock className="w-4 h-4 text-red-600" />
                                                ) : (
                                                    <ActionIcon className="w-4 h-4 text-gray-600 dark:text-gray-400" />
                                                )}
                                            </div>
                                            <div>
                                                <div className="flex items-center gap-2">
                                                    <span className="font-medium text-gray-900 dark:text-white text-sm">
                                                        {log.action.replace(/_/g, ' ')}
                                                    </span>
                                                    {isHighRisk && (
                                                        <AlertTriangle className="w-4 h-4 text-red-500" />
                                                    )}
                                                </div>
                                                <div className="text-xs text-gray-500">
                                                    {log.email} • {log.role} • {log.module}
                                                    {log.resource && ` • ${log.resource}`}
                                                </div>
                                            </div>
                                        </div>
                                        <div className="flex items-center gap-3">
                                            {getStatusBadge(log.status)}
                                            <div className="text-xs text-gray-500 font-mono">
                                                {formatLatency(log.latency_ms)}
                                            </div>
                                            <div className="text-xs text-gray-500">
                                                {new Date(log.created_at).toLocaleString('vi-VN')}
                                            </div>
                                            <Eye className="w-4 h-4 text-gray-400" />
                                        </div>
                                    </div>
                                );
                            })
                        )}
                    </div>
                </CardContent>
            </Card>
        </div>
    );
}
