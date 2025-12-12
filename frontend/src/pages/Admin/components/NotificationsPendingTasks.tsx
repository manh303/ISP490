import React from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../../../components/ui/figma/card';
import { Badge } from '../../../components/ui/figma/badge';
import { Button } from '../../../components/ui/figma/button';
import {
    Bell,
    User,
    Brain,
    Activity,
    Database,
    CheckCircle,
    XCircle,
    Eye,
    AlertTriangle,
    Clock
} from 'lucide-react';
import { Link } from 'react-router-dom';

type NotificationType = 'USER' | 'DSS' | 'PIPELINE' | 'DATASET';
type NotificationPriority = 'HIGH' | 'MEDIUM' | 'LOW';

interface Notification {
    notification_id: number;
    type: NotificationType;
    title: string;
    message: string;
    priority: NotificationPriority;
    created_at: string;
    is_read: boolean;
    action_url?: string;
    requires_approval?: boolean;
    related_id?: number;
}

interface NotificationsPendingTasksProps {
    notifications: Notification[];
    onViewDetail?: (notification: Notification) => void;
    onApprove?: (notificationId: number, relatedId: number) => void;
    onReject?: (notificationId: number, relatedId: number) => void;
    onMarkAsRead?: (notificationId: number) => void;
    isLoading?: boolean;
}

const getTypeIcon = (type: NotificationType) => {
    switch (type) {
        case 'USER':
            return User;
        case 'DSS':
            return Brain;
        case 'PIPELINE':
            return Activity;
        case 'DATASET':
            return Database;
        default:
            return Bell;
    }
};

const getTypeColor = (type: NotificationType) => {
    switch (type) {
        case 'USER':
            return 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-300';
        case 'DSS':
            return 'bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-300';
        case 'PIPELINE':
            return 'bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-300';
        case 'DATASET':
            return 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300';
        default:
            return 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300';
    }
};

const getPriorityBadge = (priority: NotificationPriority) => {
    switch (priority) {
        case 'HIGH':
            return <Badge variant="destructive" className="text-xs">HIGH</Badge>;
        case 'MEDIUM':
            return <Badge className="bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-300 text-xs">MEDIUM</Badge>;
        case 'LOW':
            return <Badge variant="secondary" className="text-xs">LOW</Badge>;
        default:
            return null;
    }
};

const formatTimeAgo = (dateString: string) => {
    const date = new Date(dateString);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffMins = Math.floor(diffMs / (1000 * 60));

    if (diffMins < 1) return 'Just now';
    if (diffMins < 60) return `${diffMins} min ago`;
    const diffHours = Math.floor(diffMins / 60);
    if (diffHours < 24) return `${diffHours}h ago`;
    const diffDays = Math.floor(diffHours / 24);
    return `${diffDays}d ago`;
};

export default function NotificationsPendingTasks({
    notifications,
    onViewDetail,
    onApprove,
    onReject,
    onMarkAsRead,
    isLoading = false,
}: NotificationsPendingTasksProps) {
    if (isLoading) {
        return (
            <div className="space-y-4">
                <h2 className="text-xl font-bold text-gray-900 dark:text-white flex items-center gap-2">
                    <Bell className="w-5 h-5 text-yellow-600" />
                    Notifications & Pending Tasks
                </h2>
                <div className="animate-pulse">
                    <div className="h-64 bg-gray-200 dark:bg-gray-700 rounded"></div>
                </div>
            </div>
        );
    }

    const unreadCount = notifications.filter(n => !n.is_read).length;
    const pendingApprovalCount = notifications.filter(n => n.requires_approval).length;
    const highPriorityCount = notifications.filter(n => n.priority === 'HIGH').length;

    // Group notifications by type
    const groupedByType = notifications.reduce((acc, notif) => {
        if (!acc[notif.type]) acc[notif.type] = [];
        acc[notif.type].push(notif);
        return acc;
    }, {} as Record<NotificationType, Notification[]>);

    return (
        <div className="space-y-4">
            <div className="flex items-center justify-between">
                <h2 className="text-xl font-bold text-gray-900 dark:text-white flex items-center gap-2">
                    <Bell className="w-5 h-5 text-yellow-600" />
                    Notifications & Pending Tasks
                    {unreadCount > 0 && (
                        <Badge className="bg-red-500 text-white ml-2">{unreadCount} new</Badge>
                    )}
                </h2>
                <div className="flex gap-2">
                    {pendingApprovalCount > 0 && (
                        <Badge className="bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-300">
                            {pendingApprovalCount} pending approval
                        </Badge>
                    )}
                    {highPriorityCount > 0 && (
                        <Badge variant="destructive">
                            {highPriorityCount} high priority
                        </Badge>
                    )}
                </div>
            </div>

            <Card>
                <CardHeader>
                    <CardTitle className="text-base flex items-center justify-between">
                        <span>All Notifications</span>
                        <span className="text-sm font-normal text-gray-500">{notifications.length} total</span>
                    </CardTitle>
                </CardHeader>
                <CardContent>
                    <div className="space-y-2 max-h-96 overflow-y-auto">
                        {notifications.length === 0 ? (
                            <div className="text-center text-gray-500 py-8 flex flex-col items-center">
                                <CheckCircle className="w-8 h-8 text-green-500 mb-2" />
                                <span>No pending notifications!</span>
                            </div>
                        ) : (
                            notifications.map((notification) => {
                                const TypeIcon = getTypeIcon(notification.type);

                                return (
                                    <div
                                        key={notification.notification_id}
                                        className={`p-4 rounded-lg border transition-colors ${!notification.is_read
                                                ? 'bg-blue-50 border-blue-200 dark:bg-blue-950 dark:border-blue-800'
                                                : notification.priority === 'HIGH'
                                                    ? 'bg-red-50 border-red-200 dark:bg-red-950 dark:border-red-800'
                                                    : 'bg-white border-gray-200 dark:bg-gray-800 dark:border-gray-700'
                                            }`}
                                    >
                                        <div className="flex items-start gap-3">
                                            <div className={`p-2 rounded-full ${getTypeColor(notification.type)}`}>
                                                <TypeIcon className="w-4 h-4" />
                                            </div>

                                            <div className="flex-1 min-w-0">
                                                <div className="flex items-center gap-2 mb-1">
                                                    <Badge variant="outline" className="text-xs">{notification.type}</Badge>
                                                    {getPriorityBadge(notification.priority)}
                                                    {notification.requires_approval && (
                                                        <Badge className="bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-300 text-xs animate-pulse">
                                                            <AlertTriangle className="w-3 h-3 mr-1" />
                                                            Needs Action
                                                        </Badge>
                                                    )}
                                                    {!notification.is_read && (
                                                        <span className="w-2 h-2 bg-blue-500 rounded-full"></span>
                                                    )}
                                                </div>

                                                <div className="font-medium text-gray-900 dark:text-white text-sm">
                                                    {notification.title}
                                                </div>
                                                <div className="text-xs text-gray-600 dark:text-gray-400 mt-1">
                                                    {notification.message}
                                                </div>

                                                <div className="flex items-center justify-between mt-3">
                                                    <div className="flex items-center text-xs text-gray-500">
                                                        <Clock className="w-3 h-3 mr-1" />
                                                        {formatTimeAgo(notification.created_at)}
                                                    </div>

                                                    <div className="flex gap-2">
                                                        {notification.requires_approval && notification.related_id && (
                                                            <>
                                                                <Button
                                                                    size="sm"
                                                                    variant="default"
                                                                    className="h-7 text-xs bg-green-600 hover:bg-green-700"
                                                                    onClick={(e) => {
                                                                        e.stopPropagation();
                                                                        onApprove?.(notification.notification_id, notification.related_id!);
                                                                    }}
                                                                >
                                                                    <CheckCircle className="w-3 h-3 mr-1" />
                                                                    Approve
                                                                </Button>
                                                                <Button
                                                                    size="sm"
                                                                    variant="destructive"
                                                                    className="h-7 text-xs"
                                                                    onClick={(e) => {
                                                                        e.stopPropagation();
                                                                        onReject?.(notification.notification_id, notification.related_id!);
                                                                    }}
                                                                >
                                                                    <XCircle className="w-3 h-3 mr-1" />
                                                                    Reject
                                                                </Button>
                                                            </>
                                                        )}
                                                        <Button
                                                            size="sm"
                                                            variant="outline"
                                                            className="h-7 text-xs"
                                                            onClick={() => onViewDetail?.(notification)}
                                                        >
                                                            <Eye className="w-3 h-3 mr-1" />
                                                            View
                                                        </Button>
                                                        {!notification.is_read && (
                                                            <Button
                                                                size="sm"
                                                                variant="ghost"
                                                                className="h-7 text-xs"
                                                                onClick={(e) => {
                                                                    e.stopPropagation();
                                                                    onMarkAsRead?.(notification.notification_id);
                                                                }}
                                                            >
                                                                Mark Read
                                                            </Button>
                                                        )}
                                                    </div>
                                                </div>
                                            </div>
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
