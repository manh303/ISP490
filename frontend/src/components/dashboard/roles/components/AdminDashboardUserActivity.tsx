import React from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../../../ui/figma/card';
import { Users, Activity } from 'lucide-react';

interface ActivityLog {
  log_id: number;
  action: string;
  email: string;
  created_at: string;
}

interface AdminDashboardUserActivityProps {
  activityLogs: ActivityLog[];
}

export default function AdminDashboardUserActivity({
  activityLogs,
}: AdminDashboardUserActivityProps) {
  console.log('AdminDashboardUserActivity - activityLogs:', activityLogs);

  // Calculate summary stats
  const totalActivities = activityLogs.length;
  const uniqueActions = new Set(activityLogs.map(log => log.action)).size;
  const latestActivity = activityLogs.length > 0 ? new Date(activityLogs[0].created_at).toLocaleDateString() : 'N/A';

  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
      {/* User Summary */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Users className="w-4 h-4" />
            Tóm tắt người dùng
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-2">
            <div className="flex justify-between">
              <span className="text-sm text-gray-600">Tổng hoạt động:</span>
              <span className="text-sm font-medium">{totalActivities}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-sm text-gray-600">Hành động khác nhau:</span>
              <span className="text-sm font-medium">{uniqueActions}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-sm text-gray-600">Hoạt động gần nhất:</span>
              <span className="text-sm font-medium">{latestActivity}</span>
            </div>
          </div>
          <div className="mt-4 text-xs text-gray-500">
            Lưu ý: Dữ liệu dựa trên nhật ký hoạt động hệ thống. Thông tin người dùng có thể không khả dụng.
          </div>
        </CardContent>
      </Card>

      {/* Recent Activity */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Activity className="w-4 h-4" />
            Hoạt động gần đây
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-2">
            {activityLogs.slice(0, 5).map((log) => (
              <div key={log.log_id} className="flex justify-between items-center py-2 border-b last:border-b-0">
                <div>
                  <div className="text-sm font-medium">{log.action?.replace(/_/g, ' ')}</div>
                  <div className="text-xs text-gray-500">{log.email}</div>
                </div>
                <div className="text-xs text-gray-500">
                  {new Date(log.created_at).toLocaleDateString()}
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>
    </div>
  );
}