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
  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
      {/* User Summary */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Users className="w-4 h-4" />
            User Summary
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="text-center text-gray-500">
            User summary data would go here
          </div>
        </CardContent>
      </Card>

      {/* Recent Activity */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Activity className="w-4 h-4" />
            Recent Activity
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