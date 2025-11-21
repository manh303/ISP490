import React from "react";
import { Activity } from 'lucide-react';
import ActivityLogsTable from "../../components/admin/ActivityLogsTable";

export default function ActivityLogsPage() {
  return (
    <div className="activity-logs-page p-6">
      <div className="flex items-center gap-3 mb-6">
        <Activity className="h-8 w-8 text-blue-600" />
        <h1 className="text-3xl font-bold">Activity Logs</h1>
      </div>
      
      <div className="bg-white rounded-lg shadow-sm p-6">
        <ActivityLogsTable />
      </div>
    </div>
  );
}