import React, { useState } from "react";
import { BarChart3 } from 'lucide-react';
import ActivityStatsChart from "../../components/admin/ActivityStatsChart";
import { Button } from '../../components/ui/figma/button';
import { Input } from '../../components/ui/figma/input';

export default function ActivityStatsPage() {
  const [days, setDays] = useState(7);

  return (
    <div className="activity-stats-page p-6">
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-3">
          <BarChart3 className="h-8 w-8 text-blue-600" />
          <h1 className="text-3xl font-bold">Activity Statistics</h1>
        </div>
        
        <div className="flex items-center gap-2">
          <label htmlFor="days" className="text-sm font-medium">Days:</label>
          <Input
            id="days"
            type="number"
            min={1}
            max={365}
            value={days}
            onChange={(e) => setDays(Number(e.target.value))}
            className="w-20"
          />
        </div>
      </div>
      
      <div className="bg-white rounded-lg shadow-sm p-6">
        <ActivityStatsChart days={days} />
      </div>
    </div>
  );
}