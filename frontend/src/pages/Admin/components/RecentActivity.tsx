import { Card, CardContent, CardHeader, CardTitle } from "../../../components/ui/figma/card";
import { Badge } from "../../../components/ui/figma/badge";
import { Avatar, AvatarFallback } from "../../../components/ui/figma/avatar";
import { UserPlus, Settings, AlertCircle, CheckCircle } from "lucide-react";

const activities = [
  {
    id: 1,
    type: "user",
    message: "New user registered: john.doe@example.com",
    timestamp: "5 minutes ago",
    icon: UserPlus,
    status: "success",
  },
  {
    id: 2,
    type: "crawler",
    message: "Crawler 'ProductData-01' completed successfully",
    timestamp: "12 minutes ago",
    icon: CheckCircle,
    status: "success",
  },
  {
    id: 3,
    type: "system",
    message: "System configuration updated",
    timestamp: "1 hour ago",
    icon: Settings,
    status: "info",
  },
  {
    id: 4,
    type: "alert",
    message: "High memory usage detected on server-02",
    timestamp: "2 hours ago",
    icon: AlertCircle,
    status: "warning",
  },
  {
    id: 5,
    type: "crawler",
    message: "Crawler 'MarketAnalysis-03' started",
    timestamp: "3 hours ago",
    icon: CheckCircle,
    status: "success",
  },
];

const statusColors = {
  success: "bg-green-100 text-green-800 dark:bg-green-950 dark:text-green-400",
  info: "bg-blue-100 text-blue-800 dark:bg-blue-950 dark:text-blue-400",
  warning: "bg-orange-100 text-orange-800 dark:bg-orange-950 dark:text-orange-400",
};

export function RecentActivity() {
  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <Card className="dark:bg-gray-950 dark:border-gray-800">
        <CardHeader>
          <CardTitle className="dark:text-white">Recent Activity</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            {activities.map((activity) => {
              const Icon = activity.icon;
              return (
                <div
                  key={activity.id}
                  className="flex items-start gap-4 pb-4 border-b last:border-0 dark:border-gray-800"
                >
                  <Avatar className="w-10 h-10">
                    <AvatarFallback className={statusColors[activity.status as keyof typeof statusColors]}>
                      <Icon className="w-5 h-5" />
                    </AvatarFallback>
                  </Avatar>
                  <div className="flex-1 min-w-0">
                    <p className="text-gray-900 dark:text-white">
                      {activity.message}
                    </p>
                    <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
                      {activity.timestamp}
                    </p>
                  </div>
                  <Badge
                    variant="secondary"
                    className={statusColors[activity.status as keyof typeof statusColors]}
                  >
                    {activity.type}
                  </Badge>
                </div>
              );
            })}
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
