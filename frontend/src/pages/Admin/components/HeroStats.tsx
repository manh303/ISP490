import { Users, Bot, Database, TrendingUp } from "lucide-react";
import { Card, CardContent } from "../../../components/ui/figma/card";

const stats = [
  {
    label: "Total Users",
    value: "12,453",
    change: "+12%",
    icon: Users,
    color: "text-blue-600 dark:text-blue-400",
    bgColor: "bg-blue-50 dark:bg-blue-950",
  },
  {
    label: "Active Crawlers",
    value: "847",
    change: "+8%",
    icon: Bot,
    color: "text-purple-600 dark:text-purple-400",
    bgColor: "bg-purple-50 dark:bg-purple-950",
  },
  {
    label: "Data Volume",
    value: "2.4 TB",
    change: "+23%",
    icon: Database,
    color: "text-green-600 dark:text-green-400",
    bgColor: "bg-green-50 dark:bg-green-950",
  },
  {
    label: "System Health",
    value: "98.5%",
    change: "Optimal",
    icon: TrendingUp,
    color: "text-orange-600 dark:text-orange-400",
    bgColor: "bg-orange-50 dark:bg-orange-950",
  },
];

export function HeroStats() {
  return (
    <div className="bg-gradient-to-br from-blue-50 to-purple-50 dark:from-gray-900 dark:to-gray-800 border-b dark:border-gray-800">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
        <div className="text-center mb-8">
          <h1 className="text-gray-900 dark:text-white mb-2">Welcome Admin</h1>
          <p className="text-gray-600 dark:text-gray-400">
            Here's what's happening with your system today
          </p>
        </div>

        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
          {stats.map((stat) => {
            const Icon = stat.icon;
            return (
              <Card key={stat.label} className="dark:bg-gray-950 dark:border-gray-800">
                <CardContent className="p-6">
                  <div className="flex items-center justify-between mb-4">
                    <div className={`${stat.bgColor} ${stat.color} p-3 rounded-lg`}>
                      <Icon className="w-5 h-5" />
                    </div>
                    <span className="text-green-600 dark:text-green-400 text-sm">
                      {stat.change}
                    </span>
                  </div>
                  <div className="text-2xl text-gray-900 dark:text-white mb-1">
                    {stat.value}
                  </div>
                  <div className="text-sm text-gray-600 dark:text-gray-400">
                    {stat.label}
                  </div>
                </CardContent>
              </Card>
            );
          })}
        </div>
      </div>
    </div>
  );
}
