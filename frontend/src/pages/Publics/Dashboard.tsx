import {
  BarChart3,
  LogOut,
  GitBranch,
  DollarSign,
  TrendingUp,
  Settings,
  ArrowRight,
  Activity,
  Users,
  ShoppingCart,
  TrendingDown,
} from "lucide-react";
import { Button } from "../../components/ui/figma/button";
import { Card } from "../../components/ui/figma/card";
import type { Page } from "../../App";

interface DashboardProps {
  navigateTo: (page: Page) => void;
  onLogout: () => void;
}

export function Dashboard({
  navigateTo,
  onLogout,
}: DashboardProps) {
  const modules = [
    {
      icon: GitBranch,
      title: "Scenario Analysis",
      description:
        "Simulate and evaluate business scenarios",
      color: "blue",
      page: "scenario" as Page,
      stats: "12 active scenarios",
    },
    {
      icon: DollarSign,
      title: "Revenue Report",
      description: "Monitor real-time revenue",
      color: "red",
      page: "revenue" as Page,
      stats: "2.4M VNĐ today",
    },
    {
      icon: TrendingUp,
      title: "Trend Forecast",
      description: "Predict market trends with AI",
      color: "yellow",
      page: "forecast" as Page,
      stats: "95% accuracy",
    },
    {
      icon: Settings,
      title: "Operation Optimization",
      description: "Improve performance and processes",
      color: "green",
      page: "operation" as Page,
      stats: "18% improvement",
    },
  ];

  const quickStats = [
    {
      label: "Total Revenue",
      value: "48.5M",
      change: "+12.5%",
      trend: "up",
      icon: DollarSign,
    },
    {
      label: "Customers",
      value: "1,234",
      change: "+8.2%",
      trend: "up",
      icon: Users,
    },
    {
      label: "Orders",
      value: "856",
      change: "-2.3%",
      trend: "down",
      icon: ShoppingCart,
    },
    {
      label: "Activity",
      value: "92%",
      change: "+5.1%",
      trend: "up",
      icon: Activity,
    },
  ];

  const getColorClasses = (color: string) => {
    const colors = {
      blue: {
        bg: "bg-gradient-to-br from-blue-500 to-blue-600 hover:from-blue-600 hover:to-blue-700",
        icon: "bg-blue-100 text-blue-600",
        text: "text-blue-600",
      },
      red: {
        bg: "bg-gradient-to-br from-red-500 to-red-600 hover:from-red-600 hover:to-red-700",
        icon: "bg-red-100 text-red-600",
        text: "text-red-600",
      },
      yellow: {
        bg: "bg-gradient-to-br from-yellow-500 to-yellow-600 hover:from-yellow-600 hover:to-yellow-700",
        icon: "bg-yellow-100 text-yellow-600",
        text: "text-yellow-600",
      },
      green: {
        bg: "bg-gradient-to-br from-green-500 to-green-600 hover:from-green-600 hover:to-green-700",
        icon: "bg-green-100 text-green-600",
        text: "text-green-600",
      },
    };
    return colors[color as keyof typeof colors];
  };

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      {/* <header className="bg-white border-b border-gray-200 shadow-sm">
        <div className="max-w-7xl mx-auto px-6 h-20 flex items-center justify-between">
          <div className="flex items-center gap-2">
            <div className="bg-gradient-to-br from-blue-600 to-blue-700 p-2 rounded-lg">
              <BarChart3 className="w-6 h-6 text-white" />
            </div>
            <span className="text-gray-900">DSS Analytics</span>
          </div>

          <div className="flex items-center gap-4">
            <Button
              variant="outline"
              onClick={() => navigateTo("home")}
            >
              Home
            </Button>
            <Button
              variant="ghost"
              onClick={onLogout}
              className="gap-2"
            >
              <LogOut className="w-4 h-4" />
              Logout
            </Button>
          </div>
        </div>
      </header> */}

      {/* Main Content */}
      <main className="max-w-7xl mx-auto px-6 py-8">
        {/* Welcome Section */}
        <div className="mb-8">
          <h1 className="text-gray-900 mb-2">
            Welcome back! 👋
          </h1>
          <p className="text-gray-600">
            This is an overview of your business metrics and system modules
          </p>
        </div>

        {/* Quick Stats */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
          {quickStats.map((stat) => {
            const Icon = stat.icon;
            const isPositive = stat.trend === "up";

            return (
              <Card key={stat.label} className="p-6">
                <div className="flex items-start justify-between mb-4">
                  <div
                    className={`p-3 rounded-lg ${isPositive ? "bg-green-100" : "bg-red-100"}`}
                  >
                    <Icon
                      className={`w-6 h-6 ${isPositive ? "text-green-600" : "text-red-600"}`}
                    />
                  </div>
                  <span
                    className={`text-sm ${isPositive ? "text-green-600" : "text-red-600"} flex items-center gap-1`}
                  >
                    {isPositive ? (
                      <TrendingUp className="w-4 h-4" />
                    ) : (
                      <TrendingDown className="w-4 h-4" />
                    )}
                    {stat.change}
                  </span>
                </div>
                <p className="text-gray-600 text-sm mb-1">
                  {stat.label}
                </p>
                <p className="text-gray-900">{stat.value}</p>
              </Card>
            );
          })}
        </div>

        {/* Modules Section */}
        <div className="mb-8">
          <h2 className="text-gray-900 mb-6">
            System Modules
          </h2>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            {modules.map((module) => {
              const Icon = module.icon;
              const colors = getColorClasses(module.color);

              return (
                <Card
                  key={module.title}
                  className="overflow-hidden cursor-pointer group hover:shadow-xl transition-all duration-300"
                  onClick={() => navigateTo(module.page)}
                >
                  <div className={`h-2 ${colors.bg}`} />
                  <div className="p-6">
                    <div className="flex items-start gap-4 mb-4">
                      <div
                        className={`p-3 rounded-xl ${colors.icon}`}
                      >
                        <Icon className="w-6 h-6" />
                      </div>
                      <div className="flex-1">
                        <h3 className="text-gray-900 mb-2">
                          {module.title}
                        </h3>
                        <p className="text-gray-600 text-sm mb-3">
                          {module.description}
                        </p>
                        <p className={`text-sm ${colors.text}`}>
                          {module.stats}
                        </p>
                      </div>
                    </div>
                    <Button
                      className={`w-full ${colors.bg} text-white gap-2 group-hover:gap-4 transition-all`}
                    >
                      Access module
                      <ArrowRight className="w-4 h-4" />
                    </Button>
                  </div>
                </Card>
              );
            })}
          </div>
        </div>

        {/* Recent Activity */}
        <Card className="p-6">
          <h3 className="text-gray-900 mb-4">
            Recent Activity
          </h3>
          <div className="space-y-4">
            <div className="flex items-center gap-4 pb-4 border-b border-gray-100">
              <div className="w-2 h-2 bg-blue-500 rounded-full" />
              <div className="flex-1">
                <p className="text-gray-900 text-sm">
                  Scenario analysis "Market Expansion" has been completed
                </p>
                <p className="text-gray-500 text-xs">
                  2 hours ago
                </p>
              </div>
            </div>
            <div className="flex items-center gap-4 pb-4 border-b border-gray-100">
              <div className="w-2 h-2 bg-green-500 rounded-full" />
              <div className="flex-1">
                <p className="text-gray-900 text-sm">
                  Q1 revenue report has been generated
                </p>
                <p className="text-gray-500 text-xs">
                  5 hours ago
                </p>
              </div>
            </div>
            <div className="flex items-center gap-4">
              <div className="w-2 h-2 bg-yellow-500 rounded-full" />
              <div className="flex-1">
                <p className="text-gray-900 text-sm">
                  Trend forecast for next month is ready
                </p>
                <p className="text-gray-500 text-xs">
                  1 day ago
                </p>
              </div>
            </div>
          </div>
        </Card>
      </main>
    </div>
  );
}