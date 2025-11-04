import { Users, Settings, FileText, ArrowRight } from "lucide-react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "../../../components/ui/figma/card";
import { Button } from "../../../components/ui/figma/button";

const quickAccessItems = [
  {
    title: "Manage Users",
    description: "Add, edit, or remove user accounts",
    icon: Users,
    color: "text-blue-600 dark:text-blue-400",
    bgColor: "bg-blue-50 dark:bg-blue-950",
  },
  {
    title: "Configure Crawlers",
    description: "Set up and manage web crawlers",
    icon: Settings,
    color: "text-purple-600 dark:text-purple-400",
    bgColor: "bg-purple-50 dark:bg-purple-950",
  },
  {
    title: "View Logs",
    description: "Monitor system activity and errors",
    icon: FileText,
    color: "text-green-600 dark:text-green-400",
    bgColor: "bg-green-50 dark:bg-green-950",
  },
];

export function QuickAccessCards() {
  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <h2 className="text-gray-900 dark:text-white mb-6">Quick Access</h2>
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        {quickAccessItems.map((item) => {
          const Icon = item.icon;
          return (
            <Card key={item.title} className="dark:bg-gray-950 dark:border-gray-800 hover:shadow-lg transition-shadow cursor-pointer">
              <CardHeader>
                <div className={`${item.bgColor} ${item.color} w-12 h-12 rounded-lg flex items-center justify-center mb-4`}>
                  <Icon className="w-6 h-6" />
                </div>
                <CardTitle className="dark:text-white">{item.title}</CardTitle>
                <CardDescription className="dark:text-gray-400">
                  {item.description}
                </CardDescription>
              </CardHeader>
              <CardContent>
                <Button variant="ghost" className="w-full justify-between group">
                  Access
                  <ArrowRight className="w-4 h-4 group-hover:translate-x-1 transition-transform" />
                </Button>
              </CardContent>
            </Card>
          );
        })}
      </div>
    </div>
  );
}
