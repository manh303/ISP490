import { TrendingUp, Bell, Database, FileText } from "lucide-react";
import { Card } from "../../../components/ui/figma/card";
import { marketStats } from "../data/mockData";

const iconMap = {
  "trending-up": TrendingUp,
  "bell": Bell,
  "database": Database,
  "file-text": FileText
};

export function Hero() {
  return (
    <section className="container mx-auto px-4 py-12">
      <div className="text-center mb-12">
        <h1 className="mb-4">Market Overview</h1>
        <p className="text-muted-foreground max-w-2xl mx-auto">
          Real-time insights and analytics to drive your business decisions forward
        </p>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        {marketStats.map((stat) => {
          const Icon = iconMap[stat.icon as keyof typeof iconMap];
          return (
            <Card key={stat.label} className="p-6">
              <div className="flex items-start justify-between mb-4">
                <div className={`p-2 rounded-lg ${
                  stat.change === "up" 
                    ? "bg-green-500/10 text-green-500" 
                    : stat.change === "down"
                    ? "bg-red-500/10 text-red-500"
                    : "bg-blue-500/10 text-blue-500"
                }`}>
                  <Icon className="w-4 h-4" />
                </div>
              </div>
              <div className="space-y-1">
                <p className="text-muted-foreground text-sm">{stat.label}</p>
                <p className="text-3xl">{stat.value}</p>
              </div>
            </Card>
          );
        })}
      </div>
    </section>
  );
}
