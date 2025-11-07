import { Card } from "../../../components/ui/figma/card";
import { Badge } from "../../../components/ui/figma/badge";
import { Button } from "../../../components/ui/figma/button";
import { AlertCircle, AlertTriangle, Info, Download, BarChart3 } from "lucide-react";
import { alerts } from "../data/mockData";

const alertConfig = {
  critical: {
    icon: AlertCircle,
    variant: "destructive" as const,
    color: "text-red-500"
  },
  warning: {
    icon: AlertTriangle,
    variant: "default" as const,
    color: "text-yellow-500"
  },
  info: {
    icon: Info,
    variant: "secondary" as const,
    color: "text-blue-500"
  }
};

export function AlertSummary() {
  return (
    <section className="container mx-auto px-4 py-12 bg-muted/30">
      <div className="mb-8">
        <h2 className="mb-2">Alert Summary</h2>
        <p className="text-muted-foreground">
          Critical updates and notifications requiring attention
        </p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="lg:col-span-2">
          <Card className="p-6">
            <div className="space-y-4">
              {alerts.map((alert) => {
                const config = alertConfig[alert.type as keyof typeof alertConfig];
                const Icon = config.icon;
                
                return (
                  <div key={alert.id} className="flex items-start gap-4 pb-4 border-b last:border-b-0 last:pb-0">
                    <div className={`p-2 rounded-lg bg-background ${config.color}`}>
                      <Icon className="w-4 h-4" />
                    </div>
                    <div className="flex-1 min-w-0">
                      <p className="mb-1">{alert.message}</p>
                      <p className="text-xs text-muted-foreground">{alert.timestamp}</p>
                    </div>
                    <Badge variant={config.variant} className="shrink-0">
                      {alert.type}
                    </Badge>
                  </div>
                );
              })}
            </div>
          </Card>
        </div>

        <div className="space-y-4">
          <Card className="p-6">
            <h3 className="mb-4">Quick Actions</h3>
            <div className="space-y-3">
              <Button className="w-full justify-start gap-2">
                <BarChart3 className="w-4 h-4" />
                View Dashboard
              </Button>
              <Button variant="outline" className="w-full justify-start gap-2">
                <Download className="w-4 h-4" />
                Export Report
              </Button>
            </div>
          </Card>

          <Card className="p-6 bg-gradient-to-br from-blue-500/10 to-purple-600/10 border-blue-500/20">
            <h3 className="mb-2">Premium Analytics</h3>
            <p className="text-sm text-muted-foreground mb-4">
              Unlock advanced insights with AI-powered predictions
            </p>
            <Button variant="outline" size="sm" className="w-full">
              Learn More
            </Button>
          </Card>
        </div>
      </div>
    </section>
  );
}
