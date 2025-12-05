import { GitBranch, DollarSign, TrendingUp, Settings, ArrowRight } from "lucide-react";
import { Card } from "../../components/ui/figma/card";
import { Button } from "../../components/ui/figma/button";
import type { Page } from "../../App";

interface FeaturesProps {
  navigateTo: (page: Page) => void;
}

export function Features({ navigateTo }: FeaturesProps) {
  const features = [
    {
      icon: GitBranch,
      title: "Scenario Analysis",
      description: "Simulate and evaluate different business scenarios to find the optimal solution for your business.",
      color: "blue",
      page: "scenario" as Page,
    },
    {
      icon: DollarSign,
      title: "Real-time Revenue Reports",
      description: "Monitor and analyze revenue in real-time with interactive charts, helping you grasp business situation instantly.",
      color: "red",
      page: "revenue" as Page,
    },
    {
      icon: TrendingUp,
      title: "Market Trend Forecasting",
      description: "Use AI and machine learning to predict market trends, helping businesses be proactive in their strategies.",
      color: "yellow",
      page: "forecast" as Page,
    },
    {
      icon: Settings,
      title: "Operations Optimization",
      description: "Analyze operational performance and provide recommendations to improve processes, save costs, and increase productivity.",
      color: "green",
      page: "operation" as Page,
    },
  ];

  const getColorClasses = (color: string) => {
    const colors = {
      blue: {
        icon: "bg-blue-100 text-blue-600",
        button: "bg-blue-50 text-blue-700 hover:bg-blue-100",
      },
      red: {
        icon: "bg-red-100 text-red-600",
        button: "bg-red-50 text-red-700 hover:bg-red-100",
      },
      yellow: {
        icon: "bg-yellow-100 text-yellow-600",
        button: "bg-yellow-50 text-yellow-700 hover:bg-yellow-100",
      },
      green: {
        icon: "bg-green-100 text-green-600",
        button: "bg-green-50 text-green-700 hover:bg-green-100",
      },
    };
    return colors[color as keyof typeof colors];
  };

  return (
    <section className="py-20 bg-gray-50">
      <div className="max-w-7xl mx-auto px-6">
        {/* Section Title */}
        {/* <div className="text-center mb-16">
          <h2 className="text-gray-900 mb-4">
            Các Tính Năng Nổi Bật
          </h2>
          <p className="text-gray-600 text-xl max-w-3xl mx-auto">
            Giải pháp toàn diện giúp doanh nghiệp đưa ra quyết định thông minh dựa trên dữ liệu
          </p>
        </div> */}

        {/* Features Grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
          {features.map((feature) => {
            const Icon = feature.icon;
            const colors = getColorClasses(feature.color);
            
            return (
              <Card key={feature.title} className="p-8 hover:shadow-lg transition-shadow">
                <div className="flex items-start gap-6">
                  <div className={`p-4 rounded-xl ${colors.icon} flex-shrink-0`}>
                    <Icon className="w-8 h-8" />
                  </div>
                  
                  <div className="flex-1">
                    <h3 className="text-gray-900 mb-3">
                      {feature.title}
                    </h3>
                    <p className="text-gray-600 mb-4">
                      {feature.description}
                    </p>
                    <Button 
                      variant="ghost"
                      className={`gap-2 ${colors.button}`}
                      onClick={() => navigateTo(feature.page)}
                    >
                      Learn More
                      <ArrowRight className="w-4 h-4" />
                    </Button>
                  </div>
                </div>
              </Card>
            );
          })}
        </div>
      </div>
    </section>
  );
}
