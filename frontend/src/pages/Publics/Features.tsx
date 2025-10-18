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
      title: "Phân Tích Kịch Bản",
      description: "Mô phỏng và đánh giá nhiều kịch bản kinh doanh khác nhau để tìm ra giải pháp tối ưu nhất cho doanh nghiệp của bạn.",
      color: "blue",
      page: "scenario" as Page,
    },
    {
      icon: DollarSign,
      title: "Báo Cáo Doanh Thu Thời Gian Thực",
      description: "Theo dõi và phân tích doanh thu trực tiếp với các biểu đồ tương tác, giúp bạn nắm bắt tình hình kinh doanh ngay lập tức.",
      color: "red",
      page: "revenue" as Page,
    },
    {
      icon: TrendingUp,
      title: "Dự Báo Xu Hướng Thị Trường",
      description: "Sử dụng AI và machine learning để dự đoán xu hướng thị trường, giúp doanh nghiệp chủ động trong chiến lược.",
      color: "yellow",
      page: "forecast" as Page,
    },
    {
      icon: Settings,
      title: "Tối Ưu Hóa Vận Hành",
      description: "Phân tích hiệu suất vận hành và đưa ra các khuyến nghị để cải thiện quy trình, tiết kiệm chi phí và nâng cao năng suất.",
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
        <div className="text-center mb-16">
          <h2 className="text-gray-900 mb-4">
            Các Tính Năng Nổi Bật
          </h2>
          <p className="text-gray-600 text-xl max-w-3xl mx-auto">
            Giải pháp toàn diện giúp doanh nghiệp đưa ra quyết định thông minh dựa trên dữ liệu
          </p>
        </div>

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
                      Tìm hiểu thêm
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
