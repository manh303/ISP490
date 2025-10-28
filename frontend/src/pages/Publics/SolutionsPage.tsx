import { Header } from "./Header";
import { Footer } from "./Footer";
import { GitBranch, DollarSign, TrendingUp, Settings, CheckCircle, ArrowRight, Sparkles, BarChart3, Users, Zap } from "lucide-react";
import { Card } from "../../components/ui/figma/card";
import { Button } from "../../components/ui/figma/button";
import { Badge } from "../../components/ui/figma/badge";
// import { ImageWithFallback } from "../../components/figma/ImageWithFallback";
import type { Page } from "../../App";

interface SolutionsPageProps {
  navigateTo: (page: Page) => void;
  isLoggedIn: boolean;
  onLogout: () => void;
}

export function SolutionsPage({ navigateTo, isLoggedIn, onLogout }: SolutionsPageProps) {
  const solutions = [
    {
      icon: GitBranch,
      title: "Phân Tích Kịch Bản",
      description: "Mô phỏng và đánh giá nhiều kịch bản kinh doanh khác nhau để tìm ra giải pháp tối ưu nhất.",
      color: "blue",
      features: [
        "Mô phỏng đa kịch bản",
        "Phân tích rủi ro chi tiết",
        "So sánh ROI tự động",
        "Báo cáo trực quan",
      ],
      page: "scenario" as Page,
    },
    {
      icon: DollarSign,
      title: "Báo Cáo Doanh Thu",
      description: "Theo dõi và phân tích doanh thu trực tiếp với các biểu đồ tương tác và insights thông minh.",
      color: "red",
      features: [
        "Cập nhật theo thời gian thực",
        "Phân tích đa chiều",
        "Cảnh báo xu hướng",
        "Tích hợp đa nguồn",
      ],
      page: "revenue" as Page,
    },
    {
      icon: TrendingUp,
      title: "Dự Báo Xu Hướng",
      description: "Sử dụng AI và machine learning để dự đoán xu hướng thị trường với độ chính xác cao.",
      color: "yellow",
      features: [
        "AI dự báo thông minh",
        "Độ chính xác 95%+",
        "Phân tích yếu tố ảnh hưởng",
        "Khuyến nghị chiến lược",
      ],
      page: "forecast" as Page,
    },
    {
      icon: Settings,
      title: "Tối Ưu Vận Hành",
      description: "Phân tích hiệu suất vận hành và đưa ra khuyến nghị để cải thiện quy trình và năng suất.",
      color: "green",
      features: [
        "Phát hiện điểm nghẽn",
        "Tối ưu quy trình tự động",
        "Giảm chi phí vận hành",
        "Tăng năng suất 30%+",
      ],
      page: "operation" as Page,
    },
  ];

  const benefits = [
    {
      icon: Sparkles,
      title: "Công Nghệ AI Tiên Tiến",
      description: "Sử dụng machine learning và AI để phân tích dữ liệu và đưa ra dự báo chính xác",
    },
    {
      icon: BarChart3,
      title: "Trực Quan Hóa Dữ Liệu",
      description: "Dashboard và biểu đồ trực quan giúp bạn hiểu rõ dữ liệu trong nháy mắt",
    },
    {
      icon: Users,
      title: "Dễ Sử Dụng",
      description: "Giao diện thân thiện, không cần chuyên môn kỹ thuật để sử dụng hiệu quả",
    },
    {
      icon: Zap,
      title: "Tích Hợp Linh Hoạt",
      description: "Kết nối dễ dàng với các hệ thống hiện có của doanh nghiệp",
    },
  ];

  const getColorClasses = (color: string) => {
    const colors = {
      blue: "from-blue-500 to-blue-600",
      red: "from-red-500 to-red-600",
      yellow: "from-yellow-500 to-yellow-600",
      green: "from-green-500 to-green-600",
    };
    return colors[color as keyof typeof colors];
  };

  return (
    <div className="min-h-screen bg-white">
   
      {/* Hero Section */}
      <section className="relative py-20 bg-gradient-to-br from-blue-600 to-blue-700 text-white overflow-hidden">
        <div className="absolute inset-0 opacity-10">
          <div className="absolute inset-0" style={{
            backgroundImage: 'url("data:image/svg+xml,%3Csvg width="60" height="60" viewBox="0 0 60 60" xmlns="http://www.w3.org/2000/svg"%3E%3Cg fill="none" fill-rule="evenodd"%3E%3Cg fill="%23ffffff" fill-opacity="1"%3E%3Cpath d="M36 34v-4h-2v4h-4v2h4v4h2v-4h4v-2h-4zm0-30V0h-2v4h-4v2h4v4h2V6h4V4h-4zM6 34v-4H4v4H0v2h4v4h2v-4h4v-2H6zM6 4V0H4v4H0v2h4v4h2V6h4V4H6z"/%3E%3C/g%3E%3C/g%3E%3C/svg%3E")',
          }} />
        </div>
        
        <div className="max-w-7xl mx-auto px-6 relative">
          <div className="text-center max-w-3xl mx-auto">
            <Badge className="bg-white/20 text-white border-white/30 mb-6">
              Giải Pháp Toàn Diện
            </Badge>
            <h1 className="text-white mb-6">
              Giải Pháp DSS Cho
              <br />
              Mọi Nhu Cầu Doanh Nghiệp
            </h1>
            <p className="text-blue-100 text-xl mb-8">
              Hệ thống module mạnh mẽ giúp bạn phân tích, dự báo và tối ưu hóa 
              mọi khía cạnh của doanh nghiệp
            </p>
            <Button 
              size="lg"
              className="bg-white text-blue-600 hover:bg-blue-50 gap-2"
              onClick={() => navigateTo(isLoggedIn ? "dashboard" : "login")}
            >
              Bắt Đầu Miễn Phí
              <ArrowRight className="w-5 h-5" />
            </Button>
          </div>
        </div>
      </section>

      {/* Solutions Grid */}
      <section className="py-20 bg-gray-50">
        <div className="max-w-7xl mx-auto px-6">
          <div className="text-center mb-16">
            <h2 className="text-gray-900 mb-4">
              4 Module Chính
            </h2>
            <p className="text-gray-600 text-lg max-w-3xl mx-auto">
              Mỗi module được thiết kế riêng để giải quyết những thách thức cụ thể của doanh nghiệp
            </p>
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
            {solutions.map((solution) => {
              const Icon = solution.icon;
              
              return (
                <Card key={solution.title} className="overflow-hidden hover:shadow-xl transition-shadow">
                  <div className={`h-2 bg-gradient-to-r ${getColorClasses(solution.color)}`} />
                  <div className="p-8">
                    <div className="flex items-start gap-4 mb-6">
                      <div className={`p-4 rounded-xl bg-gradient-to-br ${getColorClasses(solution.color)} text-white`}>
                        <Icon className="w-8 h-8" />
                      </div>
                      <div className="flex-1">
                        <h3 className="text-gray-900 mb-3">
                          {solution.title}
                        </h3>
                        <p className="text-gray-600">
                          {solution.description}
                        </p>
                      </div>
                    </div>

                    <div className="space-y-3 mb-6">
                      {solution.features.map((feature) => (
                        <div key={feature} className="flex items-center gap-3">
                          <CheckCircle className="w-5 h-5 text-green-600 flex-shrink-0" />
                          <span className="text-gray-700">{feature}</span>
                        </div>
                      ))}
                    </div>

                    <Button 
                      className="w-full gap-2"
                      onClick={() => navigateTo(isLoggedIn ? solution.page : "login")}
                    >
                      Trải Nghiệm Module
                      <ArrowRight className="w-4 h-4" />
                    </Button>
                  </div>
                </Card>
              );
            })}
          </div>
        </div>
      </section>

      {/* Benefits Section */}
      <section className="py-20 bg-white">
        <div className="max-w-7xl mx-auto px-6">
          <div className="text-center mb-16">
            <h2 className="text-gray-900 mb-4">
              Tại Sao Chọn DSS Analytics?
            </h2>
            <p className="text-gray-600 text-lg max-w-3xl mx-auto">
              Những lợi thế vượt trội giúp doanh nghiệp của bạn thành công
            </p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-8">
            {benefits.map((benefit) => {
              const Icon = benefit.icon;
              return (
                <Card key={benefit.title} className="p-6 text-center hover:shadow-lg transition-shadow">
                  <div className="bg-blue-100 w-16 h-16 rounded-full flex items-center justify-center mx-auto mb-4">
                    <Icon className="w-8 h-8 text-blue-600" />
                  </div>
                  <h3 className="text-gray-900 mb-3">
                    {benefit.title}
                  </h3>
                  <p className="text-gray-600 text-sm">
                    {benefit.description}
                  </p>
                </Card>
              );
            })}
          </div>
        </div>
      </section>

      {/* CTA Section */}
      <section className="py-20 bg-gradient-to-br from-blue-600 to-blue-700 text-white">
        <div className="max-w-4xl mx-auto px-6 text-center">
          <h2 className="text-white mb-6">
            Sẵn Sàng Chuyển Đổi Doanh Nghiệp?
          </h2>
          <p className="text-blue-100 text-xl mb-8">
            Bắt đầu hành trình số hóa với DSS Analytics ngay hôm nay
          </p>
          <div className="flex gap-4 justify-center">
            <Button 
              size="lg"
              className="bg-white text-blue-600 hover:bg-blue-50 gap-2"
              onClick={() => navigateTo("login")}
            >
              Đăng Ký Dùng Thử
              <ArrowRight className="w-5 h-5" />
            </Button>
            <Button 
              size="lg"
              variant="outline"
              className="border-white text-white hover:bg-white/10"
              onClick={() => navigateTo("contact")}
            >
              Liên Hệ Tư Vấn
            </Button>
          </div>
        </div>
      </section>


    </div>
  );
}
