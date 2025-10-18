import { Header } from "./Header";
import { Footer } from "./Footer";
import { Play, CheckCircle, GitBranch, DollarSign, TrendingUp, Settings, ArrowRight, Users, Building2, ShoppingBag, Award } from "lucide-react";
import { Card } from "../../components/ui/figma/card";
import { Button } from "../../components/ui/figma/button";
import { Badge } from "../../components/ui/figma/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "../../components/ui/figma/tabs";
import { ImageWithFallback } from "../../components/figma/ImageWithFallback";
import type { Page } from "../../App";

interface ExplorePageProps {
  navigateTo: (page: Page) => void;
  isLoggedIn: boolean;
  onLogout: () => void;
}

export function ExplorePage({ navigateTo, isLoggedIn, onLogout }: ExplorePageProps) {
  const demos = [
    {
      id: "scenario",
      icon: GitBranch,
      title: "Phân Tích Kịch Bản",
      description: "Mô phỏng các tình huống kinh doanh và đánh giá rủi ro",
      color: "blue",
      features: [
        "So sánh đa kịch bản",
        "Phân tích ROI chi tiết",
        "Đánh giá rủi ro tự động",
        "Báo cáo trực quan",
      ],
      page: "scenario" as Page,
    },
    {
      id: "revenue",
      icon: DollarSign,
      title: "Báo Cáo Doanh Thu",
      description: "Theo dõi doanh thu theo thời gian thực",
      color: "red",
      features: [
        "Dashboard thời gian thực",
        "Phân tích theo danh mục",
        "Biểu đồ tương tác",
        "Cảnh báo xu hướng",
      ],
      page: "revenue" as Page,
    },
    {
      id: "forecast",
      icon: TrendingUp,
      title: "Dự Báo Xu Hướng",
      description: "AI dự đoán xu hướng thị trường với độ chính xác cao",
      color: "yellow",
      features: [
        "Dự báo AI thông minh",
        "Độ chính xác 95%+",
        "Phân tích yếu tố",
        "Khuyến nghị chiến lược",
      ],
      page: "forecast" as Page,
    },
    {
      id: "operation",
      icon: Settings,
      title: "Tối Ưu Vận Hành",
      description: "Cải thiện hiệu suất và quy trình vận hành",
      color: "green",
      features: [
        "Phát hiện điểm nghẽn",
        "Tối ưu tự động",
        "Giảm chi phí 20%+",
        "Tăng năng suất",
      ],
      page: "operation" as Page,
    },
  ];

  const useCases = [
    {
      icon: Building2,
      title: "Doanh Nghiệp Lớn",
      description: "Quản lý và phân tích dữ liệu phức tạp từ nhiều chi nhánh",
      stats: "500+ nhân viên",
    },
    {
      icon: ShoppingBag,
      title: "Bán Lẻ & TMĐT",
      description: "Tối ưu hóa tồn kho, dự báo nhu cầu và phân tích khách hàng",
      stats: "1000+ đơn/ngày",
    },
    {
      icon: Users,
      title: "SME",
      description: "Giải pháp linh hoạt và hiệu quả về chi phí cho doanh nghiệp vừa và nhỏ",
      stats: "10-200 nhân viên",
    },
    {
      icon: Award,
      title: "Startup",
      description: "Công cụ phân tích mạnh mẽ giúp scale nhanh chóng",
      stats: "Gói ưu đãi đặc biệt",
    },
  ];

  const testimonials = [
    {
      name: "Nguyễn Văn A",
      position: "CEO, Công ty ABC",
      content: "DSS Analytics đã giúp chúng tôi tăng 45% hiệu quả vận hành chỉ trong 3 tháng. Công cụ phân tích cực kỳ mạnh mẽ và dễ sử dụng.",
    },
    {
      name: "Trần Thị B",
      position: "CFO, XYZ Corp",
      content: "Tính năng dự báo doanh thu giúp chúng tôi lập kế hoạch tài chính chính xác hơn rất nhiều. Độ chính xác lên đến 94%!",
    },
    {
      name: "Lê Văn C",
      position: "COO, DEF Ltd",
      content: "Module tối ưu vận hành đã tiết kiệm cho chúng tôi hơn 2 tỷ VNĐ chi phí mỗi năm. ROI rất ấn tượng!",
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
      <Header navigateTo={navigateTo} isLoggedIn={isLoggedIn} onLogout={onLogout} />
      
      {/* Hero Section */}
      <section className="relative py-20 bg-gradient-to-br from-blue-600 to-blue-700 text-white overflow-hidden">
        <div className="absolute inset-0 opacity-10">
          <div className="absolute inset-0" style={{
            backgroundImage: 'url("data:image/svg+xml,%3Csvg width="60" height="60" viewBox="0 0 60 60" xmlns="http://www.w3.org/2000/svg"%3E%3Cg fill="none" fill-rule="evenodd"%3E%3Cg fill="%23ffffff" fill-opacity="1"%3E%3Cpath d="M36 34v-4h-2v4h-4v2h4v4h2v-4h4v-2h-4zm0-30V0h-2v4h-4v2h4v4h2V6h4V4h-4zM6 34v-4H4v4H0v2h4v4h2v-4h4v-2H6zM6 4V0H4v4H0v2h4v4h2V6h4V4H6z"/%3E%3C/g%3E%3C/g%3E%3C/svg%3E")',
          }} />
        </div>
        
        <div className="max-w-7xl mx-auto px-6 relative">
          <div className="text-center max-w-3xl mx-auto">
            <Badge className="bg-white/20 text-white border-white/30 mb-6 gap-2">
              <Play className="w-4 h-4" />
              Demo Tương Tác
            </Badge>
            <h1 className="text-white mb-6">
              Khám Phá Sức Mạnh Của
              <br />
              DSS Analytics
            </h1>
            <p className="text-blue-100 text-xl mb-8">
              Trải nghiệm các tính năng mạnh mẽ và xem cách DSS Analytics có thể 
              chuyển đổi doanh nghiệp của bạn
            </p>
            <Button 
              size="lg"
              className="bg-white text-blue-600 hover:bg-blue-50 gap-2"
              onClick={() => navigateTo(isLoggedIn ? "dashboard" : "login")}
            >
              {isLoggedIn ? "Vào Dashboard" : "Đăng Nhập Để Trải Nghiệm"}
              <ArrowRight className="w-5 h-5" />
            </Button>
          </div>
        </div>
      </section>

      {/* Interactive Demo Tabs */}
      <section className="py-20 bg-gray-50">
        <div className="max-w-7xl mx-auto px-6">
          <div className="text-center mb-12">
            <h2 className="text-gray-900 mb-4">
              Các Module Có Sẵn
            </h2>
            <p className="text-gray-600 text-lg max-w-3xl mx-auto">
              Chọn module bạn muốn khám phá chi tiết
            </p>
          </div>

          <Tabs defaultValue="scenario" className="w-full">
            <TabsList className="grid w-full grid-cols-4 mb-8">
              {demos.map((demo) => (
                <TabsTrigger key={demo.id} value={demo.id}>
                  {demo.title}
                </TabsTrigger>
              ))}
            </TabsList>

            {demos.map((demo) => {
              const Icon = demo.icon;
              return (
                <TabsContent key={demo.id} value={demo.id}>
                  <Card className="overflow-hidden">
                    <div className={`h-2 bg-gradient-to-r ${getColorClasses(demo.color)}`} />
                    <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 p-8">
                      <div>
                        <div className="flex items-center gap-4 mb-6">
                          <div className={`p-4 rounded-xl bg-gradient-to-br ${getColorClasses(demo.color)} text-white`}>
                            <Icon className="w-8 h-8" />
                          </div>
                          <div>
                            <h3 className="text-gray-900 mb-1">
                              {demo.title}
                            </h3>
                            <p className="text-gray-600">{demo.description}</p>
                          </div>
                        </div>

                        <div className="space-y-4 mb-8">
                          <h4 className="text-gray-900">Tính năng chính:</h4>
                          {demo.features.map((feature) => (
                            <div key={feature} className="flex items-center gap-3">
                              <CheckCircle className="w-5 h-5 text-green-600 flex-shrink-0" />
                              <span className="text-gray-700">{feature}</span>
                            </div>
                          ))}
                        </div>

                        <Button 
                          size="lg"
                          className={`w-full bg-gradient-to-r ${getColorClasses(demo.color)} text-white gap-2`}
                          onClick={() => navigateTo(isLoggedIn ? demo.page : "login")}
                        >
                          <Play className="w-5 h-5" />
                          {isLoggedIn ? "Trải Nghiệm Ngay" : "Đăng Nhập Để Xem"}
                        </Button>
                      </div>

                      <div className="relative h-[400px] rounded-xl overflow-hidden bg-gray-100">
                        <ImageWithFallback
                          src="https://images.unsplash.com/photo-1726138388546-30955e45aaec?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3Nzg4Nzd8MHwxfHNlYXJjaHwxfHx0ZWNobm9sb2d5JTIwc29sdXRpb25zfGVufDF8fHx8MTc2MDM0NTE2NXww&ixlib=rb-4.1.0&q=80&w=1080&utm_source=figma&utm_medium=referral"
                          alt={demo.title}
                          className="w-full h-full object-cover"
                        />
                        <div className="absolute inset-0 bg-gradient-to-t from-black/60 to-transparent flex items-end p-6">
                          <div className="text-white">
                            <p className="text-sm text-blue-200 mb-1">Preview</p>
                            <p className="text-white">{demo.title}</p>
                          </div>
                        </div>
                      </div>
                    </div>
                  </Card>
                </TabsContent>
              );
            })}
          </Tabs>
        </div>
      </section>

      {/* Use Cases */}
      <section className="py-20 bg-white">
        <div className="max-w-7xl mx-auto px-6">
          <div className="text-center mb-16">
            <h2 className="text-gray-900 mb-4">
              Phù Hợp Cho Mọi Loại Hình Doanh Nghiệp
            </h2>
            <p className="text-gray-600 text-lg max-w-3xl mx-auto">
              DSS Analytics được thiết kế linh hoạt để đáp ứng nhu cầu của mọi quy mô
            </p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
            {useCases.map((useCase) => {
              const Icon = useCase.icon;
              return (
                <Card key={useCase.title} className="p-6 hover:shadow-lg transition-shadow">
                  <div className="bg-blue-100 w-16 h-16 rounded-xl flex items-center justify-center mb-4">
                    <Icon className="w-8 h-8 text-blue-600" />
                  </div>
                  <h3 className="text-gray-900 mb-3">
                    {useCase.title}
                  </h3>
                  <p className="text-gray-600 text-sm mb-4">
                    {useCase.description}
                  </p>
                  <Badge variant="secondary" className="text-xs">
                    {useCase.stats}
                  </Badge>
                </Card>
              );
            })}
          </div>
        </div>
      </section>

      {/* Testimonials */}
      <section className="py-20 bg-gradient-to-br from-blue-50 to-white">
        <div className="max-w-7xl mx-auto px-6">
          <div className="text-center mb-16">
            <h2 className="text-gray-900 mb-4">
              Khách Hàng Nói Gì Về Chúng Tôi
            </h2>
            <p className="text-gray-600 text-lg max-w-3xl mx-auto">
              Những phản hồi thực tế từ các doanh nghiệp đang sử dụng DSS Analytics
            </p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
            {testimonials.map((testimonial) => (
              <Card key={testimonial.name} className="p-6">
                <div className="text-gray-600 mb-6 italic">
                  "{testimonial.content}"
                </div>
                <div className="border-t pt-4">
                  <p className="text-gray-900 mb-1">{testimonial.name}</p>
                  <p className="text-gray-500 text-sm">{testimonial.position}</p>
                </div>
              </Card>
            ))}
          </div>
        </div>
      </section>

      {/* CTA Section */}
      <section className="py-20 bg-gradient-to-br from-blue-600 to-blue-700 text-white">
        <div className="max-w-4xl mx-auto px-6 text-center">
          <h2 className="text-white mb-6">
            Sẵn Sàng Bắt Đầu?
          </h2>
          <p className="text-blue-100 text-xl mb-8">
            Đăng nhập ngay để trải nghiệm toàn bộ tính năng của DSS Analytics
          </p>
          <div className="flex gap-4 justify-center">
            <Button 
              size="lg"
              className="bg-white text-blue-600 hover:bg-blue-50 gap-2"
              onClick={() => navigateTo("login")}
            >
              Đăng Nhập Ngay
              <ArrowRight className="w-5 h-5" />
            </Button>
            <Button 
              size="lg"
              variant="outline"
              className="border-white text-white hover:bg-white/10"
              onClick={() => navigateTo("contact")}
            >
              Đặt Lịch Demo
            </Button>
          </div>
        </div>
      </section>

      <Footer />
    </div>
  );
}
