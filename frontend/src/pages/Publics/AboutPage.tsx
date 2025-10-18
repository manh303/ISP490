import { Target, Users, Award, TrendingUp, CheckCircle, Zap, Shield, Globe } from "lucide-react";
import { Card } from "../../components/ui/figma/card";
import { ImageWithFallback } from "../../components/figma/ImageWithFallback";
import type { Page } from "../../App";

interface AboutPageProps {
  navigateTo: (page: Page) => void;
  isLoggedIn: boolean;
  onLogout: () => void;
}

export function AboutPage( { navigateTo, isLoggedIn, onLogout }: AboutPageProps) {
  const values = [
    {
      icon: Target,
      title: "Định Hướng Khách Hàng",
      description: "Chúng tôi đặt nhu cầu và thành công của khách hàng làm trọng tâm trong mọi quyết định",
    },
    {
      icon: Zap,
      title: "Đổi Mới Liên Tục",
      description: "Không ngừng cải tiến và phát triển công nghệ để mang đến giải pháp tốt nhất",
    },
    {
      icon: Shield,
      title: "Bảo Mật & Tin Cậy",
      description: "Cam kết bảo vệ dữ liệu và quyền riêng tư của khách hàng ở mức cao nhất",
    },
    {
      icon: Globe,
      title: "Tầm Nhìn Toàn Cầu",
      description: "Phát triển giải pháp đáp ứng tiêu chuẩn quốc tế và xu hướng thế giới",
    },
  ];

  const stats = [
    { label: "Khách Hàng", value: "500+", icon: Users },
    { label: "Dự Án Thành Công", value: "1,200+", icon: CheckCircle },
    { label: "Năm Kinh Nghiệm", value: "10+", icon: Award },
    { label: "Tăng Trưởng", value: "45%", icon: TrendingUp },
  ];

  const team = [
    {
      name: "Nguyễn Văn A",
      position: "CEO & Founder",
      description: "15 năm kinh nghiệm trong lĩnh vực phân tích dữ liệu và AI",
    },
    {
      name: "Trần Thị B",
      position: "CTO",
      description: "Chuyên gia công nghệ với nhiều giải thưởng quốc tế",
    },
    {
      name: "Lê Văn C",
      position: "Head of Product",
      description: "10 năm phát triển sản phẩm cho doanh nghiệp lớn",
    },
    {
      name: "Phạm Thị D",
      position: "Head of Customer Success",
      description: "Chuyên gia tư vấn với hơn 300 dự án triển khai thành công",
    },
  ];

  return (
    <div className="min-h-screen bg-white">
     
      {/* Hero Section */}
      <section className="relative py-20 bg-gradient-to-br from-blue-50 to-white overflow-hidden">
        <div className="max-w-7xl mx-auto px-6">
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-12 items-center">
            <div>
              <h1 className="text-gray-900 mb-6">
                Về DSS Analytics
              </h1>
              <p className="text-gray-600 text-lg mb-6">
                Chúng tôi là đội ngũ chuyên gia đam mê công nghệ, tận tâm phát triển các giải pháp 
                hỗ trợ ra quyết định thông minh cho doanh nghiệp Việt Nam.
              </p>
              <p className="text-gray-600 text-lg">
                Với hơn 10 năm kinh nghiệm trong lĩnh vực phân tích dữ liệu và AI, chúng tôi hiểu rõ 
                những thách thức mà doanh nghiệp đang phải đối mặt và cam kết mang đến những công cụ 
                mạnh mẽ nhất để giúp bạn thành công.
              </p>
            </div>
            <div className="relative h-[400px] rounded-2xl overflow-hidden shadow-2xl">
              <ImageWithFallback
                src="https://images.unsplash.com/photo-1709715357520-5e1047a2b691?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3Nzg4Nzd8MHwxfHNlYXJjaHwxfHxidXNpbmVzcyUyMHRlYW0lMjBtZWV0aW5nfGVufDF8fHx8MTc2MDMyMzA5Mnww&ixlib=rb-4.1.0&q=80&w=1080&utm_source=figma&utm_medium=referral"
                alt="Team"
                className="w-full h-full object-cover"
              />
            </div>
          </div>
        </div>
      </section>

      {/* Stats Section */}
      <section className="py-16 bg-blue-600">
        <div className="max-w-7xl mx-auto px-6">
          <div className="grid grid-cols-1 md:grid-cols-4 gap-8">
            {stats.map((stat) => {
              const Icon = stat.icon;
              return (
                <div key={stat.label} className="text-center text-white">
                  <Icon className="w-12 h-12 mx-auto mb-4 opacity-80" />
                  <p className="mb-2">{stat.value}</p>
                  <p className="text-blue-100">{stat.label}</p>
                </div>
              );
            })}
          </div>
        </div>
      </section>

      {/* Mission & Vision */}
      <section className="py-20 bg-white">
        <div className="max-w-7xl mx-auto px-6">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-12">
            <Card className="p-8 bg-gradient-to-br from-blue-50 to-white border-blue-200">
              <Target className="w-12 h-12 text-blue-600 mb-4" />
              <h2 className="text-gray-900 mb-4">
                Sứ Mệnh
              </h2>
              <p className="text-gray-600 text-lg">
                Trao quyền cho các doanh nghiệp Việt Nam với công nghệ phân tích dữ liệu tiên tiến, 
                giúp họ đưa ra quyết định thông minh, nhanh chóng và chính xác hơn để phát triển 
                bền vững trong thời đại số.
              </p>
            </Card>

            <Card className="p-8 bg-gradient-to-br from-purple-50 to-white border-purple-200">
              <TrendingUp className="w-12 h-12 text-purple-600 mb-4" />
              <h2 className="text-gray-900 mb-4">
                Tầm Nhìn
              </h2>
              <p className="text-gray-600 text-lg">
                Trở thành nền tảng hỗ trợ ra quyết định hàng đầu tại Việt Nam, được tin dùng bởi 
                hàng nghìn doanh nghiệp và góp phần thúc đẩy chuyển đổi số toàn diện cho nền kinh tế.
              </p>
            </Card>
          </div>
        </div>
      </section>

      {/* Values Section */}
      <section className="py-20 bg-gray-50">
        <div className="max-w-7xl mx-auto px-6">
          <div className="text-center mb-16">
            <h2 className="text-gray-900 mb-4">
              Giá Trị Cốt Lõi
            </h2>
            <p className="text-gray-600 text-lg max-w-3xl mx-auto">
              Những giá trị định hướng mọi hành động và quyết định của chúng tôi
            </p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-8">
            {values.map((value) => {
              const Icon = value.icon;
              return (
                <Card key={value.title} className="p-6 hover:shadow-lg transition-shadow">
                  <div className="bg-blue-100 w-16 h-16 rounded-xl flex items-center justify-center mb-4">
                    <Icon className="w-8 h-8 text-blue-600" />
                  </div>
                  <h3 className="text-gray-900 mb-3">
                    {value.title}
                  </h3>
                  <p className="text-gray-600">
                    {value.description}
                  </p>
                </Card>
              );
            })}
          </div>
        </div>
      </section>

      {/* Team Section */}
      <section className="py-20 bg-white">
        <div className="max-w-7xl mx-auto px-6">
          <div className="text-center mb-16">
            <h2 className="text-gray-900 mb-4">
              Đội Ngũ Lãnh Đạo
            </h2>
            <p className="text-gray-600 text-lg max-w-3xl mx-auto">
              Những người dẫn dắt DSS Analytics hướng tới tương lai
            </p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-8">
            {team.map((member) => (
              <Card key={member.name} className="p-6 text-center hover:shadow-lg transition-shadow">
                <div className="w-24 h-24 bg-gradient-to-br from-blue-500 to-blue-600 rounded-full mx-auto mb-4 flex items-center justify-center text-white text-2xl">
                  {member.name.charAt(0)}
                </div>
                <h3 className="text-gray-900 mb-2">
                  {member.name}
                </h3>
                <p className="text-blue-600 text-sm mb-3">
                  {member.position}
                </p>
                <p className="text-gray-600 text-sm">
                  {member.description}
                </p>
              </Card>
            ))}
          </div>
        </div>
      </section>


    </div>
  );
}
