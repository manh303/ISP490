import { Users, Bot, Server } from "lucide-react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "../../../components/ui/figma/card";

const features = [
  {
    title: "Quản lý người dùng",
    description: "Tạo, phân quyền, và theo dõi tài khoản người dùng trong hệ thống một cách dễ dàng và bảo mật.",
    icon: Users,
    color: "from-blue-600 to-blue-700",
  },
  {
    title: "Giám sát Crawler",
    description: "Theo dõi tiến trình và nhật ký thu thập dữ liệu từ các nguồn khác nhau theo thời gian thực.",
    icon: Bot,
    color: "from-blue-700 to-blue-800",
  },
  {
    title: "Quản lý hệ thống",
    description: "Kiểm soát ELT Jobs, logs và pipeline để đảm bảo hệ thống vận hành trơn tru và hiệu quả.",
    icon: Server,
    color: "from-blue-800 to-blue-900",
  },
];

export function DSSFeatures() {
  return (
    <section className="py-20 bg-white">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="text-center mb-16">
          <h2 className="text-blue-900 mb-4">
            Tính Năng Quản Trị Mạnh Mẽ
          </h2>
          <p className="text-xl text-gray-600 max-w-3xl mx-auto">
            Công cụ toàn diện giúp quản trị viên kiểm soát mọi khía cạnh của hệ thống
          </p>
        </div>

        <div className="grid md:grid-cols-3 gap-8">
          {features.map((feature) => {
            const Icon = feature.icon;
            return (
              <Card key={feature.title} className="border-blue-100 hover:shadow-xl transition-shadow">
                <CardHeader>
                  <div className={`w-14 h-14 bg-gradient-to-br ${feature.color} rounded-xl flex items-center justify-center mb-4`}>
                    <Icon className="w-7 h-7 text-white" />
                  </div>
                  <CardTitle className="text-blue-900">{feature.title}</CardTitle>
                  <CardDescription className="text-gray-600 leading-relaxed">
                    {feature.description}
                  </CardDescription>
                </CardHeader>
              </Card>
            );
          })}
        </div>
      </div>
    </section>
  );
}
