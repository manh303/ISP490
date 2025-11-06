import { Card } from "../../../../components/ui/figma/card";
import { LayoutDashboard, Lightbulb, FileDown } from "lucide-react";
import { features } from "../../data/analystData";

const iconMap = {
  "layout-dashboard": LayoutDashboard,
  "lightbulb": Lightbulb,
  "file-down": FileDown
};

export function KeyFeatures() {
  return (
    <section className="py-20 bg-white">
      <div className="container mx-auto px-4">
        <div className="text-center mb-16">
          <div className="inline-block px-4 py-2 bg-blue-100 text-blue-600 rounded-full text-sm mb-4">
            Tính Năng Chính
          </div>
          <h2 className="text-3xl lg:text-4xl text-gray-900 mb-4">
            Công Cụ Mạnh Mẽ Cho Nhà Phân Tích
          </h2>
          <p className="text-lg text-gray-600 max-w-2xl mx-auto">
            Tất cả những gì bạn cần để phân tích dữ liệu và đưa ra quyết định thông minh
          </p>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
          {features.map((feature) => {
            const Icon = iconMap[feature.icon as keyof typeof iconMap];
            return (
              <Card 
                key={feature.id} 
                className="p-8 hover:shadow-xl transition-all duration-300 border-2 hover:border-blue-200 group"
              >
                <div className="w-14 h-14 bg-gradient-to-br from-blue-500 to-blue-600 rounded-xl flex items-center justify-center mb-6 group-hover:scale-110 transition-transform">
                  <Icon className="w-7 h-7 text-white" />
                </div>
                <h3 className="text-xl text-gray-900 mb-3">{feature.title}</h3>
                <p className="text-gray-600 leading-relaxed">
                  {feature.description}
                </p>
              </Card>
            );
          })}
        </div>
      </div>
    </section>
  );
}
