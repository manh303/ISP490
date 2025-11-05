import { Button } from "../../../../components/ui/figma/button";
import { BarChart3 } from "lucide-react";

export function AnalystHeader() {
  const menuItems = ["Dashboard", "Báo Cáo", "Insights", "Liên Hệ"];

  return (
    <header className="border-b bg-white sticky top-0 z-50 shadow-sm">
      <div className="container mx-auto px-4 py-4">
        <div className="flex items-center justify-between gap-8">
          {/* Logo */}
          <div className="flex items-center gap-2">
            <div className="w-8 h-8 bg-gradient-to-br from-blue-500 to-blue-600 rounded-lg flex items-center justify-center">
              <BarChart3 className="w-5 h-5 text-white" />
            </div>
            <span className="text-blue-600">DSS Analytics</span>
          </div>

          {/* Navigation */}
          <nav className="hidden md:flex items-center gap-8">
            {menuItems.map((item) => (
              <a
                key={item}
                href="#"
                className="text-sm text-gray-600 hover:text-blue-600 transition-colors"
              >
                {item}
              </a>
            ))}
          </nav>

          {/* CTA Button */}
          <Button className="bg-blue-600 hover:bg-blue-700">
            Đăng Nhập Analyst
          </Button>
        </div>
      </div>
    </header>
  );
}
