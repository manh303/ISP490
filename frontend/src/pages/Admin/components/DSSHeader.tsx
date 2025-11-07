import { Button } from "../../../components/ui/figma/button";
import { Database } from "lucide-react";

export function DSSHeader() {
  const menuItems = ["Trang Chủ", "Hệ Thống", "Người Dùng", "Cấu Hình", "Liên Hệ"];

  return (
    <header className="border-b border-blue-100 bg-white">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex items-center justify-between h-16">
          {/* Logo */}
          <div className="flex items-center gap-2">
            <div className="w-10 h-10 bg-gradient-to-br from-blue-900 to-blue-600 rounded-lg flex items-center justify-center">
              <Database className="w-6 h-6 text-white" />
            </div>
            <span className="text-blue-900">DSS Analytics</span>
          </div>

          {/* Menu */}
          <nav className="hidden md:flex items-center gap-8">
            {menuItems.map((item) => (
              <a
                key={item}
                href="#"
                className="text-gray-700 hover:text-blue-600 transition-colors"
              >
                {item}
              </a>
            ))}
          </nav>

          {/* Login Button */}
          <Button className="bg-blue-600 hover:bg-blue-700">
            Đăng Nhập Admin
          </Button>
        </div>
      </div>
    </header>
  );
}
