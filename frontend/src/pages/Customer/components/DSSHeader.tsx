import { Button } from '../../../components/ui/figma/button';
import { BarChart3 } from 'lucide-react';

export function DSSHeader() {
  return (
    <header className="border-b bg-white sticky top-0 z-50 shadow-sm">
      <div className="container mx-auto px-4 py-4 flex items-center justify-between">
        {/* Logo */}
        <div className="flex items-center gap-2">
          <div className="w-10 h-10 bg-gradient-to-br from-green-500 to-blue-600 rounded-lg flex items-center justify-center">
            <BarChart3 className="w-6 h-6 text-white" />
          </div>
          <span className="text-gray-900">DSS Analytics</span>
        </div>

        {/* Navigation */}
        <nav className="hidden md:flex items-center gap-8">
          <a href="#" className="text-gray-600 hover:text-gray-900 transition-colors">
            Trang Chủ
          </a>
          <a href="#" className="text-gray-600 hover:text-gray-900 transition-colors">
            Báo Cáo Của Tôi
          </a>
          <a href="#" className="text-gray-600 hover:text-gray-900 transition-colors">
            Hỗ Trợ
          </a>
          <a href="#" className="text-gray-600 hover:text-gray-900 transition-colors">
            Liên Hệ
          </a>
        </nav>

        {/* Login Button */}
        <Button className="bg-blue-600 hover:bg-blue-700">
          Đăng Nhập Customer
        </Button>
      </div>
    </header>
  );
}
