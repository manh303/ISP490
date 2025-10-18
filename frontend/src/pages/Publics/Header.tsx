import { BarChart3, LogOut } from "lucide-react";
import { Button } from "../../components/ui/figma/button";
import type { Page } from "../App";

interface HeaderProps {
  navigateTo: (page: Page) => void;
  isLoggedIn: boolean;
  onLogout: () => void;
}

export function Header({ navigateTo, isLoggedIn, onLogout }: HeaderProps) {
  return (
    <header className="bg-white border-b border-gray-200 shadow-sm sticky top-0 z-50">
      <div className="max-w-7xl mx-auto px-6 h-20 flex items-center justify-between">
        {/* Logo */}
        <button 
          onClick={() => navigateTo("home")}
          className="flex items-center gap-2 hover:opacity-80 transition-opacity"
        >
          <div className="bg-gradient-to-br from-blue-600 to-blue-700 p-2 rounded-lg">
            <BarChart3 className="w-6 h-6 text-white" />
          </div>
          <span className="text-gray-900">DSS Analytics</span>
        </button>

        {/* Navigation Links */}
        <nav className="flex items-center gap-8">
          <button 
            onClick={() => navigateTo("home")}
            className="text-gray-600 hover:text-gray-900 transition-colors"
          >
            Trang Chủ
          </button>
          <button 
            onClick={() => navigateTo("solutions")}
            className="text-gray-600 hover:text-gray-900 transition-colors"
          >
            Giải Pháp
          </button>
          <button 
            onClick={() => navigateTo("about")}
            className="text-gray-600 hover:text-gray-900 transition-colors"
          >
            Về Chúng Tôi
          </button>
          <button 
            onClick={() => navigateTo("contact")}
            className="text-gray-600 hover:text-gray-900 transition-colors"
          >
            Liên Hệ
          </button>
        </nav>

        {/* Auth Buttons */}
        <div className="flex items-center gap-4">
          {isLoggedIn ? (
            <>
              <Button 
                variant="outline"
                onClick={() => navigateTo("dashboard")}
              >
                Dashboard
              </Button>
              <Button 
                variant="ghost"
                onClick={onLogout}
                className="gap-2"
              >
                <LogOut className="w-4 h-4" />
                Đăng Xuất
              </Button>
            </>
          ) : (
            <>
              <Button 
                variant="outline"
                onClick={() => navigateTo("login")}
              >
                Đăng Nhập
              </Button>
              <Button className="bg-gradient-to-r from-blue-600 to-blue-700 hover:from-blue-700 hover:to-blue-800">
                Đăng Ký
              </Button>
            </>
          )}
        </div>
      </div>
    </header>
  );
}
