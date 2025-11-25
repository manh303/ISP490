import { BarChart3, LogOut } from "lucide-react";
import { Button } from "../../components/ui/figma/button";
import { useNavigate } from "react-router";
import { useAuth } from "../../contexts/AuthContext";

export function Header() {
  const navigate = useNavigate();
  const { isAuthenticated, logout, user, hasRole } = useAuth();

  const handleNavigate = (path: string) => {
    navigate(path);
  };

  const handleDashboardNavigate = () => {
    if (hasRole('admin')) {
      navigate('/admin/dashboard');
    } else if (hasRole('analyst')) {
      navigate('/analyst/dashboard');
    } else if (hasRole('dataengineer')) {
      navigate('/dataengineer/dashboard');
    } else if (hasRole('ML')) {
      navigate('/ml/dashboard');
    } else {
      // Default fallback dashboard
      navigate('/dashboard');
    }
  };

  const getDashboardLabel = () => {
    if (hasRole('admin')) {
      return 'Admin Dashboard';
    } else if (hasRole('analyst')) {
      return 'Analyst Dashboard';
    } else if (hasRole('dataengineer')) {
      return 'Engineer Dashboard';
       } else if (hasRole('ML')) {
      return 'ML Dashboard';
    } else {
      return 'Dashboard';
    }
  };

  const handleLogout = async () => {
    try {
      await logout();
      navigate('/');
    } catch (err) {
      console.error('Logout failed', err);
    }
  };

  return (
    <header className="bg-white border-b border-gray-200 shadow-sm sticky top-0 z-50">
      <div className="max-w-7xl mx-auto px-6 h-20 flex items-center justify-between">
        {/* Logo */}
        <button
          onClick={() => handleNavigate('/')}
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
            onClick={() => handleNavigate('/')}
            className="text-gray-600 hover:text-gray-900 transition-colors"
          >
            Trang Chủ
          </button>
          <button
            onClick={() => handleNavigate('/solutions')}
            className="text-gray-600 hover:text-gray-900 transition-colors"
          >
            Giải Pháp
          </button>
          <button
            onClick={() => handleNavigate('/about')}
            className="text-gray-600 hover:text-gray-900 transition-colors"
          >
            Về Chúng Tôi
          </button>
          <button
            onClick={() => handleNavigate('/contact')}
            className="text-gray-600 hover:text-gray-900 transition-colors"
          >
            Liên Hệ
          </button>
        </nav>

        {/* Auth Buttons */}
        <div className="flex items-center gap-4">
          {isAuthenticated ? (
            <>
              <Button
                variant="outline"
                onClick={handleDashboardNavigate}
              >
                {getDashboardLabel()}
              </Button>
              <Button
                variant="ghost"
                onClick={handleLogout}
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
                onClick={() => handleNavigate('/signin')}
              >
                Đăng Nhập
              </Button>
              <Button
                onClick={() => handleNavigate('/signup')}
                className="inline-flex items-center justify-center gap-2 rounded-lg transition px-4 py-3 text-sm bg-brand-500 text-white shadow-theme-xs hover:bg-brand-600 disabled:bg-brand-300"
              >
                Đăng Ký
              </Button>
            </>
          )}
        </div>
      </div>
    </header>
  );
}
