import { Database, Mail, Phone, MapPin } from "lucide-react";

export function DSSFooter() {
  const footerSections = {
    "Sản Phẩm": ["Tính Năng", "Tài Liệu", "API", "Bảng Giá"],
    "Công Ty": ["Về Chúng Tôi", "Blog", "Tuyển Dụng", "Liên Hệ"],
    "Hỗ Trợ": ["Trung Tâm Hỗ Trợ", "Cộng Đồng", "Trạng Thái Hệ Thống", "FAQ"],
  };

  return (
    <footer className="bg-blue-900 text-white">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
        <div className="grid grid-cols-1 md:grid-cols-4 gap-8 mb-8">
          {/* Logo & Description */}
          <div className="md:col-span-1">
            <div className="flex items-center gap-2 mb-4">
              <div className="w-10 h-10 bg-gradient-to-br from-blue-400 to-blue-600 rounded-lg flex items-center justify-center">
                <Database className="w-6 h-6 text-white" />
              </div>
              <span className="text-white">DSS Analytics</span>
            </div>
            <p className="text-blue-200 text-sm mb-4">
              Nền tảng phân tích dữ liệu toàn diện cho doanh nghiệp hiện đại
            </p>
            <div className="flex gap-3 text-blue-200 text-sm">
              <Mail className="w-4 h-4" />
              <Phone className="w-4 h-4" />
              <MapPin className="w-4 h-4" />
            </div>
          </div>

          {/* Footer Links */}
          {Object.entries(footerSections).map(([title, links]) => (
            <div key={title}>
              <h4 className="text-white mb-4">{title}</h4>
              <ul className="space-y-2">
                {links.map((link) => (
                  <li key={link}>
                    <a
                      href="#"
                      className="text-blue-200 hover:text-white transition-colors text-sm"
                    >
                      {link}
                    </a>
                  </li>
                ))}
              </ul>
            </div>
          ))}
        </div>

        <div className="border-t border-blue-800 pt-8 flex flex-col md:flex-row justify-between items-center">
          <p className="text-blue-200 text-sm mb-4 md:mb-0">
            © 2025 DSS Analytics. Tất cả quyền được bảo lưu.
          </p>
          <div className="flex gap-6 text-sm text-blue-200">
            <a href="#" className="hover:text-white transition-colors">
              Chính Sách Bảo Mật
            </a>
            <a href="#" className="hover:text-white transition-colors">
              Điều Khoản Sử Dụng
            </a>
            <a href="#" className="hover:text-white transition-colors">
              Cookies
            </a>
          </div>
        </div>
      </div>
    </footer>
  );
}
