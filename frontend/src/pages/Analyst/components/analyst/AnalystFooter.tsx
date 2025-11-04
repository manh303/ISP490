import { Separator } from ".../../../components/ui/figma/separator";
import { BarChart3, Mail, MapPin, Phone } from "lucide-react";

export function AnalystFooter() {
  const footerLinks = {
    "Sản Phẩm": ["Dashboard", "Báo Cáo", "Analytics", "API"],
    "Công Ty": ["Về Chúng Tôi", "Tuyển Dụng", "Blog", "Đối Tác"],
    "Hỗ Trợ": ["Tài Liệu", "Hướng Dẫn", "FAQs", "Liên Hệ"],
    "Pháp Lý": ["Điều Khoản", "Bảo Mật", "Cookie", "Tuân Thủ"]
  };

  return (
    <footer className="bg-gray-50 border-t">
      <div className="container mx-auto px-4 py-12">
        {/* Main Footer Content */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-6 gap-8 mb-12">
          {/* Brand */}
          <div className="lg:col-span-2">
            <div className="flex items-center gap-2 mb-4">
              <div className="w-8 h-8 bg-gradient-to-br from-blue-500 to-blue-600 rounded-lg flex items-center justify-center">
                <BarChart3 className="w-5 h-5 text-white" />
              </div>
              <span className="text-blue-600">DSS Analytics</span>
            </div>
            <p className="text-sm text-gray-600 mb-4 leading-relaxed">
              Nền tảng phân tích dữ liệu hàng đầu dành cho các nhà phân tích chuyên nghiệp.
            </p>
            <div className="space-y-2 text-sm text-gray-600">
              <div className="flex items-center gap-2">
                <Mail className="w-4 h-4 text-blue-500" />
                <span>contact@dssanalytics.vn</span>
              </div>
              <div className="flex items-center gap-2">
                <Phone className="w-4 h-4 text-blue-500" />
                <span>+84 (28) 1234 5678</span>
              </div>
              <div className="flex items-center gap-2">
                <MapPin className="w-4 h-4 text-blue-500" />
                <span>Hà Nội, Việt Nam</span>
              </div>
            </div>
          </div>

          {/* Links */}
          {Object.entries(footerLinks).map(([category, links]) => (
            <div key={category}>
              <h4 className="text-sm text-gray-900 mb-4">{category}</h4>
              <ul className="space-y-2">
                {links.map((link) => (
                  <li key={link}>
                    <a
                      href="#"
                      className="text-sm text-gray-600 hover:text-blue-600 transition-colors"
                    >
                      {link}
                    </a>
                  </li>
                ))}
              </ul>
            </div>
          ))}
        </div>

        <Separator className="mb-8" />

        {/* Bottom Footer */}
        <div className="flex flex-col md:flex-row items-center justify-between gap-4">
          <p className="text-sm text-gray-600">
            © 2025 DSS Analytics. Bản quyền thuộc về công ty.
          </p>
          <div className="flex items-center gap-6">
            <a href="#" className="text-sm text-gray-600 hover:text-blue-600 transition-colors">
              Tiếng Việt
            </a>
            <a href="#" className="text-sm text-gray-600 hover:text-blue-600 transition-colors">
              English
            </a>
          </div>
        </div>
      </div>
    </footer>
  );
}
