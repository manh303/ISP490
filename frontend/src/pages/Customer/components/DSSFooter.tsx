
import { BarChart3, Mail, Phone, MapPin } from 'lucide-react';
import { Separator } from '../../../components/ui/figma/separator';

export function DSSFooter() {
  return (
    <footer className="bg-gray-50 border-t">
      <div className="container mx-auto px-4 py-12">
        <div className="grid grid-cols-1 md:grid-cols-4 gap-8 mb-8">
          {/* Company */}
          <div>
            <div className="flex items-center gap-2 mb-4">
              <div className="w-10 h-10 bg-gradient-to-br from-green-500 to-blue-600 rounded-lg flex items-center justify-center">
                <BarChart3 className="w-6 h-6 text-white" />
              </div>
              <span className="text-gray-900">DSS Analytics</span>
            </div>
            <p className="text-gray-600">
              Hệ thống phân tích dữ liệu kinh doanh dành cho người bán hàng
            </p>
          </div>

          {/* Sản Phẩm */}
          <div>
            <h3 className="text-gray-900 mb-4">Sản Phẩm</h3>
            <ul className="space-y-2 text-gray-600">
              <li><a href="#" className="hover:text-green-600 transition-colors">Dashboard</a></li>
              <li><a href="#" className="hover:text-green-600 transition-colors">Báo Cáo</a></li>
              <li><a href="#" className="hover:text-green-600 transition-colors">Insights</a></li>
              <li><a href="#" className="hover:text-green-600 transition-colors">Bảng Giá</a></li>
            </ul>
          </div>

          {/* Hỗ Trợ */}
          <div>
            <h3 className="text-gray-900 mb-4">Hỗ Trợ</h3>
            <ul className="space-y-2 text-gray-600">
              <li><a href="#" className="hover:text-green-600 transition-colors">Trung Tâm Trợ Giúp</a></li>
              <li><a href="#" className="hover:text-green-600 transition-colors">Hướng Dẫn Sử Dụng</a></li>
              <li><a href="#" className="hover:text-green-600 transition-colors">Câu Hỏi Thường Gặp</a></li>
              <li><a href="#" className="hover:text-green-600 transition-colors">Liên Hệ</a></li>
            </ul>
          </div>

          {/* Liên Hệ */}
          <div>
            <h3 className="text-gray-900 mb-4">Liên Hệ</h3>
            <ul className="space-y-3 text-gray-600">
              <li className="flex items-start gap-2">
                <Mail className="w-5 h-5 text-green-600 flex-shrink-0 mt-0.5" />
                <span>support@dss-analytics.vn</span>
              </li>
              <li className="flex items-start gap-2">
                <Phone className="w-5 h-5 text-green-600 flex-shrink-0 mt-0.5" />
                <span>1900 1234</span>
              </li>
              <li className="flex items-start gap-2">
                <MapPin className="w-5 h-5 text-green-600 flex-shrink-0 mt-0.5" />
                <span>Hà Nội, Việt Nam</span>
              </li>
            </ul>
          </div>
        </div>

        <Separator className="my-8" />

        <div className="flex flex-col md:flex-row justify-between items-center gap-4 text-gray-600">
          <p>© 2025 DSS Analytics. Bản quyền thuộc về chúng tôi.</p>
          <div className="flex gap-6">
            <a href="#" className="hover:text-green-600 transition-colors">Chính Sách Bảo Mật</a>
            <a href="#" className="hover:text-green-600 transition-colors">Điều Khoản Sử Dụng</a>
          </div>
        </div>
      </div>
    </footer>
  );
}
