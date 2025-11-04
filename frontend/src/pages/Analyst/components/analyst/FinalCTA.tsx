import { Button } from ".../../../components/ui/figma/button";
import { ArrowRight, Sparkles } from "lucide-react";

export function FinalCTA() {
  return (
    <section className="py-20 bg-gradient-to-br from-blue-600 via-blue-700 to-blue-800 relative overflow-hidden">
      {/* Background Pattern */}
      <div className="absolute inset-0 opacity-10">
        <div className="absolute top-0 left-0 w-64 h-64 bg-white rounded-full blur-3xl" />
        <div className="absolute bottom-0 right-0 w-96 h-96 bg-white rounded-full blur-3xl" />
      </div>

      <div className="container mx-auto px-4 relative z-10">
        <div className="max-w-3xl mx-auto text-center">
          <div className="inline-flex items-center gap-2 px-4 py-2 bg-blue-500/50 text-white rounded-full text-sm mb-6">
            <Sparkles className="w-4 h-4" />
            <span>Sẵn Sàng Bắt Đầu</span>
          </div>

          <h2 className="text-3xl lg:text-5xl text-white mb-6">
            Khám Phá Dữ Liệu – Tạo Ra Chiến Lược Thông Minh
          </h2>

          <p className="text-lg text-blue-100 mb-10 leading-relaxed">
            Tham gia cùng hàng trăm nhà phân tích đang sử dụng DSS Analytics để biến dữ liệu thành lợi thế cạnh tranh
          </p>

          <div className="flex flex-col sm:flex-row gap-4 justify-center">
            <Button 
              size="lg" 
              className="bg-white text-blue-600 hover:bg-blue-50 group text-lg px-8 py-6"
            >
              Đăng Nhập Analyst Portal
              <ArrowRight className="w-5 h-5 ml-2 group-hover:translate-x-1 transition-transform" />
            </Button>
            <Button 
              size="lg" 
              variant="outline" 
              className="border-2 border-white text-white hover:bg-white/10 text-lg px-8 py-6"
            >
              Đặt Lịch Demo
            </Button>
          </div>

          {/* Trust Indicators */}
          <div className="grid grid-cols-3 gap-8 mt-16 pt-16 border-t border-white/20">
            <div>
              <p className="text-3xl text-white mb-2">500+</p>
              <p className="text-sm text-blue-100">Nhà Phân Tích</p>
            </div>
            <div>
              <p className="text-3xl text-white mb-2">10k+</p>
              <p className="text-sm text-blue-100">Báo Cáo/Tháng</p>
            </div>
            <div>
              <p className="text-3xl text-white mb-2">99.9%</p>
              <p className="text-sm text-blue-100">Uptime</p>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
