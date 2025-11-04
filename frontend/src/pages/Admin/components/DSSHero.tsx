import { Button } from "../../../components/ui/figma/button";
import { ArrowRight } from "lucide-react";
import { ImageWithFallback } from "./figma/ImageWithFallback";

export function DSSHero() {
  return (
    <section className="bg-gradient-to-br from-blue-50 via-white to-blue-50 py-20">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="grid lg:grid-cols-2 gap-12 items-center">
          {/* Left Content */}
          <div>
            <div className="inline-block px-4 py-2 bg-blue-100 text-blue-800 rounded-full mb-6">
              Admin Portal
            </div>
            <h1 className="text-blue-900 mb-6">
              Quản Lý & Vận Hành Hệ Thống Dữ Liệu Toàn Diện
            </h1>
            <p className="text-xl text-gray-600 mb-8 leading-relaxed">
              Dành cho quản trị viên – người vận hành, giám sát và đảm bảo hệ thống dữ liệu hoạt động hiệu quả.
            </p>
            <Button size="lg" className="bg-blue-600 hover:bg-blue-700 gap-2">
              Vào Bảng Quản Trị
              <ArrowRight className="w-5 h-5" />
            </Button>
          </div>

          {/* Right Image */}
          <div className="relative">
            <div className="absolute inset-0 bg-gradient-to-tr from-blue-600/20 to-blue-400/20 rounded-2xl blur-3xl"></div>
            <div className="relative bg-white rounded-2xl shadow-2xl p-4 border border-blue-100">
              <ImageWithFallback
                src="https://images.unsplash.com/photo-1759752394755-1241472b589d?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3Nzg4Nzd8MHwxfHNlYXJjaHwxfHxkYXNoYm9hcmQlMjBhbmFseXRpY3MlMjBjb21wdXRlciUyMHNjcmVlbnxlbnwxfHx8fDE3NjE3NjI3MjR8MA&ixlib=rb-4.1.0&q=80&w=1080"
                alt="Dashboard Analytics"
                className="w-full h-auto rounded-xl"
              />
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
