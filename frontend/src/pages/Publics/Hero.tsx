import { ArrowRight, Play } from "lucide-react";
import { Button } from "../../components/ui/figma/button";
import { ImageWithFallback } from "../../components/figma/ImageWithFallback";
import type { Page } from "../../App";

interface HeroProps {
  navigateTo: (page: Page) => void;
}

export function Hero({ navigateTo }: HeroProps) {
    return (
      <section className="relative min-h-[700px] flex flex-col justify-center overflow-hidden font-sans">
        {/* Background Image with Overlay - kéo dài xuống toàn bộ section */}
        <div className="absolute inset-0 h-full">
          <ImageWithFallback
            src="https://images.unsplash.com/photo-1694702740570-0a31ee1525c7?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3Nzg4Nzd8MHwxfHNlYXJjaHwxfHxtb2Rlcm4lMjBvZmZpY2UlMjBidWlsZGluZ3xlbnwxfHx8fDE3NjAzMTE4MzR8MA&ixlib=rb-4.1.0&q=80&w=1080&utm_source=figma&utm_medium=referral"
            alt="Hero Background"
            className="w-full h-full object-cover"
          />
          <div className="absolute inset-0 bg-gradient-to-b from-blue-900/90 via-blue-800/80 to-blue-900/70" />
        </div>

        {/* Content */}
        <div className="relative max-w-4xl mx-auto text-center px-6 pt-20 pb-10">
          <h1 className="text-white mb-6 font-bold text-4xl md:text-5xl leading-tight drop-shadow-lg">
            Hệ Thống Hỗ Trợ Ra <br /> Quyết Định Thông Minh
          </h1>
          <p className="text-blue-100 text-xl mb-8 max-w-2xl mx-auto drop-shadow">
            Phân tích dữ liệu chuyên sâu, dự báo xu hướng chính xác và tối ưu hóa vận hành để đưa ra những quyết định kinh doanh hiệu quả nhất
          </p>
          <div className="flex gap-4 justify-center mb-12">
            <Button
              size="lg"
              className="bg-white text-blue-900 hover:bg-blue-50 gap-2 shadow-md"
              onClick={() => navigateTo("explore")}
            >
              Khám Phá Giải Pháp
              <ArrowRight className="w-5 h-5" />
            </Button>
            <Button
              size="lg"
              variant="outline"
              className="border-white text-white hover:bg-white/10 gap-2 shadow-md"
              onClick={() => navigateTo("explore")}
            >
              <Play className="w-5 h-5" />
              Xem Demo
            </Button>
          </div>
          {/* Các Tính Năng Nổi Bật nằm trong vùng overlay */}
          <div className="rounded-xl bg-white/80 shadow-lg py-6 px-3 text-center max-w-3xl mx-auto mb-8 backdrop-blur-md">
            <h2 className="text-blue-900 mb-4">Các Tính Năng Nổi Bật</h2>
            <p className="text-gray-700 max-w-3xl mx-auto">
              Giải pháp toàn diện giúp doanh nghiệp đưa ra quyết định thông minh dựa trên dữ liệu
            </p>
          </div>
        </div>
      </section>
    );
}
