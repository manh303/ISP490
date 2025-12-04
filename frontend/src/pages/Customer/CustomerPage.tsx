import { DSSHeader } from './components/DSSHeader';
import { FeatureCard } from './components/FeatureCard';
import { TestimonialCard } from './components/TestimonialCard';
import { DSSFooter } from './components/DSSFooter';
import { ImageWithFallback } from '../../components/figma/ImageWithFallback';
import { Button } from '../../components/ui/figma/button';
import { Card } from '../../components/ui/figma/card';
import { 
  LayoutDashboard, 
  Lightbulb, 
  FileDown, 
  TrendingUp, 
  ShoppingBag,
  Users,
  ArrowRight,
  CheckCircle2
} from 'lucide-react';

export default function CustomerPage() {
  return (
    <div className="min-h-screen bg-gradient-to-b from-brand-50/30 to-white">
      {/* <DSSHeader /> */}

      {/* Hero Section */}
      <section className="container mx-auto px-4 py-20 md:py-28">
        <div className="grid lg:grid-cols-2 gap-12 items-center">
          <div>
            <div className="inline-block mb-6">
              <span className="bg-brand-100 text-brand-700 px-4 py-2 rounded-full">
                Dành cho Người Bán Hàng
              </span>
            </div>
            <h1 className="text-gray-900 mb-6">
              Hiểu Doanh Nghiệp Của Bạn, Dẫn Đầu Với Dữ Liệu
            </h1>
            <p className="text-gray-600 mb-8 leading-relaxed">
              Dành cho nhà bán hàng – theo dõi hiệu suất, sản phẩm bán chạy, và gợi ý phát triển. 
              Biến dữ liệu thành hành động với DSS Analytics.
            </p>
            <div className="flex flex-col sm:flex-row gap-4">
              <Button size="lg" className="bg-brand-500 hover:bg-brand-600 text-white">
                Xem Báo Cáo Của Tôi
                <ArrowRight className="w-5 h-5 ml-2" />
              </Button>
              <Button size="lg" variant="outline" className="border-2 border-blue-600 text-blue-600 hover:bg-blue-50">
                Tìm Hiểu Thêm
              </Button>
            </div>

            {/* Stats */}
            <div className="grid grid-cols-3 gap-6 mt-12 pt-12 border-t border-gray-200">
              <div>
                <p className="text-brand-600 mb-1">5,000+</p>
                <p className="text-gray-600">Người bán</p>
              </div>
              <div>
                <p className="text-brand-600 mb-1">+45%</p>
                <p className="text-gray-600">Tăng doanh thu</p>
              </div>
              <div>
                <p className="text-brand-600 mb-1">24/7</p>
                <p className="text-gray-600">Hỗ trợ</p>
              </div>
            </div>
          </div>

          <div className="relative">
            <div className="absolute inset-0 bg-gradient-to-tr from-brand-200 to-blue-200 rounded-3xl blur-3xl opacity-30"></div>
            <div className="relative bg-white rounded-2xl shadow-2xl p-8 border-2 border-gray-100">
              <ImageWithFallback
                src="https://images.unsplash.com/photo-1759752394755-1241472b589d?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3Nzg4Nzd8MHwxfHNlYXJjaHwxfHxidXNpbmVzcyUyMGRhc2hib2FyZCUyMGxhcHRvcHxlbnwxfHx8fDE3NjE2NTk4OTN8MA&ixlib=rb-4.1.0&q=80&w=1080&utm_source=figma&utm_medium=referral"
                alt="Dashboard Analytics"
                className="w-full h-auto rounded-xl"
              />
              
              {/* Floating Cards */}
              <div className="absolute -top-6 -right-6 bg-white rounded-xl shadow-xl p-4 border-2 border-brand-100">
                <div className="flex items-center gap-3">
                  <div className="w-12 h-12 bg-brand-100 rounded-lg flex items-center justify-center">
                    <TrendingUp className="w-6 h-6 text-brand-600" />
                  </div>
                  <div>
                    <p className="text-gray-500">Doanh thu</p>
                    <p className="text-gray-900">+32.5%</p>
                  </div>
                </div>
              </div>

              <div className="absolute -bottom-6 -left-6 bg-white rounded-xl shadow-xl p-4 border-2 border-blue-100">
                <div className="flex items-center gap-3">
                  <div className="w-12 h-12 bg-blue-100 rounded-lg flex items-center justify-center">
                    <ShoppingBag className="w-6 h-6 text-blue-600" />
                  </div>
                  <div>
                    <p className="text-gray-500">Đơn hàng</p>
                    <p className="text-gray-900">1,247</p>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* Feature Section */}
      <section className="py-20 bg-white">
        <div className="container mx-auto px-4">
          <div className="text-center mb-16">
            <h2 className="text-gray-900 mb-4">Tính Năng Nổi Bật</h2>
            <p className="text-gray-600 max-w-2xl mx-auto">
              Công cụ mạnh mẽ giúp bạn quản lý và phát triển doanh nghiệp hiệu quả hơn
            </p>
          </div>

          <div className="grid md:grid-cols-3 gap-8 max-w-6xl mx-auto">
            <FeatureCard
              icon={LayoutDashboard}
              title="Dashboard Cá Nhân Hóa"
              description="Chỉ hiển thị dữ liệu cửa hàng của bạn với giao diện trực quan, dễ hiểu. Theo dõi doanh thu, đơn hàng và khách hàng theo thời gian thực."
              color="blue"
            />
            <FeatureCard
              icon={Lightbulb}
              title="Business Insights"
              description="Nhận gợi ý tối ưu hóa kinh doanh dựa trên AI. Phát hiện xu hướng, sản phẩm tiềm năng và cơ hội tăng trưởng cho cửa hàng của bạn."
              color="blue"
            />
            <FeatureCard
              icon={FileDown}
              title="Báo Cáo Xuất File"
              description="Tải xuống kết quả kinh doanh chi tiết dưới dạng Excel hoặc PDF. Chia sẻ báo cáo với đối tác hoặc lưu trữ dễ dàng."
              color="purple"
            />
          </div>
        </div>
      </section>

      {/* Benefits Section */}
      <section className="py-20 bg-gradient-to-b from-brand-50 to-blue-50">
        <div className="container mx-auto px-4">
          <div className="grid lg:grid-cols-2 gap-12 items-center max-w-6xl mx-auto">
            <div>
              <ImageWithFallback
                src="https://images.unsplash.com/photo-1531058240690-006c446962d8?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3Nzg4Nzd8MHwxfHNlYXJjaHwxfHxzbWFsbCUyMGJ1c2luZXNzJTIwb3duZXJ8ZW58MXx8fHwxNzYxNzI3NDI2fDA&ixlib=rb-4.1.0&q=80&w=1080&utm_source=figma&utm_medium=referral"
                alt="Business Owner"
                className="w-full h-auto rounded-2xl shadow-2xl"
              />
            </div>

            <div>
              <h2 className="text-gray-900 mb-6">
                Tại Sao Nên Chọn DSS Analytics?
              </h2>
              <p className="text-gray-600 mb-8">
                Chúng tôi hiểu những thách thức của người bán hàng và tạo ra giải pháp phù hợp nhất
              </p>

              <div className="space-y-4">
                {[
                  'Giao diện đơn giản, dễ sử dụng cho mọi đối tượng',
                  'Dữ liệu chính xác, cập nhật theo thời gian thực',
                  'Gợi ý thông minh giúp tăng doanh thu',
                  'Bảo mật thông tin tuyệt đối',
                  'Hỗ trợ khách hàng nhanh chóng 24/7',
                  'Chi phí hợp lý, phù hợp với mọi quy mô'
                ].map((benefit, index) => (
                  <div key={index} className="flex items-start gap-3">
                    <CheckCircle2 className="w-6 h-6 text-brand-600 flex-shrink-0 mt-0.5" />
                    <p className="text-gray-700">{benefit}</p>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* Testimonials Section */}
      <section className="py-20 bg-white">
        <div className="container mx-auto px-4">
          <div className="text-center mb-16">
            <h2 className="text-gray-900 mb-4">Câu Chuyện Thành Công</h2>
            <p className="text-gray-600 max-w-2xl mx-auto">
              Hàng nghìn người bán đã tin tưởng và phát triển cùng DSS Analytics
            </p>
          </div>

          <div className="grid md:grid-cols-3 gap-8 max-w-6xl mx-auto">
            <TestimonialCard
              name="Nguyễn Văn An"
              role="Chủ cửa hàng thời trang"
              content="DSS Analytics giúp tôi hiểu rõ sản phẩm nào bán chạy, khách hàng nào trung thành. Doanh thu tăng 40% chỉ sau 3 tháng sử dụng!"
              image="https://images.unsplash.com/photo-1472099645785-5658abf4ff4e?w=400&h=400&fit=crop"
              rating={5}
            />
            <TestimonialCard
              name="Trần Thị Hương"
              role="Chủ shop online"
              content="Giao diện rất dễ sử dụng, báo cáo chi tiết giúp tôi đưa ra quyết định kinh doanh chính xác hơn. Rất hài lòng với dịch vụ!"
              image="https://images.unsplash.com/photo-1438761681033-6461ffad8d80?w=400&h=400&fit=crop"
              rating={5}
            />
            <TestimonialCard
              name="Lê Minh Tuấn"
              role="Chủ cửa hàng điện tử"
              content="Tính năng insights thông minh thật sự hữu ích. Tôi biết được sản phẩm nào cần nhập thêm, sản phẩm nào nên giảm giá. Tuyệt vời!"
              image="https://images.unsplash.com/photo-1500648767791-00dcc994a43e?w=400&h=400&fit=crop"
              rating={5}
            />
          </div>
        </div>
      </section>

      {/* Stats Section */}
      <section className="py-16 bg-gradient-to-r from-brand-600 to-blue-600">
        <div className="container mx-auto px-4">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-8 text-center text-white">
            <div>
              <div className="flex items-center justify-center gap-2 mb-2">
                <Users className="w-6 h-6" />
                <p className="mb-0">5,000+</p>
              </div>
              <p className="text-brand-100">Người bán tin dùng</p>
            </div>
            <div>
              <div className="flex items-center justify-center gap-2 mb-2">
                <TrendingUp className="w-6 h-6" />
                <p className="mb-0">+45%</p>
              </div>
              <p className="text-brand-100">Tăng trưởng TB</p>
            </div>
            <div>
              <div className="flex items-center justify-center gap-2 mb-2">
                <ShoppingBag className="w-6 h-6" />
                <p className="mb-0">1M+</p>
              </div>
              <p className="text-brand-100">Đơn hàng phân tích</p>
            </div>
            <div>
              <div className="flex items-center justify-center gap-2 mb-2">
                <CheckCircle2 className="w-6 h-6" />
                <p className="mb-0">99.9%</p>
              </div>
              <p className="text-brand-100">Độ chính xác</p>
            </div>
          </div>
        </div>
      </section>

      {/* Final CTA Section */}
      <section className="py-20 bg-gradient-to-b from-white to-brand-50">
        <div className="container mx-auto px-4">
          <Card className="max-w-4xl mx-auto bg-gradient-to-br from-brand-500 to-blue-600 text-white border-none shadow-2xl">
            <div className="p-12 md:p-16 text-center">
              <h2 className="text-white mb-6">
                Sẵn Sàng Khai Thác Dữ Liệu Để Phát Triển Doanh Nghiệp?
              </h2>
              <p className="text-brand-50 mb-8 max-w-2xl mx-auto">
                Tham gia cùng hàng nghìn người bán đang phát triển vượt bậc với DSS Analytics. 
                Bắt đầu hành trình tăng trưởng của bạn ngay hôm nay!
              </p>
              <div className="flex flex-col sm:flex-row gap-4 justify-center">
                <Button size="lg" className="bg-white text-brand-600 hover:bg-brand-50">
                  Đăng Ký Ngay
                  <ArrowRight className="w-5 h-5 ml-2" />
                </Button>
                <Button size="lg" variant="outline" className="border-2 border-white text-white hover:bg-white/10">
                  Xem Demo Miễn Phí
                </Button>
              </div>
              <p className="text-brand-100 mt-6">
                Dùng thử miễn phí 14 ngày • Không cần thẻ tín dụng
              </p>
            </div>
          </Card>
        </div>
      </section>

      {/* <DSSFooter /> */}
    </div>
  );
}
