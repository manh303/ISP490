import { Button } from "../../../../components/ui/figma/button";
import { Card } from "../../../../components/ui/figma/card";
import { ArrowRight, TrendingUp, PieChart, BarChart3, Activity } from "lucide-react";

export function AnalystHero() {
  return (
    <section className="bg-gradient-to-br from-blue-50 via-white to-blue-50 py-20">
      <div className="container mx-auto px-4">
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-12 items-center">
          {/* Left Content */}
          <div className="space-y-6">
            <div className="inline-block px-4 py-2 bg-blue-100 text-blue-600 rounded-full text-sm">
              Dành cho Nhà Phân Tích
            </div>
            
            <h1 className="text-4xl lg:text-5xl text-gray-900">
              Phân Tích Dữ Liệu – Hiểu Sâu Hơn, Quyết Định Tốt Hơn
            </h1>
            
            <p className="text-lg text-gray-600">
              Dành cho nhà phân tích – người biến dữ liệu thành chiến lược, dự báo và hành động.
            </p>

            <div className="flex flex-col sm:flex-row gap-4">
              <Button size="lg" className="bg-blue-600 hover:bg-blue-700 group">
                Truy Cập Dashboard
                <ArrowRight className="w-4 h-4 ml-2 group-hover:translate-x-1 transition-transform" />
              </Button>
              <Button size="lg" variant="outline" className="border-blue-600 text-blue-600 hover:bg-blue-50">
                Xem Demo
              </Button>
            </div>

            {/* Mini Stats */}
            <div className="grid grid-cols-2 gap-4 pt-8">
              <div>
                <p className="text-3xl text-blue-600">247+</p>
                <p className="text-sm text-gray-600">Dự Án Hoàn Thành</p>
              </div>
              <div>
                <p className="text-3xl text-blue-600">98.5%</p>
                <p className="text-sm text-gray-600">Độ Chính Xác</p>
              </div>
            </div>
          </div>

          {/* Right - Dashboard Illustration */}
          <div className="relative">
            <Card className="p-6 bg-white shadow-2xl">
              <div className="space-y-4">
                {/* Dashboard Header */}
                <div className="flex items-center justify-between pb-4 border-b">
                  <h3 className="text-gray-900">Dashboard Analytics</h3>
                  <div className="flex gap-2">
                    <div className="w-3 h-3 rounded-full bg-red-400" />
                    <div className="w-3 h-3 rounded-full bg-yellow-400" />
                    <div className="w-3 h-3 rounded-full bg-green-400" />
                  </div>
                </div>

                {/* Charts Grid */}
                <div className="grid grid-cols-2 gap-4">
                  <div className="bg-gradient-to-br from-blue-500 to-blue-600 rounded-lg p-4 text-white">
                    <TrendingUp className="w-6 h-6 mb-2 opacity-80" />
                    <p className="text-2xl mb-1">+23.5%</p>
                    <p className="text-xs opacity-80">Tăng Trưởng</p>
                  </div>
                  <div className="bg-gray-50 rounded-lg p-4 flex items-center justify-center">
                    <PieChart className="w-12 h-12 text-blue-500" />
                  </div>
                  <div className="bg-gray-50 rounded-lg p-4 flex items-center justify-center">
                    <BarChart3 className="w-12 h-12 text-blue-500" />
                  </div>
                  <div className="bg-gradient-to-br from-blue-100 to-blue-200 rounded-lg p-4">
                    <Activity className="w-6 h-6 mb-2 text-blue-600" />
                    <p className="text-2xl mb-1 text-gray-900">156</p>
                    <p className="text-xs text-gray-600">Báo Cáo</p>
                  </div>
                </div>

                {/* Data Bars */}
                <div className="space-y-2 pt-2">
                  <div className="flex items-center gap-2">
                    <div className="h-2 bg-blue-500 rounded-full" style={{ width: "75%" }} />
                    <span className="text-xs text-gray-500">75%</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <div className="h-2 bg-blue-400 rounded-full" style={{ width: "60%" }} />
                    <span className="text-xs text-gray-500">60%</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <div className="h-2 bg-blue-300 rounded-full" style={{ width: "85%" }} />
                    <span className="text-xs text-gray-500">85%</span>
                  </div>
                </div>
              </div>
            </Card>

            {/* Floating Elements */}
            <div className="absolute -top-4 -right-4 w-20 h-20 bg-blue-500 rounded-full opacity-10 blur-2xl" />
            <div className="absolute -bottom-4 -left-4 w-32 h-32 bg-blue-400 rounded-full opacity-10 blur-3xl" />
          </div>
        </div>
      </div>
    </section>
  );
}
