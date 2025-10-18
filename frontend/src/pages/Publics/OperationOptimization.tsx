import { BarChart3, LogOut, ArrowLeft, Settings, Zap, Clock, Package, Users, CheckCircle, XCircle, AlertTriangle } from "lucide-react";
import { Button } from "../../components/ui/figma/button";
import { Card } from "../../components/ui/figma/card";
import { Badge } from "../../components/ui/figma/badge";
import { Progress } from "../../components/ui/figma/progress";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, LineChart, Line } from "recharts";
import type { Page } from "../App";

interface OperationOptimizationProps {
  navigateTo: (page: Page) => void;
  onLogout: () => void;
}

export function OperationOptimization({ navigateTo, onLogout }: OperationOptimizationProps) {
  const efficiencyData = [
    { department: "Sản xuất", current: 78, target: 85, optimized: 92 },
    { department: "Logistics", current: 82, target: 88, optimized: 95 },
    { department: "Bán hàng", current: 75, target: 80, optimized: 88 },
    { department: "CSKH", current: 88, target: 90, optimized: 96 },
    { department: "IT", current: 85, target: 90, optimized: 94 },
  ];

  const timelineData = [
    { week: "T1", before: 45, after: 38 },
    { week: "T2", before: 48, after: 36 },
    { week: "T3", before: 46, after: 35 },
    { week: "T4", before: 52, after: 37 },
    { week: "T5", before: 49, after: 34 },
    { week: "T6", before: 51, after: 33 },
  ];

  const optimizations = [
    {
      id: 1,
      title: "Tự Động Hóa Quy Trình Nhập Liệu",
      impact: "Cao",
      savings: "2.4M VNĐ/tháng",
      time: "Tiết kiệm 120 giờ/tháng",
      status: "Đang triển khai",
      progress: 75,
      icon: Zap,
    },
    {
      id: 2,
      title: "Tối Ưu Lộ Trình Giao Hàng",
      impact: "Trung bình",
      savings: "1.8M VNĐ/tháng",
      time: "Giảm 15% thời gian giao hàng",
      status: "Hoàn thành",
      progress: 100,
      icon: Package,
    },
    {
      id: 3,
      title: "Cải Thiện Quy Trình Đào Tạo",
      impact: "Cao",
      savings: "3.2M VNĐ/tháng",
      time: "Tăng 25% hiệu suất nhân viên",
      status: "Đề xuất",
      progress: 0,
      icon: Users,
    },
  ];

  const issues = [
    {
      type: "critical",
      title: "Quy Trình Phê Duyệt Chậm",
      description: "Thời gian phê duyệt trung bình 5.2 ngày, cao hơn 60% so với tiêu chuẩn",
      department: "Hành chính",
    },
    {
      type: "warning",
      title: "Tỷ Lệ Lỗi Cao",
      description: "8.5% đơn hàng bị lỗi trong khâu đóng gói",
      department: "Kho vận",
    },
    {
      type: "info",
      title: "Cơ Hội Cải Thiện",
      description: "Có thể giảm 30% thời gian xử lý bằng automation",
      department: "CSKH",
    },
  ];

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white border-b border-gray-200 shadow-sm">
        <div className="max-w-7xl mx-auto px-6 h-20 flex items-center justify-between">
          <div className="flex items-center gap-4">
            <Button variant="ghost" onClick={() => navigateTo("dashboard")} className="gap-2">
              <ArrowLeft className="w-4 h-4" />
              Dashboard
            </Button>
            <div className="h-8 w-px bg-gray-200" />
            <div className="flex items-center gap-2">
              <div className="bg-gradient-to-br from-green-600 to-green-700 p-2 rounded-lg">
                <Settings className="w-5 h-5 text-white" />
              </div>
              <span className="text-gray-900">Tối Ưu Vận Hành</span>
            </div>
          </div>

          <div className="flex items-center gap-4">
            <Button variant="outline" onClick={() => navigateTo("home")}>
              Trang Chủ
            </Button>
            <Button variant="ghost" onClick={onLogout} className="gap-2">
              <LogOut className="w-4 h-4" />
              Đăng Xuất
            </Button>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="max-w-7xl mx-auto px-6 py-8">
        {/* Page Title */}
        <div className="mb-8">
          <h1 className="text-gray-900 mb-2">
            Tối Ưu Hóa Vận Hành
          </h1>
          <p className="text-gray-600">
            Phân tích và cải thiện hiệu suất vận hành doanh nghiệp
          </p>
        </div>

        {/* Quick Metrics */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
          <Card className="p-6 bg-gradient-to-br from-green-500 to-green-600 text-white">
            <div className="flex items-start justify-between mb-4">
              <Zap className="w-8 h-8 opacity-80" />
            </div>
            <p className="text-green-100 text-sm mb-1">Hiệu Suất Tổng Thể</p>
            <p className="mb-2">82.4%</p>
            <span className="text-green-100 text-sm">+18% so với quý trước</span>
          </Card>

          <Card className="p-6">
            <div className="flex items-start justify-between mb-4">
              <div className="p-3 rounded-lg bg-blue-100">
                <Clock className="w-6 h-6 text-blue-600" />
              </div>
            </div>
            <p className="text-gray-600 text-sm mb-1">Thời Gian Xử Lý TB</p>
            <p className="text-gray-900">35 phút</p>
            <p className="text-green-600 text-sm">Giảm 26%</p>
          </Card>

          <Card className="p-6">
            <div className="flex items-start justify-between mb-4">
              <div className="p-3 rounded-lg bg-yellow-100">
                <Package className="w-6 h-6 text-yellow-600" />
              </div>
            </div>
            <p className="text-gray-600 text-sm mb-1">Chi Phí Vận Hành</p>
            <p className="text-gray-900">45.2M VNĐ</p>
            <p className="text-green-600 text-sm">Tiết kiệm 15%</p>
          </Card>

          <Card className="p-6">
            <div className="flex items-start justify-between mb-4">
              <div className="p-3 rounded-lg bg-purple-100">
                <Users className="w-6 h-6 text-purple-600" />
              </div>
            </div>
            <p className="text-gray-600 text-sm mb-1">Năng Suất Nhân Viên</p>
            <p className="text-gray-900">94.5%</p>
            <p className="text-green-600 text-sm">+12%</p>
          </Card>
        </div>

        {/* Efficiency Analysis */}
        <Card className="p-6 mb-8">
          <h2 className="text-gray-900 mb-6">
            Phân Tích Hiệu Suất Theo Phòng Ban
          </h2>
          <ResponsiveContainer width="100%" height={350}>
            <BarChart data={efficiencyData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis dataKey="department" stroke="#6b7280" />
              <YAxis stroke="#6b7280" />
              <Tooltip />
              <Legend />
              <Bar dataKey="current" fill="#9ca3af" name="Hiện tại (%)" />
              <Bar dataKey="target" fill="#fbbf24" name="Mục tiêu (%)" />
              <Bar dataKey="optimized" fill="#10b981" name="Sau tối ưu (%)" />
            </BarChart>
          </ResponsiveContainer>
        </Card>

        {/* Optimization Projects */}
        <div className="mb-8">
          <h2 className="text-gray-900 mb-6">
            Các Dự Án Tối Ưu Hóa
          </h2>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            {optimizations.map((opt) => {
              const Icon = opt.icon;
              const statusColor = 
                opt.status === "Hoàn thành" ? "green" :
                opt.status === "Đang triển khai" ? "blue" : "gray";
              
              return (
                <Card key={opt.id} className="p-6">
                  <div className="flex items-start justify-between mb-4">
                    <div className={`p-3 rounded-lg bg-${statusColor}-100`}>
                      <Icon className={`w-6 h-6 text-${statusColor}-600`} />
                    </div>
                    <Badge 
                      variant={opt.impact === "Cao" ? "default" : "secondary"}
                      className={opt.impact === "Cao" ? "bg-orange-100 text-orange-700" : ""}
                    >
                      {opt.impact}
                    </Badge>
                  </div>
                  
                  <h3 className="text-gray-900 mb-3">
                    {opt.title}
                  </h3>
                  
                  <div className="space-y-2 mb-4">
                    <div className="flex items-center gap-2 text-sm">
                      <span className="text-gray-600">Tiết kiệm:</span>
                      <span className="text-green-600">{opt.savings}</span>
                    </div>
                    <div className="flex items-center gap-2 text-sm">
                      <span className="text-gray-600">Lợi ích:</span>
                      <span className="text-blue-600">{opt.time}</span>
                    </div>
                  </div>

                  <div className="space-y-2">
                    <div className="flex items-center justify-between text-sm">
                      <span className="text-gray-600">{opt.status}</span>
                      <span className="text-gray-900">{opt.progress}%</span>
                    </div>
                    <Progress value={opt.progress} className="h-2" />
                  </div>

                  <Button 
                    className="w-full mt-4 bg-green-600 hover:bg-green-700"
                    variant={opt.status === "Hoàn thành" ? "outline" : "default"}
                  >
                    {opt.status === "Hoàn thành" ? "Xem Chi Tiết" : 
                     opt.status === "Đang triển khai" ? "Theo Dõi" : "Bắt Đầu"}
                  </Button>
                </Card>
              );
            })}
          </div>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
          {/* Timeline Improvement */}
          <Card className="p-6">
            <h3 className="text-gray-900 mb-6">
              Cải Thiện Theo Thời Gian
            </h3>
            <ResponsiveContainer width="100%" height={300}>
              <LineChart data={timelineData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis dataKey="week" stroke="#6b7280" />
                <YAxis stroke="#6b7280" />
                <Tooltip />
                <Legend />
                <Line type="monotone" dataKey="before" stroke="#ef4444" strokeWidth={2} name="Trước tối ưu (phút)" />
                <Line type="monotone" dataKey="after" stroke="#10b981" strokeWidth={2} name="Sau tối ưu (phút)" />
              </LineChart>
            </ResponsiveContainer>
            <div className="mt-6 grid grid-cols-2 gap-4">
              <div className="p-4 bg-red-50 rounded-lg border border-red-200">
                <p className="text-red-900 text-sm mb-1">TB Trước</p>
                <p className="text-red-600">48.5 phút</p>
              </div>
              <div className="p-4 bg-green-50 rounded-lg border border-green-200">
                <p className="text-green-900 text-sm mb-1">TB Sau</p>
                <p className="text-green-600">35.5 phút</p>
              </div>
            </div>
          </Card>

          {/* Issues & Opportunities */}
          <Card className="p-6">
            <h3 className="text-gray-900 mb-6">
              Vấn Đề & Cơ Hội
            </h3>
            <div className="space-y-4">
              {issues.map((issue, index) => {
                const icons = {
                  critical: { icon: XCircle, color: "red" },
                  warning: { icon: AlertTriangle, color: "yellow" },
                  info: { icon: CheckCircle, color: "blue" },
                };
                const { icon: Icon, color } = icons[issue.type as keyof typeof icons];
                
                return (
                  <div 
                    key={index}
                    className={`p-4 rounded-lg border bg-${color}-50 border-${color}-200`}
                  >
                    <div className="flex items-start gap-3">
                      <Icon className={`w-5 h-5 text-${color}-600 flex-shrink-0 mt-0.5`} />
                      <div className="flex-1">
                        <div className="flex items-center justify-between mb-1">
                          <h4 className={`text-${color}-900`}>
                            {issue.title}
                          </h4>
                          <Badge variant="outline" className={`text-${color}-700 border-${color}-300`}>
                            {issue.department}
                          </Badge>
                        </div>
                        <p className={`text-sm text-${color}-700`}>
                          {issue.description}
                        </p>
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>

            <Button className="w-full mt-6 bg-green-600 hover:bg-green-700">
              Tạo Kế Hoạch Hành Động
            </Button>
          </Card>
        </div>
      </main>
    </div>
  );
}
