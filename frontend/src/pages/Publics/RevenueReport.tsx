import { BarChart3, LogOut, ArrowLeft, DollarSign, TrendingUp, TrendingDown, Download, Calendar } from "lucide-react";
import { Button } from "../../components/ui/figma/button";
import { Card } from "../../components/ui/figma/card";
import { Badge } from "../../components/ui/figma/badge";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "../../components/ui/figma/select";
import { AreaChart, Area, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, PieChart, Pie, Cell } from "recharts";
import type { Page } from "../App";

interface RevenueReportProps {
  navigateTo: (page: Page) => void;
  onLogout: () => void;
}

export function RevenueReport({ navigateTo, onLogout }: RevenueReportProps) {
  const revenueData = [
    { time: "00:00", revenue: 245, orders: 12 },
    { time: "04:00", revenue: 180, orders: 8 },
    { time: "08:00", revenue: 520, orders: 28 },
    { time: "12:00", revenue: 890, orders: 45 },
    { time: "16:00", revenue: 1240, orders: 62 },
    { time: "20:00", revenue: 980, orders: 51 },
    { time: "23:00", revenue: 750, orders: 38 },
  ];

  const categoryData = [
    { name: "Điện tử", value: 35, color: "#3b82f6" },
    { name: "Thời trang", value: 25, color: "#ef4444" },
    { name: "Thực phẩm", value: 20, color: "#f59e0b" },
    { name: "Gia dụng", value: 15, color: "#10b981" },
    { name: "Khác", value: 5, color: "#6b7280" },
  ];

  const monthlyData = [
    { month: "T1", revenue: 45.2, target: 40 },
    { month: "T2", revenue: 52.8, target: 45 },
    { month: "T3", revenue: 48.6, target: 50 },
    { month: "T4", revenue: 61.5, target: 55 },
    { month: "T5", revenue: 58.9, target: 58 },
    { month: "T6", revenue: 67.3, target: 60 },
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
              <div className="bg-gradient-to-br from-red-600 to-red-700 p-2 rounded-lg">
                <DollarSign className="w-5 h-5 text-white" />
              </div>
              <span className="text-gray-900">Báo Cáo Doanh Thu</span>
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
        <div className="mb-8 flex items-center justify-between">
          <div>
            <h1 className="text-gray-900 mb-2">
              Báo Cáo Doanh Thu Thời Gian Thực
            </h1>
            <p className="text-gray-600">
              Theo dõi và phân tích doanh thu chi tiết theo thời gian
            </p>
          </div>
          <div className="flex items-center gap-3">
            <Select defaultValue="today">
              <SelectTrigger className="w-40">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="today">Hôm nay</SelectItem>
                <SelectItem value="week">Tuần này</SelectItem>
                <SelectItem value="month">Tháng này</SelectItem>
                <SelectItem value="year">Năm này</SelectItem>
              </SelectContent>
            </Select>
            <Button className="bg-gradient-to-r from-red-600 to-red-700 hover:from-red-700 hover:to-red-800 gap-2">
              <Download className="w-4 h-4" />
              Xuất Báo Cáo
            </Button>
          </div>
        </div>

        {/* Quick Stats */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
          <Card className="p-6 bg-gradient-to-br from-red-500 to-red-600 text-white">
            <div className="flex items-start justify-between mb-4">
              <DollarSign className="w-8 h-8 opacity-80" />
              <Badge className="bg-white/20 text-white border-white/30">
                Hôm nay
              </Badge>
            </div>
            <p className="text-red-100 text-sm mb-1">Tổng Doanh Thu</p>
            <p className="mb-2">4.8M VNĐ</p>
            <span className="text-red-100 text-sm flex items-center gap-1">
              <TrendingUp className="w-4 h-4" />
              +12.5% so với hôm qua
            </span>
          </Card>

          <Card className="p-6">
            <div className="flex items-start justify-between mb-4">
              <div className="p-3 rounded-lg bg-blue-100">
                <Calendar className="w-6 h-6 text-blue-600" />
              </div>
              <span className="text-sm text-green-600 flex items-center gap-1">
                <TrendingUp className="w-4 h-4" />
                +8.2%
              </span>
            </div>
            <p className="text-gray-600 text-sm mb-1">Đơn Hàng</p>
            <p className="text-gray-900">244 đơn</p>
          </Card>

          <Card className="p-6">
            <div className="flex items-start justify-between mb-4">
              <div className="p-3 rounded-lg bg-yellow-100">
                <BarChart3 className="w-6 h-6 text-yellow-600" />
              </div>
              <span className="text-sm text-green-600 flex items-center gap-1">
                <TrendingUp className="w-4 h-4" />
                +5.1%
              </span>
            </div>
            <p className="text-gray-600 text-sm mb-1">Giá Trị TB</p>
            <p className="text-gray-900">19.7K VNĐ</p>
          </Card>

          <Card className="p-6">
            <div className="flex items-start justify-between mb-4">
              <div className="p-3 rounded-lg bg-green-100">
                <TrendingUp className="w-6 h-6 text-green-600" />
              </div>
              <span className="text-sm text-red-600 flex items-center gap-1">
                <TrendingDown className="w-4 h-4" />
                -2.3%
              </span>
            </div>
            <p className="text-gray-600 text-sm mb-1">Tỷ Lệ Chuyển Đổi</p>
            <p className="text-gray-900">3.8%</p>
          </Card>
        </div>

        {/* Real-time Revenue Chart */}
        <Card className="p-6 mb-8">
          <div className="flex items-center justify-between mb-6">
            <h2 className="text-gray-900">
              Doanh Thu Theo Giờ
            </h2>
            <div className="flex items-center gap-2">
              <div className="w-3 h-3 bg-green-500 rounded-full animate-pulse" />
              <span className="text-sm text-gray-600">Cập nhật trực tiếp</span>
            </div>
          </div>
          <ResponsiveContainer width="100%" height={350}>
            <AreaChart data={revenueData}>
              <defs>
                <linearGradient id="colorRevenue" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#ef4444" stopOpacity={0.3} />
                  <stop offset="95%" stopColor="#ef4444" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis dataKey="time" stroke="#6b7280" />
              <YAxis stroke="#6b7280" />
              <Tooltip />
              <Area type="monotone" dataKey="revenue" stroke="#ef4444" fillOpacity={1} fill="url(#colorRevenue)" strokeWidth={2} name="Doanh thu (K VNĐ)" />
            </AreaChart>
          </ResponsiveContainer>
        </Card>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
          {/* Category Distribution */}
          <Card className="p-6">
            <h3 className="text-gray-900 mb-6">
              Phân Bố Theo Danh Mục
            </h3>
            <ResponsiveContainer width="100%" height={300}>
              <PieChart>
                <Pie
                  data={categoryData}
                  cx="50%"
                  cy="50%"
                  labelLine={false}
                  label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}
                  outerRadius={100}
                  fill="#8884d8"
                  dataKey="value"
                >
                  {categoryData.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={entry.color} />
                  ))}
                </Pie>
                <Tooltip />
              </PieChart>
            </ResponsiveContainer>
            <div className="mt-6 space-y-2">
              {categoryData.map((cat) => (
                <div key={cat.name} className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <div className="w-3 h-3 rounded-full" style={{ backgroundColor: cat.color }} />
                    <span className="text-gray-700 text-sm">{cat.name}</span>
                  </div>
                  <span className="text-gray-900 text-sm">{cat.value}%</span>
                </div>
              ))}
            </div>
          </Card>

          {/* Monthly Performance */}
          <Card className="p-6">
            <h3 className="text-gray-900 mb-6">
              Hiệu Suất Theo Tháng
            </h3>
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={monthlyData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis dataKey="month" stroke="#6b7280" />
                <YAxis stroke="#6b7280" />
                <Tooltip />
                <Legend />
                <Bar dataKey="revenue" fill="#ef4444" name="Doanh thu (M VNĐ)" />
                <Bar dataKey="target" fill="#fca5a5" name="Mục tiêu (M VNĐ)" />
              </BarChart>
            </ResponsiveContainer>
            <div className="mt-6 grid grid-cols-2 gap-4">
              <div className="p-4 bg-red-50 rounded-lg border border-red-200">
                <p className="text-red-900 text-sm mb-1">Tổng 6 Tháng</p>
                <p className="text-red-600">334.3M VNĐ</p>
              </div>
              <div className="p-4 bg-green-50 rounded-lg border border-green-200">
                <p className="text-green-900 text-sm mb-1">Đạt Mục Tiêu</p>
                <p className="text-green-600">108.5%</p>
              </div>
            </div>
          </Card>
        </div>
      </main>
    </div>
  );
}
