import { useState } from "react";
import { BarChart3, LogOut, ArrowLeft, GitBranch, Plus, Play, TrendingUp, TrendingDown } from "lucide-react";
import { Button } from "../../components/ui/figma/button";
import { Card } from "../../components/ui/figma/card";
import { Badge } from "../../components/ui/figma/badge";
import { LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from "recharts";
import type { Page } from "../App";

interface ScenarioAnalysisProps {
  navigateTo: (page: Page) => void;
  onLogout: () => void;
}

export function ScenarioAnalysis({ navigateTo, onLogout }: ScenarioAnalysisProps) {
  const [selectedScenario, setSelectedScenario] = useState<number>(0);

  const scenarios = [
    {
      id: 1,
      name: "Mở Rộng Thị Trường Miền Bắc",
      status: "Đang chạy",
      roi: "+24.5%",
      risk: "Trung bình",
      timeline: "6 tháng",
    },
    {
      id: 2,
      name: "Tăng Đầu Tư Marketing",
      status: "Hoàn thành",
      roi: "+18.2%",
      risk: "Thấp",
      timeline: "3 tháng",
    },
    {
      id: 3,
      name: "Ra Mắt Sản Phẩm Mới",
      status: "Lập kế hoạch",
      roi: "+31.8%",
      risk: "Cao",
      timeline: "12 tháng",
    },
  ];

  const comparisonData = [
    { month: "T1", baseline: 45, scenario1: 48, scenario2: 52 },
    { month: "T2", baseline: 52, scenario1: 56, scenario2: 61 },
    { month: "T3", baseline: 48, scenario1: 54, scenario2: 64 },
    { month: "T4", baseline: 61, scenario1: 68, scenario2: 78 },
    { month: "T5", baseline: 55, scenario1: 65, scenario2: 82 },
    { month: "T6", baseline: 67, scenario1: 78, scenario2: 95 },
  ];

  const impactData = [
    { category: "Doanh Thu", current: 100, projected: 124 },
    { category: "Chi Phí", current: 100, projected: 108 },
    { category: "Lợi Nhuận", current: 100, projected: 145 },
    { category: "Thị Phần", current: 100, projected: 118 },
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
              <div className="bg-gradient-to-br from-blue-600 to-blue-700 p-2 rounded-lg">
                <GitBranch className="w-5 h-5 text-white" />
              </div>
              <span className="text-gray-900">Phân Tích Kịch Bản</span>
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
              Phân Tích Kịch Bản Kinh Doanh
            </h1>
            <p className="text-gray-600">
              Mô phỏng và đánh giá các kịch bản để tìm ra chiến lược tối ưu
            </p>
          </div>
          <Button className="bg-gradient-to-r from-blue-600 to-blue-700 hover:from-blue-700 hover:to-blue-800 gap-2">
            <Plus className="w-4 h-4" />
            Tạo Kịch Bản Mới
          </Button>
        </div>

        {/* Scenarios List */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
          {scenarios.map((scenario, index) => (
            <Card
              key={scenario.id}
              className={`p-6 cursor-pointer transition-all ${
                selectedScenario === index ? "border-blue-500 border-2 shadow-lg" : "hover:shadow-md"
              }`}
              onClick={() => setSelectedScenario(index)}
            >
              <div className="flex items-start justify-between mb-4">
                <h3 className="text-gray-900 flex-1">
                  {scenario.name}
                </h3>
                <Badge variant={scenario.status === "Hoàn thành" ? "default" : "secondary"}>
                  {scenario.status}
                </Badge>
              </div>
              <div className="space-y-3">
                <div className="flex items-center justify-between">
                  <span className="text-gray-600 text-sm">ROI Dự Kiến</span>
                  <span className="text-green-600 flex items-center gap-1">
                    <TrendingUp className="w-4 h-4" />
                    {scenario.roi}
                  </span>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-gray-600 text-sm">Mức Rủi Ro</span>
                  <span className={`text-sm ${
                    scenario.risk === "Thấp" ? "text-green-600" :
                    scenario.risk === "Trung bình" ? "text-yellow-600" : "text-red-600"
                  }`}>
                    {scenario.risk}
                  </span>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-gray-600 text-sm">Thời Gian</span>
                  <span className="text-gray-900 text-sm">{scenario.timeline}</span>
                </div>
              </div>
              <Button className="w-full mt-4 bg-blue-600 hover:bg-blue-700 gap-2">
                <Play className="w-4 h-4" />
                Chạy Mô Phỏng
              </Button>
            </Card>
          ))}
        </div>

        {/* Comparison Chart */}
        <Card className="p-6 mb-8">
          <h2 className="text-gray-900 mb-6">
            So Sánh Kịch Bản
          </h2>
          <ResponsiveContainer width="100%" height={350}>
            <LineChart data={comparisonData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis dataKey="month" stroke="#6b7280" />
              <YAxis stroke="#6b7280" />
              <Tooltip />
              <Legend />
              <Line type="monotone" dataKey="baseline" stroke="#9ca3af" strokeWidth={2} name="Kịch bản hiện tại" />
              <Line type="monotone" dataKey="scenario1" stroke="#3b82f6" strokeWidth={2} name="Kịch bản 1" />
              <Line type="monotone" dataKey="scenario2" stroke="#10b981" strokeWidth={2} name="Kịch bản 2" />
            </LineChart>
          </ResponsiveContainer>
        </Card>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
          {/* Impact Analysis */}
          <Card className="p-6">
            <h3 className="text-gray-900 mb-6">
              Phân Tích Tác Động
            </h3>
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={impactData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis dataKey="category" stroke="#6b7280" />
                <YAxis stroke="#6b7280" />
                <Tooltip />
                <Legend />
                <Bar dataKey="current" fill="#9ca3af" name="Hiện tại" />
                <Bar dataKey="projected" fill="#3b82f6" name="Dự kiến" />
              </BarChart>
            </ResponsiveContainer>
          </Card>

          {/* Key Metrics */}
          <Card className="p-6">
            <h3 className="text-gray-900 mb-6">
              Chỉ Số Quan Trọng
            </h3>
            <div className="space-y-4">
              <div className="p-4 bg-green-50 rounded-lg border border-green-200">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-green-900">Lợi Nhuận Tăng Thêm</span>
                  <TrendingUp className="w-5 h-5 text-green-600" />
                </div>
                <p className="text-green-600">+2.4 tỷ VNĐ</p>
                <p className="text-green-700 text-sm mt-1">+45% so với kịch bản hiện tại</p>
              </div>

              <div className="p-4 bg-blue-50 rounded-lg border border-blue-200">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-blue-900">Thị Phần Mục Tiêu</span>
                  <BarChart3 className="w-5 h-5 text-blue-600" />
                </div>
                <p className="text-blue-600">18.5%</p>
                <p className="text-blue-700 text-sm mt-1">+4.2% so với hiện tại</p>
              </div>

              <div className="p-4 bg-yellow-50 rounded-lg border border-yellow-200">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-yellow-900">Thời Gian Hoàn Vốn</span>
                  <TrendingDown className="w-5 h-5 text-yellow-600" />
                </div>
                <p className="text-yellow-600">8.5 tháng</p>
                <p className="text-yellow-700 text-sm mt-1">Nhanh hơn 3.5 tháng</p>
              </div>
            </div>
          </Card>
        </div>
      </main>
    </div>
  );
}
