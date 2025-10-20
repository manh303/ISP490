import { BarChart3, LogOut, ArrowLeft, TrendingUp, Brain, Sparkles, AlertCircle, CheckCircle } from "lucide-react";
import { Button } from "../../components/ui/figma/button";
import { Card } from "../../components/ui/figma/card";
import { Badge } from "../../components/ui/figma/badge";
import { Progress } from "../../components/ui/figma/progress";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, RadarChart, PolarGrid, PolarAngleAxis, PolarRadiusAxis, Radar } from "recharts";
import type { Page } from "../App";

interface MarketForecastProps {
  navigateTo: (page: Page) => void;
  onLogout: () => void;
}

export function MarketForecast({ navigateTo, onLogout }: MarketForecastProps) {
  const forecastData = [
    { month: "T1", actual: 45.2, forecast: null, lower: null, upper: null },
    { month: "T2", actual: 52.8, forecast: null, lower: null, upper: null },
    { month: "T3", actual: 48.6, forecast: null, lower: null, upper: null },
    { month: "T4", actual: 61.5, forecast: null, lower: null, upper: null },
    { month: "T5", actual: 58.9, forecast: null, lower: null, upper: null },
    { month: "T6", actual: 67.3, forecast: null, lower: null, upper: null },
    { month: "T7", actual: null, forecast: 72.5, lower: 68, upper: 77 },
    { month: "T8", actual: null, forecast: 78.2, lower: 72, upper: 84 },
    { month: "T9", actual: null, forecast: 75.8, lower: 69, upper: 82 },
    { month: "T10", actual: null, forecast: 82.4, lower: 75, upper: 90 },
    { month: "T11", actual: null, forecast: 88.6, lower: 80, upper: 97 },
    { month: "T12", actual: null, forecast: 95.3, lower: 86, upper: 105 },
  ];

  const trendData = [
    { factor: "Cạnh tranh", score: 85 },
    { factor: "Nhu cầu", score: 92 },
    { factor: "Giá cả", score: 78 },
    { factor: "Mùa vụ", score: 65 },
    { factor: "Kinh tế", score: 88 },
  ];

  const insights = [
    {
      type: "positive",
      title: "Xu Hướng Tăng Trưởng Mạnh",
      description: "Thị trường dự kiến tăng 41.6% trong 6 tháng tới",
      confidence: 95,
    },
    {
      type: "warning",
      title: "Cạnh Tranh Gia Tăng",
      description: "3 đối thủ mới gia nhập thị trường trong Q3",
      confidence: 82,
    },
    {
      type: "positive",
      title: "Nhu Cầu Cao Điểm",
      description: "Dự báo nhu cầu tăng mạnh trong mùa lễ hội cuối năm",
      confidence: 89,
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
              <div className="bg-gradient-to-br from-yellow-600 to-yellow-700 p-2 rounded-lg">
                <TrendingUp className="w-5 h-5 text-white" />
              </div>
              <span className="text-gray-900">Dự Báo Xu Hướng</span>
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
          <div className="flex items-center gap-3 mb-2">
            <h1 className="text-gray-900">
              Dự Báo Xu Hướng Thị Trường
            </h1>
            <Badge className="bg-gradient-to-r from-yellow-500 to-yellow-600 text-white gap-1">
              <Sparkles className="w-3 h-3" />
              AI-Powered
            </Badge>
          </div>
          <p className="text-gray-600">
            Dự đoán xu hướng thị trường sử dụng Machine Learning và phân tích dữ liệu lớn
          </p>
        </div>

        {/* Model Accuracy */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
          <Card className="p-6 bg-gradient-to-br from-yellow-500 to-yellow-600 text-white">
            <div className="flex items-start justify-between mb-4">
              <Brain className="w-8 h-8 opacity-80" />
              <Sparkles className="w-5 h-5" />
            </div>
            <p className="text-yellow-100 text-sm mb-1">Độ Chính Xác Mô Hình</p>
            <p className="mb-2">95.3%</p>
            <Progress value={95} className="bg-white/20 h-2" />
          </Card>

          <Card className="p-6">
            <div className="flex items-start justify-between mb-4">
              <div className="p-3 rounded-lg bg-blue-100">
                <BarChart3 className="w-6 h-6 text-blue-600" />
              </div>
            </div>
            <p className="text-gray-600 text-sm mb-1">Điểm Dữ Liệu</p>
            <p className="text-gray-900">12,458</p>
            <p className="text-gray-500 text-xs mt-1">Dữ liệu từ 3 năm qua</p>
          </Card>

          <Card className="p-6">
            <div className="flex items-start justify-between mb-4">
              <div className="p-3 rounded-lg bg-green-100">
                <CheckCircle className="w-6 h-6 text-green-600" />
              </div>
            </div>
            <p className="text-gray-600 text-sm mb-1">Cập Nhật Cuối</p>
            <p className="text-gray-900">2 giờ trước</p>
            <p className="text-gray-500 text-xs mt-1">Tự động mỗi 4 giờ</p>
          </Card>
        </div>

        {/* Forecast Chart */}
        <Card className="p-6 mb-8">
          <h2 className="text-gray-900 mb-6">
            Dự Báo Doanh Thu 6 Tháng Tới
          </h2>
          <ResponsiveContainer width="100%" height={400}>
            <LineChart data={forecastData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis dataKey="month" stroke="#6b7280" />
              <YAxis stroke="#6b7280" />
              <Tooltip />
              <Legend />
              <Line 
                type="monotone" 
                dataKey="actual" 
                stroke="#3b82f6" 
                strokeWidth={3} 
                name="Thực tế (M VNĐ)"
                dot={{ fill: '#3b82f6', r: 5 }}
              />
              <Line 
                type="monotone" 
                dataKey="forecast" 
                stroke="#eab308" 
                strokeWidth={3} 
                strokeDasharray="5 5"
                name="Dự báo (M VNĐ)"
                dot={{ fill: '#eab308', r: 5 }}
              />
              <Line 
                type="monotone" 
                dataKey="upper" 
                stroke="#fbbf24" 
                strokeWidth={1} 
                strokeDasharray="3 3"
                name="Giới hạn trên"
                dot={false}
              />
              <Line 
                type="monotone" 
                dataKey="lower" 
                stroke="#fbbf24" 
                strokeWidth={1} 
                strokeDasharray="3 3"
                name="Giới hạn dưới"
                dot={false}
              />
            </LineChart>
          </ResponsiveContainer>
        </Card>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
          {/* Market Factors */}
          <Card className="p-6">
            <h3 className="text-gray-900 mb-6">
              Yếu Tố Ảnh Hưởng Thị Trường
            </h3>
            <ResponsiveContainer width="100%" height={300}>
              <RadarChart data={trendData}>
                <PolarGrid stroke="#e5e7eb" />
                <PolarAngleAxis dataKey="factor" stroke="#6b7280" />
                <PolarRadiusAxis angle={90} domain={[0, 100]} stroke="#6b7280" />
                <Radar name="Điểm ảnh hưởng" dataKey="score" stroke="#eab308" fill="#eab308" fillOpacity={0.6} />
                <Tooltip />
              </RadarChart>
            </ResponsiveContainer>
            <div className="mt-6 space-y-3">
              {trendData.map((trend) => (
                <div key={trend.factor}>
                  <div className="flex items-center justify-between mb-1">
                    <span className="text-gray-700 text-sm">{trend.factor}</span>
                    <span className="text-gray-900 text-sm">{trend.score}/100</span>
                  </div>
                  <Progress value={trend.score} className="h-2" />
                </div>
              ))}
            </div>
          </Card>

          {/* AI Insights */}
          <Card className="p-6">
            <h3 className="text-gray-900 mb-6">
              Nhận Định AI
            </h3>
            <div className="space-y-4">
              {insights.map((insight, index) => (
                <div 
                  key={index}
                  className={`p-4 rounded-lg border ${
                    insight.type === "positive" 
                      ? "bg-green-50 border-green-200" 
                      : "bg-yellow-50 border-yellow-200"
                  }`}
                >
                  <div className="flex items-start gap-3 mb-2">
                    {insight.type === "positive" ? (
                      <CheckCircle className="w-5 h-5 text-green-600 flex-shrink-0 mt-0.5" />
                    ) : (
                      <AlertCircle className="w-5 h-5 text-yellow-600 flex-shrink-0 mt-0.5" />
                    )}
                    <div className="flex-1">
                      <h4 className={`${
                        insight.type === "positive" ? "text-green-900" : "text-yellow-900"
                      } mb-1`}>
                        {insight.title}
                      </h4>
                      <p className={`text-sm ${
                        insight.type === "positive" ? "text-green-700" : "text-yellow-700"
                      }`}>
                        {insight.description}
                      </p>
                    </div>
                  </div>
                  <div className="flex items-center gap-2 mt-3">
                    <Progress value={insight.confidence} className="flex-1 h-1.5" />
                    <span className={`text-xs ${
                      insight.type === "positive" ? "text-green-600" : "text-yellow-600"
                    }`}>
                      {insight.confidence}% tin cậy
                    </span>
                  </div>
                </div>
              ))}
            </div>

            <div className="mt-6 p-4 bg-gray-50 rounded-lg border border-gray-200">
              <div className="flex items-start gap-2">
                <Brain className="w-5 h-5 text-gray-600 flex-shrink-0 mt-0.5" />
                <div>
                  <p className="text-gray-900 text-sm mb-1">Khuyến Nghị Chiến Lược</p>
                  <p className="text-gray-600 text-sm">
                    Tăng cường đầu tư marketing trong Q3 và Q4 để tận dụng xu hướng tăng trưởng. 
                    Chuẩn bị nguồn lực đối phó với cạnh tranh gia tăng.
                  </p>
                </div>
              </div>
            </div>
          </Card>
        </div>
      </main>
    </div>
  );
}
