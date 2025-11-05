import { Card } from "../../../../components/ui/figma/card";
import { LineChart, Line, BarChart, Bar, PieChart, Pie, Cell, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from "recharts";
import { dashboardData, pieChartData } from "../../data/analystData";

export function VisualizationSection() {
  return (
    <section className="py-20 bg-gradient-to-br from-blue-50 via-white to-blue-50">
      <div className="container mx-auto px-4">
        <div className="text-center mb-16">
          <div className="inline-block px-4 py-2 bg-blue-100 text-blue-600 rounded-full text-sm mb-4">
            Trực Quan Hóa Dữ Liệu
          </div>
          <h2 className="text-3xl lg:text-4xl text-gray-900 mb-4">
            Biến Dữ Liệu Thành Insight
          </h2>
          <p className="text-lg text-gray-600 max-w-2xl mx-auto">
            Công cụ biểu đồ mạnh mẽ giúp bạn nhìn thấy xu hướng và đưa ra quyết định nhanh chóng
          </p>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 mb-8">
          {/* Line & Bar Chart Combined */}
          <Card className="p-6 bg-white">
            <h3 className="text-gray-900 mb-6">Xu Hướng Doanh Thu & Tăng Trưởng</h3>
            <ResponsiveContainer width="100%" height={300}>
              <LineChart data={dashboardData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
                <XAxis dataKey="month" stroke="#6B7280" className="text-xs" />
                <YAxis stroke="#6B7280" className="text-xs" />
                <Tooltip 
                  contentStyle={{ 
                    backgroundColor: 'white',
                    border: '1px solid #E5E7EB',
                    borderRadius: '8px',
                    boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)'
                  }}
                />
                <Legend />
                <Line 
                  type="monotone" 
                  dataKey="revenue" 
                  stroke="#3B82F6" 
                  strokeWidth={3}
                  dot={{ fill: '#3B82F6', r: 5 }}
                  name="Doanh Thu"
                />
                <Line 
                  type="monotone" 
                  dataKey="growth" 
                  stroke="#60A5FA" 
                  strokeWidth={3}
                  dot={{ fill: '#60A5FA', r: 5 }}
                  name="Tăng Trưởng"
                />
              </LineChart>
            </ResponsiveContainer>
          </Card>

          {/* Pie Chart */}
          <Card className="p-6 bg-white">
            <h3 className="text-gray-900 mb-6">Phân Bố Theo Bộ Phận</h3>
            <ResponsiveContainer width="100%" height={300}>
              <PieChart>
                <Pie
                  data={pieChartData}
                  cx="50%"
                  cy="50%"
                  labelLine={false}
                  // label={({ name, percent }) => `${name}: ${(percent * 100).toFixed(0)}%`}
                  outerRadius={100}
                  fill="#8884d8"
                  dataKey="value"
                >
                  {pieChartData.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={entry.color} />
                  ))}
                </Pie>
                <Tooltip 
                  contentStyle={{ 
                    backgroundColor: 'white',
                    border: '1px solid #E5E7EB',
                    borderRadius: '8px'
                  }}
                />
              </PieChart>
            </ResponsiveContainer>
          </Card>
        </div>

        {/* Bar Chart */}
        <Card className="p-6 bg-white">
          <h3 className="text-gray-900 mb-6">So Sánh Hiệu Suất & Mục Tiêu</h3>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={dashboardData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#E5E7EB" />
              <XAxis dataKey="month" stroke="#6B7280" className="text-xs" />
              <YAxis stroke="#6B7280" className="text-xs" />
              <Tooltip 
                contentStyle={{ 
                  backgroundColor: 'white',
                  border: '1px solid #E5E7EB',
                  borderRadius: '8px',
                  boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)'
                }}
              />
              <Legend />
              <Bar dataKey="revenue" fill="#3B82F6" radius={[8, 8, 0, 0]} name="Doanh Thu" />
              <Bar dataKey="target" fill="#93C5FD" radius={[8, 8, 0, 0]} name="Mục Tiêu" />
            </BarChart>
          </ResponsiveContainer>
        </Card>
      </div>
    </section>
  );
}
