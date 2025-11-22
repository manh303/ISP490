import { Card, CardContent, CardHeader, CardTitle } from '../../../components/ui/figma/card';
import { TrendingUp, Target, Cpu, Activity } from 'lucide-react';
import { LineChart, Line, ResponsiveContainer } from 'recharts';

const kpiData = [
  {
    title: 'Total Predictions Today',
    value: '24,563',
    change: '+12.5%',
    trend: 'up',
    icon: Target,
    data: [30, 40, 35, 50, 49, 60, 70, 91, 125],
    color: '#1d4ed8',
  },
  {
    title: 'Accuracy Last 30 Days',
    value: '94.2%',
    change: '+2.1%',
    trend: 'up',
    icon: TrendingUp,
    data: [85, 87, 88, 90, 89, 91, 93, 92, 94.2],
    color: '#10b981',
  },
  {
    title: 'Models Active',
    value: '12',
    change: '+2',
    trend: 'up',
    icon: Cpu,
    data: [8, 8, 9, 10, 10, 11, 11, 12, 12],
    color: '#f59e0b',
  },
  {
    title: 'Training Jobs Running',
    value: '3',
    change: '-1',
    trend: 'down',
    icon: Activity,
    data: [5, 4, 6, 5, 4, 3, 4, 3, 3],
    color: '#8b5cf6',
  },
];

export function DashboardHome() {
  return (
    <div className="space-y-6">
      {/* KPI Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-6">
        {kpiData.map((kpi, index) => {
          const Icon = kpi.icon;
          return (
            <Card key={index} className="rounded-xl shadow-sm border-gray-200">
              <CardHeader className="flex flex-row items-center justify-between pb-2">
                <CardTitle className="text-sm text-gray-600">{kpi.title}</CardTitle>
                <div className="w-10 h-10 rounded-lg bg-gradient-to-br from-[#1d4ed8]/10 to-[#1e3a8a]/10 flex items-center justify-center">
                  <Icon className="w-5 h-5" style={{ color: kpi.color }} />
                </div>
              </CardHeader>
              <CardContent>
                <div className="space-y-3">
                  <div className="flex items-end justify-between">
                    <div>
                      <div className="text-gray-900">{kpi.value}</div>
                      <div className={`text-xs ${kpi.trend === 'up' ? 'text-green-600' : 'text-red-600'}`}>
                        {kpi.change} vs last week
                      </div>
                    </div>
                    <div className="w-24 h-12">
                      <ResponsiveContainer width="100%" height="100%">
                        <LineChart data={kpi.data.map((value, i) => ({ value, index: i }))}>
                          <Line
                            type="monotone"
                            dataKey="value"
                            stroke={kpi.color}
                            strokeWidth={2}
                            dot={false}
                          />
                        </LineChart>
                      </ResponsiveContainer>
                    </div>
                  </div>
                </div>
              </CardContent>
            </Card>
          );
        })}
      </div>

      {/* Model Performance Overview */}
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
        <Card className="rounded-xl shadow-sm border-gray-200">
          <CardHeader>
            <CardTitle>Recent Model Performance</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              {[
                { name: 'Product Recommendation', accuracy: 96.5, status: 'Excellent' },
                { name: 'Price Prediction', accuracy: 94.2, status: 'Good' },
                { name: 'Demand Forecast', accuracy: 91.8, status: 'Good' },
                { name: 'Customer Segmentation', accuracy: 89.3, status: 'Fair' },
              ].map((model, i) => (
                <div key={i} className="space-y-2">
                  <div className="flex items-center justify-between text-sm">
                    <span className="text-gray-700">{model.name}</span>
                    <span className="text-gray-900">{model.accuracy}%</span>
                  </div>
                  <div className="w-full bg-gray-100 rounded-full h-2">
                    <div
                      className="bg-gradient-to-r from-[#1d4ed8] to-[#1e3a8a] h-2 rounded-full transition-all"
                      style={{ width: `${model.accuracy}%` }}
                    ></div>
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>

        <Card className="rounded-xl shadow-sm border-gray-200">
          <CardHeader>
            <CardTitle>Recent Activity</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              {[
                { action: 'Price prediction model retrained', time: '2 minutes ago', status: 'success' },
                { action: 'New customer segment identified', time: '15 minutes ago', status: 'info' },
                { action: 'Demand forecast pipeline completed', time: '1 hour ago', status: 'success' },
                { action: 'Product recommendation updated', time: '2 hours ago', status: 'info' },
                { action: 'Data warehouse sync completed', time: '3 hours ago', status: 'success' },
              ].map((activity, i) => (
                <div key={i} className="flex items-start gap-3">
                  <div className={`w-2 h-2 rounded-full mt-2 ${
                    activity.status === 'success' ? 'bg-green-500' : 'bg-blue-500'
                  }`}></div>
                  <div className="flex-1">
                    <div className="text-sm text-gray-900">{activity.action}</div>
                    <div className="text-xs text-gray-500">{activity.time}</div>
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Quick Stats */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <Card className="rounded-xl shadow-sm border-gray-200 bg-gradient-to-br from-[#1d4ed8] to-[#1e3a8a] text-white">
          <CardContent className="pt-6">
            <div className="space-y-2">
              <div className="text-sm opacity-90">Total Products Analyzed</div>
              <div className="text-white">1,245,678</div>
              <div className="text-xs opacity-75">Across all platforms</div>
            </div>
          </CardContent>
        </Card>

        <Card className="rounded-xl shadow-sm border-gray-200 bg-gradient-to-br from-[#10b981] to-[#059669] text-white">
          <CardContent className="pt-6">
            <div className="space-y-2">
              <div className="text-sm opacity-90">Customer Segments</div>
              <div className="text-white">47</div>
              <div className="text-xs opacity-75">Active behavioral groups</div>
            </div>
          </CardContent>
        </Card>

        <Card className="rounded-xl shadow-sm border-gray-200 bg-gradient-to-br from-[#f59e0b] to-[#d97706] text-white">
          <CardContent className="pt-6">
            <div className="space-y-2">
              <div className="text-sm opacity-90">Data Pipeline Runs</div>
              <div className="text-white">8,432</div>
              <div className="text-xs opacity-75">This month</div>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
