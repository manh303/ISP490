import { Card, CardContent, CardHeader, CardTitle } from '../../../components/ui/figma/card';
import { Input } from '../../../components/ui/figma/input';
import { Button } from '../../../components/ui/figma/button';
import { Badge } from '../../../components/ui/figma/badge';
import { Search, Calendar, TrendingUp, BarChart3 } from 'lucide-react';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '../../../components/ui/figma/select';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '../../../components/ui/figma/table';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';

const forecastData = [
  { date: '2024-11-01', actual: 1250, forecast: null, confidence: null },
  { date: '2024-11-02', actual: 1320, forecast: null, confidence: null },
  { date: '2024-11-03', actual: 1180, forecast: null, confidence: null },
  { date: '2024-11-04', actual: 1450, forecast: null, confidence: null },
  { date: '2024-11-05', actual: 1520, forecast: null, confidence: null },
  { date: '2024-11-06', actual: null, forecast: 1580, confidence: 0.89 },
  { date: '2024-11-07', actual: null, forecast: 1650, confidence: 0.87 },
  { date: '2024-11-08', actual: null, forecast: 1720, confidence: 0.85 },
  { date: '2024-11-09', actual: null, forecast: 1890, confidence: 0.83 },
  { date: '2024-11-10', actual: null, forecast: 2150, confidence: 0.81 },
  { date: '2024-11-11', actual: null, forecast: 1980, confidence: 0.79 },
  { date: '2024-11-12', actual: null, forecast: 1840, confidence: 0.77 },
];

const tableData = [
  { date: '2024-11-12', demand: 1840, confidence: 77, model: 'v3.1.2', createdAt: '2024-11-05 09:15' },
  { date: '2024-11-11', demand: 1980, confidence: 79, model: 'v3.1.2', createdAt: '2024-11-05 09:15' },
  { date: '2024-11-10', demand: 2150, confidence: 81, model: 'v3.1.2', createdAt: '2024-11-05 09:15' },
  { date: '2024-11-09', demand: 1890, confidence: 83, model: 'v3.1.2', createdAt: '2024-11-05 09:15' },
  { date: '2024-11-08', demand: 1720, confidence: 85, model: 'v3.1.2', createdAt: '2024-11-05 09:15' },
  { date: '2024-11-07', demand: 1650, confidence: 87, model: 'v3.1.2', createdAt: '2024-11-05 09:15' },
  { date: '2024-11-06', demand: 1580, confidence: 89, model: 'v3.1.2', createdAt: '2024-11-05 09:15' },
];

export function DemandForecast() {
  const totalForecast7Days = 12810;
  const totalForecast30Days = 45620;
  const peakDemandDate = '2024-11-10';
  const peakDemandValue = 2150;

  return (
    <div className="space-y-6">
      {/* Filter Bar */}
      <Card className="rounded-xl shadow-sm border-gray-200 bg-[#f8fafc]">
        <CardContent className="pt-6">
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            <div className="relative">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
              <Input
                type="text"
                placeholder="Search product or category..."
                className="pl-10 bg-white"
                defaultValue="Wireless Bluetooth Headphones"
              />
            </div>
            <Select defaultValue="7">
              <SelectTrigger className="bg-white">
                <SelectValue placeholder="Forecast Horizon" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="7">7 Days</SelectItem>
                <SelectItem value="14">14 Days</SelectItem>
                <SelectItem value="30">30 Days</SelectItem>
              </SelectContent>
            </Select>
            <Input type="date" className="bg-white" defaultValue="2024-11-01" />
            <Button className="bg-[#1d4ed8] hover:bg-[#1e3a8a]">
              Apply Filters
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* KPIs */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <Card className="rounded-xl shadow-sm border-gray-200">
          <CardContent className="pt-6">
            <div className="flex items-start justify-between">
              <div className="space-y-2">
                <div className="text-sm text-gray-600">Total Forecast (7 Days)</div>
                <div className="text-gray-900">{totalForecast7Days.toLocaleString()}</div>
                <div className="text-sm text-gray-500">Units</div>
              </div>
              <div className="w-12 h-12 rounded-lg bg-blue-50 flex items-center justify-center">
                <TrendingUp className="w-6 h-6 text-[#1d4ed8]" />
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="rounded-xl shadow-sm border-gray-200">
          <CardContent className="pt-6">
            <div className="flex items-start justify-between">
              <div className="space-y-2">
                <div className="text-sm text-gray-600">Total Forecast (30 Days)</div>
                <div className="text-gray-900">{totalForecast30Days.toLocaleString()}</div>
                <div className="text-sm text-gray-500">Units</div>
              </div>
              <div className="w-12 h-12 rounded-lg bg-green-50 flex items-center justify-center">
                <BarChart3 className="w-6 h-6 text-green-600" />
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="rounded-xl shadow-sm border-gray-200 bg-gradient-to-br from-[#1d4ed8] to-[#1e3a8a] text-white">
          <CardContent className="pt-6">
            <div className="flex items-start justify-between">
              <div className="space-y-2">
                <div className="text-sm opacity-90">Peak Demand Date</div>
                <div className="text-white">{peakDemandDate}</div>
                <div className="text-xs opacity-75">{peakDemandValue.toLocaleString()} units expected</div>
              </div>
              <div className="w-12 h-12 rounded-lg bg-white/20 flex items-center justify-center">
                <Calendar className="w-6 h-6" />
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Main Forecast Chart */}
      <Card className="rounded-xl shadow-sm border-gray-200">
        <CardHeader>
          <CardTitle className="text-gray-900">Demand Forecast Chart</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="h-80">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={forecastData}>
                <defs>
                  <linearGradient id="actualGradient" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#6b7280" stopOpacity={0.3}/>
                    <stop offset="95%" stopColor="#6b7280" stopOpacity={0}/>
                  </linearGradient>
                  <linearGradient id="forecastGradient" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#1d4ed8" stopOpacity={0.3}/>
                    <stop offset="95%" stopColor="#1d4ed8" stopOpacity={0}/>
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis 
                  dataKey="date" 
                  stroke="#6b7280"
                  tick={{ fill: '#6b7280', fontSize: 12 }}
                />
                <YAxis 
                  stroke="#6b7280"
                  tick={{ fill: '#6b7280', fontSize: 12 }}
                />
                <Tooltip 
                  contentStyle={{ 
                    backgroundColor: 'white', 
                    border: '1px solid #e5e7eb',
                    borderRadius: '8px',
                    boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)'
                  }}
                />
                <Legend />
                <Area
                  type="monotone"
                  dataKey="actual"
                  stroke="#6b7280"
                  strokeWidth={2}
                  fill="url(#actualGradient)"
                  name="Actual Demand"
                />
                <Area
                  type="monotone"
                  dataKey="forecast"
                  stroke="#1d4ed8"
                  strokeWidth={2}
                  fill="url(#forecastGradient)"
                  name="Forecasted Demand"
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>
          <div className="mt-4 p-4 bg-blue-50 rounded-lg border border-blue-200">
            <div className="flex items-start gap-3">
              <TrendingUp className="w-5 h-5 text-[#1d4ed8] mt-0.5" />
              <div>
                <div className="text-sm text-gray-900">Peak demand expected on {peakDemandDate}</div>
                <div className="text-xs text-gray-600 mt-1">
                  Consider increasing inventory by 40% to meet the forecasted demand of {peakDemandValue.toLocaleString()} units.
                </div>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Details Table */}
      <Card className="rounded-xl shadow-sm border-gray-200">
        <CardHeader>
          <CardTitle className="text-gray-900">Forecast Details</CardTitle>
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Forecast Date</TableHead>
                <TableHead>Predicted Demand</TableHead>
                <TableHead>Confidence Level</TableHead>
                <TableHead>Model Version</TableHead>
                <TableHead>Created At</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {tableData.map((row, i) => (
                <TableRow key={i}>
                  <TableCell>
                    <div className="flex items-center gap-2">
                      {row.date}
                      {row.date === peakDemandDate && (
                        <Badge className="bg-orange-500 text-xs">Peak</Badge>
                      )}
                    </div>
                  </TableCell>
                  <TableCell>
                    <span className="text-gray-900">{row.demand.toLocaleString()} units</span>
                  </TableCell>
                  <TableCell>
                    <div className="flex items-center gap-3">
                      <div className="flex-1 max-w-[100px]">
                        <div className="w-full bg-gray-100 rounded-full h-2">
                          <div
                            className="bg-gradient-to-r from-[#1d4ed8] to-[#1e3a8a] h-2 rounded-full"
                            style={{ width: `${row.confidence}%` }}
                          ></div>
                        </div>
                      </div>
                      <span className="text-sm text-gray-600">{row.confidence}%</span>
                    </div>
                  </TableCell>
                  <TableCell>
                    <Badge variant="outline" className="text-xs">{row.model}</Badge>
                  </TableCell>
                  <TableCell className="text-sm text-gray-500">{row.createdAt}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  );
}