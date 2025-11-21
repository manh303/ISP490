import { Card, CardContent, CardHeader, CardTitle } from '../../../components/ui/figma/card';
import { Input } from '../../../components/ui/figma/input';
import { Button } from '../../../components/ui/figma/button';
import { Badge } from '../../../components/ui/figma/badge';
import { Search, TrendingDown, TrendingUp, AlertCircle } from 'lucide-react';
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
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, Area, AreaChart, ResponsiveContainer } from 'recharts';

const predictionData = [
  { date: '2024-11-01', predicted: 89.99, actual: 91.50, ciLower: 85.00, ciUpper: 95.00 },
  { date: '2024-11-02', predicted: 88.50, actual: 89.00, ciLower: 84.00, ciUpper: 93.00 },
  { date: '2024-11-03', predicted: 87.20, actual: 88.20, ciLower: 83.00, ciUpper: 92.00 },
  { date: '2024-11-04', predicted: 86.80, actual: 87.50, ciLower: 82.50, ciUpper: 91.50 },
  { date: '2024-11-05', predicted: 85.90, actual: 86.80, ciLower: 81.50, ciUpper: 90.50 },
  { date: '2024-11-06', predicted: 84.50, actual: null, ciLower: 80.00, ciUpper: 89.00 },
  { date: '2024-11-07', predicted: 83.20, actual: null, ciLower: 78.50, ciUpper: 87.50 },
  { date: '2024-11-08', predicted: 82.90, actual: null, ciLower: 78.00, ciUpper: 87.00 },
  { date: '2024-11-09', predicted: 82.50, actual: null, ciLower: 77.50, ciUpper: 86.50 },
  { date: '2024-11-10', predicted: 81.99, actual: null, ciLower: 77.00, ciUpper: 86.00 },
];

const tableData = [
  { date: '2024-11-10', predicted: 81.99, ciLower: 77.00, ciUpper: 86.00, model: 'v2.3.1', platform: 'Shopee', createdAt: '2024-11-09 14:32' },
  { date: '2024-11-09', predicted: 82.50, ciLower: 77.50, ciUpper: 86.50, model: 'v2.3.1', platform: 'Shopee', createdAt: '2024-11-08 14:32' },
  { date: '2024-11-08', predicted: 82.90, ciLower: 78.00, ciUpper: 87.00, model: 'v2.3.1', platform: 'Shopee', createdAt: '2024-11-07 14:32' },
  { date: '2024-11-07', predicted: 83.20, ciLower: 78.50, ciUpper: 87.50, model: 'v2.3.1', platform: 'Shopee', createdAt: '2024-11-06 14:32' },
  { date: '2024-11-06', predicted: 84.50, ciLower: 80.00, ciUpper: 89.00, model: 'v2.3.0', platform: 'Shopee', createdAt: '2024-11-05 14:32' },
];

export function PricePredictions() {
  const currentPrice = 89.99;
  const predictedPrice = 81.99;
  const priceDiff = ((predictedPrice - currentPrice) / currentPrice * 100).toFixed(1);
  const suggestedReduction = (currentPrice - predictedPrice).toFixed(2);

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
                placeholder="Search product..."
                className="pl-10 bg-white"
                defaultValue="Wireless Bluetooth Headphones"
              />
            </div>
            <Select defaultValue="shopee">
              <SelectTrigger className="bg-white">
                <SelectValue placeholder="Platform" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="shopee">Shopee</SelectItem>
                <SelectItem value="lazada">Lazada</SelectItem>
                <SelectItem value="tiki">Tiki</SelectItem>
              </SelectContent>
            </Select>
            <Input type="date" className="bg-white" defaultValue="2024-11-01" />
            <Button className="bg-[#1d4ed8] hover:bg-[#1e3a8a]">
              Apply Filters
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Top KPIs */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <Card className="rounded-xl shadow-sm border-gray-200">
          <CardContent className="pt-6">
            <div className="flex items-start justify-between">
              <div className="space-y-2">
                <div className="text-sm text-gray-600">Price Difference</div>
                <div className="text-gray-900">{priceDiff}%</div>
                <div className="flex items-center gap-2 text-sm text-green-600">
                  <TrendingDown className="w-4 h-4" />
                  Decreasing trend
                </div>
              </div>
              <div className="w-12 h-12 rounded-lg bg-green-50 flex items-center justify-center">
                <TrendingDown className="w-6 h-6 text-green-600" />
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="rounded-xl shadow-sm border-gray-200">
          <CardContent className="pt-6">
            <div className="flex items-start justify-between">
              <div className="space-y-2">
                <div className="text-sm text-gray-600">Predicted Price (7 days)</div>
                <div className="text-gray-900">${predictedPrice}</div>
                <div className="text-sm text-gray-500">Current: ${currentPrice}</div>
              </div>
              <div className="w-12 h-12 rounded-lg bg-blue-50 flex items-center justify-center">
                <TrendingUp className="w-6 h-6 text-[#1d4ed8]" />
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="rounded-xl shadow-sm border-gray-200 bg-gradient-to-br from-[#1d4ed8] to-[#1e3a8a] text-white">
          <CardContent className="pt-6">
            <div className="flex items-start justify-between">
              <div className="space-y-2">
                <div className="text-sm opacity-90">Suggested Action</div>
                <div className="text-white">Reduce by ${suggestedReduction}</div>
                <div className="text-xs opacity-75">To match market trend</div>
              </div>
              <div className="w-12 h-12 rounded-lg bg-white/20 flex items-center justify-center">
                <AlertCircle className="w-6 h-6" />
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Main Prediction Chart */}
      <Card className="rounded-xl shadow-sm border-gray-200">
        <CardHeader>
          <CardTitle className="text-gray-900">Price Prediction Chart</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="h-80">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={predictionData}>
                <defs>
                  <linearGradient id="confidenceBand" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#1d4ed8" stopOpacity={0.2}/>
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
                  domain={[75, 100]}
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
                  dataKey="ciUpper"
                  stroke="none"
                  fill="url(#confidenceBand)"
                  fillOpacity={1}
                  name="Confidence Band"
                />
                <Area
                  type="monotone"
                  dataKey="ciLower"
                  stroke="none"
                  fill="white"
                  fillOpacity={1}
                />
                <Line
                  type="monotone"
                  dataKey="predicted"
                  stroke="#1d4ed8"
                  strokeWidth={2}
                  dot={{ fill: '#1d4ed8', r: 4 }}
                  name="Predicted Price"
                />
                <Line
                  type="monotone"
                  dataKey="actual"
                  stroke="#6b7280"
                  strokeWidth={2}
                  dot={{ fill: '#6b7280', r: 4 }}
                  strokeDasharray="5 5"
                  name="Actual Price"
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </CardContent>
      </Card>

      {/* Details Table */}
      <Card className="rounded-xl shadow-sm border-gray-200">
        <CardHeader>
          <CardTitle className="text-gray-900">Prediction Details</CardTitle>
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Date</TableHead>
                <TableHead>Predicted Price</TableHead>
                <TableHead>CI Lower</TableHead>
                <TableHead>CI Upper</TableHead>
                <TableHead>Model Version</TableHead>
                <TableHead>Platform</TableHead>
                <TableHead>Created At</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {tableData.map((row, i) => (
                <TableRow key={i}>
                  <TableCell>{row.date}</TableCell>
                  <TableCell>
                    <span className="text-gray-900">${row.predicted}</span>
                  </TableCell>
                  <TableCell className="text-gray-600">${row.ciLower.toFixed(2)}</TableCell>
                  <TableCell className="text-gray-600">${row.ciUpper.toFixed(2)}</TableCell>
                  <TableCell>
                    <Badge variant="outline" className="text-xs">{row.model}</Badge>
                  </TableCell>
                  <TableCell>
                    <Badge className="bg-[#1d4ed8] text-xs">{row.platform}</Badge>
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
