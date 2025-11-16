import { Card, CardContent, CardHeader, CardTitle } from '../../../components/ui/figma/card';
import { Badge } from '../../../components/ui/figma/badge';
import { Button } from '../../../components/ui/figma/button';
import { Table2, TrendingUp, Database } from 'lucide-react';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '../../../components/ui/figma/table';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';

const facts = [
  {
    name: 'fact_sales',
    records: 15234567,
    columns: 15,
    lastUpdated: '2024-11-16 08:45',
    status: 'Active',
    description: 'Sales transactions with product, customer, and platform details',
    growthRate: 12.5,
  },
  {
    name: 'fact_inventory',
    records: 3456789,
    columns: 10,
    lastUpdated: '2024-11-16 08:30',
    status: 'Active',
    description: 'Product inventory levels across platforms',
    growthRate: 8.3,
  },
  {
    name: 'fact_pricing',
    records: 8923456,
    columns: 8,
    lastUpdated: '2024-11-16 08:40',
    status: 'Active',
    description: 'Historical pricing data for products',
    growthRate: 15.7,
  },
  {
    name: 'fact_customer_activity',
    records: 23456780,
    columns: 12,
    lastUpdated: '2024-11-16 08:35',
    status: 'Active',
    description: 'Customer browsing and interaction events',
    growthRate: 22.1,
  },
];

const recordGrowth = [
  { month: 'Jun', sales: 12500000, inventory: 2800000, pricing: 7200000 },
  { month: 'Jul', sales: 13200000, inventory: 2950000, pricing: 7600000 },
  { month: 'Aug', sales: 13800000, inventory: 3100000, pricing: 8100000 },
  { month: 'Sep', sales: 14200000, inventory: 3250000, pricing: 8400000 },
  { month: 'Oct', sales: 14800000, inventory: 3380000, pricing: 8700000 },
  { month: 'Nov', sales: 15234567, inventory: 3456789, pricing: 8923456 },
];

export function Facts() {
  const totalRecords = facts.reduce((sum, fact) => sum + fact.records, 0);

  return (
    <div className="space-y-6">
      {/* Overview Cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <Card className="rounded-xl shadow-sm border-gray-200">
          <CardContent className="pt-6">
            <div className="flex items-center justify-between">
              <div className="space-y-1">
                <div className="text-sm text-gray-600">Total Fact Tables</div>
                <div className="text-gray-900">{facts.length}</div>
              </div>
              <div className="w-12 h-12 rounded-lg bg-blue-50 flex items-center justify-center">
                <Table2 className="w-6 h-6 text-[#1d4ed8]" />
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="rounded-xl shadow-sm border-gray-200">
          <CardContent className="pt-6">
            <div className="flex items-center justify-between">
              <div className="space-y-1">
                <div className="text-sm text-gray-600">Total Records</div>
                <div className="text-gray-900">{totalRecords.toLocaleString()}</div>
              </div>
              <div className="w-12 h-12 rounded-lg bg-green-50 flex items-center justify-center">
                <Database className="w-6 h-6 text-green-600" />
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="rounded-xl shadow-sm border-gray-200 bg-gradient-to-br from-[#1d4ed8] to-[#1e3a8a] text-white">
          <CardContent className="pt-6">
            <div className="flex items-center justify-between">
              <div className="space-y-1">
                <div className="text-sm opacity-90">Avg Growth Rate</div>
                <div className="text-white">
                  {(facts.reduce((sum, f) => sum + f.growthRate, 0) / facts.length).toFixed(1)}%
                </div>
                <div className="text-xs opacity-75">Monthly average</div>
              </div>
              <div className="w-12 h-12 rounded-lg bg-white/20 flex items-center justify-center">
                <TrendingUp className="w-6 h-6" />
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Growth Chart */}
      <Card className="rounded-xl shadow-sm border-gray-200">
        <CardHeader>
          <CardTitle className="text-gray-900">Record Growth Trend</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="h-80">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={recordGrowth}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis 
                  dataKey="month" 
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
                <Bar dataKey="sales" fill="#1d4ed8" radius={[8, 8, 0, 0]} name="Sales" />
                <Bar dataKey="inventory" fill="#10b981" radius={[8, 8, 0, 0]} name="Inventory" />
                <Bar dataKey="pricing" fill="#f59e0b" radius={[8, 8, 0, 0]} name="Pricing" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </CardContent>
      </Card>

      {/* Fact Tables */}
      <Card className="rounded-xl shadow-sm border-gray-200">
        <CardHeader>
          <CardTitle className="text-gray-900">Fact Tables</CardTitle>
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Fact Table Name</TableHead>
                <TableHead>Description</TableHead>
                <TableHead>Records</TableHead>
                <TableHead>Columns</TableHead>
                <TableHead>Growth Rate</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Last Updated</TableHead>
                <TableHead></TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {facts.map((fact, i) => (
                <TableRow key={i}>
                  <TableCell>
                    <div className="flex items-center gap-2">
                      <Table2 className="w-4 h-4 text-[#1d4ed8]" />
                      <span className="text-gray-900">{fact.name}</span>
                    </div>
                  </TableCell>
                  <TableCell className="max-w-sm">
                    <div className="text-sm text-gray-600">{fact.description}</div>
                  </TableCell>
                  <TableCell>
                    <span className="text-gray-900">{fact.records.toLocaleString()}</span>
                  </TableCell>
                  <TableCell>
                    <Badge variant="outline" className="text-xs">{fact.columns} cols</Badge>
                  </TableCell>
                  <TableCell>
                    <div className="flex items-center gap-2">
                      <TrendingUp className="w-4 h-4 text-green-600" />
                      <span className="text-green-600">+{fact.growthRate}%</span>
                    </div>
                  </TableCell>
                  <TableCell>
                    <Badge className="bg-green-500 text-xs">{fact.status}</Badge>
                  </TableCell>
                  <TableCell className="text-sm text-gray-500">{fact.lastUpdated}</TableCell>
                  <TableCell>
                    <Button size="sm" variant="outline">View Details</Button>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </div>
  );
}
