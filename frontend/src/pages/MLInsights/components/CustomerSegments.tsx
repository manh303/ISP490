import { useState } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../../../components/ui/figma/card';
import { Badge } from '../../../components/ui/figma/badge';
import { Button } from '../../../components/ui/figma/button';
import { Users, ShoppingCart, DollarSign, Calendar } from 'lucide-react';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '../../../components/ui/figma/table';
import { Sheet, SheetContent, SheetHeader, SheetTitle } from '../../../components/ui/figma/sheet';
import { PieChart, Pie, Cell, ResponsiveContainer, Legend, Tooltip, BarChart, Bar, XAxis, YAxis, CartesianGrid } from 'recharts';

const segments = [
  {
    id: 'SEG-001',
    name: 'High-Value Frequent Buyers',
    description: 'Customers who purchase frequently with high average order value',
    avgPurchaseValue: 245.50,
    purchaseFrequency: 8.3,
    customerCount: 12450,
    createdAt: '2024-10-15',
  },
  {
    id: 'SEG-002',
    name: 'Occasional Big Spenders',
    description: 'Infrequent buyers who make large purchases when they do buy',
    avgPurchaseValue: 389.99,
    purchaseFrequency: 2.1,
    customerCount: 8920,
    createdAt: '2024-10-12',
  },
  {
    id: 'SEG-003',
    name: 'Budget-Conscious Regulars',
    description: 'Regular customers who typically purchase lower-priced items',
    avgPurchaseValue: 45.20,
    purchaseFrequency: 12.5,
    customerCount: 24560,
    createdAt: '2024-10-08',
  },
  {
    id: 'SEG-004',
    name: 'New Explorers',
    description: 'Recently acquired customers still exploring the platform',
    avgPurchaseValue: 78.90,
    purchaseFrequency: 1.8,
    customerCount: 15780,
    createdAt: '2024-11-01',
  },
  {
    id: 'SEG-005',
    name: 'At-Risk Churners',
    description: 'Previously active customers showing declining engagement',
    avgPurchaseValue: 156.30,
    purchaseFrequency: 0.5,
    customerCount: 6340,
    createdAt: '2024-09-22',
  },
];

const platformDistribution = [
  { name: 'Shopee', value: 42, color: '#1d4ed8' },
  { name: 'Lazada', value: 28, color: '#1e3a8a' },
  { name: 'Tiki', value: 18, color: '#3b82f6' },
  { name: 'Others', value: 12, color: '#93c5fd' },
];

const topCategories = [
  { category: 'Electronics', value: 3250 },
  { category: 'Fashion', value: 2890 },
  { category: 'Home & Living', value: 2340 },
  { category: 'Beauty', value: 1980 },
  { category: 'Sports', value: 1560 },
];

const sampleCustomers = [
  { name: 'John Anderson', spend: '$2,450', frequency: 12, lastOrder: '2024-11-14' },
  { name: 'Sarah Chen', spend: '$1,980', frequency: 9, lastOrder: '2024-11-13' },
  { name: 'Michael Brown', spend: '$3,120', frequency: 15, lastOrder: '2024-11-15' },
  { name: 'Emma Wilson', spend: '$1,750', frequency: 8, lastOrder: '2024-11-12' },
  { name: 'David Lee', spend: '$2,890', frequency: 11, lastOrder: '2024-11-14' },
];

export function CustomerSegments() {
  const [selectedSegment, setSelectedSegment] = useState<any>(null);

  return (
    <div className="space-y-6">
      {/* Overview Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
        <Card className="rounded-xl shadow-sm border-gray-200">
          <CardContent className="pt-6">
            <div className="flex items-center justify-between">
              <div className="space-y-1">
                <div className="text-sm text-gray-600">Total Segments</div>
                <div className="text-gray-900">{segments.length}</div>
              </div>
              <div className="w-12 h-12 rounded-lg bg-blue-50 flex items-center justify-center">
                <Users className="w-6 h-6 text-[#1d4ed8]" />
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="rounded-xl shadow-sm border-gray-200">
          <CardContent className="pt-6">
            <div className="flex items-center justify-between">
              <div className="space-y-1">
                <div className="text-sm text-gray-600">Total Customers</div>
                <div className="text-gray-900">
                  {segments.reduce((sum, seg) => sum + seg.customerCount, 0).toLocaleString()}
                </div>
              </div>
              <div className="w-12 h-12 rounded-lg bg-green-50 flex items-center justify-center">
                <ShoppingCart className="w-6 h-6 text-green-600" />
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="rounded-xl shadow-sm border-gray-200">
          <CardContent className="pt-6">
            <div className="flex items-center justify-between">
              <div className="space-y-1">
                <div className="text-sm text-gray-600">Avg Purchase Value</div>
                <div className="text-gray-900">
                  ${(segments.reduce((sum, seg) => sum + seg.avgPurchaseValue, 0) / segments.length).toFixed(2)}
                </div>
              </div>
              <div className="w-12 h-12 rounded-lg bg-orange-50 flex items-center justify-center">
                <DollarSign className="w-6 h-6 text-orange-600" />
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="rounded-xl shadow-sm border-gray-200">
          <CardContent className="pt-6">
            <div className="flex items-center justify-between">
              <div className="space-y-1">
                <div className="text-sm text-gray-600">Latest Segment</div>
                <div className="text-gray-900">Nov 1, 2024</div>
              </div>
              <div className="w-12 h-12 rounded-lg bg-purple-50 flex items-center justify-center">
                <Calendar className="w-6 h-6 text-purple-600" />
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Segments Table */}
      <Card className="rounded-xl shadow-sm border-gray-200">
        <CardHeader>
          <CardTitle className="text-gray-900">Customer Segments</CardTitle>
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Segment Name</TableHead>
                <TableHead>Description</TableHead>
                <TableHead>Avg Purchase Value</TableHead>
                <TableHead>Purchase Frequency</TableHead>
                <TableHead>Customer Count</TableHead>
                <TableHead>Created At</TableHead>
                <TableHead></TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {segments.map((segment) => (
                <TableRow key={segment.id} className="cursor-pointer hover:bg-gray-50">
                  <TableCell>
                    <div className="text-gray-900">{segment.name}</div>
                  </TableCell>
                  <TableCell className="max-w-xs">
                    <div className="text-sm text-gray-600 truncate">{segment.description}</div>
                  </TableCell>
                  <TableCell>
                    <span className="text-gray-900">${segment.avgPurchaseValue.toFixed(2)}</span>
                  </TableCell>
                  <TableCell>
                    <Badge variant="outline" className="text-xs">{segment.purchaseFrequency}/month</Badge>
                  </TableCell>
                  <TableCell>
                    <span className="text-gray-900">{segment.customerCount.toLocaleString()}</span>
                  </TableCell>
                  <TableCell className="text-sm text-gray-500">{segment.createdAt}</TableCell>
                  <TableCell>
                    <Button 
                      size="sm" 
                      variant="outline"
                      onClick={() => setSelectedSegment(segment)}
                    >
                      View Details
                    </Button>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      {/* Segment Detail Sheet */}
      <Sheet open={!!selectedSegment} onOpenChange={() => setSelectedSegment(null)}>
        <SheetContent className="w-full sm:max-w-2xl overflow-y-auto">
          {selectedSegment && (
            <>
              <SheetHeader>
                <SheetTitle>{selectedSegment.name}</SheetTitle>
              </SheetHeader>
              <div className="mt-6 space-y-6">
                {/* Description */}
                <div>
                  <div className="text-sm text-gray-600 mb-2">Description</div>
                  <div className="text-gray-900">{selectedSegment.description}</div>
                </div>

                {/* Metrics Grid */}
                <div className="grid grid-cols-2 gap-4">
                  <Card className="rounded-lg">
                    <CardContent className="pt-4">
                      <div className="space-y-1">
                        <div className="text-sm text-gray-600">Avg Purchase Value</div>
                        <div className="text-gray-900">${selectedSegment.avgPurchaseValue.toFixed(2)}</div>
                      </div>
                    </CardContent>
                  </Card>
                  <Card className="rounded-lg">
                    <CardContent className="pt-4">
                      <div className="space-y-1">
                        <div className="text-sm text-gray-600">Purchase Frequency</div>
                        <div className="text-gray-900">{selectedSegment.purchaseFrequency}/month</div>
                      </div>
                    </CardContent>
                  </Card>
                  <Card className="rounded-lg">
                    <CardContent className="pt-4">
                      <div className="space-y-1">
                        <div className="text-sm text-gray-600">Total Customers</div>
                        <div className="text-gray-900">{selectedSegment.customerCount.toLocaleString()}</div>
                      </div>
                    </CardContent>
                  </Card>
                  <Card className="rounded-lg">
                    <CardContent className="pt-4">
                      <div className="space-y-1">
                        <div className="text-sm text-gray-600">Created Date</div>
                        <div className="text-gray-900">{selectedSegment.createdAt}</div>
                      </div>
                    </CardContent>
                  </Card>
                </div>

                {/* Platform Distribution */}
                <Card className="rounded-lg">
                  <CardHeader>
                    <CardTitle className="text-base">Platform Distribution</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="h-64">
                      <ResponsiveContainer width="100%" height="100%">
                        <PieChart>
                          <Pie
                            data={platformDistribution}
                            cx="50%"
                            cy="50%"
                            labelLine={false}
                            label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}
                            outerRadius={80}
                            fill="#8884d8"
                            dataKey="value"
                          >
                            {platformDistribution.map((entry, index) => (
                              <Cell key={`cell-${index}`} fill={entry.color} />
                            ))}
                          </Pie>
                          <Tooltip />
                        </PieChart>
                      </ResponsiveContainer>
                    </div>
                  </CardContent>
                </Card>

                {/* Top Categories */}
                <Card className="rounded-lg">
                  <CardHeader>
                    <CardTitle className="text-base">Top Categories Purchased</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="h-64">
                      <ResponsiveContainer width="100%" height="100%">
                        <BarChart data={topCategories}>
                          <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                          <XAxis 
                            dataKey="category" 
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
                            }}
                          />
                          <Bar dataKey="value" fill="#1d4ed8" radius={[8, 8, 0, 0]} />
                        </BarChart>
                      </ResponsiveContainer>
                    </div>
                  </CardContent>
                </Card>

                {/* Sample Customers */}
                <Card className="rounded-lg">
                  <CardHeader>
                    <CardTitle className="text-base">Sample Customers</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead>Name</TableHead>
                          <TableHead>Total Spend</TableHead>
                          <TableHead>Frequency</TableHead>
                          <TableHead>Last Order</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {sampleCustomers.map((customer, i) => (
                          <TableRow key={i}>
                            <TableCell className="text-gray-900">{customer.name}</TableCell>
                            <TableCell className="text-gray-900">{customer.spend}</TableCell>
                            <TableCell>
                              <Badge variant="outline" className="text-xs">{customer.frequency} orders</Badge>
                            </TableCell>
                            <TableCell className="text-sm text-gray-500">{customer.lastOrder}</TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </CardContent>
                </Card>
              </div>
            </>
          )}
        </SheetContent>
      </Sheet>
    </div>
  );
}
