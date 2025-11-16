import { useState } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '../../../components/ui/figma/card';
import { Input } from '../../../components/ui/figma/input';
import { Button } from '../../../components/ui/figma/button';
import { Badge } from '../../../components/ui/figma/badge';
import { Search, ExternalLink, Star } from 'lucide-react';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '../../../components/ui/figma/select';
import { Sheet, SheetContent, SheetHeader, SheetTitle } from '../../../components/ui/figma/sheet';

const originalProduct = {
  id: 'PROD-001',
  name: 'Wireless Bluetooth Headphones - Premium Sound Quality',
  image: 'https://images.unsplash.com/photo-1505740420928-5e560c06d30e?w=400&h=400&fit=crop',
  price: 89.99,
  rating: 4.5,
  brand: 'AudioTech',
  category: 'Electronics > Audio',
  platform: 'Shopee',
};

const recommendations = [
  {
    id: 'REC-001',
    name: 'Premium Over-Ear Headphones with Active Noise Cancellation',
    image: 'https://images.unsplash.com/photo-1546435770-a3e426bf472b?w=400&h=400&fit=crop',
    similarityScore: 94.5,
    type: 'Hybrid',
    price: 129.99,
    rating: 4.7,
    buyCount: 1243,
    platform: 'Shopee',
  },
  {
    id: 'REC-002',
    name: 'Sport Wireless Earbuds - Water Resistant',
    image: 'https://images.unsplash.com/photo-1590658268037-6bf12165a8df?w=400&h=400&fit=crop',
    similarityScore: 89.2,
    type: 'Content-Based',
    price: 59.99,
    rating: 4.3,
    buyCount: 2156,
    platform: 'Lazada',
  },
  {
    id: 'REC-003',
    name: 'Studio Quality Monitor Headphones',
    image: 'https://images.unsplash.com/photo-1484704849700-f032a568e944?w=400&h=400&fit=crop',
    similarityScore: 87.8,
    type: 'Collaborative',
    price: 149.99,
    rating: 4.8,
    buyCount: 892,
    platform: 'Shopee',
  },
  {
    id: 'REC-004',
    name: 'Compact Foldable Travel Headphones',
    image: 'https://images.unsplash.com/photo-1487215078519-e21cc028cb29?w=400&h=400&fit=crop',
    similarityScore: 85.1,
    type: 'Hybrid',
    price: 69.99,
    rating: 4.4,
    buyCount: 1567,
    platform: 'Tiki',
  },
  {
    id: 'REC-005',
    name: 'Gaming Headset with RGB Lighting',
    image: 'https://images.unsplash.com/photo-1599669454699-248893623440?w=400&h=400&fit=crop',
    similarityScore: 82.3,
    type: 'Content-Based',
    price: 79.99,
    rating: 4.2,
    buyCount: 3421,
    platform: 'Shopee',
  },
];

export function ProductRecommendations() {
  const [selectedProduct, setSelectedProduct] = useState<any>(null);

  return (
    <div className="space-y-6">
      {/* Filter Bar */}
      <Card className="rounded-xl shadow-sm border-gray-200 bg-[#f8fafc]">
        <CardContent className="pt-6">
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-4">
            <div className="lg:col-span-2 relative">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
              <Input
                type="text"
                placeholder="Search product..."
                className="pl-10 bg-white"
                defaultValue="Wireless Bluetooth Headphones"
              />
            </div>
            <Select defaultValue="hybrid">
              <SelectTrigger className="bg-white">
                <SelectValue placeholder="Recommendation Type" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="collaborative">Collaborative Filtering</SelectItem>
                <SelectItem value="content">Content-Based</SelectItem>
                <SelectItem value="hybrid">Hybrid</SelectItem>
              </SelectContent>
            </Select>
            <Select defaultValue="all">
              <SelectTrigger className="bg-white">
                <SelectValue placeholder="Platform" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Platforms</SelectItem>
                <SelectItem value="shopee">Shopee</SelectItem>
                <SelectItem value="lazada">Lazada</SelectItem>
                <SelectItem value="tiki">Tiki</SelectItem>
              </SelectContent>
            </Select>
            <Button className="bg-[#1d4ed8] hover:bg-[#1e3a8a]">
              Apply Filters
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Main Content Grid */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Left: Original Product Info */}
        <Card className="rounded-xl shadow-sm border-gray-200">
          <CardHeader>
            <CardTitle className="text-gray-900">Original Product</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <img
              src={originalProduct.image}
              alt={originalProduct.name}
              className="w-full h-48 object-cover rounded-lg"
            />
            <div className="space-y-3">
              <h3 className="text-gray-900">{originalProduct.name}</h3>
              <div className="flex items-center gap-2">
                <div className="flex items-center gap-1">
                  <Star className="w-4 h-4 fill-yellow-400 text-yellow-400" />
                  <span className="text-sm text-gray-600">{originalProduct.rating}</span>
                </div>
                <span className="text-gray-400">•</span>
                <Badge variant="outline" className="text-xs">{originalProduct.platform}</Badge>
              </div>
              <div className="text-gray-900">${originalProduct.price}</div>
              <div className="space-y-1 text-sm text-gray-600">
                <div>Brand: {originalProduct.brand}</div>
                <div>Category: {originalProduct.category}</div>
              </div>
              <Button variant="outline" className="w-full">
                <ExternalLink className="w-4 h-4 mr-2" />
                View on Platform
              </Button>
            </div>
          </CardContent>
        </Card>

        {/* Right: Recommendations List */}
        <div className="lg:col-span-2 space-y-4">
          <Card className="rounded-xl shadow-sm border-gray-200">
            <CardHeader>
              <CardTitle className="text-gray-900">Recommended Products</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                {recommendations.map((rec) => (
                  <div
                    key={rec.id}
                    className="flex gap-4 p-4 rounded-lg border border-gray-200 hover:border-[#1d4ed8] hover:shadow-md transition-all cursor-pointer"
                    onClick={() => setSelectedProduct(rec)}
                  >
                    <img
                      src={rec.image}
                      alt={rec.name}
                      className="w-24 h-24 object-cover rounded-lg"
                    />
                    <div className="flex-1 space-y-2">
                      <div className="flex items-start justify-between">
                        <h4 className="text-gray-900 flex-1">{rec.name}</h4>
                        <Badge className="bg-[#1d4ed8]">{rec.similarityScore}% Match</Badge>
                      </div>
                      <div className="flex items-center gap-3 text-sm">
                        <Badge variant="outline" className="text-xs">{rec.type}</Badge>
                        <span className="text-gray-400">•</span>
                        <Badge variant="outline" className="text-xs">{rec.platform}</Badge>
                      </div>
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-4">
                          <div className="text-gray-900">${rec.price}</div>
                          <div className="flex items-center gap-1 text-sm text-gray-600">
                            <Star className="w-4 h-4 fill-yellow-400 text-yellow-400" />
                            {rec.rating}
                          </div>
                          <div className="text-sm text-gray-500">{rec.buyCount.toLocaleString()} sold</div>
                        </div>
                        <Button size="sm" variant="outline">
                          View Details
                        </Button>
                      </div>
                      {/* Similarity Progress Bar */}
                      <div className="w-full bg-gray-100 rounded-full h-1.5">
                        <div
                          className="bg-gradient-to-r from-[#1d4ed8] to-[#1e3a8a] h-1.5 rounded-full transition-all"
                          style={{ width: `${rec.similarityScore}%` }}
                        ></div>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        </div>
      </div>

      {/* Detail Sheet */}
      <Sheet open={!!selectedProduct} onOpenChange={() => setSelectedProduct(null)}>
        <SheetContent className="w-full sm:max-w-lg overflow-y-auto">
          {selectedProduct && (
            <>
              <SheetHeader>
                <SheetTitle>Product Details</SheetTitle>
              </SheetHeader>
              <div className="mt-6 space-y-6">
                <img
                  src={selectedProduct.image}
                  alt={selectedProduct.name}
                  className="w-full h-64 object-cover rounded-lg"
                />
                <div className="space-y-4">
                  <h3 className="text-gray-900">{selectedProduct.name}</h3>
                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <div className="text-sm text-gray-500">Price</div>
                      <div className="text-gray-900">${selectedProduct.price}</div>
                    </div>
                    <div>
                      <div className="text-sm text-gray-500">Rating</div>
                      <div className="flex items-center gap-1">
                        <Star className="w-4 h-4 fill-yellow-400 text-yellow-400" />
                        <span className="text-gray-900">{selectedProduct.rating}</span>
                      </div>
                    </div>
                    <div>
                      <div className="text-sm text-gray-500">Similarity Score</div>
                      <div className="text-gray-900">{selectedProduct.similarityScore}%</div>
                    </div>
                    <div>
                      <div className="text-sm text-gray-500">Type</div>
                      <div className="text-gray-900">{selectedProduct.type}</div>
                    </div>
                  </div>
                  <div>
                    <div className="text-sm text-gray-500 mb-2">Recommendation Strength</div>
                    <div className="w-full bg-gray-100 rounded-full h-3">
                      <div
                        className="bg-gradient-to-r from-[#1d4ed8] to-[#1e3a8a] h-3 rounded-full"
                        style={{ width: `${selectedProduct.similarityScore}%` }}
                      ></div>
                    </div>
                  </div>
                  <Button className="w-full bg-[#1d4ed8] hover:bg-[#1e3a8a]">
                    <ExternalLink className="w-4 h-4 mr-2" />
                    View on {selectedProduct.platform}
                  </Button>
                </div>
              </div>
            </>
          )}
        </SheetContent>
      </Sheet>
    </div>
  );
}
