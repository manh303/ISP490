import React from 'react';
import { Button } from '../../../ui/figma/button';
import { Input } from '../../../ui/figma/input';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../../../ui/figma/select';
import { Card, CardContent, CardHeader, CardTitle } from '../../../ui/figma/card';

interface Platform {
  platform_code: string;
  platform_name: string;
}

interface Category {
  category_key: string;
  category_name: string;
}

interface AdminDashboardFiltersProps {
  fromDate: string;
  toDate: string;
  selectedPlatform: string;
  selectedCategory: string;
  platforms: Platform[];
  categories: Category[];
  onFromDateChange: (value: string) => void;
  onToDateChange: (value: string) => void;
  onPlatformChange: (value: string) => void;
  onCategoryChange: (value: string) => void;
  onApplyFilters: () => void;
}

export default function AdminDashboardFilters({
  fromDate,
  toDate,
  selectedPlatform,
  selectedCategory,
  platforms,
  categories,
  onFromDateChange,
  onToDateChange,
  onPlatformChange,
  onCategoryChange,
  onApplyFilters,
}: AdminDashboardFiltersProps) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Bộ lọc</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="flex flex-wrap gap-4 items-end">
          <div>
            <label className="block text-sm font-medium mb-1">Từ ngày</label>
            <Input
              type="date"
              value={fromDate}
              onChange={(e) => onFromDateChange(e.target.value)}
            />
          </div>
          <div>
            <label className="block text-sm font-medium mb-1">Đến ngày</label>
            <Input
              type="date"
              value={toDate}
              onChange={(e) => onToDateChange(e.target.value)}
            />
          </div>
          <div>
            <label className="block text-sm font-medium mb-1">Nền tảng</label>
            <Select value={selectedPlatform} onValueChange={onPlatformChange}>
              <SelectTrigger className="w-40 bg-white">
                <SelectValue placeholder="Tất cả nền tảng" />
              </SelectTrigger>
              <SelectContent className="bg-white max-h-60 overflow-y-auto">
                <SelectItem value="all-platforms">Tất cả nền tảng</SelectItem>
                {platforms.map(platform => (
                  <SelectItem key={platform.platform_code} value={platform.platform_code}>
                    {platform.platform_name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div>
            <label className="block text-sm font-medium mb-1">Danh mục</label>
            <Select value={selectedCategory} onValueChange={onCategoryChange}>
              <SelectTrigger className="w-40 bg-white">
                <SelectValue placeholder="Tất cả danh mục" />
              </SelectTrigger>
              <SelectContent className="bg-white max-h-60 overflow-y-auto">
                <SelectItem value="all-categories">Tất cả danh mục</SelectItem>
                {categories.slice(0, 50).map(category => (
                  <SelectItem key={category.category_key} value={category.category_key}>
                    {category.category_name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <Button variant="default" onClick={onApplyFilters}>Áp dụng bộ lọc</Button>
        </div>
      </CardContent>
    </Card>
  );
}