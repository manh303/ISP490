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
        <CardTitle>Filters</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="flex flex-wrap gap-4 items-end">
          <div>
            <label className="block text-sm font-medium mb-1">From Date</label>
            <Input
              type="date"
              value={fromDate}
              onChange={(e) => onFromDateChange(e.target.value)}
            />
          </div>
          <div>
            <label className="block text-sm font-medium mb-1">To Date</label>
            <Input
              type="date"
              value={toDate}
              onChange={(e) => onToDateChange(e.target.value)}
            />
          </div>
          <div>
            <label className="block text-sm font-medium mb-1">Platform</label>
            <Select value={selectedPlatform} onValueChange={onPlatformChange}>
              <SelectTrigger className="w-40">
                <SelectValue placeholder="All Platforms" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all-platforms">All Platforms</SelectItem>
                {platforms.map(platform => (
                  <SelectItem key={platform.platform_code} value={platform.platform_code}>
                    {platform.platform_name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div>
            <label className="block text-sm font-medium mb-1">Category</label>
            <Select value={selectedCategory} onValueChange={onCategoryChange}>
              <SelectTrigger className="w-40">
                <SelectValue placeholder="All Categories" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all-categories">All Categories</SelectItem>
                {categories.slice(0, 50).map(category => (
                  <SelectItem key={category.category_key} value={category.category_key}>
                    {category.category_name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <Button onClick={onApplyFilters}>Apply Filters</Button>
        </div>
      </CardContent>
    </Card>
  );
}