import { useState, useEffect } from 'react';
import { ChevronDown, Loader2 } from 'lucide-react';
import { Button } from '../ui/figma/button';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '../ui/figma/select';
import { getCategories, type Category } from '../../services/analyticsApi';

interface CategorySelectProps {
  value?: string;
  onValueChange: (value: string | undefined) => void;
  platformCode?: string;
  parentCategoryKey?: string;
  placeholder?: string;
}

export function CategorySelect({ value, onValueChange, platformCode, parentCategoryKey, placeholder = 'Select category' }: CategorySelectProps) {
  const [categories, setCategories] = useState<Category[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const loadCategories = async () => {
      try {
        setLoading(true);
        const params: any = {};
        if (platformCode) params.platform_code = platformCode;
        if (parentCategoryKey) params.parent_category_key = parentCategoryKey;
        
        const data = await getCategories(params);
        setCategories(data);
      } catch (error) {
        console.error('Error loading categories:', error);
      } finally {
        setLoading(false);
      }
    };

    loadCategories();
  }, [platformCode, parentCategoryKey]);

  if (loading) {
    return (
      <Button variant="outline" disabled className="w-[200px]">
        <Loader2 className="mr-2 h-4 w-4 animate-spin" />
        Loading...
      </Button>
    );
  }

  return (
    <Select value={value} onValueChange={(val) => onValueChange(val === 'all' ? undefined : val)}>
      <SelectTrigger className="w-[200px] bg-white">
        <SelectValue placeholder={placeholder} />
      </SelectTrigger>
      <SelectContent className="bg-white max-h-60 overflow-y-auto">
        <SelectItem value="all">All categories</SelectItem>
        {categories.map((category) => (
          <SelectItem key={category.category_key} value={category.category_key}>
            {category.category_name}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}