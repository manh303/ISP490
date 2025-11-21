import { useState, useEffect } from 'react';
import { ChevronDown, Loader2 } from 'lucide-react';
import { getCategories, type Category } from '../../services/analyticsApi';

interface CategoryHierarchySelectorProps {
  platformCode?: string;
  onCategoryChange: (categoryKey: string | undefined, parentKey: string | undefined) => void;
  className?: string;
}

export function CategoryHierarchySelector({
  platformCode,
  onCategoryChange,
  className = "",
}: CategoryHierarchySelectorProps) {
  const [parentCategories, setParentCategories] = useState<Category[]>([]);
  const [childCategories, setChildCategories] = useState<Category[]>([]);
  const [selectedParent, setSelectedParent] = useState<string>('');
  const [selectedChild, setSelectedChild] = useState<string>('');
  const [loading, setLoading] = useState(false);

  // Load parent categories (level 1)
  useEffect(() => {
    const loadParentCategories = async () => {
      try {
        setLoading(true);
        const params: any = {};
        if (platformCode) params.platform_code = platformCode;

        const data = await getCategories(params);
        // Filter for parent categories (level 1 or no parent)
        const parents = data.filter(cat => !cat.parent_key || cat.level === 1);
        setParentCategories(parents);
      } catch (error) {
        console.error('Error loading parent categories:', error);
      } finally {
        setLoading(false);
      }
    };

    loadParentCategories();
  }, [platformCode]);

  // Load child categories when parent changes
  useEffect(() => {
    const loadChildCategories = async () => {
      if (!selectedParent) {
        setChildCategories([]);
        setSelectedChild('');
        onCategoryChange(undefined, selectedParent || undefined);
        return;
      }

      try {
        const params: any = { parent_category_key: selectedParent };
        if (platformCode) params.platform_code = platformCode;

        const data = await getCategories(params);
        setChildCategories(data);
        setSelectedChild('');
        onCategoryChange(undefined, selectedParent);
      } catch (error) {
        console.error('Error loading child categories:', error);
      }
    };

    loadChildCategories();
  }, [selectedParent, platformCode, onCategoryChange]);

  // Handle child category selection
  const handleChildChange = (childKey: string) => {
    setSelectedChild(childKey);
    onCategoryChange(childKey, selectedParent);
  };

  return (
    <div className={`flex gap-2 ${className}`}>
      {/* Parent Category Selector */}
      <div className="relative">
        <select
          value={selectedParent}
          onChange={(e) => setSelectedParent(e.target.value)}
          disabled={loading}
          className="h-11 appearance-none rounded-lg border border-gray-300 bg-transparent px-4 py-2.5 pr-11 text-sm shadow-theme-xs focus:border-brand-300 focus:outline-hidden focus:ring-3 focus:ring-brand-500/10 dark:border-gray-700 dark:bg-gray-900 dark:text-white/90 disabled:opacity-50"
        >
          <option value="">
            {loading ? 'Đang tải...' : 'Chọn danh mục cha'}
          </option>
          {parentCategories.map((category) => (
            <option key={category.category_key} value={category.category_key}>
              {category.category_name}
            </option>
          ))}
        </select>
        <ChevronDown className="absolute right-3 top-1/2 h-5 w-5 -translate-y-1/2 text-gray-500 pointer-events-none" />
      </div>

      {/* Child Category Selector */}
      {selectedParent && (
        <div className="relative">
          <select
            value={selectedChild}
            onChange={(e) => handleChildChange(e.target.value)}
            className="h-11 appearance-none rounded-lg border border-gray-300 bg-transparent px-4 py-2.5 pr-11 text-sm shadow-theme-xs focus:border-brand-300 focus:outline-hidden focus:ring-3 focus:ring-brand-500/10 dark:border-gray-700 dark:bg-gray-900 dark:text-white/90"
          >
            <option value="">Chọn danh mục con</option>
            {childCategories.map((category) => (
              <option key={category.category_key} value={category.category_key}>
                {category.category_name}
              </option>
            ))}
          </select>
          <ChevronDown className="absolute right-3 top-1/2 h-5 w-5 -translate-y-1/2 text-gray-500 pointer-events-none" />
        </div>
      )}
    </div>
  );
}