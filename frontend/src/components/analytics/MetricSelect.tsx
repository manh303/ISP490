import { ChevronDown } from 'lucide-react';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '../ui/figma/select';

interface MetricSelectProps {
  value?: string;
  onValueChange: (value: string) => void;
}

const metrics = [
  { value: 'revenue', label: 'Doanh thu' },
  { value: 'review_count', label: 'Số đánh giá' },
  { value: 'avg_rating', label: 'Đánh giá trung bình' },
  { value: 'price_growth', label: 'Tăng trưởng giá' },
];

export function MetricSelect({ value = 'revenue', onValueChange }: MetricSelectProps) {
  return (
    <Select value={value} onValueChange={onValueChange}>
      <SelectTrigger className="w-[200px]">
        <SelectValue placeholder="Chọn tiêu chí" />
      </SelectTrigger>
      <SelectContent>
        {metrics.map((metric) => (
          <SelectItem key={metric.value} value={metric.value}>
            {metric.label}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}