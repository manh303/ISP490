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
import { getPlatforms, type Platform } from '../../services/analyticsApi';

interface PlatformSelectProps {
  value?: string;
  onValueChange: (value: string | undefined) => void;
  placeholder?: string;
}

export function PlatformSelect({ value, onValueChange, placeholder = 'Select platform' }: PlatformSelectProps) {
  const [platforms, setPlatforms] = useState<Platform[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const loadPlatforms = async () => {
      try {
        const data = await getPlatforms();
        setPlatforms(data);
      } catch (error) {
        console.error('Error loading platforms:', error);
      } finally {
        setLoading(false);
      }
    };

    loadPlatforms();
  }, []);

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
        <SelectItem value="all">All platforms</SelectItem>
        {platforms.map((platform) => (
          <SelectItem key={platform.platform_code} value={platform.platform_code}>
            {platform.platform_name}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}