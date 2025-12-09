import { useState, useEffect } from 'react';
import { Calendar, ChevronDown } from 'lucide-react';
import { Button } from '../ui/figma/button';
import { Popover, PopoverContent, PopoverTrigger } from '../ui/figma/popover';
import { Calendar as CalendarComponent } from '../ui/figma/calendar';
import { format } from 'date-fns';
// import { vi } from 'date-fns/locale';
interface DateRangePickerProps {
  fromDate: Date | undefined;
  toDate: Date | undefined;
  onFromDateChange: (date: Date | undefined) => void;
  onToDateChange: (date: Date | undefined) => void;
}

export function DateRangePicker({ fromDate, toDate, onFromDateChange, onToDateChange }: DateRangePickerProps) {
  const [isOpen, setIsOpen] = useState(false);

  const formatDateRange = () => {
    if (!fromDate && !toDate) return 'Select time period';
    if (fromDate && toDate) {
      return `${format(fromDate, 'dd/MM/yyyy')} - ${format(toDate, 'dd/MM/yyyy')}`;
    }
    if (fromDate) return `From ${format(fromDate, 'dd/MM/yyyy')}`;
    if (toDate) return `To ${format(toDate, 'dd/MM/yyyy')}`;
    return 'Select time period';
  };

  return (
    <Popover open={isOpen} onOpenChange={setIsOpen}>
      <PopoverTrigger asChild>
        <Button variant="outline" className="w-[280px] justify-start text-left font-normal">
          <Calendar className="mr-2 h-4 w-4" />
          {formatDateRange()}
          <ChevronDown className="ml-auto h-4 w-4" />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-auto p-0 bg-white" align="start">
        <div className="p-3">
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="text-sm font-medium mb-2 block">From date</label>
              <CalendarComponent
                mode="single"
                selected={fromDate}
                onSelect={onFromDateChange}
                initialFocus
                // locale={vi}
                // weekStartsOn={1}
              />
            </div>
            <div>
              <label className="text-sm font-medium mb-2 block">To date</label>
              <CalendarComponent
                mode="single"
                selected={toDate}
                onSelect={onToDateChange}
                initialFocus
                // locale={vi}
                // weekStartsOn={1}
              />
            </div>
          </div>
          <div className="flex justify-end gap-2 mt-4">
            <Button
              variant="outline"
              size="sm"
              onClick={() => {
                onFromDateChange(undefined);
                onToDateChange(undefined);
              }}
            >
              Clear
            </Button>
            <Button
              size="sm"
              onClick={() => setIsOpen(false)}>
              Apply
            </Button>
          </div>
        </div>
      </PopoverContent>
    </Popover>
  );
}