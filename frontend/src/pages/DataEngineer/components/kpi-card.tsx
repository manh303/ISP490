interface KpiCardProps {
  icon: string;
  label: string;
  value: string;
  variant?: 'default' | 'warning';
}

export function KpiCard({ icon, label, value, variant = 'default' }: KpiCardProps) {
  const bgColor = variant === 'warning' ? 'bg-amber-50 border-amber-200' : 'bg-white border-slate-200';
  const textColor = variant === 'warning' ? 'text-amber-900' : 'text-slate-900';
  
  return (
    <div className={`${bgColor} border rounded-lg p-4 shadow-sm hover:shadow-md transition-shadow`}>
      <div className="flex items-center gap-2 mb-2">
        <span className="text-2xl">{icon}</span>
      </div>
      <div className={`${textColor}`}>{value}</div>
      <div className="text-slate-500 text-sm">{label}</div>
    </div>
  );
}
