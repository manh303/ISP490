import { CheckCircle, AlertTriangle } from 'lucide-react';

interface EtlJobCardProps {
  name: string;
  status: 'SUCCESS' | 'DEGRADED' | 'FAILED';
  successRate: number;
}

export function EtlJobCard({ name, status, successRate }: EtlJobCardProps) {
  const statusConfig = {
    SUCCESS: {
      icon: CheckCircle,
      color: 'text-green-600',
      bgColor: 'bg-green-50',
      borderColor: 'border-green-200',
    },
    DEGRADED: {
      icon: AlertTriangle,
      color: 'text-amber-600',
      bgColor: 'bg-amber-50',
      borderColor: 'border-amber-200',
    },
    FAILED: {
      icon: AlertTriangle,
      color: 'text-red-600',
      bgColor: 'bg-red-50',
      borderColor: 'border-red-200',
    },
  };

  const config = statusConfig[status];
  const Icon = config.icon;

  return (
    <div className={`${config.bgColor} ${config.borderColor} border rounded-lg p-5 shadow-sm hover:shadow-md transition-shadow`}>
      <h3 className="text-slate-800 mb-3">{name}</h3>
      <div className="flex items-center gap-2 mb-3">
        <Icon className={`w-5 h-5 ${config.color}`} />
        <span className={`${config.color}`}>{status}</span>
      </div>
      <div className="flex items-center gap-2">
        <div className="flex-1 bg-slate-200 rounded-full h-2 overflow-hidden">
          <div
            className={`h-full ${status === 'SUCCESS' ? 'bg-green-500' : status === 'DEGRADED' ? 'bg-amber-500' : 'bg-red-500'}`}
            style={{ width: `${successRate}%` }}
          />
        </div>
        <span className="text-slate-700 text-sm">{successRate}%</span>
      </div>
    </div>
  );
}
