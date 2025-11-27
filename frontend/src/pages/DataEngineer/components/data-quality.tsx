import { AlertCircle, AlertTriangle, Info } from 'lucide-react';

export function DataQuality() {
  const issues = [
    { severity: 'CRITICAL', count: 1, icon: AlertCircle, color: 'text-red-600' },
    { severity: 'HIGH', count: 1, icon: AlertTriangle, color: 'text-orange-600' },
    { severity: 'MEDIUM', count: 2, icon: Info, color: 'text-amber-600' },
  ];

  return (
    <div className="bg-white border border-slate-200 rounded-lg p-5 shadow-sm h-full">
      <h2 className="text-slate-800 mb-4 flex items-center gap-2">
        <AlertTriangle className="w-5 h-5 text-amber-600" />
        Data Quality
      </h2>
      <div className="mb-4">
        <div className="text-slate-900 text-2xl">⚠️ 4 Issues</div>
      </div>
      <div className="space-y-3">
        {issues.map((issue) => {
          const Icon = issue.icon;
          return (
            <div key={issue.severity} className="flex items-center justify-between p-3 bg-slate-50 rounded-md">
              <div className="flex items-center gap-2">
                <Icon className={`w-4 h-4 ${issue.color}`} />
                <span className="text-slate-700">{issue.severity}</span>
              </div>
              <span className={`${issue.color}`}>{issue.count}</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}
