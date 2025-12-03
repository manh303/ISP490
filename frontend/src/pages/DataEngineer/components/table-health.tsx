import { CheckCircle, Database } from 'lucide-react';

export function TableHealth() {
  const tables = [
    { name: 'fact_product', rows: '126.8K', size: '267MB', status: 'HEALTHY' },
    { name: 'fact_review', rows: '104.2K', size: '189MB', status: 'HEALTHY' },
    { name: 'dim_product', rows: '55.6K', size: '12MB', status: 'HEALTHY' },
  ];

  return (
    <div className="bg-white border border-slate-200 rounded-lg shadow-sm overflow-hidden">
      <div className="p-5 border-b border-slate-200">
        <h2 className="text-slate-800 flex items-center gap-2">
          <Database className="w-5 h-5 text-slate-600" />
          Table Health
        </h2>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full">
          <thead className="bg-slate-50 border-b border-slate-200">
            <tr>
              <th className="text-left p-4 text-slate-700">Bảng</th>
              <th className="text-left p-4 text-slate-700">Hàng</th>
              <th className="text-left p-4 text-slate-700">Kích thước</th>
              <th className="text-left p-4 text-slate-700">Trạng thái</th>
            </tr>
          </thead>
          <tbody>
            {tables.map((table, index) => (
              <tr
                key={table.name}
                className={`border-b border-slate-100 hover:bg-slate-50 transition-colors ${
                  index === tables.length - 1 ? 'border-b-0' : ''
                }`}
              >
                <td className="p-4">
                  <div className="flex items-center gap-2">
                    <Database className="w-4 h-4 text-slate-400" />
                    <span className="text-slate-800 font-mono text-sm">{table.name}</span>
                  </div>
                </td>
                <td className="p-4 text-slate-700">{table.rows}</td>
                <td className="p-4 text-slate-700">{table.size}</td>
                <td className="p-4">
                  <div className="flex items-center gap-2 text-green-600">
                    <CheckCircle className="w-4 h-4" />
                    <span>{table.status}</span>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
