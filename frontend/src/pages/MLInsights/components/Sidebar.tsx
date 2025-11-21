import { 
  Sparkles, 
  ShoppingBag, 
  TrendingUp, 
  BarChart3, 
  Users, 
  Database, 
  Box, 
  GitBranch,
  Settings,
  Key,
  FileText
} from 'lucide-react';

interface SidebarProps {
  currentPage: string;
  onNavigate: (page: string) => void;
}

export function Sidebar({ currentPage, onNavigate }: SidebarProps) {
  const menuSections = [
    {
      title: 'ML Insights',
      items: [
        { id: 'dashboard', label: 'Dashboard', icon: Sparkles },
        { id: 'product-recommendation', label: 'Product Recommendation', icon: ShoppingBag },
        { id: 'price-prediction', label: 'Price Prediction', icon: TrendingUp },
        { id: 'demand-forecast', label: 'Demand Forecast', icon: BarChart3 },
        { id: 'customer-segments', label: 'Customer Segments', icon: Users },
      ],
    },
    {
      title: 'Data Warehouse',
      items: [
        { id: 'dimensions', label: 'Dimensions', icon: Box },
        { id: 'facts', label: 'Facts', icon: Database },
        { id: 'pipelines', label: 'Pipelines', icon: GitBranch },
      ],
    },
    {
      title: 'System',
      items: [
        { id: 'settings', label: 'Settings', icon: Settings },
        { id: 'api-keys', label: 'API Keys', icon: Key },
        { id: 'logs', label: 'Logs', icon: FileText },
      ],
    },
  ];

  return (
    <div className="w-64 bg-[#0f172a] text-white flex flex-col">
      {/* Logo */}
      <div className="p-6 border-b border-white/10">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 bg-gradient-to-br from-[#1d4ed8] to-[#1e3a8a] rounded-lg flex items-center justify-center">
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none">
              <path d="M12 2L3 7L12 12L21 7L12 2Z" fill="white" opacity="0.9"/>
              <path d="M3 17L12 22L21 17" stroke="white" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" opacity="0.7"/>
              <path d="M3 12L12 17L21 12" stroke="white" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" opacity="0.7"/>
            </svg>
          </div>
          <div>
            <div className="tracking-tight">DataML</div>
            <div className="text-xs text-white/60">Insights Platform</div>
          </div>
        </div>
      </div>

      {/* Navigation */}
      <nav className="flex-1 overflow-y-auto p-4 space-y-6">
        {menuSections.map((section) => (
          <div key={section.title}>
            <div className="px-3 mb-2 text-xs tracking-wider text-white/40 uppercase">
              {section.title}
            </div>
            <div className="space-y-1">
              {section.items.map((item) => {
                const Icon = item.icon;
                const isActive = currentPage === item.id;
                return (
                  <button
                    key={item.id}
                    onClick={() => onNavigate(item.id)}
                    className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-lg transition-all ${
                      isActive
                        ? 'bg-[#1d4ed8] text-white'
                        : 'text-white/70 hover:bg-[#1d4ed8]/20 hover:text-white'
                    }`}
                  >
                    <Icon className="w-5 h-5" />
                    <span className="text-sm">{item.label}</span>
                  </button>
                );
              })}
            </div>
          </div>
        ))}
      </nav>
    </div>
  );
}
