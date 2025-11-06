import { Database, Workflow, Brain, BarChart3, ArrowRight } from "lucide-react";

export function DSSSystemOverview() {
  const steps = [
    {
      icon: Workflow,
      title: "Crawler",
      description: "Thu thập dữ liệu",
    },
    {
      icon: Database,
      title: "Data Warehouse",
      description: "Lưu trữ tập trung",
    },
    {
      icon: Brain,
      title: "ML Pipeline",
      description: "Xử lý & phân tích",
    },
    {
      icon: BarChart3,
      title: "Dashboard",
      description: "Trực quan hóa",
    },
  ];

  return (
    <section className="py-20 bg-gradient-to-br from-blue-900 via-blue-800 to-blue-900 text-white">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="text-center mb-16">
          <h2 className="text-white mb-4">
            Kiến Trúc Hệ Thống
          </h2>
          <p className="text-xl text-blue-100 max-w-3xl mx-auto">
            Quy trình xử lý dữ liệu từ đầu đến cuối được thiết kế tối ưu và tự động hóa
          </p>
        </div>

        {/* System Flow */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-6 relative">
          {steps.map((step, index) => {
            const Icon = step.icon;
            return (
              <div key={step.title}>
                <div className="relative bg-white/10 backdrop-blur-sm border border-white/20 rounded-xl p-6 hover:bg-white/15 transition-all">
                  <div className="w-16 h-16 bg-gradient-to-br from-blue-400 to-blue-600 rounded-xl flex items-center justify-center mb-4 mx-auto">
                    <Icon className="w-8 h-8 text-white" />
                  </div>
                  <h3 className="text-white text-center mb-2">{step.title}</h3>
                  <p className="text-blue-200 text-center text-sm">
                    {step.description}
                  </p>
                </div>
                {/* Arrow between steps */}
                {index < steps.length - 1 && (
                  <div className="hidden md:flex absolute top-1/2 -translate-y-1/2 left-[calc((100%/4)*{index}+100%/4-20px)] items-center justify-center w-10">
                    <ArrowRight className="w-6 h-6 text-blue-300" />
                  </div>
                )}
              </div>
            );
          })}
        </div>

        {/* Additional Info */}
        <div className="mt-16 grid md:grid-cols-3 gap-8">
          <div className="text-center">
            <div className="text-4xl text-blue-300 mb-2">99.9%</div>
            <div className="text-blue-100">Uptime</div>
          </div>
          <div className="text-center">
            <div className="text-4xl text-blue-300 mb-2">24/7</div>
            <div className="text-blue-100">Giám sát</div>
          </div>
          <div className="text-center">
            <div className="text-4xl text-blue-300 mb-2">Real-time</div>
            <div className="text-blue-100">Xử lý dữ liệu</div>
          </div>
        </div>
      </div>
    </section>
  );
}
