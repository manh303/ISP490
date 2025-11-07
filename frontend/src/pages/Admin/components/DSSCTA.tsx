import { Button } from "../../../components/ui/figma/button";
import { ArrowRight, Shield } from "lucide-react";

export function DSSCTA() {
  return (
    <section className="py-20 bg-gradient-to-br from-blue-50 to-white">
      <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 text-center">
        <div className="bg-white rounded-2xl shadow-xl p-12 border border-blue-100">
          <div className="w-16 h-16 bg-gradient-to-br from-blue-600 to-blue-800 rounded-2xl flex items-center justify-center mx-auto mb-6">
            <Shield className="w-8 h-8 text-white" />
          </div>
          <h2 className="text-blue-900 mb-4">
            Bắt đầu vận hành hệ thống ngay hôm nay
          </h2>
          <p className="text-xl text-gray-600 mb-8 max-w-2xl mx-auto">
            Truy cập Admin Portal để quản lý, giám sát và tối ưu hóa hệ thống DSS Analytics của bạn
          </p>
          <Button size="lg" className="bg-blue-600 hover:bg-blue-700 gap-2">
            Truy cập Admin Portal
            <ArrowRight className="w-5 h-5" />
          </Button>
          <p className="text-sm text-gray-500 mt-6">
            Yêu cầu quyền quản trị viên để truy cập
          </p>
        </div>
      </div>
    </section>
  );
}
