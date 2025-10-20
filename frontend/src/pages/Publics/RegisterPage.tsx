import { ArrowLeft } from "lucide-react";
import type { Page } from "../App";

interface RegisterPageProps {
  navigateTo: (page: Page) => void;
}

export function RegisterPage({ navigateTo }: RegisterPageProps) {
  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 via-white to-blue-50 flex items-center justify-center p-8">
      {/* Back Button */}
      <button
        onClick={() => navigateTo("home")}
        className="absolute top-8 left-8 flex items-center gap-2 text-gray-600 hover:text-gray-900 transition-colors"
      >
        <ArrowLeft className="w-5 h-5" />
        <span>Quay lại trang chủ</span>
      </button>

      {/* Register Card */}
      <div className="w-full max-w-[500px]">
        <div className="bg-white overflow-clip relative rounded-[16px] shadow-[0px_20px_25px_-5px_rgba(0,0,0,0.1),0px_8px_10px_-6px_rgba(0,0,0,0.1)] p-8">
          {/* Header */}
          <div className="mb-8">
            <div className="w-64 h-10 bg-gray-400 mb-4"></div>
            <div className="w-80 h-4 bg-gray-300"></div>
          </div>

          {/* Form */}
          <div className="space-y-6">
            {/* Name Fields */}
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <div className="w-16 h-4 bg-gray-400"></div>
                <div className="w-full h-12 bg-white border-2 border-gray-300 rounded-lg"></div>
              </div>
              <div className="space-y-2">
                <div className="w-12 h-4 bg-gray-400"></div>
                <div className="w-full h-12 bg-white border-2 border-gray-300 rounded-lg"></div>
              </div>
            </div>

            {/* Email */}
            <div className="space-y-2">
              <div className="w-16 h-4 bg-gray-400"></div>
              <div className="w-full h-12 bg-white border-2 border-gray-300 rounded-lg"></div>
            </div>

            {/* Phone */}
            <div className="space-y-2">
              <div className="w-24 h-4 bg-gray-400"></div>
              <div className="w-full h-12 bg-white border-2 border-gray-300 rounded-lg"></div>
            </div>

            {/* Company */}
            <div className="space-y-2">
              <div className="w-20 h-4 bg-gray-400"></div>
              <div className="w-full h-12 bg-white border-2 border-gray-300 rounded-lg"></div>
            </div>

            {/* Password */}
            <div className="space-y-2">
              <div className="w-20 h-4 bg-gray-400"></div>
              <div className="w-full h-12 bg-white border-2 border-gray-300 rounded-lg"></div>
            </div>

            {/* Confirm Password */}
            <div className="space-y-2">
              <div className="w-32 h-4 bg-gray-400"></div>
              <div className="w-full h-12 bg-white border-2 border-gray-300 rounded-lg"></div>
            </div>

            {/* Terms Checkbox */}
            <div className="flex items-center gap-2">
              <div className="w-5 h-5 bg-white border-2 border-gray-300 rounded"></div>
              <div className="w-64 h-4 bg-gray-300"></div>
            </div>

            {/* Register Button */}
            <button className="w-full h-12 bg-blue-600 hover:bg-blue-700 rounded-lg transition-colors"></button>
          </div>

          {/* Login Link */}
          <div className="mt-6 text-center">
            <div className="flex items-center justify-center gap-2">
              <div className="w-40 h-4 bg-gray-300"></div>
              <button
                onClick={() => navigateTo("login")}
                className="w-24 h-4 bg-blue-400 hover:opacity-80 transition-opacity"
              ></button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
