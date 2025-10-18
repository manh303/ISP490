import { ArrowLeft } from "lucide-react";
import type { Page } from "../App";

interface ForgotPasswordPageProps {
  navigateTo: (page: Page) => void;
}

export function ForgotPasswordPage({ navigateTo }: ForgotPasswordPageProps) {
  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 via-white to-blue-50 flex items-center justify-center p-8">
      {/* Back Button */}
      <button
        onClick={() => navigateTo("login")}
        className="absolute top-8 left-8 flex items-center gap-2 text-gray-600 hover:text-gray-900 transition-colors"
      >
        <ArrowLeft className="w-5 h-5" />
        <span>Quay lại đăng nhập</span>
      </button>

      {/* Forgot Password Card */}
      <div className="w-full max-w-[450px]">
        <div className="bg-white overflow-clip relative rounded-[16px] shadow-[0px_20px_25px_-5px_rgba(0,0,0,0.1),0px_8px_10px_-6px_rgba(0,0,0,0.1)] p-8">
          {/* Header */}
          <div className="mb-8 text-center">
            <div className="w-48 h-10 bg-gray-400 mx-auto mb-4"></div>
            <div className="w-80 h-4 bg-gray-300 mx-auto mb-2"></div>
            <div className="w-64 h-4 bg-gray-300 mx-auto"></div>
          </div>

          {/* Icon */}
          <div className="w-20 h-20 bg-blue-100 rounded-full mx-auto mb-6"></div>

          {/* Form */}
          <div className="space-y-6">
            {/* Email */}
            <div className="space-y-2">
              <div className="w-32 h-4 bg-gray-400"></div>
              <div className="w-full h-12 bg-white border-2 border-gray-300 rounded-lg"></div>
            </div>

            {/* Submit Button */}
            <button className="w-full h-12 bg-blue-600 hover:bg-blue-700 rounded-lg transition-colors"></button>
          </div>

          {/* Back to Login */}
          <div className="mt-6 text-center">
            <button
              onClick={() => navigateTo("login")}
              className="w-40 h-4 bg-blue-400 mx-auto hover:opacity-80 transition-opacity"
            ></button>
          </div>
        </div>
      </div>
    </div>
  );
}
