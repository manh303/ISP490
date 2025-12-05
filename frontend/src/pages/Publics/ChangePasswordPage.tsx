import { ArrowLeft, Shield } from "lucide-react";
import type { Page } from "../../App";

interface ChangePasswordPageProps {
  navigateTo: (page: Page) => void;
  onLogout: () => void;
}

export function ChangePasswordPage({ navigateTo, onLogout }: ChangePasswordPageProps) {
  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white border-b border-gray-200 shadow-sm">
        <div className="max-w-7xl mx-auto px-6 h-20 flex items-center justify-between">
          <div className="flex items-center gap-4">
            <button
              onClick={() => navigateTo("dashboard")}
              className="flex items-center gap-2 text-gray-600 hover:text-gray-900 transition-colors"
            >
              <ArrowLeft className="w-4 h-4" />
              Dashboard
            </button>
          </div>

          <div className="flex items-center gap-4">
            <button
              onClick={onLogout}
              className="text-gray-600 hover:text-gray-900"
            >
              Logout
            </button>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="max-w-2xl mx-auto px-6 py-16">
        <div className="bg-white rounded-[16px] shadow-lg p-8">
          {/* Header */}
          <div className="flex items-center gap-4 mb-8">
            <div className="w-16 h-16 bg-blue-100 rounded-full flex items-center justify-center">
              <Shield className="w-8 h-8 text-blue-600" />
            </div>
            <div>
              <div className="w-48 h-8 bg-gray-600 mb-2"></div>
              <div className="w-64 h-4 bg-gray-300"></div>
            </div>
          </div>

          {/* Form */}
          <div className="space-y-6">
            {/* Current Password */}
            <div className="space-y-2">
              <div className="w-32 h-4 bg-gray-400"></div>
              <div className="w-full h-12 bg-white border-2 border-gray-300 rounded-lg"></div>
            </div>

            {/* New Password */}
            <div className="space-y-2">
              <div className="w-28 h-4 bg-gray-400"></div>
              <div className="w-full h-12 bg-white border-2 border-gray-300 rounded-lg"></div>
            </div>

            {/* Confirm New Password */}
            <div className="space-y-2">
              <div className="w-40 h-4 bg-gray-400"></div>
              <div className="w-full h-12 bg-white border-2 border-gray-300 rounded-lg"></div>
            </div>

            {/* Password Requirements */}
            <div className="bg-blue-50 border-2 border-blue-200 rounded-lg p-4">
              <div className="w-40 h-5 bg-blue-600 mb-3"></div>
              <div className="space-y-2">
                {[1, 2, 3, 4].map((i) => (
                  <div key={i} className="flex items-center gap-2">
                    <div className="w-4 h-4 bg-green-500 rounded-full"></div>
                    <div className="w-48 h-3 bg-gray-300"></div>
                  </div>
                ))}
              </div>
            </div>

            {/* Buttons */}
            <div className="flex gap-4">
              <button
                onClick={() => navigateTo("dashboard")}
                className="flex-1 h-12 bg-white border-2 border-gray-300 hover:bg-gray-50 rounded-lg transition-colors"
              ></button>
              <button className="flex-1 h-12 bg-blue-600 hover:bg-blue-700 rounded-lg transition-colors"></button>
            </div>
          </div>
        </div>
      </main>
    </div>
  );
}
