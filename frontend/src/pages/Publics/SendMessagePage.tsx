import { Header } from "./Header";
import { Footer } from "./Footer";
import type { Page } from "../../App";

interface SendMessagePageProps {
  navigateTo: (page: Page) => void;
  isLoggedIn: boolean;
  onLogout: () => void;
}

export function SendMessagePage({ navigateTo, isLoggedIn, onLogout }: SendMessagePageProps) {
  return (
    <div className="min-h-screen bg-white">
      {/* <Header navigateTo={navigateTo} isLoggedIn={isLoggedIn} onLogout={onLogout} /> */}
      
      {/* Hero Section */}
      <section className="py-16 bg-gray-100">
        <div className="max-w-2xl mx-auto px-6 text-center">
          <div className="w-64 h-10 bg-gray-400 mx-auto mb-4"></div>
          <div className="w-96 h-4 bg-gray-300 mx-auto"></div>
        </div>
      </section>

      {/* Form Section */}
      <section className="py-16 bg-white">
        <div className="max-w-2xl mx-auto px-6">
          <div className="bg-white border-2 border-gray-300 rounded-lg p-8">
            {/* Form Header */}
            <div className="flex items-center gap-3 mb-8">
              <div className="w-12 h-12 bg-blue-200 rounded"></div>
              <div>
                <div className="w-48 h-6 bg-gray-600 mb-2"></div>
                <div className="w-64 h-3 bg-gray-300"></div>
              </div>
            </div>

            {/* Form Fields */}
            <div className="space-y-6">
              {/* Name & Email */}
              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-2">
                  <div className="w-24 h-4 bg-gray-400"></div>
                  <div className="w-full h-12 bg-white border-2 border-gray-300 rounded"></div>
                </div>
                <div className="space-y-2">
                  <div className="w-16 h-4 bg-gray-400"></div>
                  <div className="w-full h-12 bg-white border-2 border-gray-300 rounded"></div>
                </div>
              </div>

              {/* Phone & Company */}
              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-2">
                  <div className="w-32 h-4 bg-gray-400"></div>
                  <div className="w-full h-12 bg-white border-2 border-gray-300 rounded"></div>
                </div>
                <div className="space-y-2">
                  <div className="w-20 h-4 bg-gray-400"></div>
                  <div className="w-full h-12 bg-white border-2 border-gray-300 rounded"></div>
                </div>
              </div>

              {/* Subject */}
              <div className="space-y-2">
                <div className="w-24 h-4 bg-gray-400"></div>
                <div className="w-full h-12 bg-white border-2 border-gray-300 rounded"></div>
              </div>

              {/* Message */}
              <div className="space-y-2">
                <div className="w-28 h-4 bg-gray-400"></div>
                <div className="w-full h-40 bg-white border-2 border-gray-300 rounded"></div>
              </div>

              {/* Submit Button */}
              <button className="w-full h-12 bg-blue-600 hover:bg-blue-700 rounded transition-colors"></button>
            </div>
          </div>
        </div>
      </section>

      {/* <Footer /> */}
    </div>
  );
}
