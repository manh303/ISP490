
import type { Page } from "../../App";

interface ContactPageProps {
  navigateTo: (page: Page) => void;
  isLoggedIn: boolean;
  onLogout: () => void;
}

export function ContactPage( { navigateTo, isLoggedIn, onLogout }: ContactPageProps) {
  return (
    <div className="min-h-screen bg-white">
      
      
      {/* Hero Section */}
      <section className="py-20 bg-gray-100">
        <div className="max-w-4xl mx-auto px-6 text-center">
          <div className="w-80 h-12 bg-gray-400 mx-auto mb-6"></div>
          <div className="w-full max-w-2xl h-6 bg-gray-300 mx-auto"></div>
        </div>
      </section>

      {/* Contact Info Cards */}
      <section className="py-16 bg-white">
        <div className="max-w-7xl mx-auto px-6">
          <div className="grid grid-cols-4 gap-6 mb-12">
            {[1, 2, 3, 4].map((i) => (
              <div key={i} className="bg-white border-2 border-gray-300 p-6 text-center">
                <div className="w-16 h-16 bg-blue-200 rounded-full mx-auto mb-4"></div>
                <div className="w-32 h-6 bg-gray-600 mx-auto mb-3"></div>
                <div className="w-full h-4 bg-gray-300 mb-2"></div>
                <div className="w-3/4 h-4 bg-gray-300 mx-auto"></div>
              </div>
            ))}
          </div>

          {/* CTA to Send Message */}
          <div className="text-center">
            <div className="w-48 h-8 bg-gray-600 mx-auto mb-4"></div>
            <div className="w-96 h-4 bg-gray-300 mx-auto mb-8"></div>
            <button
              onClick={() => navigateTo("send-message")}
              className="w-48 h-12 bg-blue-600 hover:bg-blue-700 rounded mx-auto transition-colors"
            ></button>
          </div>
        </div>
      </section>

      {/* Office Location with Map */}
      <section className="py-16 bg-gray-50">
        <div className="max-w-7xl mx-auto px-6">
          <div className="text-center mb-12">
            <div className="w-64 h-10 bg-gray-400 mx-auto mb-4"></div>
            <div className="w-96 h-4 bg-gray-300 mx-auto"></div>
          </div>

          {/* Map Placeholder */}
          <div className="w-full h-96 bg-gray-300 rounded relative overflow-hidden">
            <div className="absolute bottom-0 left-0 right-0 bg-gradient-to-t from-black/60 to-transparent p-8">
              <div className="w-48 h-6 bg-white/80 mb-2"></div>
              <div className="w-64 h-4 bg-white/60"></div>
            </div>
          </div>
        </div>
      </section>

 
    </div>
  );
}
