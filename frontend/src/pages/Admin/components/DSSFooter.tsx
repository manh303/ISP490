import { Database, Mail, Phone, MapPin } from "lucide-react";

export function DSSFooter() {
  const footerSections = {
    "Products": ["Features", "Documentation", "API", "Pricing"],
    "Company": ["About Us", "Blog", "Careers", "Contact"],
    "Support": ["Support Center", "Community", "System Status", "FAQ"],
  };

  return (
    <footer className="bg-blue-900 text-white">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
        <div className="grid grid-cols-1 md:grid-cols-4 gap-8 mb-8">
          {/* Logo & Description */}
          <div className="md:col-span-1">
            <div className="flex items-center gap-2 mb-4">
              <div className="w-10 h-10 bg-gradient-to-br from-blue-400 to-blue-600 rounded-lg flex items-center justify-center">
                <Database className="w-6 h-6 text-white" />
              </div>
              <span className="text-white">DSS Analytics</span>
            </div>
            <p className="text-blue-200 text-sm mb-4">
              Comprehensive data analytics platform for modern businesses
            </p>
            <div className="flex gap-3 text-blue-200 text-sm">
              <Mail className="w-4 h-4" />
              <Phone className="w-4 h-4" />
              <MapPin className="w-4 h-4" />
            </div>
          </div>

          {/* Footer Links */}
          {Object.entries(footerSections).map(([title, links]) => (
            <div key={title}>
              <h4 className="text-white mb-4">{title}</h4>
              <ul className="space-y-2">
                {links.map((link) => (
                  <li key={link}>
                    <a
                      href="#"
                      className="text-blue-200 hover:text-white transition-colors text-sm"
                    >
                      {link}
                    </a>
                  </li>
                ))}
              </ul>
            </div>
          ))}
        </div>

        <div className="border-t border-blue-800 pt-8 flex flex-col md:flex-row justify-between items-center">
          <p className="text-blue-200 text-sm mb-4 md:mb-0">
            © 2025 DSS Analytics. All rights reserved.
          </p>
          <div className="flex gap-6 text-sm text-blue-200">
            <a href="#" className="hover:text-white transition-colors">
              Privacy Policy
            </a>
            <a href="#" className="hover:text-white transition-colors">
              Terms of Service
            </a>
            <a href="#" className="hover:text-white transition-colors">
              Cookies
            </a>
          </div>
        </div>
      </div>
    </footer>
  );
}
