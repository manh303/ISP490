export function Footer() {
  const footerLinks = {
    Product: ["Features", "Pricing", "API", "Documentation"],
    Company: ["About", "Blog", "Careers", "Contact"],
    Resources: ["Help Center", "Community", "Status", "Terms of Service"],
    Legal: ["Privacy Policy", "Cookie Policy", "Licenses", "Security"],
  };

  return (
    <footer className="bg-gray-50 dark:bg-gray-900 border-t dark:border-gray-800 mt-12">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
        <div className="grid grid-cols-2 md:grid-cols-4 gap-8 mb-8">
          {Object.entries(footerLinks).map(([category, links]) => (
            <div key={category}>
              <h4 className="text-gray-900 dark:text-white mb-4">{category}</h4>
              <ul className="space-y-2">
                {links.map((link) => (
                  <li key={link}>
                    <a
                      href="#"
                      className="text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-white transition-colors"
                    >
                      {link}
                    </a>
                  </li>
                ))}
              </ul>
            </div>
          ))}
        </div>
        <div className="border-t dark:border-gray-800 pt-8 flex flex-col md:flex-row justify-between items-center">
          <div className="flex items-center gap-3 mb-4 md:mb-0">
            <div className="w-8 h-8 bg-gradient-to-br from-blue-600 to-purple-600 rounded-lg flex items-center justify-center">
              <span className="text-white">A</span>
            </div>
            <span className="text-gray-900 dark:text-white">AdminHub</span>
          </div>
          <p className="text-gray-600 dark:text-gray-400">
            © 2025 AdminHub. All rights reserved.
          </p>
        </div>
      </div>
    </footer>
  );
}
