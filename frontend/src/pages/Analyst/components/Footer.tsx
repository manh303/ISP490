import { Separator } from "../../../components/ui/figma/separator";

export function Footer() {
  const footerLinks = {
    Product: ["Features", "Pricing", "Analytics", "Reports"],
    Company: ["About", "Careers", "Press", "Contact"],
    Resources: ["Documentation", "Help Center", "API", "Community"],
    Legal: ["Privacy", "Terms", "Security", "Compliance"]
  };

  return (
    <footer className="border-t mt-12">
      <div className="container mx-auto px-4 py-12">
        <div className="grid grid-cols-2 md:grid-cols-4 gap-8 mb-8">
          {Object.entries(footerLinks).map(([category, links]) => (
            <div key={category}>
              <h4 className="mb-4">{category}</h4>
              <ul className="space-y-2">
                {links.map((link) => (
                  <li key={link}>
                    <a
                      href="#"
                      className="text-sm text-muted-foreground hover:text-foreground transition-colors"
                    >
                      {link}
                    </a>
                  </li>
                ))}
              </ul>
            </div>
          ))}
        </div>

        <Separator className="mb-8" />

        <div className="flex flex-col md:flex-row items-center justify-between gap-4">
          <div className="flex items-center gap-2">
            <div className="w-6 h-6 bg-gradient-to-br from-blue-500 to-purple-600 rounded" />
            <span className="text-sm">Market Insights Platform</span>
          </div>
          <p className="text-sm text-muted-foreground">
            © 2025 Market Insights. All rights reserved.
          </p>
        </div>
      </div>
    </footer>
  );
}
