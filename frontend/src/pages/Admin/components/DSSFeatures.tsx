import { Users, Bot, Server } from "lucide-react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "../../../components/ui/figma/card";

const features = [
  {
    title: "User Management",
    description: "Create, assign permissions, and monitor user accounts in the system easily and securely.",
    icon: Users,
    color: "from-blue-600 to-blue-700",
  },
  {
    title: "Crawler Monitoring",
    description: "Monitor progress and logs of data collection from various sources in real-time.",
    icon: Bot,
    color: "from-blue-700 to-blue-800",
  },
  {
    title: "System Management",
    description: "Control ELT Jobs, logs and pipeline to ensure smooth and efficient system operation.",
    icon: Server,
    color: "from-blue-800 to-blue-900",
  },
];

export function DSSFeatures() {
  return (
    <section className="py-20 bg-white">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="text-center mb-16">
          <h2 className="text-blue-900 mb-4">
            Powerful Admin Features
          </h2>
          <p className="text-xl text-gray-600 max-w-3xl mx-auto">
            Comprehensive tools to help administrators control every aspect of the system
          </p>
        </div>

        <div className="grid md:grid-cols-3 gap-8">
          {features.map((feature) => {
            const Icon = feature.icon;
            return (
              <Card key={feature.title} className="border-blue-100 hover:shadow-xl transition-shadow">
                <CardHeader>
                  <div className={`w-14 h-14 bg-gradient-to-br ${feature.color} rounded-xl flex items-center justify-center mb-4`}>
                    <Icon className="w-7 h-7 text-white" />
                  </div>
                  <CardTitle className="text-blue-900">{feature.title}</CardTitle>
                  <CardDescription className="text-gray-600 leading-relaxed">
                    {feature.description}
                  </CardDescription>
                </CardHeader>
              </Card>
            );
          })}
        </div>
      </div>
    </section>
  );
}
