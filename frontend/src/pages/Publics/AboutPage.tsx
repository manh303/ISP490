import { Target, Users, Award, TrendingUp, CheckCircle, Zap, Shield, Globe } from "lucide-react";
import { Card } from "../../components/ui/figma/card";
import { ImageWithFallback } from "../../components/figma/ImageWithFallback";
import type { Page } from "../../App";

interface AboutPageProps {
  navigateTo: (page: Page) => void;
  isLoggedIn: boolean;
  onLogout: () => void;
}

export function AboutPage( { navigateTo, isLoggedIn, onLogout }: AboutPageProps) {
  const values = [
    {
      icon: Target,
      title: "Customer Focus",
      description: "We put customer needs and success at the center of every decision",
    },
    {
      icon: Zap,
      title: "Continuous Innovation",
      description: "Constantly improving and developing technology to bring the best solutions",
    },
    {
      icon: Shield,
      title: "Security & Trust",
      description: "Committed to protecting customer data and privacy at the highest level",
    },
    {
      icon: Globe,
      title: "Global Vision",
      description: "Developing solutions that meet international standards and global trends",
    },
  ];

  const stats = [
    { label: "Customers", value: "500+", icon: Users },
    { label: "Successful Projects", value: "1,200+", icon: CheckCircle },
    { label: "Years of Experience", value: "10+", icon: Award },
    { label: "Growth", value: "45%", icon: TrendingUp },
  ];

  const team = [
    {
      name: "Nguyễn Văn A",
      position: "CEO & Founder",
      description: "15 years of experience in data analysis and AI",
    },
    {
      name: "Trần Thị B",
      position: "CTO",
      description: "Technology expert with multiple international awards",
    },
    {
      name: "Lê Văn C",
      position: "Head of Product",
      description: "10 years developing products for large enterprises",
    },
    {
      name: "Phạm Thị D",
      position: "Head of Customer Success",
      description: "Consulting expert with over 300 successful project implementations",
    },
  ];

  return (
    <div className="min-h-screen bg-white">
     
      {/* Hero Section */}
      <section className="relative py-20 bg-gradient-to-br from-blue-50 to-white overflow-hidden">
        <div className="max-w-7xl mx-auto px-6">
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-12 items-center">
            <div>
              <h1 className="text-gray-900 mb-6">
                About DSS Analytics
              </h1>
              <p className="text-gray-600 text-lg mb-6">
                We are a team of technology enthusiasts, dedicated to developing solutions 
                that support smart decision-making for Vietnamese businesses.
              </p>
              <p className="text-gray-600 text-lg">
                With over 10 years of experience in data analysis and AI, we clearly understand 
                the challenges businesses are facing and are committed to bringing the most 
                powerful tools to help you succeed.
              </p>
            </div>
            <div className="relative h-[400px] rounded-2xl overflow-hidden shadow-2xl">
              <ImageWithFallback
                src="https://images.unsplash.com/photo-1709715357520-5e1047a2b691?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3Nzg4Nzd8MHwxfHNlYXJjaHwxfHxidXNpbmVzcyUyMHRlYW0lMjBtZWV0aW5nfGVufDF8fHx8MTc2MDMyMzA5Mnww&ixlib=rb-4.1.0&q=80&w=1080&utm_source=figma&utm_medium=referral"
                alt="Team"
                className="w-full h-full object-cover"
              />
            </div>
          </div>
        </div>
      </section>

      {/* Stats Section */}
      <section className="py-16 bg-blue-600">
        <div className="max-w-7xl mx-auto px-6">
          <div className="grid grid-cols-1 md:grid-cols-4 gap-8">
            {stats.map((stat) => {
              const Icon = stat.icon;
              return (
                <div key={stat.label} className="text-center text-white">
                  <Icon className="w-12 h-12 mx-auto mb-4 opacity-80" />
                  <p className="mb-2">{stat.value}</p>
                  <p className="text-blue-100">{stat.label}</p>
                </div>
              );
            })}
          </div>
        </div>
      </section>

      {/* Mission & Vision */}
      <section className="py-20 bg-white">
        <div className="max-w-7xl mx-auto px-6">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-12">
            <Card className="p-8 bg-gradient-to-br from-blue-50 to-white border-blue-200">
              <Target className="w-12 h-12 text-blue-600 mb-4" />
              <h2 className="text-gray-900 mb-4">
                Mission
              </h2>
              <p className="text-gray-600 text-lg">
                Empower Vietnamese businesses with advanced data analytics technology, 
                helping them make smarter, faster, and more accurate decisions to develop 
                sustainably in the digital age.
              </p>
            </Card>

            <Card className="p-8 bg-gradient-to-br from-purple-50 to-white border-purple-200">
              <TrendingUp className="w-12 h-12 text-purple-600 mb-4" />
              <h2 className="text-gray-900 mb-4">
                Vision
              </h2>
              <p className="text-gray-600 text-lg">
                Become Vietnam's leading decision support platform, trusted by 
                thousands of businesses and contributing to comprehensive digital transformation for the economy.
              </p>
            </Card>
          </div>
        </div>
      </section>

      {/* Values Section */}
      <section className="py-20 bg-gray-50">
        <div className="max-w-7xl mx-auto px-6">
          <div className="text-center mb-16">
            <h2 className="text-gray-900 mb-4">
              Core Values
            </h2>
            <p className="text-gray-600 text-lg max-w-3xl mx-auto">
              The values that guide all our actions and decisions
            </p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-8">
            {values.map((value) => {
              const Icon = value.icon;
              return (
                <Card key={value.title} className="p-6 hover:shadow-lg transition-shadow">
                  <div className="bg-blue-100 w-16 h-16 rounded-xl flex items-center justify-center mb-4">
                    <Icon className="w-8 h-8 text-blue-600" />
                  </div>
                  <h3 className="text-gray-900 mb-3">
                    {value.title}
                  </h3>
                  <p className="text-gray-600">
                    {value.description}
                  </p>
                </Card>
              );
            })}
          </div>
        </div>
      </section>

      {/* Team Section */}
      <section className="py-20 bg-white">
        <div className="max-w-7xl mx-auto px-6">
          <div className="text-center mb-16">
            <h2 className="text-gray-900 mb-4">
              Leadership Team
            </h2>
            <p className="text-gray-600 text-lg max-w-3xl mx-auto">
              The people leading DSS Analytics towards the future
            </p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-8">
            {team.map((member) => (
              <Card key={member.name} className="p-6 text-center hover:shadow-lg transition-shadow">
                <div className="w-24 h-24 bg-gradient-to-br from-blue-500 to-blue-600 rounded-full mx-auto mb-4 flex items-center justify-center text-white text-2xl">
                  {member.name.charAt(0)}
                </div>
                <h3 className="text-gray-900 mb-2">
                  {member.name}
                </h3>
                <p className="text-blue-600 text-sm mb-3">
                  {member.position}
                </p>
                <p className="text-gray-600 text-sm">
                  {member.description}
                </p>
              </Card>
            ))}
          </div>
        </div>
      </section>


    </div>
  );
}
