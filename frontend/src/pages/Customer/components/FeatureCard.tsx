import { Card } from '../../../components/ui/figma/card';
import { LucideIcon } from 'lucide-react';

interface FeatureCardProps {
  icon: LucideIcon;
  title: string;
  description: string;
  color: 'green' | 'blue' | 'purple';
}

export function FeatureCard({ icon: Icon, title, description, color }: FeatureCardProps) {
  const colorClasses = {
    green: 'bg-green-100 text-green-600',
    blue: 'bg-blue-100 text-blue-600',
    purple: 'bg-purple-100 text-purple-600',
  };

  return (
    <Card className="p-8 hover:shadow-lg transition-shadow border-2 border-gray-100 bg-white">
      <div className={`w-16 h-16 rounded-2xl ${colorClasses[color]} flex items-center justify-center mb-6`}>
        <Icon className="w-8 h-8" />
      </div>
      <h3 className="mb-4 text-gray-900">{title}</h3>
      <p className="text-gray-600 leading-relaxed">{description}</p>
    </Card>
  );
}
