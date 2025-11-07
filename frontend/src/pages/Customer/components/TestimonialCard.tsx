import { Card } from '../../../components/ui/figma/card';
import { Star } from 'lucide-react';

interface TestimonialCardProps {
  name: string;
  role: string;
  content: string;
  image: string;
  rating: number;
}

export function TestimonialCard({ name, role, content, image, rating }: TestimonialCardProps) {
  return (
    <Card className="p-8 bg-white border-2 border-gray-100">
      <div className="flex gap-1 mb-4">
        {Array.from({ length: rating }).map((_, i) => (
          <Star key={i} className="w-5 h-5 fill-yellow-400 text-yellow-400" />
        ))}
      </div>
      <p className="text-gray-700 mb-6 leading-relaxed">"{content}"</p>
      <div className="flex items-center gap-4">
        <img
          src={image}
          alt={name}
          className="w-12 h-12 rounded-full object-cover"
        />
        <div>
          <p className="text-gray-900">{name}</p>
          <p className="text-gray-500">{role}</p>
        </div>
      </div>
    </Card>
  );
}
