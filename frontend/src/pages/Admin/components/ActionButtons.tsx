import { UserPlus, Calendar } from "lucide-react";
import { Button } from "../../../components/ui/figma/button";

export function ActionButtons() {
  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <div className="bg-gradient-to-r from-blue-600 to-purple-600 rounded-lg p-8 text-white">
        <h3 className="mb-2">Ready to take action?</h3>
        <p className="text-blue-100 mb-6">
          Quickly add new users or schedule crawler tasks
        </p>
        <div className="flex flex-wrap gap-4">
          <Button size="lg" variant="secondary" className="gap-2">
            <UserPlus className="w-5 h-5" />
            Add User
          </Button>
          <Button size="lg" variant="outline" className="gap-2 bg-transparent border-white text-white hover:bg-white hover:text-blue-600">
            <Calendar className="w-5 h-5" />
            Schedule Crawler
          </Button>
        </div>
      </div>
    </div>
  );
}
