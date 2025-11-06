import { Card } from "../../../components/ui/figma/card";
import { Badge } from "../../../components/ui/figma/badge";
import { TrendingUp, TrendingDown, Minus, ArrowRight } from "lucide-react";
import { insights } from "../data/mockData";

export function InsightsCards() {
  return (
    <section className="container mx-auto px-4 py-12">
      <div className="mb-8">
        <h2 className="mb-2">Business Insights</h2>
        <p className="text-muted-foreground">
          Latest analysis and strategic recommendations
        </p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        {insights.map((insight) => (
          <Card key={insight.id} className="p-6 hover:shadow-lg transition-shadow cursor-pointer group">
            <div className="flex items-start justify-between mb-4">
              <Badge variant="secondary">{insight.category}</Badge>
              <div className={`p-1 rounded ${
                insight.trend === "positive"
                  ? "bg-green-500/10 text-green-500"
                  : insight.trend === "negative"
                  ? "bg-red-500/10 text-red-500"
                  : "bg-gray-500/10 text-gray-500"
              }`}>
                {insight.trend === "positive" ? (
                  <TrendingUp className="w-4 h-4" />
                ) : insight.trend === "negative" ? (
                  <TrendingDown className="w-4 h-4" />
                ) : (
                  <Minus className="w-4 h-4" />
                )}
              </div>
            </div>

            <h3 className="mb-3">{insight.title}</h3>
            <p className="text-muted-foreground mb-4 text-sm">
              {insight.summary}
            </p>

            <div className="flex items-center justify-between">
              <span className="text-xs text-muted-foreground">{insight.date}</span>
              <ArrowRight className="w-4 h-4 text-muted-foreground group-hover:text-foreground group-hover:translate-x-1 transition-all" />
            </div>
          </Card>
        ))}
      </div>
    </section>
  );
}
