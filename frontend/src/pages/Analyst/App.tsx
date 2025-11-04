import { AnalystHeader } from "./components/analyst/AnalystHeader";
import { AnalystHero } from "./components/analyst/AnalystHero";
import { KeyFeatures } from "./components/analyst/KeyFeatures";
import { VisualizationSection } from "./components/analyst/VisualizationSection";
import { FinalCTA } from "./components/analyst/FinalCTA";
import { AnalystFooter } from "./components/analyst/AnalystFooter";

export default function App() {
  return (
    <div className="min-h-screen bg-white">
      <AnalystHeader />
      <main>
        <AnalystHero />
        <KeyFeatures />
        <VisualizationSection />
        <FinalCTA />
      </main>
      <AnalystFooter />
    </div>
  );
}
