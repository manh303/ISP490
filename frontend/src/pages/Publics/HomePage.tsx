import { Hero } from "./Hero";
import { Features } from "./Features";
import type { Page } from "../../App";

interface HomePageProps {
  navigateTo: (page: Page) => void;
  isLoggedIn: boolean;
  onLogout: () => void;
}

export function HomePage({ navigateTo, isLoggedIn, onLogout }: HomePageProps) {
  return (
    <div className="min-h-screen">
      <Hero navigateTo={navigateTo} />
      <Features navigateTo={navigateTo} />
    </div>
  );
}
