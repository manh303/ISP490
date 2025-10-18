import { Header } from "./Header";
import { Hero } from "./Hero";
import { Features } from "./Features";
import { Footer } from "./Footer";
import type { Page } from "../App";

interface HomePageProps {
  navigateTo: (page: Page) => void;
  isLoggedIn: boolean;
  onLogout: () => void;
}

export function HomePage({ navigateTo, isLoggedIn, onLogout }: HomePageProps) {
  return (
    <div className="min-h-screen">
      <Header navigateTo={navigateTo} isLoggedIn={isLoggedIn} onLogout={onLogout} />
      <Hero navigateTo={navigateTo} />
      <Features navigateTo={navigateTo} />
      <Footer />
    </div>
  );
}
