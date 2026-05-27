import { Header } from "@/components/Header";
import type { ReactNode } from "react";

interface LayoutProps {
  children: ReactNode;
  lastUpdated: Date | null;
  isLive: boolean;
  isLoading: boolean;
}

export function Layout({
  children,
  lastUpdated,
  isLive,
  isLoading,
}: LayoutProps) {
  return (
    <div className="min-h-screen bg-background flex flex-col">
      <Header lastUpdated={lastUpdated} isLive={isLive} isLoading={isLoading} />
      <main className="flex-1 w-full max-w-screen-xl mx-auto px-4 sm:px-6 py-4 sm:py-6">
        {children}
      </main>
      <footer className="bg-card border-t border-border/60 py-4">
        <div className="max-w-screen-xl mx-auto px-4 sm:px-6 flex flex-col sm:flex-row items-center justify-between gap-2">
          <p className="text-xs text-muted-foreground">
            &copy; {new Date().getFullYear()}. Built with love using{" "}
            <a
              href={`https://caffeine.ai?utm_source=caffeine-footer&utm_medium=referral&utm_content=${encodeURIComponent(typeof window !== "undefined" ? window.location.hostname : "")}`}
              target="_blank"
              rel="noopener noreferrer"
              className="text-primary hover:underline transition-colors"
            >
              caffeine.ai
            </a>
          </p>
          <p className="text-xs text-muted-foreground">
            Preise via CoinGecko &bull; Kein Finanzberater
          </p>
        </div>
      </footer>
    </div>
  );
}
