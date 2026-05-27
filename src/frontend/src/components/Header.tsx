import { Badge } from "@/components/ui/badge";
import { ActivityIcon } from "lucide-react";

interface HeaderProps {
  lastUpdated: Date | null;
  isLive: boolean;
  isLoading: boolean;
}

export function Header({ lastUpdated, isLive, isLoading }: HeaderProps) {
  const timeStr = lastUpdated
    ? lastUpdated.toLocaleTimeString("de-DE", {
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
      })
    : null;

  return (
    <header className="bg-card border-b border-border/60 sticky top-0 z-40 backdrop-blur-sm supports-[backdrop-filter]:bg-card/80">
      <div className="max-w-screen-xl mx-auto px-4 sm:px-6 h-14 flex items-center justify-between gap-3">
        {/* Branding */}
        <div className="flex items-center gap-2.5 min-w-0">
          <div className="w-8 h-8 rounded-lg bg-primary/15 border border-primary/30 flex items-center justify-center shrink-0">
            <span className="text-[13px] font-display font-bold text-primary">
              ₿
            </span>
          </div>
          <div className="min-w-0">
            <span className="font-display font-bold text-[17px] tracking-tight text-foreground truncate block">
              CryptoMarket
            </span>
            <span className="text-[10px] text-muted-foreground uppercase tracking-wider hidden sm:block">
              Live · On-Chain · ICP
            </span>
          </div>
        </div>

        {/* Live indicator + time */}
        <div className="flex items-center gap-2 shrink-0">
          {isLoading ? (
            <Badge
              variant="outline"
              className="gap-1.5 text-[11px] border-muted-foreground/30 text-muted-foreground"
              data-ocid="header.loading_state"
            >
              <span className="w-1.5 h-1.5 rounded-full bg-muted-foreground animate-pulse" />
              Lädt...
            </Badge>
          ) : isLive ? (
            <Badge
              variant="outline"
              className="gap-1.5 text-[11px] border-price-up/40 text-price-up"
              data-ocid="header.live_badge"
            >
              <ActivityIcon className="w-3 h-3" />
              LIVE
            </Badge>
          ) : null}
          {timeStr && !isLoading && (
            <span
              className="text-[11px] text-muted-foreground font-mono hidden sm:block"
              data-ocid="header.last_updated"
            >
              {timeStr}
            </span>
          )}
        </div>
      </div>
    </header>
  );
}
