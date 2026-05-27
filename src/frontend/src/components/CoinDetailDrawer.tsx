import { CoinChartWidget } from "@/components/CoinChartWidget";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import {
  formatCompactNumber,
  formatPercent,
  formatPrice,
  formatSupply,
} from "@/lib/format";
import type { Coin } from "@/types/coin";
import { StarIcon } from "lucide-react";

interface CoinDetailDrawerProps {
  coin: Coin | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  isFavorite: boolean;
  onToggleFavorite: (id: string) => void;
}

function StatRow({ label, value, trend }: { label: string; value: string; trend?: number }) {
  const trendColor =
    trend === undefined ? "text-foreground" : trend >= 0 ? "text-price-up" : "text-price-down";
  return (
    <div className="flex items-center justify-between py-2.5 border-b border-border/40 last:border-b-0">
      <span className="text-xs text-muted-foreground">{label}</span>
      <span className={`text-sm font-mono font-semibold tabular-nums ${trendColor}`}>{value}</span>
    </div>
  );
}

export function CoinDetailDrawer({
  coin,
  open,
  onOpenChange,
  isFavorite,
  onToggleFavorite,
}: CoinDetailDrawerProps) {
  if (!coin) return null;
  const positive24h = coin.priceChangePercentage24h >= 0;
  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent
        side="right"
        className="w-full sm:max-w-xl bg-card border-border/60 overflow-y-auto p-0"
        data-ocid="coinDetail.drawer"
      >
        <SheetHeader className="px-5 pt-5 pb-3 border-b border-border/40">
          <div className="flex items-center gap-3">
            <img
              src={coin.image}
              alt={coin.name}
              className="w-12 h-12 rounded-full shrink-0"
              onError={(e) => { (e.target as HTMLImageElement).style.visibility = "hidden" }}
            />
            <div className="min-w-0 flex-1">
              <div className="flex items-center gap-2">
                <SheetTitle className="text-lg font-display font-bold text-foreground truncate">
                  {coin.name}
                </SheetTitle>
                <span className="text-xs text-muted-foreground uppercase tracking-wider shrink-0">
                  {coin.symbol}
                </span>
                <span className="text-[10px] text-muted-foreground font-mono px-1.5 py-0.5 rounded bg-muted/40 shrink-0">
                  #{coin.marketCapRank}
                </span>
              </div>
              <SheetDescription className="text-xs flex items-baseline gap-2">
                <span className="font-mono text-foreground tabular-nums">
                  {formatPrice(coin.currentPrice)}
                </span>
                <span className={`font-semibold tabular-nums ${positive24h ? "text-price-up" : "text-price-down"}`}>
                  {formatPercent(coin.priceChangePercentage24h)} (24h)
                </span>
              </SheetDescription>
            </div>
            <button
              type="button"
              onClick={() => onToggleFavorite(coin.id)}
              className={`shrink-0 w-9 h-9 rounded-full flex items-center justify-center border transition-colors ${
                isFavorite
                  ? "bg-primary/15 border-primary/30 text-primary"
                  : "bg-card border-border/60 text-muted-foreground hover:text-foreground"
              }`}
              aria-label={isFavorite ? "Aus Watchlist entfernen" : "Zur Watchlist hinzufügen"}
              data-ocid="coinDetail.favorite_button"
            >
              <StarIcon className="w-4 h-4" fill={isFavorite ? "currentColor" : "none"} />
            </button>
          </div>
        </SheetHeader>

        <div className="px-5 py-4 space-y-5">
          {/* Interactive chart */}
          <CoinChartWidget coinId={coin.id} open={open} />

          {/* Performance grid */}
          <div>
            <p className="text-[10px] font-medium text-muted-foreground uppercase tracking-wider mb-2">
              Performance
            </p>
            <div className="grid grid-cols-3 gap-2">
              {[
                { label: "1h", value: coin.priceChangePercentage1h },
                { label: "24h", value: coin.priceChangePercentage24h },
                { label: "7d", value: coin.priceChangePercentage7d },
              ].map((p) => {
                const pos = p.value >= 0;
                return (
                  <div
                    key={p.label}
                    className="rounded-lg border border-border/50 bg-background/40 px-3 py-2.5 text-center"
                  >
                    <p className="text-[10px] text-muted-foreground uppercase tracking-wider">
                      {p.label}
                    </p>
                    <p className={`text-sm font-mono font-semibold tabular-nums mt-0.5 ${pos ? "text-price-up" : "text-price-down"}`}>
                      {formatPercent(p.value)}
                    </p>
                  </div>
                );
              })}
            </div>
          </div>

          {/* Market data */}
          <div>
            <p className="text-[10px] font-medium text-muted-foreground uppercase tracking-wider mb-1">
              Marktdaten
            </p>
            <div className="rounded-lg border border-border/50 bg-background/40 px-3">
              <StatRow label="Marktkapitalisierung" value={`${formatCompactNumber(coin.marketCap)} €`} />
              <StatRow label="24h Volumen" value={`${formatCompactNumber(coin.totalVolume)} €`} />
              <StatRow label="24h Hoch" value={formatPrice(coin.high24h)} />
              <StatRow label="24h Tief" value={formatPrice(coin.low24h)} />
              <StatRow label="All-Time-High" value={formatPrice(coin.ath)} trend={coin.athChangePercentage} />
              <StatRow label="Abstand zum ATH" value={formatPercent(coin.athChangePercentage)} trend={coin.athChangePercentage} />
              <StatRow label="Umlaufmenge" value={formatSupply(coin.circulatingSupply, coin.symbol)} />
              <StatRow label="Gesamtmenge" value={formatSupply(coin.totalSupply, coin.symbol)} />
            </div>
          </div>
        </div>
      </SheetContent>
    </Sheet>
  );
}
