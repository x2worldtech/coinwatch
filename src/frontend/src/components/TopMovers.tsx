import { Skeleton } from "@/components/ui/skeleton";
import { formatPercent, formatPrice } from "@/lib/format";
import type { Coin, Timeframe } from "@/types/coin";
import { TrendingDownIcon, TrendingUpIcon } from "lucide-react";
import { memo, useMemo } from "react";

function pctFor(c: Coin, tf: Timeframe): number {
  if (tf === "1h") return c.priceChangePercentage1h;
  if (tf === "7d") return c.priceChangePercentage7d;
  return c.priceChangePercentage24h;
}

interface MoverCardProps {
  coin: Coin;
  pct: number;
  onSelect?: (coin: Coin) => void;
}

const MoverCard = memo(function MoverCard({
  coin,
  pct,
  onSelect,
}: MoverCardProps) {
  const positive = pct >= 0;
  return (
    <button
      type="button"
      onClick={() => onSelect?.(coin)}
      className="flex items-center gap-2.5 px-3 py-2.5 rounded-lg border border-border/50 bg-card/60 hover:bg-card hover:border-border transition-colors text-left min-w-[180px] shrink-0 cursor-pointer"
      data-ocid={`topMovers.item.${coin.id}`}
    >
      <img
        src={coin.image}
        alt={coin.name}
        className="w-7 h-7 rounded-full shrink-0"
        loading="lazy"
        onError={(e) => {
          (e.target as HTMLImageElement).style.visibility = "hidden";
        }}
      />
      <div className="min-w-0 flex-1">
        <p className="text-xs font-display font-semibold text-foreground truncate leading-tight">
          {coin.symbol}
        </p>
        <p className="text-[10px] text-muted-foreground font-mono tabular-nums">
          {formatPrice(coin.currentPrice)}
        </p>
      </div>
      <span
        className={`text-xs font-semibold tabular-nums shrink-0 ${
          positive ? "text-price-up" : "text-price-down"
        }`}
      >
        {formatPercent(pct)}
      </span>
    </button>
  );
});

interface TopMoversProps {
  coins: Coin[] | undefined;
  isLoading: boolean;
  timeframe: Timeframe;
  onSelect?: (coin: Coin) => void;
}

function Row({ label, icon, items, color, onSelect, isLoading }: {
  label: string;
  icon: React.ReactNode;
  items: { coin: Coin; pct: number }[];
  color: string;
  onSelect?: (coin: Coin) => void;
  isLoading: boolean;
}) {
  return (
    <div className="flex flex-col gap-2">
      <div className="flex items-center gap-1.5 px-1">
        <span className={color}>{icon}</span>
        <h3 className="text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">
          {label}
        </h3>
      </div>
      <div className="flex gap-2 overflow-x-auto pb-1 -mx-1 px-1 scrollbar-thin snap-x">
        {isLoading
          ? ["a", "b", "c", "d", "e"].map((k) => (
              <Skeleton
                key={k}
                className="h-[52px] w-[180px] rounded-lg shrink-0"
              />
            ))
          : items.map(({ coin, pct }) => (
              <div key={coin.id} className="snap-start">
                <MoverCard coin={coin} pct={pct} onSelect={onSelect} />
              </div>
            ))}
      </div>
    </div>
  );
}

export function TopMovers({
  coins,
  isLoading,
  timeframe,
  onSelect,
}: TopMoversProps) {
  const { gainers, losers } = useMemo(() => {
    if (!coins || coins.length === 0)
      return { gainers: [], losers: [] };
    const withPct = coins.map((c) => ({ coin: c, pct: pctFor(c, timeframe) }));
    const valid = withPct.filter((x) => Number.isFinite(x.pct) && x.pct !== 0);
    const sorted = [...valid].sort((a, b) => b.pct - a.pct);
    return {
      gainers: sorted.slice(0, 5),
      losers: sorted.slice(-5).reverse(),
    };
  }, [coins, timeframe]);

  return (
    <div
      className="grid grid-cols-1 lg:grid-cols-2 gap-4 mb-4 sm:mb-6"
      data-ocid="topMovers.container"
    >
      <Row
        label={`Top Gewinner (${timeframe})`}
        icon={<TrendingUpIcon className="w-3.5 h-3.5" />}
        items={gainers}
        color="text-price-up"
        onSelect={onSelect}
        isLoading={isLoading}
      />
      <Row
        label={`Top Verlierer (${timeframe})`}
        icon={<TrendingDownIcon className="w-3.5 h-3.5" />}
        items={losers}
        color="text-price-down"
        onSelect={onSelect}
        isLoading={isLoading}
      />
    </div>
  );
}
