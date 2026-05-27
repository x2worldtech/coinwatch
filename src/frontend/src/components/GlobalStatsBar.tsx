import { Skeleton } from "@/components/ui/skeleton";
import { formatCompactNumber, formatPercentPlain } from "@/lib/format";
import type { GlobalStats } from "@/types/coin";

interface GlobalStatsBarProps {
  data: GlobalStats | undefined;
  isLoading: boolean;
  coinCount: number;
}

interface StatItemProps {
  label: string;
  value: string;
  trend?: number;
  testId?: string;
}

function StatItem({ label, value, trend, testId }: StatItemProps) {
  const trendColor =
    trend === undefined
      ? "text-muted-foreground"
      : trend >= 0
        ? "text-price-up"
        : "text-price-down";
  return (
    <div
      className="flex flex-col gap-0.5 px-3 sm:px-4 py-2.5 border-r border-border/40 last:border-r-0 min-w-[120px]"
      data-ocid={testId}
    >
      <span className="text-[10px] font-medium text-muted-foreground uppercase tracking-wider">
        {label}
      </span>
      <div className="flex items-baseline gap-1.5">
        <span className="text-sm font-mono font-semibold text-foreground tabular-nums">
          {value}
        </span>
        {trend !== undefined && (
          <span className={`text-[11px] font-semibold ${trendColor}`}>
            {trend >= 0 ? "▲" : "▼"}
            {formatPercentPlain(Math.abs(trend))}
          </span>
        )}
      </div>
    </div>
  );
}

export function GlobalStatsBar({
  data,
  isLoading,
  coinCount,
}: GlobalStatsBarProps) {
  if (isLoading) {
    return (
      <div
        className="rounded-xl border border-border/60 bg-card overflow-hidden mb-4 sm:mb-6"
        data-ocid="globalStats.loading"
      >
        <div className="flex flex-wrap">
          {["a", "b", "c", "d", "e"].map((k) => (
            <div
              key={k}
              className="flex flex-col gap-1 px-3 sm:px-4 py-2.5 border-r border-border/40 last:border-r-0 min-w-[120px]"
            >
              <Skeleton className="h-3 w-16 rounded" />
              <Skeleton className="h-4 w-24 rounded" />
            </div>
          ))}
        </div>
      </div>
    );
  }

  if (!data) {
    return null;
  }

  return (
    <div
      className="rounded-xl border border-border/60 bg-card overflow-hidden mb-4 sm:mb-6"
      data-ocid="globalStats.container"
    >
      <div className="flex flex-wrap">
        <StatItem
          label="Marktkapital."
          value={`${formatCompactNumber(data.totalMarketCap)} €`}
          trend={data.marketCapChangePercentage24h}
          testId="globalStats.marketCap"
        />
        <StatItem
          label="24h Volumen"
          value={`${formatCompactNumber(data.totalVolume24h)} €`}
          testId="globalStats.volume"
        />
        <StatItem
          label="BTC Dominanz"
          value={formatPercentPlain(data.btcDominance)}
          testId="globalStats.btcDominance"
        />
        <StatItem
          label="ETH Dominanz"
          value={formatPercentPlain(data.ethDominance)}
          testId="globalStats.ethDominance"
        />
        <StatItem
          label="Coins"
          value={`${data.activeCryptocurrencies > 0 ? data.activeCryptocurrencies.toLocaleString("de-DE") : coinCount}`}
          testId="globalStats.activeCoins"
        />
      </div>
    </div>
  );
}
