import { Sparkline } from "@/components/Sparkline";
import type { PriceDirection } from "@/hooks/usePriceDirections";
import {
  formatCompactNumber,
  formatPercent,
  formatPrice,
} from "@/lib/format";
import type { Coin, Timeframe } from "@/types/coin";
import { StarIcon } from "lucide-react";
import { memo, useCallback } from "react";

interface CoinRowProps {
  coin: Coin;
  rank: number;
  timeframe: Timeframe;
  isFavorite: boolean;
  flash: PriceDirection;
  onToggleFavorite: (id: string) => void;
  onSelect: (coin: Coin) => void;
}

function pctFor(c: Coin, tf: Timeframe): number {
  if (tf === "1h") return c.priceChangePercentage1h;
  if (tf === "7d") return c.priceChangePercentage7d;
  return c.priceChangePercentage24h;
}

function CoinRowImpl({
  coin,
  rank,
  timeframe,
  isFavorite,
  flash,
  onToggleFavorite,
  onSelect,
}: CoinRowProps) {
  const pct = pctFor(coin, timeframe);
  const positive = pct >= 0;
  const sparkPositive = coin.priceChangePercentage7d >= 0;

  const handleClick = useCallback(() => {
    onSelect(coin);
  }, [coin, onSelect]);

  const handleFavClick = useCallback(
    (e: React.MouseEvent) => {
      e.stopPropagation();
      onToggleFavorite(coin.id);
    },
    [coin.id, onToggleFavorite],
  );

  const handleKey = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        onSelect(coin);
      }
    },
    [coin, onSelect],
  );

  const flashClass =
    flash === "up"
      ? "bg-price-up/[0.06]"
      : flash === "down"
        ? "bg-price-down/[0.06]"
        : "";

  return (
    // biome-ignore lint/a11y/useSemanticElements: row contains a nested favorite <button>; using a native <button> for the row would nest interactive elements. The div carries role+tabIndex+keydown so it is keyboard-accessible.
    <div
      role="button"
      tabIndex={0}
      onClick={handleClick}
      onKeyDown={handleKey}
      aria-label={`${coin.name} Details öffnen`}
      className={`grid grid-cols-[auto_auto_1fr_auto_auto] sm:grid-cols-[auto_auto_1fr_auto_auto_auto_auto] md:grid-cols-[auto_auto_1fr_auto_auto_auto_auto_auto] items-center gap-2 sm:gap-4 px-3 sm:px-4 py-3 sm:py-3.5 border-b border-border/40 hover:bg-card/60 focus:outline-none focus:ring-1 focus:ring-primary/40 focus-visible:ring-2 cursor-pointer transition-colors ${flashClass}`}
      data-ocid={`market.item.${coin.id}`}
    >
      {/* Favorite */}
      <button
        type="button"
        onClick={handleFavClick}
        className={`w-6 h-6 flex items-center justify-center rounded transition-colors ${
          isFavorite
            ? "text-primary"
            : "text-muted-foreground/40 hover:text-foreground"
        }`}
        aria-label={
          isFavorite ? "Aus Watchlist entfernen" : "Zur Watchlist hinzufügen"
        }
        data-ocid={`market.favorite.${coin.id}`}
      >
        <StarIcon className="w-3.5 h-3.5" fill={isFavorite ? "currentColor" : "none"} />
      </button>

      {/* Rank */}
      <span className="text-xs text-muted-foreground w-6 text-right tabular-nums shrink-0">
        {rank}
      </span>

      {/* Coin info */}
      <div className="flex items-center gap-2.5 min-w-0">
        <img
          src={coin.image}
          alt={coin.name}
          className="w-7 h-7 rounded-full shrink-0"
          loading="lazy"
          onError={(e) => {
            (e.target as HTMLImageElement).style.visibility = "hidden";
          }}
        />
        <div className="min-w-0">
          <p className="text-sm font-display font-semibold text-foreground truncate leading-tight">
            {coin.name}
          </p>
          <p className="text-[11px] text-muted-foreground uppercase tracking-wider">
            {coin.symbol}
          </p>
        </div>
      </div>

      {/* Sparkline — hidden on mobile */}
      <div className="hidden sm:block shrink-0">
        <Sparkline data={coin.sparkline7d} positive={sparkPositive} />
      </div>

      {/* Price */}
      <div className="text-right shrink-0">
        <p
          className="text-sm font-mono font-semibold text-foreground tabular-nums"
          data-ocid={`market.price.${coin.id}`}
        >
          {formatPrice(coin.currentPrice)}
        </p>
        <p className="text-[10px] text-muted-foreground sm:hidden">
          MK {formatCompactNumber(coin.marketCap)} €
        </p>
      </div>

      {/* Change */}
      <div
        className={`text-right shrink-0 px-2 py-0.5 rounded text-xs font-semibold tabular-nums ${
          positive
            ? "bg-price-up/10 text-price-up"
            : "bg-price-down/10 text-price-down"
        }`}
        data-ocid={`market.change.${coin.id}`}
      >
        {formatPercent(pct)}
      </div>

      {/* Volume — hidden on mobile and small screens */}
      <p className="hidden md:block text-right text-xs text-muted-foreground tabular-nums shrink-0 min-w-[88px]">
        {formatCompactNumber(coin.totalVolume)} €
      </p>

      {/* Market cap — hidden on mobile */}
      <p className="hidden sm:block text-right text-xs text-muted-foreground tabular-nums shrink-0 min-w-[88px]">
        {formatCompactNumber(coin.marketCap)} €
      </p>
    </div>
  );
}

export const CoinRow = memo(CoinRowImpl);
