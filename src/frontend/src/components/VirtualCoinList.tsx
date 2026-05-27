import { CoinRow } from "@/components/CoinRow";
import type { PriceDirection } from "@/hooks/usePriceDirections";
import type { Coin, Timeframe } from "@/types/coin";
import { useVirtualizer } from "@tanstack/react-virtual";
import { useEffect, useRef } from "react";

interface VirtualCoinListProps {
  coins: Coin[];
  timeframe: Timeframe;
  watchlistHas: (id: string) => boolean;
  flashes: Record<string, PriceDirection>;
  onToggleFavorite: (id: string) => void;
  onSelect: (coin: Coin) => void;
  estimatedRowHeight?: number;
  onEndReached?: () => void;
}

/**
 * Virtualized list of coin rows. Renders only the rows visible in the viewport
 * (plus a small overscan) — necessary when displaying up to 1000 coins.
 *
 * Uses window-scrolling: the parent's scroll position drives the virtualizer,
 * so the table flows naturally inside the page without an inner scrollbar.
 */
export function VirtualCoinList({
  coins,
  timeframe,
  watchlistHas,
  flashes,
  onToggleFavorite,
  onSelect,
  estimatedRowHeight = 64,
  onEndReached,
}: VirtualCoinListProps) {
  const parentRef = useRef<HTMLDivElement | null>(null);

  const virtualizer = useVirtualizer({
    count: coins.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => estimatedRowHeight,
    overscan: 6,
  });

  // Trigger onEndReached when the last item is rendered (within 5 from the end).
  const items = virtualizer.getVirtualItems();
  const lastItem = items[items.length - 1];
  useEffect(() => {
    if (!onEndReached) return;
    if (!lastItem) return;
    if (lastItem.index >= coins.length - 5) {
      onEndReached();
    }
  }, [lastItem, coins.length, onEndReached]);

  return (
    <div
      ref={parentRef}
      className="overflow-auto max-h-[70vh]"
      data-ocid="market.virtual_list"
    >
      <div style={{ height: virtualizer.getTotalSize(), position: "relative", width: "100%" }}>
        {items.map((vi) => {
          const coin = coins[vi.index];
          return (
            <div
              key={coin.id}
              data-index={vi.index}
              ref={virtualizer.measureElement}
              style={{
                position: "absolute",
                top: 0,
                left: 0,
                right: 0,
                transform: `translateY(${vi.start}px)`,
              }}
            >
              <CoinRow
                coin={coin}
                rank={coin.marketCapRank}
                timeframe={timeframe}
                isFavorite={watchlistHas(coin.id)}
                flash={flashes[coin.id] ?? null}
                onToggleFavorite={onToggleFavorite}
                onSelect={onSelect}
              />
            </div>
          );
        })}
      </div>
    </div>
  );
}
