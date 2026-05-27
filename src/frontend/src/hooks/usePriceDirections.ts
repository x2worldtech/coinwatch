import type { Coin } from "@/types/coin";
import { useEffect, useRef, useState } from "react";

export type PriceDirection = "up" | "down" | null;

export type PriceDirectionMap = Record<string, PriceDirection>;

/**
 * Tracks coin prices across renders and exposes the direction of the latest change
 * per coin id. The direction resets to null after `holdMs` so the flash animation
 * only plays once per real update.
 */
export function usePriceDirections(coins: Coin[] | undefined, holdMs = 1200) {
  const prevPrices = useRef<Map<string, number>>(new Map());
  const [directions, setDirections] = useState<PriceDirectionMap>({});

  useEffect(() => {
    if (!coins || coins.length === 0) return;
    const updates: PriceDirectionMap = {};
    let changed = false;
    for (const c of coins) {
      const prev = prevPrices.current.get(c.id);
      if (prev !== undefined && prev !== c.currentPrice) {
        updates[c.id] = c.currentPrice > prev ? "up" : "down";
        changed = true;
      }
      prevPrices.current.set(c.id, c.currentPrice);
    }
    if (changed) {
      setDirections((prev) => ({ ...prev, ...updates }));
      const timer = window.setTimeout(() => {
        setDirections((prev) => {
          const next = { ...prev };
          for (const id of Object.keys(updates)) {
            next[id] = null;
          }
          return next;
        });
      }, holdMs);
      return () => window.clearTimeout(timer);
    }
  }, [coins, holdMs]);

  return directions;
}
