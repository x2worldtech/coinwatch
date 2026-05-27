import { createActor, type ChartKind as BackendChartKind } from "@/backend";
import { ChartKindCandle, ChartKindLine, isLineKind } from "@/lib/chartKind";
import type { Candle, ChartKind, CoinChart, GlobalStats, LinePoint } from "@/types/coin";
import type { Coin } from "@/types/coin";
import { useActor } from "@caffeineai/core-infrastructure";
import { useInfiniteQuery, useQuery } from "@tanstack/react-query";

const COINGECKO_API = "https://api.coingecko.com/api/v3";

// ---------- CoinGecko fallback shapes ----------

interface CoinGeckoCoin {
  id: string;
  symbol: string;
  name: string;
  image: string;
  current_price: number;
  market_cap: number;
  market_cap_rank: number;
  price_change_percentage_24h: number;
  price_change_percentage_1h_in_currency?: number;
  price_change_percentage_7d_in_currency?: number;
  total_volume?: number;
  high_24h?: number;
  low_24h?: number;
  circulating_supply?: number;
  total_supply?: number;
  ath?: number;
  ath_change_percentage?: number;
  sparkline_in_7d?: { price: number[] };
}

interface CoinGeckoGlobalResponse {
  data: {
    active_cryptocurrencies: number;
    markets: number;
    total_market_cap: Record<string, number>;
    total_volume: Record<string, number>;
    market_cap_percentage: Record<string, number>;
    market_cap_change_percentage_24h_usd: number;
  };
}

function mapCoinGeckoCoin(c: CoinGeckoCoin): Coin {
  return {
    id: c.id,
    symbol: c.symbol.toUpperCase(),
    name: c.name,
    image: c.image,
    currentPrice: c.current_price,
    marketCap: c.market_cap,
    marketCapRank: c.market_cap_rank,
    priceChangePercentage1h: c.price_change_percentage_1h_in_currency ?? 0,
    priceChangePercentage24h: c.price_change_percentage_24h ?? 0,
    priceChangePercentage7d: c.price_change_percentage_7d_in_currency ?? 0,
    totalVolume: c.total_volume ?? 0,
    high24h: c.high_24h ?? 0,
    low24h: c.low_24h ?? 0,
    circulatingSupply: c.circulating_supply ?? 0,
    totalSupply: c.total_supply ?? 0,
    ath: c.ath ?? 0,
    athChangePercentage: c.ath_change_percentage ?? 0,
    sparkline7d: c.sparkline_in_7d?.price?.slice(-48) ?? [],
  };
}

export async function fetchCoinGeckoMarket(page = 1, perPage = 100): Promise<Coin[]> {
  const url = new URL(`${COINGECKO_API}/coins/markets`);
  url.searchParams.set("vs_currency", "eur");
  url.searchParams.set("order", "market_cap_desc");
  url.searchParams.set("per_page", String(perPage));
  url.searchParams.set("page", String(page));
  url.searchParams.set("sparkline", "true");
  url.searchParams.set("price_change_percentage", "1h,24h,7d");

  const res = await fetch(url.toString(), { headers: { Accept: "application/json" } });
  if (!res.ok) throw new Error(`CoinGecko error: ${res.status}`);
  const data: CoinGeckoCoin[] = await res.json();
  return data.map(mapCoinGeckoCoin);
}

export async function fetchCoinGeckoGlobal(): Promise<GlobalStats> {
  const res = await fetch(`${COINGECKO_API}/global`, { headers: { Accept: "application/json" } });
  if (!res.ok) throw new Error(`CoinGecko global error: ${res.status}`);
  const json: CoinGeckoGlobalResponse = await res.json();
  const d = json.data;
  return {
    totalMarketCap: d.total_market_cap.eur ?? 0,
    totalVolume24h: d.total_volume.eur ?? 0,
    marketCapChangePercentage24h: d.market_cap_change_percentage_24h_usd ?? 0,
    btcDominance: d.market_cap_percentage.btc ?? 0,
    ethDominance: d.market_cap_percentage.eth ?? 0,
    activeCryptocurrencies: d.active_cryptocurrencies ?? 0,
    markets: d.markets ?? 0,
  };
}

export async function fetchCoinGeckoChartLine(coinId: string, days: number): Promise<LinePoint[]> {
  const url = new URL(`${COINGECKO_API}/coins/${coinId}/market_chart`);
  url.searchParams.set("vs_currency", "eur");
  url.searchParams.set("days", days === 0 ? "max" : String(days));
  const res = await fetch(url.toString(), { headers: { Accept: "application/json" } });
  if (!res.ok) throw new Error(`CoinGecko chart error: ${res.status}`);
  const json: { prices: [number, number][] } = await res.json();
  return json.prices.map(([t, p]) => ({ timestamp: t, price: p }));
}

export async function fetchCoinGeckoChartCandles(coinId: string, days: number): Promise<Candle[]> {
  const url = new URL(`${COINGECKO_API}/coins/${coinId}/ohlc`);
  url.searchParams.set("vs_currency", "eur");
  url.searchParams.set("days", days === 0 ? "max" : String(days));
  const res = await fetch(url.toString(), { headers: { Accept: "application/json" } });
  if (!res.ok) throw new Error(`CoinGecko OHLC error: ${res.status}`);
  const json: [number, number, number, number, number][] = await res.json();
  return json.map(([t, o, h, l, c]) => ({ timestamp: t, open: o, high: h, low: l, close: c }));
}

// ---------- Hooks ----------

const PER_PAGE = 100;
export const MAX_PAGES = 10; // 10 × 100 = 1000 coins
export const TOTAL_COINS_TARGET = MAX_PAGES * PER_PAGE;

function mapBackendCoin(c: {
  id: string; symbol: string; name: string; image: string;
  currentPrice: number; marketCap: number; marketCapRank: bigint;
  priceChangePercentage1h: number; priceChangePercentage24h: number; priceChangePercentage7d: number;
  totalVolume: number; high24h: number; low24h: number;
  circulatingSupply: number; totalSupply: number; ath: number; athChangePercentage: number;
  sparkline7d: number[];
}): Coin {
  return {
    id: c.id,
    symbol: c.symbol.toUpperCase(),
    name: c.name,
    image: c.image,
    currentPrice: c.currentPrice,
    marketCap: c.marketCap,
    marketCapRank: Number(c.marketCapRank),
    priceChangePercentage1h: c.priceChangePercentage1h,
    priceChangePercentage24h: c.priceChangePercentage24h,
    priceChangePercentage7d: c.priceChangePercentage7d,
    totalVolume: c.totalVolume,
    high24h: c.high24h,
    low24h: c.low24h,
    circulatingSupply: c.circulatingSupply,
    totalSupply: c.totalSupply,
    ath: c.ath,
    athChangePercentage: c.athChangePercentage,
    sparkline7d: c.sparkline7d,
  };
}

/**
 * Infinite-query: pulls one page of 100 coins at a time, up to MAX_PAGES.
 * Use `fetchNextPage` to load more.
 */
export function useMarketDataInfinite() {
  const { actor, isFetching } = useActor(createActor);

  return useInfiniteQuery<{ coins: Coin[]; page: number }>({
    queryKey: ["marketData", "infinite"],
    initialPageParam: 1,
    getNextPageParam: (last, allPages) => {
      if (last.coins.length < PER_PAGE) return undefined;
      if (allPages.length >= MAX_PAGES) return undefined;
      return last.page + 1;
    },
    queryFn: async ({ pageParam }): Promise<{ coins: Coin[]; page: number }> => {
      const page = pageParam as number;
      if (actor && !isFetching) {
        try {
          const result = await actor.getMarketDataPage(BigInt(page), BigInt(PER_PAGE));
          if (result.__kind__ === "ok") {
            return { coins: result.ok.coins.map(mapBackendCoin), page };
          }
        } catch (_) {
          // fall through
        }
      }
      return { coins: await fetchCoinGeckoMarket(page, PER_PAGE), page };
    },
    staleTime: 60_000,
    refetchInterval: 60_000,
  });
}

export function useGlobalStats() {
  const { actor, isFetching } = useActor(createActor);

  return useQuery<GlobalStats>({
    queryKey: ["globalStats"],
    queryFn: async (): Promise<GlobalStats> => {
      if (actor && !isFetching) {
        try {
          const result = await actor.getGlobalStats();
          if (result.__kind__ === "ok") {
            const { stats } = result.ok;
            return {
              totalMarketCap: stats.totalMarketCap,
              totalVolume24h: stats.totalVolume24h,
              marketCapChangePercentage24h: stats.marketCapChangePercentage24h,
              btcDominance: stats.btcDominance,
              ethDominance: stats.ethDominance,
              activeCryptocurrencies: Number(stats.activeCryptocurrencies),
              markets: Number(stats.markets),
            };
          }
        } catch (_) {}
      }
      return fetchCoinGeckoGlobal();
    },
    staleTime: 60_000,
    refetchInterval: 60_000,
  });
}

export function useCoinChart(
  coinId: string | null,
  days: number,
  kind: ChartKind,
  enabled: boolean,
) {
  const { actor, isFetching } = useActor(createActor);

  return useQuery<CoinChart>({
    queryKey: ["coinChart", coinId, days, kind],
    enabled: enabled && !!coinId,
    queryFn: async (): Promise<CoinChart> => {
      if (!coinId) throw new Error("no coin");
      const backendKind: BackendChartKind = kind === "line" ? ChartKindLine : ChartKindCandle;
      if (actor && !isFetching) {
        try {
          const result = await actor.getCoinChart(coinId, BigInt(days), backendKind);
          if (result.__kind__ === "ok") {
            return {
              coinId: result.ok.coinId,
              days: Number(result.ok.days),
              kind: isLineKind(result.ok.kind) ? "line" : "candle",
              line: result.ok.line.map((p) => ({ timestamp: Number(p.timestamp), price: p.price })),
              candles: result.ok.candles.map((c) => ({
                timestamp: Number(c.timestamp),
                open: c.open, high: c.high, low: c.low, close: c.close,
              })),
              updatedAt: Number(result.ok.updatedAt),
            };
          }
        } catch (_) {}
      }
      if (kind === "line") {
        const line = await fetchCoinGeckoChartLine(coinId, days);
        return { coinId, days, kind: "line", line, candles: [], updatedAt: Date.now() };
      }
      const candles = await fetchCoinGeckoChartCandles(coinId, days);
      return { coinId, days, kind: "candle", line: [], candles, updatedAt: Date.now() };
    },
    staleTime: 5 * 60_000,
  });
}
