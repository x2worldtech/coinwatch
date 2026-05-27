export interface Coin {
  id: string;
  symbol: string;
  name: string;
  image: string;
  currentPrice: number;
  marketCap: number;
  marketCapRank: number;
  priceChangePercentage1h: number;
  priceChangePercentage24h: number;
  priceChangePercentage7d: number;
  totalVolume: number;
  high24h: number;
  low24h: number;
  circulatingSupply: number;
  totalSupply: number;
  ath: number;
  athChangePercentage: number;
  sparkline7d: number[];
}

export interface GlobalStats {
  totalMarketCap: number;
  totalVolume24h: number;
  marketCapChangePercentage24h: number;
  btcDominance: number;
  ethDominance: number;
  activeCryptocurrencies: number;
  markets: number;
}

export interface MarketData {
  coins: Coin[];
  page: number;
  perPage: number;
  updatedAt: number;
}

export interface GlobalData { stats: GlobalStats; updatedAt: number }

export type Timeframe = "1h" | "24h" | "7d";

export type ChartKind = "line" | "candle";
export type ChartTimeframe = "1h" | "24h" | "7d" | "30d" | "90d" | "1y" | "all";

export interface LinePoint { timestamp: number; price: number }
export interface Candle {
  timestamp: number;
  open: number;
  high: number;
  low: number;
  close: number;
}

export interface CoinChart {
  coinId: string;
  days: number;
  kind: ChartKind;
  line: LinePoint[];
  candles: Candle[];
  updatedAt: number;
}

export const CHART_TIMEFRAMES: ChartTimeframe[] = ["1h", "24h", "7d", "30d", "90d", "1y", "all"];

export function timeframeToDays(tf: ChartTimeframe): number {
  switch (tf) {
    case "1h": return 1;
    case "24h": return 1;
    case "7d": return 7;
    case "30d": return 30;
    case "90d": return 90;
    case "1y": return 365;
    case "all": return 0;
  }
}

export function timeframeLabel(tf: ChartTimeframe): string {
  switch (tf) {
    case "1h": return "1 Std";
    case "24h": return "24 Std";
    case "7d": return "7 T";
    case "30d": return "30 T";
    case "90d": return "90 T";
    case "1y": return "1 J";
    case "all": return "Alle";
  }
}
