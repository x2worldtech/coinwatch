import type { backendInterface, ChartKind } from "../backend";

function makeCoin(
  rank: number,
  id: string,
  name: string,
  symbol: string,
  image: string,
  price: number,
  marketCap: number,
  ch24: number,
  ch1h: number,
  ch7d: number,
  vol: number,
  sparkline: number[],
) {
  return {
    id, name, symbol, image,
    currentPrice: price,
    marketCap,
    marketCapRank: BigInt(rank),
    priceChangePercentage1h: ch1h,
    priceChangePercentage24h: ch24,
    priceChangePercentage7d: ch7d,
    totalVolume: vol,
    high24h: price * 1.04,
    low24h: price * 0.96,
    circulatingSupply: marketCap / price,
    totalSupply: (marketCap / price) * 1.1,
    ath: price * 1.5,
    athChangePercentage: -25,
    sparkline7d: sparkline,
  };
}

const COINS = [
  makeCoin(1, "bitcoin", "Bitcoin", "BTC", "https://assets.coingecko.com/coins/images/1/large/bitcoin.png", 67234.58, 1_320_000_000_000, 2.34, 0.5, 5.1, 42_000_000_000, [60000, 61200, 62500, 63100, 64800, 65200, 67234.58]),
  makeCoin(2, "ethereum", "Ethereum", "ETH", "https://assets.coingecko.com/coins/images/279/large/ethereum.png", 3512.45, 421_000_000_000, -1.12, -0.3, 2.4, 18_500_000_000, [3700, 3650, 3600, 3550, 3480, 3520, 3512.45]),
  makeCoin(3, "binancecoin", "BNB", "BNB", "https://assets.coingecko.com/coins/images/825/large/bnb-icon2_2x.png", 608.22, 89_000_000_000, 0.87, 0.1, 1.5, 1_500_000_000, [590, 595, 600, 598, 605, 607, 608.22]),
  makeCoin(4, "solana", "Solana", "SOL", "https://assets.coingecko.com/coins/images/4128/large/solana.png", 174.83, 81_000_000_000, 5.21, 1.2, 12.8, 3_200_000_000, [155, 158, 160, 165, 168, 171, 174.83]),
  makeCoin(5, "ripple", "XRP", "XRP", "https://assets.coingecko.com/coins/images/44/large/xrp-symbol-white-128.png", 0.6123, 33_000_000_000, -0.43, -0.1, -2.1, 900_000_000, [0.63, 0.625, 0.62, 0.615, 0.61, 0.614, 0.6123]),
  makeCoin(44, "internet-computer", "Internet Computer", "ICP", "https://assets.coingecko.com/coins/images/14495/large/Internet_Computer_logo.png", 6.85, 3_100_000_000, -3.21, -0.8, -7.4, 110_000_000, [7.5, 7.3, 7.1, 6.95, 6.9, 6.88, 6.85]),
];

function makeFakeLine(coinId: string, days: number) {
  const points = Math.min(days * 24, 168);
  const out: Array<{ timestamp: bigint; price: number }> = [];
  const base = COINS.find((c) => c.id === coinId)?.currentPrice ?? 1000;
  const now = Date.now();
  for (let i = points; i > 0; i--) {
    const t = now - i * 60 * 60 * 1000;
    const wobble = Math.sin(i / 5) * 0.02 + (Math.random() - 0.5) * 0.01;
    out.push({ timestamp: BigInt(t), price: base * (1 + wobble) });
  }
  return out;
}

function makeFakeCandles(coinId: string, days: number) {
  const points = Math.min(days * 4, 200);
  const out: Array<{ timestamp: bigint; open: number; high: number; low: number; close: number }> = [];
  const base = COINS.find((c) => c.id === coinId)?.currentPrice ?? 1000;
  const now = Date.now();
  let prev = base;
  for (let i = points; i > 0; i--) {
    const t = now - i * 4 * 60 * 60 * 1000;
    const change = (Math.random() - 0.5) * 0.03;
    const close = prev * (1 + change);
    const high = Math.max(prev, close) * (1 + Math.random() * 0.01);
    const low = Math.min(prev, close) * (1 - Math.random() * 0.01);
    out.push({ timestamp: BigInt(t), open: prev, high, low, close });
    prev = close;
  }
  return out;
}

export const mockBackend: backendInterface = {
  getMarketData: async () => ({
    __kind__: "ok" as const,
    ok: {
      updatedAt: BigInt(Date.now()),
      page: 1n,
      perPage: 100n,
      coins: COINS,
    },
  }),
  getMarketDataPage: async (page, perPage) => ({
    __kind__: "ok" as const,
    ok: { updatedAt: BigInt(Date.now()), page, perPage, coins: COINS },
  }),
  getGlobalStats: async () => ({
    __kind__: "ok" as const,
    ok: {
      updatedAt: BigInt(Date.now()),
      stats: {
        totalMarketCap: 2_400_000_000_000,
        totalVolume24h: 95_000_000_000,
        marketCapChangePercentage24h: 1.85,
        btcDominance: 54.2,
        ethDominance: 17.5,
        activeCryptocurrencies: BigInt(15234),
        markets: BigInt(1100),
      },
    },
  }),
  getCoinChart: async (coinId, days, kind: ChartKind) => {
    const isLine = (kind as unknown as { line?: null }).line === null;
    return {
      __kind__: "ok" as const,
      ok: {
        coinId,
        days,
        kind,
        line: isLine ? makeFakeLine(coinId, Number(days)) : [],
        candles: !isLine ? makeFakeCandles(coinId, Number(days)) : [],
        updatedAt: BigInt(Date.now()),
      },
    };
  },
  transform: async (input) => ({
    status: BigInt(200),
    body: input.response.body,
    headers: input.response.headers,
  }),
};
