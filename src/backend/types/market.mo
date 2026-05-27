module {
  public type SparklineData = [Float];

  public type Coin = {
    id : Text;
    symbol : Text;
    name : Text;
    image : Text;
    currentPrice : Float;
    marketCap : Float;
    marketCapRank : Nat;
    priceChangePercentage1h : Float;
    priceChangePercentage24h : Float;
    priceChangePercentage7d : Float;
    totalVolume : Float;
    high24h : Float;
    low24h : Float;
    circulatingSupply : Float;
    totalSupply : Float;
    ath : Float;
    athChangePercentage : Float;
    sparkline7d : SparklineData;
  };

  public type GlobalStats = {
    totalMarketCap : Float;
    totalVolume24h : Float;
    marketCapChangePercentage24h : Float;
    btcDominance : Float;
    ethDominance : Float;
    activeCryptocurrencies : Nat;
    markets : Nat;
  };

  public type MarketResponse = {
    coins : [Coin];
    page : Nat;
    perPage : Nat;
    updatedAt : Int;
  };

  public type GlobalResponse = {
    stats : GlobalStats;
    updatedAt : Int;
  };

  // ---------- Chart data ----------
  // Used for the on-demand detail-drawer chart. Timeframe is expressed in days
  // and the chart kind picks between line (price points) and candle (OHLC).

  public type ChartKind = { #line; #candle };

  // One OHLC candle. `timestamp` is milliseconds since the unix epoch.
  public type Candle = {
    timestamp : Int;
    open : Float;
    high : Float;
    low : Float;
    close : Float;
  };

  // One line point. Same timestamp encoding as candles.
  public type LinePoint = {
    timestamp : Int;
    price : Float;
  };

  public type ChartData = {
    coinId : Text;
    days : Nat;
    kind : ChartKind;
    line : [LinePoint];
    candles : [Candle];
    updatedAt : Int;
  };
};
