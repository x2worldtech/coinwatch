import OutCall "mo:caffeineai-http-outcalls/outcall";
import Types "../types/market";
import Common "../types/common";
import MarketLib "../lib/market";

import Time "mo:core/Time";
import Text "mo:core/Text";
import List "mo:core/List";
import Iter "mo:core/Iter";
import Nat "mo:core/Nat";

mixin (
  cache : MarketLib.CoinCache,
  globalCache : MarketLib.GlobalCache,
  chartCache : MarketLib.ChartCache,
) {
  func now_() : Int { Time.now() };

  // ---------- Char helpers ----------

  func charDigit(c : Char) : ?Nat {
    switch c {
      case '0' ?0; case '1' ?1; case '2' ?2; case '3' ?3;
      case '4' ?4; case '5' ?5; case '6' ?6; case '7' ?7;
      case '8' ?8; case '9' ?9; case _ null;
    };
  };

  func isWs(c : Char) : Bool {
    c == ' ' or c == '\n' or c == '\t' or c == '\r';
  };

  // ---------- Streaming float parser ----------

  func parseFloatFromIter(chars : Iter.Iter<Char>) : Float {
    var neg = false;
    var intPart : Float = 0.0;
    var fracPart : Float = 0.0;
    var fracDiv : Float = 1.0;
    var expPart : Int = 0;
    var expNeg = false;
    var phase = 0;
    var sawDigit = false;
    var started = false;
    label scan loop {
      switch (chars.next()) {
        case null { break scan };
        case (?c) {
          if (not started and isWs(c)) {} else {
            started := true;
            if (c == ',' or c == '}' or c == ']' or isWs(c)) { break scan };
            switch phase {
              case 0 {
                if (c == '-') { neg := true }
                else if (c == '+') {}
                else if (c == '.') { phase := 1 }
                else if (c == 'e' or c == 'E') { phase := 2 }
                else switch (charDigit c) {
                  case (?v) { intPart := intPart * 10.0 + v.toFloat(); sawDigit := true };
                  case null {};
                };
              };
              case 1 {
                if (c == 'e' or c == 'E') { phase := 2 }
                else switch (charDigit c) {
                  case (?v) {
                    fracDiv := fracDiv * 10.0;
                    fracPart := fracPart + v.toFloat() / fracDiv;
                    sawDigit := true;
                  };
                  case null {};
                };
              };
              case _ {
                if (c == '-') { expNeg := true }
                else if (c == '+') {}
                else switch (charDigit c) {
                  case (?v) { expPart := expPart * 10 + v };
                  case null {};
                };
              };
            };
          };
        };
      };
    };
    if (not sawDigit) { return 0.0 };
    var result = intPart + fracPart;
    if (expPart != 0) {
      var mult : Float = 1.0;
      var ei = expPart;
      while (ei > 0) { mult := mult * 10.0; ei -= 1 };
      if (expNeg) { result := result / mult } else { result := result * mult };
    };
    if (neg) { -result } else { result };
  };

  // ---------- Float parser with delimiter report ----------

  type ParsedFloat = { value : Float; closed : Bool };

  func parseFloatPrefixed(first : Char, chars : Iter.Iter<Char>) : ParsedFloat {
    var neg = false;
    var intPart : Float = 0.0;
    var fracPart : Float = 0.0;
    var fracDiv : Float = 1.0;
    var expPart : Int = 0;
    var expNeg = false;
    var phase = 0;
    var sawDigit = false;
    var closed = false;
    var current : ?Char = ?first;
    label scan loop {
      let c = switch current { case (?x) x; case null { break scan } };
      current := null;
      if (c == ',') { break scan };
      if (c == ']') { closed := true; break scan };
      if (c == '}') { break scan };
      if (isWs(c)) { break scan };
      switch phase {
        case 0 {
          if (c == '-') { neg := true }
          else if (c == '+') {}
          else if (c == '.') { phase := 1 }
          else if (c == 'e' or c == 'E') { phase := 2 }
          else switch (charDigit c) {
            case (?v) { intPart := intPart * 10.0 + v.toFloat(); sawDigit := true };
            case null {};
          };
        };
        case 1 {
          if (c == 'e' or c == 'E') { phase := 2 }
          else switch (charDigit c) {
            case (?v) {
              fracDiv := fracDiv * 10.0;
              fracPart := fracPart + v.toFloat() / fracDiv;
              sawDigit := true;
            };
            case null {};
          };
        };
        case _ {
          if (c == '-') { expNeg := true }
          else if (c == '+') {}
          else switch (charDigit c) {
            case (?v) { expPart := expPart * 10 + v };
            case null {};
          };
        };
      };
      current := chars.next();
    };
    if (not sawDigit) { return { value = 0.0; closed } };
    var result = intPart + fracPart;
    if (expPart != 0) {
      var mult : Float = 1.0;
      var ei = expPart;
      while (ei > 0) { mult := mult * 10.0; ei -= 1 };
      if (expNeg) { result := result / mult } else { result := result * mult };
    };
    { value = if (neg) -result else result; closed };
  };

  // ---------- Streaming string reader ----------

  func readJsonString(chars : Iter.Iter<Char>) : Text {
    let buf = List.empty<Char>();
    label scan loop {
      switch (chars.next()) {
        case null { break scan };
        case (?c) {
          if (c == '\"') { break scan };
          buf.add(c);
        };
      };
    };
    Text.fromIter(buf.values());
  };

  // ---------- Slice helpers ----------

  func sliceAfter(json : Text, needle : Text) : ?Text {
    let parts = json.split(#text needle);
    ignore parts.next();
    parts.next();
  };

  func extractStr(json : Text, key : Text) : Text {
    switch (sliceAfter(json, "\"" # key # "\":\"")) {
      case null { "" };
      case (?after) { readJsonString(after.toIter()) };
    };
  };

  func extractFloat(json : Text, key : Text) : Float {
    switch (sliceAfter(json, "\"" # key # "\":")) {
      case null { 0.0 };
      case (?after) { parseFloatFromIter(after.toIter()) };
    };
  };

  func extractFloatIn(json : Text, outerKey : Text, innerKey : Text) : Float {
    switch (sliceAfter(json, "\"" # outerKey # "\":")) {
      case null { 0.0 };
      case (?inner) { extractFloat(inner, innerKey) };
    };
  };

  func floatToNatSafe(f : Float) : Nat {
    if (f <= 0.0) return 0;
    let rounded = f + 0.5;
    var n : Nat = 0;
    var rem = rounded;
    while (rem >= 1_000_000.0) { n += 1_000_000; rem := rem - 1_000_000.0 };
    while (rem >= 1_000.0)     { n += 1_000;     rem := rem - 1_000.0 };
    while (rem >= 1.0)         { n += 1;         rem := rem - 1.0 };
    n;
  };

  // ---------- Sparkline parsing ----------

  func parseSparkline(coinJson : Text) : [Float] {
    let prices = List.empty<Float>();
    let after = switch (sliceAfter(coinJson, "\"sparkline_in_7d\":{\"price\":[")) {
      case null { return [] };
      case (?s) { s };
    };
    let chars = after.toIter();
    label scan loop {
      var first : ?Char = null;
      label peek loop {
        switch (chars.next()) {
          case null { break scan };
          case (?c) {
            if (c == ']') { break scan };
            if (c == ',' or isWs(c)) {} else { first := ?c; break peek };
          };
        };
      };
      switch first {
        case null { break scan };
        case (?c0) {
          let f = parseFloatPrefixed(c0, chars);
          prices.add(f.value);
          if (f.closed) { break scan };
        };
      };
    };
    prices.toArray();
  };

  // ---------- Coin parsing ----------

  func parseCoin(coinJson : Text) : ?Types.Coin {
    let id = extractStr(coinJson, "id");
    if (id == "") { return null };
    let symbol = extractStr(coinJson, "symbol");
    let name = extractStr(coinJson, "name");
    let image = extractStr(coinJson, "image");
    let currentPrice = extractFloat(coinJson, "current_price");
    let marketCap = extractFloat(coinJson, "market_cap");
    let marketCapRank = floatToNatSafe(extractFloat(coinJson, "market_cap_rank"));
    let priceChange24h = extractFloat(coinJson, "price_change_percentage_24h");
    let priceChange1h = extractFloat(coinJson, "price_change_percentage_1h_in_currency");
    let priceChange7d = extractFloat(coinJson, "price_change_percentage_7d_in_currency");
    let totalVolume = extractFloat(coinJson, "total_volume");
    let high24h = extractFloat(coinJson, "high_24h");
    let low24h = extractFloat(coinJson, "low_24h");
    let circulatingSupply = extractFloat(coinJson, "circulating_supply");
    let totalSupply = extractFloat(coinJson, "total_supply");
    let ath = extractFloat(coinJson, "ath");
    let athChange = extractFloat(coinJson, "ath_change_percentage");
    let sparkline = parseSparkline(coinJson);
    ?{
      id;
      symbol;
      name;
      image;
      currentPrice;
      marketCap;
      marketCapRank;
      priceChangePercentage1h = priceChange1h;
      priceChangePercentage24h = priceChange24h;
      priceChangePercentage7d = priceChange7d;
      totalVolume;
      high24h;
      low24h;
      circulatingSupply;
      totalSupply;
      ath;
      athChangePercentage = athChange;
      sparkline7d = sparkline;
    };
  };

  func parseCoins(body : Text) : [Types.Coin] {
    let coinsBuf = List.empty<Types.Coin>();
    let trimmed = switch (body.trim(#text " ").stripStart(#char '[')) {
      case (?t) t;
      case null body;
    };
    let chunks = trimmed.split(#text "},{");
    for (chunk in chunks) {
      switch (parseCoin(chunk)) {
        case (?coin) { coinsBuf.add(coin) };
        case null {};
      };
    };
    coinsBuf.toArray();
  };

  // ---------- Global stats parsing ----------

  func parseGlobalStats(body : Text) : Types.GlobalStats {
    let active = floatToNatSafe(extractFloat(body, "active_cryptocurrencies"));
    let mkts = floatToNatSafe(extractFloat(body, "markets"));
    let totalMc = extractFloatIn(body, "total_market_cap", "eur");
    let totalVol = extractFloatIn(body, "total_volume", "eur");
    let btcDom = extractFloatIn(body, "market_cap_percentage", "btc");
    let ethDom = extractFloatIn(body, "market_cap_percentage", "eth");
    let mcChange = extractFloat(body, "market_cap_change_percentage_24h_usd");
    {
      totalMarketCap = totalMc;
      totalVolume24h = totalVol;
      marketCapChangePercentage24h = mcChange;
      btcDominance = btcDom;
      ethDominance = ethDom;
      activeCryptocurrencies = active;
      markets = mkts;
    };
  };

  // ---------- Chart parsing ----------
  //
  // Line endpoint returns: { "prices": [[ts, p], ...], "market_caps":..., "total_volumes":... }
  // OHLC endpoint returns: [[ts, o, h, l, c], ...]
  // We stream-parse both into typed records.

  // Reads a fixed-length JSON number array starting AFTER the opening '['.
  // Returns up to `expectedLen` Floats then stops at ']'.
  func readNumberArray(chars : Iter.Iter<Char>, expectedLen : Nat) : [Float] {
    let buf = List.empty<Float>();
    var count = 0;
    label scan loop {
      if (count >= expectedLen) { break scan };
      var first : ?Char = null;
      label peek loop {
        switch (chars.next()) {
          case null { break scan };
          case (?c) {
            if (c == ']') { break scan };
            if (c == ',' or isWs(c) or c == '[') {} else { first := ?c; break peek };
          };
        };
      };
      switch first {
        case null { break scan };
        case (?c0) {
          let f = parseFloatPrefixed(c0, chars);
          buf.add(f.value);
          count += 1;
          if (f.closed) { break scan };
        };
      };
    };
    buf.toArray();
  };

  // Parse line chart: stream pairs [timestamp, price] until the outer ']' of "prices".
  func parseLinePoints(body : Text) : [Types.LinePoint] {
    let after = switch (sliceAfter(body, "\"prices\":[")) {
      case null { return [] };
      case (?s) { s };
    };
    let chars = after.toIter();
    let buf = List.empty<Types.LinePoint>();
    label scan loop {
      // Find '[' that starts a pair, or ']' that ends the outer array.
      var sawOpen = false;
      label findOpen loop {
        switch (chars.next()) {
          case null { break scan };
          case (?c) {
            if (c == ']') { break scan };
            if (c == '[') { sawOpen := true; break findOpen };
          };
        };
      };
      if (not sawOpen) { break scan };
      let pair = readNumberArray(chars, 2);
      if (pair.size() >= 2) {
        let ts = pair[0];
        let price = pair[1];
        // Float -> Int for timestamp via integer accumulation.
        let tsInt = floatToNatSafe(ts);
        buf.add({ timestamp = tsInt; price });
      };
    };
    buf.toArray();
  };

  // Parse OHLC chart: [[ts, o, h, l, c], ...]
  func parseCandles(body : Text) : [Types.Candle] {
    let chars = body.toIter();
    let buf = List.empty<Types.Candle>();
    // Drop leading '[' of outer array
    label preamble loop {
      switch (chars.next()) {
        case null { return buf.toArray() };
        case (?c) {
          if (c == '[') { break preamble };
        };
      };
    };
    label scan loop {
      var sawOpen = false;
      label findOpen loop {
        switch (chars.next()) {
          case null { break scan };
          case (?c) {
            if (c == ']') { break scan };
            if (c == '[') { sawOpen := true; break findOpen };
          };
        };
      };
      if (not sawOpen) { break scan };
      let tup = readNumberArray(chars, 5);
      if (tup.size() >= 5) {
        let tsInt = floatToNatSafe(tup[0]);
        buf.add({
          timestamp = tsInt;
          open = tup[1];
          high = tup[2];
          low = tup[3];
          close = tup[4];
        });
      };
    };
    buf.toArray();
  };

  // ---------- Page URL builder ----------

  func marketUrl(page : Nat, perPage : Nat) : Text {
    "https://api.coingecko.com/api/v3/coins/markets"
    # "?vs_currency=eur"
    # "&order=market_cap_desc"
    # "&per_page=" # Nat.toText(perPage)
    # "&page=" # Nat.toText(page)
    # "&sparkline=true"
    # "&price_change_percentage=1h,24h,7d";
  };

  // ---------- Public API ----------

  public query func transform(input : OutCall.TransformationInput) : async OutCall.TransformationOutput {
    OutCall.transform(input);
  };

  // Returns one page of coins. Pages are 1-indexed. `perPage` is capped at 250.
  // Cache TTL is 120 seconds; stale snapshots are returned if the outcall fails.
  public func getMarketDataPage(page : Nat, perPage : Nat) : async Common.ApiResult<Types.MarketResponse> {
    let safePage = if (page == 0) 1 else page;
    let safePerPage = if (perPage == 0 or perPage > 250) 100 else perPage;
    let ttl : Int = 120_000_000_000;
    let now = now_();
    if (MarketLib.isFresh(cache, safePage, ttl, now)) {
      return MarketLib.buildResponse(cache, safePage);
    };
    let url = marketUrl(safePage, safePerPage);
    try {
      let body = await OutCall.httpGetRequest(url, [], transform);
      let coins = parseCoins(body);
      if (coins.size() == 0) {
        switch (MarketLib.buildResponse(cache, safePage)) {
          case (#ok r) { return #ok r };
          case (#err _) { return #err ("CoinGecko returned no coins") };
        };
      };
      MarketLib.updateCache(cache, safePage, safePerPage, coins, now);
      MarketLib.buildResponse(cache, safePage);
    } catch (e) {
      switch (MarketLib.buildResponse(cache, safePage)) {
        case (#ok r) { #ok r };
        case (#err _) { #err ("HTTP outcall failed: " # e.message()) };
      };
    };
  };

  // Convenience wrapper: first page with default perPage (100).
  public func getMarketData() : async Common.ApiResult<Types.MarketResponse> {
    await getMarketDataPage(1, 100);
  };

  public func getGlobalStats() : async Common.ApiResult<Types.GlobalResponse> {
    let ttl : Int = 120_000_000_000;
    let now = now_();
    if (MarketLib.isGlobalFresh(globalCache, ttl, now)) {
      return MarketLib.buildGlobalResponse(globalCache);
    };
    let url = "https://api.coingecko.com/api/v3/global";
    try {
      let body = await OutCall.httpGetRequest(url, [], transform);
      let stats = parseGlobalStats(body);
      MarketLib.updateGlobalCache(globalCache, stats, now);
      MarketLib.buildGlobalResponse(globalCache);
    } catch (e) {
      if (globalCache.fetchedAt > 0) {
        return MarketLib.buildGlobalResponse(globalCache);
      };
      #err ("HTTP outcall failed: " # e.message());
    };
  };

  // Per-coin chart data, on demand. `days` is one of 1, 7, 30, 90, 365, 0 (max).
  // `kind` picks line (price points) or candle (OHLC). Cache TTL is 5 minutes.
  public func getCoinChart(coinId : Text, days : Nat, kind : Types.ChartKind) : async Common.ApiResult<Types.ChartData> {
    let safeCoin = if (coinId == "") "bitcoin" else coinId;
    let safeDays = if (days == 0) 365 else days;
    let key = MarketLib.chartKey(safeCoin, safeDays, kind);
    let ttl : Int = 300_000_000_000;
    let now = now_();
    if (MarketLib.isChartFresh(chartCache, key, ttl, now)) {
      return MarketLib.buildChartResponse(chartCache, key);
    };
    let daysParam = Nat.toText(safeDays);
    let url = switch kind {
      case (#line) {
        "https://api.coingecko.com/api/v3/coins/" # safeCoin
        # "/market_chart?vs_currency=eur&days=" # daysParam;
      };
      case (#candle) {
        "https://api.coingecko.com/api/v3/coins/" # safeCoin
        # "/ohlc?vs_currency=eur&days=" # daysParam;
      };
    };
    try {
      let body = await OutCall.httpGetRequest(url, [], transform);
      let data : Types.ChartData = switch kind {
        case (#line) {
          let pts = parseLinePoints(body);
          {
            coinId = safeCoin;
            days = safeDays;
            kind = #line;
            line = pts;
            candles = [];
            updatedAt = now;
          };
        };
        case (#candle) {
          let cs = parseCandles(body);
          {
            coinId = safeCoin;
            days = safeDays;
            kind = #candle;
            line = [];
            candles = cs;
            updatedAt = now;
          };
        };
      };
      MarketLib.updateChartCache(chartCache, key, data, now);
      MarketLib.buildChartResponse(chartCache, key);
    } catch (e) {
      switch (MarketLib.buildChartResponse(chartCache, key)) {
        case (#ok r) { #ok r };
        case (#err _) { #err ("HTTP outcall failed: " # e.message()) };
      };
    };
  };
};
