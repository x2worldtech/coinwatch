import Types "../types/market";
import Common "../types/common";
import Map "mo:core/Map";
import Text "mo:core/Text";
import Nat "mo:core/Nat";

module {
  // ---------- Per-page coin cache ----------
  //
  // We now load the market in pages of up to 250 coins each. The user can lazy-
  // load further pages via "Mehr laden". To keep things simple the cache holds
  // a Map from page-number to its last successful snapshot. TTL is checked
  // separately per page.

  public type PageEntry = {
    var coins : [Types.Coin];
    var fetchedAt : Int;
    perPage : Nat;
  };

  public type CoinCache = {
    pages : Map.Map<Nat, PageEntry>;
  };

  public func newCache() : CoinCache {
    { pages = Map.empty<Nat, PageEntry>() };
  };

  func getPage(cache : CoinCache, page : Nat) : ?PageEntry {
    Map.get(cache.pages, Nat.compare, page);
  };

  public func isFresh(cache : CoinCache, page : Nat, ttlNanos : Int, now : Int) : Bool {
    switch (getPage(cache, page)) {
      case (?entry) entry.fetchedAt > 0 and (now - entry.fetchedAt) < ttlNanos;
      case null false;
    };
  };

  public func updateCache(
    cache : CoinCache,
    page : Nat,
    perPage : Nat,
    coins : [Types.Coin],
    now : Int,
  ) : () {
    switch (getPage(cache, page)) {
      case (?entry) {
        entry.coins := coins;
        entry.fetchedAt := now;
      };
      case null {
        Map.add(cache.pages, Nat.compare, page, {
          var coins = coins;
          var fetchedAt = now;
          perPage;
        });
      };
    };
  };

  public func buildResponse(
    cache : CoinCache,
    page : Nat,
  ) : Common.ApiResult<Types.MarketResponse> {
    switch (getPage(cache, page)) {
      case (?entry) #ok {
        coins = entry.coins;
        page;
        perPage = entry.perPage;
        updatedAt = entry.fetchedAt;
      };
      case null #err ("Page " # Nat.toText(page) # " not yet loaded");
    };
  };

  public func hasAnyPage(cache : CoinCache) : Bool {
    Map.size(cache.pages) > 0;
  };

  // ---------- Global stats cache ----------

  public type GlobalCache = {
    var stats : ?Types.GlobalStats;
    var fetchedAt : Int;
  };

  public func newGlobalCache() : GlobalCache {
    { var stats = null; var fetchedAt = 0 };
  };

  public func isGlobalFresh(cache : GlobalCache, ttlNanos : Int, now : Int) : Bool {
    cache.fetchedAt > 0 and (now - cache.fetchedAt) < ttlNanos;
  };

  public func updateGlobalCache(cache : GlobalCache, stats : Types.GlobalStats, now : Int) : () {
    cache.stats := ?stats;
    cache.fetchedAt := now;
  };

  public func buildGlobalResponse(cache : GlobalCache) : Common.ApiResult<Types.GlobalResponse> {
    switch (cache.stats) {
      case (?s) #ok { stats = s; updatedAt = cache.fetchedAt };
      case null #err ("Global stats not yet loaded");
    };
  };

  // ---------- Chart cache ----------
  //
  // Coin-level chart data fetched on demand. Key encodes coinId + days + kind.

  public type ChartEntry = {
    var data : Types.ChartData;
    var fetchedAt : Int;
  };

  public type ChartCache = {
    entries : Map.Map<Text, ChartEntry>;
  };

  public func newChartCache() : ChartCache {
    { entries = Map.empty<Text, ChartEntry>() };
  };

  public func chartKey(coinId : Text, days : Nat, kind : Types.ChartKind) : Text {
    let k = switch kind { case (#line) "L"; case (#candle) "C" };
    coinId # ":" # Nat.toText(days) # ":" # k;
  };

  public func getChart(cache : ChartCache, key : Text) : ?ChartEntry {
    Map.get(cache.entries, Text.compare, key);
  };

  public func isChartFresh(cache : ChartCache, key : Text, ttlNanos : Int, now : Int) : Bool {
    switch (getChart(cache, key)) {
      case (?entry) entry.fetchedAt > 0 and (now - entry.fetchedAt) < ttlNanos;
      case null false;
    };
  };

  public func updateChartCache(cache : ChartCache, key : Text, data : Types.ChartData, now : Int) : () {
    switch (getChart(cache, key)) {
      case (?entry) {
        entry.data := data;
        entry.fetchedAt := now;
      };
      case null {
        Map.add(cache.entries, Text.compare, key, {
          var data;
          var fetchedAt = now;
        });
      };
    };
  };

  public func buildChartResponse(cache : ChartCache, key : Text) : Common.ApiResult<Types.ChartData> {
    switch (getChart(cache, key)) {
      case (?entry) #ok (entry.data);
      case null #err ("Chart not yet loaded: " # key);
    };
  };
};
