import MarketLib "lib/market";
import MarketApi "mixins/market-api";

// All caches are transient — they hold memoized HTTP responses and don't need
// to survive upgrades. No stable fields means no migration is required: a
// fresh deploy and any upgrade from a previously cleared state are both
// implicit-compatible (adding/removing actor fields is implicit migration).
actor {
  transient let cache = MarketLib.newCache();
  transient let globalCache = MarketLib.newGlobalCache();
  transient let chartCache = MarketLib.newChartCache();
  include MarketApi(cache, globalCache, chartCache);
};
