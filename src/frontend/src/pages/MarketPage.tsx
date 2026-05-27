import { CoinDetailDrawer } from "@/components/CoinDetailDrawer";
import { GlobalStatsBar } from "@/components/GlobalStatsBar";
import { Layout } from "@/components/Layout";
import { TopMovers } from "@/components/TopMovers";
import { VirtualCoinList } from "@/components/VirtualCoinList";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import { usePriceDirections } from "@/hooks/usePriceDirections";
import { useWatchlist } from "@/hooks/useWatchlist";
import { MAX_PAGES, TOTAL_COINS_TARGET, useGlobalStats, useMarketDataInfinite } from "@/lib/api";
import type { Coin, Timeframe } from "@/types/coin";
import {
  ArrowUpDownIcon,
  RefreshCwIcon,
  SearchIcon,
  StarIcon,
  XIcon,
} from "lucide-react";
import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";

type SortKey = "rank" | "price" | "change" | "marketCap" | "volume";
type SortDir = "asc" | "desc";
type ViewTab = "all" | "watchlist" | "gainers" | "losers";

function pctFor(c: Coin, tf: Timeframe): number {
  if (tf === "1h") return c.priceChangePercentage1h;
  if (tf === "7d") return c.priceChangePercentage7d;
  return c.priceChangePercentage24h;
}

interface ColProps {
  k: SortKey;
  label: string;
  className?: string;
  sortKey: SortKey;
  sortDir: SortDir;
  onSort: (k: SortKey) => void;
}

function Col({ k, label, className, sortKey, sortDir, onSort }: ColProps) {
  const active = sortKey === k;
  return (
    <button
      type="button"
      onClick={() => onSort(k)}
      className={`flex items-center gap-1 text-[11px] font-medium uppercase tracking-wider transition-colors ${
        active ? "text-foreground" : "text-muted-foreground hover:text-foreground"
      } ${className ?? ""}`}
      data-ocid={`market.sort_${k}`}
    >
      {label}
      <ArrowUpDownIcon
        className={`w-3 h-3 transition-opacity ${active ? "opacity-100" : "opacity-40"} ${
          active && sortDir === "asc" ? "rotate-180" : ""
        }`}
      />
    </button>
  );
}

function TableHeader(props: {
  sortKey: SortKey;
  sortDir: SortDir;
  timeframe: Timeframe;
  onSort: (k: SortKey) => void;
}) {
  return (
    <div className="grid grid-cols-[auto_auto_1fr_auto_auto] sm:grid-cols-[auto_auto_1fr_auto_auto_auto_auto] md:grid-cols-[auto_auto_1fr_auto_auto_auto_auto_auto] items-center gap-2 sm:gap-4 px-3 sm:px-4 py-2.5 border-b border-border/60 bg-muted/20 sticky top-14 z-30 backdrop-blur-sm">
      <span className="w-6" />
      <Col k="rank" label="#" className="w-6 justify-end" {...props} />
      <span className="text-[11px] text-muted-foreground uppercase tracking-wider">Krypto</span>
      <span className="hidden sm:block" />
      <Col k="price" label="Preis" className="justify-end" {...props} />
      <Col k="change" label={`${props.timeframe} %`} className="justify-end min-w-[60px]" {...props} />
      <Col k="volume" label="Volumen" className="hidden md:flex justify-end min-w-[80px]" {...props} />
      <Col k="marketCap" label="Marktkapital." className="hidden sm:flex justify-end min-w-[88px]" {...props} />
    </div>
  );
}

const TIMEFRAMES: Timeframe[] = ["1h", "24h", "7d"];

export default function MarketPage() {
  const {
    data: pages,
    isLoading,
    isError,
    dataUpdatedAt,
    refetch,
    isFetching,
    fetchNextPage,
    hasNextPage,
    isFetchingNextPage,
  } = useMarketDataInfinite();

  // Flatten the paginated data, deduplicating by id (in case a coin appears twice
  // across consecutive snapshots).
  const coins = useMemo<Coin[]>(() => {
    if (!pages) return [];
    const seen = new Set<string>();
    const out: Coin[] = [];
    for (const page of pages.pages) {
      for (const c of page.coins) {
        if (seen.has(c.id)) continue;
        seen.add(c.id);
        out.push(c);
      }
    }
    return out;
  }, [pages]);

  const { data: globalStats, isLoading: globalLoading } = useGlobalStats();
  const watchlist = useWatchlist();
  const flashes = usePriceDirections(coins);

  const [search, setSearch] = useState("");
  const [sortKey, setSortKey] = useState<SortKey>("marketCap");
  const [sortDir, setSortDir] = useState<SortDir>("desc");
  const [timeframe, setTimeframe] = useState<Timeframe>("24h");
  const [tab, setTab] = useState<ViewTab>("all");
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  const [selected, setSelected] = useState<Coin | null>(null);
  const [drawerOpen, setDrawerOpen] = useState(false);

  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (dataUpdatedAt) setLastUpdated(new Date(dataUpdatedAt));
  }, [dataUpdatedAt]);

  // Keep selected coin in sync with refreshed market data
  useEffect(() => {
    if (!selected) return;
    const fresh = coins.find((c) => c.id === selected.id);
    if (fresh && fresh !== selected) setSelected(fresh);
  }, [coins, selected]);

  // Cmd/Ctrl + K focuses the search
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        inputRef.current?.focus();
        inputRef.current?.select();
      } else if (e.key === "Escape" && document.activeElement === inputRef.current) {
        inputRef.current?.blur();
        if (search) setSearch("");
      }
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [search]);

  const handleSort = useCallback((k: SortKey) => {
    if (sortKey === k) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortKey(k);
      setSortDir(k === "rank" ? "asc" : "desc");
    }
  }, [sortKey]);

  const handleSelect = useCallback((c: Coin) => {
    setSelected(c);
    setDrawerOpen(true);
  }, []);

  const handleDrawerOpenChange = useCallback((open: boolean) => {
    setDrawerOpen(open);
    if (!open) {
      window.setTimeout(() => setSelected(null), 300);
    }
  }, []);

  const filteredAndSorted = useMemo(() => {
    if (coins.length === 0) return [];
    const q = search.trim().toLowerCase();

    let base: Coin[];
    if (tab === "watchlist") {
      base = coins.filter((c) => watchlist.has(c.id));
    } else if (tab === "gainers") {
      base = [...coins]
        .filter((c) => pctFor(c, timeframe) > 0)
        .sort((a, b) => pctFor(b, timeframe) - pctFor(a, timeframe))
        .slice(0, 50);
    } else if (tab === "losers") {
      base = [...coins]
        .filter((c) => pctFor(c, timeframe) < 0)
        .sort((a, b) => pctFor(a, timeframe) - pctFor(b, timeframe))
        .slice(0, 50);
    } else {
      base = coins;
    }

    const filtered = q
      ? base.filter(
          (c) =>
            c.name.toLowerCase().includes(q) ||
            c.symbol.toLowerCase().includes(q) ||
            c.id.toLowerCase().includes(q),
        )
      : base;

    if ((tab === "gainers" || tab === "losers") && sortKey === "marketCap") {
      return filtered;
    }

    return [...filtered].sort((a, b) => {
      let diff = 0;
      if (sortKey === "rank") diff = a.marketCapRank - b.marketCapRank;
      else if (sortKey === "price") diff = a.currentPrice - b.currentPrice;
      else if (sortKey === "change") diff = pctFor(a, timeframe) - pctFor(b, timeframe);
      else if (sortKey === "marketCap") diff = a.marketCap - b.marketCap;
      else if (sortKey === "volume") diff = a.totalVolume - b.totalVolume;
      return sortDir === "asc" ? diff : -diff;
    });
  }, [coins, search, sortKey, sortDir, timeframe, tab, watchlist]);

  const counts = useMemo(() => {
    if (coins.length === 0) return { gainers: 0, losers: 0, watchlist: 0, all: 0 };
    let g = 0;
    let l = 0;
    for (const c of coins) {
      const p = pctFor(c, timeframe);
      if (p > 0) g++;
      else if (p < 0) l++;
    }
    return { gainers: g, losers: l, watchlist: watchlist.ids.length, all: coins.length };
  }, [coins, timeframe, watchlist.ids]);

  // Eager-load the next page when the user scrolls near the end of the list,
  // unless they're in a filtered tab where loading more doesn't help.
  const canAutoLoad = tab === "all" && !search;
  const handleEndReached = useCallback(() => {
    if (!canAutoLoad) return;
    if (hasNextPage && !isFetchingNextPage) {
      fetchNextPage();
    }
  }, [canAutoLoad, hasNextPage, isFetchingNextPage, fetchNextPage]);

  return (
    <Layout
      lastUpdated={lastUpdated}
      isLive={!isLoading && !isError}
      isLoading={isLoading}
    >
      <GlobalStatsBar
        data={globalStats}
        isLoading={globalLoading && !globalStats}
        coinCount={coins.length}
      />

      <TopMovers
        coins={coins}
        isLoading={isLoading}
        timeframe={timeframe}
        onSelect={handleSelect}
      />

      {/* Toolbar */}
      <div className="flex flex-col sm:flex-row gap-2 sm:gap-3 mb-3">
        <div className="relative flex-1 min-w-0">
          <SearchIcon className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground pointer-events-none" />
          <Input
            ref={inputRef}
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Suche nach Name oder Symbol..."
            className="pl-9 pr-20 bg-card border-border/60 focus:border-primary/60 placeholder:text-muted-foreground/60"
            data-ocid="market.search_input"
          />
          <div className="absolute right-2 top-1/2 -translate-y-1/2 flex items-center gap-1">
            {search ? (
              <button
                type="button"
                onClick={() => setSearch("")}
                className="w-6 h-6 flex items-center justify-center text-muted-foreground hover:text-foreground transition-colors rounded"
                aria-label="Suche löschen"
                data-ocid="market.search_clear_button"
              >
                <XIcon className="w-3.5 h-3.5" />
              </button>
            ) : (
              <kbd className="hidden sm:inline-flex h-5 select-none items-center gap-1 rounded border border-border/60 bg-muted/40 px-1.5 font-mono text-[10px] font-medium text-muted-foreground">
                ⌘K
              </kbd>
            )}
          </div>
        </div>

        <div className="flex items-center rounded-lg border border-border/60 bg-card p-0.5 shrink-0">
          {TIMEFRAMES.map((tf) => (
            <button
              key={tf}
              type="button"
              onClick={() => setTimeframe(tf)}
              className={`px-2.5 sm:px-3 py-1.5 rounded-md text-xs font-semibold uppercase tracking-wider transition-colors ${
                timeframe === tf
                  ? "bg-primary/15 text-primary"
                  : "text-muted-foreground hover:text-foreground"
              }`}
              data-ocid={`market.timeframe_${tf}`}
            >
              {tf}
            </button>
          ))}
        </div>

        <button
          type="button"
          onClick={() => refetch()}
          disabled={isFetching}
          className="hidden sm:flex items-center justify-center w-9 h-9 rounded-lg border border-border/60 bg-card text-muted-foreground hover:text-foreground hover:border-border transition-colors disabled:opacity-50"
          aria-label="Daten aktualisieren"
          data-ocid="market.refresh_button"
        >
          <RefreshCwIcon className={`w-4 h-4 ${isFetching ? "animate-spin" : ""}`} />
        </button>
      </div>

      {/* Tabs */}
      <div className="flex items-center gap-1 mb-3 overflow-x-auto -mx-1 px-1" data-ocid="market.tabs">
        {(
          [
            { key: "all", label: "Alle", count: counts.all },
            { key: "watchlist", label: "Watchlist", count: counts.watchlist, icon: <StarIcon className="w-3 h-3" /> },
            { key: "gainers", label: "Top Gewinner", count: counts.gainers },
            { key: "losers", label: "Top Verlierer", count: counts.losers },
          ] as Array<{ key: ViewTab; label: string; count: number; icon?: React.ReactNode }>
        ).map(({ key, label, count, icon }) => {
          const active = tab === key;
          return (
            <button
              key={key}
              type="button"
              onClick={() => setTab(key)}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-semibold transition-colors shrink-0 ${
                active
                  ? "bg-card border border-border text-foreground"
                  : "text-muted-foreground hover:text-foreground border border-transparent"
              }`}
              data-ocid={`market.tab_${key}`}
            >
              {icon}
              <span>{label}</span>
              <span className="text-[10px] tabular-nums opacity-70">{count}</span>
            </button>
          );
        })}
      </div>

      {/* Table */}
      <div className="rounded-xl border border-border/60 overflow-hidden bg-background shadow-subtle">
        <TableHeader
          sortKey={sortKey}
          sortDir={sortDir}
          timeframe={timeframe}
          onSort={handleSort}
        />

        {isLoading && (
          <div className="space-y-0" data-ocid="market.loading_state">
            {Array.from({ length: 12 }, (_, i) => i).map((i) => (
              <div key={i} className="flex items-center gap-3 px-4 py-3.5 border-b border-border/30">
                <Skeleton className="w-4 h-4 rounded" />
                <Skeleton className="w-6 h-4 rounded" />
                <Skeleton className="w-7 h-7 rounded-full" />
                <div className="flex-1 space-y-1">
                  <Skeleton className="h-4 w-28 rounded" />
                  <Skeleton className="h-3 w-12 rounded" />
                </div>
                <Skeleton className="hidden sm:block h-7 w-[88px] rounded" />
                <Skeleton className="h-4 w-20 rounded" />
                <Skeleton className="h-6 w-14 rounded" />
                <Skeleton className="hidden md:block h-4 w-20 rounded" />
                <Skeleton className="hidden sm:block h-4 w-20 rounded" />
              </div>
            ))}
          </div>
        )}

        {isError && (
          <div className="flex flex-col items-center justify-center gap-3 py-16 text-center" data-ocid="market.error_state">
            <p className="text-muted-foreground text-sm">Daten konnten nicht geladen werden.</p>
            <button type="button" onClick={() => refetch()} className="text-primary text-sm hover:underline">
              Erneut versuchen
            </button>
          </div>
        )}

        {!isLoading && !isError && filteredAndSorted.length === 0 && (
          <div className="flex flex-col items-center justify-center gap-2 py-16 text-center px-6" data-ocid="market.empty_state">
            {tab === "watchlist" && watchlist.ids.length === 0 ? (
              <>
                <StarIcon className="w-8 h-8 text-muted-foreground/40" />
                <p className="text-muted-foreground text-sm">Noch keine Coins in deiner Watchlist.</p>
                <p className="text-muted-foreground/60 text-xs max-w-sm">
                  Tippe auf das Stern-Symbol neben einem Coin, um ihn hier zu speichern.
                </p>
                <button type="button" onClick={() => setTab("all")} className="text-primary text-sm hover:underline mt-1">
                  Alle Coins anzeigen
                </button>
              </>
            ) : search ? (
              <>
                <p className="text-muted-foreground text-sm">Keine Ergebnisse für &quot;{search}&quot;</p>
                <button type="button" onClick={() => setSearch("")} className="text-primary text-sm hover:underline">
                  Suche zurücksetzen
                </button>
              </>
            ) : (
              <p className="text-muted-foreground text-sm">Keine Coins im aktuellen Filter.</p>
            )}
          </div>
        )}

        {!isLoading && !isError && filteredAndSorted.length > 0 && (
          <VirtualCoinList
            coins={filteredAndSorted}
            timeframe={timeframe}
            watchlistHas={watchlist.has}
            flashes={flashes}
            onToggleFavorite={watchlist.toggle}
            onSelect={handleSelect}
            onEndReached={handleEndReached}
          />
        )}
      </div>

      {/* Mehr laden */}
      {!isLoading && !isError && canAutoLoad && hasNextPage && (
        <div className="flex justify-center mt-4">
          <button
            type="button"
            onClick={() => fetchNextPage()}
            disabled={isFetchingNextPage}
            className="px-4 py-2 rounded-lg border border-border/60 bg-card text-sm font-semibold text-foreground hover:bg-card/80 disabled:opacity-60 transition-colors"
            data-ocid="market.load_more_button"
          >
            {isFetchingNextPage
              ? `Lädt... (${coins.length} von ${TOTAL_COINS_TARGET})`
              : `Mehr laden (${coins.length} von ${TOTAL_COINS_TARGET})`}
          </button>
        </div>
      )}

      {!isLoading && !isError && canAutoLoad && !hasNextPage && coins.length >= MAX_PAGES * 100 && (
        <p className="text-center text-xs text-muted-foreground mt-4">
          Alle {coins.length} Coins geladen.
        </p>
      )}

      <CoinDetailDrawer
        coin={selected}
        open={drawerOpen}
        onOpenChange={handleDrawerOpenChange}
        isFavorite={selected ? watchlist.has(selected.id) : false}
        onToggleFavorite={watchlist.toggle}
      />
    </Layout>
  );
}
