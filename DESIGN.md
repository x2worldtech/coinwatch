# Design Brief

## Direction

CryptoMarket — professional, data-dense market-cap dashboard for the German audience. Faster, richer and more useful than CoinMarketCap or CoinGecko: every coin tells its story in three timeframes (1h / 24h / 7d), the global market state is always visible, and a single tap opens a coin's full profile.

## Tone

Brutalist-minimalist with precision: ultra-deep charcoal background, vivid neon accents (emerald up, crimson down). No decoration, pure information hierarchy. Every pixel earns its space.

## Differentiation

- Live global stats bar (total market cap, 24h volume, BTC / ETH dominance, active coins) sits above the table — context first, list second.
- Top Movers strip surfaces the five biggest gainers and losers per timeframe in one glance.
- Persistent watchlist (★) stored client-side; "Watchlist", "Top Gewinner", "Top Verlierer" tabs reuse the same row.
- Subtle price-flash highlight on each row when the live tick moves — green up, red down, settles in 1.2 s.
- One-tap coin drawer with large 7-day chart, ATH/ATL distance, 24 h high/low, supply, volume — no page reload, no route change.

## Color Palette

| Token      | OKLCH           | Role                                 |
| ---------- | --------------- | ------------------------------------ |
| background | 0.10 0.008 260  | Ultra-deep charcoal page background  |
| foreground | 0.95 0.005 260  | Near-white text on dark              |
| card       | 0.14 0.012 265  | Elevated surface for data sections   |
| primary    | 0.72 0.22 145   | Neon emerald accent for positive     |
| accent     | 0.55 0.22 25    | Vivid crimson accent for negative    |
| muted      | 0.18 0.015 265  | Secondary surface for subtle detail  |
| border     | 0.22 0.015 265  | Hairline borders                     |

## Typography

- Display: Space Grotesk — section headers, coin names, hero numbers
- Body: DM Sans — labels, descriptions
- Mono: Geist Mono — every price, percentage, market cap
- Scale: hero `text-3xl font-bold tracking-tight`, h2 `text-lg font-display font-bold`, label `text-[10px] uppercase tracking-wider`, body `text-sm`, micro `text-[11px]`

## Elevation & Depth

Minimal: card surfaces sit 1px above background via subtle border, no drop shadows except a faint table-card shadow for separation. Neon accents handle visual pop.

## Structural Zones

| Zone           | Background      | Border          | Notes                                  |
| -------------- | --------------- | --------------- | -------------------------------------- |
| Header         | bg-card / 80    | border-border   | Sticky, blurred, LIVE pulse + clock    |
| Global stats   | bg-card         | border-border   | 5 columns, divided, single row         |
| Top movers     | bg-card / 60    | border-border   | Two horizontally-scrolling card rows   |
| Toolbar        | bg-card         | border-border   | Search · timeframe · refresh          |
| Tabs           | inline          | —               | Pill tabs, active gets card bg         |
| Data table     | bg-background   | border-border   | Sticky column header below site header |
| Coin drawer    | bg-card         | border-l        | Slides from right, full-height         |
| Footer         | bg-card         | border-border   | Attribution + data source              |

## Spacing & Rhythm

Compact density for data: base unit 4px, section gaps 16-24px, card padding 12-16px, row height 56-60px. Mobile shrinks gaps and hides volume/market-cap columns while keeping price, change, sparkline visible.

## Component Patterns

- Buttons: minimal, transparent or muted fill, rounded 6-8px, no shadows. Icon-only buttons sit on a 36x36 square.
- Pills (tabs / timeframe): rounded-md, active state uses card background and 1px border, inactive is text-only.
- Cards: 1px border on background, rounded 8-12px, hover state shifts background by ~5 %.
- Price badges: neon background at 10 % opacity, neon foreground, rounded 4px, font-semibold, tabular-nums.
- Sparklines: SVG with neon stroke (1.5px) and matching gradient fill (35 % → 0 %), inline 88×32 px.
- Star button: outline by default, filled emerald when in watchlist.

## Motion

Restrained:
- Price-flash: 1.2 s ease-out background fade on a row when its price changes.
- Sheet drawer: 300-500 ms slide-in from right, Radix-driven, no easing custom.
- Sort arrow: rotates 180° to indicate direction.
- Loading spinner on the refresh button uses Tailwind `animate-spin`.

No hover scaling, no page transitions, no decorative animation.

## Constraints

- Performance first: memoized rows (`React.memo`), single canister query (60 s cache server-side, 30 s on client), sparkline rendered as static SVG with `useId`-based gradients (no re-keyed defs).
- Stale-data fallback: if the upstream HTTP outcall fails, the canister returns the previous successful snapshot instead of erroring.
- German number formatting (`1.234,56 €`, `Mio`, `Mrd`, `Bio`) — handled centrally in `lib/format.ts`.
- Mobile-first: search and timeframe stack on small screens, columns drop progressively (volume hidden below `md`, market-cap and sparkline hidden below `sm`).
- Keyboard: `⌘K` / `Ctrl+K` focuses the search, `Esc` clears it.
- Watchlist persists via `localStorage`, multi-tab synced via the `storage` event.

## Signature Detail

The combination of a never-changing global stats bar, instant timeframe pivoting (1h ↔ 24h ↔ 7d) and tap-to-detail drawer makes the page feel like a trading terminal that still respects the eye — high information density without visual noise.
