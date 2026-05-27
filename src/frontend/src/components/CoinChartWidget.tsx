import { useCoinChart } from "@/lib/api";
import { formatPrice } from "@/lib/format";
import {
  CHART_TIMEFRAMES,
  type ChartKind,
  type ChartTimeframe,
  timeframeLabel,
  timeframeToDays,
} from "@/types/coin";
import { LineChartIcon, BarChart3Icon } from "lucide-react";
import {
  type CandlestickData,
  ColorType,
  createChart,
  CrosshairMode,
  type IChartApi,
  type ISeriesApi,
  type LineData,
  LineStyle,
  type UTCTimestamp,
} from "lightweight-charts";
import { useEffect, useMemo, useRef, useState } from "react";

interface CoinChartWidgetProps {
  coinId: string;
  open: boolean;
}

export function CoinChartWidget({ coinId, open }: CoinChartWidgetProps) {
  const [timeframe, setTimeframe] = useState<ChartTimeframe>("7d");
  const [kind, setKind] = useState<ChartKind>("line");
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesRef = useRef<ISeriesApi<"Line"> | ISeriesApi<"Candlestick"> | null>(null);

  const days = timeframeToDays(timeframe);
  const { data, isLoading, isError } = useCoinChart(coinId, days, kind, open);

  // Construct chart once when container is mounted.
  useEffect(() => {
    if (!open) return;
    const container = containerRef.current;
    if (!container) return;
    const chart = createChart(container, {
      width: container.clientWidth,
      height: 320,
      layout: {
        background: { type: ColorType.Solid, color: "transparent" },
        textColor: "rgba(220, 220, 230, 0.75)",
        fontFamily: "DM Sans, system-ui, sans-serif",
        fontSize: 11,
      },
      grid: {
        vertLines: { color: "rgba(255,255,255,0.05)", style: LineStyle.Dotted },
        horzLines: { color: "rgba(255,255,255,0.05)", style: LineStyle.Dotted },
      },
      rightPriceScale: { borderVisible: false, scaleMargins: { top: 0.1, bottom: 0.1 } },
      timeScale: { borderVisible: false, timeVisible: true, secondsVisible: false },
      crosshair: {
        mode: CrosshairMode.Magnet,
        vertLine: { color: "rgba(120,255,180,0.4)", width: 1, style: LineStyle.Solid, labelBackgroundColor: "#0f1418" },
        horzLine: { color: "rgba(120,255,180,0.4)", width: 1, style: LineStyle.Solid, labelBackgroundColor: "#0f1418" },
      },
      handleScroll: true,
      handleScale: true,
    });
    chartRef.current = chart;
    const ro = new ResizeObserver((entries) => {
      for (const e of entries) {
        chart.applyOptions({ width: e.contentRect.width });
      }
    });
    ro.observe(container);
    return () => {
      ro.disconnect();
      chart.remove();
      chartRef.current = null;
      seriesRef.current = null;
    };
  }, [open]);

  // Rebuild series whenever the chart kind changes.
  useEffect(() => {
    const chart = chartRef.current;
    if (!chart) return;
    if (seriesRef.current) {
      chart.removeSeries(seriesRef.current);
      seriesRef.current = null;
    }
    if (kind === "line") {
      seriesRef.current = chart.addLineSeries({
        color: "oklch(0.72 0.22 145)",
        lineWidth: 2,
        priceFormat: { type: "price", precision: 4, minMove: 0.0001 },
      });
    } else {
      seriesRef.current = chart.addCandlestickSeries({
        upColor: "oklch(0.72 0.22 145)",
        downColor: "oklch(0.62 0.24 25)",
        wickUpColor: "oklch(0.72 0.22 145)",
        wickDownColor: "oklch(0.62 0.24 25)",
        borderVisible: false,
      });
    }
  }, [kind]);

  // Push data into the active series whenever new data lands.
  useEffect(() => {
    if (!data || !seriesRef.current) return;
    if (kind === "line") {
      const points: LineData[] = data.line.map((p) => ({
        time: Math.floor(p.timestamp / 1000) as UTCTimestamp,
        value: p.price,
      }));
      (seriesRef.current as ISeriesApi<"Line">).setData(points);
    } else {
      const candles: CandlestickData[] = data.candles.map((c) => ({
        time: Math.floor(c.timestamp / 1000) as UTCTimestamp,
        open: c.open, high: c.high, low: c.low, close: c.close,
      }));
      (seriesRef.current as ISeriesApi<"Candlestick">).setData(candles);
    }
    chartRef.current?.timeScale().fitContent();
  }, [data, kind]);

  const summary = useMemo(() => {
    if (!data) return null;
    if (kind === "line" && data.line.length > 0) {
      const first = data.line[0].price;
      const last = data.line[data.line.length - 1].price;
      const change = ((last - first) / first) * 100;
      return { price: last, change };
    }
    if (kind === "candle" && data.candles.length > 0) {
      const first = data.candles[0].open;
      const last = data.candles[data.candles.length - 1].close;
      const change = ((last - first) / first) * 100;
      return { price: last, change };
    }
    return null;
  }, [data, kind]);

  return (
    <div className="space-y-3" data-ocid="coinChart.container">
      {/* Header: price + change */}
      <div className="flex items-baseline justify-between gap-3">
        <div>
          {summary ? (
            <>
              <p className="text-2xl font-display font-bold text-foreground tabular-nums">
                {formatPrice(summary.price)}
              </p>
              <p
                className={`text-xs font-semibold tabular-nums ${
                  summary.change >= 0 ? "text-price-up" : "text-price-down"
                }`}
              >
                {summary.change >= 0 ? "▲" : "▼"}{" "}
                {Math.abs(summary.change).toFixed(2)} % über {timeframeLabel(timeframe)}
              </p>
            </>
          ) : (
            <p className="text-xs text-muted-foreground">Lade Chart...</p>
          )}
        </div>
        {/* Chart kind toggle */}
        <div className="flex items-center rounded-lg border border-border/60 bg-card p-0.5 shrink-0">
          <button
            type="button"
            onClick={() => setKind("line")}
            className={`px-2 py-1 rounded-md flex items-center gap-1 text-[11px] font-semibold transition-colors ${
              kind === "line"
                ? "bg-primary/15 text-primary"
                : "text-muted-foreground hover:text-foreground"
            }`}
            aria-label="Linien-Chart"
            data-ocid="coinChart.kind_line"
          >
            <LineChartIcon className="w-3.5 h-3.5" />
            Linie
          </button>
          <button
            type="button"
            onClick={() => setKind("candle")}
            className={`px-2 py-1 rounded-md flex items-center gap-1 text-[11px] font-semibold transition-colors ${
              kind === "candle"
                ? "bg-primary/15 text-primary"
                : "text-muted-foreground hover:text-foreground"
            }`}
            aria-label="Candlestick-Chart"
            data-ocid="coinChart.kind_candle"
          >
            <BarChart3Icon className="w-3.5 h-3.5" />
            Kerzen
          </button>
        </div>
      </div>

      {/* Chart area */}
      <div className="relative rounded-lg border border-border/50 bg-background/40 p-2">
        <div
          ref={containerRef}
          className="w-full"
          style={{ height: 320 }}
          data-ocid="coinChart.canvas_container"
        />
        {(isLoading || isError) && (
          <div className="absolute inset-0 flex items-center justify-center text-xs text-muted-foreground pointer-events-none">
            {isError ? "Chart konnte nicht geladen werden" : "Lade Daten..."}
          </div>
        )}
      </div>

      {/* Timeframe selector */}
      <div className="flex flex-wrap gap-1">
        {CHART_TIMEFRAMES.map((tf) => (
          <button
            key={tf}
            type="button"
            onClick={() => setTimeframe(tf)}
            className={`px-2.5 py-1 rounded-md text-[11px] font-semibold uppercase tracking-wider transition-colors ${
              timeframe === tf
                ? "bg-primary/15 text-primary border border-primary/30"
                : "bg-card border border-border/60 text-muted-foreground hover:text-foreground"
            }`}
            data-ocid={`coinChart.tf_${tf}`}
          >
            {timeframeLabel(tf)}
          </button>
        ))}
      </div>
    </div>
  );
}
