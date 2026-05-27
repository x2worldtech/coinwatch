const DE_LOCALE = "de-DE";

export function formatPrice(value: number): string {
  if (value === 0) return "€0,00";

  if (value < 0.01) {
    return new Intl.NumberFormat(DE_LOCALE, {
      style: "currency",
      currency: "EUR",
      minimumFractionDigits: 6,
      maximumFractionDigits: 6,
    }).format(value);
  }

  if (value < 1) {
    return new Intl.NumberFormat(DE_LOCALE, {
      style: "currency",
      currency: "EUR",
      minimumFractionDigits: 4,
      maximumFractionDigits: 4,
    }).format(value);
  }

  return new Intl.NumberFormat(DE_LOCALE, {
    style: "currency",
    currency: "EUR",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(value);
}

export function formatMarketCap(value: number): string {
  if (value >= 1_000_000_000_000) {
    const n = value / 1_000_000_000_000;
    return `${new Intl.NumberFormat(DE_LOCALE, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(n)} Bio €`;
  }
  if (value >= 1_000_000_000) {
    const n = value / 1_000_000_000;
    return `${new Intl.NumberFormat(DE_LOCALE, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(n)} Mrd €`;
  }
  if (value >= 1_000_000) {
    const n = value / 1_000_000;
    return `${new Intl.NumberFormat(DE_LOCALE, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(n)} Mio €`;
  }
  return new Intl.NumberFormat(DE_LOCALE, {
    style: "currency",
    currency: "EUR",
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(value);
}

export function formatVolume(value: number): string {
  return formatMarketCap(value);
}

export function formatCompactNumber(value: number): string {
  if (!Number.isFinite(value) || value === 0) return "—";
  if (value >= 1_000_000_000_000) {
    return `${new Intl.NumberFormat(DE_LOCALE, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(value / 1_000_000_000_000)} Bio`;
  }
  if (value >= 1_000_000_000) {
    return `${new Intl.NumberFormat(DE_LOCALE, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(value / 1_000_000_000)} Mrd`;
  }
  if (value >= 1_000_000) {
    return `${new Intl.NumberFormat(DE_LOCALE, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(value / 1_000_000)} Mio`;
  }
  if (value >= 1_000) {
    return `${new Intl.NumberFormat(DE_LOCALE, {
      minimumFractionDigits: 1,
      maximumFractionDigits: 1,
    }).format(value / 1_000)} Tsd`;
  }
  return new Intl.NumberFormat(DE_LOCALE, {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(value);
}

export function formatPercent(value: number): string {
  if (!Number.isFinite(value)) return "—";
  const formatted = new Intl.NumberFormat(DE_LOCALE, {
    minimumFractionDigits: 1,
    maximumFractionDigits: 2,
  }).format(Math.abs(value));
  if (value > 0) return `▲ ${formatted} %`;
  if (value < 0) return `▼ ${formatted} %`;
  return `${formatted} %`;
}

export function formatPercentPlain(value: number): string {
  if (!Number.isFinite(value)) return "—";
  return `${new Intl.NumberFormat(DE_LOCALE, {
    minimumFractionDigits: 1,
    maximumFractionDigits: 2,
  }).format(value)} %`;
}

export function formatNumber(value: number): string {
  return new Intl.NumberFormat(DE_LOCALE).format(value);
}

export function formatSupply(value: number, symbol: string): string {
  if (!value || !Number.isFinite(value) || value <= 0) return "—";
  return `${formatCompactNumber(value)} ${symbol}`;
}
