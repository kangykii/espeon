import type { TradeOutcome } from "./types";
import { icons } from "./icons";

export type PnlRange = "30d" | "90d" | "1y" | "all";

const periodMs: Record<Exclude<PnlRange, "all">, number> = {
  "30d": 30 * 86_400_000,
  "90d": 90 * 86_400_000,
  "1y": 365 * 86_400_000,
};

const formatPnl = (value: number) => new Intl.NumberFormat(undefined, {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
  signDisplay: "exceptZero",
}).format(value);

const formatDate = (value: number) => new Date(value).toLocaleDateString(undefined, {
  month: "short",
  day: "numeric",
});

export function renderPositionsChart(outcomes: TradeOutcome[], range: PnlRange): string {
  const now = Date.now();
  const cutoff = range === "all" ? -Infinity : now - periodMs[range];
  const trades = outcomes
    .filter((item) => item.grossRealizedPnl !== null && Number.isFinite(item.grossRealizedPnl))
    .map((item) => ({ time: new Date(item.completedAt).getTime(), pnl: item.grossRealizedPnl! }))
    .filter((item) => Number.isFinite(item.time) && item.time >= cutoff)
    .sort((a, b) => a.time - b.time);

  let total = 0;
  const points = trades.map((trade) => ({ time: trade.time, value: total += trade.pnl }));
  const option = (value: PnlRange, label: string) => `<option value="${value}" ${range === value ? "selected" : ""}>${label}</option>`;
  const header = `<div class="pnl-chart-header"><h3 title="Cumulative gross realized P&L from completed trades with known cost basis, before fees.">Realized P&amp;L</h3><div class="pnl-chart-controls"><strong class="${total < 0 ? "is-negative" : ""}">${points.length ? formatPnl(total) : "—"}</strong><label><span class="sr-only">Chart period</span><select id="pnl-chart-range" aria-label="Chart period">${option("30d", "30 days")}${option("90d", "90 days")}${option("1y", "1 year")}${option("all", "All time")}</select>${icons.chevronDown}</label></div></div>`;
  if (!points.length) {
    return `<section class="pnl-chart" aria-label="Realized P&L chart">${header}<div class="pnl-chart-empty"><svg viewBox="0 0 1000 265" aria-hidden="true"><g class="pnl-chart-grid"><line x1="72" x2="968" y1="35" y2="35"/><line x1="72" x2="968" y1="95" y2="95"/><line x1="72" x2="968" y1="155" y2="155"/><line x1="72" x2="968" y1="215" y2="215"/></g><line class="pnl-chart-empty-baseline" x1="72" x2="968" y1="155" y2="155"/></svg><span title="P&L appears after a completed broker round trip with known cost basis.">No P&amp;L data</span></div></section>`;
  }

  const left = 72, right = 968, top = 22, bottom = 220;
  const values = [0, ...points.map((point) => point.value)];
  const rawMin = Math.min(...values), rawMax = Math.max(...values);
  const padding = Math.max((rawMax - rawMin) * 0.16, Math.abs(rawMax || rawMin) * 0.08, 0.01);
  const min = rawMin - padding, max = rawMax + padding;
  const first = points[0].time, last = points.at(-1)!.time;
  const timePadding = Math.max((last - first) * 0.05, 3_600_000);
  const start = range === "all" ? first - timePadding : cutoff;
  const end = range === "all" ? last + timePadding : now;
  const x = (time: number) => left + (time - start) / Math.max(end - start, 1) * (right - left);
  const y = (value: number) => bottom - (value - min) / (max - min) * (bottom - top);
  const startY = y(0);
  const line = [`M ${left} ${startY}`, ...points.map((point) => `L ${x(point.time)} ${y(point.value)}`)].join(" ");
  const lastPoint = points.at(-1)!;
  const area = `${line} L ${x(lastPoint.time)} ${bottom} L ${left} ${bottom} Z`;
  const ticks = Array.from({ length: 4 }, (_, index) => {
    const value = max - index * (max - min) / 3;
    const position = y(value);
    return `<g><line x1="${left}" x2="${right}" y1="${position}" y2="${position}"/><text x="${left - 13}" y="${position + 4}" text-anchor="end">${formatPnl(value)}</text></g>`;
  }).join("");
  const dates = [start, (start + end) / 2, end].map((time, index) => `<text x="${[left, (left + right) / 2, right][index]}" y="250" text-anchor="${["start", "middle", "end"][index]}">${formatDate(time)}</text>`).join("");

  return `<section class="pnl-chart" aria-label="Realized P&L chart">${header}<svg class="pnl-chart-plot" viewBox="0 0 1000 265" role="img" aria-label="Cumulative realized P&L across ${points.length} completed trades"><defs><linearGradient id="pnl-area-gradient" x1="0" x2="0" y1="0" y2="1"><stop class="pnl-chart-gradient-start" offset="0%"/><stop class="pnl-chart-gradient-end" offset="100%"/></linearGradient></defs><g class="pnl-chart-grid">${ticks}<line class="pnl-chart-zero" x1="${left}" x2="${right}" y1="${startY}" y2="${startY}"/></g><path class="pnl-chart-area" d="${area}"/><path class="pnl-chart-line" d="${line}"/><circle class="pnl-chart-endpoint" cx="${x(lastPoint.time)}" cy="${y(lastPoint.value)}" r="4"/><g class="pnl-chart-dates">${dates}</g></svg></section>`;
}
