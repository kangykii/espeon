import "@fontsource/ibm-plex-sans/latin-400.css";
import "@fontsource/ibm-plex-sans/latin-500.css";
import "@fontsource/ibm-plex-sans/latin-600.css";
import "./styles.css";
import "./windowChrome.css";
import "./positionsChart.css";
import { harnessService } from "./services/harness";
import { bindWindowChrome } from "./windowChrome";
import { icons, renderIcons } from "./icons";
import { renderPositionsChart, type PnlRange } from "./positionsChart";
import type { ConnectorSection, ConnectorSettings, ConnectorSettingsUpdate, ContextRecord, Decision, EventView, Hypothesis, LoopView, ReplayState, RunSnapshot, SearchHit, UpdateStatus, WorkspaceSnapshot } from "./types";

type Tab = "home" | "activity" | "positions" | "evidence" | "history";
type Theme = "light" | "dark";
const app = document.querySelector<HTMLDivElement>("#app")!;
const preferences = {
  theme: (localStorage.getItem("jev.ui.theme") as Theme) || "light",
  sidebarCollapsed: localStorage.getItem("jev.ui.sidebar-collapsed") !== "false",
  inspectorCollapsed: localStorage.getItem("jev.ui.inspector-collapsed") === "true",
  sidebarWidth: Number(localStorage.getItem("jev.ui.sidebar-width")) || 292,
  inspectorWidth: Number(localStorage.getItem("jev.ui.inspector-width")) || 344,
};
let compactInspectorOpen = false;
let pnlRange: PnlRange = "all";
let promptDraft = "";
let settingsPage = "overview";
let updateStatus: UpdateStatus | null = null;
let updateChecking = false;
let mcpChecking = false;
let mcpProbeMessage = "";
let liveRiskApprovalBusy = false;
let liveRiskApprovalMessage = "";
let updateRetryTimer: number | null = null;
const sidebarIsCollapsed = () => preferences.sidebarCollapsed;
const inspectorIsCollapsed = () => window.innerWidth <= 1080 ? !compactInspectorOpen : preferences.inspectorCollapsed;
const state: { workspace: WorkspaceSnapshot | null; selectedSnapshot: RunSnapshot | null; selectedRunId: string | null; selectedLoopId: string | null; selectedEvidenceId: string | null; replay: ReplayState | null; tab: Tab; searchResults: SearchHit[]; searchQuery: string; notice: string; noticeTone: "neutral" | "error" | "success"; loading: boolean; stoppingRunId: string | null; settingsOpen: boolean; connectorSettings: ConnectorSettings | null; settingsSaving: boolean; showArchived: boolean; renamingRunId: string | null; editingTitleRunId: string | null } = {
  workspace: null, selectedSnapshot: null, selectedRunId: null, selectedLoopId: null, selectedEvidenceId: null, replay: null,
  tab: "home", searchResults: [], searchQuery: "", notice: "Hydrating local workspace…", noticeTone: "neutral", loading: true,
  stoppingRunId: null, settingsOpen: false, connectorSettings: null, settingsSaving: false, showArchived: false, renamingRunId: null, editingTitleRunId: null,
};

function escapeHtml(value: unknown): string { const node = document.createElement("div"); node.textContent = String(value ?? ""); return node.innerHTML; }
function shortId(value: string | null | undefined): string { return value ? `${value.slice(0, 7)}…${value.slice(-4)}` : "—"; }
function runTitle(runId: string, fallback: string): string { return state.workspace?.runHistory.find((run) => run.runId === runId)?.thesis ?? fallback; }
function dateTime(value: string | null | undefined): string { if (!value) return "—"; const parsed = new Date(value); return Number.isNaN(parsed.getTime()) ? escapeHtml(value) : parsed.toLocaleString([], { dateStyle: "medium", timeStyle: "short" }); }
function relativeTime(value: string): string { const minutes = Math.floor((Date.now() - new Date(value).getTime()) / 60_000); if (minutes < 1) return "now"; if (minutes < 60) return `${minutes}m`; const hours = Math.floor(minutes / 60); return hours < 24 ? `${hours}h` : `${Math.floor(hours / 24)}d`; }
function currentSnapshot(): RunSnapshot | null {
  const active = state.workspace?.activeRuns.find((run) => run.runId === state.selectedRunId);
  if (active) return active;
  return state.selectedSnapshot?.runId === state.selectedRunId && state.selectedSnapshot.status === "active" ? state.selectedSnapshot : null;
}
function selectedRunSnapshot(): RunSnapshot | null {
  return state.selectedSnapshot?.runId === state.selectedRunId ? state.selectedSnapshot : currentSnapshot();
}
function currentHypotheses(): Hypothesis[] { return state.replay?.hypotheses ?? currentSnapshot()?.hypotheses ?? []; }
function currentLoops(): LoopView[] { return state.replay?.loops ?? currentSnapshot()?.loops ?? []; }
function selectedLoop(): LoopView | null { return currentLoops().find((loop) => loop.id === state.selectedLoopId) ?? currentLoops()[0] ?? null; }
function hypothesisFor(loop: LoopView | null): Hypothesis | null { return loop ? currentHypotheses().find((item) => item.id === loop.hypothesisId) ?? null : currentHypotheses().at(-1) ?? null; }
function latestDecision(loopId: string): Decision | null { return [...(state.replay?.decisions ?? [])].reverse().find((item) => item.loopId === loopId) ?? null; }
function positionFor(loopId: string) { return [...(state.replay?.positions ?? currentSnapshot()?.positions ?? [])].reverse().find((item) => item.loopId === loopId) ?? null; }
function selectedEvidence(): ContextRecord | null { const records = state.replay?.contextPoolRecords ?? []; return records.find((item) => item.id === state.selectedEvidenceId || item.canonicalEntityId === state.selectedEvidenceId) ?? null; }

function readableContextValue(value: unknown): string {
  if (typeof value === "boolean") return value ? "Yes" : "No";
  if (typeof value === "number") return Number.isFinite(value) ? new Intl.NumberFormat(undefined, { maximumFractionDigits: 8 }).format(value) : String(value);
  if (value === null || value === undefined || value === "") return "Not available";
  if (typeof value === "string") return value;
  return JSON.stringify(value);
}

function contextOperand(value: unknown): string {
  if (typeof value === "string" || typeof value === "number") return String(value);
  if (!value || typeof value !== "object" || Array.isArray(value)) return "the recorded value";
  const node = value as Record<string, unknown>;
  const op = String(node.op ?? "");
  if (op === "current_mid") return "the current market midpoint";
  if (op === "current_bid") return "the current bid price";
  if (op === "current_ask") return "the current ask price";
  if (op === "series") {
    const period = String(node.period ?? "market");
    const column = String(node.column ?? "value").replaceAll("_", " ");
    const lag = Number(node.lag ?? 0);
    const relative = lag === 0 ? "latest" : lag === 1 ? "previous" : `${lag} bars earlier`;
    return `the ${relative} ${period} candle ${column}`;
  }
  if (op === "constant") return String(node.value ?? "the recorded constant");
  return op ? op.replaceAll("_", " ") : "the recorded value";
}

function contextFormulaDescription(value: Record<string, unknown>): string {
  const op = String(value.op ?? "");
  const left = contextOperand(value.left);
  const right = contextOperand(value.right);
  const relations: Record<string, string> = {
    greater_than: "is above", greater_than_or_equal: "is at or above",
    less_than: "is below", less_than_or_equal: "is at or below",
    equal: "matches", equals: "matches", not_equal: "does not match",
  };
  if (relations[op]) return `${left[0]?.toUpperCase() ?? ""}${left.slice(1)} ${relations[op]} ${right}.`;
  if (op === "and" || op === "or") {
    const conjunction = op === "and" ? "and" : "or";
    return `${contextFormulaDescription((value.left as Record<string, unknown>) ?? {})} ${conjunction} ${contextFormulaDescription((value.right as Record<string, unknown>) ?? {})}`;
  }
  if (op === "series" || op.startsWith("current_")) return `${contextOperand(value)} is used as the context value.`;
  return `Context is calculated using ${op.replaceAll("_", " ") || "the recorded rule"}.`;
}

function liveContextSection(decision: Decision | null): string {
  const snapshot = decision?.resolvedState.liveContextSnapshot;
  const symbol = hypothesisFor(selectedLoop())?.instruments[0] ?? "";
  const feedStates = (state.workspace?.integrations ?? []).filter((item) => item.id.startsWith("market-") && item.id.endsWith(`:${symbol}`));
  const healthRows = feedStates.length ? `<dl>${feedStates.map((item) => `${definitionRow(item.label, `${item.state}: ${item.detail}`)}`).join("")}</dl>` : `<p class="muted">Feed workers have not reported health yet.</p>`;
  if (!snapshot) {
    return `<section class="inspector-section"><h3>Live market context</h3><p class="muted">No resolved live snapshot yet. A missing or stale feed skips Jev and is recorded in Activity.</p>${healthRows}</section>`;
  }
  const latest = [...snapshot.candles].sort((a, b) => b.openTime.localeCompare(a.openTime))[0];
  const formulaRows = snapshot.fields.map((field) => `<article class="context-item"><strong>${escapeHtml(field.label)}</strong><span class="context-value">${escapeHtml(readableContextValue(field.value))}</span><p class="context-explanation">${escapeHtml(contextFormulaDescription(field.formula))}</p><time>Observed ${dateTime(field.observedAt)} · ${field.provenance.length} source${field.provenance.length === 1 ? "" : "s"}</time><details class="context-technical"><summary>Formula and source details</summary><p>Exact formula</p><pre>${escapeHtml(JSON.stringify(field.formula, null, 2))}</pre><p>Provenance</p><ul>${field.provenance.map((source) => `<li>${escapeHtml(source)}</li>`).join("") || "<li>None recorded</li>"}</ul><p>Source observation IDs</p><ul>${(field.sourceObservationIds ?? []).map((id) => `<li><code>${escapeHtml(id)}</code></li>`).join("") || "<li>None recorded</li>"}</ul></details></article>`).join("");
  return `<section class="inspector-section"><h3>Live market context</h3><dl>${definitionRow("Snapshot", shortId(snapshot.id), true)}${definitionRow("Freshness", snapshot.freshnessState)}${definitionRow("Data route", snapshot.qualityState ?? "legacy")}${definitionRow("Bid / Ask", `${snapshot.quote.bid} / ${snapshot.quote.ask}`)}${definitionRow("Mid / Spread", `${snapshot.quote.mid} / ${snapshot.quote.spread}`)}${definitionRow("Quote received", dateTime(snapshot.quote.receivedAt))}${definitionRow("Latest closed bar", latest ? `${latest.period} · ${dateTime(latest.openTime)} · O ${latest.open} H ${latest.high} L ${latest.low} C ${latest.close}` : "None")}${definitionRow("Bar source", latest?.provenance)}${definitionRow("Volume measure", latest?.volumeKind ?? "legacy")}${definitionRow("Provider volume", latest?.providerVolume ?? "unavailable")}</dl>${formulaRows}<h3>Feed services</h3>${healthRows}</section>`;
}
function statusChip(value: string, label = value): string {
  const tone = /active|connected|filled|accepted|approved|long|buy|keep|eligible/i.test(value) ? "positive" : /blocked|reject|error|stopped|short|sell|degraded|invalid/i.test(value) ? "negative" : /configured|hold|pending|jev2|modify|split/i.test(value) ? "warning" : "neutral";
  return `<span class="chip" data-tone="${tone}">${escapeHtml(label)}</span>`;
}

function loopRow(loop: LoopView): string {
  const hypothesis = hypothesisFor(loop); const decision = latestDecision(loop.id); const position = positionFor(loop.id); const selected = selectedLoop()?.id === loop.id;
  return `<button class="thread-row ${selected ? "is-active" : ""}" data-loop-id="${escapeHtml(loop.id)}" type="button"><span class="thread-indicator" data-state="${escapeHtml(loop.state)}"></span><span class="thread-copy"><span class="thread-title">${escapeHtml(hypothesis?.instruments.join(" · ") || "Unassigned loop")}</span><span class="thread-meta">${escapeHtml(hypothesis?.timeframe.label || "No timeframe")} · ${escapeHtml(loop.state)}</span><span class="thread-signal">${decision ? `${escapeHtml(decision.action)} · ${(decision.confidence * 100).toFixed(0)}%` : "Waiting for Jev"}${position ? ` · ${escapeHtml(position.direction)} ${escapeHtml(position.state)}` : ""}</span></span>${loop.parentLoopId ? `<span class="spawn-mark" title="Spawned hypothesis">${icons.branch}</span>` : ""}</button>`;
}

function windowChrome(): string {
  const windowControls = harnessService.isDesktop
    ? `<div aria-label="Window controls" class="window-chrome-controls" role="group"><button aria-label="Minimize" class="window-control" id="window-minimize" title="Minimize" type="button">${icons.minimize}</button><button aria-label="Maximize" class="window-control" id="window-maximize" title="Maximize" type="button">${icons.maximize}</button><button aria-label="Close" class="window-control" id="window-close" title="Close" type="button">${icons.close}</button></div>`
    : "";
  return `<div aria-hidden="true" class="window-frame-overlay"></div><header class="window-chrome ${harnessService.isDesktop ? "" : "window-chrome-browser"}"><div aria-label="Espeon controls" class="window-chrome-actions" role="group"><button aria-label="Home" class="window-chrome-action" id="go-home" title="Home" type="button"><svg viewBox="0 0 24 24"><path d="m3 10 9-7 9 7"/><path d="M5 9v12h14V9M9 21v-7h6v7"/></svg></button><span aria-hidden="true" class="window-chrome-separator"></span><button aria-label="Toggle sidebar" aria-expanded="${!sidebarIsCollapsed()}" class="window-chrome-action" id="toggle-sidebar" title="Toggle session sidebar (Ctrl+B)" type="button">${icons.panel}</button><button aria-label="New run" class="window-chrome-action" id="new-run" title="New session (Ctrl+N)" type="button">${icons.plus}</button><span aria-hidden="true" class="window-chrome-separator"></span><button aria-label="Toggle context" aria-expanded="${!inspectorIsCollapsed()}" class="window-chrome-action" id="toggle-inspector" title="Toggle session context (Ctrl+I)" type="button">${icons.inspect}</button><button aria-label="Connectors and settings" class="window-chrome-action" id="open-settings" title="Connectors and settings" type="button">${icons.settings}</button><button aria-label="Toggle theme" class="window-chrome-action" id="toggle-theme" title="Toggle theme" type="button">${preferences.theme === "dark" ? icons.sun : icons.moon}</button></div><div class="window-chrome-drag"><span>Espeon</span></div>${windowControls}</header>`;
}

function sidebar(): string {
  const snapshot = currentSnapshot(); const loops = currentLoops(); const history = state.workspace?.runHistory ?? []; const visibleHistory = history.filter((run) => !run.archived);
  return `<aside class="sidebar ${preferences.sidebarCollapsed ? "is-collapsed" : ""}" aria-label="Run navigation"><div class="sidebar-body"><form class="sidebar-search" id="workspace-search"><span>${icons.search}</span><input id="workspace-query" value="${escapeHtml(state.searchQuery)}" placeholder="Search experiments" aria-label="Search experiments"/><kbd>Ctrl+K</kbd></form><section class="nav-section"><div class="section-label"><span class="section-name">${icons.activity}Current run</span>${snapshot ? statusChip(snapshot.status) : ""}</div>${snapshot ? `<button class="run-summary is-active" data-run-id="${escapeHtml(snapshot.runId)}"><span class="run-orb"></span><span><strong>${escapeHtml(runTitle(snapshot.runId, snapshot.thesis))}</strong><small>${loops.length} loop${loops.length === 1 ? "" : "s"} · ${snapshot.events.length} events</small></span></button>` : `<div class="sidebar-empty">No active run selected.</div>`}</section><section class="nav-section loop-section"><div class="section-label"><span class="section-name">${icons.branch}Jev loops</span><span class="count">${loops.length}</span></div><div class="thread-list">${loops.length ? loops.map(loopRow).join("") : `<div class="sidebar-empty">Loops appear after a run starts.</div>`}</div></section><section class="nav-section history-section"><div class="section-label"><span class="section-name">${icons.history}Experiment history</span><button class="section-link" id="view-all-history" type="button">View all</button></div><div class="history-list">${visibleHistory.slice(0, 8).map((run) => `<button class="history-row ${run.runId === state.selectedRunId ? "is-active" : ""}" data-run-id="${escapeHtml(run.runId)}" type="button"><span>${escapeHtml(run.thesis)}</span><small>${escapeHtml(run.status)} · ${relativeTime(run.startedAt)}</small></button>`).join("") || `<div class="sidebar-empty">Canonical run history is empty.</div>`}</div></section></div></aside><div class="resize-handle resize-sidebar" aria-hidden="true"></div>`;
}

function eventKind(event: EventView): { label: string; tone: string } {
  if (event.kind.startsWith("market_service_state_changed") || event.kind.startsWith("live_context_")) return { label: "Market data", tone: "market" };
  if (/decision|jev/i.test(event.kind)) return { label: "Jev", tone: "jev" };
  if (/execution|position|fill/i.test(event.kind)) return { label: "Execution", tone: "execution" };
  if (/guardrail|order/i.test(event.kind)) return { label: "Harness", tone: "harness" };
  if (/web|retriev|context/i.test(event.kind)) return { label: "Research", tone: "research" };
  if (/hypothesis|review|thesis/i.test(event.kind)) return { label: "World model", tone: "world" };
  return { label: "System", tone: "system" };
}

function activityLabel(event: EventView): string {
  const labels: Record<string, string> = {
    harness_worker_failed: "Harness recovery blocked",
    broker_sync_failed: "Broker reconciliation failed",
    broker_sync_degraded: "Broker snapshot incomplete",
    market_service_state_changed: "Feed status",
    live_context_resolution_failed: "Waiting for candles",
    live_context_resolved: "Live context ready",
    jev1_decision_recorded: "Jev entry decision",
    jev2_decision_recorded: "Jev position decision",
    order_evaluated: "Deterministic risk check",
    execution_recorded: "Broker execution result",
  };
  return labels[event.kind] ?? event.kind.replaceAll("_", " ");
}

function activityDescription(event: EventView): string {
  return event.detail?.trim() || event.summary?.trim() || "This event was recorded in the run activity.";
}

function routineActivity(event: EventView): boolean {
  return ["broker_state_synchronized", "live_context_resolved", "loop_cadence_mapped"].includes(event.kind);
}

function eventDetails(event: EventView): string {
  if (["harness_worker_failed", "broker_sync_failed", "broker_sync_degraded"].includes(event.kind)) {
    return `<p>${escapeHtml(event.detail || event.summary || "The run is waiting for recovery to become safe.")}</p>`;
  }
  if (event.kind === "live_context_resolution_failed") {
    return `<p>${escapeHtml(event.detail || "Required live market data is not ready. Jev was not called and no order was sent.")}</p>`;
  }
  if (event.kind === "order_evaluated" || event.kind === "guardrail_evaluated") {
    const rejected = event.detail?.startsWith("Rejected:") ?? false;
    const accepted = event.detail?.startsWith("Approved:") ?? false;
    const label = event.kind === "guardrail_evaluated"
      ? accepted ? "Risk gate passed" : rejected ? "Risk gate blocked" : "Risk result unavailable"
      : accepted ? "Order approved" : rejected ? "Order rejected" : "Risk result unavailable";
    const reason = event.detail?.replace(/^(Approved|Rejected):\s*/, "");
    return `<p><span class="chip" data-tone="${rejected ? "negative" : accepted ? "positive" : "warning"}">${label}</span> ${escapeHtml(reason || event.summary || "Risk evaluation result unavailable.")}</p>`;
  }
  if (event.kind === "market_service_state_changed" || event.kind === "live_context_resolved") {
    return event.detail ? `<p>${escapeHtml(event.detail)}</p>` : event.summary ? `<p>${escapeHtml(event.summary)}</p>` : "";
  }
  const decision = state.replay?.decisions.find((item) => item.id === event.aggregateId);
  if (decision) return `<div class="structured-line"><span>Action</span><strong>${escapeHtml(decision.action)}</strong><span>Confidence</span><strong>${(decision.confidence * 100).toFixed(0)}%</strong></div><p>${escapeHtml(decision.rationale)}</p>`;
  const execution = state.replay?.executions.find((item) => item.executionId === event.aggregateId);
  if (execution) return `<div class="structured-line"><span>Status</span><strong>${escapeHtml(execution.status)}</strong><span>Lots</span><strong>${execution.filledQuantity} @ ${execution.averagePrice ?? "—"}</strong></div>${execution.rejectionReason ? `<p>${escapeHtml(execution.rejectionReason)}</p>` : ""}`;
  const review = state.replay?.hypothesisReviews.find((item) => item.id === event.aggregateId);
  if (review) return `<div class="structured-line"><span>Action</span><strong>${escapeHtml(review.action.toUpperCase())}</strong><span>Route</span><strong>${review.routing?.escalated ? "Escalated" : "Base model"}</strong></div><p>${escapeHtml(review.diagnosis || review.rationale)}</p><p>${escapeHtml(review.continuationRationale || review.rationale)}</p>`;
  const trigger = [...(state.replay?.autonomousReviewTriggers ?? [])].reverse().find((item) => item.triggerId === event.aggregateId);
  if (trigger) return `<div class="structured-line"><span>Status</span><strong>${escapeHtml(trigger.status)}</strong><span>Reasons</span><strong>${escapeHtml(trigger.kinds.join(", ").replaceAll("_", " "))}</strong></div><p>No-trade ${trigger.noTradeStreak}/${trigger.noTradeThreshold} · losses ${trigger.consecutiveLosses}/${trigger.lossThreshold} · trades ${trigger.completedTradesSinceReview}/${trigger.periodicTradeThreshold}</p>`;
  const outcome = state.replay?.tradeOutcomes.find((item) => item.id === event.aggregateId);
  if (outcome) return `<div class="structured-line"><span>Outcome</span><strong>${escapeHtml(outcome.classification.toUpperCase())}</strong><span>Gross P&amp;L</span><strong>${outcome.grossRealizedPnl === null ? "Unknown basis" : outcome.grossRealizedPnl.toFixed(2)}</strong></div><p>${escapeHtml(outcome.instrument)} · ${outcome.quantity} · ${outcome.entryExecutionIds.length} entry fill(s) / ${outcome.exitExecutionIds.length} exit fill(s)</p>`;
  return event.summary ? `<p>${escapeHtml(event.summary)}</p>` : "";
}

type MarketPathStep = { label: string; state: "done" | "current" | "pending" | "blocked" | "skipped"; detail: string };

function marketWarmupBanner(events: EventView[]): string {
  if (currentSnapshot()?.status !== "active") return "";
  const contextEvent = [...events].reverse().find((event) => event.kind === "live_context_resolution_failed" || event.kind === "live_context_resolved");
  const pending = (label: string): MarketPathStep => ({ label, state: "pending", detail: "Waiting" });
  const steps: MarketPathStep[] = [
    { label: "Market data", state: contextEvent ? "done" : "current", detail: contextEvent ? "Fresh feed received" : "Collecting quote and candles" },
    { label: "Live context", state: contextEvent?.kind === "live_context_resolved" ? "done" : contextEvent?.kind === "live_context_resolution_failed" ? "blocked" : "pending", detail: contextEvent?.kind === "live_context_resolved" ? "Snapshot resolved" : contextEvent?.kind === "live_context_resolution_failed" ? "Snapshot unavailable" : "Waiting" },
    pending("Jev"), pending("Risk checks"), pending("cTrader FIX"),
  ];
  let stateName = "waiting";
  let badge = "PREPARING";
  let title = "Warming up market data";
  let message = "Espeon is collecting a fresh cTrader quote and the required completed candles.";

  if (contextEvent?.kind === "live_context_resolution_failed") {
    const cadence = contextEvent.loopId ? state.replay?.cadences.find((item) => item.loopId === contextEvent.loopId) : undefined;
    const seconds = cadence?.jev1IntervalSeconds;
    const retry = seconds ? `Retrying on the ${seconds >= 60 ? `${Math.round(seconds / 60)}-minute` : `${seconds}-second`} loop cycle.` : "Retrying on the next scheduled loop cycle.";
    steps[0] = { label: "Market data", state: "blocked", detail: "Feed or candles unavailable" };
    steps[1] = { label: "Live context", state: "skipped", detail: "Not resolved" };
    steps[2] = { label: "Jev", state: "skipped", detail: "Not called" };
    steps[3] = { label: "Risk checks", state: "skipped", detail: "No order" };
    steps[4] = { label: "cTrader FIX", state: "skipped", detail: "No order sent" };
    badge = "WAITING";
    title = "Waiting for market data";
    message = `${contextEvent.detail || "Required fresh market data is not available."} Jev was not called and no order was sent. ${retry}`;
  } else if (contextEvent) {
    const cycleEvents = events.filter((event) => event.sequence > contextEvent.sequence && (!contextEvent.loopId || event.loopId === contextEvent.loopId));
    const decisionEvent = [...cycleEvents].reverse().find((event) => event.kind === "jev1_decision_recorded" || event.kind === "jev2_decision_recorded");
    if (!decisionEvent) {
      steps[2] = { label: "Jev", state: "current", detail: "Waiting for decision" };
      badge = "READY";
      title = "Live context is ready for Jev";
      message = "Fresh live context is recorded. Jev is the next canonical step.";
    } else {
      const decision = state.replay?.decisions.find((item) => item.id === decisionEvent.aggregateId);
      const action = decision?.action ?? "Decision recorded";
      const noTrade = /no.?trade|hold|wait/i.test(action);
      steps[2] = { label: "Jev", state: "done", detail: action };
      if (noTrade) {
        steps[3] = { label: "Risk checks", state: "skipped", detail: "No trade requested" };
        steps[4] = { label: "cTrader FIX", state: "skipped", detail: "No order sent" };
        stateName = "complete";
        badge = "NO TRADE";
        title = "Jev chose not to trade";
        message = decision?.rationale || `Jev recorded ${action}; risk approval and broker execution were not needed.`;
      } else {
        const orderEvent = cycleEvents.find((event) => event.kind === "order_evaluated" && event.causationEventId === decisionEvent.id);
        const guardEvent = cycleEvents.find((event) => event.kind === "guardrail_evaluated" && event.aggregateId === decisionEvent.aggregateId);
        const order = state.replay?.orders.find((item) => item.decisionId === decisionEvent.aggregateId);
        const rejected = Boolean(order && (/reject|block|invalid|fail/i.test(order.status) || order.rejectionReasons.length > 0)) || Boolean(guardEvent?.detail?.startsWith("Rejected:"));
        const reason = order?.rejectionReasons.join("; ") || guardEvent?.detail?.replace(/^Rejected:\s*/, "") || "Deterministic risk policy rejected the order.";
        if (!orderEvent && !order && !guardEvent) {
          steps[3] = { label: "Risk checks", state: "current", detail: "Waiting for evaluation" };
          steps[4] = pending("cTrader FIX");
          stateName = "progress";
          badge = "IN PROGRESS";
          title = "Jev decision recorded";
          message = `Jev selected ${action}. Waiting for the deterministic risk evaluation.`;
        } else if (rejected) {
          steps[3] = { label: "Risk checks", state: "blocked", detail: reason };
          steps[4] = { label: "cTrader FIX", state: "skipped", detail: "Order blocked; not sent" };
          stateName = "blocked";
          badge = "RISK BLOCKED";
          title = "Risk checks blocked the order";
          message = reason;
        } else {
          const execution = state.replay?.executions.find((item) => item.causedByDecisionId === decisionEvent.aggregateId);
          steps[3] = { label: "Risk checks", state: "done", detail: "Order approved" };
          if (!execution) {
            steps[4] = { label: "cTrader FIX", state: "current", detail: "Waiting for broker result" };
            stateName = "progress";
            badge = "IN PROGRESS";
            title = "Order approved; awaiting cTrader FIX";
            message = "Deterministic risk checks passed. The card will update when the broker execution is recorded.";
          } else if (/reject|fail|error/i.test(execution.status) || execution.rejectionReason) {
            steps[4] = { label: "cTrader FIX", state: "blocked", detail: execution.rejectionReason || execution.status };
            stateName = "blocked";
            badge = "BROKER REJECTED";
            title = "cTrader FIX did not fill the order";
            message = execution.rejectionReason || `Broker result: ${execution.status}.`;
          } else {
            steps[4] = { label: "cTrader FIX", state: "done", detail: `${execution.status} · ${execution.filledQuantity} lots` };
            stateName = "complete";
            badge = "RECORDED";
            title = "Broker result recorded";
            message = `${execution.status}: ${execution.filledQuantity} lots${execution.averagePrice === null ? "" : ` at ${execution.averagePrice}`}.`;
          }
        }
      }
    }
  }
  return `<section class="market-warmup" data-state="${stateName}" aria-live="polite"><div class="market-warmup-heading"><span class="market-warmup-badge">${escapeHtml(badge)}</span><div><h2>${escapeHtml(title)}</h2><p>${escapeHtml(message)}</p></div></div>${marketPath(steps)}</section>`;
}

function marketPath(steps: MarketPathStep[]): string {
  return `<ol class="market-warmup-path" aria-label="Canonical activity progress">${steps.map((step, index) => `<li data-step="${step.state}" title="${escapeHtml(step.detail)}"><span>${index + 1}</span><div><strong>${escapeHtml(step.label)}</strong><small>${escapeHtml(step.detail)}</small></div></li>`).join("")}</ol>`;
}

function activityView(): string {
  const events = selectedRunSnapshot()?.events ?? [];
  if (!state.selectedRunId) return welcomeView();
  if (!events.length) return `<div class="empty-state"><span class="empty-icon">${icons.activity}</span><h2>Waiting for canonical activity</h2><p>World-model, Jev, harness, and execution events will appear here.</p></div>`;
  const orderedEvents = [...events].reverse();
  return `<div class="activity-scroll-layout"><nav class="activity-navigator" id="activity-navigator" role="group" aria-label="Activity navigator. Select a marker or use arrow keys to jump through events." aria-controls="workbench-scroll" tabindex="0"><ol class="activity-navigator-track">${orderedEvents.map((event, index) => { const kind = eventKind(event); const routine = routineActivity(event); const label = activityLabel(event); const description = activityDescription(event); return `<li class="activity-mark-row" data-activity-index="${index}"><button class="activity-mark ${routine ? "is-routine" : "is-major"}" data-activity-index="${index}" type="button" aria-label="Jump to ${escapeHtml(kind.label)}: ${escapeHtml(label)}. ${escapeHtml(description)}"><span class="activity-mark-line" aria-hidden="true"></span><span class="activity-mark-preview" role="tooltip"><strong>${escapeHtml(label)}</strong><span>${escapeHtml(description)}</span></span></button></li>`; }).join("")}</ol></nav><div class="activity-feed" id="activity-feed">${marketWarmupBanner(events)}<div class="activity-stream">${orderedEvents.map((event, index) => { const kind = eventKind(event); return `<article class="activity-card ${routineActivity(event) ? "is-routine" : ""}" data-activity-index="${index}" data-tone="${kind.tone}"><div class="activity-content"><div class="activity-header"><span class="activity-source">${escapeHtml(kind.label)}</span><span class="activity-kind">${escapeHtml(activityLabel(event))}</span><time>${dateTime(event.occurredAt)}</time></div>${eventDetails(event)}<div class="canonical-ref" title="Canonical event sequence #${event.sequence}"><span>${escapeHtml(event.aggregateType)}</span><span>#${events.length - index}</span><code>${escapeHtml(shortId(event.id))}</code></div></div></article>`; }).join("")}</div></div></div>`;
}

function positionRows(): string {
  const positions = state.replay?.positions ?? [];
  return positions.map((position) => {
    const execution = state.replay?.executions.find((item) => item.executionId === position.openedByExecutionId);
    const order = state.replay?.orders.find((item) => item.decisionId === execution?.causedByDecisionId);
    const realizedPnl = (state.replay?.tradeOutcomes ?? []).filter((item) => item.positionId === position.id && item.grossRealizedPnl !== null).reduce((sum, item) => sum + item.grossRealizedPnl!, 0);
    const hasKnownPnl = (state.replay?.tradeOutcomes ?? []).some((item) => item.positionId === position.id && item.grossRealizedPnl !== null);
    return `<tr><td><strong>${escapeHtml(order?.instrument || "—")}</strong><small>${escapeHtml(position.direction)}</small></td><td>${statusChip(position.state)}</td><td class="mono">${order?.quantity ?? execution?.filledQuantity ?? "—"}</td><td class="mono">${execution?.averagePrice ?? order?.referencePrice ?? "—"}</td><td class="mono ${hasKnownPnl ? realizedPnl < 0 ? "pnl-negative" : "pnl-positive" : ""}">${hasKnownPnl ? realizedPnl.toFixed(2) : "—"}</td><td><button class="text-button" data-loop-id="${escapeHtml(position.loopId)}">${escapeHtml(shortId(position.loopId))}</button></td><td>${escapeHtml(execution?.status || "—")}</td></tr>`;
  }).join("");
}

function positionsView(): string {
  const executions = state.replay?.executions ?? [];
  return `<div class="table-section"><div class="content-section-heading"><div><h2>Positions & execution</h2></div><span>${state.replay?.positions.length ?? 0} positions</span></div>${renderPositionsChart(state.replay?.tradeOutcomes ?? [], pnlRange)}<div class="table-wrap"><table><thead><tr><th>Instrument</th><th>State</th><th>Lots</th><th>Entry</th><th>P&amp;L</th><th>Origin loop</th><th>Broker</th></tr></thead><tbody>${positionRows() || `<tr><td colspan="7" class="table-empty">No positions recorded for this run.</td></tr>`}</tbody></table></div><div class="execution-list"><h3>Execution ledger</h3>${[...executions].reverse().map((item) => `<article class="execution-row"><div>${statusChip(item.status)}<strong>${escapeHtml(item.executionKind)}</strong><span>${escapeHtml(item.action)}</span></div><div><span>${item.filledQuantity} lots @ ${item.averagePrice ?? "—"}</span><code>${escapeHtml(shortId(item.executionId))}</code><time>${dateTime(item.executedAt)}</time></div>${item.rejectionReason ? `<p>${escapeHtml(item.rejectionReason)}</p>` : ""}</article>`).join("") || `<p class="muted">No broker executions yet.</p>`}</div></div>`;
}

function evidenceCard(record: ContextRecord): string {
  return `<button class="evidence-card ${selectedEvidence()?.id === record.id ? "is-selected" : ""}" data-evidence-id="${escapeHtml(record.id)}" type="button"><div><span class="source-class">${escapeHtml(record.sourceClass.replaceAll("_", " "))}</span>${statusChip(record.trustLevel)}</div><strong>${escapeHtml(record.title)}</strong><p>${escapeHtml(record.text)}</p><footer><span>${escapeHtml(record.publisher)}</span><time>${dateTime(record.observedAt)}</time><code>${escapeHtml(shortId(record.canonicalEntityId))}</code></footer></button>`;
}

function evidenceView(): string {
  const records = state.replay?.contextPoolRecords ?? [];
  return `<div class="evidence-view"><div class="content-section-heading"><div><h2>Evidence & provenance</h2></div><span>${records.length} canonical records</span></div><form class="content-search" id="evidence-search"><span>${icons.search}</span><input id="evidence-query" value="${escapeHtml(state.searchQuery)}" placeholder="Search exact IDs, prior experiments, or semantic context"/><button type="submit">Search</button></form>${state.searchResults.length ? `<section class="search-results"><h3>Cross-run search results</h3>${state.searchResults.map((hit) => `<button class="search-hit" data-evidence-id="${escapeHtml(hit.canonicalEntityId)}"><span>${escapeHtml(hit.canonicalEntityType)}</span><p>${escapeHtml(hit.text)}</p><code>${escapeHtml(shortId(hit.canonicalEntityId))}</code></button>`).join("")}</section>` : ""}<div class="evidence-grid">${records.map(evidenceCard).join("") || `<div class="empty-state compact"><h3>No indexed evidence in this run</h3><p>Context records appear after retrieval, research, and canonical indexing.</p></div>`}</div></div>`;
}

function historyView(): string {
  const allRuns = state.workspace?.runHistory ?? [];
  const runs = allRuns.filter((run) => run.archived === state.showArchived);
  return `<div class="history-view"><div class="content-section-heading"><div><h2>Experiments</h2></div><div class="history-filters" role="group" aria-label="Filter experiments"><button class="${state.showArchived ? "" : "is-active"}" aria-pressed="${!state.showArchived}" data-history-filter="recent" type="button">Recent</button><button class="${state.showArchived ? "is-active" : ""}" aria-pressed="${state.showArchived}" data-history-filter="archived" type="button">Archived</button></div></div><div class="history-table">${runs.map((run) => `<article class="history-card"><button class="history-card-open" data-open-run="${escapeHtml(run.runId)}" type="button"><span class="history-state" data-state="${escapeHtml(run.status)}"></span><span class="history-card-copy"><strong>${escapeHtml(run.thesis)}</strong><small>${dateTime(run.startedAt)} · ${escapeHtml(run.status)}</small></span><code>${escapeHtml(shortId(run.runId))}</code>${icons.chevron}</button><div class="history-card-actions"><button data-rename-run="${escapeHtml(run.runId)}" type="button" title="Rename experiment">${icons.rename}<span>Rename</span></button><button data-archive-run="${escapeHtml(run.runId)}" data-archived="${run.archived}" type="button" title="${run.status === "active" ? "Stop the experiment before archiving" : run.archived ? "Restore experiment" : "Archive experiment"}" ${run.status === "active" ? "disabled" : ""}>${run.archived ? icons.restoreArchive : icons.archive}<span>${run.archived ? "Restore" : "Archive"}</span></button></div></article>`).join("") || `<div class="empty-state compact"><h3>${state.showArchived ? "No archived experiments" : "No experiments yet"}</h3></div>`}</div></div>`;
}

function welcomeView(): string {
  return `<div class="welcome"><h1>What hypothesis should the harness test?</h1><p>Describe the market behavior in plain language. The world model will create a versioned hypothesis, select context, and start a supervised Jev loop.</p><div class="welcome-notes"><span>Rust-authoritative state</span><span>No per-trade approvals</span><span>Full canonical replay</span></div></div>`;
}

function homeView(): string {
  const recent = (state.workspace?.runHistory ?? []).filter((run) => !run.archived).slice(0, 5);
  return `<section class="home-page"><div class="home-intro"><span class="home-eyebrow">ESPEON</span><h1>What are we testing?</h1><p>Describe a trading hypothesis or market behavior to start a session.</p></div><form class="composer home-composer" id="run-composer"><div class="composer-shell"><textarea id="thesis-input" rows="2" placeholder="Describe a trading hypothesis or vague market behavior…" ${state.loading ? "disabled" : ""}>${escapeHtml(promptDraft)}</textarea><div class="composer-footer"><div><span class="composer-mode">Autonomous</span><span class="composer-detail">World model → Jev → deterministic harness</span></div><button class="send-button" type="submit" title="Start run (Ctrl+Enter)">${state.loading ? `<span class="spinner small"></span>` : icons.arrow}</button></div></div></form><section class="home-recent"><header><h2>Recent sessions</h2><button class="section-link" id="view-all-home" type="button">View all ${icons.chevron}</button></header><div class="home-session-list">${recent.map((run) => `<button class="home-session" data-open-run="${escapeHtml(run.runId)}" type="button"><span class="history-state" data-state="${escapeHtml(run.status)}"></span><span><strong>${escapeHtml(run.thesis)}</strong><small>${escapeHtml(run.status)} · ${relativeTime(run.startedAt)}</small></span>${icons.chevron}</button>`).join("") || `<p class="home-empty">Your recent trading sessions will appear here.</p>`}</div></section></section>`;
}

function tabContent(): string {
  if (state.loading) return `<div class="loading-view"><span class="spinner"></span><p>Hydrating canonical workspace…</p></div>`;
  if (state.tab === "positions") return positionsView();
  if (state.tab === "evidence") return evidenceView();
  if (state.tab === "history") return historyView();
  return activityView();
}

function mainPanel(): string {
  if (state.tab === "home") return `<main class="workbench-main home-mode"><section class="workbench-scroll home-scroll">${homeView()}</section></main>`;
  if (state.tab === "history") return `<main class="history-page"><section class="history-scroll">${historyView()}</section></main>`;
  const snapshot = currentSnapshot(); const historyRun = state.workspace?.runHistory.find((run) => run.runId === state.selectedRunId); const title = historyRun?.thesis || snapshot?.thesis || "New autonomous run";
  const stopping = snapshot !== null && state.stoppingRunId === snapshot.runId;
  const editingTitle = state.editingTitleRunId === state.selectedRunId && Boolean(state.selectedRunId);
  const titleView = editingTitle
    ? `<form class="run-title-edit" id="run-title-form"><input id="run-title-input" name="name" value="${escapeHtml(title)}" maxlength="120" required autocomplete="off" aria-label="Trade title"/><button type="submit" class="icon-button" title="Save title" aria-label="Save title">${icons.check}</button><button type="button" class="icon-button" id="cancel-title-edit" title="Cancel" aria-label="Cancel title edit">${icons.close}</button></form>`
    : `<h1 id="run-title" ${state.selectedRunId ? 'title="Double-click to rename"' : ""}>${escapeHtml(title)}</h1>${state.selectedRunId ? `<button class="title-edit-button" id="edit-run-title" type="button" aria-label="Rename trade" title="Rename trade">${icons.rename}</button>` : ""}`;
  const activeRun = currentSnapshot() !== null;
  const continuationLabel = activeRun ? "Steering active run · delivered at next Jev decision" : state.selectedRunId ? `Continuing from ${shortId(state.selectedRunId)} · history informs a new run` : "World model → Jev → deterministic harness";
  const composerPlaceholder = activeRun ? "Steer the active run…" : state.selectedRunId ? "Continue with a new instruction using this run as context…" : "Describe a trading hypothesis or vague market behavior…";
  const composerAction = activeRun ? "Steer active run" : state.selectedRunId ? "Continue from this run" : "Start run";
  return `<main class="workbench-main"><header class="workbench-header"><div class="header-title"><div class="run-title-wrap">${titleView}</div></div><div class="header-actions"><nav class="tabs" aria-label="Run views" role="group">${(["activity", "positions", "evidence"] as const).map((tab) => `<button type="button" data-tab="${tab}" aria-pressed="${state.tab === tab}" class="${state.tab === tab ? "is-active" : ""}">${icons[tab === "positions" ? "position" : tab]}<span>${tab[0].toUpperCase() + tab.slice(1)}</span>${tab === "activity" && selectedRunSnapshot() ? `<em>${selectedRunSnapshot()!.events.length}</em>` : ""}</button>`).join("")}</nav>${snapshot ? `<button class="secondary-button danger" id="stop-run" type="button" ${stopping ? "disabled" : ""}>${stopping ? `<span class="spinner small"></span>` : icons.stop}<span>${stopping ? "Stopping…" : "Stop run"}</span></button>` : ""}</div></header>${state.notice ? `<div class="notice" data-tone="${state.noticeTone}"><span>${escapeHtml(state.notice)}</span><button aria-label="Dismiss notice" id="dismiss-notice" type="button">${icons.close}</button></div>` : ""}<section class="workbench-scroll" id="workbench-scroll">${tabContent()}</section><form class="composer" id="run-composer"><div class="composer-shell"><textarea id="thesis-input" rows="2" placeholder="${composerPlaceholder}" ${state.loading ? "disabled" : ""}>${escapeHtml(promptDraft)}</textarea><div class="composer-footer"><div><span class="composer-mode">Autonomous</span><span class="composer-detail">${escapeHtml(continuationLabel)}</span></div><button class="send-button" type="submit" title="${composerAction} (Ctrl+Enter)">${state.loading ? `<span class="spinner small"></span>` : icons.arrow}</button></div></div></form></main>`;
}

function definitionRow(label: string, value: unknown, mono = false): string { return `<div class="definition-row"><dt>${escapeHtml(label)}</dt><dd class="${mono ? "mono" : ""}">${escapeHtml(value ?? "—")}</dd></div>`; }

function liveReviewMetrics(loop: LoopView): { noTrade: number; noTradeThreshold: number; losses: number; trades: number } {
  const completed = [...(state.replay?.autonomousReviewTriggers ?? [])].reverse().find((item) => item.loopId === loop.id && item.status === "completed");
  const checkpoint = completed ? new Date(completed.createdAt).getTime() : 0;
  const decisions = (state.replay?.decisions ?? []).filter((item) => item.loopId === loop.id && item.stage === "jev1" && new Date(item.createdAt).getTime() > checkpoint);
  let noTrade = 0;
  for (const decision of [...decisions].reverse()) {
    if (decision.action.toLowerCase() === "notrade") { noTrade += 1; continue; }
    const order = state.replay?.orders.find((item) => item.decisionId === decision.id);
    if (order?.status === "rejected" && order.rejectionReasons.length === 1 && order.rejectionReasons[0].includes("confidence")) { noTrade += 1; continue; }
    if (decision.confidence >= 0.65) break;
  }
  const outcomes = (state.replay?.tradeOutcomes ?? []).filter((item) => item.loopId === loop.id && item.classification !== "unknown" && new Date(item.completedAt).getTime() > checkpoint);
  let losses = 0;
  for (const outcome of [...outcomes].reverse()) { if (outcome.classification !== "loss") break; losses += 1; }
  const cadence = (state.replay?.cadences ?? []).find((item) => item.loopId === loop.id) as { timeframeHorizonMinutes?: number; jev1IntervalSeconds?: number } | undefined;
  const rawThreshold = Math.ceil(((cadence?.timeframeHorizonMinutes ?? 60) * 60) / Math.max(1, cadence?.jev1IntervalSeconds ?? 300));
  return { noTrade, noTradeThreshold: Math.max(6, Math.min(20, rawThreshold)), losses, trades: outcomes.length };
}

function inspector(): string {
  const loop = selectedLoop(); const hypothesis = hypothesisFor(loop); const decision = loop ? latestDecision(loop.id) : null; const position = loop ? positionFor(loop.id) : null; const evidence = selectedEvidence();
  const allocation = loop ? [...(state.replay?.capitalAllocations ?? [])].reverse().flatMap((item) => item.allocations).find((item) => item.loopId === loop.id) : null;
  const context = state.replay?.contextVersions.find((item) => item.id === loop?.contextVersionId);
  const reviewTrigger = loop ? [...(state.replay?.autonomousReviewTriggers ?? [])].reverse().find((item) => item.loopId === loop.id) : null;
  const review = reviewTrigger?.reviewId ? state.replay?.hypothesisReviews.find((item) => item.id === reviewTrigger.reviewId) : null;
  const metrics = loop ? liveReviewMetrics(loop) : null;
  const reviewSection = loop && metrics ? `<section class="inspector-section"><h3>Autonomous review</h3>${reviewTrigger ? `<div class="decision-callout"><div><strong>${escapeHtml(reviewTrigger.status)}</strong><span>${escapeHtml(reviewTrigger.kinds.join(" · ").replaceAll("_", " "))}</span></div>${review ? `<p>${escapeHtml(review.diagnosis || review.rationale)}</p>` : `<p>Deterministic counters are tracking the next model review.</p>`}</div>` : `<p class="muted">Monitoring canonical Jev decisions and completed broker round trips.</p>`}<dl>${definitionRow("No-trade", `${metrics.noTrade} / ${reviewTrigger?.noTradeThreshold ?? metrics.noTradeThreshold}`)}${definitionRow("Consecutive losses", `${metrics.losses} / ${reviewTrigger?.lossThreshold ?? 3}`)}${definitionRow("Completed trades", `${metrics.trades} / ${reviewTrigger?.periodicTradeThreshold ?? 10}`)}${definitionRow("Review state", reviewTrigger?.status ?? "monitoring")}${definitionRow("Model route", review?.routing ? (review.routing.escalated ? `Escalated · ${review.routing.selectedModel}` : `Base · ${review.routing.selectedModel}`) : "—")}${definitionRow("Action", review?.action?.toUpperCase())}${definitionRow("Severity", review?.problemSeverity)}</dl></section>` : "";
  const evidenceContent = evidence ? `<section class="inspector-section"><div class="source-heading">${statusChip(evidence.trustLevel)}<span>${escapeHtml(evidence.sourceClass)}</span></div><h3>${escapeHtml(evidence.title)}</h3><p class="inspector-copy">${escapeHtml(evidence.text)}</p>${evidence.provenanceUri.startsWith("http") ? `<a class="source-link" href="${escapeHtml(evidence.provenanceUri)}" target="_blank" rel="noreferrer">Open source ${icons.external}</a>` : ""}</section><section class="inspector-section"><h3>Provenance</h3><dl>${definitionRow("Publisher", evidence.publisher)}${definitionRow("Observed", dateTime(evidence.observedAt))}${definitionRow("Retrieved", dateTime(evidence.ingestedAt))}${definitionRow("Canonical ID", evidence.canonicalEntityId, true)}${definitionRow("Event ID", evidence.canonicalEventId, true)}</dl></section><section class="inspector-section"><h3>Metadata</h3><pre>${escapeHtml(JSON.stringify(evidence.metadata, null, 2))}</pre></section>` : "";
  const loopContent = loop && hypothesis ? `<section class="inspector-section hero-detail"><div class="loop-title"><span class="loop-state-dot"></span><div><h3>${escapeHtml(hypothesis.instruments.join(" · "))}</h3><p>${loop.parentLoopId ? "Spawned hypothesis" : "Original hypothesis"}</p></div>${statusChip(loop.state)}</div></section><section class="inspector-section"><h3>Hypothesis</h3><p class="inspector-copy">${escapeHtml(hypothesis.strategyMechanism)}</p><dl>${definitionRow("Timeframe", hypothesis.timeframe.label)}${definitionRow("Thesis version", `v${loop.thesisVersion}`)}${definitionRow("Context version", `v${loop.contextVersion}`)}${definitionRow("Loop ID", loop.id, true)}</dl></section><section class="inspector-section"><h3>Latest Jev decision</h3>${decision ? `<div class="decision-callout"><div><strong>${escapeHtml(decision.action)}</strong><span>${(decision.confidence * 100).toFixed(0)}% confidence</span></div><p>${escapeHtml(decision.rationale)}</p></div>` : `<p class="muted">No Jev decision recorded.</p>`}<dl>${definitionRow("Stage", decision?.stage)}${definitionRow("Position", position ? `${position.direction} · ${position.state}` : "Flat")}${definitionRow("Capital allocation", allocation ? `${(allocation.fraction * 100).toFixed(1)}%` : `${(loop.allocatedFraction * 100).toFixed(1)}%`)}</dl></section>${liveContextSection(decision)}${reviewSection}<section class="inspector-section"><h3>Selected context</h3>${context?.items.map((item) => `<article class="context-item"><strong>${escapeHtml(item.source.replaceAll("_", " "))}</strong><span>${escapeHtml(item.content)}</span><time>Observed ${dateTime(item.observedAt)}</time><details class="context-technical"><summary>Source record</summary><dl>${definitionRow("Source type", item.source, true)}${definitionRow("Source ID", item.sourceId, true)}</dl></details></article>`).join("") || `<p class="muted">No context items available.</p>`}</section>` : "";
  const empty = `<section class="empty-inspector"><span>${icons.inspect}</span><h3>Select a Jev loop</h3><p>Loop thesis, context, Jev state, position, confidence, and capital allocation will appear here.</p></section>`;
  return `<div class="resize-handle resize-inspector" aria-hidden="true"></div><aside class="inspector ${preferences.inspectorCollapsed ? "is-collapsed" : ""}"><div class="inspector-header"><div><h2>${evidence ? "Evidence record" : loop ? "Selected Jev loop" : "Run context"}</h2></div><button class="icon-button" id="close-inspector" type="button" aria-label="Close context">${icons.close}</button></div><div class="inspector-scroll">${evidenceContent || loopContent || empty}</div></aside>`;
}

function settingsModal(): string {
  if (!state.settingsOpen) return "";
  const settings = state.connectorSettings;
  const pages: { id: string; label: string; title: string; description: string; sections?: string[] }[] = [
    { id: "overview", label: "Quick setup", title: "Quick setup", description: "Choose Espeon’s providers. Open cTrader to set up MCP and Open API together." },
    { id: "models", label: "AI models", title: "AI models", description: "Configure the world model and independent Jev decision engine.", sections: ["world-model", "jev"] },
    { id: "ctrader", label: "cTrader", title: "cTrader hybrid connection", description: "MCP supplies account context; Open API supplies broker historical bars and tick volume. FIX remains the execution session.", sections: ["ctrader-mcp", "ctrader-open-api"] },
    { id: "execution", label: "Execution & risk", title: "Execution & risk", description: "Review live order readiness and configure FIX price and trade sessions.", sections: ["fix-common", "fix-price", "fix-trade"] },
    { id: "advanced", label: "Advanced", title: "Advanced settings", description: "Optional market data, review policy, and update access.", sections: ["twelve-data", "autonomous-review", "updates"] },
  ];
  const page = pages.find((item) => item.id === settingsPage) ?? pages[0];
  const adapterPanel = !settings ? `<div class="settings-loading"><span class="spinner"></span><p>Loading local connector configuration…</p></div>` : `<section class="settings-adapters"><label><span>World model</span><select name="worldModelAdapter"><option value="openrouter" ${settings.worldModelAdapter === "openrouter" ? "selected" : ""}>OpenRouter</option><option value="simulated" ${settings.worldModelAdapter === "simulated" ? "selected" : ""}>Simulated</option></select></label><label><span>Jev engine</span><select name="jevAdapter"><option value="typesafe" ${settings.jevAdapter === "typesafe" ? "selected" : ""}>TypeSafe Jev</option><option value="simulated" ${settings.jevAdapter === "simulated" ? "selected" : ""}>Simulated</option></select></label><label><span>Execution broker</span><select name="brokerAdapter"><option value="ctrader-fix" ${settings.brokerAdapter === "ctrader-fix" ? "selected" : ""}>cTrader FIX</option><option value="simulated" ${settings.brokerAdapter === "simulated" ? "selected" : ""}>Simulated</option></select></label><p>Provider choices apply after restarting Espeon.</p></section>`;
  const visibleFields = (section: ConnectorSection) => section.fields.filter((field) => ![
    "CTRADER_MCP_ENVIRONMENT",
    "CTRADER_MARKET_QUOTE_MAX_AGE_SECONDS",
    "CTRADER_MARKET_HISTORY_DEPTH",
    "CTRADER_FIX_HEARTBEAT_SECONDS",
    "CTRADER_FIX_TIMEOUT_SECONDS",
    "CTRADER_FIX_RECONNECT_ATTEMPTS",
    "CTRADER_FIX_PRICE_SENDER_SUB_ID",
    "CTRADER_FIX_PRICE_TARGET_COMP_ID",
    "CTRADER_FIX_PRICE_TARGET_SUB_ID",
    "CTRADER_FIX_TRADE_SENDER_SUB_ID",
    "CTRADER_FIX_TRADE_TARGET_COMP_ID",
    "CTRADER_FIX_TRADE_TARGET_SUB_ID",
  ].includes(field.key));
  const sections = settings && page.sections ? settings.sections.filter((section) => page.sections?.includes(section.id)).map((section) => {
    const fields = visibleFields(section);
    return `<section class="connector-section"><header><div>${section.title === page.title ? "" : `<h3>${escapeHtml(section.title)}</h3>`}<p>${escapeHtml(section.description)}</p></div><span>${fields.filter((field) => field.configured).length}/${fields.length} set</span></header><div class="connector-fields">${fields.map((field) => field.kind === "boolean"
      ? `<label class="connector-field connector-field-switch"><span>${escapeHtml(field.label)}</span><span class="connector-switch"><input name="${escapeHtml(field.key)}" type="checkbox" role="switch" aria-label="${escapeHtml(field.label)}" ${field.value === "true" ? "checked" : ""}/><span class="connector-switch-track"></span><i>${field.value === "true" ? "On" : "Off"}</i></span></label>`
      : `<label class="connector-field"><span>${escapeHtml(field.label)}${field.required ? `<em>required</em>` : ""}</span><div><input name="${escapeHtml(field.key)}" type="${field.kind === "secret" ? "password" : "text"}" value="${escapeHtml(field.value)}" placeholder="${escapeHtml(field.placeholder)}" autocomplete="off" spellcheck="false"/><i data-configured="${field.configured}">${field.configured ? "Stored" : "Not set"}</i></div></label>`).join("")}</div></section>`;
  }).join("") : "";
  const openApiSection = settings?.sections.find((section) => section.id === "ctrader-open-api");
  const openApiFieldSet = new Set(openApiSection?.fields.filter((field) => field.configured).map((field) => field.key) ?? []);
  const openApiReady = openApiFieldSet.has("CTRADER_OPEN_API_CLIENT_ID")
    && openApiFieldSet.has("CTRADER_OPEN_API_CLIENT_SECRET")
    && (openApiFieldSet.has("CTRADER_OPEN_API_ACCESS_TOKEN") || openApiFieldSet.has("CTRADER_OPEN_API_REFRESH_TOKEN"));
  const pageBody = page.id === "overview" ? adapterPanel
    : page.id === "ctrader" ? `<div class="settings-update" data-check-state="${mcpChecking ? "checking" : mcpProbeMessage.startsWith("Connection check failed") ? "error" : mcpProbeMessage ? "result" : "idle"}"><div><strong>MCP account lookup</strong><span role="status" aria-live="polite">${escapeHtml(mcpProbeMessage || "Uses the active cTrader Desktop session for account and symbol details.")}</span></div><button class="secondary-button" id="probe-mcp" type="button" ${mcpChecking ? "disabled" : ""}>${mcpChecking ? "Checking…" : "Check MCP"}</button></div><div class="settings-update" data-check-state="${openApiReady ? "result" : "warning"}"><div><strong>Open API market data</strong><span>${openApiReady ? "Client credentials and an authorized token are saved. Espeon discovers the account, symbol IDs, lot size, and min/step/max trade volumes automatically." : "Optional. Add Client ID, Client secret, and an access or refresh token to enable cTrader historical bars and tick volume. Espeon discovers the account, symbol IDs, lot size, and min/step/max trade volumes automatically."}</span></div></div>${sections}`
    : page.id === "execution" ? `${settings ? riskReadinessPanel() : ""}${sections}`
    : page.id === "updates" ? `<div class="settings-update"><div><strong>Espeon updates</strong><span>${escapeHtml(updateStatus?.message ?? "Checks automatically on launch and every six hours.")}</span></div><button class="secondary-button" id="check-updates" type="button" ${updateChecking ? "disabled" : ""}>${updateChecking ? "Checking…" : "Check now"}</button></div>${sections}`
    : page.id === "advanced" ? `<div class="settings-update"><div><strong>Espeon updates</strong><span>${escapeHtml(updateStatus?.message ?? "Checks automatically on launch and every six hours.")}</span></div><button class="secondary-button" id="check-updates" type="button" ${updateChecking ? "disabled" : ""}>${updateChecking ? "Checking…" : "Check now"}</button></div>${sections}`
    : sections;
  const content = `<form id="connector-settings-form" class="settings-form"><div class="settings-layout"><nav class="settings-nav" aria-label="Settings categories">${pages.map((item) => `<button type="button" data-settings-page="${item.id}" class="${item.id === page.id ? "is-active" : ""}" aria-current="${item.id === page.id ? "page" : "false"}">${item.label}</button>`).join("")}</nav><div class="settings-scroll"><div class="settings-page-heading"><h3>${page.title}</h3><p>${page.description}</p></div><div class="settings-page-content">${pageBody}</div></div></div><footer class="settings-actions"><p>Settings are stored locally. Connector changes apply after restarting Espeon.</p><div><button class="secondary-button" id="cancel-settings" type="button">Cancel</button><button class="primary-button" type="submit" ${state.settingsSaving ? "disabled" : ""}>${state.settingsSaving ? "Saving…" : "Save settings"}</button></div></footer></form>`;
  return `<div class="settings-backdrop" id="settings-backdrop"><section class="settings-dialog" role="dialog" aria-modal="true" aria-labelledby="settings-title"><header class="settings-header"><div><h2 id="settings-title">Settings</h2><p>Connect and configure Espeon’s providers.</p></div><button class="icon-button" id="close-settings" type="button" aria-label="Close settings" title="Close">${icons.close}</button></header>${content}</section></div>`;
}

function riskReadinessPanel(): string {
  const settings = state.connectorSettings;
  if (!settings) {
    return `<div id="order-risk-readiness" class="settings-update" data-check-state="checking"><div><strong>Order risk gate</strong><span role="status" aria-live="polite">Loading broker risk readiness…</span></div></div>`;
  }
  const simulated = settings.brokerAdapter === "simulated";
  const selected = state.selectedSnapshot;
  const active = selected?.status === "active" && !!state.selectedRunId;
  const fieldValue = (key: string) => settings.sections.flatMap((section) => section.fields).find((field) => field.key === key)?.value ?? "";
  const instrument = selected?.hypotheses[0]?.instruments[0] ?? "";
  const isDemo = fieldValue("CTRADER_FIX_TRADE_SENDER_COMP_ID").split(".")[0].trim().toLowerCase() === "demo";
  const riskNote = isDemo
    ? "Espeon reads this symbol’s lot size and allowed quantity increments from cTrader Open API. Enter the current Demo account risk values; they authorize one cycle only."
    : "Espeon reads this symbol’s lot size and allowed quantity increments from cTrader Open API. Enter the current account risk values; they authorize one cycle only.";
  const confirmation = isDemo
    ? "I verified the active Demo account and approve one Jev cycle using cTrader Open API symbol limits."
    : `I checked this account in cTrader and confirm these current account values for ${escapeHtml(instrument || "the active symbol")}.`;
  const approval = !simulated
    ? `<form id="human-live-risk-form" class="manual-risk-approval"><strong>Human-verify one ${isDemo ? "Demo" : "live"} decision cycle</strong><p>${riskNote}</p><div class="manual-risk-identity"><label><span>Configured cTrader account</span><input name="accountId" value="${escapeHtml(fieldValue("CTRADER_MCP_ACCOUNT_ID"))}" readonly/></label><label><span>Environment</span><input name="environment" value="${escapeHtml(fieldValue("CTRADER_MCP_ENVIRONMENT"))}" readonly/></label><label><span>Active instrument</span><input name="instrument" value="${escapeHtml(instrument)}" readonly/></label></div><div class="manual-risk-grid"><label><span>Account equity</span><input name="equity" type="number" min="0.000001" step="any" required/></label><label><span>Free margin</span><input name="freeMargin" type="number" min="0" step="any" required/></label><label><span>Deposit currency</span><input name="depositCurrency" type="text" maxlength="8" placeholder="EUR" required/></label><label><span>Total open exposure</span><input name="accountOpenExposure" type="number" min="0" step="any" required/></label><label><span>${escapeHtml(instrument || "Symbol")} quote to deposit rate</span><input name="quoteToDeposit" type="number" min="0.000001" step="any" placeholder="1.0 when currencies match" required/></label></div><label class="manual-risk-confirm"><input name="confirmed" type="checkbox" required/><span>${confirmation}</span></label><button class="primary-button" type="submit" ${!active || liveRiskApprovalBusy ? "disabled" : ""}>${liveRiskApprovalBusy ? "Running one cycle…" : "Verify values and run one cycle"}</button><span class="manual-risk-result" role="status" aria-live="polite">${escapeHtml(liveRiskApprovalMessage || (active ? "This runs one immediate Jev decision cycle; it does not lower the confidence threshold." : "Select an active run before verifying a live cycle."))}</span></form>`
    : "";
  return `<div id="order-risk-readiness" class="settings-update" data-check-state="${simulated ? "result" : "warning"}"><div><strong>Order risk gate</strong><span role="status" aria-live="polite">${escapeHtml(settings.riskReadiness)}</span></div></div>${approval}`;
}

function renameModal(): string {
  if (!state.renamingRunId) return "";
  const run = state.workspace?.runHistory.find((item) => item.runId === state.renamingRunId);
  if (!run) return "";
  return `<div class="rename-backdrop" id="rename-backdrop"><section class="rename-dialog" role="dialog" aria-modal="true" aria-labelledby="rename-title"><header><h2 id="rename-title">Rename experiment</h2><button class="icon-button" id="close-rename" type="button" aria-label="Close">${icons.close}</button></header><form id="rename-run-form"><label for="run-name">Name</label><input id="run-name" name="name" value="${escapeHtml(run.thesis)}" maxlength="120" required autocomplete="off"/><footer><button class="secondary-button" id="cancel-rename" type="button">Cancel</button><button class="primary-button" type="submit">Save</button></footer></form></section></div>`;
}

function render(): void {
  const activeSettingsInput = document.activeElement instanceof HTMLInputElement
    && document.activeElement.closest("#connector-settings-form")
    ? document.activeElement
    : null;
  const activeSettingsName = activeSettingsInput?.name ?? null;
  const activeSettingsSelection = activeSettingsInput && activeSettingsInput.type !== "password"
    ? [activeSettingsInput.selectionStart, activeSettingsInput.selectionEnd] as const
    : null;
  const settingsScrollTop = document.querySelector<HTMLElement>(".settings-scroll")?.scrollTop ?? 0;
  document.documentElement.dataset.theme = preferences.theme;
  document.documentElement.classList.toggle("is-tauri", harnessService.isDesktop);
  document.documentElement.style.setProperty("--sidebar-width", `${sidebarIsCollapsed() ? 0 : preferences.sidebarWidth}px`);
  document.documentElement.style.setProperty("--inspector-width", `${inspectorIsCollapsed() ? 0 : preferences.inspectorWidth}px`);
  app.innerHTML = `${windowChrome()}<div class="app-shell ${state.tab === "home" ? "home-state" : ""} ${sidebarIsCollapsed() ? "sidebar-collapsed" : ""} ${inspectorIsCollapsed() ? "inspector-collapsed" : ""} ${state.tab === "history" ? "history-open" : ""}">${sidebarIsCollapsed() ? "" : `<button class="sidebar-scrim" id="sidebar-scrim" type="button" aria-label="Close navigation"></button>`}${sidebar()}${mainPanel()}${inspector()}</div>${settingsModal()}${renameModal()}`;
  renderIcons(app);
  bindInteractions();
  bindWindowChrome();
  if (state.settingsOpen) {
    const settingsScroll = document.querySelector<HTMLElement>(".settings-scroll");
    if (settingsScroll) settingsScroll.scrollTop = settingsScrollTop;
    if (activeSettingsName) {
      const input = document.querySelector<HTMLInputElement>(`#connector-settings-form input[name="${CSS.escape(activeSettingsName)}"]`);
      input?.focus({ preventScroll: true });
      if (input && activeSettingsSelection?.[0] !== null && activeSettingsSelection?.[0] !== undefined && activeSettingsSelection[1] !== null) {
        input.setSelectionRange(activeSettingsSelection[0], activeSettingsSelection[1]);
      }
    }
  }
}

async function hydrate(selectRun = true): Promise<void> {
  try {
    state.workspace = await harnessService.hydrate();
    if (selectRun && !state.selectedRunId) state.selectedRunId = state.workspace.activeRuns[0]?.runId ?? state.workspace.runHistory.find((run) => !run.archived)?.runId ?? null;
    if (selectRun && state.selectedRunId && harnessService.isDesktop) {
      void loadRun(state.selectedRunId, true);
    }
    state.notice = harnessService.isDesktop ? "Workspace restored from canonical state." : "Browser preview — launch the Tauri app to connect the Rust harness.";
    state.noticeTone = harnessService.isDesktop ? "success" : "neutral";
  } catch (error) { state.notice = `Workspace hydration failed: ${String(error)}`; state.noticeTone = "error"; }
  finally { state.loading = false; render(); }
}

async function loadRun(runId: string, shouldRender = true): Promise<void> {
  if (state.selectedRunId !== runId) {
    state.selectedSnapshot = null;
    state.replay = null;
    state.selectedLoopId = null;
  }
  state.selectedRunId = runId; state.selectedEvidenceId = null;
  if (harnessService.isDesktop) {
    try { const [replay, snapshot] = await Promise.all([harnessService.replayRun(runId), harnessService.getRunSnapshot(runId)]); if (state.selectedRunId !== runId) return; state.replay = replay; state.selectedSnapshot = snapshot; const loops = state.replay.loops; if (!loops.some((loop) => loop.id === state.selectedLoopId)) state.selectedLoopId = loops[0]?.id ?? null; }
    catch (error) { state.notice = `Could not replay run: ${String(error)}`; state.noticeTone = "error"; }
  }
  if (shouldRender) render();
}

async function startRun(): Promise<void> {
  const thesis = document.querySelector<HTMLTextAreaElement>("#thesis-input")?.value.trim() ?? "";
  if (!thesis) { state.notice = "Enter a thesis or market behavior first."; state.noticeTone = "error"; render(); return; }
  const activeRun = state.tab !== "home" ? currentSnapshot() : null;
  const continuationFromRunId = state.tab === "home" || activeRun ? null : state.selectedRunId;
  state.loading = true; state.notice = activeRun ? "Sending steering instruction to the active run…" : "Starting the local autonomous run…"; state.noticeTone = "neutral"; render();
  try {
    const snapshot = activeRun
      ? await harnessService.steerRun(activeRun.runId, thesis)
      : await harnessService.startRun(thesis, continuationFromRunId);
    promptDraft = ""; await hydrate(false); state.selectedRunId = snapshot.runId; state.selectedLoopId = snapshot.loops[0]?.id ?? null; await loadRun(snapshot.runId, false); state.tab = "activity";
    state.notice = activeRun ? "Steering instruction queued for the next Jev decision." : continuationFromRunId ? `Continued from ${shortId(continuationFromRunId)} in a new run.` : "Autonomous run started."; state.noticeTone = "success";
  } catch (error) { state.notice = String(error); state.noticeTone = "error"; }
  finally { state.loading = false; render(); }
}

async function stopRun(): Promise<void> {
  const runId = currentSnapshot()?.runId;
  if (!runId || state.stoppingRunId) return;
  state.stoppingRunId = runId;
  state.notice = "Stopping the run and reconciling broker state…";
  state.noticeTone = "neutral";
  render();
  try { await harnessService.stopRun(runId); await hydrate(false); await loadRun(runId, false); state.notice = "Run stopped through the authoritative Rust harness."; state.noticeTone = "success"; }
  catch (error) { await hydrate(false); await loadRun(runId, false); state.notice = `Stop remains pending; no new entries are allowed. ${String(error)} Select Stop run again to retry closure.`; state.noticeTone = "error"; }
  finally { state.stoppingRunId = null; render(); if (updateStatus?.state === "waiting") void checkForUpdates(); }
}

async function checkForUpdates(): Promise<void> {
  if (!harnessService.isDesktop || updateChecking) return;
  updateChecking = true;
  if (state.settingsOpen) render();
  try {
    updateStatus = await harnessService.checkForUpdates();
    if (updateStatus.state === "waiting" || updateStatus.state === "error") {
      if (updateStatus.state === "waiting") {
        state.notice = updateStatus.message;
        state.noticeTone = "neutral";
      }
      if (updateRetryTimer !== null) window.clearTimeout(updateRetryTimer);
      updateRetryTimer = window.setTimeout(() => void checkForUpdates(), updateStatus.state === "waiting" ? 60_000 : 15 * 60_000);
    } else if (updateRetryTimer !== null) {
      window.clearTimeout(updateRetryTimer);
      updateRetryTimer = null;
    }
  } catch (error) {
    updateStatus = { state: "error", message: `Update check failed: ${String(error)}`, version: null };
    if (updateRetryTimer !== null) window.clearTimeout(updateRetryTimer);
    updateRetryTimer = window.setTimeout(() => void checkForUpdates(), 15 * 60_000);
  } finally {
    updateChecking = false;
    if (state.settingsOpen || updateStatus?.state === "waiting") render();
  }
}

async function search(query?: string): Promise<void> {
  const value = query ?? document.querySelector<HTMLInputElement>("#workspace-query, #evidence-query")?.value.trim() ?? "";
  state.searchQuery = value; state.tab = "evidence"; state.searchResults = value ? await harnessService.searchContext(value) : []; render();
}

function closeRename(): void {
  state.renamingRunId = null;
  render();
}

async function renameRun(form: HTMLFormElement, runId = state.renamingRunId): Promise<void> {
  const name = String(new FormData(form).get("name") ?? "").trim();
  if (!runId || !name) return;
  try {
    await harnessService.renameRun(runId, name);
    state.workspace = await harnessService.hydrate();
    state.renamingRunId = null;
    state.editingTitleRunId = null;
    state.notice = "Experiment renamed.";
    state.noticeTone = "success";
  } catch (error) {
    state.notice = `Could not rename experiment: ${String(error)}`;
    state.noticeTone = "error";
  }
  render();
}

async function setRunArchived(runId: string, archived: boolean): Promise<void> {
  try {
    await harnessService.setRunArchived(runId, archived);
    state.workspace = await harnessService.hydrate();
    if (archived && state.selectedRunId === runId) {
      state.selectedRunId = null;
      state.selectedSnapshot = null;
      state.selectedLoopId = null;
      state.selectedEvidenceId = null;
      state.replay = null;
    }
    state.notice = archived ? "Experiment archived." : "Experiment restored.";
    state.noticeTone = "success";
  } catch (error) {
    state.notice = `Could not ${archived ? "archive" : "restore"} experiment: ${String(error)}`;
    state.noticeTone = "error";
  }
  render();
}

function bindResize(handleSelector: string, side: "sidebar" | "inspector"): void {
  document.querySelector(handleSelector)?.addEventListener("mousedown", (event) => {
    event.preventDefault(); const startX = (event as MouseEvent).clientX; const start = side === "sidebar" ? preferences.sidebarWidth : preferences.inspectorWidth;
    const onMove = (move: MouseEvent) => { const delta = side === "sidebar" ? move.clientX - startX : startX - move.clientX; const value = Math.max(side === "sidebar" ? 240 : 300, Math.min(side === "sidebar" ? 440 : 520, start + delta)); if (side === "sidebar") preferences.sidebarWidth = value; else preferences.inspectorWidth = value; document.documentElement.style.setProperty(side === "sidebar" ? "--sidebar-width" : "--inspector-width", `${value}px`); };
    const onUp = () => { localStorage.setItem(`jev.ui.${side}-width`, String(side === "sidebar" ? preferences.sidebarWidth : preferences.inspectorWidth)); window.removeEventListener("mousemove", onMove); window.removeEventListener("mouseup", onUp); };
    window.addEventListener("mousemove", onMove); window.addEventListener("mouseup", onUp);
  });
}

async function openSettings(): Promise<void> {
  state.settingsOpen = true;
  settingsPage = "ctrader";
  mcpProbeMessage = "";
  state.connectorSettings = null;
  render();
  try {
    state.connectorSettings = await harnessService.getConnectorSettings();
  } catch (error) {
    state.settingsOpen = false;
    state.notice = `Could not load connector settings: ${String(error)}`;
    state.noticeTone = "error";
  }
  render();
}

async function probeMcpConnection(): Promise<void> {
  if (mcpChecking) return;
  mcpChecking = true;
  mcpProbeMessage = "Checking saved cTrader MCP settings…";
  render();
  try { mcpProbeMessage = await harnessService.probeMcpConnection(); }
  catch (error) { mcpProbeMessage = `Connection check failed: ${String(error)}`; }
  finally { mcpChecking = false; render(); }
}

async function approveHumanVerifiedLiveCycle(form: HTMLFormElement): Promise<void> {
  if (liveRiskApprovalBusy) return;
  const runId = state.selectedRunId;
  if (!runId || !state.selectedSnapshot || state.selectedSnapshot.status !== "active") {
    liveRiskApprovalMessage = "Select an active run before verifying a live cycle.";
    render();
    return;
  }
  const data = new FormData(form);
  const numeric = (name: string) => Number(data.get(name));
  liveRiskApprovalBusy = true;
  liveRiskApprovalMessage = "Checking account identity and running one Jev cycle…";
  render();
  try {
    const result = await harnessService.approveHumanVerifiedLiveCycle({
      runId,
      accountId: String(data.get("accountId") ?? "").trim(),
      environment: String(data.get("environment") ?? "").trim(),
      instrument: String(data.get("instrument") ?? "").trim(),
      equity: numeric("equity"),
      freeMargin: numeric("freeMargin"),
      depositCurrency: String(data.get("depositCurrency") ?? "").trim().toUpperCase(),
      accountOpenExposure: numeric("accountOpenExposure"),
      quoteToDeposit: numeric("quoteToDeposit"),
      confirmed: data.get("confirmed") === "on",
    });
    state.selectedSnapshot = result;
    if (state.workspace) {
      const index = state.workspace.activeRuns.findIndex((run) => run.runId === result.runId);
      if (index >= 0) state.workspace.activeRuns[index] = result;
    }
    liveRiskApprovalMessage = "One verified cycle completed. Check Activity for its decision, risk result, and execution outcome.";
    state.notice = liveRiskApprovalMessage;
    state.noticeTone = "success";
    await loadRun(result.runId, false);
  } catch (error) {
    liveRiskApprovalMessage = String(error);
    state.notice = `Live cycle was not approved: ${String(error)}`;
    state.noticeTone = "error";
  } finally {
    liveRiskApprovalBusy = false;
    render();
  }
}

function closeSettings(): void {
  if (state.settingsSaving) return;
  state.settingsOpen = false;
  render();
}

async function saveSettings(): Promise<void> {
  const values: Record<string, string> = {};
  for (const section of state.connectorSettings?.sections ?? []) {
    for (const field of section.fields) values[field.key] = field.value.trim();
  }
  const update: ConnectorSettingsUpdate = {
    worldModelAdapter: state.connectorSettings?.worldModelAdapter ?? "openrouter",
    jevAdapter: state.connectorSettings?.jevAdapter ?? "typesafe",
    brokerAdapter: state.connectorSettings?.brokerAdapter ?? "ctrader-fix",
    values,
  };
  state.settingsSaving = true;
  render();
  try {
    state.connectorSettings = await harnessService.saveConnectorSettings(update);
    state.settingsOpen = false;
    await hydrate(false);
    state.notice = "Connector settings saved locally. Restart Espeon to activate the new adapters.";
    state.noticeTone = "success";
    void checkForUpdates();
  } catch (error) {
    state.notice = `Could not save connector settings: ${String(error)}`;
    state.noticeTone = "error";
  } finally {
    state.settingsSaving = false;
    render();
  }
}

function toggleSidebar(): void {
  preferences.sidebarCollapsed = !preferences.sidebarCollapsed;
  if (!preferences.sidebarCollapsed && window.innerWidth <= 1080) compactInspectorOpen = false;
  localStorage.setItem("jev.ui.sidebar-collapsed", String(preferences.sidebarCollapsed));
  render();
}

function toggleInspector(): void {
  if (window.innerWidth <= 1080) {
    compactInspectorOpen = !compactInspectorOpen;
    if (compactInspectorOpen) {
      preferences.sidebarCollapsed = true;
      localStorage.setItem("jev.ui.sidebar-collapsed", "true");
    }
    render();
    return;
  }
  preferences.inspectorCollapsed = !preferences.inspectorCollapsed;
  localStorage.setItem("jev.ui.inspector-collapsed", String(preferences.inspectorCollapsed));
  render();
}

function showInspector(): void {
  if (window.innerWidth <= 1080) {
    compactInspectorOpen = true;
  if (window.innerWidth <= 820) {
    preferences.sidebarCollapsed = true;
    localStorage.setItem("jev.ui.sidebar-collapsed", "true");
  }
  }
  else preferences.inspectorCollapsed = false;
  render();
}

function focusWorkspaceSearch(): void {
  if (sidebarIsCollapsed()) toggleSidebar();
  const input = document.querySelector<HTMLInputElement>("#workspace-query");
  input?.focus();
  input?.select();
}

function startNewRun(): void {
  promptDraft = "";
  state.selectedRunId = null;
  state.selectedSnapshot = null;
  state.selectedLoopId = null;
  state.selectedEvidenceId = null;
  state.replay = null;
  state.tab = "activity";
  render();
  document.querySelector<HTMLTextAreaElement>("#thesis-input")?.focus();
}

function resizePrompt(): void {
  const textarea = document.querySelector<HTMLTextAreaElement>("#thesis-input");
  const composer = document.querySelector<HTMLElement>("#run-composer");
  const main = document.querySelector<HTMLElement>(".workbench-main");
  if (!textarea || !composer || !main) return;
  textarea.style.height = "54px";
  const composerWithoutEditor = composer.offsetHeight - textarea.offsetHeight;
  const maximum = Math.max(54, Math.floor(main.clientHeight / 2) - composerWithoutEditor);
  const height = Math.min(maximum, Math.max(54, textarea.scrollHeight));
  textarea.style.height = `${height}px`;
  textarea.style.overflowY = textarea.scrollHeight > height ? "auto" : "hidden";
}

let activityNavigatorResizeObserver: ResizeObserver | null = null;
function bindActivityNavigator(): void {
  activityNavigatorResizeObserver?.disconnect();
  activityNavigatorResizeObserver = null;
  const navigator = document.querySelector<HTMLElement>("#activity-navigator");
  const track = document.querySelector<HTMLElement>(".activity-navigator-track");
  const scroller = document.querySelector<HTMLElement>("#workbench-scroll");
  const feed = document.querySelector<HTMLElement>("#activity-feed");
  if (!navigator || !track || !scroller || !feed) return;

  const update = () => {
    navigator.style.height = `${scroller.clientHeight}px`;
    const marks = [...navigator.querySelectorAll<HTMLElement>(".activity-mark-row")];
    const cards = [...feed.querySelectorAll<HTMLElement>(".activity-card[data-activity-index]")];
    const count = marks.length;
    const trackHeight = Math.max(0, track.clientHeight - 8);
    const gap = 15;
    const compactHeight = Math.max(0, (count - 1) * gap);
    const distribution = count <= 1 ? 0 : Math.min(1, Math.max(0, (count - 18) / 12));
    const compactOffset = Math.max(0, (trackHeight - compactHeight) / 2);
    marks.forEach((row, index) => {
      const compactTop = compactOffset + index * gap;
      const distributedTop = 4 + index / Math.max(1, count - 1) * trackHeight;
      const top = compactTop + (distributedTop - compactTop) * distribution;
      row.style.top = `${top}px`;
      row.style.transform = "translateY(-50%)";
    });
    const scrollerTop = scroller.getBoundingClientRect().top;
    const visible = cards.findIndex(card => card.getBoundingClientRect().bottom > scrollerTop + 12);
    const activeIndex = visible < 0 ? Math.max(0, cards.length - 1) : visible;
    marks.forEach((row, index) => {
      const mark = row.querySelector<HTMLElement>(".activity-mark");
      if (index === activeIndex) mark?.setAttribute("aria-current", "true");
      else mark?.removeAttribute("aria-current");
    });
    navigator.setAttribute("aria-label", count ? `Activity navigator. Event ${activeIndex + 1} of ${count} is in view. Select a marker or use arrow keys to jump through events.` : "Activity navigator. No events.");
  };
  const jumpTo = (target: HTMLElement) => {
    const top = target.getBoundingClientRect().top - scroller.getBoundingClientRect().top + scroller.scrollTop;
    scroller.scrollTo({ top: Math.max(0, top - 8), behavior: "smooth" });
  };
  navigator.addEventListener("click", (event) => {
    const mark = (event.target as HTMLElement).closest<HTMLElement>(".activity-mark");
    const target = mark && feed.querySelector<HTMLElement>(`.activity-card[data-activity-index="${CSS.escape(mark.dataset.activityIndex ?? "")}"]`);
    if (target) jumpTo(target);
  });
  navigator.addEventListener("keydown", (event) => {
    const cards = [...feed.querySelectorAll<HTMLElement>(".activity-card[data-activity-index]")];
    if (!cards.length) return;
    const top = scroller.getBoundingClientRect().top;
    let current = cards.reduce((best, card, index) => Math.abs(card.getBoundingClientRect().top - top) < Math.abs(cards[best].getBoundingClientRect().top - top) ? index : best, 0);
    if (event.key === "ArrowDown" || event.key === "PageDown") current = Math.min(cards.length - 1, current + (event.key === "PageDown" ? 8 : 1));
    else if (event.key === "ArrowUp" || event.key === "PageUp") current = Math.max(0, current - (event.key === "PageUp" ? 8 : 1));
    else if (event.key === "Home") current = 0;
    else if (event.key === "End") current = cards.length - 1;
    else return;
    event.preventDefault();
    jumpTo(cards[current]);
  });
  scroller.addEventListener("scroll", update, { passive: true });
  activityNavigatorResizeObserver = new ResizeObserver(update);
  activityNavigatorResizeObserver.observe(scroller);
  activityNavigatorResizeObserver.observe(track);
  activityNavigatorResizeObserver.observe(feed);
  requestAnimationFrame(update);
}

function bindInteractions(): void {
  bindActivityNavigator();
  const prompt = document.querySelector<HTMLTextAreaElement>("#thesis-input");
  prompt?.addEventListener("input", () => { promptDraft = prompt.value; resizePrompt(); });
  resizePrompt();
  document.querySelector<HTMLSelectElement>("#pnl-chart-range")?.addEventListener("change", (event) => {
    pnlRange = (event.currentTarget as HTMLSelectElement).value as PnlRange;
    render();
  });
  document.querySelector("#run-composer")?.addEventListener("submit", (event) => { event.preventDefault(); void startRun(); });
  document.querySelector("#stop-run")?.addEventListener("click", () => void stopRun());
  document.querySelector("#workspace-search")?.addEventListener("submit", (event) => { event.preventDefault(); void search((event.currentTarget as HTMLFormElement).querySelector("input")?.value); });
  document.querySelector("#evidence-search")?.addEventListener("submit", (event) => { event.preventDefault(); void search((event.currentTarget as HTMLFormElement).querySelector("input")?.value); });
  document.querySelectorAll<HTMLElement>("[data-tab]").forEach((button) => button.addEventListener("click", () => { state.tab = button.dataset.tab as Tab; render(); }));
  const editTitle = () => {
    if (!state.selectedRunId) return;
    state.editingTitleRunId = state.selectedRunId;
    render();
    const input = document.querySelector<HTMLInputElement>("#run-title-input");
    input?.focus();
    input?.select();
  };
  document.querySelector("#edit-run-title")?.addEventListener("click", editTitle);
  document.querySelector("#run-title")?.addEventListener("dblclick", editTitle);
  document.querySelector("#cancel-title-edit")?.addEventListener("click", () => { state.editingTitleRunId = null; render(); });
  document.querySelector("#run-title-form")?.addEventListener("submit", (event) => { event.preventDefault(); void renameRun(event.currentTarget as HTMLFormElement, state.editingTitleRunId); });
  document.querySelector("#view-all-history")?.addEventListener("click", () => { state.tab = "history"; state.showArchived = false; render(); });
  document.querySelector("#view-all-home")?.addEventListener("click", () => { state.tab = "history"; state.showArchived = false; render(); });
  document.querySelectorAll<HTMLElement>("[data-history-filter]").forEach((button) => button.addEventListener("click", () => { state.showArchived = button.dataset.historyFilter === "archived"; render(); }));
  document.querySelectorAll<HTMLElement>("[data-open-run]").forEach((button) => button.addEventListener("click", () => { state.tab = "activity"; void loadRun(button.dataset.openRun!); }));
  document.querySelectorAll<HTMLElement>("[data-rename-run]").forEach((button) => button.addEventListener("click", () => { state.renamingRunId = button.dataset.renameRun ?? null; render(); document.querySelector<HTMLInputElement>("#run-name")?.focus(); }));
  document.querySelectorAll<HTMLElement>("[data-archive-run]").forEach((button) => button.addEventListener("click", () => void setRunArchived(button.dataset.archiveRun!, button.dataset.archived !== "true")));
  document.querySelectorAll("#close-rename, #cancel-rename").forEach((button) => button.addEventListener("click", closeRename));
  document.querySelector("#rename-backdrop")?.addEventListener("mousedown", (event) => { if (event.target === event.currentTarget) closeRename(); });
  document.querySelector("#rename-run-form")?.addEventListener("submit", (event) => { event.preventDefault(); void renameRun(event.currentTarget as HTMLFormElement); });
  document.querySelectorAll<HTMLElement>("[data-loop-id]").forEach((button) => button.addEventListener("click", () => { state.selectedLoopId = button.dataset.loopId ?? null; state.selectedEvidenceId = null; showInspector(); }));
  document.querySelectorAll<HTMLElement>("[data-run-id]").forEach((button) => button.addEventListener("click", () => void loadRun(button.dataset.runId!)));
  document.querySelectorAll<HTMLElement>("[data-evidence-id]").forEach((button) => button.addEventListener("click", () => { state.selectedEvidenceId = button.dataset.evidenceId ?? null; showInspector(); }));
  document.querySelector("#dismiss-notice")?.addEventListener("click", () => { state.notice = ""; render(); });
  document.querySelectorAll("#open-settings, [data-open-settings]").forEach((button) => button.addEventListener("click", () => void openSettings()));
  document.querySelectorAll("#close-settings, #cancel-settings").forEach((button) => button.addEventListener("click", closeSettings));
  document.querySelector("#check-updates")?.addEventListener("click", () => void checkForUpdates());
  document.querySelector("#probe-mcp")?.addEventListener("click", () => void probeMcpConnection());
  document.querySelector<HTMLFormElement>("#human-live-risk-form")?.addEventListener("submit", (event) => {
    event.preventDefault();
    void approveHumanVerifiedLiveCycle(event.currentTarget as HTMLFormElement);
  });
  document.querySelector("#settings-backdrop")?.addEventListener("mousedown", (event) => { if (event.target === event.currentTarget) closeSettings(); });
  document.querySelector("#connector-settings-form")?.addEventListener("submit", (event) => { event.preventDefault(); void saveSettings(); });
  document.querySelectorAll<HTMLElement>("[data-settings-page]").forEach((button) => button.addEventListener("click", () => { settingsPage = button.dataset.settingsPage ?? "overview"; render(); }));
  document.querySelectorAll<HTMLInputElement>("#connector-settings-form input[name]").forEach((input) => {
    const updateField = () => {
      for (const section of state.connectorSettings?.sections ?? []) {
        const field = section.fields.find((candidate) => candidate.key === input.name);
        if (field) {
          field.value = input.type === "checkbox" ? String(input.checked) : input.value;
          field.configured = input.type === "checkbox" || input.value.length > 0;
          break;
        }
      }
      if (input.type === "checkbox") {
        const label = input.closest(".connector-switch")?.querySelector("i");
        if (label) label.textContent = input.checked ? "On" : "Off";
      }
    };
    input.addEventListener("input", updateField);
    input.addEventListener("change", updateField);
  });
  document.querySelectorAll<HTMLSelectElement>("#connector-settings-form select").forEach((select) => {
    select.addEventListener("change", () => {
      if (!state.connectorSettings) return;
      if (select.name === "worldModelAdapter") state.connectorSettings.worldModelAdapter = select.value;
      else if (select.name === "jevAdapter") state.connectorSettings.jevAdapter = select.value;
      else if (select.name === "brokerAdapter") {
        state.connectorSettings.brokerAdapter = select.value;
        const simulated = select.value === "simulated";
        state.connectorSettings.riskReadiness = simulated
          ? "Simulated execution uses the configured paper risk policy. No cTrader account is used."
          : "Live entries are blocked: cTrader FIX does not provide a fresh account risk snapshot. MCP account and volume checks alone cannot approve or size a live order.";
        const panel = document.querySelector<HTMLElement>("#order-risk-readiness");
        if (panel) {
          panel.dataset.checkState = simulated ? "result" : "error";
          const message = panel.querySelector<HTMLElement>("[role=status]");
          if (message) message.textContent = state.connectorSettings.riskReadiness;
        }
      }
    });
  });
  document.querySelector("#toggle-theme")?.addEventListener("click", () => { preferences.theme = preferences.theme === "dark" ? "light" : "dark"; localStorage.setItem("jev.ui.theme", preferences.theme); render(); });
  document.querySelector("#toggle-sidebar")?.addEventListener("click", toggleSidebar);
  document.querySelector("#sidebar-scrim")?.addEventListener("click", toggleSidebar);
  document.querySelector("#go-home")?.addEventListener("click", () => { state.tab = "home"; state.notice = ""; render(); });
  document.querySelector("#toggle-inspector")?.addEventListener("click", toggleInspector);
  document.querySelector("#close-inspector")?.addEventListener("click", toggleInspector);
  document.querySelector("#new-run")?.addEventListener("click", startNewRun);
  bindResize(".resize-sidebar", "sidebar"); bindResize(".resize-inspector", "inspector");
}

window.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && state.renamingRunId) { event.preventDefault(); closeRename(); return; }
  if (event.key === "Escape" && state.editingTitleRunId) { event.preventDefault(); state.editingTitleRunId = null; render(); return; }
  if (event.key === "Escape" && state.settingsOpen) { event.preventDefault(); closeSettings(); return; }
  if (event.key === "Escape" && !sidebarIsCollapsed()) { event.preventDefault(); toggleSidebar(); return; }
  if (state.renamingRunId || state.settingsOpen) return;
  const command = event.ctrlKey || event.metaKey;
  if (command && event.key.toLowerCase() === "k") { event.preventDefault(); focusWorkspaceSearch(); }
  if (command && event.key.toLowerCase() === "b") { event.preventDefault(); toggleSidebar(); }
  if (command && event.key.toLowerCase() === "i") { event.preventDefault(); toggleInspector(); }
  if (command && event.key === "Enter") { event.preventDefault(); void startRun(); }
  if (command && event.key.toLowerCase() === "n") { event.preventDefault(); startNewRun(); }
});

let windowLayoutBand = window.innerWidth <= 820 ? 0 : window.innerWidth <= 1080 ? 1 : 2;
window.addEventListener("resize", () => {
  resizePrompt();
  const nextBand = window.innerWidth <= 820 ? 0 : window.innerWidth <= 1080 ? 1 : 2;
  if (nextBand === windowLayoutBand) return;
  windowLayoutBand = nextBand;
  compactInspectorOpen = false;
  render();
});

render();
void harnessService.subscribe(
  async (snapshot) => {
    if (!state.workspace) return;
    const index = state.workspace.activeRuns.findIndex((run) => run.runId === snapshot.runId);
    if (snapshot.status === "active") { if (index >= 0) state.workspace.activeRuns[index] = snapshot; else state.workspace.activeRuns.unshift(snapshot); }
    else if (index >= 0) state.workspace.activeRuns.splice(index, 1);
    if (state.selectedRunId === snapshot.runId) { state.selectedSnapshot = snapshot; await loadRun(snapshot.runId, false); }
    try {
      const latestHealth = await harnessService.marketHealth();
      state.workspace.integrations = [...state.workspace.integrations.filter((item) => !item.id.startsWith("market-")), ...latestHealth];
    } catch { /* Canonical activity still displays feed failures if the health view is unavailable. */ }
    render();
  },
  (error) => { state.notice = `Harness degraded: ${error.message}`; state.noticeTone = "error"; render(); },
);
void hydrate().then(() => {
  if (!harnessService.isDesktop) return;
  window.setInterval(() => {
    if (state.loading || state.settingsSaving) return;
    void (async () => {
      try {
        const workspace = await harnessService.hydrate();
        const runId = state.selectedRunId;
        const [replay, snapshot] = runId
          ? await Promise.all([harnessService.replayRun(runId), harnessService.getRunSnapshot(runId)])
          : [null, null];
        const changed = state.replay?.lastSequence !== replay?.lastSequence
          || state.selectedSnapshot?.status !== snapshot?.status
          || state.workspace?.activeRuns.length !== workspace.activeRuns.length
          || JSON.stringify(state.workspace?.integrations) !== JSON.stringify(workspace.integrations);
        state.workspace = workspace;
        if (runId !== state.selectedRunId) return;
        state.replay = replay;
        state.selectedSnapshot = snapshot;
        if (changed) render();
      } catch (error) {
        state.notice = `Canonical workspace refresh failed: ${String(error)}`;
        state.noticeTone = "error";
        render();
      }
    })();
  }, 10_000);
  window.setTimeout(() => void checkForUpdates(), 5_000);
  window.setInterval(() => void checkForUpdates(), 6 * 60 * 60_000);
});
