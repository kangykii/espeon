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
import type { ConnectorSettings, ConnectorSettingsUpdate, ContextRecord, Decision, EventView, Hypothesis, LoopView, ReplayState, RunSnapshot, SearchHit, UpdateStatus, WorkspaceSnapshot } from "./types";

type Tab = "activity" | "positions" | "evidence" | "history";
type Theme = "light" | "dark";
const app = document.querySelector<HTMLDivElement>("#app")!;
const preferences = {
  theme: (localStorage.getItem("jev.ui.theme") as Theme) || "light",
  sidebarCollapsed: localStorage.getItem("jev.ui.sidebar-collapsed") === "true",
  inspectorCollapsed: localStorage.getItem("jev.ui.inspector-collapsed") === "true",
  sidebarWidth: Number(localStorage.getItem("jev.ui.sidebar-width")) || 292,
  inspectorWidth: Number(localStorage.getItem("jev.ui.inspector-width")) || 344,
};
let compactSidebarOpen = false;
let compactInspectorOpen = false;
let pnlRange: PnlRange = "all";
let promptDraft = "";
let updateStatus: UpdateStatus | null = null;
let updateChecking = false;
let updateRetryTimer: number | null = null;
const sidebarIsCollapsed = () => window.innerWidth <= 820 ? !compactSidebarOpen : preferences.sidebarCollapsed;
const inspectorIsCollapsed = () => window.innerWidth <= 1080 ? !compactInspectorOpen : preferences.inspectorCollapsed;
const state: { workspace: WorkspaceSnapshot | null; selectedSnapshot: RunSnapshot | null; selectedRunId: string | null; selectedLoopId: string | null; selectedEvidenceId: string | null; replay: ReplayState | null; tab: Tab; searchResults: SearchHit[]; searchQuery: string; notice: string; noticeTone: "neutral" | "error" | "success"; loading: boolean; stoppingRunId: string | null; settingsOpen: boolean; connectorSettings: ConnectorSettings | null; settingsSaving: boolean; showArchived: boolean; renamingRunId: string | null } = {
  workspace: null, selectedSnapshot: null, selectedRunId: null, selectedLoopId: null, selectedEvidenceId: null, replay: null,
  tab: "activity", searchResults: [], searchQuery: "", notice: "Hydrating local workspace…", noticeTone: "neutral", loading: true,
  stoppingRunId: null, settingsOpen: false, connectorSettings: null, settingsSaving: false, showArchived: false, renamingRunId: null,
};

function escapeHtml(value: unknown): string { const node = document.createElement("div"); node.textContent = String(value ?? ""); return node.innerHTML; }
function shortId(value: string | null | undefined): string { return value ? `${value.slice(0, 7)}…${value.slice(-4)}` : "—"; }
function runTitle(runId: string, fallback: string): string { return state.workspace?.runHistory.find((run) => run.runId === runId)?.thesis ?? fallback; }
function dateTime(value: string | null | undefined): string { if (!value) return "—"; const parsed = new Date(value); return Number.isNaN(parsed.getTime()) ? escapeHtml(value) : parsed.toLocaleString([], { dateStyle: "medium", timeStyle: "short" }); }
function relativeTime(value: string): string { const minutes = Math.floor((Date.now() - new Date(value).getTime()) / 60_000); if (minutes < 1) return "now"; if (minutes < 60) return `${minutes}m`; const hours = Math.floor(minutes / 60); return hours < 24 ? `${hours}h` : `${Math.floor(hours / 24)}d`; }
function currentSnapshot(): RunSnapshot | null {
  const active = state.workspace?.activeRuns.find((run) => run.runId === state.selectedRunId);
  if (active) return active;
  return state.selectedSnapshot?.status === "active" ? state.selectedSnapshot : null;
}
function currentHypotheses(): Hypothesis[] { return state.replay?.hypotheses ?? currentSnapshot()?.hypotheses ?? []; }
function currentLoops(): LoopView[] { return state.replay?.loops ?? currentSnapshot()?.loops ?? []; }
function selectedLoop(): LoopView | null { return currentLoops().find((loop) => loop.id === state.selectedLoopId) ?? currentLoops()[0] ?? null; }
function hypothesisFor(loop: LoopView | null): Hypothesis | null { return loop ? currentHypotheses().find((item) => item.id === loop.hypothesisId) ?? null : currentHypotheses().at(-1) ?? null; }
function latestDecision(loopId: string): Decision | null { return [...(state.replay?.decisions ?? [])].reverse().find((item) => item.loopId === loopId) ?? null; }
function positionFor(loopId: string) { return [...(state.replay?.positions ?? currentSnapshot()?.positions ?? [])].reverse().find((item) => item.loopId === loopId) ?? null; }
function selectedEvidence(): ContextRecord | null { const records = state.replay?.contextPoolRecords ?? []; return records.find((item) => item.id === state.selectedEvidenceId || item.canonicalEntityId === state.selectedEvidenceId) ?? null; }

function liveContextSection(decision: Decision | null): string {
  const snapshot = decision?.resolvedState.liveContextSnapshot;
  if (!snapshot) {
    return `<section class="inspector-section"><h3>Live market context</h3><p class="muted">No resolved live snapshot yet. A missing or stale feed skips Jev and is recorded in Activity.</p></section>`;
  }
  const latest = [...snapshot.candles].sort((a, b) => b.openTime.localeCompare(a.openTime))[0];
  const formulaRows = snapshot.fields.map((field) => `<div class="context-item"><strong>${escapeHtml(field.label)}</strong><span>${escapeHtml(String(field.value))}</span><small>${escapeHtml(JSON.stringify(field.formula))}</small><time>${dateTime(field.observedAt)} · ${escapeHtml(field.provenance.join(", "))}</time></div>`).join("");
  return `<section class="inspector-section"><h3>Live market context</h3><dl>${definitionRow("Snapshot", shortId(snapshot.id), true)}${definitionRow("Freshness", snapshot.freshnessState)}${definitionRow("Bid / Ask", `${snapshot.quote.bid} / ${snapshot.quote.ask}`)}${definitionRow("Mid / Spread", `${snapshot.quote.mid} / ${snapshot.quote.spread}`)}${definitionRow("Quote received", dateTime(snapshot.quote.receivedAt))}${definitionRow("Latest closed bar", latest ? `${latest.period} · ${dateTime(latest.openTime)} · O ${latest.open} H ${latest.high} L ${latest.low} C ${latest.close}` : "None")}</dl>${formulaRows}</section>`;
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
  return `<div aria-hidden="true" class="window-frame-overlay"></div><header class="window-chrome ${harnessService.isDesktop ? "" : "window-chrome-browser"}"><div aria-label="Espeon controls" class="window-chrome-actions" role="group"><button aria-label="Toggle sidebar" aria-expanded="${!sidebarIsCollapsed()}" class="window-chrome-action" id="toggle-sidebar" title="Toggle sidebar (Ctrl+B)" type="button">${icons.panel}</button><button aria-label="New run" class="window-chrome-action" id="new-run" title="New run (Ctrl+N)" type="button">${icons.plus}</button><button aria-label="Search experiments" class="window-chrome-action" id="chrome-search" title="Search experiments (Ctrl+K)" type="button">${icons.search}</button><button aria-label="Connectors and settings" class="window-chrome-action" id="open-settings" title="Connectors and settings" type="button">${icons.settings}</button><button aria-label="Toggle theme" class="window-chrome-action" id="toggle-theme" title="Toggle theme" type="button">${preferences.theme === "dark" ? icons.sun : icons.moon}</button><button aria-label="Toggle context" aria-expanded="${!inspectorIsCollapsed()}" class="window-chrome-action" id="toggle-inspector" title="Toggle context (Ctrl+I)" type="button">${icons.inspect}</button></div><div class="window-chrome-drag"><span>Espeon</span></div>${windowControls}</header>`;
}

function sidebar(): string {
  const snapshot = currentSnapshot(); const loops = currentLoops(); const history = state.workspace?.runHistory ?? []; const visibleHistory = history.filter((run) => !run.archived);
  return `<aside class="sidebar ${preferences.sidebarCollapsed ? "is-collapsed" : ""}" aria-label="Run navigation"><div class="sidebar-body"><form class="sidebar-search" id="workspace-search"><span>${icons.search}</span><input id="workspace-query" value="${escapeHtml(state.searchQuery)}" placeholder="Search experiments" aria-label="Search experiments"/><kbd>Ctrl+K</kbd></form><section class="nav-section"><div class="section-label"><span class="section-name">${icons.activity}Current run</span>${snapshot ? statusChip(snapshot.status) : ""}</div>${snapshot ? `<button class="run-summary is-active" data-run-id="${escapeHtml(snapshot.runId)}"><span class="run-orb"></span><span><strong>${escapeHtml(runTitle(snapshot.runId, snapshot.thesis))}</strong><small>${loops.length} loop${loops.length === 1 ? "" : "s"} · ${snapshot.events.length} events</small></span></button>` : `<div class="sidebar-empty">No active run selected.</div>`}</section><section class="nav-section loop-section"><div class="section-label"><span class="section-name">${icons.branch}Jev loops</span><span class="count">${loops.length}</span></div><div class="thread-list">${loops.length ? loops.map(loopRow).join("") : `<div class="sidebar-empty">Loops appear after a run starts.</div>`}</div></section><section class="nav-section history-section"><div class="section-label"><span class="section-name">${icons.history}Experiment history</span><button class="section-link" id="view-all-history" type="button">View all</button></div><div class="history-list">${visibleHistory.slice(0, 8).map((run) => `<button class="history-row ${run.runId === state.selectedRunId ? "is-active" : ""}" data-run-id="${escapeHtml(run.runId)}" type="button"><span>${escapeHtml(run.thesis)}</span><small>${escapeHtml(run.status)} · ${relativeTime(run.startedAt)}</small></button>`).join("") || `<div class="sidebar-empty">Canonical run history is empty.</div>`}</div></section></div></aside><div class="resize-handle resize-sidebar" aria-hidden="true"></div>`;
}

function eventKind(event: EventView): { label: string; tone: string } {
  if (/decision|jev/i.test(event.kind)) return { label: "Jev", tone: "jev" };
  if (/execution|position|fill/i.test(event.kind)) return { label: "Execution", tone: "execution" };
  if (/guardrail|order/i.test(event.kind)) return { label: "Harness", tone: "harness" };
  if (/web|retriev|context/i.test(event.kind)) return { label: "Research", tone: "research" };
  if (/hypothesis|review|thesis/i.test(event.kind)) return { label: "World model", tone: "world" };
  return { label: "System", tone: "system" };
}

function eventDetails(event: EventView): string {
  const decision = state.replay?.decisions.find((item) => item.id === event.aggregateId);
  if (decision) return `<div class="structured-line"><span>Action</span><strong>${escapeHtml(decision.action)}</strong><span>Confidence</span><strong>${(decision.confidence * 100).toFixed(0)}%</strong></div><p>${escapeHtml(decision.rationale)}</p>`;
  const execution = state.replay?.executions.find((item) => item.executionId === event.aggregateId);
  if (execution) return `<div class="structured-line"><span>Status</span><strong>${escapeHtml(execution.status)}</strong><span>Fill</span><strong>${execution.filledQuantity} @ ${execution.averagePrice ?? "—"}</strong></div>${execution.rejectionReason ? `<p>${escapeHtml(execution.rejectionReason)}</p>` : ""}`;
  const review = state.replay?.hypothesisReviews.find((item) => item.id === event.aggregateId);
  if (review) return `<div class="structured-line"><span>Action</span><strong>${escapeHtml(review.action.toUpperCase())}</strong><span>Route</span><strong>${review.routing?.escalated ? "Escalated" : "Base model"}</strong></div><p>${escapeHtml(review.diagnosis || review.rationale)}</p><p>${escapeHtml(review.continuationRationale || review.rationale)}</p>`;
  const trigger = [...(state.replay?.autonomousReviewTriggers ?? [])].reverse().find((item) => item.triggerId === event.aggregateId);
  if (trigger) return `<div class="structured-line"><span>Status</span><strong>${escapeHtml(trigger.status)}</strong><span>Reasons</span><strong>${escapeHtml(trigger.kinds.join(", ").replaceAll("_", " "))}</strong></div><p>No-trade ${trigger.noTradeStreak}/${trigger.noTradeThreshold} · losses ${trigger.consecutiveLosses}/${trigger.lossThreshold} · trades ${trigger.completedTradesSinceReview}/${trigger.periodicTradeThreshold}</p>`;
  const outcome = state.replay?.tradeOutcomes.find((item) => item.id === event.aggregateId);
  if (outcome) return `<div class="structured-line"><span>Outcome</span><strong>${escapeHtml(outcome.classification.toUpperCase())}</strong><span>Gross P&amp;L</span><strong>${outcome.grossRealizedPnl === null ? "Unknown basis" : outcome.grossRealizedPnl.toFixed(2)}</strong></div><p>${escapeHtml(outcome.instrument)} · ${outcome.quantity} · ${outcome.entryExecutionIds.length} entry fill(s) / ${outcome.exitExecutionIds.length} exit fill(s)</p>`;
  return event.summary ? `<p>${escapeHtml(event.summary)}</p>` : "";
}

function activityView(): string {
  const events = currentSnapshot()?.events ?? [];
  if (!state.selectedRunId) return welcomeView();
  if (!events.length) return `<div class="empty-state"><span class="empty-icon">${icons.activity}</span><h2>Waiting for canonical activity</h2><p>World-model, Jev, harness, and execution events will appear here.</p></div>`;
  return `<div class="activity-stream">${[...events].reverse().map((event) => { const kind = eventKind(event); return `<article class="activity-card" data-tone="${kind.tone}"><div class="activity-rail"><span class="activity-node"></span></div><div class="activity-content"><div class="activity-header"><span class="activity-source">${escapeHtml(kind.label)}</span><span class="activity-kind">${escapeHtml(event.kind.replaceAll("_", " "))}</span><time>${dateTime(event.occurredAt)}</time></div>${eventDetails(event)}<div class="canonical-ref"><span>#${event.sequence}</span><code>${escapeHtml(shortId(event.id))}</code><span>${escapeHtml(event.aggregateType)}</span></div></div></article>`; }).join("")}</div>`;
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
  return `<div class="table-section"><div class="content-section-heading"><div><h2>Positions & execution</h2></div><span>${state.replay?.positions.length ?? 0} positions</span></div>${renderPositionsChart(state.replay?.tradeOutcomes ?? [], pnlRange)}<div class="table-wrap"><table><thead><tr><th>Instrument</th><th>State</th><th>Quantity</th><th>Entry</th><th>P&amp;L</th><th>Origin loop</th><th>Broker</th></tr></thead><tbody>${positionRows() || `<tr><td colspan="7" class="table-empty">No positions recorded for this run.</td></tr>`}</tbody></table></div><div class="execution-list"><h3>Execution ledger</h3>${[...executions].reverse().map((item) => `<article class="execution-row"><div>${statusChip(item.status)}<strong>${escapeHtml(item.executionKind)}</strong><span>${escapeHtml(item.action)}</span></div><div><span>${item.filledQuantity} @ ${item.averagePrice ?? "—"}</span><code>${escapeHtml(shortId(item.executionId))}</code><time>${dateTime(item.executedAt)}</time></div>${item.rejectionReason ? `<p>${escapeHtml(item.rejectionReason)}</p>` : ""}</article>`).join("") || `<p class="muted">No broker executions yet.</p>`}</div></div>`;
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
  return `<div class="history-view"><div class="content-section-heading"><div><h2>${state.showArchived ? "Archived" : "Recent"} experiments</h2></div><div class="history-filters"><button class="${state.showArchived ? "" : "is-active"}" data-history-filter="recent" type="button">Recent</button><button class="${state.showArchived ? "is-active" : ""}" data-history-filter="archived" type="button">Archived</button></div></div><div class="history-table">${runs.map((run) => `<article class="history-card"><button class="history-card-open" data-open-run="${escapeHtml(run.runId)}" type="button"><span class="history-state" data-state="${escapeHtml(run.status)}"></span><span class="history-card-copy"><strong>${escapeHtml(run.thesis)}</strong><small>${dateTime(run.startedAt)} · ${escapeHtml(run.status)}</small></span><code>${escapeHtml(shortId(run.runId))}</code>${icons.chevron}</button><div class="history-card-actions"><button data-rename-run="${escapeHtml(run.runId)}" type="button" title="Rename experiment">${icons.rename}<span>Rename</span></button><button data-archive-run="${escapeHtml(run.runId)}" data-archived="${run.archived}" type="button" title="${run.status === "active" ? "Stop the experiment before archiving" : run.archived ? "Restore experiment" : "Archive experiment"}" ${run.status === "active" ? "disabled" : ""}>${run.archived ? icons.restoreArchive : icons.archive}<span>${run.archived ? "Restore" : "Archive"}</span></button></div></article>`).join("") || `<div class="empty-state compact"><h3>${state.showArchived ? "No archived experiments" : "No experiments yet"}</h3></div>`}</div></div>`;
}

function welcomeView(): string {
  return `<div class="welcome"><h1>What hypothesis should the harness test?</h1><p>Describe the market behavior in plain language. The world model will create a versioned hypothesis, select context, and start a supervised Jev loop.</p><div class="welcome-notes"><span>Rust-authoritative state</span><span>No per-trade approvals</span><span>Full canonical replay</span></div></div>`;
}

function tabContent(): string {
  if (state.loading) return `<div class="loading-view"><span class="spinner"></span><p>Hydrating canonical workspace…</p></div>`;
  if (state.tab === "positions") return positionsView();
  if (state.tab === "evidence") return evidenceView();
  if (state.tab === "history") return historyView();
  return activityView();
}

function mainPanel(): string {
  const snapshot = currentSnapshot(); const historyRun = state.workspace?.runHistory.find((run) => run.runId === state.selectedRunId); const title = state.tab === "history" ? "All experiments" : historyRun?.thesis || snapshot?.thesis || "New autonomous run";
  const stopping = snapshot !== null && state.stoppingRunId === snapshot.runId;
  return `<main class="workbench-main ${state.tab === "history" ? "history-mode" : ""}"><header class="workbench-header"><div class="header-title"><div><h1>${escapeHtml(title)}</h1></div></div><div class="header-actions">${snapshot && state.tab !== "history" ? `<button class="secondary-button danger" id="stop-run" type="button" ${stopping ? "disabled" : ""}>${stopping ? `<span class="spinner small"></span>` : icons.stop}<span>${stopping ? "Stopping…" : "Stop run"}</span></button>` : ""}</div></header><nav class="tabs" aria-label="Run views">${(["activity", "positions", "evidence"] as Tab[]).map((tab) => `<button type="button" data-tab="${tab}" class="${state.tab === tab ? "is-active" : ""}">${icons[tab === "positions" ? "position" : tab]}<span>${tab[0].toUpperCase() + tab.slice(1)}</span>${tab === "activity" && snapshot ? `<em>${snapshot.events.length}</em>` : ""}</button>`).join("")}${state.tab === "history" ? `<button type="button" data-tab="history" class="is-active">${icons.history}<span>View all</span></button>` : ""}</nav>${state.notice ? `<div class="notice" data-tone="${state.noticeTone}"><span>${escapeHtml(state.notice)}</span><button aria-label="Dismiss notice" id="dismiss-notice" type="button">${icons.close}</button></div>` : ""}<section class="workbench-scroll" id="workbench-scroll">${tabContent()}</section>${state.tab === "history" ? "" : `<form class="composer" id="run-composer"><div class="composer-shell"><textarea id="thesis-input" rows="2" placeholder="Describe a trading hypothesis or vague market behavior…" ${state.loading ? "disabled" : ""}>${escapeHtml(promptDraft)}</textarea><div class="composer-footer"><div><span class="composer-mode">Autonomous</span><span class="composer-detail">World model → Jev → deterministic harness</span></div><button class="send-button" type="submit" title="Start run (Ctrl+Enter)">${state.loading ? `<span class="spinner small"></span>` : icons.arrow}</button></div></div></form>`}</main>`;
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
  const loopContent = loop && hypothesis ? `<section class="inspector-section hero-detail"><div class="loop-title"><span class="loop-state-dot"></span><div><h3>${escapeHtml(hypothesis.instruments.join(" · "))}</h3><p>${loop.parentLoopId ? "Spawned hypothesis" : "Original hypothesis"}</p></div>${statusChip(loop.state)}</div></section><section class="inspector-section"><h3>Hypothesis</h3><p class="inspector-copy">${escapeHtml(hypothesis.strategyMechanism)}</p><dl>${definitionRow("Timeframe", hypothesis.timeframe.label)}${definitionRow("Thesis version", `v${loop.thesisVersion}`)}${definitionRow("Context version", `v${loop.contextVersion}`)}${definitionRow("Loop ID", loop.id, true)}</dl></section><section class="inspector-section"><h3>Latest Jev decision</h3>${decision ? `<div class="decision-callout"><div><strong>${escapeHtml(decision.action)}</strong><span>${(decision.confidence * 100).toFixed(0)}% confidence</span></div><p>${escapeHtml(decision.rationale)}</p></div>` : `<p class="muted">No Jev decision recorded.</p>`}<dl>${definitionRow("Stage", decision?.stage)}${definitionRow("Position", position ? `${position.direction} · ${position.state}` : "Flat")}${definitionRow("Capital allocation", allocation ? `${(allocation.fraction * 100).toFixed(1)}%` : `${(loop.allocatedFraction * 100).toFixed(1)}%`)}</dl></section>${liveContextSection(decision)}${reviewSection}<section class="inspector-section"><h3>Selected context</h3>${context?.items.map((item) => `<div class="context-item"><strong>${escapeHtml(item.sourceId)}</strong><span>${escapeHtml(item.content)}</span><time>${dateTime(item.observedAt)}</time></div>`).join("") || `<p class="muted">No context items available.</p>`}</section>` : "";
  const empty = `<section class="empty-inspector"><span>${icons.inspect}</span><h3>Select a Jev loop</h3><p>Loop thesis, context, Jev state, position, confidence, and capital allocation will appear here.</p></section>`;
  return `<div class="resize-handle resize-inspector" aria-hidden="true"></div><aside class="inspector ${preferences.inspectorCollapsed ? "is-collapsed" : ""}"><div class="inspector-header"><div><h2>${evidence ? "Evidence record" : loop ? "Selected Jev loop" : "Run context"}</h2></div><button class="icon-button" id="close-inspector" type="button" aria-label="Close context">${icons.close}</button></div><div class="inspector-scroll">${evidenceContent || loopContent || empty}</div></aside>`;
}

function settingsModal(): string {
  if (!state.settingsOpen) return "";
  const settings = state.connectorSettings;
  const content = !settings
    ? `<div class="settings-loading"><span class="spinner"></span><p>Loading local connector configuration…</p></div>`
    : `<form id="connector-settings-form"><section class="adapter-grid"><label><span>World model</span><select name="worldModelAdapter"><option value="openrouter" ${settings.worldModelAdapter === "openrouter" ? "selected" : ""}>OpenRouter</option><option value="simulated" ${settings.worldModelAdapter === "simulated" ? "selected" : ""}>Simulated</option></select></label><label><span>Jev engine</span><select name="jevAdapter"><option value="typesafe" ${settings.jevAdapter === "typesafe" ? "selected" : ""}>TypeSafe Jev</option><option value="simulated" ${settings.jevAdapter === "simulated" ? "selected" : ""}>Simulated</option></select></label><label><span>Execution broker</span><select name="brokerAdapter"><option value="ctrader-fix" ${settings.brokerAdapter === "ctrader-fix" ? "selected" : ""}>cTrader FIX</option><option value="simulated" ${settings.brokerAdapter === "simulated" ? "selected" : ""}>Simulated</option></select></label></section><div class="connector-sections">${settings.sections.map((section) => `<section class="connector-section"><header><div><h3>${escapeHtml(section.title)}</h3><p>${escapeHtml(section.description)}</p></div><span>${section.fields.filter((field) => field.configured).length}/${section.fields.length} set</span></header><div class="connector-fields">${section.fields.map((field) => `<label class="connector-field"><span>${escapeHtml(field.label)}${field.required ? `<em>required</em>` : ""}</span><div><input name="${escapeHtml(field.key)}" type="${field.kind === "secret" ? "password" : "text"}" value="${escapeHtml(field.value)}" placeholder="${escapeHtml(field.placeholder)}" autocomplete="off" spellcheck="false"/><i data-configured="${field.configured}">${field.configured ? "Stored" : "Not set"}</i></div></label>`).join("")}</div></section>`).join("")}</div><footer class="settings-actions"><p>Settings remain on this computer in the project’s local configuration. Connector changes apply after restarting the app.</p><div><button class="secondary-button" id="cancel-settings" type="button">Cancel</button><button class="primary-button" type="submit" ${state.settingsSaving ? "disabled" : ""}>${state.settingsSaving ? "Saving…" : "Save settings"}</button></div></footer></form>`;
  return `<div class="settings-backdrop" id="settings-backdrop"><section class="settings-dialog" role="dialog" aria-modal="true" aria-labelledby="settings-title"><header class="settings-header"><div><h2 id="settings-title">API keys & connectors</h2><p>Configure the model, context, price, and execution connections used by this harness.</p></div><button class="icon-button" id="close-settings" type="button" title="Close">${icons.close}</button></header><div class="settings-update"><div><strong>Espeon updates</strong><span>${escapeHtml(updateStatus?.message ?? "Checks automatically on launch and every six hours.")}</span></div><button class="secondary-button" id="check-updates" type="button" ${updateChecking ? "disabled" : ""}>${updateChecking ? "Checking…" : "Check now"}</button></div><div class="settings-scroll">${content}</div></section></div>`;
}

function renameModal(): string {
  if (!state.renamingRunId) return "";
  const run = state.workspace?.runHistory.find((item) => item.runId === state.renamingRunId);
  if (!run) return "";
  return `<div class="rename-backdrop" id="rename-backdrop"><section class="rename-dialog" role="dialog" aria-modal="true" aria-labelledby="rename-title"><header><h2 id="rename-title">Rename experiment</h2><button class="icon-button" id="close-rename" type="button" aria-label="Close">${icons.close}</button></header><form id="rename-run-form"><label for="run-name">Name</label><input id="run-name" name="name" value="${escapeHtml(run.thesis)}" maxlength="120" required autocomplete="off"/><footer><button class="secondary-button" id="cancel-rename" type="button">Cancel</button><button class="primary-button" type="submit">Save</button></footer></form></section></div>`;
}

function render(): void {
  document.documentElement.dataset.theme = preferences.theme;
  document.documentElement.classList.toggle("is-tauri", harnessService.isDesktop);
  document.documentElement.style.setProperty("--sidebar-width", `${sidebarIsCollapsed() ? 0 : preferences.sidebarWidth}px`);
  document.documentElement.style.setProperty("--inspector-width", `${inspectorIsCollapsed() ? 0 : preferences.inspectorWidth}px`);
  app.innerHTML = `${windowChrome()}<div class="app-shell ${sidebarIsCollapsed() ? "sidebar-collapsed" : ""} ${inspectorIsCollapsed() ? "inspector-collapsed" : ""} ${state.tab === "history" ? "history-open" : ""}">${sidebar()}${mainPanel()}${inspector()}</div>${settingsModal()}${renameModal()}`;
  renderIcons(app);
  bindInteractions();
  bindWindowChrome();
}

async function hydrate(selectRun = true): Promise<void> {
  try {
    state.workspace = await harnessService.hydrate();
    if (selectRun && !state.selectedRunId) state.selectedRunId = state.workspace.activeRuns[0]?.runId ?? state.workspace.runHistory.find((run) => !run.archived)?.runId ?? null;
    if (state.selectedRunId && harnessService.isDesktop) await loadRun(state.selectedRunId, false);
    state.notice = harnessService.isDesktop ? "Workspace restored from canonical state." : "Browser preview — launch the Tauri app to connect the Rust harness.";
    state.noticeTone = harnessService.isDesktop ? "success" : "neutral";
  } catch (error) { state.notice = `Workspace hydration failed: ${String(error)}`; state.noticeTone = "error"; }
  finally { state.loading = false; render(); }
}

async function loadRun(runId: string, shouldRender = true): Promise<void> {
  state.selectedRunId = runId; state.selectedEvidenceId = null;
  if (harnessService.isDesktop) {
    try { const [replay, snapshot] = await Promise.all([harnessService.replayRun(runId), harnessService.getRunSnapshot(runId)]); state.replay = replay; state.selectedSnapshot = snapshot; const loops = state.replay.loops; if (!loops.some((loop) => loop.id === state.selectedLoopId)) state.selectedLoopId = loops[0]?.id ?? null; }
    catch (error) { state.notice = `Could not replay run: ${String(error)}`; state.noticeTone = "error"; }
  }
  if (shouldRender) render();
}

async function startRun(): Promise<void> {
  const thesis = document.querySelector<HTMLTextAreaElement>("#thesis-input")?.value.trim() ?? "";
  if (!thesis) { state.notice = "Enter a thesis or market behavior first."; state.noticeTone = "error"; render(); return; }
  state.loading = true; state.notice = "Starting the local autonomous run…"; state.noticeTone = "neutral"; render();
  try {
    const snapshot = await harnessService.startRun(thesis); promptDraft = ""; await hydrate(false); state.selectedRunId = snapshot.runId; state.selectedLoopId = snapshot.loops[0]?.id ?? null; await loadRun(snapshot.runId, false); state.tab = "activity";
    state.notice = "Autonomous run started. The harness no longer requires per-trade approval."; state.noticeTone = "success";
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
  catch (error) { state.notice = String(error); state.noticeTone = "error"; }
  finally { state.stoppingRunId = null; render(); if (updateStatus?.state === "waiting") void checkForUpdates(); }
}

async function checkForUpdates(): Promise<void> {
  if (!harnessService.isDesktop || updateChecking) return;
  updateChecking = true;
  if (state.settingsOpen) render();
  try {
    updateStatus = await harnessService.checkForUpdates();
    if (updateStatus.state === "waiting") {
      state.notice = updateStatus.message;
      state.noticeTone = "neutral";
      if (updateRetryTimer !== null) window.clearTimeout(updateRetryTimer);
      updateRetryTimer = window.setTimeout(() => void checkForUpdates(), 60_000);
    } else if (updateRetryTimer !== null) {
      window.clearTimeout(updateRetryTimer);
      updateRetryTimer = null;
    }
  } catch (error) {
    updateStatus = { state: "error", message: `Update check failed: ${String(error)}`, version: null };
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

async function renameRun(form: HTMLFormElement): Promise<void> {
  const runId = state.renamingRunId;
  const name = String(new FormData(form).get("name") ?? "").trim();
  if (!runId || !name) return;
  try {
    await harnessService.renameRun(runId, name);
    state.workspace = await harnessService.hydrate();
    state.renamingRunId = null;
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
    state.showArchived = archived;
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

function closeSettings(): void {
  if (state.settingsSaving) return;
  state.settingsOpen = false;
  render();
}

async function saveSettings(form: HTMLFormElement): Promise<void> {
  const data = new FormData(form);
  const values: Record<string, string> = {};
  for (const section of state.connectorSettings?.sections ?? []) {
    for (const field of section.fields) values[field.key] = String(data.get(field.key) ?? "").trim();
  }
  const update: ConnectorSettingsUpdate = {
    worldModelAdapter: String(data.get("worldModelAdapter") ?? "openrouter"),
    jevAdapter: String(data.get("jevAdapter") ?? "typesafe"),
    brokerAdapter: String(data.get("brokerAdapter") ?? "ctrader-fix"),
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
  if (window.innerWidth <= 820) {
    compactSidebarOpen = !compactSidebarOpen;
    if (compactSidebarOpen) compactInspectorOpen = false;
    render();
    return;
  }
  preferences.sidebarCollapsed = !preferences.sidebarCollapsed;
  localStorage.setItem("jev.ui.sidebar-collapsed", String(preferences.sidebarCollapsed));
  render();
}

function toggleInspector(): void {
  if (window.innerWidth <= 1080) {
    compactInspectorOpen = !compactInspectorOpen;
    if (compactInspectorOpen) compactSidebarOpen = false;
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
    if (window.innerWidth <= 820) compactSidebarOpen = false;
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

function bindInteractions(): void {
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
  document.querySelector("#view-all-history")?.addEventListener("click", () => { state.tab = "history"; state.showArchived = false; render(); });
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
  document.querySelector("#settings-backdrop")?.addEventListener("mousedown", (event) => { if (event.target === event.currentTarget) closeSettings(); });
  document.querySelector("#connector-settings-form")?.addEventListener("submit", (event) => { event.preventDefault(); void saveSettings(event.currentTarget as HTMLFormElement); });
  document.querySelector("#toggle-theme")?.addEventListener("click", () => { preferences.theme = preferences.theme === "dark" ? "light" : "dark"; localStorage.setItem("jev.ui.theme", preferences.theme); render(); });
  document.querySelector("#toggle-sidebar")?.addEventListener("click", toggleSidebar);
  document.querySelector("#chrome-search")?.addEventListener("click", focusWorkspaceSearch);
  document.querySelector("#toggle-inspector")?.addEventListener("click", toggleInspector);
  document.querySelector("#close-inspector")?.addEventListener("click", toggleInspector);
  document.querySelector("#new-run")?.addEventListener("click", startNewRun);
  bindResize(".resize-sidebar", "sidebar"); bindResize(".resize-inspector", "inspector");
}

window.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && state.renamingRunId) { event.preventDefault(); closeRename(); return; }
  if (event.key === "Escape" && state.settingsOpen) { event.preventDefault(); closeSettings(); return; }
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
  compactSidebarOpen = false;
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
    render();
  },
  (error) => { state.notice = `Harness degraded: ${error.message}`; state.noticeTone = "error"; render(); },
);
void hydrate().then(() => {
  if (!harnessService.isDesktop) return;
  window.setTimeout(() => void checkForUpdates(), 5_000);
  window.setInterval(() => void checkForUpdates(), 6 * 60 * 60_000);
});
