import { invoke } from "@tauri-apps/api/core";
import { listen, type UnlistenFn } from "@tauri-apps/api/event";
import type { ConnectorSettings, ConnectorSettingsUpdate, IntegrationStatus, ReplayState, RunSnapshot, SearchHit, UpdateStatus, WorkspaceSnapshot } from "../types";

const isTauri = "__TAURI_INTERNALS__" in window;

const browserPreview: WorkspaceSnapshot = {
  activeRuns: [],
  runHistory: [],
  hydratedAt: new Date().toISOString(),
  integrations: [
    { id: "harness", label: "Rust harness", state: "degraded", detail: "Open in the Tauri desktop app" },
    { id: "sqlite", label: "Canonical store", state: "degraded", detail: "Desktop runtime unavailable" },
    { id: "qdrant", label: "Qdrant", state: "degraded", detail: "Desktop runtime unavailable" },
    { id: "openrouter", label: "OpenRouter", state: "disabled", detail: "Status supplied by Rust" },
    { id: "jev", label: "TypeSafe Jev", state: "disabled", detail: "Status supplied by Rust" },
    { id: "mcp", label: "cTrader MCP", state: "disabled", detail: "Status supplied by Rust" },
    { id: "fix-price", label: "FIX price", state: "disabled", detail: "Status supplied by Rust" },
    { id: "fix-trade", label: "FIX trade", state: "disabled", detail: "Status supplied by Rust" },
  ],
};

export const harnessService = {
  isDesktop: isTauri,

  hydrate(): Promise<WorkspaceSnapshot> {
    return isTauri ? invoke("hydrate_workspace") : Promise.resolve(browserPreview);
  },

  marketHealth(): Promise<IntegrationStatus[]> {
    return isTauri ? invoke("get_market_health") : Promise.resolve([]);
  },

  checkForUpdates(): Promise<UpdateStatus> {
    return isTauri ? invoke("check_for_updates") : Promise.resolve({ state: "current", message: "Updates are available in the desktop app.", version: null });
  },

  renameRun(runId: string, name: string): Promise<void> {
    if (!isTauri) return Promise.reject(new Error("Rename experiments from the desktop application."));
    return invoke("rename_run", { runId, name });
  },

  setRunArchived(runId: string, archived: boolean): Promise<void> {
    if (!isTauri) return Promise.reject(new Error("Archive experiments from the desktop application."));
    return invoke("set_run_archived", { runId, archived });
  },

  startRun(thesis: string, continuationFromRunId: string | null = null): Promise<RunSnapshot> {
    if (!isTauri) return Promise.reject(new Error("Start runs from the Tauri desktop application."));
    return invoke("start_run", { thesis, continuationFromRunId });
  },

  steerRun(runId: string, instruction: string): Promise<RunSnapshot> {
    if (!isTauri) return Promise.reject(new Error("Steer runs from the Tauri desktop application."));
    return invoke("steer_run", { runId, instruction });
  },

  stopRun(runId: string): Promise<RunSnapshot> {
    if (!isTauri) return Promise.reject(new Error("Stop runs from the Tauri desktop application."));
    return invoke("stop_run", { runId });
  },

  replayRun(runId: string): Promise<ReplayState> {
    if (!isTauri) return Promise.reject(new Error("Canonical replay is available in the desktop application."));
    return invoke("replay_run", { runId });
  },

  getRunSnapshot(runId: string): Promise<RunSnapshot> {
    if (!isTauri) return Promise.reject(new Error("Canonical snapshots are available in the desktop application."));
    return invoke("get_run_snapshot", { runId });
  },

  searchContext(query: string): Promise<SearchHit[]> {
    if (!isTauri) return Promise.resolve([]);
    return invoke("search_context", { query });
  },

  getConnectorSettings(): Promise<ConnectorSettings> {
    if (!isTauri) return Promise.reject(new Error("Connector settings are available in the desktop application."));
    return invoke("get_connector_settings");
  },

  probeMcpConnection(): Promise<string> {
    if (!isTauri) return Promise.reject(new Error("cTrader MCP checks require the desktop application."));
    return invoke("probe_mcp_connection");
  },

  getLiveRiskAccountValues(): Promise<{ equity: number; freeMargin: number; observedAt: string }> {
    if (!isTauri) return Promise.reject(new Error("Live account values require the desktop application."));
    return invoke("get_live_risk_account_values");
  },

  approveHumanVerifiedLiveCycle(input: {
    runId: string;
    accountId: string;
    environment: string;
    instrument: string;
    depositCurrency: string;
    accountOpenExposure: number;
    quoteToDeposit: number;
    confirmed: boolean;
  }): Promise<RunSnapshot> {
    if (!isTauri) return Promise.reject(new Error("Live risk verification is available in the desktop application."));
    return invoke("approve_human_verified_live_cycle", {
      runId: input.runId,
      accountId: input.accountId,
      environment: input.environment,
      instrument: input.instrument,
      depositCurrency: input.depositCurrency,
      accountOpenExposure: input.accountOpenExposure,
      quoteToDeposit: input.quoteToDeposit,
      confirmed: input.confirmed,
    });
  },

  saveConnectorSettings(update: ConnectorSettingsUpdate): Promise<ConnectorSettings> {
    if (!isTauri) return Promise.reject(new Error("Connector settings are available in the desktop application."));
    return invoke("save_connector_settings", { update });
  },

  async subscribe(
    onSnapshot: (snapshot: RunSnapshot) => void,
    onError: (error: { runId?: string; message: string }) => void,
  ): Promise<UnlistenFn[]> {
    if (!isTauri) return [];
    return Promise.all([
      listen<RunSnapshot>("harness:snapshot", (event) => onSnapshot(event.payload)),
      listen<{ runId?: string; message: string }>("harness:error", (event) => onError(event.payload)),
    ]);
  },
};
