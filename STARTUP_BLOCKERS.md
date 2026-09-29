# Espeon startup blockers and latency map

**Source of truth checked:** 2026-09-27. This document describes the current working tree. Earlier versions described the pre-refactor synchronous path and are superseded here.

## Three different waits

1. **App launch:** process start until the Tauri window appears.
2. **Workspace hydration:** UI loads the canonical workspace and selected run.
3. **Trade readiness:** connectors have fresh market data and metadata, a DRAFT contract finishes warm-up/activation, and the next supervised cycle can make a decision.

These are separate. A visible app or an “active” run does not establish that a connector is ready to trade.

## Current startup path

```mermaid
sequenceDiagram
  participant T as Tauri process
  participant A as AppState::load
  participant DB as SQLite / canonical replay
  participant C as Connector workers
  participant W as Tauri window + UI
  participant R as Retrieval worker
  participant L as Run loop

  T->>A: resolve project root and load configuration
  A->>DB: open store, restore active runs by replay
  A->>C: construct adapters; start FIX/cache and MCP validator workers
  A-->>T: return state
  T->>W: create window
  W->>A: hydrate workspace and selected run
  W->>R: Qdrant initialization/indexing runs asynchronously
  W->>L: start supervised cycles for restored runs
  L->>C: reconcile broker truth before decisions or new entries
  L->>L: resolve fresh context; keep required-context contracts in DRAFT during warm-up
  L->>L: activate validated contract, spawn Jev loop, then make Jev decision
```

`AppState::load()` is still synchronous before Tauri creates the window. Current inspection found local config/store initialization and canonical replay on that path. Qdrant starts after setup, TypeSafe model discovery is lazy on first inference, and broker reconciliation happens in the supervised run cycle after the window exists.

## App-launch blockers

| Stage | Current behavior | What can block or fail |
| --- | --- | --- |
| Project root/config | Uses `ESPEON_PROJECT_ROOT`, source checkout config, or `%LOCALAPPDATA%\\EspeonData`; validates/loads `config/harness.json`. | Invalid config, inaccessible directories, or filesystem errors abort state initialization; `run()` currently expects `AppState::load()` to succeed, so fatal initialization can prevent the window. |
| SQLite/runtime store | Opens canonical SQLite state and schema before Tauri setup. | DB open, migration, lock, corruption, or filesystem errors can prevent launch. |
| Adapter construction | Builds OpenRouter/TypeSafe clients and FIX/cache/MCP background workers from configured adapters. TypeSafe model discovery is deferred until inference. | Invalid adapter configuration can select unavailable adapters; FIX/cache thread or local cache setup can degrade the connector. Network logon and MCP metadata work are asynchronous and should not hold window creation. |
| Active-run replay | Reconstructs active runs and pending startup DRAFT contracts before the window. It does not reconcile broker state here. | Replay or required canonical-store reads/writes can fail initialization. Work can grow with active runs and event history; there is no measured startup profile in the repository. |
| Window/hydration | Tauri creates the window after state load. The UI then hydrates workspace and selected-run state. | Large replay/snapshot payloads or command errors can delay populated UI after the window appears. |

Qdrant Python/FastEmbed initialization is no longer a pre-window blocker. Each bridge invocation has a 45-second startup/operation limit because each request launches Python and initializes the local embedding models. Retrieval failure is logged/degraded and does not prevent the first window. A cold embedding-model setup can still delay retrieval availability after launch.

## Run start and first-decision path

Starting a run still waits for the world model to formulate/repair and validate the contract. Provider calls and bounded repair/escalation can make the Start command slow. Once required contract records are committed, `HarnessController::start()` returns without synchronously running Jev or sending an order; `start_run` then starts the supervised loop worker.

If required market context is missing, the run is persisted with a DRAFT contract and a visible `live_context_resolution_failed` warm-up event. The worker retries context resolution. It must validate fresh quote/candles/formulas before activation and Jev invocation. A failed formulation or startup commit is canonically stopped; restore also stops incomplete startup graphs. The first decision may therefore happen well after the Start response.

The run loop is flagged for an immediate first cycle, so normal strategy cadence is not a prerequisite for the initial Jev attempt. After the window exists, that first decision can still wait on worker dispatch and its prerequisite work:

- FIX logon, a fresh quote, and enough completed candles;
- external backfill/stream availability and formula resolution;
- MCP account/symbol/volume metadata readiness for entry validation;
- OpenRouter/TypeSafe latency, confidence/risk gates, and broker response.

Controller work uses a shared mutex. A long world-model formulation/repair or a slow run cycle can serialize other commands that need controller state, including other starts and some UI operations. This is a code-level latency risk, but there is no phase timing proving it is the cause of a particular delay.

An earlier independent MCP Inspector probe returned HTTP 404 on 2026-09-27. On 2026-09-28, Espeon's UI reported a successful read-only account response from cTrader Demo. That confirms account lookup worked at that time; live order execution remains unverified. The successful real-model end-to-end test uses `SimulatedBroker` and does not prove live FIX order execution.

## What has been verified and what has not

- **Simulated trading verified on 2026-09-27:** the configured OpenRouter → TypeSafe Jev → SimulatedBroker end-to-end test completed. Jev chose Long at 1.000 confidence, deterministic risk accepted the order, one simulated position opened, and the decision carried fresh dynamic context and the one-trade lifecycle cap. The test uses `SimulatedBroker`; it does not prove cTrader live connectivity or execution.
- **Startup verified on 2026-09-27:** Espeon PID 17776 remained running with window title `Espeon`, and `http://127.0.0.1:1420/` returned HTTP 200 with page title `Espeon`.
- **Live cTrader order execution remains unverified:** an in-app read-only account response succeeded on 2026-09-28. No evidence in this repository establishes that a live cTrader order was accepted and reconciled. The one-cycle human-verified live-risk path is tested against local fixtures; it does not prove live execution.
- These checks do not provide a timed cold-start profile or prove live FIX/MCP order execution.

For a specific slow launch, capture process start, `AppState::load` begin/end, SQLite/replay duration, Tauri window setup, frontend hydration, first quote, candle warm-up, MCP metadata readiness, and first Jev response. Do not attribute “takes long to start” to a single cause without those timings; app launch, run start, and first-decision readiness have different critical paths.
