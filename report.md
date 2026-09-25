# Build Progress

## Phase 1 — Complete

- Built the local Tauri 2 shell, Rust harness, TypeScript UI, simulated adapters, SQLite storage, search, and start/stop lifecycle.

## Phase 2 — Complete

- Added immutable canonical records for thesis/context versions, decisions, executions, reviews, and capital allocations.
- Added current run, loop, and position state with full causal IDs.
- Linked every decision to exact thesis/context versions and every execution to its causing decision.
- Linked indexed memories to verified canonical records and events.
- Recorded spawned-loop parent hypothesis and creation-event provenance.
- Added deterministic event-only replay with causal-integrity validation.
- Verified replay still works after deleting the search index.
- Verified canonical events reject update and deletion.
- Passed 3 Rust integration tests, frontend build, Tauri release build, and executable launch test.

## Phase 3 — Complete

- Added a real project-local Qdrant context pool with dense MiniLM and sparse BM25 hybrid retrieval.
- Added provenance/trust metadata for live-official, historical, external-untrusted, and internal canonical records.
- Added transactional automatic indexing for every new canonical event.
- Added exact canonical lookup, semantic search, keyword retrieval, payload filters, and full-record inspection.
- Added the world model's narrow search → inspect → sufficiency → search-again → act workflow.
- Added provenance-required context ingestion for future source adapters.
- Added the master `.env`, tracked `.env.example`, required production key placeholders, and optional backup-provider keys.
- Passed all 4 integration tests, including real Qdrant retrieval, release build, and executable launch.

## Phase 4 — Complete

- Added complete versioned hypotheses for vague and precise prompts: instruments, mechanism, timeframe, context, Jev question, evidence criteria, and lifecycle rules.
- Preserved explicit user timeframes and added world-model timeframe selection when none is supplied.
- Added immutable keep, modify, split, and stop review decisions with exact evidence references and deterministic replay.
- Kept loop creation/stopping, timeframe-to-Jev cadence mapping, capital reallocation, and execution exclusively in the Rust harness.
- Added the narrow world-model system prompt and context-only retrieval boundary.
- Preserved mandatory provenance classes and exact canonical records in ranked retrieval results.
- Passed all 5 integration tests, frontend build, formatting check, and optimized Rust build.

## Phase 5 — Implementation Complete; Live Sign-off Pending

- Added the real structured-output world-model adapter and activated it in local configuration.
- Added the real TypeSafe System One adapter with model discovery, typed Jev1/Jev2 Choice requests, strict response validation, and complete inference metadata.
- Added fresh structured context assembly with source IDs, timestamps, provenance, position-minimum state, and private harness-control exclusion.
- Added cadence-driven autonomous cycles for every active loop, separate inference-error events, paper execution, and automatic log re-indexing.
- Implemented HOLD, SELL, and BUY MORE state flow; BUY MORE requires a separate Jev1 confirmation.
- Passed 2 production HTTP contract tests, 6 integration tests, the local Qdrant test, frontend build, formatting check, and optimized Rust build.
- A live TypeSafe model-discovery/inference request passed using the configured Jev key.
- Final end-to-end live sign-off is pending only because no real world-model API key/model is configured.

## Phase 6 — Complete

- Added deterministic confidence/NO TRADE gates, stale-signal and duplicate-order rejection, allocation-based sizing, quantity rounding, stop losses, and account exposure/position limits.
- Made canonical approved orders and position controls mandatory at the broker boundary for both entries and exits; deterministic stops can override model HOLD decisions.
- Added equal capital recalculation on loop creation/change and zero allocation on loop termination, without exposing capital state to Jev.
- Added deterministic inference retry backoff, recovery clearing, and automatic loop pausing after repeated failures.
- Added immutable order, position-control, and failure-state records with replay validation.
- Passed all Rust tests (including real local Qdrant), frontend production build, formatting check, and optimized Rust build. Broker remains simulated until Phase 7; AI key finalization remains Phase 9.

## Phase 7 — Implementation Complete; Demo Credentials Pending

- Replaced the selected simulated broker with a local cTrader FIX 4.4 adapter using separate price and trade sessions, optional TLS, reconnect/logon retries, live quote snapshots, market-order submission, fills, rejections, and position reports.
- Kept broker access entirely below the deterministic harness; Jev and the world model receive no broker credentials or broker API access.
- Added canonical fill/rejection details, cTrader position IDs, broker synchronization events, disconnect-safe state transitions, and broker-truth position reconciliation.
- Added canonical active-run restoration and automatic loop resumption after application restart.
- Replaced obsolete generic broker environment variables with the cTrader MCP/FIX split and adapter-specific required fields.
- Passed the socket-level quote → fill → reconcile test, broker failure/reconciliation tests, restart test, full Rust suite, local Qdrant test, frontend build, formatting check, and optimized build. The opt-in live demo smoke test is ready once cTrader demo credentials are entered.

## Phase 8 — Complete

- Enriched automatic canonical-event indexing with loop, thesis/context versions, instruments, timeframe, timestamps, outcomes, provenance and exact record/event links.
- Added cross-run historical retrieval with chronological output, exact canonical lookup, and supporting/contradictory/related evidence classification.
- Added immutable, replayable world-model review packages containing current state, recent Jev decisions, executions, positions, prior reviews, spawned hypotheses and historical evidence.
- Preserved KEEP/MODIFY/SPLIT/STOP and added a structured competing-hypothesis candidate to SPLIT without activating it as a Phase 8 requirement.
- Kept the memory and review-package interfaces provider/model-neutral; no Phase 9 provider or model routing is embedded.
- Passed 22 automated Rust tests, including the two-experiment real-Qdrant memory loop, exact decision inspection, immutable replay, broker tests and provider-independence checks, plus frontend, formatting and optimized builds.

## Phase 9 — Complete

- Added OpenRouter routing with GPT-6 Luna Pro as the base model and Claude Opus 5.5 as deterministic escalation through the existing world-model adapter.
- Added controlled two-step web research, canonical external-untrusted evidence ingestion, explicit publication/event/retrieval dates, and deterministic stale/undated primary-evidence rejection.
- Added stable regression coverage for escalation, new hypotheses, contradictory evidence, tool selection, provenance, recency, stale rejection, action schemas, and authority boundaries.
- Passed the live base review, live time-sensitive web/recency flow, and live base-to-Opus escalation smoke tests using the configured OpenRouter key.

## Phase 10 — Complete

- Replaced the basic dashboard with a resizable Codex-style Espeon interface using locally packaged IBM Plex Sans, light/dark themes, keyboard navigation, a run composer, loop/thread sidebar, central activity views, contextual inspector and integration status bar.
- Added Rust-supplied workspace hydration, historical snapshots, run history and non-secret integration health plus live Tauri snapshot/error events and restart recovery.
- Added canonical positions/execution, evidence/provenance, world-model/Jev/harness activity and audit-history surfaces without moving trading authority into TypeScript.
- Added MIT attribution, responsive visual verification at 1440×960 and 900×700, production frontend compilation and Tauri NSIS packaging configuration.

## In-app connector configuration — Complete

- Added an in-app API Keys & Connectors panel for OpenRouter, TypeSafe Jev, cTrader MCP, and separate cTrader FIX price/trade sessions.
- Secrets are masked, existing secret values can be retained without re-entry, and settings persist only to the local `.env` and harness configuration.
- Missing credentials now leave the requested adapter explicitly unavailable instead of crashing the Tauri application or silently selecting simulation.
- Rebuilt the release executable and NSIS installer; all 37 automated tests passed and the responsive native Espeon window was verified running.

## Broker-state and retrieval hardening — Complete

- Fixed Qdrant query normalization at both the Rust/Python boundary and embedding boundary, with regression tests for plain, legacy-list, empty, and invalid structured inputs.
- Made partial fills canonical: positions use confirmed cumulative fill quantity, remain explicitly partial until broker truth converges, and partial closes preserve remaining exposure.
- Made Stop flatten broker positions before finalizing a run; rejected or incomplete closes leave the run active for recovery.
- Reconciliation now requires a connected, complete broker snapshot, updates known quantities, and imports broker-only positions with canonical decisions, orders, executions, and controls.
- Added per-run cancellation tokens and execution-boundary checks so a sleeping or in-flight scheduler cannot begin another broker action after Stop.
- Updated deterministic replay for the new lifecycle, reconciliation, partial-fill, and broker-import events.
- Passed the complete Rust suite (36 passed, 5 credential-gated tests ignored), 4 Python bridge tests, frontend production build, optimized Tauri build, and NSIS packaging.

## Native Windows launch — Complete

- Compiled release builds as a Windows GUI application so Espeon runs without a command shell.
- Explicitly enabled the operating system's native decorated title bar.
- Suppressed console windows for the local Python/Qdrant child process.
- Rebuilt the executable and NSIS installer; verified PE subsystem `Windows GUI` and a responsive `Espeon` window.

## Qdrant root-cause and Jev connector verification — Complete

- Fixed the real Qdrant failure: Rust emitted UTF-8 JSON while the Python pipe used the Windows locale, corrupting smart quotes into surrogate characters rejected by FastEmbed's tokenizer.
- Made the bridge protocol explicitly strict UTF-8 in both directions, kept scalar embedding inputs, and added Unicode/process-boundary regression coverage.
- Added restore-time retrieval-outbox recovery after broker reconciliation; replayed the exact failed production records and verified zero pending records.
- Checked the Jev adapter against TypeSafe's current OpenAPI contract and passed live authenticated model discovery plus Jev1 and Jev2 Choice requests with validated confidence/probabilities.
- Passed 6 Python boundary tests, the full Rust suite, the offline end-to-end Qdrant test, frontend production build, Tauri release build, and NSIS packaging.

## cTrader broker-sync configuration — Complete

- Replaced dotenv-incompatible Windows backslash paths and normalized the FIX symbol map syntax.
- Made cTrader configuration parsing surface its real `.env` error instead of silently degrading to misleading missing-field messages.
- Corrected the FIX transport pairing: shared FIX hostname, TLS enabled, price port 5211, and trade port 5212.
- Rebuilt and restarted Espeon; restored runs now record complete cTrader FIX synchronization with `connected=true` and zero open broker positions.

## TypeSafe and responsive Stop recovery — Complete

- Stopped both remaining active runs in canonical state; no active runs or loops remain and broker truth reported no open positions.
- Verified the previously rejected production Jev1 payloads against TypeSafe: the unchanged requests now succeed, confirming the request schema is valid and the earlier HTTP 400 was transient upstream behavior.
- Added one bounded retry for transient TypeSafe 400/timeout/rate-limit/server failures and now preserve the sanitized provider response body plus request ID when a rejection persists.
- Moved network- and storage-heavy Tauri commands off the native event loop, made Stop signal cancellation immediately, and added a visible stopping state so the window remains responsive while broker flattening completes.
- Fixed stopped historical runs being mistaken for active runs in the UI, which previously left a stale Stop button visible.
- Passed the live authenticated Jev1/Jev2 smoke test, all connector and lifecycle tests, the complete Rust/Qdrant suite, frontend production build, optimized Tauri build, and NSIS packaging.

## TypeSafe input-budget fix — Complete

- Found the `max_tokens_exceeded` root cause: the harness appended up to 12 full Qdrant hits after constructing Jev state, with no final request-size budget.
- Added deterministic post-retrieval compaction that removes duplicate evidence, limits field/name/source sizes, caps the serialized Jev state, and records included-versus-available field counts.
- Kept complete context and provenance immutable in canonical storage; only the model-bound Jev decision view is compacted, and exact thesis/context version IDs remain attached.
- Passed a live TypeSafe call with an intentionally oversized 20-record state after compaction, plus the complete Rust/Qdrant suite and frontend production build.

## Autonomous world-model reviews — Complete

- Added replayable no-trade, consecutive-loss, and periodic-trade triggers with horizon-scaled thresholds, combined reasons, restart recovery, and immutable checkpoints.
- Added broker-fill-derived round-trip outcomes with partial-close aggregation, gross P&L classification, exact execution references, and unknown-basis exclusion for imported positions.
- Runs OpenRouter review work outside the controller lock with cancellation, deterministic retry history, stale-result rejection, and loss-triggered entry blocking.
- Enforced autonomous KEEP/MODIFY/STOP/SPLIT lifecycle rules, deterministic flattening, mandatory SPLIT escalation/evidence/capacity checks, immediate child scheduling, and capital redistribution.
- Added local in-app review policy settings plus Activity/inspector visibility for counters, reasons, routing, diagnosis, actions, and outcomes.

## Live Jev context pipeline — Complete

- Added versioned typed live-context formula ASTs, deterministic Rust validation/evaluation, and fresh quote/completed-candle context on every Jev cycle.
- Added persistent cTrader FIX quote workers, Open API trendbar backfill, local UTC bar aggregation, cache gap repair, request throttling, and token-refresh fallback.
- Added immutable canonical quote, candle, and resolved-context snapshots with exact decision references and replay validation.
- Missing, stale, incomplete, or invalid required market data now records `live_context_resolution_failed`, skips Jev, and never creates a synthetic `NO TRADE`; deterministic stop checks remain independent.
- Verified the real cTrader Demo FIX execution path with a broker-confirmed BTCUSD round trip: 0.01 BTC bought at 83,917.78 and closed at 83,913.70; final broker reconciliation reported flat. The broker rejected 0.0001 BTC and confirmed two-decimal quantity precision, making 0.01 BTC the minimum for this symbol.
- Fixed the cTrader FIX price protocol at the root: market-data requests now use supported subscription mode `263=1`, include incremental refresh mode `265=1`, preserve cTrader's required repeating-group order, and surface `35=Y` rejection details instead of timing out. Verified both one-shot price retrieval and the persistent live bid/ask worker against cTrader Demo, passed all 29 normal Rust tests, and rebuilt the release executable.
- Added all Open API/history settings to in-app Connectors and exposed live quote, latest candle, formula values, provenance, snapshot ID, and freshness in the loop inspector.
- Passed Rust compilation/unit tests, targeted lifecycle/replay tests, and the frontend production build.

## Espeon desktop startup — Complete

- Moved the localhost Vite URL into a development-only Tauri config. The normal build embeds the frontend and opens without a local server.
- Renamed the window, installer, executable, page title, and visible app branding to Espeon while preserving the existing application identifier and runtime state.
- Built the release executable and NSIS installer, then launched the executable from a different working directory. The window loaded the Espeon UI and restored canonical state.

## Espeon window chrome — Complete

- Adapted Loci Lite's permanent frameless titlebar, draggable center, window controls, and themed frame for Espeon's plain TypeScript UI.
- Moved sidebar, New run, search, settings, theme, and inspector controls into the titlebar; kept Stop run and run views beside the current experiment.
- Removed in-app brand marks while retaining the executable and desktop shortcut icon.
- Built the frontend, release executable, and NSIS installer, then launched the packaged UI and visually checked the titlebar at Windows display scaling.
- Consolidated app actions on the left of the titlebar and replaced UI SVGs with Lucide icons, including sidebar navigation and window controls.
- Removed the Local harness status strip and Local runtime sidebar footer, allowing the content and compact panels to reach the bottom edge.
- Rebuilt the release executable and installer, launched Espeon from the desktop target, and checked the open and collapsed sidebar layouts.

## Espeon icon and positions design — Complete

- Recorded the permanent Lucide-only interface icon rule in `AGENTS.md` and routed the context control through the shared Lucide icon map.
- Replaced the right sidebar toggle with a layers icon, changed its label to Context, and removed the experiment eyebrow above the run title.
- Added a dark Positions chart inspired by the supplied visual. It plots cumulative realized P&L from canonical completed trade outcomes, supports period selection, and shows an honest empty state when a run has no known P&L.
- Filled the Positions table P&L cells from those same outcomes where a cost basis is known.
- Matched the Positions chart to both page themes, removed its subtitle and footnote, and moved P&L methodology to hover help.
- Reframed the supplied Espeon mark and regenerated Windows executable and desktop icon assets from it.

## Smaller desktop app and experiment history — Complete

- Reduced the default Espeon window to 1180 × 760 and regenerated the desktop icon with a smaller centered mark and rounded tile corners. The brand mark remains outside the in-app UI.
- Added View all beside Experiment history in the sidebar. It opens a full-width experiment list with Recent and Archived views while the sidebar shows only recent experiments.
- Added persistent experiment rename, archive, and restore actions. Display names and archive state live in a separate presentation table, leaving canonical run records intact. Active experiments must be stopped before archival.
- Built the frontend, release executable, and NSIS installer. Visual inspection is reserved for the user.
- Made the run prompt grow and shrink with its text, capped the full composer at half the visible workbench, and enabled internal scrolling beyond that cap. Preserved draft text across UI redraws. Rebuilt the release executable and installer.
