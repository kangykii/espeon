#![recursion_limit = "256"]

mod adapters;
mod config;
mod context;
mod contracts;
mod ctrader_fix;
mod domain;
mod evidence;
mod freshness;
mod harness;
mod jev_compiler;
mod market_cache;
mod market_data;
mod mcp_context;
mod ports;
mod replay;
mod retrieval;
mod risk;
mod skills;
mod storage;
mod typesafe;
mod updates;
mod world_model_api;

use domain::{
    ContextIngestRequest, ContextPoolRecord, HypothesisReviewRequest, IntegrationStatus,
    ReplayState, RetrievalRequest, RetrievalTrace, RunSnapshot, SearchHit, WorkspaceSnapshot,
    WorldModelReviewPackage,
};
use harness::HarnessController;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
use tauri::{AppHandle, Emitter, Manager, State};

struct AppState {
    controller: Arc<Mutex<HarnessController>>,
    canonical_store: storage::CanonicalStore,
    market_data: Arc<dyn ports::MarketDataProvider>,
    restored_run_ids: Vec<String>,
    project_root: PathBuf,
    retrieval_ready: Arc<AtomicBool>,
    retrieval_config: retrieval::RetrievalConfig,
    installing_update: Arc<AtomicBool>,
    run_workers: Mutex<HashMap<String, RunWorker>>,
}

struct RunWorker {
    cancellation: Arc<AtomicBool>,
    handle: JoinHandle<()>,
}

impl AppState {
    fn load() -> anyhow::Result<Self> {
        let source_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("src-tauri has project parent")
            .to_path_buf();
        let project_root = if let Ok(override_root) = std::env::var("ESPEON_PROJECT_ROOT") {
            PathBuf::from(override_root)
        } else if source_root.join("config").join("harness.json").is_file() {
            source_root
        } else {
            let local = std::env::var("LOCALAPPDATA")
                .map(PathBuf::from)
                .unwrap_or_else(|_| std::env::temp_dir());
            local.join("EspeonData")
        };
        let config_path = project_root.join("config").join("harness.json");
        if !config_path.is_file() {
            std::fs::create_dir_all(config_path.parent().expect("config parent"))?;
            let mut defaults: serde_json::Value =
                serde_json::from_str(include_str!("../../config/harness.json"))?;
            defaults["worldModelAdapter"] = "simulated".into();
            defaults["jevAdapter"] = "simulated".into();
            defaults["brokerAdapter"] = "simulated".into();
            std::fs::write(
                &config_path,
                serde_json::to_string_pretty(&defaults)? + "\n",
            )?;
        }
        let config = config::HarnessConfig::load(&project_root)?;
        let runtime_path = config.runtime_path(&project_root)?;
        let store = storage::CanonicalStore::open(&runtime_path)?;
        let canonical_store = store.clone();
        let retrieval_config = retrieval::RetrievalConfig::load(&project_root)?;
        let retrieval_ready = Arc::new(AtomicBool::new(false));
        let world_model: Box<dyn ports::WorldModel> = match config.world_model_adapter.as_str() {
            "openrouter" => match world_model_api::OpenRouterWorldModel::load(&project_root) {
                Ok(adapter) => Box::new(adapter),
                Err(error) => Box::new(adapters::UnavailableWorldModel(format!(
                    "OpenRouter unavailable: {error}"
                ))),
            },
            _ => Box::new(adapters::SimulatedWorldModel),
        };
        let jev: Box<dyn ports::JevEngine> = match config.jev_adapter.as_str() {
            "typesafe" => match typesafe::TypeSafeConfig::load(&project_root)
                .and_then(typesafe::TypeSafeJev::new)
            {
                Ok(adapter) => Box::new(adapter),
                Err(error) => Box::new(adapters::UnavailableJev(format!(
                    "TypeSafe Jev unavailable: {error}"
                ))),
            },
            _ => Box::new(adapters::SimulatedJev),
        };
        let (broker, market_data): (
            Box<dyn ports::ExecutionBroker>,
            Arc<dyn ports::MarketDataProvider>,
        ) = match config.broker_adapter.as_str() {
            "ctrader-fix" => match ctrader_fix::CTraderFixConfig::load(&project_root) {
                Ok(fix_config) => {
                    let quote_feed =
                        match ctrader_fix::CTraderFixQuoteFeed::start(fix_config.clone()) {
                            Ok(feed) => Some(feed),
                            Err(error) => {
                                eprintln!("cTrader FIX quote feed could not start: {error:#}");
                                None
                            }
                        };
                    let market_data: Arc<dyn ports::MarketDataProvider> = match quote_feed
                        .clone()
                        .ok_or_else(|| anyhow::anyhow!("cTrader FIX quote feed unavailable"))
                        .and_then(|feed| {
                            market_cache::MarketContextCacheProvider::start_with_feed(
                                &runtime_path,
                                &project_root,
                                fix_config.clone(),
                                feed,
                            )
                        }) {
                        Ok(provider) => Arc::new(provider),
                        Err(error) => Arc::new(market_data::UnavailableMarketDataProvider(
                            format!("local live-context cache unavailable: {error}"),
                        )),
                    };
                    (
                        Box::new(
                            ctrader_fix::CTraderFixBroker::with_entry_validation_and_quote_feed(
                                fix_config.clone(),
                                fix_config.validate_mcp_identity().and_then(|_| {
                                    mcp_context::McpExecutionValidator::start(
                                        &project_root,
                                        ctrader_fix::CTraderFixQuoteFeed::configured_symbols(
                                            &fix_config,
                                        ),
                                    )
                                }),
                                quote_feed,
                            ),
                        ),
                        market_data,
                    )
                }
                Err(error) => (
                    Box::new(adapters::UnavailableBroker(format!(
                        "cTrader FIX unavailable: {error}"
                    ))),
                    Arc::new(market_data::UnavailableMarketDataProvider(format!(
                        "cTrader FIX price feed unavailable: {error}"
                    ))),
                ),
            },
            _ => (
                Box::new(adapters::SimulatedBroker),
                Arc::new(market_data::SimulatedMarketDataProvider::new()),
            ),
        };
        let mut controller = HarnessController::with_optional_retrieval_adapters(
            store,
            config.minimum_confidence,
            None,
            world_model,
            jev,
            broker,
            config.risk_policy,
        );
        controller.configure_market_data(Arc::clone(&market_data));
        controller.configure_autonomous_reviews(config.autonomous_review.clone());
        let restored = controller.restore_active_runs()?;
        let controller = Arc::new(Mutex::new(controller));
        Ok(Self {
            controller,
            canonical_store,
            market_data,
            restored_run_ids: restored,
            project_root,
            retrieval_ready,
            retrieval_config,
            installing_update: Arc::new(AtomicBool::new(false)),
            run_workers: Mutex::new(HashMap::new()),
        })
    }
}

fn run_snapshot_from_store(
    store: &storage::CanonicalStore,
    run_id: &str,
) -> anyhow::Result<RunSnapshot> {
    let replay = replay::replay_run(store, run_id)?;
    Ok(RunSnapshot {
        run_id: run_id.to_owned(),
        status: replay.status,
        thesis: replay.human_thesis,
        hypotheses: replay.hypotheses,
        cadences: replay.cadences,
        loops: replay.loops,
        positions: replay.positions,
        events: store.events_for_run(run_id)?,
    })
}

fn workspace_snapshot_from_store(
    store: &storage::CanonicalStore,
    integrations: Vec<IntegrationStatus>,
) -> anyhow::Result<WorkspaceSnapshot> {
    let run_history = store.run_summaries()?;
    let active_runs = run_history
        .iter()
        .filter(|run| run.status == "active")
        .map(|run| run_snapshot_from_store(store, &run.run_id))
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(WorkspaceSnapshot {
        active_runs,
        run_history,
        integrations,
        hydrated_at: chrono::Utc::now(),
    })
}

fn env_setting(project_root: &std::path::Path, name: &str) -> Option<String> {
    crate::config::env_file_values(project_root)
        .ok()
        .and_then(|values| values.get(name).cloned())
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            std::env::var(name)
                .ok()
                .filter(|value| !value.trim().is_empty())
        })
}

fn configured(project_root: &std::path::Path, names: &[&str]) -> bool {
    names.iter().all(|name| {
        env_setting(project_root, name)
            .map(|value| {
                !value.starts_with("REQUIRED_")
                    && !value.starts_with("CHANGE_ME")
                    && !value.starts_with("YOUR_")
            })
            .unwrap_or(false)
    })
}

fn integration_statuses(
    project_root: &std::path::Path,
    config: &config::HarnessConfig,
    retrieval_ready: bool,
) -> Vec<IntegrationStatus> {
    let status = |id: &str, label: &str, state: &str, detail: &str| IntegrationStatus {
        id: id.into(),
        label: label.into(),
        state: state.into(),
        detail: detail.into(),
    };
    let openrouter = configured(project_root, &["OPENROUTER_API_KEY"]);
    let jev = configured(project_root, &["JEV_API_KEY"]);
    let mcp_enabled = env_setting(project_root, "CTRADER_MCP_ENABLED")
        .map(|value| value.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let mcp_identity_configured =
        mcp_enabled && configured(project_root, &["CTRADER_MCP_ACCOUNT_ID"]);
    let open_api_token = configured(project_root, &["CTRADER_OPEN_API_ACCESS_TOKEN"])
        || configured(project_root, &["CTRADER_OPEN_API_REFRESH_TOKEN"]);
    let open_api_market_data = configured(
        project_root,
        &[
            "CTRADER_OPEN_API_CLIENT_ID",
            "CTRADER_OPEN_API_CLIENT_SECRET",
        ],
    ) && open_api_token;
    let price_fix = configured(
        project_root,
        &["CTRADER_FIX_PRICE_HOST", "CTRADER_FIX_PRICE_USERNAME"],
    );
    let twelve_data = configured(
        project_root,
        &["TWELVE_DATA_API_KEY", "TWELVE_DATA_SYMBOL_MAP"],
    );
    let trade_fix = configured(
        project_root,
        &["CTRADER_FIX_TRADE_HOST", "CTRADER_FIX_TRADE_USERNAME"],
    );
    vec![
        status(
            "harness",
            "Rust harness",
            "connected",
            "Authoritative runtime ready",
        ),
        status(
            "sqlite",
            "Canonical store",
            "connected",
            "SQLite event store open",
        ),
        status(
            "qdrant",
            "Qdrant",
            if retrieval_ready {
                "connected"
            } else {
                "degraded"
            },
            if retrieval_ready {
                "Local context index ready"
            } else {
                "Local context index unavailable"
            },
        ),
        status(
            "openrouter",
            "OpenRouter",
            if openrouter { "configured" } else { "degraded" },
            if openrouter {
                "World-model routing configured"
            } else {
                "API key missing"
            },
        ),
        status(
            "jev",
            "TypeSafe Jev",
            if jev { "configured" } else { "degraded" },
            if jev {
                "Independent Jev provider configured"
            } else {
                "JEV_API_KEY missing"
            },
        ),
        status(
            "mcp",
            "cTrader MCP",
            if mcp_identity_configured {
                "configured"
            } else if mcp_enabled {
                "degraded"
            } else {
                "disabled"
            },
            if mcp_identity_configured {
                "Read-only account identity and detected environment configured; Open API supplies symbol lot size and volume rules"
            } else if mcp_enabled {
                "MCP account ID missing"
            } else {
                "Read-only broker lookup disabled; Demo can use its configured fixed quantity"
            },
        ),
        status(
            "ctrader-open-api",
            "cTrader Open API",
            if open_api_market_data {
                "configured"
            } else {
                "disabled"
            },
            if open_api_market_data {
                "Broker candles and automatic symbol lot-size/volume metadata configured"
            } else {
                "Optional broker metadata source not configured; Twelve Data and FIX price fallbacks remain available"
            },
        ),
        status(
            "twelve-data-market-data",
            "Twelve Data",
            if twelve_data {
                "configured"
            } else {
                "disabled"
            },
            if twelve_data {
                "REST candle confirmation and WebSocket price configured"
            } else {
                "Optional external data unavailable; qualified FIX price bars remain available"
            },
        ),
        status(
            "fix-price",
            "FIX price",
            if price_fix && config.broker_adapter == "ctrader-fix" {
                "configured"
            } else {
                "degraded"
            },
            if price_fix {
                "Price session credentials present"
            } else {
                "Price session not configured"
            },
        ),
        status(
            "fix-trade",
            "FIX trade",
            if config.broker_adapter != "ctrader-fix" {
                "disabled"
            } else {
                "degraded"
            },
            if config.broker_adapter != "ctrader-fix" {
                "Simulated broker selected"
            } else if trade_fix {
                "Open API discovers lot size and volume limits; account-validated MCP supplies fresh equity and free margin for each approved cycle"
            } else {
                "FIX trade session not configured; live entries require cTrader MCP account values and Open API symbol limits"
            },
        ),
        status(
            "broker-risk",
            "Broker risk sizing",
            if config.broker_adapter == "simulated" {
                "configured"
            } else {
                "degraded"
            },
            if config.broker_adapter == "simulated" {
                "Paper risk policy is scoped to the simulated broker"
            } else {
                "Fresh equity/free margin are fetched from account-validated MCP and symbol volume limits from Open API; the one-cycle form still asks for deposit currency, total open exposure, and quote-to-deposit conversion"
            },
        ),
    ]
}

fn emit_snapshot(app: &AppHandle, snapshot: &RunSnapshot) {
    if let Err(error) = app.emit("harness:snapshot", snapshot) {
        eprintln!(
            "Failed to deliver harness snapshot for run {}: {error}",
            snapshot.run_id
        );
    }
}

fn emit_error(app: &AppHandle, run_id: &str, message: &str) {
    eprintln!("Harness run {run_id}: {message}");
    if let Err(error) = app.emit(
        "harness:error",
        serde_json::json!({"runId":run_id,"message":message}),
    ) {
        eprintln!("Failed to deliver harness error for run {run_id}: {error}");
    }
}

fn report_worker_failure(
    controller: &Arc<Mutex<HarnessController>>,
    app: &AppHandle,
    run_id: &str,
    stage: &str,
    message: &str,
) {
    if let Err(error) = controller
        .lock()
        .record_worker_failure(run_id, stage, message)
    {
        eprintln!("Failed to persist {stage} worker failure for run {run_id}: {error:#}");
    }
    emit_error(app, run_id, message);
}

#[tauri::command]
async fn start_run(
    thesis: String,
    continuation_from_run_id: Option<String>,
    state: State<'_, AppState>,
    app: AppHandle,
) -> Result<RunSnapshot, String> {
    let controller = Arc::clone(&state.controller);
    let installing_update = Arc::clone(&state.installing_update);
    let snapshot = tauri::async_runtime::spawn_blocking(move || {
        let mut controller = controller.lock();
        if installing_update.load(Ordering::SeqCst) {
            return Err(
                "Espeon is installing an update; start the experiment after restart.".into(),
            );
        }
        match continuation_from_run_id.as_deref() {
            Some(source_run_id) => controller.continue_from_run(&thesis, source_run_id),
            None => controller.start(&thesis),
        }
        .map_err(|error| format!("{error:#}"))
    })
    .await
    .map_err(|error| format!("start-run worker failed: {error}"))??;
    let run_id = snapshot.run_id.clone();
    let cancellation = Arc::new(AtomicBool::new(false));
    emit_snapshot(&app, &snapshot);
    let handle = spawn_run_loop(
        Arc::clone(&state.controller),
        Arc::clone(&state.market_data),
        run_id.clone(),
        Arc::clone(&cancellation),
        app,
    );
    state.run_workers.lock().insert(
        run_id,
        RunWorker {
            cancellation,
            handle,
        },
    );
    Ok(snapshot)
}

#[tauri::command]
async fn steer_run(
    run_id: String,
    instruction: String,
    state: State<'_, AppState>,
    app: AppHandle,
) -> Result<RunSnapshot, String> {
    let controller = Arc::clone(&state.controller);
    let snapshot = tauri::async_runtime::spawn_blocking(move || {
        controller
            .lock()
            .steer_run(&run_id, &instruction)
            .map_err(|error| format!("{error:#}"))
    })
    .await
    .map_err(|error| format!("steer-run worker failed: {error}"))??;
    emit_snapshot(&app, &snapshot);
    Ok(snapshot)
}

fn spawn_run_loop(
    controller: Arc<Mutex<HarnessController>>,
    market_data: Arc<dyn ports::MarketDataProvider>,
    run_id: String,
    cancellation: Arc<AtomicBool>,
    app: AppHandle,
) -> JoinHandle<()> {
    thread::spawn(move || loop {
        if cancellation.load(Ordering::SeqCst) {
            break;
        }
        let interval_result = {
            let mut controller = controller.lock();
            if !controller.is_active(&run_id) {
                break;
            }
            if controller.take_immediate_cycle(&run_id) {
                Ok(0)
            } else {
                controller.cycle_interval_seconds(&run_id)
            }
        };
        let interval = match interval_result {
            Ok(interval) => interval,
            Err(error) => {
                report_worker_failure(
                    &controller,
                    &app,
                    &run_id,
                    "cadence",
                    &format!("Jev cadence unavailable: {error:#}"),
                );
                thread::sleep(Duration::from_secs(1));
                continue;
            }
        };
        let deadline = std::time::Instant::now() + Duration::from_secs(interval);
        while std::time::Instant::now() < deadline {
            if cancellation.load(Ordering::SeqCst) {
                return;
            }
            if controller.lock().take_immediate_cycle(&run_id) {
                break;
            }
            thread::sleep(
                deadline
                    .saturating_duration_since(std::time::Instant::now())
                    .min(Duration::from_millis(250)),
            );
        }
        if cancellation.load(Ordering::SeqCst) {
            break;
        }
        if let Err(error) = controller.lock().enforce_due_contract_stops(&run_id) {
            report_worker_failure(
                &controller,
                &app,
                &run_id,
                "contract_stop",
                &format!("Contract stop enforcement failed before market refresh: {error:#}"),
            );
            continue;
        }
        let requests = {
            let controller = controller.lock();
            if !controller.is_active(&run_id) {
                break;
            }
            controller.market_requests(&run_id)
        };
        let requests = match requests {
            Ok(requests) => requests,
            Err(error) => {
                report_worker_failure(
                    &controller,
                    &app,
                    &run_id,
                    "market_requests",
                    &format!("Market-data requirements failed: {error:#}"),
                );
                continue;
            }
        };
        for request in requests {
            if cancellation.load(Ordering::SeqCst) {
                break;
            }
            if market_data.snapshot(&request).is_err() {
                if let Err(error) = market_data.refresh(&request) {
                    report_worker_failure(
                        &controller,
                        &app,
                        &run_id,
                        "market_refresh",
                        &format!(
                            "Market-data refresh failed for {}: {error:#}",
                            request.instrument
                        ),
                    );
                }
            }
        }
        if cancellation.load(Ordering::SeqCst) {
            break;
        }
        let pending_activation = controller
            .lock()
            .advance_pending_startup_activation(&run_id);
        match pending_activation {
            Ok(Some(snapshot)) => {
                emit_snapshot(&app, &snapshot);
                continue;
            }
            Ok(None) => {}
            Err(error) => {
                report_worker_failure(
                    &controller,
                    &app,
                    &run_id,
                    "contract_activation",
                    &format!("Pending HypothesisContract activation failed: {error:#}"),
                );
                continue;
            }
        }
        let result = {
            let mut controller = controller.lock();
            if cancellation.load(Ordering::SeqCst) || !controller.is_active(&run_id) {
                break;
            }
            controller
                .run_cycle_cancellable(&run_id, &cancellation)
                .map(|snapshot| {
                    let jobs = controller.take_review_jobs(&run_id);
                    let model = controller.world_model();
                    let retry = controller.review_retry_policy();
                    (snapshot, jobs, model, retry)
                })
        };
        match result {
            Ok((snapshot, jobs, model, (max_attempts, retry_base_seconds))) => {
                emit_snapshot(&app, &snapshot);
                for job in jobs {
                    if cancellation.load(Ordering::SeqCst) {
                        if let Err(error) = controller.lock().record_review_job_state(
                            &job,
                            "cancelled",
                            0,
                            Some("run stopped before provider call"),
                        ) {
                            emit_error(
                                &app,
                                &run_id,
                                &format!("Review cancellation checkpoint failed: {error:#}"),
                            );
                        }
                        break;
                    }
                    if let Err(error) = controller
                        .lock()
                        .record_review_job_state(&job, "running", 0, None)
                    {
                        emit_error(
                            &app,
                            &run_id,
                            &format!("Review start checkpoint failed: {error:#}"),
                        );
                        controller.lock().requeue_review_job(job);
                        break;
                    }
                    let mut final_result = None;
                    for attempt in 1..=max_attempts {
                        let attempt_result = model.review_hypothesis(&job.package);
                        match attempt_result {
                            Ok(decision) => {
                                final_result = Some(Ok(decision));
                                break;
                            }
                            Err(error) if attempt < max_attempts => {
                                let message = error.to_string();
                                if let Err(checkpoint_error) =
                                    controller.lock().record_review_job_state(
                                        &job,
                                        "retrying",
                                        attempt,
                                        Some(&message),
                                    )
                                {
                                    emit_error(
                                        &app,
                                        &run_id,
                                        &format!(
                                            "Review retry checkpoint failed: {checkpoint_error:#}"
                                        ),
                                    );
                                    controller.lock().requeue_review_job(job.clone());
                                    break;
                                }
                                let delay = retry_base_seconds
                                    .saturating_mul(1u64 << attempt.saturating_sub(1).min(8));
                                let deadline =
                                    std::time::Instant::now() + Duration::from_secs(delay);
                                while std::time::Instant::now() < deadline {
                                    if cancellation.load(Ordering::SeqCst) {
                                        break;
                                    }
                                    thread::sleep(
                                        Duration::from_millis(250).min(
                                            deadline.saturating_duration_since(
                                                std::time::Instant::now(),
                                            ),
                                        ),
                                    );
                                }
                                if cancellation.load(Ordering::SeqCst) {
                                    break;
                                }
                            }
                            Err(error) => {
                                final_result = Some(Err(error));
                                break;
                            }
                        }
                    }
                    if cancellation.load(Ordering::SeqCst) {
                        if let Err(error) = controller.lock().record_review_job_state(
                            &job,
                            "cancelled",
                            0,
                            Some("run stopped while provider work was in flight"),
                        ) {
                            emit_error(
                                &app,
                                &run_id,
                                &format!("Review cancellation checkpoint failed: {error:#}"),
                            );
                        }
                        break;
                    }
                    if let Some(review_result) = final_result {
                        match controller
                            .lock()
                            .complete_autonomous_review(&job, review_result)
                        {
                            Ok(snapshot) => {
                                emit_snapshot(&app, &snapshot);
                            }
                            Err(error) => {
                                emit_error(&app, &run_id, &error.to_string());
                            }
                        }
                    }
                }
            }
            Err(error) => {
                if cancellation.load(Ordering::SeqCst) {
                    break;
                }
                report_worker_failure(
                    &controller,
                    &app,
                    &run_id,
                    "cycle",
                    &format!("Jev cycle stopped before completion: {error:#}"),
                );
            }
        }
    })
}

#[tauri::command]
async fn stop_run(
    run_id: String,
    state: State<'_, AppState>,
    app: AppHandle,
) -> Result<RunSnapshot, String> {
    let worker = state.run_workers.lock().remove(&run_id);
    if let Some(worker) = &worker {
        worker.cancellation.store(true, Ordering::SeqCst);
    }
    let controller = Arc::clone(&state.controller);
    let stop_run_id = run_id.clone();
    let result = tauri::async_runtime::spawn_blocking(move || {
        if let Some(worker) = worker {
            let _ = worker.handle.join();
        }
        let mut last_error = String::new();
        for attempt in 0..3 {
            if attempt > 0 {
                thread::sleep(Duration::from_secs(attempt as u64));
            }
            match controller.lock().stop(&stop_run_id) {
                Ok(snapshot) => return Ok(snapshot),
                Err(error) => last_error = format!("{error:#}"),
            }
        }
        Err(last_error)
    })
    .await
    .map_err(|error| format!("stop-run worker failed: {error}"))?;
    let snapshot = match result {
        Ok(snapshot) => snapshot,
        Err(error) => {
            if let Err(record_error) =
                state
                    .controller
                    .lock()
                    .record_worker_failure(&run_id, "operator_stop", &error)
            {
                eprintln!(
                    "Failed to persist operator-stop failure for run {run_id}: {record_error:#}"
                );
            }
            emit_error(&app, &run_id, &error);
            let resume_stop_recovery = {
                let mut controller = state.controller.lock();
                let active = controller.is_active(&run_id);
                if active {
                    controller.schedule_immediate_cycle(&run_id);
                }
                active
            };
            if resume_stop_recovery {
                let cancellation = Arc::new(AtomicBool::new(false));
                let handle = spawn_run_loop(
                    Arc::clone(&state.controller),
                    Arc::clone(&state.market_data),
                    run_id.clone(),
                    Arc::clone(&cancellation),
                    app.clone(),
                );
                state.run_workers.lock().insert(
                    run_id.clone(),
                    RunWorker {
                        cancellation,
                        handle,
                    },
                );
            }
            return Err(error);
        }
    };
    emit_snapshot(&app, &snapshot);
    let (packages, world_model) = {
        let mut controller = state.controller.lock();
        (
            controller.take_stop_wrapups(&run_id),
            controller.world_model(),
        )
    };
    if !packages.is_empty() {
        let controller = Arc::clone(&state.controller);
        let app_for_wrapup = app.clone();
        thread::spawn(move || {
            for package in packages {
                let result = world_model.review_hypothesis(&package);
                if let Err(error) = controller.lock().record_stop_wrapup(&package, result) {
                    emit_error(
                        &app_for_wrapup,
                        &package.run_id,
                        &format!("Stopped-run retrospective could not be recorded: {error:#}"),
                    );
                }
            }
        });
    }
    Ok(snapshot)
}

#[tauri::command]
async fn review_hypothesis(
    request: HypothesisReviewRequest,
    state: State<'_, AppState>,
    app: AppHandle,
) -> Result<RunSnapshot, String> {
    let controller = Arc::clone(&state.controller);
    let snapshot = tauri::async_runtime::spawn_blocking(move || {
        controller
            .lock()
            .review_hypothesis(&request)
            .map_err(|error| error.to_string())
    })
    .await
    .map_err(|error| format!("review worker failed: {error}"))??;
    emit_snapshot(&app, &snapshot);
    Ok(snapshot)
}

#[tauri::command]
async fn hydrate_workspace(state: State<'_, AppState>) -> Result<WorkspaceSnapshot, String> {
    let project_root = state.project_root.clone();
    let retrieval_ready = state.retrieval_ready.load(Ordering::SeqCst);
    let canonical_store = state.canonical_store.clone();
    let market_data = Arc::clone(&state.market_data);
    tauri::async_runtime::spawn_blocking(move || {
        let config =
            config::HarnessConfig::load(&project_root).map_err(|error| error.to_string())?;
        let mut integrations = integration_statuses(&project_root, &config, retrieval_ready);
        integrations.extend(market_data.health_statuses());
        workspace_snapshot_from_store(&canonical_store, integrations)
            .map_err(|error| error.to_string())
    })
    .await
    .map_err(|error| format!("workspace worker failed: {error}"))?
}

#[tauri::command]
async fn get_market_health(state: State<'_, AppState>) -> Result<Vec<IntegrationStatus>, String> {
    let controller = Arc::clone(&state.controller);
    tauri::async_runtime::spawn_blocking(move || controller.lock().market_data_statuses())
        .await
        .map_err(|error| format!("market-health worker failed: {error}"))
}

#[tauri::command]
async fn check_for_updates(
    app: AppHandle,
    state: State<'_, AppState>,
) -> Result<updates::UpdateStatus, String> {
    Ok(updates::check(
        app,
        &state.project_root,
        Arc::clone(&state.controller),
        Arc::clone(&state.installing_update),
    )
    .await)
}

#[tauri::command]
async fn rename_run(
    run_id: String,
    name: String,
    state: State<'_, AppState>,
) -> Result<(), String> {
    let controller = Arc::clone(&state.controller);
    tauri::async_runtime::spawn_blocking(move || {
        controller
            .lock()
            .rename_run(&run_id, &name)
            .map_err(|error| error.to_string())
    })
    .await
    .map_err(|error| format!("rename worker failed: {error}"))?
}

#[tauri::command]
async fn set_run_archived(
    run_id: String,
    archived: bool,
    state: State<'_, AppState>,
) -> Result<(), String> {
    let controller = Arc::clone(&state.controller);
    tauri::async_runtime::spawn_blocking(move || {
        controller
            .lock()
            .set_run_archived(&run_id, archived)
            .map_err(|error| error.to_string())
    })
    .await
    .map_err(|error| format!("archive worker failed: {error}"))?
}

#[tauri::command]
fn get_connector_settings(state: State<'_, AppState>) -> Result<config::ConnectorSettings, String> {
    config::load_connector_settings(&state.project_root).map_err(|error| error.to_string())
}

#[tauri::command]
async fn probe_mcp_connection(state: State<'_, AppState>) -> Result<String, String> {
    let root = state.project_root.clone();
    tauri::async_runtime::spawn_blocking(move || {
        mcp_context::McpBrokerContext::probe(&root).map_err(|error| format!("{error:#}"))
    })
    .await
    .map_err(|error| format!("MCP connection check failed: {error}"))?
}

#[tauri::command]
async fn get_live_risk_account_values(state: State<'_, AppState>) -> Result<serde_json::Value, String> {
    let root = state.project_root.clone();
    tauri::async_runtime::spawn_blocking(move || {
        let client = mcp_context::McpBrokerContext::load(&root)
            .map_err(|error| format!("load cTrader MCP settings: {error:#}"))?
            .ok_or_else(|| "Enable cTrader MCP to fetch current account risk values".to_owned())?;
        let (equity, free_margin, observed_at) = client
            .live_risk_values()
            .map_err(|error| format!("fetch current cTrader account values: {error:#}"))?;
        Ok(serde_json::json!({
            "equity": equity,
            "freeMargin": free_margin,
            "observedAt": observed_at,
        }))
    })
    .await
    .map_err(|error| format!("cTrader account-value worker failed: {error}"))?
}

#[tauri::command]
async fn approve_human_verified_live_cycle(
    run_id: String,
    account_id: String,
    environment: String,
    instrument: String,
    deposit_currency: String,
    account_open_exposure: f64,
    quote_to_deposit: f64,
    confirmed: bool,
    state: State<'_, AppState>,
    app: AppHandle,
) -> Result<RunSnapshot, String> {
    let controller = Arc::clone(&state.controller);
    let project_root = state.project_root.clone();
    let instrument_for_metadata = instrument.clone();
    let snapshot = tauri::async_runtime::spawn_blocking(move || {
        let mcp = mcp_context::McpBrokerContext::load(&project_root)
            .map_err(|error| format!("load cTrader MCP settings: {error:#}"))?
            .ok_or_else(|| "Enable cTrader MCP to fetch current account risk values".to_owned())?;
        let (equity, free_margin, observed_at) = mcp
            .live_risk_values()
            .map_err(|error| format!("fetch current cTrader account values: {error:#}"))?;
        let open_api = market_data::CTraderOpenApiConfig::load_optional(&project_root)
            .map_err(|error| format!("load cTrader Open API settings: {error:#}"))?
            .ok_or_else(|| "cTrader Open API credentials are required to discover symbol lot size and volume limits".to_string())?;
        let volume_rules = open_api
            .volume_rules(std::slice::from_ref(&instrument_for_metadata))
            .map_err(|error| format!("discover broker volume metadata: {error:#}"))?;
        let rules = volume_rules
            .get(&instrument_for_metadata.to_ascii_uppercase().replace(['/', '-', '_'], ""))
            .ok_or_else(|| "cTrader Open API returned no volume limits for the active instrument".to_string())?;
        let risk_snapshot = ports::BrokerRiskSnapshot {
            account_id,
            environment,
            equity,
            free_margin,
            deposit_asset_id: deposit_currency.clone(),
            deposit_currency_code: deposit_currency,
            observed_at,
            account_open_exposure,
            quote_to_deposit: HashMap::from([(instrument, quote_to_deposit)]),
        };
        controller
            .lock()
            .run_human_verified_live_cycle(
                &run_id,
                risk_snapshot,
                Some(rules.minimum_lots),
                Some(rules.step_lots),
                confirmed,
            )
            .map_err(|error| format!("human-verified live cycle failed: {error:#}"))
    })
    .await
    .map_err(|error| format!("human-verified live-cycle worker failed: {error}"))??;
    emit_snapshot(&app, &snapshot);
    Ok(snapshot)
}

#[tauri::command]
fn save_connector_settings(
    update: config::ConnectorSettingsUpdate,
    state: State<'_, AppState>,
) -> Result<config::ConnectorSettings, String> {
    config::save_connector_settings(&state.project_root, update).map_err(|error| error.to_string())
}

#[tauri::command]
async fn get_run_snapshot(
    run_id: String,
    state: State<'_, AppState>,
) -> Result<RunSnapshot, String> {
    let canonical_store = state.canonical_store.clone();
    tauri::async_runtime::spawn_blocking(move || {
        run_snapshot_from_store(&canonical_store, &run_id).map_err(|error| error.to_string())
    })
    .await
    .map_err(|error| format!("snapshot worker failed: {error}"))?
}

#[tauri::command]
async fn prepare_review_package(
    request: HypothesisReviewRequest,
    state: State<'_, AppState>,
) -> Result<WorldModelReviewPackage, String> {
    let controller = Arc::clone(&state.controller);
    tauri::async_runtime::spawn_blocking(move || {
        controller
            .lock()
            .assemble_review_package(&request)
            .map_err(|error| error.to_string())
    })
    .await
    .map_err(|error| format!("review-package worker failed: {error}"))?
}

#[tauri::command]
async fn search_context(
    query: String,
    state: State<'_, AppState>,
) -> Result<Vec<SearchHit>, String> {
    let controller = Arc::clone(&state.controller);
    tauri::async_runtime::spawn_blocking(move || {
        controller
            .lock()
            .search(&query)
            .map_err(|error| error.to_string())
    })
    .await
    .map_err(|error| format!("search worker failed: {error}"))?
}

#[tauri::command]
async fn replay_run(run_id: String, state: State<'_, AppState>) -> Result<ReplayState, String> {
    let canonical_store = state.canonical_store.clone();
    tauri::async_runtime::spawn_blocking(move || {
        replay::replay_run(&canonical_store, &run_id).map_err(|error| error.to_string())
    })
    .await
    .map_err(|error| format!("replay worker failed: {error}"))?
}

#[tauri::command]
async fn retrieve_context(
    request: RetrievalRequest,
    state: State<'_, AppState>,
) -> Result<RetrievalTrace, String> {
    let controller = Arc::clone(&state.controller);
    tauri::async_runtime::spawn_blocking(move || {
        controller
            .lock()
            .retrieve_context(&request)
            .map_err(|error| error.to_string())
    })
    .await
    .map_err(|error| format!("retrieval worker failed: {error}"))?
}

#[tauri::command]
async fn ingest_context(
    request: ContextIngestRequest,
    state: State<'_, AppState>,
) -> Result<ContextPoolRecord, String> {
    let controller = Arc::clone(&state.controller);
    tauri::async_runtime::spawn_blocking(move || {
        controller
            .lock()
            .ingest_context(&request)
            .map_err(|error| error.to_string())
    })
    .await
    .map_err(|error| format!("context-ingest worker failed: {error}"))?
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let state = AppState::load().expect("initialize local harness state");
    tauri::Builder::default()
        .plugin(tauri_plugin_updater::Builder::new().build())
        .manage(state)
        .setup(|app| {
            let (controller, market_data, run_ids, retrieval_config, retrieval_ready) = {
                let state = app.state::<AppState>();
                (
                    Arc::clone(&state.controller),
                    Arc::clone(&state.market_data),
                    state.restored_run_ids.clone(),
                    state.retrieval_config.clone(),
                    Arc::clone(&state.retrieval_ready),
                )
            };
            let retrieval_controller = Arc::clone(&controller);
            thread::spawn(
                move || match retrieval::QdrantContextPool::new(retrieval_config) {
                    Ok(pool) => {
                        let store = retrieval_controller.lock().canonical_store_handle();
                        match pool.rebuild_if_needed(&store) {
                            Ok(rebuilt) => {
                                if rebuilt {
                                    eprintln!("Rebuilt compact evidence index from canonical events");
                                }
                                let indexing = (|| -> anyhow::Result<()> {
                                    for run in store.run_summaries()? {
                                        pool.sync_pending_events(&store, &run.run_id)
                                            .map_err(|error| {
                                                anyhow::anyhow!("run {}: {error:#}", run.run_id)
                                            })?;
                                    }
                                    retrieval_controller
                                        .lock()
                                        .configure_context_pool(pool);
                                    let (pool, store) = retrieval_controller
                                        .lock()
                                        .pending_indexer()
                                        .ok_or_else(|| {
                                            anyhow::anyhow!(
                                                "context pool disappeared during bootstrap"
                                            )
                                        })?;
                                    for run in store.run_summaries()? {
                                        pool.sync_pending_events(&store, &run.run_id)
                                            .map_err(|error| {
                                                anyhow::anyhow!("run {}: {error:#}", run.run_id)
                                            })?;
                                    }
                                    Ok(())
                                })();
                                match indexing {
                                    Ok(()) => retrieval_ready.store(true, Ordering::SeqCst),
                                    Err(error) => eprintln!(
                                        "Deferred evidence indexing failed; retrieval stays unavailable until retry: {error:#}"
                                    ),
                                }
                            }
                            Err(error) => eprintln!(
                                "Compact evidence index rebuild failed; retrieval stays unavailable until retry: {error:#}"
                            ),
                        }
                    }
                    Err(error) => eprintln!("Local context index unavailable: {error:#}"),
                },
            );
            for run_id in run_ids {
                let cancellation = Arc::new(AtomicBool::new(false));
                let handle = spawn_run_loop(
                    Arc::clone(&controller),
                    Arc::clone(&market_data),
                    run_id.clone(),
                    Arc::clone(&cancellation),
                    app.handle().clone(),
                );
                app.state::<AppState>().run_workers.lock().insert(
                    run_id,
                    RunWorker {
                        cancellation,
                        handle,
                    },
                );
            }
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            start_run,
            steer_run,
            stop_run,
            review_hypothesis,
            hydrate_workspace,
            get_market_health,
            check_for_updates,
            rename_run,
            set_run_archived,
            get_run_snapshot,
            prepare_review_package,
            search_context,
            replay_run,
            retrieve_context,
            ingest_context,
            get_connector_settings,
            probe_mcp_connection,
            get_live_risk_account_values,
            approve_human_verified_live_cycle,
            save_connector_settings
        ])
        .run(tauri::generate_context!())
        .expect("run Autonomous Jev Harness");
}
