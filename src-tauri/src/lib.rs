mod adapters;
mod config;
mod context;
mod ctrader_fix;
mod domain;
mod harness;
mod market_data;
mod ports;
mod replay;
mod retrieval;
mod risk;
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
use std::time::Duration;
use tauri::{AppHandle, Emitter, Manager, State};

struct AppState {
    controller: Arc<Mutex<HarnessController>>,
    restored_run_ids: Vec<String>,
    project_root: PathBuf,
    retrieval_ready: bool,
    installing_update: Arc<AtomicBool>,
    run_cancellations: Mutex<HashMap<String, Arc<AtomicBool>>>,
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
            std::fs::write(&config_path, serde_json::to_string_pretty(&defaults)? + "\n")?;
        }
        let config = config::HarnessConfig::load(&project_root)?;
        let runtime_path = config.runtime_path(&project_root)?;
        let store = storage::CanonicalStore::open(&runtime_path)?;
        let retrieval_config = retrieval::RetrievalConfig::load(&project_root)?;
        let context_pool = match retrieval::QdrantContextPool::new(retrieval_config) {
            Ok(pool) => Some(pool),
            Err(error) => {
                eprintln!("Local context index unavailable: {error}");
                None
            }
        };
        let retrieval_ready = context_pool.is_some();
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
            Box<dyn ports::MarketDataProvider>,
        ) = match config.broker_adapter.as_str() {
            "ctrader-fix" => match ctrader_fix::CTraderFixConfig::load(&project_root) {
                Ok(fix_config) => {
                    let market_data: Box<dyn ports::MarketDataProvider> =
                        match market_data::CTraderOpenApiConfig::load(&project_root).and_then(
                            |open_api| {
                                market_data::HybridCTraderMarketDataProvider::new(
                                    fix_config.clone(),
                                    open_api,
                                )
                            },
                        ) {
                            Ok(provider) => Box::new(provider),
                            Err(error) => Box::new(market_data::UnavailableMarketDataProvider(
                                format!("cTrader live-context pipeline unavailable: {error}"),
                            )),
                        };
                    (
                        Box::new(ctrader_fix::CTraderFixBroker::new(fix_config)),
                        market_data,
                    )
                }
                Err(error) => (
                    Box::new(adapters::UnavailableBroker(format!(
                        "cTrader FIX unavailable: {error}"
                    ))),
                    Box::new(market_data::UnavailableMarketDataProvider(format!(
                        "cTrader FIX price feed unavailable: {error}"
                    ))),
                ),
            },
            _ => (
                Box::new(adapters::SimulatedBroker),
                Box::new(market_data::SimulatedMarketDataProvider::new()),
            ),
        };
        let mut controller = HarnessController::with_optional_retrieval_adapters(
            store,
            config.minimum_confidence,
            context_pool,
            world_model,
            jev,
            broker,
            config.risk_policy,
        );
        controller.configure_market_data(market_data);
        controller.configure_autonomous_reviews(config.autonomous_review.clone());
        let restored = controller.restore_active_runs()?;
        let controller = Arc::new(Mutex::new(controller));
        Ok(Self {
            controller,
            restored_run_ids: restored,
            project_root,
            retrieval_ready,
            installing_update: Arc::new(AtomicBool::new(false)),
            run_cancellations: Mutex::new(HashMap::new()),
        })
    }
}

fn env_setting(project_root: &std::path::Path, name: &str) -> Option<String> {
    std::fs::read_to_string(project_root.join(".env"))
        .ok()
        .and_then(|contents| {
            contents
                .lines()
                .filter_map(|line| line.split_once('='))
                .find(|(key, _)| key.trim() == name)
                .map(|(_, value)| value.trim().to_owned())
                .filter(|value| !value.is_empty())
        })
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
    let price_fix = configured(
        project_root,
        &["CTRADER_FIX_PRICE_HOST", "CTRADER_FIX_PRICE_USERNAME"],
    );
    let open_api = configured(
        project_root,
        &[
            "CTRADER_OPEN_API_CLIENT_ID",
            "CTRADER_OPEN_API_CLIENT_SECRET",
            "CTRADER_OPEN_API_ACCESS_TOKEN",
            "CTRADER_OPEN_API_ACCOUNT_ID",
            "CTRADER_OPEN_API_SYMBOL_MAP",
        ],
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
            if retrieval_ready { "connected" } else { "degraded" },
            if retrieval_ready { "Local context index ready" } else { "Local context index unavailable" },
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
            if mcp_enabled {
                "configured"
            } else {
                "disabled"
            },
            if mcp_enabled {
                "Read-only context adapter enabled"
            } else {
                "Read-only adapter disabled"
            },
        ),
        status(
            "open-api-market-data",
            "cTrader candles",
            if open_api { "configured" } else { "degraded" },
            if open_api {
                "Open API completed-bar backfill configured"
            } else {
                "Open API history credentials missing"
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
            if trade_fix && config.broker_adapter == "ctrader-fix" {
                "configured"
            } else {
                "degraded"
            },
            if trade_fix {
                "Trade session credentials present"
            } else {
                "Trade session not configured"
            },
        ),
    ]
}

#[tauri::command]
async fn start_run(
    thesis: String,
    state: State<'_, AppState>,
    app: AppHandle,
) -> Result<RunSnapshot, String> {
    let controller = Arc::clone(&state.controller);
    let installing_update = Arc::clone(&state.installing_update);
    let snapshot = tauri::async_runtime::spawn_blocking(move || {
        let mut controller = controller.lock();
        if installing_update.load(Ordering::SeqCst) {
            return Err("Espeon is installing an update; start the experiment after restart.".into());
        }
        controller.start(&thesis).map_err(|error| error.to_string())
    })
    .await
    .map_err(|error| format!("start-run worker failed: {error}"))??;
    let run_id = snapshot.run_id.clone();
    let cancellation = Arc::new(AtomicBool::new(false));
    state
        .run_cancellations
        .lock()
        .insert(run_id.clone(), Arc::clone(&cancellation));
    let _ = app.emit("harness:snapshot", &snapshot);
    spawn_run_loop(Arc::clone(&state.controller), run_id, cancellation, app);
    Ok(snapshot)
}

fn spawn_run_loop(
    controller: Arc<Mutex<HarnessController>>,
    run_id: String,
    cancellation: Arc<AtomicBool>,
    app: AppHandle,
) {
    thread::spawn(move || loop {
        if cancellation.load(Ordering::SeqCst) {
            break;
        }
        let interval = {
            let mut controller = controller.lock();
            if !controller.is_active(&run_id) {
                break;
            }
            if controller.take_immediate_cycle(&run_id) {
                0
            } else {
                controller.cycle_interval_seconds(&run_id).unwrap_or(60)
            }
        };
        let deadline = std::time::Instant::now() + Duration::from_secs(interval);
        while std::time::Instant::now() < deadline {
            if cancellation.load(Ordering::SeqCst) {
                return;
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
                let _ = app.emit("harness:snapshot", snapshot);
                for job in jobs {
                    if cancellation.load(Ordering::SeqCst) {
                        let _ = controller.lock().record_review_job_state(
                            &job,
                            "cancelled",
                            0,
                            Some("run stopped before provider call"),
                        );
                        break;
                    }
                    let _ = controller
                        .lock()
                        .record_review_job_state(&job, "running", 0, None);
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
                                let _ = controller.lock().record_review_job_state(
                                    &job,
                                    "retrying",
                                    attempt,
                                    Some(&message),
                                );
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
                        let _ = controller.lock().record_review_job_state(
                            &job,
                            "cancelled",
                            0,
                            Some("run stopped while provider work was in flight"),
                        );
                        break;
                    }
                    if let Some(review_result) = final_result {
                        match controller
                            .lock()
                            .complete_autonomous_review(&job, review_result)
                        {
                            Ok(snapshot) => {
                                let _ = app.emit("harness:snapshot", snapshot);
                            }
                            Err(error) => {
                                let _ = app.emit(
                                    "harness:error",
                                    serde_json::json!({"runId":run_id,"message":error.to_string()}),
                                );
                            }
                        }
                    }
                }
            }
            Err(error) => {
                if cancellation.load(Ordering::SeqCst) {
                    break;
                }
                let _ = app.emit(
                    "harness:error",
                    serde_json::json!({"runId":run_id,"message":error.to_string()}),
                );
            }
        }
    });
}

#[tauri::command]
async fn stop_run(
    run_id: String,
    state: State<'_, AppState>,
    app: AppHandle,
) -> Result<RunSnapshot, String> {
    let cancellation = state.run_cancellations.lock().get(&run_id).cloned();
    if let Some(cancellation) = &cancellation {
        cancellation.store(true, Ordering::SeqCst);
    }
    let controller = Arc::clone(&state.controller);
    let stop_run_id = run_id.clone();
    let result = tauri::async_runtime::spawn_blocking(move || {
        controller
            .lock()
            .stop(&stop_run_id)
            .map_err(|error| error.to_string())
    })
    .await
    .map_err(|error| format!("stop-run worker failed: {error}"))?;
    let snapshot = match result {
        Ok(snapshot) => {
            state.run_cancellations.lock().remove(&run_id);
            snapshot
        }
        Err(error) => return Err(error),
    };
    let _ = app.emit("harness:snapshot", &snapshot);
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
    let _ = app.emit("harness:snapshot", &snapshot);
    Ok(snapshot)
}

#[tauri::command]
async fn hydrate_workspace(state: State<'_, AppState>) -> Result<WorkspaceSnapshot, String> {
    let project_root = state.project_root.clone();
    let retrieval_ready = state.retrieval_ready;
    let controller = Arc::clone(&state.controller);
    tauri::async_runtime::spawn_blocking(move || {
        let config =
            config::HarnessConfig::load(&project_root).map_err(|error| error.to_string())?;
        let integrations = integration_statuses(&project_root, &config, retrieval_ready);
        controller
            .lock()
            .workspace_snapshot(integrations)
            .map_err(|error| error.to_string())
    })
    .await
    .map_err(|error| format!("workspace worker failed: {error}"))?
}

#[tauri::command]
async fn check_for_updates(app: AppHandle, state: State<'_, AppState>) -> Result<updates::UpdateStatus, String> {
    Ok(updates::check(
        app,
        &state.project_root,
        Arc::clone(&state.controller),
        Arc::clone(&state.installing_update),
    ).await)
}

#[tauri::command]
async fn rename_run(run_id: String, name: String, state: State<'_, AppState>) -> Result<(), String> {
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
    let controller = Arc::clone(&state.controller);
    tauri::async_runtime::spawn_blocking(move || {
        controller
            .lock()
            .run_snapshot(&run_id)
            .map_err(|error| error.to_string())
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
    let controller = Arc::clone(&state.controller);
    tauri::async_runtime::spawn_blocking(move || {
        controller
            .lock()
            .replay(&run_id)
            .map_err(|error| error.to_string())
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
            let (controller, run_ids) = {
                let state = app.state::<AppState>();
                (
                    Arc::clone(&state.controller),
                    state.restored_run_ids.clone(),
                )
            };
            for run_id in run_ids {
                let cancellation = Arc::new(AtomicBool::new(false));
                app.state::<AppState>()
                    .run_cancellations
                    .lock()
                    .insert(run_id.clone(), Arc::clone(&cancellation));
                spawn_run_loop(
                    Arc::clone(&controller),
                    run_id,
                    cancellation,
                    app.handle().clone(),
                );
            }
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            start_run,
            stop_run,
            review_hypothesis,
            hydrate_workspace,
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
            save_connector_settings
        ])
        .run(tauri::generate_context!())
        .expect("run Autonomous Jev Harness");
}
