#[path = "../src/adapters.rs"]
mod adapters;
#[path = "../src/config.rs"]
mod config;
#[path = "../src/context.rs"]
mod context;
#[path = "../src/contracts.rs"]
mod contracts;
#[path = "../src/ctrader_fix.rs"]
mod ctrader_fix;
#[path = "../src/domain.rs"]
mod domain;
#[path = "../src/evidence.rs"]
mod evidence;
#[path = "../src/freshness.rs"]
mod freshness;
#[path = "../src/harness.rs"]
mod harness;
#[path = "../src/jev_compiler.rs"]
mod jev_compiler;
#[path = "../src/market_data.rs"]
mod market_data;
#[path = "../src/mcp_context.rs"]
mod mcp_context;
#[path = "../src/ports.rs"]
mod ports;
#[path = "../src/replay.rs"]
mod replay;
#[path = "../src/retrieval.rs"]
mod retrieval;
#[path = "../src/risk.rs"]
mod risk;
#[path = "../src/skills.rs"]
mod skills;
#[path = "../src/storage.rs"]
mod storage;

use domain::{
    ContextIngestRequest, ContextSourceClass, HypothesisReviewRequest, RetrievalFilters,
    RetrievalRequest, RetrievalTrace, TrustLevel,
};
use harness::HarnessController;
use retrieval::{QdrantContextPool, RetrievalConfig};
use rusqlite::{params, Connection};
use std::collections::HashSet;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use storage::CanonicalStore;

fn fixture_risk_snapshot(instruments: &[String]) -> ports::BrokerRiskSnapshot {
    ports::BrokerRiskSnapshot {
        account_id: "fixture-account".into(),
        environment: "demo".into(),
        equity: 10_000.0,
        free_margin: 10_000.0,
        deposit_asset_id: "USD".into(),
        deposit_currency_code: "USD".into(),
        observed_at: chrono::Utc::now(),
        account_open_exposure: 0.0,
        quote_to_deposit: instruments
            .iter()
            .cloned()
            .map(|instrument| (instrument, 1.0))
            .collect(),
    }
}

fn fixture_account_identity() -> ports::BrokerAccountIdentity {
    ports::BrokerAccountIdentity {
        account_id: "fixture-account".into(),
        environment: "demo".into(),
    }
}

#[test]
fn canonical_events_replay_the_complete_run_without_search_index() {
    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("harness.sqlite3");
    let store = CanonicalStore::open(directory.path()).unwrap();
    let mut controller = HarnessController::new(store, 0.0);

    let started = controller
        .start("Compare BTC continuation with a competing context")
        .unwrap();
    assert_eq!(started.loops.len(), 1);
    controller.run_cycle(&started.run_id).unwrap();
    let active = controller
        .review_hypothesis(&HypothesisReviewRequest {
            run_id: started.run_id.clone(),
            hypothesis_id: started.hypotheses[0].id.clone(),
            evidence_query: "evidence remains supportive; keep the hypothesis".into(),
        })
        .unwrap();
    assert_eq!(active.status, "active");
    assert_eq!(active.loops.len(), 1);
    assert_eq!(active.positions.len(), 1);

    let stopped = controller.stop(&active.run_id).unwrap();
    assert_eq!(stopped.status, "stopped");
    assert!(stopped
        .loops
        .iter()
        .all(|loop_state| loop_state.state == "stopped"));

    let connection = Connection::open(&database_path).unwrap();
    connection
        .execute("DELETE FROM search_documents", [])
        .unwrap();

    let replayed = controller.replay(&active.run_id).unwrap();
    assert_eq!(replayed.status, "stopped");
    assert_eq!(
        replayed.human_thesis,
        "Compare BTC continuation with a competing context"
    );
    assert_eq!(replayed.thesis_versions.len(), 1);
    assert_eq!(replayed.context_definitions.len(), 1);
    assert_eq!(replayed.context_versions.len(), 1);
    assert_eq!(replayed.hypotheses.len(), 1);
    assert_eq!(replayed.hypothesis_reviews.len(), 1);
    assert_eq!(replayed.cadences.len(), 1);
    assert_eq!(replayed.loops.len(), 1);
    assert_eq!(replayed.decisions.len(), 2);
    assert_eq!(replayed.executions.len(), 2);
    assert_eq!(replayed.positions.len(), 1);
    assert_eq!(replayed.orders.len(), 2);
    assert_eq!(replayed.position_controls.len(), 1);
    assert_eq!(replayed.reviews.len(), 0);
    assert_eq!(replayed.capital_allocations.len(), 2);
    assert!(replayed.memory_documents.is_empty());

    let thesis_id = &replayed.thesis_versions[0].id;
    let context_id = &replayed.context_versions[0].id;
    assert!(replayed.decisions.iter().all(|decision| {
        &decision.thesis_version_id == thesis_id && &decision.context_version_id == context_id
    }));

    let decision_ids: HashSet<&str> = replayed
        .decisions
        .iter()
        .map(|decision| decision.id.as_str())
        .collect();
    assert!(replayed
        .executions
        .iter()
        .all(|execution| decision_ids.contains(execution.caused_by_decision_id.as_str())));

    let child = replayed
        .loops
        .iter()
        .find(|loop_state| loop_state.parent_loop_id.is_none())
        .unwrap();
    assert_eq!(child.thesis_version_id, *thesis_id);
    assert!(!child.created_by_event_id.is_empty());
}

#[test]
fn active_run_restores_from_canonical_state_after_restart() {
    let directory = tempfile::tempdir().unwrap();
    let store = CanonicalStore::open(directory.path()).unwrap();
    let mut first = HarnessController::new(store, 0.0);
    let started = first
        .start("Restore BTCUSD continuation after restart")
        .unwrap();
    drop(first);

    let store = CanonicalStore::open(directory.path()).unwrap();
    let mut restored = HarnessController::new(store, 0.0);
    let restored_ids = restored.restore_active_runs().unwrap();
    assert_eq!(restored_ids, vec![started.run_id.clone()]);
    assert!(restored.is_active(&started.run_id));
    let cycle = restored.run_cycle(&started.run_id).unwrap();
    assert_eq!(cycle.status, "active");
}

#[test]
fn ui_workspace_hydrates_active_and_historical_runs_from_canonical_state() {
    let directory = tempfile::tempdir().unwrap();
    let store = CanonicalStore::open(directory.path()).unwrap();
    let mut controller = HarnessController::new(store, 0.0);
    let started = controller
        .start("Hydrate a complete operator workspace")
        .unwrap();
    let integrations = vec![domain::IntegrationStatus {
        id: "harness".into(),
        label: "Rust harness".into(),
        state: "connected".into(),
        detail: "ready".into(),
    }];

    let active_workspace = controller.workspace_snapshot(integrations.clone()).unwrap();
    assert_eq!(active_workspace.active_runs.len(), 1);
    assert_eq!(active_workspace.run_history[0].run_id, started.run_id);
    assert_eq!(active_workspace.integrations[0].state, "connected");

    controller.stop(&started.run_id).unwrap();
    let historical = controller.run_snapshot(&started.run_id).unwrap();
    assert_eq!(historical.status, "stopped");
    assert!(!historical.events.is_empty());
    assert!(!historical.hypotheses.is_empty());
    assert!(!historical.loops.is_empty());

    let stopped_workspace = controller.workspace_snapshot(integrations).unwrap();
    assert!(stopped_workspace.active_runs.is_empty());
    assert_eq!(stopped_workspace.run_history[0].status, "stopped");
}

struct FailingBroker;

impl ports::ExecutionBroker for FailingBroker {
    fn risk_snapshot(
        &self,
        instruments: &[String],
    ) -> anyhow::Result<Option<ports::BrokerRiskSnapshot>> {
        Ok(Some(fixture_risk_snapshot(instruments)))
    }

    fn risk_account_identity(&self) -> Option<ports::BrokerAccountIdentity> {
        Some(fixture_account_identity())
    }

    fn execute(&self, _request: &domain::TradeRequest) -> anyhow::Result<domain::ExecutionReceipt> {
        anyhow::bail!("broker disconnected")
    }

    fn close(
        &self,
        _position: &domain::PositionRecord,
        _order: &domain::OrderRecord,
        _caused_by_decision_id: &str,
        _execution_event_id: &str,
    ) -> anyhow::Result<domain::ExecutionReceipt> {
        anyhow::bail!("broker disconnected")
    }

    fn reference_price(&self, _instrument: &str) -> anyhow::Result<Option<f64>> {
        Ok(Some(100.0))
    }

    fn reconcile(&self) -> anyhow::Result<domain::BrokerSnapshot> {
        Ok(domain::BrokerSnapshot {
            adapter: "test-broker".into(),
            connected: true,
            complete: true,
            positions: Vec::new(),
            observed_at: chrono::Utc::now(),
        })
    }
}

#[test]
fn broker_transport_error_is_canonical_and_does_not_open_a_position() {
    let directory = tempfile::tempdir().unwrap();
    let store = CanonicalStore::open(directory.path()).unwrap();
    let mut controller =
        HarnessController::with_runtime_broker(store, 0.0, Box::new(FailingBroker));
    let started = controller
        .start("Test BTCUSD breakout over 4 hours")
        .unwrap();
    let after_cycle = controller.run_cycle(&started.run_id).unwrap();
    assert!(started.positions.is_empty());
    assert_eq!(after_cycle.loops[0].state, "jev1-broker-rejected");
    let replayed = controller.replay(&started.run_id).unwrap();
    assert_eq!(replayed.executions.len(), 1);
    assert_eq!(replayed.executions[0].status, "connection-error");
    assert!(replayed.positions.is_empty());
}

struct ReconcilingBroker {
    position_exists: Arc<AtomicBool>,
}

impl ports::ExecutionBroker for ReconcilingBroker {
    fn risk_snapshot(
        &self,
        instruments: &[String],
    ) -> anyhow::Result<Option<ports::BrokerRiskSnapshot>> {
        Ok(Some(fixture_risk_snapshot(instruments)))
    }

    fn risk_account_identity(&self) -> Option<ports::BrokerAccountIdentity> {
        Some(fixture_account_identity())
    }

    fn execute(&self, request: &domain::TradeRequest) -> anyhow::Result<domain::ExecutionReceipt> {
        Ok(domain::ExecutionReceipt {
            execution_id: uuid::Uuid::new_v4().to_string(),
            run_id: request.run_id.clone(),
            loop_id: request.loop_id.clone(),
            caused_by_decision_id: request.decision_id.clone(),
            action: request.action.clone(),
            execution_kind: "open".into(),
            status: "filled".into(),
            broker_reference: "broker-order".into(),
            broker_position_id: Some("broker-position".into()),
            filled_quantity: request.order.quantity,
            average_price: Some(request.order.reference_price),
            rejection_reason: None,
            raw_fix_report: None,
            created_by_event_id: request.execution_event_id.clone(),
            executed_at: chrono::Utc::now(),
        })
    }

    fn close(
        &self,
        _position: &domain::PositionRecord,
        _order: &domain::OrderRecord,
        _caused_by_decision_id: &str,
        _execution_event_id: &str,
    ) -> anyhow::Result<domain::ExecutionReceipt> {
        anyhow::bail!("not used")
    }

    fn reference_price(&self, _instrument: &str) -> anyhow::Result<Option<f64>> {
        Ok(Some(100.0))
    }

    fn reconcile(&self) -> anyhow::Result<domain::BrokerSnapshot> {
        Ok(domain::BrokerSnapshot {
            adapter: "reconciliation-fixture".into(),
            connected: true,
            complete: true,
            positions: self
                .position_exists
                .load(Ordering::SeqCst)
                .then(|| domain::BrokerPosition {
                    broker_position_id: "broker-position".into(),
                    instrument: "BTCUSD".into(),
                    side: "BUY".into(),
                    quantity: 500.0,
                    average_price: Some(100.0),
                })
                .into_iter()
                .collect(),
            observed_at: chrono::Utc::now(),
        })
    }
}

#[test]
fn broker_truth_reconciles_a_local_open_position_without_model_input() {
    let directory = tempfile::tempdir().unwrap();
    let store = CanonicalStore::open(directory.path()).unwrap();
    let position_exists = Arc::new(AtomicBool::new(true));
    let broker = ReconcilingBroker {
        position_exists: Arc::clone(&position_exists),
    };
    let mut controller = HarnessController::with_runtime_broker(store, 0.0, Box::new(broker));
    let started = controller
        .start("Test BTCUSD breakout over 4 hours")
        .unwrap();
    let after_cycle = controller.run_cycle(&started.run_id).unwrap();
    assert_eq!(after_cycle.positions[0].state, "reconciled-open");

    position_exists.store(false, Ordering::SeqCst);
    controller.reconcile_broker(&started.run_id).unwrap();
    let replayed = controller.replay(&started.run_id).unwrap();
    assert_eq!(replayed.positions[0].state, "reconciled-closed");
    assert!(replayed
        .decisions
        .iter()
        .all(|decision| !decision.rationale.contains("reconcile")));
}

struct PartialFillBroker {
    positions: Arc<Mutex<Vec<domain::BrokerPosition>>>,
    execute_count: Arc<std::sync::atomic::AtomicUsize>,
    close_count: Arc<std::sync::atomic::AtomicUsize>,
}

impl ports::ExecutionBroker for PartialFillBroker {
    fn risk_snapshot(
        &self,
        instruments: &[String],
    ) -> anyhow::Result<Option<ports::BrokerRiskSnapshot>> {
        Ok(Some(fixture_risk_snapshot(instruments)))
    }

    fn risk_account_identity(&self) -> Option<ports::BrokerAccountIdentity> {
        Some(fixture_account_identity())
    }

    fn execute(&self, request: &domain::TradeRequest) -> anyhow::Result<domain::ExecutionReceipt> {
        self.execute_count.fetch_add(1, Ordering::SeqCst);
        let filled = request.order.quantity / 2.0;
        self.positions.lock().unwrap().push(domain::BrokerPosition {
            broker_position_id: "partial-position".into(),
            instrument: request.order.instrument.clone(),
            side: request.order.side.clone(),
            quantity: filled,
            average_price: Some(request.order.reference_price),
        });
        Ok(domain::ExecutionReceipt {
            execution_id: uuid::Uuid::new_v4().to_string(),
            run_id: request.run_id.clone(),
            loop_id: request.loop_id.clone(),
            caused_by_decision_id: request.decision_id.clone(),
            action: request.action.clone(),
            execution_kind: "open".into(),
            status: "partially-filled".into(),
            broker_reference: "partial-order".into(),
            broker_position_id: Some("partial-position".into()),
            filled_quantity: filled,
            average_price: Some(request.order.reference_price),
            rejection_reason: None,
            raw_fix_report: None,
            created_by_event_id: request.execution_event_id.clone(),
            executed_at: chrono::Utc::now(),
        })
    }

    fn close(
        &self,
        position: &domain::PositionRecord,
        order: &domain::OrderRecord,
        caused_by_decision_id: &str,
        execution_event_id: &str,
    ) -> anyhow::Result<domain::ExecutionReceipt> {
        self.close_count.fetch_add(1, Ordering::SeqCst);
        self.positions.lock().unwrap().clear();
        Ok(domain::ExecutionReceipt {
            execution_id: uuid::Uuid::new_v4().to_string(),
            run_id: position.run_id.clone(),
            loop_id: position.loop_id.clone(),
            caused_by_decision_id: caused_by_decision_id.into(),
            action: domain::Jev1Action::Long,
            execution_kind: "close".into(),
            status: "filled".into(),
            broker_reference: "partial-close".into(),
            broker_position_id: position.broker_position_id.clone(),
            filled_quantity: order.quantity,
            average_price: Some(order.reference_price),
            rejection_reason: None,
            raw_fix_report: None,
            created_by_event_id: execution_event_id.into(),
            executed_at: chrono::Utc::now(),
        })
    }

    fn reference_price(&self, _instrument: &str) -> anyhow::Result<Option<f64>> {
        Ok(Some(100.0))
    }

    fn reconcile(&self) -> anyhow::Result<domain::BrokerSnapshot> {
        Ok(domain::BrokerSnapshot {
            adapter: "partial-fixture".into(),
            connected: true,
            complete: true,
            positions: self.positions.lock().unwrap().clone(),
            observed_at: chrono::Utc::now(),
        })
    }
}

#[test]
fn partial_fill_uses_confirmed_quantity_and_stop_flattens_broker_risk() {
    let directory = tempfile::tempdir().unwrap();
    let positions = Arc::new(Mutex::new(Vec::new()));
    let execute_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let close_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let broker = PartialFillBroker {
        positions: Arc::clone(&positions),
        execute_count: Arc::clone(&execute_count),
        close_count: Arc::clone(&close_count),
    };
    let store = CanonicalStore::open(directory.path()).unwrap();
    let mut controller = HarnessController::with_runtime_broker(store, 0.0, Box::new(broker));
    let started = controller
        .start("Test BTCUSD breakout over 4 hours")
        .unwrap();
    controller.run_cycle(&started.run_id).unwrap();
    let replayed = controller.replay(&started.run_id).unwrap();
    assert_eq!(replayed.positions[0].state, "open-partial");
    assert_eq!(
        replayed.position_controls[0].quantity,
        replayed.executions[0].filled_quantity
    );
    assert!(replayed.position_controls[0].quantity < replayed.orders[0].quantity);

    positions.lock().unwrap()[0].quantity = replayed.orders[0].quantity;
    controller.reconcile_broker(&started.run_id).unwrap();
    assert_eq!(
        controller
            .replay(&started.run_id)
            .unwrap()
            .position_controls[0]
            .quantity,
        replayed.orders[0].quantity
    );

    let stopped = controller.stop(&started.run_id).unwrap();
    assert_eq!(stopped.status, "stopped");
    assert_eq!(close_count.load(Ordering::SeqCst), 1);
    assert!(positions.lock().unwrap().is_empty());
    assert_eq!(
        controller.replay(&started.run_id).unwrap().positions[0].state,
        "closed-on-stop"
    );
}

struct SnapshotOnlyBroker {
    connected: bool,
    complete: bool,
    remote_position: Option<domain::BrokerPosition>,
}

impl ports::ExecutionBroker for SnapshotOnlyBroker {
    fn execute(&self, _request: &domain::TradeRequest) -> anyhow::Result<domain::ExecutionReceipt> {
        anyhow::bail!("fixture does not execute entries")
    }

    fn close(
        &self,
        _position: &domain::PositionRecord,
        _order: &domain::OrderRecord,
        _caused_by_decision_id: &str,
        _execution_event_id: &str,
    ) -> anyhow::Result<domain::ExecutionReceipt> {
        anyhow::bail!("fixture does not close")
    }

    fn reference_price(&self, _instrument: &str) -> anyhow::Result<Option<f64>> {
        Ok(Some(100.0))
    }

    fn reconcile(&self) -> anyhow::Result<domain::BrokerSnapshot> {
        Ok(domain::BrokerSnapshot {
            adapter: "snapshot-fixture".into(),
            connected: self.connected,
            complete: self.complete,
            positions: self.remote_position.clone().into_iter().collect(),
            observed_at: chrono::Utc::now(),
        })
    }
}

#[test]
fn incomplete_snapshot_does_not_mutate_positions() {
    let directory = tempfile::tempdir().unwrap();
    let degraded = Arc::new(AtomicBool::new(false));
    let positions = Arc::new(Mutex::new(Vec::new()));
    let store = CanonicalStore::open(directory.path()).unwrap();
    let mut controller = HarnessController::with_runtime_broker(
        store,
        0.0,
        Box::new(DegradingBroker {
            degraded: Arc::clone(&degraded),
            positions,
        }),
    );
    let started = controller
        .start("Test BTCUSD breakout over 4 hours")
        .unwrap();
    controller.run_cycle(&started.run_id).unwrap();
    assert_eq!(
        controller.run_snapshot(&started.run_id).unwrap().positions[0].state,
        "open"
    );
    degraded.store(true, Ordering::SeqCst);
    controller.reconcile_broker(&started.run_id).unwrap();
    let replayed = controller.replay(&started.run_id).unwrap();
    assert_eq!(replayed.positions[0].state, "open");
    assert!(controller
        .run_snapshot(&started.run_id)
        .unwrap()
        .events
        .iter()
        .any(|event| event.kind == "broker_sync_degraded"));
}

struct DegradingBroker {
    degraded: Arc<AtomicBool>,
    positions: Arc<Mutex<Vec<domain::BrokerPosition>>>,
}

impl ports::ExecutionBroker for DegradingBroker {
    fn risk_snapshot(
        &self,
        instruments: &[String],
    ) -> anyhow::Result<Option<ports::BrokerRiskSnapshot>> {
        Ok(Some(fixture_risk_snapshot(instruments)))
    }

    fn risk_account_identity(&self) -> Option<ports::BrokerAccountIdentity> {
        Some(fixture_account_identity())
    }

    fn execute(&self, request: &domain::TradeRequest) -> anyhow::Result<domain::ExecutionReceipt> {
        let broker_position = domain::BrokerPosition {
            broker_position_id: "degrading-position".into(),
            instrument: request.order.instrument.clone(),
            side: request.order.side.clone(),
            quantity: request.order.quantity,
            average_price: Some(request.order.reference_price),
        };
        self.positions.lock().unwrap().push(broker_position);
        Ok(domain::ExecutionReceipt {
            execution_id: uuid::Uuid::new_v4().to_string(),
            run_id: request.run_id.clone(),
            loop_id: request.loop_id.clone(),
            caused_by_decision_id: request.decision_id.clone(),
            action: request.action.clone(),
            execution_kind: "open".into(),
            status: "filled".into(),
            broker_reference: "degrading-order".into(),
            broker_position_id: Some("degrading-position".into()),
            filled_quantity: request.order.quantity,
            average_price: Some(request.order.reference_price),
            rejection_reason: None,
            raw_fix_report: None,
            created_by_event_id: request.execution_event_id.clone(),
            executed_at: chrono::Utc::now(),
        })
    }

    fn close(
        &self,
        _position: &domain::PositionRecord,
        _order: &domain::OrderRecord,
        _caused_by_decision_id: &str,
        _execution_event_id: &str,
    ) -> anyhow::Result<domain::ExecutionReceipt> {
        anyhow::bail!("not used")
    }

    fn reference_price(&self, _instrument: &str) -> anyhow::Result<Option<f64>> {
        Ok(Some(100.0))
    }

    fn reconcile(&self) -> anyhow::Result<domain::BrokerSnapshot> {
        let degraded = self.degraded.load(Ordering::SeqCst);
        Ok(domain::BrokerSnapshot {
            adapter: "degrading-fixture".into(),
            connected: !degraded,
            complete: !degraded,
            positions: if degraded {
                Vec::new()
            } else {
                self.positions.lock().unwrap().clone()
            },
            observed_at: chrono::Utc::now(),
        })
    }
}

#[test]
fn broker_only_position_is_imported_with_canonical_control() {
    let directory = tempfile::tempdir().unwrap();
    let store = CanonicalStore::open(directory.path()).unwrap();
    let mut controller = HarnessController::with_runtime_broker(
        store,
        0.0,
        Box::new(SnapshotOnlyBroker {
            connected: true,
            complete: true,
            remote_position: Some(domain::BrokerPosition {
                broker_position_id: "remote-only".into(),
                instrument: "BTCUSD".into(),
                side: "BUY".into(),
                quantity: 0.25,
                average_price: Some(80_000.0),
            }),
        }),
    );
    let started = controller
        .start("Test BTCUSD breakout over 4 hours")
        .unwrap();
    controller.reconcile_broker(&started.run_id).unwrap();
    let replayed = controller.replay(&started.run_id).unwrap();
    assert_eq!(replayed.positions.len(), 1);
    assert_eq!(replayed.positions[0].state, "reconciled-open");
    assert_eq!(replayed.position_controls[0].quantity, 0.25);
    assert!(controller
        .run_snapshot(&started.run_id)
        .unwrap()
        .events
        .iter()
        .any(|event| event.kind == "broker_position_imported"));
}

#[test]
fn cancelled_cycle_cannot_cross_broker_execution_boundary() {
    let directory = tempfile::tempdir().unwrap();
    let positions = Arc::new(Mutex::new(Vec::new()));
    let execute_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let close_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let store = CanonicalStore::open(directory.path()).unwrap();
    let mut controller = HarnessController::with_runtime_broker(
        store,
        0.0,
        Box::new(PartialFillBroker {
            positions,
            execute_count: Arc::clone(&execute_count),
            close_count,
        }),
    );
    let started = controller
        .start("Test BTCUSD breakout over 4 hours")
        .unwrap();
    let count_before = execute_count.load(Ordering::SeqCst);
    let cancellation = AtomicBool::new(true);
    assert!(controller
        .run_cycle_cancellable(&started.run_id, &cancellation)
        .is_err());
    assert_eq!(execute_count.load(Ordering::SeqCst), count_before);
}

#[test]
fn world_model_turns_vague_and_precise_prompts_into_executable_hypotheses() {
    let directory = tempfile::tempdir().unwrap();
    let store = CanonicalStore::open(directory.path()).unwrap();
    let mut controller = HarnessController::new(store, 0.0);

    let vague = controller.start("Find me something worth testing").unwrap();
    let vague_hypothesis = &vague.hypotheses[0];
    assert_eq!(vague_hypothesis.instruments, vec!["BTCUSD"]);
    assert_eq!(vague_hypothesis.timeframe.source, "world-model-selected");
    assert!(!vague_hypothesis.strategy_mechanism.is_empty());
    assert!(!vague_hypothesis.jev_question.is_empty());
    assert!(!vague_hypothesis.review_rules.invalidate_evidence.is_empty());
    assert_eq!(vague.cadences[0].hypothesis_id, vague_hypothesis.id);

    let precise = controller
        .start("Test ETHUSD breakout over 4 hours")
        .unwrap();
    let precise_hypothesis = &precise.hypotheses[0];
    assert_eq!(precise_hypothesis.instruments, vec!["ETHUSD"]);
    assert_eq!(precise_hypothesis.strategy_mechanism, "breakout");
    assert_eq!(precise_hypothesis.timeframe.horizon_minutes, 240);
    assert_eq!(precise_hypothesis.timeframe.source, "user-explicit");

    let kept = controller
        .review_hypothesis(&HypothesisReviewRequest {
            run_id: precise.run_id.clone(),
            hypothesis_id: precise.hypotheses[0].id.clone(),
            evidence_query: "evidence remains supportive; keep the hypothesis".into(),
        })
        .unwrap();
    assert_eq!(kept.loops.len(), 1);

    let split_error = controller
        .review_hypothesis(&HypothesisReviewRequest {
            run_id: precise.run_id.clone(),
            hypothesis_id: precise.hypotheses[0].id.clone(),
            evidence_query: "evidence supports a competing timeframe; split".into(),
        })
        .unwrap_err();
    assert!(
        format!("{split_error:#}").contains("two distinct trusted supporting canonical records")
    );

    let modified = controller
        .review_hypothesis(&HypothesisReviewRequest {
            run_id: precise.run_id.clone(),
            hypothesis_id: precise.hypotheses[0].id.clone(),
            evidence_query: "modify to a different timeframe".into(),
        })
        .unwrap();
    assert_eq!(
        modified
            .loops
            .iter()
            .filter(|item| item.state != "stopped")
            .count(),
        1
    );

    let latest_id = modified.hypotheses.last().unwrap().id.clone();
    let stopped = controller
        .review_hypothesis(&HypothesisReviewRequest {
            run_id: precise.run_id.clone(),
            hypothesis_id: latest_id,
            evidence_query: "invalidation persists; stop this experiment".into(),
        })
        .unwrap();
    assert_eq!(
        stopped
            .loops
            .iter()
            .filter(|item| item.state != "stopped")
            .count(),
        0
    );
    assert!(stopped
        .loops
        .iter()
        .filter(|item| item.state == "stopped")
        .all(|item| item.allocated_fraction == 0.0));
    assert!(stopped
        .loops
        .iter()
        .filter(|item| item.state != "stopped")
        .all(|item| item.allocated_fraction == 1.0));

    let replayed = controller.replay(&precise.run_id).unwrap();
    assert_eq!(replayed.hypothesis_reviews.len(), 3);
    assert!(replayed.loops.iter().all(|item| item.state == "stopped"));
    let latest_allocation = replayed.capital_allocations.last().unwrap();
    assert_eq!(
        latest_allocation
            .allocations
            .iter()
            .map(|entry| entry.fraction)
            .sum::<f64>(),
        0.0
    );
    assert_eq!(
        latest_allocation
            .allocations
            .iter()
            .filter(|entry| entry.fraction == 0.0)
            .count(),
        2
    );
}

struct ScriptedJev {
    entries: Mutex<VecDeque<domain::Jev1Action>>,
    management: Mutex<VecDeque<domain::Jev2Action>>,
}

fn inference(question_id: &str) -> domain::JevInferenceMetadata {
    domain::JevInferenceMetadata {
        provider: "typesafe-contract-fixture".into(),
        requested_model: "jev-live-test".into(),
        returned_model: "jev-live-test".into(),
        question_id: question_id.into(),
        answer_type: "choice".into(),
        probabilities: Default::default(),
        usage: Default::default(),
        request_id: Some("fixture-request".into()),
    }
}

impl ports::JevEngine for ScriptedJev {
    fn decide_entry(
        &self,
        _state: &domain::ResolvedJevState,
        _question: &str,
    ) -> anyhow::Result<domain::Jev1Decision> {
        let action = self.entries.lock().unwrap().pop_front().unwrap();
        Ok(domain::Jev1Decision {
            action,
            confidence: 0.9,
            rationale: "scripted typed entry".into(),
            inference: inference("entry_action"),
        })
    }

    fn manage_position(
        &self,
        _state: &domain::ResolvedJevState,
        _question: &str,
    ) -> anyhow::Result<domain::Jev2Decision> {
        let action = self.management.lock().unwrap().pop_front().unwrap();
        Ok(domain::Jev2Decision {
            action,
            confidence: 0.9,
            rationale: "scripted typed management".into(),
            inference: inference("position_action"),
        })
    }
}

#[test]
fn continuous_loop_routes_buy_more_through_jev1_and_sell_back_to_jev1() {
    let directory = tempfile::tempdir().unwrap();
    let store = CanonicalStore::open(directory.path()).unwrap();
    let jev = ScriptedJev {
        entries: Mutex::new(VecDeque::from([
            domain::Jev1Action::Long,
            domain::Jev1Action::Long,
        ])),
        management: Mutex::new(VecDeque::from([
            domain::Jev2Action::Hold,
            domain::Jev2Action::BuyMore,
            domain::Jev2Action::Sell,
        ])),
    };
    let mut controller = HarnessController::with_runtime_adapters(
        store,
        0.0,
        Box::new(adapters::SimulatedWorldModel),
        Box::new(jev),
    );
    let started = controller.start("BTC continuation over 1 hour").unwrap();
    controller.run_cycle(&started.run_id).unwrap();
    controller.run_cycle(&started.run_id).unwrap();
    let after_buy_more = controller.run_cycle(&started.run_id).unwrap();
    assert_eq!(after_buy_more.loops[0].state, "jev1-confirm-add");
    let after_confirmation = controller.run_cycle(&started.run_id).unwrap();
    assert_eq!(after_confirmation.loops[0].state, "jev2");
    assert_eq!(
        after_confirmation
            .positions
            .iter()
            .filter(|position| position.state == "open")
            .count(),
        2
    );
    let after_sell = controller.run_cycle(&started.run_id).unwrap();
    assert_eq!(after_sell.loops[0].state, "jev1");
    assert!(after_sell
        .positions
        .iter()
        .any(|position| position.state == "closed"));

    let replayed = controller.replay(&started.run_id).unwrap();
    let actions: Vec<&str> = replayed
        .decisions
        .iter()
        .map(|item| item.action.as_str())
        .collect();
    let buy_more_index = actions
        .iter()
        .position(|action| *action == "BuyMore")
        .unwrap();
    let confirmed_long_index = actions
        .iter()
        .rposition(|action| *action == "Long")
        .unwrap();
    assert!(buy_more_index < confirmed_long_index);
    assert!(replayed
        .executions
        .iter()
        .any(|item| item.execution_kind == "close"));
}

#[test]
fn immutable_canonical_events_reject_rewrite_and_delete() {
    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("harness.sqlite3");
    let store = CanonicalStore::open(directory.path()).unwrap();
    let mut controller = HarnessController::new(store, 0.0);
    let active = controller.start("BTC continuation").unwrap();
    controller.run_cycle(&active.run_id).unwrap();

    let connection = Connection::open(database_path).unwrap();
    let update = connection.execute(
        "UPDATE canonical_events SET kind='rewritten' WHERE run_id=?1",
        params![active.run_id],
    );
    assert!(update.is_err());
    let delete = connection.execute(
        "DELETE FROM canonical_events WHERE run_id=?1",
        params![active.run_id],
    );
    assert!(delete.is_err());
}

#[test]
fn search_memory_exposes_exact_canonical_reference() {
    let directory = tempfile::tempdir().unwrap();
    let store = CanonicalStore::open(directory.path()).unwrap();
    let mut controller = HarnessController::new(store, 0.0);
    let active = controller.start("BTC continuation").unwrap();
    controller.run_cycle(&active.run_id).unwrap();
    let replayed = controller.replay(&active.run_id).unwrap();

    let hits = controller.search("caused execution").unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].canonical_entity_type, "execution");
    assert_eq!(
        hits[0].canonical_entity_id,
        replayed.executions[0].execution_id
    );
    assert_eq!(
        hits[0].canonical_event_id,
        replayed.executions[0].created_by_event_id
    );
}

#[test]
fn qdrant_context_pool_supports_harvey_retrieval_and_automatic_logs() {
    let directory = tempfile::tempdir().unwrap();
    let qdrant_directory = tempfile::tempdir().unwrap();
    let store = CanonicalStore::open(directory.path()).unwrap();
    let project_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf();
    let pool = QdrantContextPool::new(RetrievalConfig {
        python_path: project_root
            .join(".venv")
            .join("Scripts")
            .join("python.exe"),
        bridge_path: project_root
            .join("retrieval")
            .join("qdrant_local_bridge.py"),
        storage_path: qdrant_directory.path().to_path_buf(),
        models_path: project_root.join("runtime-harness").join("models"),
        collection: format!("phase3_test_{}", uuid::Uuid::new_v4()),
    })
    .unwrap();
    RetrievalConfig::load(&project_root).unwrap();
    let mut controller = HarnessController::with_retrieval(store.clone(), 0.0, pool);

    let active = controller
        .start("Compare BTC continuation after a directional pullback")
        .unwrap();
    controller.run_cycle(&active.run_id).unwrap();
    controller
        .ingest_context(&ContextIngestRequest {
            run_id: active.run_id.clone(),
            title: "Operator-supplied recorded note".into(),
            text: "A contradictory failed historical continuation experiment invalidated the same mechanism under high volatility context.".into(),
            source_class: ContextSourceClass::HistoricalRecorded,
            trust_level: TrustLevel::Verified,
            provenance_uri: "operator-note://continuation-case".into(),
            publisher: "operator".into(),
            tags: vec!["manual-ingest".into(), "historical".into()],
            metadata: serde_json::json!({ "fixture": true }),
        })
        .unwrap();
    let replayed = controller.replay(&active.run_id).unwrap();
    assert_eq!(replayed.context_pool_records.len(), 1);

    let review_package = controller
        .assemble_review_package(&HypothesisReviewRequest {
            run_id: active.run_id.clone(),
            hypothesis_id: active.hypotheses[0].id.clone(),
            evidence_query: "prior directional pullback continuation experiments".into(),
        })
        .unwrap();
    let world_trace: RetrievalTrace = review_package.historical_retrieval.unwrap();
    assert!(world_trace.sufficient);
    assert!(world_trace.steps.iter().any(|step| step.action == "search"));
    assert!(world_trace
        .steps
        .iter()
        .any(|step| step.action == "inspect"));
    assert!(world_trace
        .steps
        .iter()
        .any(|step| step.action == "decide_sufficiency"));
    assert!(world_trace
        .hits
        .iter()
        .any(|hit| hit.source_class == "historical_recorded"));
    assert!(world_trace
        .hits
        .iter()
        .any(|hit| hit.source_class == "internal_canonical"));

    let execution_id = replayed.executions[0].execution_id.clone();
    let exact_and_semantic = controller
        .retrieve_context(&RetrievalRequest {
            question: "prior directional pullback continuation experiments".into(),
            required_source_classes: vec![
                "historical_recorded".into(),
                "internal_canonical".into(),
            ],
            exact_canonical_ids: vec![execution_id.clone()],
            limit: 12,
            max_rounds: 3,
            filters: RetrievalFilters {
                run_id: Some(active.run_id.clone()),
                ..RetrievalFilters::default()
            },
        })
        .unwrap();
    assert!(exact_and_semantic.sufficient);
    assert!(exact_and_semantic
        .hits
        .iter()
        .any(|hit| hit.canonical_entity_id == execution_id));
    assert!(exact_and_semantic
        .hits
        .iter()
        .any(|hit| hit.source_class == "historical_recorded"));

    let comparison = controller
        .start("Test BTCUSD continuation over 1 hour in a different volatility context")
        .unwrap();
    controller.run_cycle(&comparison.run_id).unwrap();
    controller
        .review_hypothesis(&HypothesisReviewRequest {
            run_id: comparison.run_id.clone(),
            hypothesis_id: comparison.hypotheses[0].id.clone(),
            evidence_query:
                "Compare supporting and contradictory history and modify the hypothesis timeframe"
                    .into(),
        })
        .unwrap();
    let comparison_replay = controller.replay(&comparison.run_id).unwrap();
    let package = comparison_replay.review_packages.last().unwrap();
    assert!(!package.recent_jev_decisions.is_empty());
    assert!(!package.recent_executions.is_empty());
    assert!(package
        .exact_canonical_ids
        .contains(&package.current_thesis.id));
    let historical = package.historical_retrieval.as_ref().unwrap();
    assert!(historical
        .hits
        .iter()
        .any(|hit| hit.run_id == active.run_id));
    assert!(historical
        .hits
        .windows(2)
        .all(|pair| pair[0].observed_at <= pair[1].observed_at));
    let current_decision_id = &package.recent_jev_decisions[0].id;
    assert!(historical
        .hits
        .iter()
        .any(|hit| &hit.canonical_entity_id == current_decision_id));
    assert!(historical
        .hits
        .iter()
        .filter(|hit| hit.tags.iter().any(|tag| tag == "experiment-memory"))
        .all(|hit| {
            hit.metadata
                .get("loop_id")
                .is_some_and(|value| !value.is_null())
                && hit
                    .metadata
                    .get("thesis_version_id")
                    .is_some_and(|value| !value.is_null())
                && hit
                    .metadata
                    .get("context_version_id")
                    .is_some_and(|value| !value.is_null())
                && hit
                    .metadata
                    .get("instruments")
                    .is_some_and(|value| value.as_array().is_some_and(|items| !items.is_empty()))
                && hit
                    .metadata
                    .get("timeframe")
                    .is_some_and(|value| !value.is_null())
                && hit
                    .metadata
                    .get("outcome")
                    .is_some_and(|value| !value.is_null())
                && hit.metadata.get("canonical_record_uri").is_some()
        }));
    assert!(package.evidence.iter().any(|item| matches!(
        item.relationship,
        domain::EvidenceRelationship::Contradictory
    )));
    let candidate_review = comparison_replay.hypothesis_reviews.last().unwrap();
    assert_eq!(candidate_review.action, domain::HypothesisAction::Modify);
    assert!(candidate_review.candidate_hypothesis.is_none());
    let serialized_package = serde_json::to_string(package).unwrap().to_ascii_lowercase();
    assert!(!serialized_package.contains("gpt-6"));
    assert!(!serialized_package.contains("opus"));
    assert!(!serialized_package.contains("openrouter"));

    assert!(store
        .pending_retrieval_events(&active.run_id, 10)
        .unwrap()
        .is_empty());
    controller.stop(&active.run_id).unwrap();
    assert!(store
        .pending_retrieval_events(&active.run_id, 10)
        .unwrap()
        .is_empty());
}
