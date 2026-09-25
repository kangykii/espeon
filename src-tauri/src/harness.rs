use crate::adapters::*;
use crate::context::StructuredContextResolver;
use crate::domain::*;
use crate::ports::*;
use crate::replay;
use crate::retrieval::QdrantContextPool;
use crate::risk::{idempotency_key, CloseRiskInput, DeterministicRiskEngine, EntryRiskInput};
use crate::storage::CanonicalStore;
use anyhow::{bail, Context, Result};
use chrono::Utc;
use serde_json::json;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use uuid::Uuid;

struct ActiveRun {
    thesis: String,
    hypotheses: Vec<HypothesisDefinition>,
    cadences: Vec<LoopCadence>,
    thesis_versions: Vec<ThesisVersion>,
    context_definition: ContextDefinition,
    context_versions: Vec<ContextVersion>,
    loops: Vec<LoopView>,
    positions: Vec<PositionRecord>,
    position_controls: Vec<PositionControlRecord>,
    failure_states: HashMap<String, LoopFailureState>,
    last_event_id: String,
}

#[derive(Clone)]
pub struct AutonomousReviewJob {
    pub trigger: AutonomousReviewTriggerRecord,
    pub request: HypothesisReviewRequest,
    pub package: WorldModelReviewPackage,
}

pub struct HarnessController {
    store: CanonicalStore,
    world_model: Arc<dyn WorldModel>,
    jev: Box<dyn JevEngine>,
    context_resolver: Box<dyn ContextResolver>,
    market_data: Box<dyn MarketDataProvider>,
    broker: Box<dyn ExecutionBroker>,
    risk: DeterministicRiskEngine,
    minimum_confidence: f64,
    allocator: EqualCapitalAllocator,
    context_pool: Option<QdrantContextPool>,
    active_runs: HashMap<String, ActiveRun>,
    review_policy: AutonomousReviewPolicy,
    pending_review_jobs: Vec<AutonomousReviewJob>,
    immediate_cycle_runs: std::collections::HashSet<String>,
}

impl HarnessController {
    pub fn has_active_runs(&self) -> bool {
        !self.active_runs.is_empty()
    }

    pub fn new(store: CanonicalStore, minimum_confidence: f64) -> Self {
        Self {
            store,
            world_model: Arc::new(SimulatedWorldModel),
            jev: Box::new(SimulatedJev),
            context_resolver: Box::new(StructuredContextResolver),
            market_data: Box::new(crate::market_data::SimulatedMarketDataProvider::new()),
            broker: Box::new(SimulatedBroker),
            risk: DeterministicRiskEngine::new(RiskPolicyConfig::default()),
            minimum_confidence,
            allocator: EqualCapitalAllocator,
            context_pool: None,
            active_runs: HashMap::new(),
            review_policy: AutonomousReviewPolicy::default(),
            pending_review_jobs: Vec::new(),
            immediate_cycle_runs: std::collections::HashSet::new(),
        }
    }

    pub fn with_adapters(
        store: CanonicalStore,
        minimum_confidence: f64,
        context_pool: QdrantContextPool,
        world_model: Box<dyn WorldModel>,
        jev: Box<dyn JevEngine>,
        broker: Box<dyn ExecutionBroker>,
        risk_policy: RiskPolicyConfig,
    ) -> Self {
        Self::with_optional_retrieval_adapters(
            store,
            minimum_confidence,
            Some(context_pool),
            world_model,
            jev,
            broker,
            risk_policy,
        )
    }

    pub fn with_optional_retrieval_adapters(
        store: CanonicalStore,
        minimum_confidence: f64,
        context_pool: Option<QdrantContextPool>,
        world_model: Box<dyn WorldModel>,
        jev: Box<dyn JevEngine>,
        broker: Box<dyn ExecutionBroker>,
        risk_policy: RiskPolicyConfig,
    ) -> Self {
        let mut controller = Self::new(store, minimum_confidence);
        controller.context_pool = context_pool;
        controller.world_model = Arc::from(world_model);
        controller.jev = jev;
        controller.broker = broker;
        controller.risk = DeterministicRiskEngine::new(risk_policy);
        controller
    }

    #[allow(dead_code)]
    pub fn with_runtime_adapters(
        store: CanonicalStore,
        minimum_confidence: f64,
        world_model: Box<dyn WorldModel>,
        jev: Box<dyn JevEngine>,
    ) -> Self {
        let mut controller = Self::new(store, minimum_confidence);
        controller.world_model = Arc::from(world_model);
        controller.jev = jev;
        controller
    }

    #[allow(dead_code)]
    pub fn with_runtime_broker(
        store: CanonicalStore,
        minimum_confidence: f64,
        broker: Box<dyn ExecutionBroker>,
    ) -> Self {
        let mut controller = Self::new(store, minimum_confidence);
        controller.broker = broker;
        controller
    }

    pub fn with_retrieval(
        store: CanonicalStore,
        minimum_confidence: f64,
        context_pool: QdrantContextPool,
    ) -> Self {
        let mut controller = Self::new(store, minimum_confidence);
        controller.context_pool = Some(context_pool);
        controller
    }

    pub fn configure_autonomous_reviews(&mut self, policy: AutonomousReviewPolicy) {
        self.review_policy = policy;
    }

    pub fn configure_market_data(&mut self, provider: Box<dyn MarketDataProvider>) {
        self.market_data = provider;
    }

    pub fn world_model(&self) -> Arc<dyn WorldModel> {
        Arc::clone(&self.world_model)
    }

    pub fn take_review_jobs(&mut self, run_id: &str) -> Vec<AutonomousReviewJob> {
        let mut selected = Vec::new();
        self.pending_review_jobs.retain(|job| {
            if job.trigger.run_id == run_id {
                selected.push(job.clone());
                false
            } else {
                true
            }
        });
        selected
    }

    pub fn review_retry_policy(&self) -> (u32, u64) {
        (
            self.review_policy.retry_attempts.max(1),
            self.review_policy.retry_base_seconds,
        )
    }

    pub fn take_immediate_cycle(&mut self, run_id: &str) -> bool {
        self.immediate_cycle_runs.remove(run_id)
    }

    fn record_autonomous_review_package(
        &mut self,
        package: &mut WorldModelReviewPackage,
        trigger: &AutonomousReviewTriggerRecord,
    ) -> Result<()> {
        let event_id = Uuid::new_v4().to_string();
        package.created_by_event_id = event_id.clone();
        package.assembled_at = Utc::now();
        let package_event = event(
            &event_id,
            &trigger.run_id,
            Some(&trigger.loop_id),
            "autonomous_review_package_assembled",
            "world_model_review_package",
            &package.id,
            Some(&trigger.created_by_event_id),
            "Immutable trigger-aware review package assembled from canonical state",
            json!({"record":package}),
        );
        self.store.append_event(&package_event)?;
        if let Some(active) = self.active_runs.get_mut(&trigger.run_id) {
            active.last_event_id = event_id;
        }
        Ok(())
    }

    pub fn record_review_job_state(
        &mut self,
        job: &AutonomousReviewJob,
        status: &str,
        attempts: u32,
        error: Option<&str>,
    ) -> Result<()> {
        if !self.is_active(&job.trigger.run_id) {
            return Ok(());
        }
        let mut record = job.trigger.clone();
        record.status = status.into();
        record.attempts = attempts;
        record.next_retry_at = (status == "retrying").then(|| {
            Utc::now()
                + chrono::Duration::seconds(
                    self.review_policy
                        .retry_base_seconds
                        .saturating_mul(1u64 << attempts.saturating_sub(1).min(8))
                        as i64,
                )
        });
        record.created_by_event_id = Uuid::new_v4().to_string();
        record.created_at = Utc::now();
        let kind = match status {
            "running" => "autonomous_review_started",
            "cancelled" => "autonomous_review_cancelled",
            _ => "autonomous_review_failed",
        };
        let event_record = event(
            &record.created_by_event_id,
            &record.run_id,
            Some(&record.loop_id),
            kind,
            "autonomous_review_trigger",
            &record.trigger_id,
            Some(&job.package.created_by_event_id),
            match status {
                "running" => "Autonomous review inference started outside the controller mutex",
                "cancelled" => "Queued or completed provider work was cancelled before mutation",
                _ => "Autonomous review provider attempt failed; no action was applied",
            },
            json!({"record":record,"error":error}),
        );
        self.store
            .record_autonomous_review_trigger(&record, &event_record)?;
        if let Some(active) = self.active_runs.get_mut(&record.run_id) {
            active.last_event_id = record.created_by_event_id;
        }
        Ok(())
    }

    pub fn start(&mut self, human_thesis: &str) -> Result<RunSnapshot> {
        if human_thesis.trim().is_empty() {
            bail!("thesis cannot be empty");
        }
        let run_id = Uuid::new_v4().to_string();
        let run_event_id = Uuid::new_v4().to_string();
        let run_event = event(
            &run_event_id,
            &run_id,
            None,
            "run_started",
            "run",
            &run_id,
            None,
            "Run started from human thesis",
            json!({ "humanThesis": human_thesis }),
        );
        self.store.create_run(&run_id, human_thesis, &run_event)?;

        let startup = match self.world_model.formulate(
            &run_id,
            human_thesis,
            self.context_pool
                .as_ref()
                .map(|pool| pool as &dyn ContextRetriever),
        ) {
            Ok(startup) => startup,
            Err(error) => {
                let stopped_at = Utc::now();
                let stop_event_id = Uuid::new_v4().to_string();
                let stop_event = event(
                    &stop_event_id,
                    &run_id,
                    None,
                    "run_stopped",
                    "run",
                    &run_id,
                    Some(&run_event_id),
                    "Run startup failed before an executable hypothesis was committed",
                    json!({"stoppedAt":stopped_at,"startupError":error.to_string()}),
                );
                self.store.stop_run(&run_id, stopped_at, &stop_event)?;
                return Err(error.context("world model could not formulate the run"));
            }
        };
        let WorldModelStartupOutput {
            thesis,
            context_definition,
            context,
            hypothesis,
            retrieval_trace: startup_retrieval_trace,
        } = startup;
        let thesis_event = event(
            &thesis.created_by_event_id,
            &run_id,
            None,
            "thesis_version_created",
            "thesis_version",
            &thesis.id,
            Some(&run_event_id),
            "World model created an immutable thesis version",
            json!({ "record": thesis }),
        );
        self.store.record_thesis(&thesis, &thesis_event)?;

        let context_definition_event = event(
            &context_definition.created_by_event_id,
            &run_id,
            None,
            "context_definition_created",
            "context_definition",
            &context_definition.id,
            Some(&thesis.created_by_event_id),
            "World model created a context definition",
            json!({ "record": context_definition }),
        );
        self.store
            .record_context_definition(&context_definition, &context_definition_event)?;

        let context_event = event(
            &context.created_by_event_id,
            &run_id,
            None,
            "context_version_created",
            "context_version",
            &context.id,
            Some(&context_definition.created_by_event_id),
            "World model created an immutable context version",
            json!({ "record": context }),
        );
        self.store
            .record_context_version(&context, &context_event)?;

        let hypothesis_event = event(
            &hypothesis.created_by_event_id,
            &run_id,
            None,
            "hypothesis_version_created",
            "hypothesis_definition",
            &hypothesis.id,
            Some(&context.created_by_event_id),
            "World model created a complete executable hypothesis",
            json!({ "record": hypothesis, "retrievalTrace": startup_retrieval_trace }),
        );
        self.store
            .record_hypothesis(&hypothesis, &hypothesis_event)?;

        let primary_loop_event_id = Uuid::new_v4().to_string();
        let primary_loop = LoopView {
            id: Uuid::new_v4().to_string(),
            run_id: run_id.clone(),
            parent_loop_id: None,
            parent_thesis_version_id: None,
            hypothesis_id: hypothesis.id.clone(),
            thesis_version_id: thesis.id.clone(),
            context_version_id: context.id.clone(),
            thesis_version: thesis.version,
            context_version: context.version,
            state: "jev1".into(),
            allocated_fraction: 0.0,
            created_by_event_id: primary_loop_event_id.clone(),
            created_at: Utc::now(),
            stopped_at: None,
        };
        let primary_loop_event = event(
            &primary_loop_event_id,
            &run_id,
            Some(&primary_loop.id),
            "loop_spawned",
            "loop",
            &primary_loop.id,
            Some(&hypothesis.created_by_event_id),
            "Primary Jev loop spawned",
            json!({ "record": primary_loop }),
        );
        self.store.spawn_loop(&primary_loop, &primary_loop_event)?;

        let mut loops = vec![primary_loop.clone()];
        let cadence = cadence_for(&primary_loop.id, &hypothesis);
        let cadence_event = event(
            &cadence.created_by_event_id,
            &run_id,
            Some(&primary_loop.id),
            "loop_cadence_mapped",
            "loop_cadence",
            &primary_loop.id,
            Some(&primary_loop_event_id),
            "Harness deterministically mapped hypothesis timeframe to Jev cadence",
            json!({ "record": cadence }),
        );
        self.store.record_cadence(&cadence, &cadence_event)?;
        let last_loop_event_id = cadence.created_by_event_id.clone();

        let allocation = self.allocator.allocation_for(loops.len());
        for loop_state in &mut loops {
            loop_state.allocated_fraction = allocation;
        }
        let allocation_event_id = Uuid::new_v4().to_string();
        let allocation_record = CapitalAllocationRecord {
            id: Uuid::new_v4().to_string(),
            run_id: run_id.clone(),
            reason: "equal allocation across active loops".into(),
            allocations: loops
                .iter()
                .map(|loop_state| AllocationEntry {
                    loop_id: loop_state.id.clone(),
                    fraction: loop_state.allocated_fraction,
                })
                .collect(),
            created_by_event_id: allocation_event_id.clone(),
            created_at: Utc::now(),
        };
        let allocation_event = event(
            &allocation_event_id,
            &run_id,
            None,
            "capital_allocation_changed",
            "capital_allocation",
            &allocation_record.id,
            Some(&last_loop_event_id),
            "Harness recorded equal capital allocation",
            json!({ "record": allocation_record }),
        );
        self.store
            .record_allocation(&allocation_record, &allocation_event)?;

        let entry_state = match self.resolve_live_jev_state(
            &run_id,
            &primary_loop.id,
            &hypothesis,
            &thesis,
            &context,
            None,
            &allocation_event_id,
        ) {
            Ok(state) => state,
            Err(error) => {
                self.record_live_context_failure(
                    &run_id,
                    &primary_loop.id,
                    &hypothesis.id,
                    &allocation_event_id,
                    &error,
                )?;
                self.active_runs.insert(
                    run_id.clone(),
                    ActiveRun {
                        thesis: human_thesis.into(),
                        hypotheses: vec![hypothesis],
                        cadences: vec![cadence],
                        thesis_versions: vec![thesis],
                        context_definition,
                        context_versions: vec![context],
                        loops,
                        positions: Vec::new(),
                        position_controls: Vec::new(),
                        failure_states: HashMap::new(),
                        last_event_id: allocation_event_id,
                    },
                );
                self.immediate_cycle_runs.insert(run_id.clone());
                if let Some(context_pool) = &self.context_pool {
                    context_pool.sync_pending_events(&self.store, &run_id)?;
                }
                return self.snapshot(&run_id);
            }
        };
        let entry_reference_price = entry_state
            .live_context_snapshot
            .as_ref()
            .map(|snapshot| snapshot.quote.mid)
            .context("initial live context omitted its quote envelope")?;
        let entry = match self
            .jev
            .decide_entry(&entry_state, &hypothesis.jev_question)
        {
            Ok(value) => value,
            Err(error) => {
                let failure_id = Uuid::new_v4().to_string();
                let failure = event(
                    &failure_id,
                    &run_id,
                    Some(&primary_loop.id),
                    "jev_inference_failed",
                    "inference_error",
                    &failure_id,
                    Some(&allocation_event_id),
                    "Initial Jev1 API failure recorded separately from a trading decision",
                    json!({"stage":"jev1","error":error.to_string()}),
                );
                self.store.append_event(&failure)?;
                return Err(error);
            }
        };
        let entry_event_id = Uuid::new_v4().to_string();
        let entry_record = DecisionRecord {
            id: Uuid::new_v4().to_string(),
            run_id: run_id.clone(),
            loop_id: primary_loop.id.clone(),
            stage: "jev1".into(),
            thesis_version_id: thesis.id.clone(),
            context_version_id: context.id.clone(),
            position_id: None,
            action: format!("{:?}", entry.action),
            confidence: entry.confidence,
            rationale: entry.rationale.clone(),
            inference: entry.inference.clone(),
            resolved_state: entry_state,
            created_by_event_id: entry_event_id.clone(),
            created_at: Utc::now(),
        };
        let entry_event = event(
            &entry_event_id,
            &run_id,
            Some(&primary_loop.id),
            "jev1_decision_recorded",
            "decision",
            &entry_record.id,
            Some(&allocation_event_id),
            "Jev1 decision recorded against exact thesis and context versions",
            json!({ "record": entry_record }),
        );
        self.store.record_decision(&entry_record, &entry_event)?;

        let execution_event_id = Uuid::new_v4().to_string();
        let side = match entry.action {
            Jev1Action::Long => "BUY",
            Jev1Action::Short => "SELL",
            Jev1Action::NoTrade => "NO_TRADE",
        };
        let instrument = hypothesis
            .instruments
            .first()
            .map(String::as_str)
            .unwrap_or("UNSPECIFIED");
        let duplicate_key = idempotency_key(
            &run_id,
            &primary_loop.id,
            &entry_record.id,
            instrument,
            side,
            entry_record.created_at,
            self.risk.config.duplicate_order_window_seconds,
        );
        let reference_price = entry_reference_price;
        let (order, gate) = self.risk.evaluate_entry(EntryRiskInput {
            run_id: &run_id,
            loop_id: &primary_loop.id,
            decision_id: &entry_record.id,
            action: entry.action.clone(),
            confidence: entry.confidence,
            signal_at: entry_record.created_at,
            instrument,
            reference_price,
            allocated_fraction: loops[0].allocated_fraction,
            current_total_exposure: 0.0,
            open_positions_in_loop: 0,
            duplicate: self.store.order_exists(&duplicate_key)?,
            minimum_confidence: self.minimum_confidence,
        });
        let order_event = event(
            &order.created_by_event_id,
            &run_id,
            Some(&primary_loop.id),
            "order_evaluated",
            "order",
            &order.id,
            Some(&entry_event_id),
            "Deterministic policy constructed and evaluated the entry order",
            json!({"record":order,"result":gate}),
        );
        self.store.record_order(&order, &order_event)?;
        let request = TradeRequest {
            run_id: run_id.clone(),
            loop_id: primary_loop.id.clone(),
            decision_id: entry_record.id.clone(),
            execution_event_id: execution_event_id.clone(),
            action: entry.action.clone(),
            confidence: entry.confidence,
            order: order.clone(),
        };
        let gate_event_id = Uuid::new_v4().to_string();
        let gate_event = event(
            &gate_event_id,
            &run_id,
            Some(&primary_loop.id),
            "guardrail_evaluated",
            "decision",
            &entry_record.id,
            Some(&order.created_by_event_id),
            "All deterministic execution checks evaluated the order",
            json!({ "decisionId": entry_record.id, "result": gate }),
        );
        self.store.append_event(&gate_event)?;

        let mut positions = Vec::new();
        let mut position_controls = Vec::new();
        let memory_causal_event_id;
        let memory_entity_type;
        let memory_entity_id;
        let evidence;
        if gate.accepted && !matches!(entry.action, Jev1Action::NoTrade) {
            let execution = self.broker.execute(&request).unwrap_or_else(|error| {
                broker_failure_receipt(&order, "open", &execution_event_id, &error)
            });
            let execution_event = event(
                &execution.created_by_event_id,
                &run_id,
                Some(&primary_loop.id),
                "execution_recorded",
                "execution",
                &execution.execution_id,
                Some(&gate_event_id),
                "Execution recorded with its causal Jev decision",
                json!({ "record": execution }),
            );
            self.store.record_execution(&execution, &execution_event)?;
            if !execution_has_fill(&execution) {
                loops[0].state = "jev1-broker-rejected".into();
                evidence = format!(
                    "Decision {} produced broker rejection {} without opening a position.",
                    entry_record.id, execution.execution_id
                );
                memory_causal_event_id = execution.created_by_event_id.clone();
                memory_entity_type = "execution";
                memory_entity_id = execution.execution_id.clone();
            } else {
                let position_event_id = Uuid::new_v4().to_string();
                let mut position = PositionRecord {
                    id: Uuid::new_v4().to_string(),
                    run_id: run_id.clone(),
                    loop_id: primary_loop.id.clone(),
                    opened_by_execution_id: execution.execution_id.clone(),
                    broker_position_id: execution.broker_position_id.clone(),
                    direction: format!("{:?}", execution.action),
                    state: if execution_fully_filled(&execution, &order) {
                        "open"
                    } else {
                        "open-partial"
                    }
                    .into(),
                    opened_at: execution.executed_at,
                    closed_by_execution_id: None,
                    closed_at: None,
                    last_event_id: position_event_id.clone(),
                };
                let position_event = event(
                    &position_event_id,
                    &run_id,
                    Some(&primary_loop.id),
                    "position_opened",
                    "position",
                    &position.id,
                    Some(&execution.created_by_event_id),
                    if execution_fully_filled(&execution, &order) {
                        "Position state opened from exact execution"
                    } else {
                        "Partially filled position opened from confirmed cumulative broker fill"
                    },
                    json!({ "record": position }),
                );
                self.store.open_position(&position, &position_event)?;
                let control = PositionControlRecord {
                    position_id: position.id.clone(),
                    order_id: order.id.clone(),
                    instrument: order.instrument.clone(),
                    quantity: execution.filled_quantity,
                    entry_price: execution.average_price.unwrap_or(order.reference_price),
                    notional: execution.filled_quantity
                        * execution.average_price.unwrap_or(order.reference_price),
                    stop_loss_price: order
                        .stop_loss_price
                        .context("approved entry order is missing stop-loss price")?,
                    created_by_event_id: Uuid::new_v4().to_string(),
                    created_at: Utc::now(),
                };
                let control_event = event(
                    &control.created_by_event_id,
                    &run_id,
                    Some(&primary_loop.id),
                    "position_control_created",
                    "position_control",
                    &position.id,
                    Some(&position_event_id),
                    "Deterministic size and stop-loss controls attached to position",
                    json!({"record":control}),
                );
                self.store
                    .record_position_control(&control, &control_event)?;
                position_controls.push(control);

                let mut management_state = entry_record.resolved_state.clone();
                management_state.position = Some(json!({
                    "positionId": position.id,
                    "direction": position.direction,
                    "state": position.state,
                    "openedAt": position.opened_at,
                    "openedByExecutionId": position.opened_by_execution_id,
                }));
                management_state.resolved_at = Utc::now();
                let management = match self.jev.manage_position(
                &management_state,
                "Choose HOLD, SELL, or BUY MORE for the current position using only the supplied thesis, timeframe, context, market state, and position state.",
            ) {
                Ok(value) => value,
                Err(error) => {
                    let failure_id = Uuid::new_v4().to_string();
                    let failure = event(
                        &failure_id,
                        &run_id,
                        Some(&primary_loop.id),
                        "jev_inference_failed",
                        "inference_error",
                        &failure_id,
                        Some(&position_event_id),
                        "Initial Jev2 API failure recorded separately from a trading decision",
                        json!({"stage":"jev2","error":error.to_string()}),
                    );
                    self.store.append_event(&failure)?;
                    return Err(error);
                }
            };
                let management_action = management.action.clone();
                let management_event_id = Uuid::new_v4().to_string();
                let management_record = DecisionRecord {
                    id: Uuid::new_v4().to_string(),
                    run_id: run_id.clone(),
                    loop_id: primary_loop.id.clone(),
                    stage: "jev2".into(),
                    thesis_version_id: thesis.id.clone(),
                    context_version_id: context.id.clone(),
                    position_id: Some(position.id.clone()),
                    action: format!("{:?}", management.action),
                    confidence: management.confidence,
                    rationale: management.rationale,
                    inference: management.inference,
                    resolved_state: management_state,
                    created_by_event_id: management_event_id.clone(),
                    created_at: Utc::now(),
                };
                let management_event = event(
                    &management_event_id,
                    &run_id,
                    Some(&primary_loop.id),
                    "jev2_decision_recorded",
                    "decision",
                    &management_record.id,
                    Some(&position_event_id),
                    "Jev2 decision recorded against exact versions and position",
                    json!({ "record": management_record }),
                );
                self.store
                    .record_decision(&management_record, &management_event)?;
                match management_action {
                    Jev2Action::Hold => loops[0].state = "jev2-hold".into(),
                    Jev2Action::BuyMore => loops[0].state = "jev1-confirm-add".into(),
                    Jev2Action::Sell => {
                        let control = position_controls.last().cloned().context(
                            "initial position is missing deterministic position control",
                        )?;
                        let side = if position.direction.to_ascii_lowercase().contains("short") {
                            "BUY"
                        } else {
                            "SELL"
                        };
                        let duplicate_key = idempotency_key(
                            &run_id,
                            &primary_loop.id,
                            &management_record.id,
                            &control.instrument,
                            side,
                            management_record.created_at,
                            self.risk.config.duplicate_order_window_seconds,
                        );
                        let (close_order, close_gate) = self.risk.evaluate_close(CloseRiskInput {
                            run_id: &run_id,
                            loop_id: &primary_loop.id,
                            decision_id: &management_record.id,
                            direction: &position.direction,
                            control: &control,
                            signal_at: management_record.created_at,
                            duplicate: self.store.order_exists(&duplicate_key)?,
                            stop_triggered: false,
                            reference_price: self
                                .broker
                                .reference_price(&control.instrument)
                                .unwrap_or(None)
                                .unwrap_or(0.0),
                        });
                        let order_event = event(
                        &close_order.created_by_event_id,
                        &run_id,
                        Some(&primary_loop.id),
                        "order_evaluated",
                        "order",
                        &close_order.id,
                        Some(&management_event_id),
                        "Deterministic policy constructed and evaluated the initial close order",
                        json!({"record":close_order,"result":close_gate}),
                    );
                        self.store.record_order(&close_order, &order_event)?;
                        if !close_gate.accepted {
                            loops[0].state = "jev2-hold".into();
                        } else {
                            let close_event_id = Uuid::new_v4().to_string();
                            let close_execution = self
                                .broker
                                .close(
                                    &position,
                                    &close_order,
                                    &management_record.id,
                                    &close_event_id,
                                )
                                .unwrap_or_else(|error| {
                                    broker_failure_receipt(
                                        &close_order,
                                        "close",
                                        &close_event_id,
                                        &error,
                                    )
                                });
                            let close_execution_event = event(
                            &close_event_id,
                            &run_id,
                            Some(&primary_loop.id),
                            "execution_recorded",
                            "execution",
                            &close_execution.execution_id,
                            Some(&close_order.created_by_event_id),
                            "Paper close executed from the validated initial Jev2 SELL decision",
                            json!({"record":close_execution}),
                        );
                            self.store
                                .record_execution(&close_execution, &close_execution_event)?;
                            if execution_fully_filled(&close_execution, &close_order) {
                                position.state = "closed".into();
                                position.closed_by_execution_id =
                                    Some(close_execution.execution_id);
                                position.closed_at = Some(close_execution.executed_at);
                                position.last_event_id = Uuid::new_v4().to_string();
                                let position_close_event = event(
                                    &position.last_event_id,
                                    &run_id,
                                    Some(&primary_loop.id),
                                    "position_closed",
                                    "position",
                                    &position.id,
                                    Some(&close_event_id),
                                    "Initial position closed and loop returned to Jev1",
                                    json!({"record":position}),
                                );
                                self.store
                                    .close_position(&position, &position_close_event)?;
                                loops[0].state = "jev1".into();
                            } else if execution_has_fill(&close_execution) {
                                let remaining =
                                    (control.quantity - close_execution.filled_quantity).max(0.0);
                                let mut updated_control = control.clone();
                                updated_control.quantity = remaining;
                                updated_control.notional = remaining * updated_control.entry_price;
                                updated_control.created_by_event_id = Uuid::new_v4().to_string();
                                updated_control.created_at = Utc::now();
                                position.state = "open-partial-close".into();
                                position.last_event_id =
                                    updated_control.created_by_event_id.clone();
                                let partial_event = event(
                                    &updated_control.created_by_event_id,
                                    &run_id,
                                    Some(&primary_loop.id),
                                    "position_partially_closed",
                                    "position_control",
                                    &position.id,
                                    Some(&close_event_id),
                                    "Partial broker close reduced the canonical remaining quantity",
                                    json!({"record":position,"control":updated_control,"filledQuantity":close_execution.filled_quantity}),
                                );
                                self.store.record_partial_close(
                                    &position,
                                    &updated_control,
                                    &partial_event,
                                )?;
                                if let Some(existing) = position_controls
                                    .iter_mut()
                                    .find(|item| item.position_id == position.id)
                                {
                                    *existing = updated_control;
                                }
                                loops[0].state = "jev2".into();
                            } else {
                                loops[0].state = "jev2-broker-rejected".into();
                            }
                        }
                    }
                }
                evidence = format!(
                "Decision {} caused execution {} and position {} using thesis {} and context {}.",
                entry_record.id, execution.execution_id, position.id, thesis.id, context.id
            );
                memory_causal_event_id = execution.created_by_event_id.clone();
                memory_entity_type = "execution";
                memory_entity_id = execution.execution_id.clone();
                positions.push(position);
            }
        } else {
            loops[0].state = "jev1-no-trade".into();
            evidence = format!(
                "Decision {} was converted to NO TRADE using thesis {} and context {}.",
                entry_record.id, thesis.id, context.id
            );
            memory_causal_event_id = entry_record.created_by_event_id.clone();
            memory_entity_type = "decision";
            memory_entity_id = entry_record.id.clone();
        }

        let initial_transition_event_id = Uuid::new_v4().to_string();
        let initial_transition_event = event(
            &initial_transition_event_id,
            &run_id,
            Some(&primary_loop.id),
            "loop_state_transitioned",
            "loop",
            &primary_loop.id,
            Some(&memory_causal_event_id),
            "Harness applied the initial typed Jev state transition",
            json!({"record":loops[0]}),
        );
        self.store
            .transition_loop(&loops[0], &initial_transition_event)?;

        let memory_event_id = Uuid::new_v4().to_string();
        let memory = MemoryDocument {
            id: Uuid::new_v4().to_string(),
            run_id: run_id.clone(),
            canonical_entity_type: memory_entity_type.into(),
            canonical_entity_id: memory_entity_id.clone(),
            canonical_event_id: memory_causal_event_id.clone(),
            text: evidence,
            indexed_by_event_id: memory_event_id.clone(),
            created_at: Utc::now(),
        };
        let memory_event = event(
            &memory_event_id,
            &run_id,
            Some(&primary_loop.id),
            "memory_indexed",
            "memory_document",
            &memory.id,
            Some(&memory_causal_event_id),
            "Secondary search memory linked to a canonical record",
            json!({ "record": memory }),
        );
        self.store.index_memory(&memory, &memory_event)?;

        let mut review_causation_event_id = memory_event_id.clone();
        let mut retrieval_trace: Option<RetrievalTrace> = None;
        let mut review_evidence_ids = vec![entry_record.id.clone(), memory_entity_id.clone()];
        if let Some(context_pool) = &self.context_pool {
            let source_specs = vec![
                (
                    "Official live BTCUSD snapshot",
                    "Official simulated feed reports a current BTCUSD directional continuation snapshot.",
                    ContextSourceClass::LiveOfficial,
                    TrustLevel::Verified,
                    "official-feed://btc-usd/latest",
                    "official-simulated-feed",
                    vec!["btc".into(), "live".into(), "official".into()],
                ),
                (
                    "Recorded BTC continuation experiment",
                    "Historical recorded experiment found continuation behavior after a directional pullback.",
                    ContextSourceClass::HistoricalRecorded,
                    TrustLevel::Verified,
                    "recorded-experiment://btc-continuation-001",
                    "jev-research-archive",
                    vec!["btc".into(), "historical".into(), "experiment".into()],
                ),
                (
                    "External market commentary",
                    "External commentary claims momentum may persist, but this record is untrusted until corroborated.",
                    ContextSourceClass::ExternalWebResearch,
                    TrustLevel::Untrusted,
                    "external-research://phase3-fixture",
                    "external-fixture",
                    vec!["btc".into(), "external".into(), "untrusted".into()],
                ),
            ];
            let mut source_records = Vec::new();
            for (title, text, source_class, trust_level, provenance_uri, publisher, tags) in
                source_specs
            {
                let ingestion_event_id = Uuid::new_v4().to_string();
                let mut record = QdrantContextPool::create_record(
                    &run_id,
                    title,
                    text,
                    source_class,
                    trust_level,
                    provenance_uri,
                    publisher,
                    "context_pool_record",
                    "",
                    &ingestion_event_id,
                    tags,
                    json!({ "adapter": "phase3-simulated-source" }),
                );
                record.canonical_entity_id = record.id.clone();
                let ingestion_event = event(
                    &ingestion_event_id,
                    &run_id,
                    None,
                    "context_record_ingested",
                    "context_pool_record",
                    &record.id,
                    Some(&review_causation_event_id),
                    "Provenance-rich context record ingested",
                    json!({ "record": record }),
                );
                self.store
                    .record_context_pool_record(&record, &ingestion_event)?;
                review_causation_event_id = ingestion_event_id;
                source_records.push(record);
            }
            context_pool.upsert(&source_records)?;
            context_pool.sync_pending_events(&self.store, &run_id)?;
            let request = RetrievalRequest {
                question: format!(
                    "Does the available evidence support the thesis: {}",
                    thesis.thesis
                ),
                required_source_classes: vec![
                    "live_official".into(),
                    "historical_recorded".into(),
                    "external_web_research".into(),
                    "internal_canonical".into(),
                ],
                exact_canonical_ids: vec![memory_entity_id.clone()],
                limit: 12,
                max_rounds: 3,
                filters: RetrievalFilters {
                    run_id: Some(run_id.clone()),
                    ..RetrievalFilters::default()
                },
            };
            let trace = self.world_model.research(context_pool, &request)?;
            review_evidence_ids
                .extend(trace.hits.iter().map(|hit| hit.canonical_entity_id.clone()));
            review_evidence_ids.sort();
            review_evidence_ids.dedup();
            retrieval_trace = Some(trace);
        }

        let retrieval_summary = retrieval_trace
            .as_ref()
            .map(|trace| {
                format!(
                    "{} retrieval steps with sufficient={}",
                    trace.steps.len(),
                    trace.sufficient
                )
            })
            .unwrap_or_else(|| "canonical evidence without configured retrieval".into());
        let critique = self
            .world_model
            .critique(&thesis, &context, &retrieval_summary)?;
        let review_event_id = Uuid::new_v4().to_string();
        let review = WorldModelReviewRecord {
            id: Uuid::new_v4().to_string(),
            run_id: run_id.clone(),
            thesis_version_id: thesis.id.clone(),
            context_version_id: context.id.clone(),
            evidence_canonical_ids: review_evidence_ids,
            critique,
            outcome: "retain".into(),
            created_by_event_id: review_event_id.clone(),
            created_at: Utc::now(),
        };
        let review_event = event(
            &review_event_id,
            &run_id,
            None,
            "startup_memory_review_recorded",
            "world_model_review",
            &review.id,
            Some(&review_causation_event_id),
            "World-model review recorded with exact evidence IDs",
            json!({ "record": review, "retrievalTrace": retrieval_trace }),
        );
        self.store.record_review(&review, &review_event)?;
        if let Some(context_pool) = &self.context_pool {
            context_pool.sync_pending_events(&self.store, &run_id)?;
        }

        self.active_runs.insert(
            run_id.clone(),
            ActiveRun {
                thesis: human_thesis.to_owned(),
                hypotheses: vec![hypothesis],
                cadences: vec![cadence],
                thesis_versions: vec![thesis],
                context_definition,
                context_versions: vec![context],
                loops,
                positions,
                position_controls,
                failure_states: HashMap::new(),
                last_event_id: review_event_id,
            },
        );
        self.reconcile_broker(&run_id)?;
        self.snapshot(&run_id)
    }

    #[allow(dead_code)]
    pub fn run_cycle(&mut self, run_id: &str) -> Result<RunSnapshot> {
        let cancellation = AtomicBool::new(false);
        self.run_cycle_cancellable(run_id, &cancellation)
    }

    pub fn run_cycle_cancellable(
        &mut self,
        run_id: &str,
        cancellation: &AtomicBool,
    ) -> Result<RunSnapshot> {
        ensure_cycle_active(cancellation)?;
        self.reconcile_broker(run_id)?;
        ensure_cycle_active(cancellation)?;
        let trigger_state = replay::replay_run(&self.store, run_id)?;
        let mut seen_review_loops = std::collections::HashSet::new();
        let loss_blocked: std::collections::HashSet<String> = trigger_state
            .autonomous_review_triggers
            .iter()
            .rev()
            .filter(|trigger| seen_review_loops.insert(trigger.loop_id.clone()))
            .filter(|trigger| {
                matches!(trigger.status.as_str(), "pending" | "running" | "retrying")
                    && trigger
                        .kinds
                        .contains(&AutonomousReviewTriggerKind::LossStreak)
            })
            .map(|trigger| trigger.loop_id.clone())
            .collect();
        let loop_ids: Vec<String> = self
            .active_runs
            .get(run_id)
            .ok_or_else(|| anyhow::anyhow!("run is not active"))?
            .loops
            .iter()
            .filter(|item| {
                if item.state == "stopped" || item.state.starts_with("review-") {
                    return false;
                }
                let loop_is_flat = !self
                    .active_runs
                    .get(run_id)
                    .map(|active| {
                        active.positions.iter().any(|position| {
                            position.loop_id == item.id && position.state.starts_with("open")
                        })
                    })
                    .unwrap_or(true);
                if loop_is_flat && loss_blocked.contains(&item.id) {
                    return false;
                }
                match self
                    .active_runs
                    .get(run_id)
                    .and_then(|active| active.failure_states.get(&item.id))
                {
                    Some(failure) if failure.paused => false,
                    Some(failure) => failure
                        .next_retry_at
                        .map(|retry_at| retry_at <= Utc::now())
                        .unwrap_or(true),
                    None => true,
                }
            })
            .map(|item| item.id.clone())
            .collect();
        for loop_id in loop_ids {
            let (mut loop_state, hypothesis, thesis, context, position, causation) = {
                let active = self.active_runs.get(run_id).unwrap();
                let loop_state = active
                    .loops
                    .iter()
                    .find(|item| item.id == loop_id)
                    .unwrap()
                    .clone();
                let hypothesis = active
                    .hypotheses
                    .iter()
                    .find(|item| item.id == loop_state.hypothesis_id)
                    .cloned()
                    .context("loop references missing hypothesis")?;
                let thesis = active
                    .thesis_versions
                    .iter()
                    .find(|item| item.id == loop_state.thesis_version_id)
                    .cloned()
                    .context("loop references missing thesis")?;
                let context = active
                    .context_versions
                    .iter()
                    .find(|item| item.id == loop_state.context_version_id)
                    .cloned()
                    .context("loop references missing context")?;
                let position = active
                    .positions
                    .iter()
                    .find(|item| item.loop_id == loop_id && item.state.starts_with("open"))
                    .cloned();
                let position = if loop_state.state == "jev1-confirm-add" {
                    None
                } else {
                    position
                };
                (
                    loop_state,
                    hypothesis,
                    thesis,
                    context,
                    position,
                    active.last_event_id.clone(),
                )
            };
            if let Some(open_position) = position.as_ref() {
                let control = self
                    .active_runs
                    .get(run_id)
                    .and_then(|active| {
                        active
                            .position_controls
                            .iter()
                            .find(|item| item.position_id == open_position.id)
                    })
                    .cloned()
                    .context("open position is missing deterministic position control")?;
                if let Some(price) = self
                    .broker
                    .reference_price(&control.instrument)
                    .unwrap_or(None)
                {
                    if self
                        .risk
                        .stop_triggered(&control, &open_position.direction, price)
                    {
                        self.close_positions_for_lifecycle(run_id, Some(&loop_id))?;
                        self.materialize_trade_outcomes(run_id)?;
                        continue;
                    }
                }
            }
            let resolved = match self.resolve_live_jev_state(
                run_id,
                &loop_id,
                &hypothesis,
                &thesis,
                &context,
                position.as_ref(),
                &causation,
            ) {
                Ok(state) => state,
                Err(error) => {
                    self.record_live_context_failure(
                        run_id,
                        &loop_id,
                        &hypothesis.id,
                        &causation,
                        &error,
                    )?;
                    continue;
                }
            };
            let cycle_reference_price = resolved
                .live_context_snapshot
                .as_ref()
                .map(|snapshot| snapshot.quote.mid)
                .context("resolved live context omitted its quote envelope")?;
            let decision_event_id = Uuid::new_v4().to_string();
            let mut last_event_id = decision_event_id.clone();
            if let Some(mut position) = position {
                let management = match self.jev.manage_position(
                    &resolved,
                    "Choose HOLD, SELL, or BUY MORE for this open position. BUY MORE only requests a separate Jev1 confirmation.",
                ) {
                    Ok(value) => value,
                    Err(error) => {
                        self.record_inference_error(run_id, &loop_id, &causation, "jev2", &error)?;
                        continue;
                    }
                };
                self.record_inference_recovered(run_id, &loop_id, &causation)?;
                let record = DecisionRecord {
                    id: Uuid::new_v4().to_string(),
                    run_id: run_id.into(),
                    loop_id: loop_id.clone(),
                    stage: "jev2".into(),
                    thesis_version_id: thesis.id.clone(),
                    context_version_id: context.id.clone(),
                    position_id: Some(position.id.clone()),
                    action: format!("{:?}", management.action),
                    confidence: management.confidence,
                    rationale: management.rationale,
                    inference: management.inference,
                    resolved_state: resolved,
                    created_by_event_id: decision_event_id.clone(),
                    created_at: Utc::now(),
                };
                let decision_event = event(
                    &decision_event_id,
                    run_id,
                    Some(&loop_id),
                    "jev2_decision_recorded",
                    "decision",
                    &record.id,
                    Some(&causation),
                    "Continuous Jev2 decision recorded with TypeSafe metadata",
                    json!({"record": record}),
                );
                self.store.record_decision(&record, &decision_event)?;
                let control = self
                    .active_runs
                    .get(run_id)
                    .and_then(|active| {
                        active
                            .position_controls
                            .iter()
                            .find(|item| item.position_id == position.id)
                    })
                    .cloned()
                    .context("open position is missing deterministic position control")?;
                let broker_price = cycle_reference_price;
                let stop_triggered =
                    self.risk
                        .stop_triggered(&control, &position.direction, broker_price);
                if stop_triggered || matches!(management.action, Jev2Action::Sell) {
                    let side = if position.direction.to_ascii_lowercase().contains("short") {
                        "BUY"
                    } else {
                        "SELL"
                    };
                    let duplicate_key = idempotency_key(
                        run_id,
                        &loop_id,
                        &record.id,
                        &control.instrument,
                        side,
                        record.created_at,
                        self.risk.config.duplicate_order_window_seconds,
                    );
                    let (order, gate) = self.risk.evaluate_close(CloseRiskInput {
                        run_id,
                        loop_id: &loop_id,
                        decision_id: &record.id,
                        direction: &position.direction,
                        control: &control,
                        signal_at: record.created_at,
                        duplicate: self.store.order_exists(&duplicate_key)?,
                        stop_triggered,
                        reference_price: broker_price,
                    });
                    let order_event = event(
                        &order.created_by_event_id,
                        run_id,
                        Some(&loop_id),
                        "order_evaluated",
                        "order",
                        &order.id,
                        Some(&decision_event_id),
                        if stop_triggered {
                            "Deterministic stop loss constructed and evaluated the close order"
                        } else {
                            "Deterministic policy constructed and evaluated the Jev2 close order"
                        },
                        json!({"record":order,"result":gate}),
                    );
                    self.store.record_order(&order, &order_event)?;
                    last_event_id = order.created_by_event_id.clone();
                    if gate.accepted {
                        ensure_cycle_active(cancellation)?;
                        let execution_event_id = Uuid::new_v4().to_string();
                        let execution = self
                            .broker
                            .close(&position, &order, &record.id, &execution_event_id)
                            .unwrap_or_else(|error| {
                                broker_failure_receipt(&order, "close", &execution_event_id, &error)
                            });
                        let execution_event = event(
                            &execution_event_id,
                            run_id,
                            Some(&loop_id),
                            "execution_recorded",
                            "execution",
                            &execution.execution_id,
                            Some(&order.created_by_event_id),
                            if stop_triggered {
                                "Paper close executed from deterministic stop-loss logic"
                            } else {
                                "Paper close executed from a validated Jev2 SELL decision"
                            },
                            json!({"record": execution}),
                        );
                        self.store.record_execution(&execution, &execution_event)?;
                        if !execution_has_fill(&execution) {
                            loop_state.state = "jev2-broker-rejected".into();
                            last_event_id = execution_event_id;
                        } else if execution_fully_filled(&execution, &order) {
                            position.state = "closed".into();
                            position.closed_by_execution_id = Some(execution.execution_id.clone());
                            position.closed_at = Some(execution.executed_at);
                            position.last_event_id = Uuid::new_v4().to_string();
                            let close_event = event(
                                &position.last_event_id,
                                run_id,
                                Some(&loop_id),
                                "position_closed",
                                "position",
                                &position.id,
                                Some(&execution_event_id),
                                "Position closed and loop returned to Jev1",
                                json!({"record": position}),
                            );
                            self.store.close_position(&position, &close_event)?;
                            loop_state.state = "jev1".into();
                            last_event_id = position.last_event_id.clone();
                            if let Some(active) = self.active_runs.get_mut(run_id) {
                                if let Some(existing) = active
                                    .positions
                                    .iter_mut()
                                    .find(|item| item.id == position.id)
                                {
                                    *existing = position;
                                }
                            }
                        } else {
                            let remaining = (control.quantity - execution.filled_quantity).max(0.0);
                            let mut updated_control = control.clone();
                            updated_control.quantity = remaining;
                            updated_control.notional = remaining * updated_control.entry_price;
                            updated_control.created_by_event_id = Uuid::new_v4().to_string();
                            updated_control.created_at = Utc::now();
                            position.state = "open-partial-close".into();
                            position.last_event_id = updated_control.created_by_event_id.clone();
                            let partial_event = event(
                                &updated_control.created_by_event_id,
                                run_id,
                                Some(&loop_id),
                                "position_partially_closed",
                                "position_control",
                                &position.id,
                                Some(&execution_event_id),
                                "Partial broker close reduced the canonical remaining quantity",
                                json!({"record":position,"control":updated_control,"filledQuantity":execution.filled_quantity}),
                            );
                            self.store.record_partial_close(
                                &position,
                                &updated_control,
                                &partial_event,
                            )?;
                            if let Some(active) = self.active_runs.get_mut(run_id) {
                                if let Some(existing) = active
                                    .position_controls
                                    .iter_mut()
                                    .find(|item| item.position_id == position.id)
                                {
                                    *existing = updated_control;
                                }
                                if let Some(existing) = active
                                    .positions
                                    .iter_mut()
                                    .find(|item| item.id == position.id)
                                {
                                    *existing = position;
                                }
                            }
                            loop_state.state = "jev2".into();
                            last_event_id = partial_event.id;
                        }
                    } else {
                        loop_state.state = "jev2-hold".into();
                    }
                } else {
                    match management.action {
                        Jev2Action::Hold => loop_state.state = "jev2-hold".into(),
                        Jev2Action::BuyMore => loop_state.state = "jev1-confirm-add".into(),
                        Jev2Action::Sell => {
                            unreachable!("SELL handled by deterministic close path")
                        }
                    }
                }
            } else {
                let entry = match self.jev.decide_entry(&resolved, &hypothesis.jev_question) {
                    Ok(value) => value,
                    Err(error) => {
                        self.record_inference_error(run_id, &loop_id, &causation, "jev1", &error)?;
                        continue;
                    }
                };
                self.record_inference_recovered(run_id, &loop_id, &causation)?;
                let record = DecisionRecord {
                    id: Uuid::new_v4().to_string(),
                    run_id: run_id.into(),
                    loop_id: loop_id.clone(),
                    stage: "jev1".into(),
                    thesis_version_id: thesis.id.clone(),
                    context_version_id: context.id.clone(),
                    position_id: None,
                    action: format!("{:?}", entry.action),
                    confidence: entry.confidence,
                    rationale: entry.rationale,
                    inference: entry.inference,
                    resolved_state: resolved,
                    created_by_event_id: decision_event_id.clone(),
                    created_at: Utc::now(),
                };
                let decision_event = event(
                    &decision_event_id,
                    run_id,
                    Some(&loop_id),
                    "jev1_decision_recorded",
                    "decision",
                    &record.id,
                    Some(&causation),
                    "Continuous Jev1 decision recorded with TypeSafe metadata",
                    json!({"record": record}),
                );
                self.store.record_decision(&record, &decision_event)?;
                let execution_event_id = Uuid::new_v4().to_string();
                let (current_total_exposure, open_positions_in_loop) = {
                    let active = self.active_runs.get(run_id).unwrap();
                    let exposure = active
                        .position_controls
                        .iter()
                        .filter(|control| {
                            active.positions.iter().any(|position| {
                                position.id == control.position_id
                                    && position.state.starts_with("open")
                            })
                        })
                        .map(|control| control.notional)
                        .sum();
                    let count = active
                        .positions
                        .iter()
                        .filter(|position| {
                            position.loop_id == loop_id && position.state.starts_with("open")
                        })
                        .count();
                    (exposure, count)
                };
                let side = match entry.action {
                    Jev1Action::Long => "BUY",
                    Jev1Action::Short => "SELL",
                    Jev1Action::NoTrade => "NO_TRADE",
                };
                let instrument = hypothesis
                    .instruments
                    .first()
                    .map(String::as_str)
                    .unwrap_or("UNSPECIFIED");
                let duplicate_key = idempotency_key(
                    run_id,
                    &loop_id,
                    &record.id,
                    instrument,
                    side,
                    record.created_at,
                    self.risk.config.duplicate_order_window_seconds,
                );
                let reference_price = cycle_reference_price;
                let (order, gate) = self.risk.evaluate_entry(EntryRiskInput {
                    run_id,
                    loop_id: &loop_id,
                    decision_id: &record.id,
                    action: entry.action.clone(),
                    confidence: entry.confidence,
                    signal_at: record.created_at,
                    instrument,
                    reference_price,
                    allocated_fraction: loop_state.allocated_fraction,
                    current_total_exposure,
                    open_positions_in_loop,
                    duplicate: self.store.order_exists(&duplicate_key)?,
                    minimum_confidence: self.minimum_confidence,
                });
                let order_event = event(
                    &order.created_by_event_id,
                    run_id,
                    Some(&loop_id),
                    "order_evaluated",
                    "order",
                    &order.id,
                    Some(&decision_event_id),
                    "Deterministic policy constructed and evaluated the continuous order",
                    json!({"record":order,"result":gate}),
                );
                self.store.record_order(&order, &order_event)?;
                let request = TradeRequest {
                    run_id: run_id.into(),
                    loop_id: loop_id.clone(),
                    decision_id: record.id.clone(),
                    execution_event_id: execution_event_id.clone(),
                    action: entry.action.clone(),
                    confidence: entry.confidence,
                    order: order.clone(),
                };
                let gate_event_id = Uuid::new_v4().to_string();
                let gate_event = event(
                    &gate_event_id,
                    run_id,
                    Some(&loop_id),
                    "guardrail_evaluated",
                    "decision",
                    &record.id,
                    Some(&order.created_by_event_id),
                    "Deterministic policy evaluated the continuous Jev1 decision",
                    json!({"decisionId":record.id,"result":gate}),
                );
                self.store.append_event(&gate_event)?;
                last_event_id = gate_event_id;
                if gate.accepted && !matches!(entry.action, Jev1Action::NoTrade) {
                    ensure_cycle_active(cancellation)?;
                    let execution = self.broker.execute(&request).unwrap_or_else(|error| {
                        broker_failure_receipt(&order, "open", &execution_event_id, &error)
                    });
                    let execution_event = event(
                        &execution_event_id,
                        run_id,
                        Some(&loop_id),
                        "execution_recorded",
                        "execution",
                        &execution.execution_id,
                        Some(&last_event_id),
                        "Paper execution recorded after continuous Jev1 confirmation",
                        json!({"record":execution}),
                    );
                    self.store.record_execution(&execution, &execution_event)?;
                    if !execution_has_fill(&execution) {
                        loop_state.state = "jev1-broker-rejected".into();
                        last_event_id = execution_event_id;
                    } else {
                        let position_event_id = Uuid::new_v4().to_string();
                        let position = PositionRecord {
                            id: Uuid::new_v4().to_string(),
                            run_id: run_id.into(),
                            loop_id: loop_id.clone(),
                            opened_by_execution_id: execution.execution_id.clone(),
                            broker_position_id: execution.broker_position_id.clone(),
                            direction: format!("{:?}", execution.action),
                            state: if execution_fully_filled(&execution, &order) {
                                "open"
                            } else {
                                "open-partial"
                            }
                            .into(),
                            opened_at: execution.executed_at,
                            closed_by_execution_id: None,
                            closed_at: None,
                            last_event_id: position_event_id.clone(),
                        };
                        let position_event = event(
                            &position_event_id,
                            run_id,
                            Some(&loop_id),
                            "position_opened",
                            "position",
                            &position.id,
                            Some(&execution_event_id),
                            if execution_fully_filled(&execution, &order) {
                                "Position opened after continuous Jev1 confirmation"
                            } else {
                                "Partially filled position opened from confirmed cumulative broker fill"
                            },
                            json!({"record":position}),
                        );
                        self.store.open_position(&position, &position_event)?;
                        let control = PositionControlRecord {
                            position_id: position.id.clone(),
                            order_id: order.id.clone(),
                            instrument: order.instrument.clone(),
                            quantity: execution.filled_quantity,
                            entry_price: execution.average_price.unwrap_or(order.reference_price),
                            notional: execution.filled_quantity
                                * execution.average_price.unwrap_or(order.reference_price),
                            stop_loss_price: order.stop_loss_price.context(
                                "approved entry order must contain a deterministic stop loss",
                            )?,
                            created_by_event_id: Uuid::new_v4().to_string(),
                            created_at: Utc::now(),
                        };
                        let control_event = event(
                            &control.created_by_event_id,
                            run_id,
                            Some(&loop_id),
                            "position_control_created",
                            "position_control",
                            &position.id,
                            Some(&position_event_id),
                            "Deterministic sizing and stop control attached to the position",
                            json!({"record":control}),
                        );
                        self.store
                            .record_position_control(&control, &control_event)?;
                        let active = self.active_runs.get_mut(run_id).unwrap();
                        active.positions.push(position);
                        active.position_controls.push(control);
                        loop_state.state = "jev2".into();
                        last_event_id = control_event.id;
                    }
                } else {
                    loop_state.state = "jev1-no-trade".into();
                }
            }
            let transition_event_id = Uuid::new_v4().to_string();
            let transition_event = event(
                &transition_event_id,
                run_id,
                Some(&loop_id),
                "loop_state_transitioned",
                "loop",
                &loop_id,
                Some(&last_event_id),
                "Harness applied the typed Jev state transition",
                json!({"record":loop_state}),
            );
            self.store.transition_loop(&loop_state, &transition_event)?;
            let active = self.active_runs.get_mut(run_id).unwrap();
            if let Some(existing) = active.loops.iter_mut().find(|item| item.id == loop_id) {
                *existing = loop_state;
            }
            active.last_event_id = transition_event_id;
        }
        self.materialize_trade_outcomes(run_id)?;
        self.evaluate_autonomous_review_triggers(run_id)?;
        if let Some(context_pool) = &self.context_pool {
            context_pool.sync_pending_events(&self.store, run_id)?;
        }
        self.snapshot(run_id)
    }

    fn materialize_trade_outcomes(&mut self, run_id: &str) -> Result<()> {
        let replayed = replay::replay_run(&self.store, run_id)?;
        let recorded: std::collections::HashSet<&str> = replayed
            .trade_outcomes
            .iter()
            .map(|outcome| outcome.position_id.as_str())
            .collect();
        for position in replayed.positions.iter().filter(|position| {
            position.state.starts_with("closed") && !recorded.contains(position.id.as_str())
        }) {
            let loop_state = replayed
                .loops
                .iter()
                .find(|item| item.id == position.loop_id)
                .context("closed position references missing loop")?;
            let entry = replayed
                .executions
                .iter()
                .find(|execution| execution.execution_id == position.opened_by_execution_id);
            let close_decisions: std::collections::HashSet<&str> = replayed
                .decisions
                .iter()
                .filter(|decision| decision.position_id.as_deref() == Some(position.id.as_str()))
                .map(|decision| decision.id.as_str())
                .collect();
            let exits: Vec<&ExecutionReceipt> = replayed
                .executions
                .iter()
                .filter(|execution| {
                    execution.execution_kind == "close"
                        && close_decisions.contains(execution.caused_by_decision_id.as_str())
                        && execution_has_fill(execution)
                })
                .collect();
            let imported = entry
                .map(|execution| execution.execution_kind == "reconciliation-import")
                .unwrap_or(true);
            let quantity = entry
                .map(|execution| execution.filled_quantity)
                .unwrap_or(0.0);
            let entry_notional = entry.and_then(|execution| {
                execution
                    .average_price
                    .map(|price| price * execution.filled_quantity)
            });
            let exit_notional = (!exits.is_empty()
                && exits
                    .iter()
                    .all(|execution| execution.average_price.is_some()))
            .then(|| {
                exits
                    .iter()
                    .map(|execution| {
                        execution.average_price.unwrap_or_default() * execution.filled_quantity
                    })
                    .sum::<f64>()
            });
            let gross_realized_pnl = if imported {
                None
            } else {
                entry_notional
                    .zip(exit_notional)
                    .map(|(entry_value, exit_value)| {
                        if position.direction.to_ascii_lowercase().contains("short") {
                            entry_value - exit_value
                        } else {
                            exit_value - entry_value
                        }
                    })
            };
            let gross_return = gross_realized_pnl
                .zip(entry_notional)
                .and_then(|(pnl, basis)| (basis.abs() > f64::EPSILON).then_some(pnl / basis));
            let classification = classify_trade_outcome(gross_realized_pnl);
            let event_id = Uuid::new_v4().to_string();
            let outcome = TradeOutcomeRecord {
                id: Uuid::new_v4().to_string(),
                run_id: run_id.into(),
                loop_id: position.loop_id.clone(),
                position_id: position.id.clone(),
                thesis_version_id: loop_state.thesis_version_id.clone(),
                context_version_id: loop_state.context_version_id.clone(),
                instrument: replayed
                    .position_controls
                    .iter()
                    .find(|control| control.position_id == position.id)
                    .map(|control| control.instrument.clone())
                    .unwrap_or_else(|| "unknown".into()),
                direction: position.direction.clone(),
                quantity,
                entry_notional,
                exit_notional,
                gross_realized_pnl,
                gross_return,
                fees_available: false,
                classification,
                entry_execution_ids: entry
                    .map(|execution| vec![execution.execution_id.clone()])
                    .unwrap_or_default(),
                exit_execution_ids: exits
                    .iter()
                    .map(|execution| execution.execution_id.clone())
                    .collect(),
                created_by_event_id: event_id.clone(),
                completed_at: position.closed_at.unwrap_or_else(Utc::now),
            };
            let outcome_event = event(
                &event_id,
                run_id,
                Some(&position.loop_id),
                "trade_outcome_recorded",
                "trade_outcome",
                &outcome.id,
                Some(&position.last_event_id),
                "Broker-confirmed round trip materialized as an immutable trade outcome",
                json!({"record": outcome}),
            );
            self.store.record_trade_outcome(&outcome, &outcome_event)?;
            if let Some(active) = self.active_runs.get_mut(run_id) {
                active.last_event_id = event_id;
            }
        }
        Ok(())
    }

    fn evaluate_autonomous_review_triggers(&mut self, run_id: &str) -> Result<()> {
        if !self.review_policy.enabled {
            return Ok(());
        }
        let replayed = replay::replay_run(&self.store, run_id)?;
        let active_loops = replayed
            .loops
            .iter()
            .filter(|loop_state| loop_state.state != "stopped")
            .cloned()
            .collect::<Vec<_>>();
        for loop_state in active_loops {
            let latest_trigger = replayed
                .autonomous_review_triggers
                .iter()
                .filter(|trigger| trigger.loop_id == loop_state.id)
                .last();
            if latest_trigger
                .and_then(|trigger| trigger.next_retry_at)
                .map(|retry_at| retry_at > Utc::now())
                .unwrap_or(false)
            {
                continue;
            }
            if let Some(trigger) = latest_trigger.filter(|trigger| {
                matches!(trigger.status.as_str(), "pending" | "running" | "retrying")
            }) {
                if !self
                    .pending_review_jobs
                    .iter()
                    .any(|job| job.trigger.trigger_id == trigger.trigger_id)
                {
                    let request = HypothesisReviewRequest {
                        run_id: run_id.into(),
                        hypothesis_id: trigger.hypothesis_id.clone(),
                        evidence_query: review_protocol(&trigger.kinds),
                    };
                    let package = if let Some(existing) = replayed
                        .review_packages
                        .iter()
                        .rev()
                        .find(|package| {
                            package
                                .autonomous_trigger
                                .as_ref()
                                .map(|item| item.trigger_id.as_str())
                                == Some(trigger.trigger_id.as_str())
                        })
                        .cloned()
                    {
                        existing
                    } else {
                        let mut package = self.assemble_review_package(&request)?;
                        package.autonomous_trigger = Some(trigger.clone());
                        package.recent_trade_outcomes = replayed
                            .trade_outcomes
                            .iter()
                            .filter(|outcome| outcome.loop_id == loop_state.id)
                            .rev()
                            .take(20)
                            .cloned()
                            .collect();
                        self.record_autonomous_review_package(&mut package, trigger)?;
                        package
                    };
                    self.pending_review_jobs.push(AutonomousReviewJob {
                        trigger: trigger.clone(),
                        request,
                        package,
                    });
                }
                continue;
            }
            let checkpoint = replayed
                .autonomous_review_triggers
                .iter()
                .rev()
                .find(|trigger| trigger.loop_id == loop_state.id && trigger.status == "completed")
                .map(|trigger| trigger.created_at);
            let decisions = replayed
                .decisions
                .iter()
                .filter(|decision| {
                    decision.loop_id == loop_state.id
                        && decision.stage == "jev1"
                        && checkpoint
                            .map(|at| decision.created_at > at)
                            .unwrap_or(true)
                })
                .collect::<Vec<_>>();
            let mut no_trade_streak = 0usize;
            for decision in decisions.iter().rev() {
                if decision.action.eq_ignore_ascii_case("NoTrade") {
                    no_trade_streak += 1;
                    continue;
                }
                let confidence_only = replayed
                    .orders
                    .iter()
                    .find(|order| order.decision_id == decision.id)
                    .map(|order| {
                        order.status == "rejected"
                            && order.rejection_reasons.len() == 1
                            && order.rejection_reasons[0].contains("confidence")
                    })
                    .unwrap_or(false);
                if confidence_only {
                    no_trade_streak += 1;
                    continue;
                }
                if decision.confidence >= self.minimum_confidence {
                    break;
                }
            }
            let outcomes = replayed
                .trade_outcomes
                .iter()
                .filter(|outcome| {
                    outcome.loop_id == loop_state.id
                        && !matches!(outcome.classification, TradeOutcomeClassification::Unknown)
                        && checkpoint
                            .map(|at| outcome.completed_at > at)
                            .unwrap_or(true)
                })
                .collect::<Vec<_>>();
            let consecutive_losses = outcomes
                .iter()
                .rev()
                .take_while(|outcome| {
                    matches!(outcome.classification, TradeOutcomeClassification::Loss)
                })
                .count();
            let cadence = replayed
                .cadences
                .iter()
                .find(|cadence| cadence.loop_id == loop_state.id)
                .context("active loop is missing its deterministic cadence")?;
            let no_trade_threshold = no_trade_review_threshold(cadence, &self.review_policy);
            let mut kinds = Vec::new();
            if consecutive_losses >= self.review_policy.consecutive_loss_threshold {
                kinds.push(AutonomousReviewTriggerKind::LossStreak);
            }
            if no_trade_streak >= no_trade_threshold {
                kinds.push(AutonomousReviewTriggerKind::NoTradeStreak);
            }
            if outcomes.len() >= self.review_policy.periodic_trade_threshold {
                kinds.push(AutonomousReviewTriggerKind::PeriodicTradeCount);
            }
            if kinds.is_empty() {
                continue;
            }
            let hypothesis = replayed
                .hypotheses
                .iter()
                .find(|hypothesis| hypothesis.id == loop_state.hypothesis_id)
                .context("review trigger loop references missing hypothesis")?;
            let event_id = Uuid::new_v4().to_string();
            let trigger = AutonomousReviewTriggerRecord {
                trigger_id: Uuid::new_v4().to_string(),
                run_id: run_id.into(),
                loop_id: loop_state.id.clone(),
                hypothesis_id: hypothesis.id.clone(),
                kinds: kinds.clone(),
                status: "pending".into(),
                attempts: 0,
                no_trade_streak,
                no_trade_threshold,
                consecutive_losses,
                loss_threshold: self.review_policy.consecutive_loss_threshold,
                completed_trades_since_review: outcomes.len(),
                periodic_trade_threshold: self.review_policy.periodic_trade_threshold,
                last_decision_id: decisions.last().map(|decision| decision.id.clone()),
                last_trade_outcome_id: outcomes.last().map(|outcome| outcome.id.clone()),
                review_id: None,
                next_retry_at: None,
                created_by_event_id: event_id.clone(),
                created_at: Utc::now(),
            };
            let trigger_event = event(
                &event_id,
                run_id,
                Some(&loop_state.id),
                "autonomous_review_triggered",
                "autonomous_review_trigger",
                &trigger.trigger_id,
                Some(&self.active_runs.get(run_id).unwrap().last_event_id),
                "Deterministic counters triggered one autonomous world-model review",
                json!({"record":trigger}),
            );
            self.store
                .record_autonomous_review_trigger(&trigger, &trigger_event)?;
            let protocol = review_protocol(&kinds);
            let request = HypothesisReviewRequest {
                run_id: run_id.into(),
                hypothesis_id: hypothesis.id.clone(),
                evidence_query: protocol,
            };
            let mut package = self.assemble_review_package(&request)?;
            package.autonomous_trigger = Some(trigger.clone());
            package.recent_trade_outcomes = replayed
                .trade_outcomes
                .iter()
                .filter(|outcome| outcome.loop_id == loop_state.id)
                .rev()
                .take(20)
                .cloned()
                .collect();
            self.record_autonomous_review_package(&mut package, &trigger)?;
            self.pending_review_jobs.push(AutonomousReviewJob {
                trigger,
                request,
                package,
            });
        }
        Ok(())
    }

    fn record_inference_error(
        &mut self,
        run_id: &str,
        loop_id: &str,
        causation: &str,
        stage: &str,
        error: &anyhow::Error,
    ) -> Result<()> {
        let event_id = Uuid::new_v4().to_string();
        let error_event = event(
            &event_id,
            run_id,
            Some(loop_id),
            "jev_inference_failed",
            "inference_error",
            &event_id,
            Some(causation),
            "Jev API failure recorded separately from trading decisions",
            json!({"stage":stage,"error":error.to_string()}),
        );
        self.store.append_event(&error_event)?;
        let previous_failures = self
            .active_runs
            .get(run_id)
            .and_then(|active| active.failure_states.get(loop_id))
            .map(|state| state.consecutive_failures)
            .unwrap_or(0);
        let failure_event_id = Uuid::new_v4().to_string();
        let failure = self.risk.retry_state(
            loop_id,
            previous_failures,
            error.to_string(),
            failure_event_id.clone(),
        );
        let failure_event = event(
            &failure_event_id,
            run_id,
            Some(loop_id),
            "loop_failure_state_changed",
            "loop_failure_state",
            loop_id,
            Some(&event_id),
            "Harness applied deterministic retry backoff after inference failure",
            json!({"record":failure}),
        );
        self.store.record_failure_state(&failure, &failure_event)?;
        let active = self.active_runs.get_mut(run_id).unwrap();
        active
            .failure_states
            .insert(loop_id.into(), failure.clone());
        if failure.paused {
            if let Some(loop_state) = active.loops.iter_mut().find(|item| item.id == loop_id) {
                loop_state.state = "paused-failure".into();
            }
        }
        active.last_event_id = failure_event_id;
        Ok(())
    }

    fn record_inference_recovered(
        &mut self,
        run_id: &str,
        loop_id: &str,
        causation: &str,
    ) -> Result<()> {
        if !self
            .active_runs
            .get(run_id)
            .map(|active| active.failure_states.contains_key(loop_id))
            .unwrap_or(false)
        {
            return Ok(());
        }
        let event_id = Uuid::new_v4().to_string();
        let recovered = LoopFailureState {
            loop_id: loop_id.into(),
            consecutive_failures: 0,
            next_retry_at: None,
            paused: false,
            last_error: None,
            updated_by_event_id: event_id.clone(),
            updated_at: Utc::now(),
        };
        let recovered_event = event(
            &event_id,
            run_id,
            Some(loop_id),
            "loop_failure_state_changed",
            "loop_failure_state",
            loop_id,
            Some(causation),
            "Successful inference cleared deterministic failure backoff",
            json!({"record":recovered}),
        );
        self.store
            .record_failure_state(&recovered, &recovered_event)?;
        let active = self.active_runs.get_mut(run_id).unwrap();
        active.failure_states.remove(loop_id);
        active.last_event_id = event_id;
        Ok(())
    }

    fn resolve_jev_state(
        &self,
        hypothesis: &HypothesisDefinition,
        thesis: &ThesisVersion,
        context: &ContextVersion,
        position: Option<&PositionRecord>,
    ) -> Result<ResolvedJevState> {
        let mut state = self
            .context_resolver
            .resolve(hypothesis, thesis, context, position)?;
        if let Some(context_pool) = &self.context_pool {
            let trace = context_pool.retrieve(&RetrievalRequest {
                question: format!(
                    "{} Required fields: {}",
                    hypothesis.jev_question,
                    hypothesis.deterministic_context.join(", ")
                ),
                required_source_classes: Vec::new(),
                exact_canonical_ids: Vec::new(),
                limit: 12,
                max_rounds: 2,
                filters: RetrievalFilters::default(),
            })?;
            for hit in trace.hits {
                if hit.source_class == "internal_canonical"
                    && hit.tags.iter().any(|tag| {
                        matches!(
                            tag.as_str(),
                            "capital_allocation_changed" | "guardrail_evaluated"
                        )
                    })
                {
                    continue;
                }
                let observed_at = chrono::DateTime::parse_from_rfc3339(&hit.observed_at)
                    .map(|value| value.with_timezone(&Utc))
                    .unwrap_or_else(|_| Utc::now());
                state.context.push(ResolvedContextField {
                    name: hit.title,
                    value: json!(hit.text),
                    source_id: hit.canonical_entity_id,
                    source_uri: hit.provenance_uri,
                    observed_at,
                });
            }
        }
        crate::context::compact_for_jev(&mut state);
        state.resolved_at = Utc::now();
        Ok(state)
    }

    fn resolve_live_jev_state(
        &mut self,
        run_id: &str,
        loop_id: &str,
        hypothesis: &HypothesisDefinition,
        thesis: &ThesisVersion,
        context: &ContextVersion,
        position: Option<&PositionRecord>,
        causation: &str,
    ) -> Result<ResolvedJevState> {
        let spec = hypothesis.live_context_spec.as_ref().context(
            "live context specification is missing; loop requires a context-spec upgrade",
        )?;
        if spec.fields.is_empty() {
            bail!("live context specification has no formula fields");
        }
        let requested_series = crate::market_data::requirements(spec)?;
        let quote_max_age_seconds = spec
            .fields
            .iter()
            .filter(|field| {
                matches!(
                    field.expression,
                    LiveExpression::CurrentBid
                        | LiveExpression::CurrentAsk
                        | LiveExpression::CurrentMid
                        | LiveExpression::CurrentSpread
                )
            })
            .map(|field| field.maximum_age_seconds)
            .min()
            .unwrap_or(15);
        let market = self.market_data.snapshot(&MarketDataRequest {
            instrument: spec.instrument.clone(),
            series: requested_series,
            quote_max_age_seconds,
        })?;
        let snapshot = crate::market_data::resolve_snapshot(
            run_id,
            loop_id,
            &thesis.id,
            &context.id,
            spec,
            market,
        )?;
        let snapshot_event = event(
            &snapshot.created_by_event_id,
            run_id,
            Some(loop_id),
            "live_context_resolved",
            "resolved_context_snapshot",
            &snapshot.id,
            Some(causation),
            "Fresh market observations resolved the versioned live-context formulas",
            json!({"record": snapshot}),
        );
        self.store
            .record_resolved_context_snapshot(&snapshot, &snapshot_event)?;
        let mut state = self.resolve_jev_state(hypothesis, thesis, context, position)?;
        for field in &snapshot.fields {
            state.context.push(ResolvedContextField {
                name: field.label.clone(),
                value: field.value.clone(),
                source_id: field.field_id.clone(),
                source_uri: format!("canonical://resolved-context/{}", snapshot.id),
                observed_at: field.observed_at,
            });
        }
        state.market_state["liveQuote"] = json!({
            "bid": snapshot.quote.bid,
            "ask": snapshot.quote.ask,
            "mid": snapshot.quote.mid,
            "spread": snapshot.quote.spread,
            "sourceTimestamp": snapshot.quote.source_timestamp,
            "receivedAt": snapshot.quote.received_at,
            "observationId": snapshot.quote.id,
            "provenance": snapshot.quote.provenance,
        });
        state.market_state["resolvedContextSnapshotId"] = json!(snapshot.id);
        state.live_context_snapshot = Some(snapshot);
        crate::context::compact_for_jev(&mut state);
        state.resolved_at = Utc::now();
        Ok(state)
    }

    fn record_live_context_failure(
        &mut self,
        run_id: &str,
        loop_id: &str,
        hypothesis_id: &str,
        causation: &str,
        error: &anyhow::Error,
    ) -> Result<()> {
        let event_id = Uuid::new_v4().to_string();
        let failure_event = event(
            &event_id,
            run_id,
            Some(loop_id),
            "live_context_resolution_failed",
            "hypothesis",
            hypothesis_id,
            Some(causation),
            "Jev inference was skipped because required live context did not resolve",
            json!({"error":error.to_string(),"jevCalled":false,"syntheticNoTrade":false}),
        );
        self.store.append_event(&failure_event)?;
        if let Some(active) = self.active_runs.get_mut(run_id) {
            active.last_event_id = event_id;
            if hypothesis_id.is_empty() {
                return Ok(());
            }
            if error.to_string().contains("context-spec upgrade") {
                if let Some(loop_state) = active.loops.iter_mut().find(|item| item.id == loop_id) {
                    loop_state.state = "context-upgrade-required".into();
                }
            }
        }
        Ok(())
    }

    pub fn assemble_review_package(
        &mut self,
        request: &HypothesisReviewRequest,
    ) -> Result<WorldModelReviewPackage> {
        if let Some(context_pool) = &self.context_pool {
            context_pool.sync_pending_events(&self.store, &request.run_id)?;
        }
        let (hypothesis, thesis, context, loop_state, causation) = {
            let active = self
                .active_runs
                .get(&request.run_id)
                .context("run is not active")?;
            let hypothesis = active
                .hypotheses
                .iter()
                .find(|item| item.id == request.hypothesis_id)
                .cloned()
                .context("hypothesis is not active in this run")?;
            let thesis = active
                .thesis_versions
                .iter()
                .find(|item| item.id == hypothesis.thesis_version_id)
                .cloned()
                .context("hypothesis references missing thesis")?;
            let context = active
                .context_versions
                .iter()
                .find(|item| item.id == hypothesis.context_version_id)
                .cloned()
                .context("hypothesis references missing context")?;
            let loop_state = active
                .loops
                .iter()
                .find(|item| item.hypothesis_id == hypothesis.id)
                .cloned()
                .context("hypothesis has no harness-managed loop")?;
            (
                hypothesis,
                thesis,
                context,
                loop_state,
                active.last_event_id.clone(),
            )
        };
        let replayed = replay::replay_run(&self.store, &request.run_id)?;
        let mut recent_jev_decisions = replayed
            .decisions
            .iter()
            .filter(|decision| decision.loop_id == loop_state.id)
            .rev()
            .take(20)
            .cloned()
            .collect::<Vec<_>>();
        recent_jev_decisions.reverse();
        let decision_ids = recent_jev_decisions
            .iter()
            .map(|decision| decision.id.as_str())
            .collect::<std::collections::HashSet<_>>();
        let recent_executions = replayed
            .executions
            .iter()
            .filter(|execution| decision_ids.contains(execution.caused_by_decision_id.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        let current_positions = replayed
            .positions
            .iter()
            .filter(|position| position.loop_id == loop_state.id)
            .cloned()
            .collect::<Vec<_>>();
        let prior_reviews = replayed
            .hypothesis_reviews
            .iter()
            .filter(|review| review.hypothesis_id == hypothesis.id)
            .cloned()
            .collect::<Vec<_>>();
        let prior_spawned_hypotheses = replayed
            .hypotheses
            .iter()
            .filter(|candidate| {
                candidate.id != hypothesis.id
                    && (candidate.root_hypothesis_id == hypothesis.root_hypothesis_id
                        || candidate.parent_hypothesis_id.as_deref()
                            == Some(hypothesis.id.as_str()))
            })
            .cloned()
            .collect::<Vec<_>>();
        let mut exact_canonical_ids = vec![
            hypothesis.id.clone(),
            thesis.id.clone(),
            context.id.clone(),
            loop_state.id.clone(),
        ];
        exact_canonical_ids.extend(recent_jev_decisions.iter().map(|item| item.id.clone()));
        exact_canonical_ids.extend(
            recent_executions
                .iter()
                .map(|item| item.execution_id.clone()),
        );
        exact_canonical_ids.sort();
        exact_canonical_ids.dedup();
        let historical_retrieval = self
            .context_pool
            .as_ref()
            .map(|pool| {
                pool.retrieve(&RetrievalRequest {
                    question: format!(
                        "{} Compare historical successful failed and contradictory experiments for instruments {} mechanism {} timeframe {} minutes. Include the same thesis under different context and different theses under similar context.",
                        request.evidence_query,
                        hypothesis.instruments.join(", "),
                        hypothesis.strategy_mechanism,
                        hypothesis.timeframe.horizon_minutes
                    ),
                    required_source_classes: vec!["internal_canonical".into()],
                    exact_canonical_ids: exact_canonical_ids.clone(),
                    limit: 30,
                    max_rounds: 3,
                    filters: RetrievalFilters::default(),
                })
            })
            .transpose()?;
        let evidence = historical_retrieval
            .as_ref()
            .map(|trace| {
                trace
                    .hits
                    .iter()
                    .cloned()
                    .map(|hit| ReviewEvidenceItem {
                        relationship: evidence_relationship(&hit),
                        hit,
                    })
                    .collect()
            })
            .unwrap_or_default();
        let event_id = Uuid::new_v4().to_string();
        let package = WorldModelReviewPackage {
            id: Uuid::new_v4().to_string(),
            run_id: request.run_id.clone(),
            evidence_query: request.evidence_query.clone(),
            current_hypothesis: hypothesis,
            current_thesis: thesis,
            current_context: context,
            current_loop: loop_state.clone(),
            recent_jev_decisions,
            recent_executions,
            current_positions,
            prior_reviews,
            prior_spawned_hypotheses,
            historical_retrieval,
            evidence,
            exact_canonical_ids,
            autonomous_trigger: None,
            recent_trade_outcomes: replayed
                .trade_outcomes
                .iter()
                .filter(|outcome| outcome.loop_id == loop_state.id)
                .rev()
                .take(20)
                .cloned()
                .collect(),
            created_by_event_id: event_id.clone(),
            assembled_at: Utc::now(),
        };
        let package_event = event(
            &event_id,
            &request.run_id,
            Some(&loop_state.id),
            "world_model_review_package_assembled",
            "world_model_review_package",
            &package.id,
            Some(&causation),
            "Provider-independent review package assembled from current state and historical memory",
            json!({"record":package}),
        );
        self.store.append_event(&package_event)?;
        self.active_runs
            .get_mut(&request.run_id)
            .unwrap()
            .last_event_id = event_id;
        if let Some(context_pool) = &self.context_pool {
            context_pool.sync_pending_events(&self.store, &request.run_id)?;
        }
        Ok(package)
    }

    pub fn review_hypothesis(&mut self, request: &HypothesisReviewRequest) -> Result<RunSnapshot> {
        let package = self.assemble_review_package(request)?;
        let decision = self.world_model.review_hypothesis(&package)?;
        self.apply_review_decision(request, &package, decision)
    }

    pub fn complete_autonomous_review(
        &mut self,
        job: &AutonomousReviewJob,
        result: Result<HypothesisReviewDecision>,
    ) -> Result<RunSnapshot> {
        let active = self
            .active_runs
            .get(&job.trigger.run_id)
            .context("run stopped while review was running")?;
        let current = active
            .loops
            .iter()
            .find(|loop_state| {
                loop_state.id == job.trigger.loop_id && loop_state.state != "stopped"
            })
            .context("review loop was stopped or superseded")?;
        if current.hypothesis_id != job.trigger.hypothesis_id {
            bail!("review result is stale because the loop hypothesis changed");
        }
        let decision = match result {
            Ok(decision) => decision,
            Err(error) => {
                let mut failed = job.trigger.clone();
                failed.status = "failed".into();
                failed.attempts = self.review_policy.retry_attempts;
                failed.next_retry_at = Some(
                    Utc::now()
                        + chrono::Duration::seconds(self.review_policy.retry_base_seconds as i64),
                );
                failed.created_by_event_id = Uuid::new_v4().to_string();
                failed.created_at = Utc::now();
                let failed_event = event(
                    &failed.created_by_event_id,
                    &failed.run_id,
                    Some(&failed.loop_id),
                    "autonomous_review_failed",
                    "autonomous_review_trigger",
                    &failed.trigger_id,
                    Some(&active.last_event_id),
                    "Autonomous review provider retries were exhausted; no action was invented",
                    json!({"record":failed,"error":error.to_string()}),
                );
                self.store
                    .record_autonomous_review_trigger(&failed, &failed_event)?;
                return self.snapshot(&job.trigger.run_id);
            }
        };
        if let Err(error) = self.validate_autonomous_review_action(job, &decision) {
            let review_event = event(
                &decision.created_by_event_id,
                &job.trigger.run_id,
                Some(&job.trigger.loop_id),
                "hypothesis_reviewed",
                "hypothesis_review",
                &decision.id,
                Some(&job.package.created_by_event_id),
                "World-model review was recorded before deterministic lifecycle validation rejected its action",
                json!({"record":decision,"reviewPackageId":job.package.id,"actionAccepted":false}),
            );
            self.store
                .record_hypothesis_review(&decision, &review_event)?;
            let mut rejected = job.trigger.clone();
            rejected.status = "completed".into();
            rejected.attempts = 1;
            rejected.review_id = Some(decision.id.clone());
            rejected.created_by_event_id = Uuid::new_v4().to_string();
            rejected.created_at = Utc::now();
            let rejected_event = event(
                &rejected.created_by_event_id,
                &rejected.run_id,
                Some(&rejected.loop_id),
                "autonomous_review_action_rejected",
                "autonomous_review_trigger",
                &rejected.trigger_id,
                Some(&decision.created_by_event_id),
                "World-model lifecycle action was rejected by deterministic review policy",
                json!({"record":rejected,"decision":decision,"reason":error.to_string()}),
            );
            self.store
                .record_autonomous_review_trigger(&rejected, &rejected_event)?;
            if let Some(active) = self.active_runs.get_mut(&rejected.run_id) {
                active.last_event_id = rejected.created_by_event_id;
            }
            return self.snapshot(&job.trigger.run_id);
        }
        if let Err(error) = self.apply_review_decision(&job.request, &job.package, decision.clone())
        {
            let message = error.to_string();
            self.record_review_job_state(job, "retrying", 1, Some(&message))?;
            return self.snapshot(&job.trigger.run_id);
        }
        let mut completed = job.trigger.clone();
        completed.status = "completed".into();
        completed.attempts = 1;
        completed.review_id = Some(decision.id);
        completed.created_by_event_id = Uuid::new_v4().to_string();
        completed.created_at = Utc::now();
        let completed_event = event(
            &completed.created_by_event_id,
            &completed.run_id,
            Some(&completed.loop_id),
            "autonomous_review_completed",
            "autonomous_review_trigger",
            &completed.trigger_id,
            Some(&job.package.created_by_event_id),
            "Autonomous review checkpointed every decision and trade it examined",
            json!({"record":completed}),
        );
        self.store
            .record_autonomous_review_trigger(&completed, &completed_event)?;
        if let Some(active) = self.active_runs.get_mut(&completed.run_id) {
            active.last_event_id = completed.created_by_event_id;
        }
        self.snapshot(&job.trigger.run_id)
    }

    fn validate_autonomous_review_action(
        &self,
        job: &AutonomousReviewJob,
        decision: &HypothesisReviewDecision,
    ) -> Result<()> {
        if matches!(decision.action, HypothesisAction::Modify)
            && (decision
                .proposed_mechanism
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_none()
                || decision.proposed_timeframe.is_none())
        {
            bail!("MODIFY rejected: a complete replacement mechanism and timeframe are required");
        }
        if !matches!(decision.action, HypothesisAction::Split) {
            return Ok(());
        }
        let routing = decision
            .routing
            .as_ref()
            .context("SPLIT requires routing metadata")?;
        if !routing.escalated {
            bail!("SPLIT rejected: escalation is mandatory");
        }
        if decision.decision_confidence < self.review_policy.split_confidence_threshold {
            bail!("SPLIT rejected: confidence is below the configured threshold");
        }
        let active_count = self
            .active_runs
            .get(&job.trigger.run_id)
            .map(|run| {
                run.loops
                    .iter()
                    .filter(|item| item.state != "stopped")
                    .count()
            })
            .unwrap_or_default();
        if active_count >= self.review_policy.max_active_loops {
            bail!("SPLIT deferred: maximum active-loop capacity is full");
        }
        let distinct_support = job
            .package
            .evidence
            .iter()
            .filter(|item| {
                matches!(item.relationship, EvidenceRelationship::Supporting)
                    && item.hit.trust_level != "untrusted"
            })
            .map(|item| {
                (
                    item.hit.canonical_entity_id.as_str(),
                    item.hit.provenance_uri.as_str(),
                )
            })
            .collect::<std::collections::HashSet<_>>()
            .len();
        if distinct_support < 2 {
            bail!("SPLIT rejected: two distinct trusted supporting canonical records are required");
        }
        let candidate = decision
            .candidate_hypothesis
            .as_ref()
            .context("SPLIT requires a complete candidate hypothesis")?;
        if candidate
            .strategy_mechanism
            .trim()
            .eq_ignore_ascii_case(job.package.current_hypothesis.strategy_mechanism.trim())
        {
            bail!("SPLIT rejected: candidate mechanism is not materially distinct");
        }
        Ok(())
    }

    fn apply_review_decision(
        &mut self,
        request: &HypothesisReviewRequest,
        package: &WorldModelReviewPackage,
        decision: HypothesisReviewDecision,
    ) -> Result<RunSnapshot> {
        let (
            current,
            parent_loop,
            last_event_id,
            next_thesis_version,
            next_context_version,
            context_definition,
        ) = {
            let active = self
                .active_runs
                .get(&request.run_id)
                .ok_or_else(|| anyhow::anyhow!("run is not active"))?;
            let current = active
                .hypotheses
                .iter()
                .find(|hypothesis| hypothesis.id == request.hypothesis_id)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("hypothesis is not active in this run"))?;
            let parent_loop = active
                .loops
                .iter()
                .find(|loop_state| loop_state.hypothesis_id == current.id)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("hypothesis has no harness-managed loop"))?;
            (
                current,
                parent_loop,
                active.last_event_id.clone(),
                active
                    .thesis_versions
                    .iter()
                    .map(|item| item.version)
                    .max()
                    .unwrap_or(0)
                    + 1,
                active
                    .context_versions
                    .iter()
                    .map(|item| item.version)
                    .max()
                    .unwrap_or(0)
                    + 1,
                active.context_definition.clone(),
            )
        };
        let mut review_causation = last_event_id.clone();
        for evidence in &decision.web_evidence {
            let ingestion_event_id = Uuid::new_v4().to_string();
            let mut record = QdrantContextPool::create_record(
                &request.run_id,
                &evidence.title,
                &evidence.claim,
                ContextSourceClass::ExternalWebResearch,
                TrustLevel::Untrusted,
                &evidence.url,
                &evidence.publisher,
                "web_evidence",
                &evidence.id,
                &ingestion_event_id,
                vec![
                    "world-model-web-research".into(),
                    if evidence.primary_eligible {
                        "recency-eligible".into()
                    } else {
                        "not-primary-eligible".into()
                    },
                ],
                json!({
                    "publicationDate": evidence.publication_date,
                    "eventDate": evidence.event_date,
                    "retrievedAt": evidence.retrieved_at,
                    "dateVerified": evidence.date_verified,
                    "recencyRequired": evidence.recency_required,
                    "usedAsPrimary": evidence.used_as_primary,
                    "primaryEligible": evidence.primary_eligible,
                    "recencyReason": evidence.recency_reason,
                    "reviewPackageId": package.id,
                    "hypothesisId": package.current_hypothesis.id,
                    "thesisVersionId": package.current_thesis.id,
                    "contextVersionId": package.current_context.id,
                    "instrument": package.current_hypothesis.instruments.first(),
                    "timeframeMinutes": package.current_hypothesis.timeframe.horizon_minutes
                }),
            );
            record.id = evidence.id.clone();
            record.canonical_entity_id = evidence.id.clone();
            record.observed_at = evidence
                .event_date
                .or(evidence.publication_date)
                .unwrap_or(evidence.retrieved_at);
            record.ingested_at = evidence.retrieved_at;
            let ingestion_event = event(
                &ingestion_event_id,
                &request.run_id,
                Some(&parent_loop.id),
                "web_evidence_ingested",
                "web_evidence",
                &evidence.id,
                Some(&review_causation),
                "External-untrusted web evidence recorded with deterministic recency metadata",
                json!({ "record": record }),
            );
            self.store
                .record_context_pool_record(&record, &ingestion_event)?;
            if let Some(context_pool) = &self.context_pool {
                context_pool.upsert(&[record])?;
            }
            review_causation = ingestion_event_id;
        }
        let review_event = event(
            &decision.created_by_event_id,
            &request.run_id,
            Some(&parent_loop.id),
            "hypothesis_reviewed",
            "hypothesis_review",
            &decision.id,
            Some(&review_causation),
            "World model reviewed a hypothesis through the OpenRouter routing and narrow research boundary",
            json!({ "record": decision, "reviewPackageId": package.id }),
        );
        self.store
            .record_hypothesis_review(&decision, &review_event)?;
        let mut causation = decision.created_by_event_id.clone();

        if matches!(
            decision.action,
            HypothesisAction::Stop | HypothesisAction::Modify
        ) {
            let pending_state = if matches!(decision.action, HypothesisAction::Stop) {
                "review-stop-pending"
            } else {
                "review-modify-pending"
            };
            let mut pending_loop = parent_loop.clone();
            pending_loop.state = pending_state.into();
            let pending_event_id = Uuid::new_v4().to_string();
            let pending_event = event(
                &pending_event_id,
                &request.run_id,
                Some(&pending_loop.id),
                "loop_state_transitioned",
                "loop",
                &pending_loop.id,
                Some(&causation),
                "Review lifecycle action blocked new exposure while deterministic flattening runs",
                json!({"record":pending_loop}),
            );
            self.store.transition_loop(&pending_loop, &pending_event)?;
            if let Some(active) = self.active_runs.get_mut(&request.run_id) {
                if let Some(loop_state) = active
                    .loops
                    .iter_mut()
                    .find(|item| item.id == pending_loop.id)
                {
                    *loop_state = pending_loop;
                }
                active.last_event_id = pending_event_id;
            }
            self.close_positions_for_lifecycle(&request.run_id, Some(&parent_loop.id))?;
            self.materialize_trade_outcomes(&request.run_id)?;
        }

        match decision.action {
            HypothesisAction::Keep => {}
            HypothesisAction::Stop => {
                let mut stopped_hypothesis = current.clone();
                stopped_hypothesis.id = Uuid::new_v4().to_string();
                stopped_hypothesis.version += 1;
                stopped_hypothesis.parent_hypothesis_id = Some(current.id.clone());
                stopped_hypothesis.status = "stopped".into();
                stopped_hypothesis.created_by_event_id = Uuid::new_v4().to_string();
                stopped_hypothesis.created_at = Utc::now();
                let stopped_event = event(
                    &stopped_hypothesis.created_by_event_id,
                    &request.run_id,
                    Some(&parent_loop.id),
                    "hypothesis_stopped",
                    "hypothesis_definition",
                    &stopped_hypothesis.id,
                    Some(&causation),
                    "World-model stop request recorded as a new immutable hypothesis version",
                    json!({ "record": stopped_hypothesis }),
                );
                self.store
                    .record_hypothesis(&stopped_hypothesis, &stopped_event)?;
                causation = stopped_hypothesis.created_by_event_id.clone();
                let stopped_at = Utc::now();
                let mut stopped_loop = parent_loop.clone();
                stopped_loop.state = "stopped".into();
                stopped_loop.stopped_at = Some(stopped_at);
                let loop_stop_event_id = Uuid::new_v4().to_string();
                let loop_stop_event = event(
                    &loop_stop_event_id,
                    &request.run_id,
                    Some(&stopped_loop.id),
                    "loop_stopped",
                    "loop",
                    &stopped_loop.id,
                    Some(&causation),
                    "Harness applied the world-model stop request",
                    json!({ "record": stopped_loop }),
                );
                self.store
                    .stop_loop(&stopped_loop.id, stopped_at, &loop_stop_event)?;
                causation = loop_stop_event_id;
                let active = self.active_runs.get_mut(&request.run_id).unwrap();
                active.hypotheses.push(stopped_hypothesis);
                if let Some(loop_state) = active
                    .loops
                    .iter_mut()
                    .find(|item| item.id == stopped_loop.id)
                {
                    *loop_state = stopped_loop;
                }
            }
            HypothesisAction::Modify | HypothesisAction::Split => {
                let now = Utc::now();
                let thesis_event_id = Uuid::new_v4().to_string();
                let mechanism = decision
                    .proposed_mechanism
                    .clone()
                    .unwrap_or_else(|| current.strategy_mechanism.clone());
                let timeframe = decision
                    .proposed_timeframe
                    .clone()
                    .unwrap_or_else(|| current.timeframe.clone());
                let thesis = ThesisVersion {
                    id: Uuid::new_v4().to_string(),
                    run_id: request.run_id.clone(),
                    version: next_thesis_version,
                    thesis: format!(
                        "Test whether {} on {} produces directional continuation over {}.",
                        mechanism,
                        current.instruments.join(", "),
                        timeframe.label
                    ),
                    provenance: format!("world-model {:?} review {}", decision.action, decision.id),
                    created_by_event_id: thesis_event_id.clone(),
                    created_at: now,
                };
                let thesis_event = event(
                    &thesis_event_id,
                    &request.run_id,
                    Some(&parent_loop.id),
                    "thesis_version_created",
                    "thesis_version",
                    &thesis.id,
                    Some(&causation),
                    "World model created a revised thesis version",
                    json!({ "record": thesis }),
                );
                self.store.record_thesis(&thesis, &thesis_event)?;

                let context_event_id = Uuid::new_v4().to_string();
                let context = ContextVersion {
                    id: Uuid::new_v4().to_string(),
                    definition_id: context_definition.id.clone(),
                    run_id: request.run_id.clone(),
                    version: next_context_version,
                    items: vec![ContextItem {
                        source: "system://world-model/review".into(),
                        source_id: decision.id.clone(),
                        observed_at: now,
                        content: format!(
                            "Review-derived mechanism={}, timeframe={}, evidence={}.",
                            mechanism,
                            timeframe.label,
                            decision.evidence_canonical_ids.join(",")
                        ),
                    }],
                    created_by_event_id: context_event_id.clone(),
                    created_at: now,
                };
                let context_event = event(
                    &context_event_id,
                    &request.run_id,
                    Some(&parent_loop.id),
                    "context_version_created",
                    "context_version",
                    &context.id,
                    Some(&thesis_event_id),
                    "World model created a revised deterministic context version",
                    json!({ "record": context }),
                );
                self.store
                    .record_context_version(&context, &context_event)?;

                let is_split = decision.action == HypothesisAction::Split;
                let hypothesis_id = Uuid::new_v4().to_string();
                let revised = HypothesisDefinition {
                    id: hypothesis_id.clone(),
                    root_hypothesis_id: if is_split {
                        hypothesis_id
                    } else {
                        current.root_hypothesis_id.clone()
                    },
                    run_id: request.run_id.clone(),
                    version: if is_split { 1 } else { current.version + 1 },
                    parent_hypothesis_id: Some(current.id.clone()),
                    original_prompt: current.original_prompt.clone(),
                    instruments: current.instruments.clone(),
                    strategy_mechanism: mechanism,
                    timeframe,
                    deterministic_context: current.deterministic_context.clone(),
                    live_context_spec: Some(if is_split {
                        crate::market_data::default_live_context_spec(
                            current
                                .instruments
                                .first()
                                .map(String::as_str)
                                .unwrap_or("BTCUSD"),
                        )
                    } else {
                        current.live_context_spec.clone().unwrap_or_else(|| {
                            crate::market_data::default_live_context_spec(
                                current
                                    .instruments
                                    .first()
                                    .map(String::as_str)
                                    .unwrap_or("BTCUSD"),
                            )
                        })
                    }),
                    jev_question: current.jev_question.clone(),
                    review_rules: current.review_rules.clone(),
                    thesis_version_id: thesis.id.clone(),
                    context_version_id: context.id.clone(),
                    status: "active".into(),
                    created_by_event_id: Uuid::new_v4().to_string(),
                    created_at: now,
                };
                let hypothesis_kind = if is_split {
                    "hypothesis_split"
                } else {
                    "hypothesis_modified"
                };
                let hypothesis_event = event(
                    &revised.created_by_event_id,
                    &request.run_id,
                    Some(&parent_loop.id),
                    hypothesis_kind,
                    "hypothesis_definition",
                    &revised.id,
                    Some(&context_event_id),
                    "World model produced an executable reviewed hypothesis version",
                    json!({ "record": revised }),
                );
                self.store.record_hypothesis(&revised, &hypothesis_event)?;
                causation = revised.created_by_event_id.clone();

                if !is_split {
                    let stopped_at = Utc::now();
                    let mut stopped_loop = parent_loop.clone();
                    stopped_loop.state = "stopped".into();
                    stopped_loop.stopped_at = Some(stopped_at);
                    let stop_event_id = Uuid::new_v4().to_string();
                    let stop_event = event(
                        &stop_event_id,
                        &request.run_id,
                        Some(&stopped_loop.id),
                        "loop_stopped",
                        "loop",
                        &stopped_loop.id,
                        Some(&causation),
                        "Harness replaced the loop after hypothesis modification",
                        json!({ "record": stopped_loop }),
                    );
                    self.store
                        .stop_loop(&stopped_loop.id, stopped_at, &stop_event)?;
                    causation = stop_event_id;
                    if let Some(active) = self.active_runs.get_mut(&request.run_id) {
                        if let Some(loop_state) = active
                            .loops
                            .iter_mut()
                            .find(|item| item.id == stopped_loop.id)
                        {
                            *loop_state = stopped_loop;
                        }
                    }
                }

                let loop_event_id = Uuid::new_v4().to_string();
                let loop_state = LoopView {
                    id: Uuid::new_v4().to_string(),
                    run_id: request.run_id.clone(),
                    parent_loop_id: Some(parent_loop.id.clone()),
                    parent_thesis_version_id: Some(current.thesis_version_id.clone()),
                    hypothesis_id: revised.id.clone(),
                    thesis_version_id: thesis.id.clone(),
                    context_version_id: context.id.clone(),
                    thesis_version: thesis.version,
                    context_version: context.version,
                    state: "jev1".into(),
                    allocated_fraction: 0.0,
                    created_by_event_id: loop_event_id.clone(),
                    created_at: now,
                    stopped_at: None,
                };
                let loop_event = event(
                    &loop_event_id,
                    &request.run_id,
                    Some(&loop_state.id),
                    "loop_spawned",
                    "loop",
                    &loop_state.id,
                    Some(&causation),
                    "Harness spawned the reviewed hypothesis loop",
                    json!({ "record": loop_state }),
                );
                self.store.spawn_loop(&loop_state, &loop_event)?;
                let cadence = cadence_for(&loop_state.id, &revised);
                let cadence_event = event(
                    &cadence.created_by_event_id,
                    &request.run_id,
                    Some(&loop_state.id),
                    "loop_cadence_mapped",
                    "loop_cadence",
                    &loop_state.id,
                    Some(&loop_event_id),
                    "Harness mapped the reviewed timeframe to Jev cadence",
                    json!({ "record": cadence }),
                );
                self.store.record_cadence(&cadence, &cadence_event)?;
                causation = cadence.created_by_event_id.clone();
                let active = self.active_runs.get_mut(&request.run_id).unwrap();
                active.thesis_versions.push(thesis);
                active.context_versions.push(context);
                active.hypotheses.push(revised);
                active.loops.push(loop_state);
                active.cadences.push(cadence);
                if is_split {
                    self.immediate_cycle_runs.insert(request.run_id.clone());
                }
            }
        }

        let allocations = {
            let active = self.active_runs.get(&request.run_id).unwrap();
            let running_count = active
                .loops
                .iter()
                .filter(|loop_state| loop_state.state != "stopped")
                .count();
            let fraction = self.allocator.allocation_for(running_count);
            active
                .loops
                .iter()
                .map(|loop_state| AllocationEntry {
                    loop_id: loop_state.id.clone(),
                    fraction: if loop_state.state == "stopped" {
                        0.0
                    } else {
                        fraction
                    },
                })
                .collect::<Vec<_>>()
        };
        let allocation_event_id = Uuid::new_v4().to_string();
        let allocation = CapitalAllocationRecord {
            id: Uuid::new_v4().to_string(),
            run_id: request.run_id.clone(),
            reason: format!(
                "harness reallocation after hypothesis {:?}",
                decision.action
            ),
            allocations,
            created_by_event_id: allocation_event_id.clone(),
            created_at: Utc::now(),
        };
        let allocation_event = event(
            &allocation_event_id,
            &request.run_id,
            None,
            "capital_allocation_changed",
            "capital_allocation",
            &allocation.id,
            Some(&causation),
            "Harness reallocated capital after applying the world-model lifecycle request",
            json!({ "record": allocation }),
        );
        self.store
            .record_allocation(&allocation, &allocation_event)?;
        {
            let active = self.active_runs.get_mut(&request.run_id).unwrap();
            for entry in &allocation.allocations {
                if let Some(loop_state) = active
                    .loops
                    .iter_mut()
                    .find(|item| item.id == entry.loop_id)
                {
                    loop_state.allocated_fraction = entry.fraction;
                }
            }
            active.last_event_id = allocation_event_id;
        }
        if let Some(context_pool) = &self.context_pool {
            context_pool.sync_pending_events(&self.store, &request.run_id)?;
        }
        self.snapshot(&request.run_id)
    }

    fn close_positions_for_lifecycle(
        &mut self,
        run_id: &str,
        only_loop_id: Option<&str>,
    ) -> Result<()> {
        let open_positions = self
            .active_runs
            .get(run_id)
            .context("run is not active")?
            .positions
            .iter()
            .filter(|position| {
                (position.state.starts_with("open") || position.state == "reconciled-open")
                    && only_loop_id
                        .map(|id| position.loop_id == id)
                        .unwrap_or(true)
            })
            .cloned()
            .collect::<Vec<_>>();
        for mut position in open_positions {
            let (loop_state, hypothesis, thesis, context, control, causation) = {
                let active = self.active_runs.get(run_id).unwrap();
                let loop_state = active
                    .loops
                    .iter()
                    .find(|item| item.id == position.loop_id)
                    .cloned()
                    .context("open position references a missing loop")?;
                let hypothesis = active
                    .hypotheses
                    .iter()
                    .find(|item| item.id == loop_state.hypothesis_id)
                    .cloned()
                    .context("open position loop references a missing hypothesis")?;
                let thesis = active
                    .thesis_versions
                    .iter()
                    .find(|item| item.id == loop_state.thesis_version_id)
                    .cloned()
                    .context("open position loop references a missing thesis version")?;
                let context = active
                    .context_versions
                    .iter()
                    .find(|item| item.id == loop_state.context_version_id)
                    .cloned()
                    .context("open position loop references a missing context version")?;
                let control = active
                    .position_controls
                    .iter()
                    .find(|item| item.position_id == position.id)
                    .cloned()
                    .context("open position is missing deterministic position control")?;
                (
                    loop_state,
                    hypothesis,
                    thesis,
                    context,
                    control,
                    active.last_event_id.clone(),
                )
            };
            let now = Utc::now();
            let decision_event_id = Uuid::new_v4().to_string();
            let decision = DecisionRecord {
                id: Uuid::new_v4().to_string(),
                run_id: run_id.into(),
                loop_id: loop_state.id.clone(),
                stage: "jev2".into(),
                thesis_version_id: thesis.id.clone(),
                context_version_id: context.id.clone(),
                position_id: Some(position.id.clone()),
                action: "STOP_CLOSE".into(),
                confidence: 1.0,
                rationale: "A deterministic lifecycle transition requires flattening the broker position before it can finalize.".into(),
                inference: JevInferenceMetadata {
                    provider: "deterministic-harness".into(),
                    requested_model: "none".into(),
                    returned_model: "none".into(),
                    question_id: "lifecycle-close".into(),
                    answer_type: "lifecycle-close".into(),
                    ..JevInferenceMetadata::default()
                },
                resolved_state: self.context_resolver.resolve(
                    &hypothesis,
                    &thesis,
                    &context,
                    Some(&position),
                )?,
                created_by_event_id: decision_event_id.clone(),
                created_at: now,
            };
            let decision_event = event(
                &decision_event_id,
                run_id,
                Some(&loop_state.id),
                "deterministic_stop_decision_recorded",
                "decision",
                &decision.id,
                Some(&causation),
                "Deterministic lifecycle close recorded before the state transition",
                json!({"record":decision}),
            );
            self.store.record_decision(&decision, &decision_event)?;

            let reference_price = self
                .broker
                .reference_price(&control.instrument)
                .unwrap_or(Some(control.entry_price))
                .unwrap_or(control.entry_price);
            let order_event_id = Uuid::new_v4().to_string();
            let order = OrderRecord {
                id: Uuid::new_v4().to_string(),
                run_id: run_id.into(),
                loop_id: loop_state.id.clone(),
                decision_id: decision.id.clone(),
                idempotency_key: format!("run-stop:{}:{}", position.id, decision.id),
                order_kind: "market_close".into(),
                instrument: control.instrument.clone(),
                side: if position.direction.to_ascii_lowercase().contains("short") {
                    "BUY"
                } else {
                    "SELL"
                }
                .into(),
                quantity: control.quantity,
                reference_price,
                notional: control.quantity * reference_price,
                stop_loss_price: None,
                signal_at: now,
                status: "approved".into(),
                rejection_reasons: Vec::new(),
                created_by_event_id: order_event_id.clone(),
                created_at: now,
            };
            let order_event = event(
                &order_event_id,
                run_id,
                Some(&loop_state.id),
                "order_evaluated",
                "order",
                &order.id,
                Some(&decision_event_id),
                "Deterministic lifecycle close order approved",
                json!({"record":order,"result":{"accepted":true,"reason":"lifecycle transition requires broker flattening"}}),
            );
            self.store.record_order(&order, &order_event)?;

            let execution_event_id = Uuid::new_v4().to_string();
            let execution = self
                .broker
                .close(&position, &order, &decision.id, &execution_event_id)
                .unwrap_or_else(|error| {
                    broker_failure_receipt(&order, "close", &execution_event_id, &error)
                });
            let execution_event = event(
                &execution_event_id,
                run_id,
                Some(&loop_state.id),
                "execution_recorded",
                "execution",
                &execution.execution_id,
                Some(&order_event_id),
                "Broker close recorded during run-stop flattening",
                json!({"record":execution}),
            );
            self.store.record_execution(&execution, &execution_event)?;
            if execution_fully_filled(&execution, &order) {
                position.state = "closed-on-stop".into();
                position.closed_by_execution_id = Some(execution.execution_id.clone());
                position.closed_at = Some(execution.executed_at);
                position.last_event_id = Uuid::new_v4().to_string();
                let close_event = event(
                    &position.last_event_id,
                    run_id,
                    Some(&loop_state.id),
                    "position_closed",
                    "position",
                    &position.id,
                    Some(&execution_event_id),
                    "Broker position flattened before run stop",
                    json!({"record":position}),
                );
                self.store.close_position(&position, &close_event)?;
                if let Some(existing) = self
                    .active_runs
                    .get_mut(run_id)
                    .unwrap()
                    .positions
                    .iter_mut()
                    .find(|item| item.id == position.id)
                {
                    *existing = position;
                }
            } else if execution_has_fill(&execution) {
                let remaining = (control.quantity - execution.filled_quantity).max(0.0);
                let mut updated_control = control.clone();
                updated_control.quantity = remaining;
                updated_control.notional = remaining * updated_control.entry_price;
                updated_control.created_by_event_id = Uuid::new_v4().to_string();
                updated_control.created_at = Utc::now();
                position.state = "open-partial-close".into();
                position.last_event_id = updated_control.created_by_event_id.clone();
                let partial_event = event(
                    &updated_control.created_by_event_id,
                    run_id,
                    Some(&loop_state.id),
                    "position_partially_closed",
                    "position_control",
                    &position.id,
                    Some(&execution_event_id),
                    "Partial stop close preserved the remaining broker quantity",
                    json!({"record":position,"control":updated_control,"filledQuantity":execution.filled_quantity}),
                );
                self.store
                    .record_partial_close(&position, &updated_control, &partial_event)?;
                if let Some(active) = self.active_runs.get_mut(run_id) {
                    if let Some(existing) = active
                        .position_controls
                        .iter_mut()
                        .find(|item| item.position_id == position.id)
                    {
                        *existing = updated_control;
                    }
                    if let Some(existing) = active
                        .positions
                        .iter_mut()
                        .find(|item| item.id == position.id)
                    {
                        *existing = position.clone();
                    }
                }
                bail!(
                    "run stop remains active because broker only closed {} of {} for position {}",
                    execution.filled_quantity,
                    control.quantity,
                    position.id
                );
            } else {
                bail!(
                    "run stop remains active because broker did not close position {}: {}",
                    position.id,
                    execution
                        .rejection_reason
                        .as_deref()
                        .unwrap_or(&execution.status)
                );
            }
        }
        Ok(())
    }

    pub fn stop(&mut self, run_id: &str) -> Result<RunSnapshot> {
        self.close_positions_for_lifecycle(run_id, None)?;
        self.materialize_trade_outcomes(run_id)?;
        let mut active = self
            .active_runs
            .remove(run_id)
            .ok_or_else(|| anyhow::anyhow!("run is not active"))?;
        let stopped_at = Utc::now();
        let mut causation = active.last_event_id.clone();
        for loop_state in &mut active.loops {
            if loop_state.state == "stopped" {
                continue;
            }
            loop_state.state = "stopped".into();
            loop_state.stopped_at = Some(stopped_at);
            let stop_event_id = Uuid::new_v4().to_string();
            let stop_event = event(
                &stop_event_id,
                run_id,
                Some(&loop_state.id),
                "loop_stopped",
                "loop",
                &loop_state.id,
                Some(&causation),
                "Loop stopped by harness lifecycle",
                json!({ "record": loop_state }),
            );
            self.store
                .stop_loop(&loop_state.id, stopped_at, &stop_event)?;
            causation = stop_event_id;
        }
        let run_stop_event_id = Uuid::new_v4().to_string();
        let run_stop_event = event(
            &run_stop_event_id,
            run_id,
            None,
            "run_stopped",
            "run",
            run_id,
            Some(&causation),
            "Run stopped after all loops recorded stop events",
            json!({ "stoppedAt": stopped_at }),
        );
        self.store.stop_run(run_id, stopped_at, &run_stop_event)?;
        if let Some(context_pool) = &self.context_pool {
            context_pool.sync_pending_events(&self.store, run_id)?;
        }
        Ok(RunSnapshot {
            run_id: run_id.to_owned(),
            status: "stopped".into(),
            thesis: active.thesis,
            hypotheses: active.hypotheses,
            cadences: active.cadences,
            loops: active.loops,
            positions: active.positions,
            events: self.store.events_for_run(run_id)?,
        })
    }

    pub fn search(&self, query: &str) -> Result<Vec<SearchHit>> {
        self.store.search(query, 25)
    }

    pub fn is_active(&self, run_id: &str) -> bool {
        self.active_runs.contains_key(run_id)
    }

    pub fn cycle_interval_seconds(&self, run_id: &str) -> Result<u64> {
        let active = self
            .active_runs
            .get(run_id)
            .ok_or_else(|| anyhow::anyhow!("run is not active"))?;
        Ok(active
            .cadences
            .iter()
            .filter(|cadence| {
                active.loops.iter().any(|loop_state| {
                    loop_state.id == cadence.loop_id && loop_state.state != "stopped"
                })
            })
            .map(|cadence| {
                let loop_state = active
                    .loops
                    .iter()
                    .find(|item| item.id == cadence.loop_id)
                    .unwrap();
                if loop_state.state.starts_with("jev2") {
                    cadence.jev2_interval_seconds
                } else {
                    cadence.jev1_interval_seconds
                }
            })
            .min()
            .unwrap_or(60)
            .max(1))
    }

    pub fn retrieve_context(&self, request: &RetrievalRequest) -> Result<RetrievalTrace> {
        self.context_pool
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Qdrant context pool is not configured"))?
            .retrieve(request)
    }

    pub fn ingest_context(&mut self, request: &ContextIngestRequest) -> Result<ContextPoolRecord> {
        let causation = self
            .active_runs
            .get(&request.run_id)
            .ok_or_else(|| anyhow::anyhow!("run is not active"))?
            .last_event_id
            .clone();
        let context_pool = self
            .context_pool
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Qdrant context pool is not configured"))?;
        let ingestion_event_id = Uuid::new_v4().to_string();
        let mut record = QdrantContextPool::create_record(
            &request.run_id,
            &request.title,
            &request.text,
            request.source_class.clone(),
            request.trust_level.clone(),
            &request.provenance_uri,
            &request.publisher,
            "context_pool_record",
            "",
            &ingestion_event_id,
            request.tags.clone(),
            request.metadata.clone(),
        );
        record.canonical_entity_id = record.id.clone();
        let ingestion_event = event(
            &ingestion_event_id,
            &request.run_id,
            None,
            "context_record_ingested",
            "context_pool_record",
            &record.id,
            Some(&causation),
            "Provenance-rich context record ingested through narrow API",
            json!({ "record": record }),
        );
        self.store
            .record_context_pool_record(&record, &ingestion_event)?;
        context_pool.upsert(&[record.clone()])?;
        context_pool.sync_pending_events(&self.store, &request.run_id)?;
        if let Some(active) = self.active_runs.get_mut(&request.run_id) {
            active.last_event_id = ingestion_event_id;
        }
        Ok(record)
    }

    pub fn replay(&self, run_id: &str) -> Result<ReplayState> {
        replay::replay_run(&self.store, run_id)
    }

    pub fn rename_run(&self, run_id: &str, name: &str) -> Result<()> {
        self.store.rename_run(run_id, name)
    }

    pub fn set_run_archived(&self, run_id: &str, archived: bool) -> Result<()> {
        self.store.set_run_archived(run_id, archived)
    }

    pub fn workspace_snapshot(
        &self,
        integrations: Vec<IntegrationStatus>,
    ) -> Result<WorkspaceSnapshot> {
        let mut active_runs = self
            .active_runs
            .keys()
            .map(|run_id| self.snapshot(run_id))
            .collect::<Result<Vec<_>>>()?;
        active_runs.sort_by(|left, right| left.run_id.cmp(&right.run_id));
        Ok(WorkspaceSnapshot {
            active_runs,
            run_history: self.store.run_summaries()?,
            integrations,
            hydrated_at: Utc::now(),
        })
    }

    pub fn run_snapshot(&self, run_id: &str) -> Result<RunSnapshot> {
        if self.active_runs.contains_key(run_id) {
            return self.snapshot(run_id);
        }
        let replay = replay::replay_run(&self.store, run_id)?;
        Ok(RunSnapshot {
            run_id: run_id.to_owned(),
            status: replay.status,
            thesis: replay.human_thesis,
            hypotheses: replay.hypotheses,
            cadences: replay.cadences,
            loops: replay.loops,
            positions: replay.positions,
            events: self.store.events_for_run(run_id)?,
        })
    }

    pub fn restore_active_runs(&mut self) -> Result<Vec<String>> {
        let run_ids = self.store.active_run_ids()?;
        for run_id in &run_ids {
            if self.active_runs.contains_key(run_id) {
                continue;
            }
            let mut state = replay::replay_run(&self.store, run_id)?;
            let events = self.store.events_for_run(run_id)?;
            let last_event_id = events
                .last()
                .map(|event| event.id.clone())
                .context("active run has no canonical events")?;
            if state.context_definitions.is_empty()
                || state.context_versions.is_empty()
                || state.thesis_versions.is_empty()
                || state.hypotheses.is_empty()
                || state.loops.is_empty()
            {
                let stopped_at = Utc::now();
                let recovery_event_id = Uuid::new_v4().to_string();
                let recovery_event = event(
                    &recovery_event_id,
                    run_id,
                    None,
                    "run_stopped",
                    "run",
                    run_id,
                    Some(&last_event_id),
                    "Recovered an interrupted startup that never committed a complete executable run",
                    json!({"stoppedAt":stopped_at,"recovery":"incomplete-startup"}),
                );
                self.store.stop_run(run_id, stopped_at, &recovery_event)?;
                continue;
            }
            for hypothesis in &mut state.hypotheses {
                if hypothesis.live_context_spec.is_none() {
                    let mut spec = crate::market_data::default_live_context_spec(
                        hypothesis
                            .instruments
                            .first()
                            .map(String::as_str)
                            .unwrap_or("BTCUSD"),
                    );
                    spec.id = format!("legacy-live-context-{}", hypothesis.id);
                    spec.created_at = hypothesis.created_at;
                    hypothesis.live_context_spec = Some(spec);
                }
            }
            let context_definition = state.context_definitions.first().cloned().unwrap();
            let failure_states = state
                .failure_states
                .iter()
                .cloned()
                .map(|failure| (failure.loop_id.clone(), failure))
                .collect();
            self.active_runs.insert(
                run_id.clone(),
                ActiveRun {
                    thesis: state.human_thesis,
                    hypotheses: state.hypotheses,
                    cadences: state.cadences,
                    thesis_versions: state.thesis_versions,
                    context_definition,
                    context_versions: state.context_versions,
                    loops: state.loops,
                    positions: state.positions,
                    position_controls: state.position_controls,
                    failure_states,
                    last_event_id,
                },
            );
            self.reconcile_broker(run_id)?;
            if let Some(context_pool) = &self.context_pool {
                // The index is secondary and may have fallen behind after a
                // prior bridge failure. Drain it after reconciliation so the
                // reconciliation event created during restore is included.
                context_pool.sync_pending_events(&self.store, run_id)?;
            }
        }
        Ok(run_ids)
    }

    fn import_broker_position(
        &mut self,
        run_id: &str,
        broker_position: &BrokerPosition,
        causation: &str,
        observed_at: chrono::DateTime<Utc>,
    ) -> Result<String> {
        let (loop_state, hypothesis, thesis, context) = {
            let active = self.active_runs.get(run_id).context("run is not active")?;
            let loop_state = active
                .loops
                .iter()
                .find(|loop_state| {
                    active
                        .hypotheses
                        .iter()
                        .find(|hypothesis| hypothesis.id == loop_state.hypothesis_id)
                        .map(|hypothesis| {
                            hypothesis.instruments.iter().any(|instrument| {
                                instrument.eq_ignore_ascii_case(&broker_position.instrument)
                            })
                        })
                        .unwrap_or(false)
                })
                .or_else(|| active.loops.iter().find(|item| item.state != "stopped"))
                .cloned()
                .context("cannot import broker position because the run has no active loop")?;
            let hypothesis = active
                .hypotheses
                .iter()
                .find(|item| item.id == loop_state.hypothesis_id)
                .cloned()
                .context("broker import loop references a missing hypothesis")?;
            let thesis = active
                .thesis_versions
                .iter()
                .find(|item| item.id == loop_state.thesis_version_id)
                .cloned()
                .context("broker import loop references a missing thesis")?;
            let context = active
                .context_versions
                .iter()
                .find(|item| item.id == loop_state.context_version_id)
                .cloned()
                .context("broker import loop references missing context")?;
            (loop_state, hypothesis, thesis, context)
        };
        let decision_event_id = Uuid::new_v4().to_string();
        let decision = DecisionRecord {
            id: Uuid::new_v4().to_string(),
            run_id: run_id.into(),
            loop_id: loop_state.id.clone(),
            stage: "jev2".into(),
            thesis_version_id: thesis.id.clone(),
            context_version_id: context.id.clone(),
            position_id: None,
            action: "RECONCILE_IMPORT".into(),
            confidence: 1.0,
            rationale:
                "Broker truth contained an open position missing from canonical local state.".into(),
            inference: JevInferenceMetadata {
                provider: "deterministic-broker-reconciliation".into(),
                requested_model: "none".into(),
                returned_model: "none".into(),
                question_id: "broker-import".into(),
                answer_type: "state-reconciliation".into(),
                ..JevInferenceMetadata::default()
            },
            resolved_state: self
                .context_resolver
                .resolve(&hypothesis, &thesis, &context, None)?,
            created_by_event_id: decision_event_id.clone(),
            created_at: observed_at,
        };
        let decision_event = event(
            &decision_event_id,
            run_id,
            Some(&loop_state.id),
            "broker_position_import_decision_recorded",
            "decision",
            &decision.id,
            Some(causation),
            "Deterministic reconciliation imported broker-only risk",
            json!({"record":decision}),
        );
        self.store.record_decision(&decision, &decision_event)?;

        let entry_price = broker_position.average_price.unwrap_or(0.0);
        let order_event_id = Uuid::new_v4().to_string();
        let order = OrderRecord {
            id: Uuid::new_v4().to_string(),
            run_id: run_id.into(),
            loop_id: loop_state.id.clone(),
            decision_id: decision.id.clone(),
            idempotency_key: format!("broker-import:{}", broker_position.broker_position_id),
            order_kind: "broker_reconciliation_import".into(),
            instrument: broker_position.instrument.clone(),
            side: broker_position.side.clone(),
            quantity: broker_position.quantity,
            reference_price: entry_price,
            notional: broker_position.quantity * entry_price,
            stop_loss_price: None,
            signal_at: observed_at,
            status: "observed".into(),
            rejection_reasons: Vec::new(),
            created_by_event_id: order_event_id.clone(),
            created_at: observed_at,
        };
        let order_event = event(
            &order_event_id,
            run_id,
            Some(&loop_state.id),
            "broker_position_import_order_recorded",
            "order",
            &order.id,
            Some(&decision_event_id),
            "Broker-only position represented by a canonical reconciliation order",
            json!({"record":order}),
        );
        self.store.record_order(&order, &order_event)?;

        let execution_event_id = Uuid::new_v4().to_string();
        let execution = ExecutionReceipt {
            execution_id: Uuid::new_v4().to_string(),
            run_id: run_id.into(),
            loop_id: loop_state.id.clone(),
            caused_by_decision_id: decision.id,
            action: if broker_position.side.eq_ignore_ascii_case("BUY") {
                Jev1Action::Long
            } else {
                Jev1Action::Short
            },
            execution_kind: "reconciliation-import".into(),
            status: "broker-open".into(),
            broker_reference: broker_position.broker_position_id.clone(),
            broker_position_id: Some(broker_position.broker_position_id.clone()),
            filled_quantity: broker_position.quantity,
            average_price: broker_position.average_price,
            rejection_reason: None,
            created_by_event_id: execution_event_id.clone(),
            executed_at: observed_at,
        };
        let execution_event = event(
            &execution_event_id,
            run_id,
            Some(&loop_state.id),
            "broker_position_imported",
            "execution",
            &execution.execution_id,
            Some(&order_event_id),
            "Remote broker position imported into canonical state",
            json!({"record":execution}),
        );
        self.store.record_execution(&execution, &execution_event)?;

        let position_event_id = Uuid::new_v4().to_string();
        let position = PositionRecord {
            id: Uuid::new_v4().to_string(),
            run_id: run_id.into(),
            loop_id: loop_state.id.clone(),
            opened_by_execution_id: execution.execution_id,
            broker_position_id: Some(broker_position.broker_position_id.clone()),
            direction: if broker_position.side.eq_ignore_ascii_case("BUY") {
                "Long"
            } else {
                "Short"
            }
            .into(),
            state: "reconciled-open".into(),
            opened_at: observed_at,
            closed_by_execution_id: None,
            closed_at: None,
            last_event_id: position_event_id.clone(),
        };
        let position_event = event(
            &position_event_id,
            run_id,
            Some(&loop_state.id),
            "position_reconciled_open",
            "position",
            &position.id,
            Some(&execution_event_id),
            "Broker-only open position added to canonical risk state",
            json!({"record":position}),
        );
        self.store.open_position(&position, &position_event)?;
        let stop_fraction = self.risk.config.stop_loss_basis_points as f64 / 10_000.0;
        let stop_loss_price = if position.direction.eq_ignore_ascii_case("short") {
            entry_price * (1.0 + stop_fraction)
        } else {
            entry_price * (1.0 - stop_fraction)
        };
        let control = PositionControlRecord {
            position_id: position.id.clone(),
            order_id: order.id,
            instrument: broker_position.instrument.clone(),
            quantity: broker_position.quantity,
            entry_price,
            notional: broker_position.quantity * entry_price,
            stop_loss_price,
            created_by_event_id: Uuid::new_v4().to_string(),
            created_at: observed_at,
        };
        let control_event = event(
            &control.created_by_event_id,
            run_id,
            Some(&loop_state.id),
            "position_control_created",
            "position_control",
            &position.id,
            Some(&position_event_id),
            "Deterministic control attached to imported broker position",
            json!({"record":control}),
        );
        self.store
            .record_position_control(&control, &control_event)?;
        let last_event_id = control_event.id.clone();
        let active = self.active_runs.get_mut(run_id).unwrap();
        active.positions.push(position);
        active.position_controls.push(control);
        if let Some(loop_record) = active
            .loops
            .iter_mut()
            .find(|item| item.id == loop_state.id)
        {
            loop_record.state = "jev2-reconciled".into();
        }
        active.last_event_id = last_event_id.clone();
        Ok(last_event_id)
    }

    pub fn reconcile_broker(&mut self, run_id: &str) -> Result<()> {
        let causation = self
            .active_runs
            .get(run_id)
            .context("run is not active")?
            .last_event_id
            .clone();
        let event_id = Uuid::new_v4().to_string();
        let snapshot = match self.broker.reconcile() {
            Ok(snapshot) => snapshot,
            Err(error) => {
                let failed = event(
                    &event_id,
                    run_id,
                    None,
                    "broker_sync_failed",
                    "broker_sync",
                    &event_id,
                    Some(&causation),
                    "Broker reconciliation failed without changing Jev or position state",
                    json!({"error":error.to_string()}),
                );
                self.store.append_event(&failed)?;
                self.active_runs.get_mut(run_id).unwrap().last_event_id = event_id;
                return Ok(());
            }
        };
        if !snapshot.connected || !snapshot.complete {
            let degraded = event(
                &event_id,
                run_id,
                None,
                "broker_sync_degraded",
                "broker_sync",
                &event_id,
                Some(&causation),
                "Incomplete broker snapshot recorded without mutating canonical positions",
                json!({"record":snapshot}),
            );
            self.store.append_event(&degraded)?;
            self.active_runs.get_mut(run_id).unwrap().last_event_id = event_id;
            return Ok(());
        }
        let sync_event = event(
            &event_id,
            run_id,
            None,
            "broker_state_synchronized",
            "broker_sync",
            &event_id,
            Some(&causation),
            "Harness synchronized canonical position state against broker truth",
            json!({"record":snapshot}),
        );
        self.store.append_event(&sync_event)?;
        let mut last_event_id = event_id.clone();
        if snapshot.adapter != "simulated" {
            let quantity_adjustments = {
                let active = self.active_runs.get(run_id).unwrap();
                snapshot
                    .positions
                    .iter()
                    .filter_map(|broker_position| {
                        let position = active.positions.iter().find(|position| {
                            position.broker_position_id.as_deref()
                                == Some(broker_position.broker_position_id.as_str())
                                && (position.state.starts_with("open")
                                    || position.state == "reconciled-open")
                        })?;
                        let control = active
                            .position_controls
                            .iter()
                            .find(|control| control.position_id == position.id)?;
                        ((control.quantity - broker_position.quantity).abs() > f64::EPSILON
                            || broker_position
                                .average_price
                                .map(|price| (price - control.entry_price).abs() > f64::EPSILON)
                                .unwrap_or(false))
                        .then(|| (position.clone(), control.clone(), broker_position.clone()))
                    })
                    .collect::<Vec<_>>()
            };
            for (mut position, mut control, broker_position) in quantity_adjustments {
                let adjustment_event_id = Uuid::new_v4().to_string();
                control.quantity = broker_position.quantity;
                if let Some(average_price) = broker_position.average_price {
                    control.entry_price = average_price;
                }
                control.notional = control.quantity * control.entry_price;
                control.created_by_event_id = adjustment_event_id.clone();
                control.created_at = snapshot.observed_at;
                position.state = "reconciled-open".into();
                position.last_event_id = adjustment_event_id.clone();
                let adjustment_event = event(
                    &adjustment_event_id,
                    run_id,
                    Some(&position.loop_id),
                    "position_quantity_reconciled",
                    "position",
                    &position.id,
                    Some(&last_event_id),
                    "Canonical position quantity updated from a complete broker snapshot",
                    json!({"record":position,"control":control,"brokerPosition":broker_position}),
                );
                self.store
                    .record_partial_close(&position, &control, &adjustment_event)?;
                let active = self.active_runs.get_mut(run_id).unwrap();
                if let Some(existing) = active
                    .positions
                    .iter_mut()
                    .find(|item| item.id == position.id)
                {
                    *existing = position;
                }
                if let Some(existing) = active
                    .position_controls
                    .iter_mut()
                    .find(|item| item.position_id == control.position_id)
                {
                    *existing = control;
                }
                last_event_id = adjustment_event_id;
            }
            let broker_ids = snapshot
                .positions
                .iter()
                .map(|position| position.broker_position_id.as_str())
                .collect::<std::collections::HashSet<_>>();
            let missing = self
                .active_runs
                .get(run_id)
                .unwrap()
                .positions
                .iter()
                .filter(|position| {
                    (position.state.starts_with("open") || position.state == "reconciled-open")
                        && position
                            .broker_position_id
                            .as_deref()
                            .map(|id| !broker_ids.contains(id))
                            .unwrap_or(true)
                })
                .map(|position| position.id.clone())
                .collect::<Vec<_>>();
            for position_id in missing {
                let mut position = self
                    .active_runs
                    .get(run_id)
                    .unwrap()
                    .positions
                    .iter()
                    .find(|position| position.id == position_id)
                    .unwrap()
                    .clone();
                position.state = "reconciled-closed".into();
                position.closed_at = Some(snapshot.observed_at);
                position.last_event_id = Uuid::new_v4().to_string();
                let position_event = event(
                    &position.last_event_id,
                    run_id,
                    Some(&position.loop_id),
                    "position_reconciled_closed",
                    "position",
                    &position.id,
                    Some(&event_id),
                    "Broker truth closed a locally open position during reconciliation",
                    json!({"record":position}),
                );
                self.store.close_position(&position, &position_event)?;
                last_event_id = position.last_event_id.clone();
                if let Some(existing) = self
                    .active_runs
                    .get_mut(run_id)
                    .unwrap()
                    .positions
                    .iter_mut()
                    .find(|existing| existing.id == position.id)
                {
                    *existing = position;
                }
            }
            let known_broker_ids = self
                .active_runs
                .values()
                .flat_map(|active| active.positions.iter())
                .filter(|position| {
                    position.state.starts_with("open") || position.state == "reconciled-open"
                })
                .filter_map(|position| position.broker_position_id.clone())
                .collect::<std::collections::HashSet<_>>();
            for broker_position in snapshot
                .positions
                .iter()
                .filter(|position| !known_broker_ids.contains(&position.broker_position_id))
            {
                last_event_id = self.import_broker_position(
                    run_id,
                    broker_position,
                    &last_event_id,
                    snapshot.observed_at,
                )?;
            }
        }
        self.active_runs.get_mut(run_id).unwrap().last_event_id = last_event_id;
        Ok(())
    }

    fn snapshot(&self, run_id: &str) -> Result<RunSnapshot> {
        let active = self
            .active_runs
            .get(run_id)
            .ok_or_else(|| anyhow::anyhow!("run is not active"))?;
        Ok(RunSnapshot {
            run_id: run_id.to_owned(),
            status: "active".into(),
            thesis: active.thesis.clone(),
            hypotheses: active.hypotheses.clone(),
            cadences: active.cadences.clone(),
            loops: active.loops.clone(),
            positions: active.positions.clone(),
            events: self.store.events_for_run(run_id)?,
        })
    }
}

fn broker_failure_receipt(
    order: &OrderRecord,
    execution_kind: &str,
    event_id: &str,
    error: &anyhow::Error,
) -> ExecutionReceipt {
    ExecutionReceipt {
        execution_id: Uuid::new_v4().to_string(),
        run_id: order.run_id.clone(),
        loop_id: order.loop_id.clone(),
        caused_by_decision_id: order.decision_id.clone(),
        action: if order.side == "BUY" {
            Jev1Action::Long
        } else {
            Jev1Action::Short
        },
        execution_kind: execution_kind.into(),
        status: "connection-error".into(),
        broker_reference: "unavailable".into(),
        broker_position_id: None,
        filled_quantity: 0.0,
        average_price: None,
        rejection_reason: Some(error.to_string()),
        created_by_event_id: event_id.into(),
        executed_at: Utc::now(),
    }
}

fn execution_has_fill(execution: &ExecutionReceipt) -> bool {
    execution.filled_quantity.is_finite()
        && execution.filled_quantity > 0.0
        && matches!(
            execution.status.as_str(),
            "filled" | "simulated-filled" | "partially-filled"
        )
}

fn execution_fully_filled(execution: &ExecutionReceipt, order: &OrderRecord) -> bool {
    execution_has_fill(execution)
        && execution.filled_quantity + f64::EPSILON >= order.quantity
        && execution.status != "partially-filled"
}

fn ensure_cycle_active(cancellation: &AtomicBool) -> Result<()> {
    if cancellation.load(Ordering::SeqCst) {
        bail!("run cycle cancelled before broker execution");
    }
    Ok(())
}

fn evidence_relationship(hit: &RetrievalHit) -> EvidenceRelationship {
    let text = format!("{} {} {}", hit.title, hit.text, hit.metadata).to_ascii_lowercase();
    if [
        "contradict",
        "invalid",
        "failed",
        "rejected",
        "stopped",
        "loss",
        "weaken",
    ]
    .iter()
    .any(|term| text.contains(term))
    {
        EvidenceRelationship::Contradictory
    } else if [
        "support",
        "filled",
        "success",
        "retain",
        "keep",
        "confirmed",
    ]
    .iter()
    .any(|term| text.contains(term))
    {
        EvidenceRelationship::Supporting
    } else {
        EvidenceRelationship::Related
    }
}

fn cadence_for(loop_id: &str, hypothesis: &HypothesisDefinition) -> LoopCadence {
    let horizon = hypothesis.timeframe.horizon_minutes;
    let (jev1, jev2, rule) = if horizon <= 15 {
        (60, 60, "sub-15m")
    } else if horizon <= 60 {
        (300, 300, "15m-to-60m")
    } else if horizon <= 240 {
        (900, 600, "1h-to-4h")
    } else if horizon <= 1440 {
        (3600, 1800, "4h-to-1d")
    } else {
        (14400, 3600, "multi-day")
    };
    LoopCadence {
        loop_id: loop_id.to_owned(),
        hypothesis_id: hypothesis.id.clone(),
        timeframe_horizon_minutes: horizon,
        jev1_interval_seconds: jev1,
        jev2_interval_seconds: jev2,
        mapping_rule: rule.into(),
        created_by_event_id: Uuid::new_v4().to_string(),
        created_at: Utc::now(),
    }
}

fn review_protocol(kinds: &[AutonomousReviewTriggerKind]) -> String {
    let reasons = kinds
        .iter()
        .map(|kind| match kind {
            AutonomousReviewTriggerKind::LossStreak => "loss streak",
            AutonomousReviewTriggerKind::NoTradeStreak => "no-trade streak",
            AutonomousReviewTriggerKind::PeriodicTradeCount => "periodic trade audit",
        })
        .collect::<Vec<_>>()
        .join(", ");
    let mut instructions = format!(
        "Autonomous review triggered by: {reasons}. Use only the immutable package and cited evidence."
    );
    if kinds.contains(&AutonomousReviewTriggerKind::LossStreak) {
        instructions.push_str(" LOSS PROTOCOL: compare realized round trips with support and invalidation rules and prior experiments. Three losses are not automatic failure; KEEP is valid when expected variance explains them. MODIFY only for an evidenced correctable defect, STOP only for clear invalidation, SPLIT only for a materially distinct mechanism.");
    }
    if kinds.contains(&AutonomousReviewTriggerKind::NoTradeStreak) {
        instructions.push_str(" NO-TRADE PROTOCOL: determine whether inactivity is expected, entry conditions are unreachable, required context is stale or missing, or the regime mismatches the hypothesis. Never weaken confidence, stop, sizing, or execution controls.");
    }
    if kinds.contains(&AutonomousReviewTriggerKind::PeriodicTradeCount) {
        instructions.push_str(" PERIODIC PROTOCOL: conduct a neutral performance audit. KEEP is the default unless material evidence identifies a problem. Do not use internet research unless current regime or catalyst evidence is necessary.");
    }
    instructions.push_str(" Return trigger-aware diagnosis, severity, decision confidence, and continuation rationale. MODIFY or SPLIT must provide a complete executable replacement or candidate hypothesis.");
    instructions
}

fn no_trade_review_threshold(cadence: &LoopCadence, policy: &AutonomousReviewPolicy) -> usize {
    if policy.no_trade_horizon_mode == "fixed_minimum" {
        return policy.no_trade_min_decisions;
    }
    let raw = (cadence.timeframe_horizon_minutes.saturating_mul(60)
        + cadence.jev1_interval_seconds.saturating_sub(1))
        / cadence.jev1_interval_seconds.max(1);
    (raw as usize).clamp(policy.no_trade_min_decisions, policy.no_trade_max_decisions)
}

fn classify_trade_outcome(pnl: Option<f64>) -> TradeOutcomeClassification {
    match pnl {
        Some(value) if value > f64::EPSILON => TradeOutcomeClassification::Win,
        Some(value) if value < -f64::EPSILON => TradeOutcomeClassification::Loss,
        Some(_) => TradeOutcomeClassification::Breakeven,
        None => TradeOutcomeClassification::Unknown,
    }
}

#[cfg(test)]
mod autonomous_review_tests {
    use super::*;

    #[test]
    fn no_trade_threshold_uses_full_horizon_and_clamps() {
        let policy = AutonomousReviewPolicy::default();
        let cadence = |horizon, interval| LoopCadence {
            loop_id: "loop".into(),
            hypothesis_id: "hypothesis".into(),
            timeframe_horizon_minutes: horizon,
            jev1_interval_seconds: interval,
            jev2_interval_seconds: interval,
            mapping_rule: "test".into(),
            created_by_event_id: "event".into(),
            created_at: Utc::now(),
        };
        assert_eq!(no_trade_review_threshold(&cadence(5, 300), &policy), 6);
        assert_eq!(no_trade_review_threshold(&cadence(60, 300), &policy), 12);
        assert_eq!(no_trade_review_threshold(&cadence(1_000, 60), &policy), 20);
    }

    #[test]
    fn outcome_classification_preserves_unknown_cost_basis() {
        assert!(matches!(
            classify_trade_outcome(Some(10.0)),
            TradeOutcomeClassification::Win
        ));
        assert!(matches!(
            classify_trade_outcome(Some(-10.0)),
            TradeOutcomeClassification::Loss
        ));
        assert!(matches!(
            classify_trade_outcome(Some(0.0)),
            TradeOutcomeClassification::Breakeven
        ));
        assert!(matches!(
            classify_trade_outcome(None),
            TradeOutcomeClassification::Unknown
        ));
    }

    #[test]
    fn combined_protocol_orders_loss_before_no_trade_before_periodic() {
        let text = review_protocol(&[
            AutonomousReviewTriggerKind::LossStreak,
            AutonomousReviewTriggerKind::NoTradeStreak,
            AutonomousReviewTriggerKind::PeriodicTradeCount,
        ]);
        assert!(text.find("LOSS PROTOCOL").unwrap() < text.find("NO-TRADE PROTOCOL").unwrap());
        assert!(text.find("NO-TRADE PROTOCOL").unwrap() < text.find("PERIODIC PROTOCOL").unwrap());
        assert!(text.contains("Never weaken confidence"));
    }
}

#[allow(clippy::too_many_arguments)]
fn event(
    id: &str,
    run_id: &str,
    loop_id: Option<&str>,
    kind: &str,
    aggregate_type: &str,
    aggregate_id: &str,
    causation_event_id: Option<&str>,
    summary: &str,
    mut payload: serde_json::Value,
) -> HarnessEvent {
    payload["summary"] = json!(summary);
    HarnessEvent {
        id: id.to_owned(),
        run_id: run_id.to_owned(),
        loop_id: loop_id.map(str::to_owned),
        kind: kind.to_owned(),
        aggregate_type: aggregate_type.to_owned(),
        aggregate_id: aggregate_id.to_owned(),
        causation_event_id: causation_event_id.map(str::to_owned),
        occurred_at: Utc::now(),
        payload,
    }
}
