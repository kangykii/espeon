use crate::adapters::*;
use crate::context::StructuredContextResolver;
use crate::domain::*;
use crate::ports::*;
use crate::replay;
use crate::retrieval::QdrantContextPool;
use crate::risk::{
    explicit_direction_constraint, idempotency_key, CloseRiskInput, DeterministicRiskEngine,
    EntryRiskInput,
};
use crate::storage::CanonicalStore;
use anyhow::{bail, Context, Result};
use chrono::Utc;
use serde_json::json;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Clone)]
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

#[derive(Clone)]
struct PendingStartupActivation {
    compiled: crate::contracts::CompiledContract,
    draft_event_id: String,
    retrieval_trace: Option<RetrievalTrace>,
    additional_evidence_ids: Vec<String>,
    independently_fresh_evidence_ids: Vec<String>,
    last_warmup_detail: Option<String>,
}

fn parse_contract_evidence_provenance(
    payload: &serde_json::Value,
) -> Result<(Option<RetrievalTrace>, Vec<String>, Vec<String>)> {
    let retrieval_trace = payload
        .get("retrievalTrace")
        .context("contract DRAFT event omitted retrievalTrace provenance")?;
    let retrieval_trace = if retrieval_trace.is_null() {
        None
    } else {
        Some(
            serde_json::from_value(retrieval_trace.clone())
                .context("contract DRAFT event has malformed retrievalTrace provenance")?,
        )
    };
    let ids = |key: &str| -> Result<Vec<String>> {
        payload
            .get(key)
            .and_then(serde_json::Value::as_array)
            .with_context(|| format!("contract DRAFT event omitted array {key}"))?
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .filter(|id| !id.trim().is_empty())
                    .map(str::to_owned)
                    .with_context(|| {
                        format!("contract DRAFT event contains an invalid ID in {key}")
                    })
            })
            .collect()
    };
    Ok((
        retrieval_trace,
        ids("additionalEvidenceIds")?,
        ids("independentlyFreshEvidenceIds")?,
    ))
}

fn validate_contract_review_evidence_provenance(
    stored_events: &[StoredEvent],
    thesis_event: &HarnessEvent,
    run_id: &str,
    parent_hypothesis_id: Option<&str>,
    proposal: &crate::contracts::HypothesisContractDraft,
    retrieval_trace: Option<&RetrievalTrace>,
    additional_evidence_ids: &[String],
    independently_fresh_ids: &[String],
) -> Result<Vec<String>> {
    let mut cause = thesis_event.causation_event_id.as_deref();
    let mut visited = std::collections::HashSet::new();
    let review_event = loop {
        let Some(id) = cause else { break None };
        if !visited.insert(id) {
            bail!("contract provenance contains a causal event cycle");
        }
        let Some(stored) = stored_events.iter().find(|stored| stored.event.id == id) else {
            bail!("contract provenance references missing causal event {id}");
        };
        if stored.event.kind == "hypothesis_reviewed" {
            break Some(&stored.event);
        }
        cause = stored.event.causation_event_id.as_deref();
    };
    let Some(review_event) = review_event else {
        if !additional_evidence_ids.is_empty() || !independently_fresh_ids.is_empty() {
            bail!(
                "initial contract has unsupported additional or independently fresh evidence IDs"
            );
        }
        return Ok(Vec::new());
    };
    let decision: HypothesisReviewDecision = serde_json::from_value(
        review_event
            .payload
            .get("record")
            .cloned()
            .context("contract review ancestor omitted its decision record")?,
    )
    .context("contract review ancestor contains a malformed decision record")?;
    if decision.created_by_event_id != review_event.id {
        bail!("contract review decision is not linked to its creation event");
    }
    let package_id = review_event
        .payload
        .get("reviewPackageId")
        .and_then(serde_json::Value::as_str)
        .context("contract review event omitted its review package ID")?;
    let review_event_index = stored_events
        .iter()
        .position(|stored| stored.event.id == review_event.id)
        .context("contract review event is absent from the canonical event sequence")?;
    let prior_events = &stored_events[..review_event_index];
    let matches_package_event = |stored: &&StoredEvent| {
        matches!(
            stored.event.kind.as_str(),
            "world_model_review_package_assembled" | "autonomous_review_package_assembled"
        ) && stored.event.aggregate_id == package_id
    };
    let package_event = if let Some(event_id) = review_event
        .payload
        .get("reviewPackageEventId")
        .and_then(serde_json::Value::as_str)
    {
        prior_events
            .iter()
            .find(|stored| stored.event.id == event_id && matches_package_event(stored))
    } else {
        prior_events.iter().rev().find(matches_package_event)
    }
    .context("contract review package creation event is missing")?;
    let package: WorldModelReviewPackage = serde_json::from_value(
        package_event
            .event
            .payload
            .get("record")
            .cloned()
            .context("contract review package event omitted its record")?,
    )
    .context("contract review package event contains a malformed record")?;
    if decision.run_id != review_event.run_id
        || decision.run_id != run_id
        || package.run_id != review_event.run_id
        || package.id != package_id
        || package_event.event.id != package.created_by_event_id
        || package_event.event.run_id != review_event.run_id
        || package_event.event.aggregate_type != "world_model_review_package"
        || decision.hypothesis_id != package.current_hypothesis.id
        || !matches!(
            decision.action,
            HypothesisAction::Modify | HypothesisAction::Split
        )
        || parent_hypothesis_id != Some(decision.hypothesis_id.as_str())
        || proposal.user_objective.trim().is_empty()
    {
        bail!("contract review decision and package provenance do not match");
    }
    if serde_json::to_value(package.historical_retrieval.as_ref())?
        != serde_json::to_value(retrieval_trace)?
    {
        bail!("contract DRAFT retrieval trace differs from its source review package");
    }
    let mut expected_additional = decision.evidence_canonical_ids.clone();
    expected_additional.extend(package.exact_canonical_ids.iter().cloned());
    expected_additional.sort();
    expected_additional.dedup();
    let mut actual_additional = additional_evidence_ids.to_vec();
    actual_additional.sort();
    actual_additional.dedup();
    if actual_additional != expected_additional
        || actual_additional.len() != additional_evidence_ids.len()
    {
        bail!("contract DRAFT additional evidence IDs differ from the validated review inputs");
    }
    let mut expected_fresh = decision
        .web_evidence
        .iter()
        .filter(|evidence| evidence.primary_eligible)
        .map(|evidence| evidence.id.clone())
        .collect::<Vec<_>>();
    expected_fresh.sort();
    expected_fresh.dedup();
    let mut actual_fresh = independently_fresh_ids.to_vec();
    actual_fresh.sort();
    actual_fresh.dedup();
    if actual_fresh != expected_fresh || actual_fresh.len() != independently_fresh_ids.len() {
        bail!("contract DRAFT fresh-evidence IDs differ from validated review evidence");
    }
    let objective = proposal.user_objective.to_ascii_lowercase();
    let freshness_required = [
        "latest",
        "current",
        "today",
        "news",
        "filing",
        "earnings",
        "guidance",
        "regulatory",
        "launch",
        "investor event",
        "macro data",
        "market-moving",
    ]
    .iter()
    .any(|term| objective.contains(term));
    let now = Utc::now();
    let max_age = if proposal.timeframe.horizon_minutes <= 1_440 {
        chrono::Duration::hours(72)
    } else if proposal.timeframe.horizon_minutes <= 10_080 {
        chrono::Duration::days(14)
    } else {
        chrono::Duration::days(30)
    };
    let is_fresh_now = |date: chrono::DateTime<Utc>| {
        date <= now + chrono::Duration::hours(24) && now.signed_duration_since(date) <= max_age
    };
    let mut currently_fresh = Vec::new();
    for evidence in &decision.web_evidence {
        let evidence_event = prior_events
            .iter()
            .rev()
            .find(|stored| {
                stored.event.kind == "web_evidence_ingested"
                    && stored.event.aggregate_id == evidence.id
                    && stored.event.run_id == decision.run_id
                    && stored.event.payload["record"]["metadata"]["reviewPackageId"]
                        == serde_json::json!(package_id)
                    && stored.event.payload["record"]["metadata"]["hypothesisId"]
                        == serde_json::json!(decision.hypothesis_id)
            })
            .with_context(|| {
                format!(
                    "review web evidence {} has no immutable ingestion event",
                    evidence.id
                )
            })?;
        let record: ContextPoolRecord = serde_json::from_value(
            evidence_event
                .event
                .payload
                .get("record")
                .cloned()
                .context("web-evidence ingestion event omitted its record")?,
        )
        .context("web-evidence ingestion record is malformed")?;
        if evidence_event.event.id != record.canonical_event_id
            || evidence_event.event.aggregate_type != "web_evidence"
            || record.id != evidence.id
            || record.canonical_entity_id != evidence.id
            || record.run_id != decision.run_id
            || record.canonical_entity_type != "web_evidence"
            || !matches!(record.source_class, ContextSourceClass::ExternalWebResearch)
            || !matches!(record.trust_level, TrustLevel::Untrusted)
            || record.provenance_uri != evidence.url
            || record.title != evidence.title
            || record.publisher != evidence.publisher
            || record.text != evidence.claim
            || record.metadata.get("reviewPackageId") != Some(&serde_json::json!(package_id))
            || record.metadata.get("hypothesisId")
                != Some(&serde_json::json!(decision.hypothesis_id))
            || record.metadata.get("dateVerified")
                != Some(&serde_json::json!(evidence.date_verified))
            || record.metadata.get("primaryEligible")
                != Some(&serde_json::json!(evidence.primary_eligible))
            || record.metadata.get("recencyRequired")
                != Some(&serde_json::json!(evidence.recency_required))
            || record.metadata.get("publicationDate")
                != Some(&serde_json::to_value(evidence.publication_date)?)
            || record.metadata.get("eventDate") != Some(&serde_json::to_value(evidence.event_date)?)
            || record.metadata.get("retrievedAt")
                != Some(&serde_json::to_value(evidence.retrieved_at)?)
        {
            bail!(
                "review web evidence {} differs from its immutable ingestion record",
                evidence.id
            );
        }
        if evidence.primary_eligible {
            let publication_fresh = evidence.publication_date.is_some_and(is_fresh_now);
            let event_fresh = evidence.event_date.is_none_or(is_fresh_now);
            if freshness_required && (!publication_fresh || !event_fresh) {
                bail!(
                    "independently fresh web evidence {} is now stale for this contract",
                    evidence.id
                );
            }
            if !freshness_required || (publication_fresh && event_fresh) {
                currently_fresh.push(evidence.id.clone());
            }
        }
    }
    Ok(currently_fresh)
}

fn is_recoverable_initial_startup_draft(
    hypothesis: &HypothesisDefinition,
    state: &crate::domain::ReplayState,
    events: &[StoredEvent],
    run_id: &str,
) -> bool {
    if hypothesis.status != "DRAFT" || hypothesis.run_id != run_id {
        return false;
    }
    let Some(mut contract) = hypothesis.contract.as_ref() else {
        return false;
    };
    if contract.state != crate::contracts::ContractState::Draft {
        return false;
    }
    let mut child = hypothesis;
    let mut visited_contracts = std::collections::HashSet::new();
    loop {
        if !visited_contracts.insert(contract.id.as_str())
            || contract.run_id != run_id
            || contract.proposal.user_objective != state.human_thesis
        {
            return false;
        }
        let Some(parent_contract_id) = contract.parent_contract_id.as_deref() else {
            if contract.version != 1 || child.parent_hypothesis_id.is_some() {
                return false;
            }
            break;
        };
        let Some(parent) = state.hypotheses.iter().find(|candidate| {
            candidate
                .contract
                .as_ref()
                .is_some_and(|item| item.id == parent_contract_id)
        }) else {
            return false;
        };
        let Some(parent_contract) = parent.contract.as_ref() else {
            return false;
        };
        if parent.status != "REJECTED"
            || parent_contract.state != crate::contracts::ContractState::Rejected
            || parent.run_id != run_id
            || child.parent_hypothesis_id.as_deref() != Some(parent.id.as_str())
            || contract.version != parent_contract.version.saturating_add(1)
        {
            return false;
        }
        child = parent;
        contract = parent_contract;
    }

    let Some(draft_event) = events.iter().find(|stored| {
        stored.event.kind == "hypothesis_version_created"
            && stored.event.id == hypothesis.created_by_event_id
            && stored.event.aggregate_id
                == hypothesis
                    .contract
                    .as_ref()
                    .map_or("", |item| item.id.as_str())
    }) else {
        return false;
    };
    if draft_event.event.run_id != run_id
        || draft_event.event.loop_id.is_some()
        || draft_event.event.aggregate_type != "hypothesis_contract"
    {
        return false;
    }
    let mut ancestor = draft_event;
    for expected_kind in [
        "context_version_created",
        "context_definition_created",
        "thesis_version_created",
    ] {
        let Some(cause_id) = ancestor.event.causation_event_id.as_deref() else {
            return false;
        };
        let Some(previous) = events.iter().find(|stored| stored.event.id == cause_id) else {
            return false;
        };
        if previous.event.kind != expected_kind
            || previous.event.run_id != run_id
            || previous.event.loop_id.is_some()
        {
            return false;
        }
        ancestor = previous;
    }
    let mut cause = ancestor.event.causation_event_id.as_deref();
    let mut visited_events = std::collections::HashSet::new();
    while let Some(id) = cause {
        if !visited_events.insert(id) {
            return false;
        }
        let Some(previous) = events.iter().find(|stored| stored.event.id == id) else {
            return false;
        };
        if previous.event.run_id != run_id || previous.event.kind == "hypothesis_reviewed" {
            return false;
        }
        if previous.event.kind == "run_started" {
            return previous.event.aggregate_id == run_id;
        }
        cause = previous.event.causation_event_id.as_deref();
    }
    false
}

fn normalize_instrument_code(instrument: &str) -> String {
    instrument
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_uppercase)
        .collect()
}

fn position_may_have_exposure(position: &PositionRecord) -> bool {
    !matches!(
        position.state.as_str(),
        "closed" | "closed-on-stop" | "reconciled-closed"
    )
}

fn normalize_contract_evidence_citations(
    proposal: &mut crate::contracts::HypothesisContractDraft,
    retrieval_trace: Option<&RetrievalTrace>,
    additional_evidence_ids: &[String],
) {
    let mut available = additional_evidence_ids.iter().cloned().collect::<std::collections::HashSet<_>>();
    if let Some(trace) = retrieval_trace {
        for hit in &trace.hits {
            for id in [&hit.id, &hit.canonical_entity_id, &hit.canonical_event_id] {
                if !id.trim().is_empty() {
                    available.insert(id.clone());
                }
            }
        }
    }
    for ids in [
        &mut proposal.supporting_evidence_ids,
        &mut proposal.contradictory_evidence_ids,
    ] {
        let mut seen = std::collections::HashSet::new();
        ids.retain_mut(|id| {
            *id = id.trim().to_owned();
            !id.is_empty() && available.contains(id) && seen.insert(id.clone())
        });
    }
}

fn normalize_compiled_contract_lifecycle(
    compiled: &mut crate::contracts::CompiledContract,
    authoritative_objective: &str,
) -> Result<()> {
    crate::contracts::normalize_contract_timeframe(
        &mut compiled.contract.proposal,
        authoritative_objective,
    )
    .context("contract timeframe normalization failed at harness boundary")?;
    crate::skills::SkillRegistry::discover_builtin()
        .normalize_and_apply_contract_against_objective(
            &mut compiled.contract.proposal,
            authoritative_objective,
        )
        .context("contract lifecycle normalization failed at harness boundary")?;
    if let Some(hypothesis_contract) = compiled.hypothesis.contract.as_mut() {
        hypothesis_contract.proposal = compiled.contract.proposal.clone();
    }
    Ok(())
}

pub struct HarnessController {
    store: CanonicalStore,
    world_model: Arc<dyn WorldModel>,
    jev: Box<dyn JevEngine>,
    context_resolver: Box<dyn ContextResolver>,
    market_data: std::sync::Arc<dyn MarketDataProvider>,
    broker: Box<dyn ExecutionBroker>,
    risk: DeterministicRiskEngine,
    minimum_confidence: f64,
    allocator: EqualCapitalAllocator,
    context_pool: Option<QdrantContextPool>,
    active_runs: HashMap<String, ActiveRun>,
    review_policy: AutonomousReviewPolicy,
    pending_review_jobs: Vec<AutonomousReviewJob>,
    pending_stop_wrapups: HashMap<String, Vec<WorldModelReviewPackage>>,
    pending_startup_activations: HashMap<String, PendingStartupActivation>,
    recovery_blocked_runs: HashMap<String, String>,
    immediate_cycle_runs: std::collections::HashSet<String>,
    manual_risk_snapshot: Option<crate::ports::BrokerRiskSnapshot>,
    manual_entry_volume_limits: Option<(String, f64, f64)>,
    manual_demo_minimum_step: Option<String>,
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
            market_data: std::sync::Arc::new(crate::market_data::SimulatedMarketDataProvider::new()),
            broker: Box::new(SimulatedBroker),
            risk: DeterministicRiskEngine::new(RiskPolicyConfig::default()),
            minimum_confidence,
            allocator: EqualCapitalAllocator,
            context_pool: None,
            active_runs: HashMap::new(),
            review_policy: AutonomousReviewPolicy::default(),
            pending_review_jobs: Vec::new(),
            pending_stop_wrapups: HashMap::new(),
            pending_startup_activations: HashMap::new(),
            recovery_blocked_runs: HashMap::new(),
            immediate_cycle_runs: std::collections::HashSet::new(),
            manual_risk_snapshot: None,
            manual_entry_volume_limits: None,
            manual_demo_minimum_step: None,
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

    pub fn configure_market_data(&mut self, provider: std::sync::Arc<dyn MarketDataProvider>) {
        self.market_data = provider;
    }

    pub fn configure_context_pool(&mut self, pool: QdrantContextPool) {
        self.context_pool = Some(pool);
    }

    pub fn run_human_verified_live_cycle(
        &mut self,
        run_id: &str,
        mut snapshot: crate::ports::BrokerRiskSnapshot,
        volume_minimum: Option<f64>,
        volume_step: Option<f64>,
        confirmed: bool,
    ) -> Result<RunSnapshot> {
        if !confirmed {
            bail!(
                "confirm the current cTrader account and broker limits before running this cycle"
            );
        }
        if !self.is_active(run_id) {
            bail!("select an active run before approving a live cycle");
        }
        let identity = self
            .broker
            .risk_account_identity()
            .context("live broker account identity is unavailable")?;
        if snapshot.account_id.trim() != identity.account_id.trim()
            || !snapshot
                .environment
                .trim()
                .eq_ignore_ascii_case(identity.environment.trim())
        {
            bail!(
                "verified account or environment does not match the configured cTrader FIX account"
            );
        }
        if !snapshot.equity.is_finite()
            || snapshot.equity <= 0.0
            || !snapshot.free_margin.is_finite()
            || snapshot.free_margin < 0.0
            || !snapshot.account_open_exposure.is_finite()
            || snapshot.account_open_exposure < 0.0
            || snapshot.deposit_asset_id.trim().is_empty()
            || !crate::risk::is_supported_currency_code(&snapshot.deposit_currency_code)
        {
            bail!("verified equity, margin, exposure, and deposit currency must be valid");
        }
        let instrument = self
            .active_runs
            .get(run_id)
            .and_then(|active| active.hypotheses.first())
            .and_then(|hypothesis| hypothesis.instruments.first())
            .context("active run has no instrument to verify")?
            .clone();
        let conversion = snapshot
            .quote_to_deposit
            .iter()
            .find(|(symbol, _)| {
                normalize_instrument_code(symbol) == normalize_instrument_code(&instrument)
            })
            .map(|(_, rate)| *rate)
            .context("verified quote-to-deposit rate is missing for the active instrument")?;
        if !conversion.is_finite() || conversion <= 0.0 {
            bail!("verified quote-to-deposit rate must be positive and finite");
        }
        let volume_limits = match (volume_minimum, volume_step) {
            (Some(minimum), Some(step))
                if minimum.is_finite() && minimum > 0.0 && step.is_finite() && step > 0.0 =>
            {
                Some((minimum, step))
            }
            (None, None) if identity.environment.trim().eq_ignore_ascii_case("demo") => None,
            (None, None) => bail!("live entries still require broker-confirmed volume limits"),
            _ => bail!("provide both positive volume limits or neither"),
        };
        snapshot.observed_at = Utc::now();
        let verification_id = Uuid::new_v4().to_string();
        let verification_event = event(
            &verification_id,
            run_id,
            None,
            "human_live_risk_verified",
            "broker_risk_verification",
            &identity.account_id,
            None,
            "Human verified the current account risk values for one decision cycle",
            json!({
                "environment":identity.environment,
                "instrument":instrument,
                "equity":snapshot.equity,
                "freeMargin":snapshot.free_margin,
                "depositCurrency":snapshot.deposit_currency_code,
                "accountOpenExposure":snapshot.account_open_exposure,
                "quoteToDeposit":conversion,
                "volumeMinimum":volume_limits.map(|limits| limits.0),
                "volumeStep":volume_limits.map(|limits| limits.1),
                "demoVolumeMetadataOverride":volume_limits.is_none(),
                "demoMinimumStepRoundUp":volume_limits.is_none() && identity.environment.trim().eq_ignore_ascii_case("demo"),
                "expiresAfterOneCycle":true
            }),
        );
        self.store.append_event(&verification_event)?;
        match volume_limits {
            Some((minimum, step)) => {
                self.broker
                    .approve_manual_entry_limits(&instrument, minimum, step)?
            }
            None => self
                .broker
                .approve_demo_entry_without_volume_metadata(&instrument)?,
        }
        self.manual_risk_snapshot = Some(snapshot);
        self.manual_entry_volume_limits =
            volume_limits.map(|(minimum, step)| (instrument.clone(), minimum, step));
        self.manual_demo_minimum_step = (volume_limits.is_none()
            && identity.environment.trim().eq_ignore_ascii_case("demo"))
        .then(|| instrument.clone());
        let result = self.run_cycle(run_id);
        self.manual_risk_snapshot = None;
        self.manual_entry_volume_limits = None;
        self.manual_demo_minimum_step = None;
        if volume_limits.is_none() {
            self.broker
                .clear_demo_entry_without_volume_metadata(&instrument);
        }
        result
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

    pub fn requeue_review_job(&mut self, job: AutonomousReviewJob) {
        if self.is_active(&job.trigger.run_id)
            && !self
                .pending_review_jobs
                .iter()
                .any(|pending| pending.trigger.trigger_id == job.trigger.trigger_id)
        {
            self.pending_review_jobs.push(job);
        }
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

    pub fn schedule_immediate_cycle(&mut self, run_id: &str) {
        if self.is_active(run_id) {
            self.immediate_cycle_runs.insert(run_id.to_owned());
        }
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
        self.start_linked(human_thesis, None)
    }

    pub fn continue_from_run(
        &mut self,
        human_thesis: &str,
        source_run_id: &str,
    ) -> Result<RunSnapshot> {
        self.start_linked(human_thesis, Some(source_run_id))
    }

    fn start_linked(
        &mut self,
        human_thesis: &str,
        source_run_id: Option<&str>,
    ) -> Result<RunSnapshot> {
        if human_thesis.trim().is_empty() {
            bail!("thesis cannot be empty");
        }
        let continuation_context = source_run_id
            .map(|source_run_id| self.continuation_context(source_run_id))
            .transpose()?;
        let run_id = Uuid::new_v4().to_string();
        let result = self.start_with_id(
            human_thesis,
            run_id.clone(),
            source_run_id,
            continuation_context,
        );
        match result {
            Ok(snapshot) => Ok(snapshot),
            Err(error) => {
                if self.store.active_run_ids()?.contains(&run_id) {
                    let stopped_at = Utc::now();
                    let last_event_id = self
                        .store
                        .events_for_run(&run_id)?
                        .last()
                        .map(|event| event.id.clone());
                    let stop_event_id = Uuid::new_v4().to_string();
                    let stop_event = event(
                        &stop_event_id,
                        &run_id,
                        None,
                        "run_stopped",
                        "run",
                        &run_id,
                        last_event_id.as_deref(),
                        "Run startup failed before background execution began",
                        json!({"stoppedAt":stopped_at,"startupError":format!("{error:#}")}),
                    );
                    self.store.stop_run(&run_id, stopped_at, &stop_event)?;
                    self.active_runs.remove(&run_id);
                    self.immediate_cycle_runs.remove(&run_id);
                }
                Err(error)
            }
        }
    }

    fn contract_validation_context(
        &self,
        proposal: &crate::contracts::HypothesisContractDraft,
        retrieval_trace: Option<&RetrievalTrace>,
        additional_evidence_ids: &[String],
    ) -> Result<crate::contracts::ContractValidationContext> {
        let mut tradable_instruments = std::collections::HashSet::new();
        for instrument in &proposal.instruments {
            if self.broker.supports_instrument(instrument)? {
                tradable_instruments.insert(instrument.trim().to_ascii_uppercase());
            }
        }
        let mut known_evidence_ids = std::collections::HashSet::new();
        if let Some(trace) = retrieval_trace {
            for hit in &trace.hits {
                for id in [&hit.id, &hit.canonical_entity_id, &hit.canonical_event_id] {
                    if !id.trim().is_empty() {
                        known_evidence_ids.insert(id.clone());
                    }
                }
            }
        }
        known_evidence_ids.extend(
            additional_evidence_ids
                .iter()
                .filter(|id| !id.trim().is_empty())
                .cloned(),
        );
        Ok(crate::contracts::ContractValidationContext {
            tradable_instruments,
            known_evidence_ids,
        })
    }

    fn persist_contract_draft(
        &mut self,
        compiled: &crate::contracts::CompiledContract,
        loop_id: Option<&str>,
        causation: Option<&str>,
        retrieval_trace: Option<&RetrievalTrace>,
        additional_evidence_ids: &[String],
        independently_fresh_evidence_ids: &[String],
    ) -> Result<String> {
        let thesis_event = event(
            &compiled.thesis.created_by_event_id,
            &compiled.thesis.run_id,
            loop_id,
            "thesis_version_created",
            "thesis_version",
            &compiled.thesis.id,
            causation,
            "HypothesisContract compiler created a thesis projection",
            json!({"record":compiled.thesis}),
        );
        self.store.record_thesis(&compiled.thesis, &thesis_event)?;
        let definition_event = event(
            &compiled.context_definition.created_by_event_id,
            &compiled.context_definition.run_id,
            loop_id,
            "context_definition_created",
            "context_definition",
            &compiled.context_definition.id,
            Some(&thesis_event.id),
            "HypothesisContract compiler created a context projection",
            json!({"record":compiled.context_definition}),
        );
        self.store
            .record_context_definition(&compiled.context_definition, &definition_event)?;
        let context_event = event(
            &compiled.context.created_by_event_id,
            &compiled.context.run_id,
            loop_id,
            "context_version_created",
            "context_version",
            &compiled.context.id,
            Some(&definition_event.id),
            "HypothesisContract compiler created a versioned context projection",
            json!({"record":compiled.context}),
        );
        self.store
            .record_context_version(&compiled.context, &context_event)?;
        let draft_event = event(
            &compiled.hypothesis.created_by_event_id,
            &compiled.hypothesis.run_id,
            loop_id,
            "hypothesis_version_created",
            "hypothesis_contract",
            &compiled.contract.id,
            Some(&context_event.id),
            "World model proposal persisted as DRAFT before deterministic activation validation",
            json!({"record":compiled.hypothesis,"retrievalTrace":retrieval_trace,
                "additionalEvidenceIds":additional_evidence_ids,
                "independentlyFreshEvidenceIds":independently_fresh_evidence_ids}),
        );
        self.store
            .record_hypothesis(&compiled.hypothesis, &draft_event)?;
        Ok(draft_event.id)
    }

    fn persist_validated_contract_draft(
        &mut self,
        mut proposal: crate::contracts::HypothesisContractDraft,
        run_id: &str,
        initial_version: u32,
        parent_contract_id: Option<String>,
        parent_hypothesis_id: Option<String>,
        root_hypothesis_id: Option<String>,
        thesis_version: i64,
        context_version: i64,
        provenance: &str,
        loop_id: Option<&str>,
        causation: Option<&str>,
        retrieval_trace: Option<&RetrievalTrace>,
        additional_evidence_ids: &[String],
        independently_fresh_evidence_ids: &[String],
    ) -> Result<(crate::contracts::CompiledContract, String)> {
        let mut parent_contract_id = parent_contract_id;
        let mut parent_hypothesis_id = parent_hypothesis_id;
        let mut root_hypothesis_id = root_hypothesis_id;
        let authoritative_objective = self.store.run_human_thesis(run_id)?;
        for repair_attempt in 0..=2u32 {
            if proposal.user_objective != authoritative_objective {
                bail!("contract proposal objective does not match the canonical run prompt");
            }
            // Every model-authored replacement crosses the same boundary,
            // including results returned by custom WorldModel adapters.
            crate::contracts::normalize_contract_timeframe(&mut proposal, &authoritative_objective)
                .context("contract timeframe normalization failed before persistence")?;
            crate::skills::SkillRegistry::discover_builtin()
                .normalize_and_apply_contract_against_objective(
                    &mut proposal,
                    &authoritative_objective,
                )
                .context("contract lifecycle normalization failed before persistence")?;
            normalize_contract_evidence_citations(
                &mut proposal,
                retrieval_trace,
                additional_evidence_ids,
            );
            let version = initial_version
                .checked_add(repair_attempt)
                .context("contract version exceeds supported range during bounded repair")?;
            let version_offset = i64::from(repair_attempt);
            let compiled = crate::contracts::compile_contract(
                proposal.clone(),
                run_id,
                version,
                parent_contract_id.clone(),
                parent_hypothesis_id.clone(),
                root_hypothesis_id.clone(),
                thesis_version
                    .checked_add(version_offset)
                    .context("thesis version exceeds supported range during repair")?,
                context_version
                    .checked_add(version_offset)
                    .context("context version exceeds supported range during repair")?,
                provenance,
                Utc::now(),
            );
            let draft_event_id = self.persist_contract_draft(
                &compiled,
                loop_id,
                causation,
                retrieval_trace,
                additional_evidence_ids,
                independently_fresh_evidence_ids,
            )?;
            match self.validated_contract_for_activation(
                &compiled,
                retrieval_trace,
                additional_evidence_ids,
                independently_fresh_evidence_ids,
            ) {
                Ok(_) => return Ok((compiled, draft_event_id)),
                Err(error) => {
                    self.reject_persisted_contract(&compiled, &draft_event_id, loop_id, &error)?;
                    if repair_attempt == 2 {
                        return Err(error.context("HypothesisContract remained invalid after two bounded repair attempts and stronger-model escalation"));
                    }
                    let escalated = repair_attempt == 1;
                    let repair = self
                        .world_model
                        .repair_contract(
                            &proposal.user_objective,
                            &proposal,
                            &format!("{error:#}"),
                            escalated,
                            retrieval_trace,
                        )
                        .with_context(|| {
                            format!(
                        "world-model {} repair failed after deterministic contract validation",
                        if escalated { "escalation" } else { "base-model" },
                    )
                        })?;
                    parent_contract_id = Some(compiled.contract.id.clone());
                    parent_hypothesis_id = Some(compiled.hypothesis.id.clone());
                    root_hypothesis_id = Some(compiled.hypothesis.root_hypothesis_id.clone());
                    proposal = repair;
                }
            }
        }
        bail!("bounded contract repair exhausted without a validated proposal")
    }

    fn validated_contract_for_activation(
        &self,
        compiled: &crate::contracts::CompiledContract,
        retrieval_trace: Option<&RetrievalTrace>,
        additional_evidence_ids: &[String],
        independently_fresh_evidence_ids: &[String],
    ) -> Result<crate::contracts::HypothesisContract> {
        let authoritative_objective = self.store.run_human_thesis(&compiled.contract.run_id)?;
        crate::contracts::validate_contract_lifecycle_against_objective(
            &compiled.contract.proposal,
            &authoritative_objective,
        )
        .context("contract lifecycle limits do not match the canonical run prompt")?;
        crate::contracts::validate_contract_timeframe_against_objective(
            &compiled.contract.proposal,
            &authoritative_objective,
        )
        .context("contract timeframe does not match the canonical run prompt")?;
        let validation_context = self.contract_validation_context(
            &compiled.contract.proposal,
            retrieval_trace,
            additional_evidence_ids,
        )?;
        crate::contracts::validate_evidence_freshness(
            &compiled.contract.proposal,
            retrieval_trace,
            independently_fresh_evidence_ids,
            Utc::now(),
        )?;
        let mut contract = compiled.contract.clone();
        crate::contracts::validate_and_activate(&mut contract, &validation_context, Utc::now())?;
        Ok(contract)
    }

    fn preflight_contract_context(
        &self,
        compiled: &crate::contracts::CompiledContract,
        loop_id: &str,
    ) -> Result<ResolvedContextSnapshot> {
        let spec = &compiled.contract.proposal.live_context_spec;
        let request = MarketDataRequest {
            instrument: spec.instrument.clone(),
            series: crate::market_data::requirements(spec)?,
            quote_max_age_seconds: compiled
                .contract
                .proposal
                .context_requirements
                .iter()
                .filter(|requirement| {
                    requirement.value_type == crate::contracts::ContextValueType::Quote
                })
                .map(|requirement| requirement.maximum_age_seconds)
                .min()
                .unwrap_or(15),
        };
        let market = self
            .market_data
            .snapshot(&request)
            .context("required contract market context is not ready")?;
        let resolved = crate::market_data::resolve_snapshot(
            &compiled.contract.run_id,
            loop_id,
            &compiled.thesis.id,
            &compiled.context.id,
            spec,
            market,
        )
        .context("required contract market context could not be resolved")?;
        validate_required_contract_context(
            &compiled.contract.proposal,
            &resolved,
            self.market_data.is_simulated(),
        )?;
        Ok(resolved)
    }

    fn reject_persisted_contract(
        &self,
        compiled: &crate::contracts::CompiledContract,
        draft_event_id: &str,
        loop_id: Option<&str>,
        error: &anyhow::Error,
    ) -> Result<()> {
        let mut rejected = compiled.hypothesis.clone();
        rejected.status = "REJECTED".into();
        if let Some(contract) = rejected.contract.as_mut() {
            contract.state = crate::contracts::ContractState::Rejected;
        }
        let rejected_id = Uuid::new_v4().to_string();
        let rejected_event = event(
            &rejected_id,
            &rejected.run_id,
            loop_id,
            "hypothesis_contract_rejected",
            "hypothesis_contract",
            &compiled.contract.id,
            Some(draft_event_id),
            "Deterministic validation rejected the DRAFT HypothesisContract",
            json!({"record":rejected,"validationError":format!("{error:#}")}),
        );
        self.store.reject_hypothesis(&rejected, &rejected_event)?;
        Ok(())
    }

    fn activate_contract(
        &mut self,
        mut compiled: crate::contracts::CompiledContract,
        draft_event_id: &str,
        loop_id: Option<&str>,
        resolved_context: &ResolvedContextSnapshot,
        retrieval_trace: Option<&RetrievalTrace>,
        additional_evidence_ids: &[String],
        independently_fresh_evidence_ids: &[String],
    ) -> Result<crate::contracts::CompiledContract> {
        let validated = self.validated_contract_for_activation(
            &compiled,
            retrieval_trace,
            additional_evidence_ids,
            independently_fresh_evidence_ids,
        );
        let context_matches_contract = resolved_context.run_id == compiled.contract.run_id
            && resolved_context.thesis_version_id == compiled.thesis.id
            && resolved_context.context_version_id == compiled.context.id
            && resolved_context.freshness_state == "fresh";
        let mut contract = match validated.and_then(|contract| {
            if context_matches_contract {
                Ok(contract)
            } else {
                bail!("resolved required context does not match the DRAFT contract projections or is not fresh")
            }
        }) {
            Ok(contract) => contract,
            Err(error) => {
                self.reject_persisted_contract(&compiled, draft_event_id, loop_id, &error)?;
                return Err(error.context("HypothesisContract did not pass deterministic activation validation"));
            }
        };

        compiled.contract = contract.clone();
        compiled.hypothesis.status = "ACTIVE".into();
        compiled.hypothesis.contract = Some(compiled.contract.clone());
        compiled.activated_by_event_id = Uuid::new_v4().to_string();
        let activation_event = event(
            &compiled.activated_by_event_id,
            &compiled.hypothesis.run_id,
            loop_id,
            "hypothesis_contract_activated",
            "hypothesis_contract",
            &compiled.contract.id,
            Some(draft_event_id),
            "Validated contract transitioned from DRAFT to ACTIVE",
            json!({"record":compiled.hypothesis}),
        );
        self.store
            .activate_hypothesis(&compiled.hypothesis, &activation_event)?;
        contract = compiled.contract.clone();
        compiled.contract = contract;
        Ok(compiled)
    }

    fn persist_pending_startup(
        &mut self,
        run_id: &str,
        human_thesis: &str,
        compiled: crate::contracts::CompiledContract,
        draft_event_id: String,
        retrieval_trace: Option<RetrievalTrace>,
        warmup_detail: &str,
    ) -> Result<RunSnapshot> {
        let event_id = Uuid::new_v4().to_string();
        let warmup_event = event(
            &event_id,
            run_id,
            None,
            "live_context_resolution_failed",
            "hypothesis_contract",
            &compiled.contract.id,
            Some(&draft_event_id),
            "HypothesisContract remains DRAFT while required live context warms up",
            json!({"error":warmup_detail,"startupWarmup":true,"jevCalled":false,"contractState":"DRAFT"}),
        );
        self.store.append_event(&warmup_event)?;
        let active = ActiveRun {
            thesis: human_thesis.to_owned(),
            hypotheses: vec![compiled.hypothesis.clone()],
            cadences: Vec::new(),
            thesis_versions: vec![compiled.thesis.clone()],
            context_definition: compiled.context_definition.clone(),
            context_versions: vec![compiled.context.clone()],
            loops: Vec::new(),
            positions: Vec::new(),
            position_controls: Vec::new(),
            failure_states: HashMap::new(),
            last_event_id: event_id,
        };
        self.active_runs.insert(run_id.to_owned(), active);
        self.pending_startup_activations.insert(
            run_id.to_owned(),
            PendingStartupActivation {
                compiled,
                draft_event_id,
                retrieval_trace,
                additional_evidence_ids: Vec::new(),
                independently_fresh_evidence_ids: Vec::new(),
                last_warmup_detail: Some(warmup_detail.to_owned()),
            },
        );
        self.immediate_cycle_runs.insert(run_id.to_owned());
        self.snapshot(run_id)
    }

    pub fn has_pending_startup_activation(&self, run_id: &str) -> bool {
        self.pending_startup_activations.contains_key(run_id)
    }

    pub fn advance_pending_startup_activation(
        &mut self,
        run_id: &str,
    ) -> Result<Option<RunSnapshot>> {
        let Some(pending) = self.pending_startup_activations.get(run_id).cloned() else {
            return Ok(None);
        };
        match self.preflight_contract_context(&pending.compiled, "startup-context-preflight") {
            Err(error) => {
                let detail = format!("{error:#}");
                if pending.last_warmup_detail.as_deref() != Some(detail.as_str()) {
                    let event_id = Uuid::new_v4().to_string();
                    let warmup_event = event(
                        &event_id,
                        run_id,
                        None,
                        "live_context_resolution_failed",
                        "hypothesis_contract",
                        &pending.compiled.contract.id,
                        Some(&pending.draft_event_id),
                        "HypothesisContract remains DRAFT while required live context warms up",
                        json!({"error":detail,"startupWarmup":true,"jevCalled":false,"contractState":"DRAFT"}),
                    );
                    self.store.append_event(&warmup_event)?;
                    if let Some(active) = self.active_runs.get_mut(run_id) {
                        active.last_event_id = event_id;
                    }
                    if let Some(pending) = self.pending_startup_activations.get_mut(run_id) {
                        pending.last_warmup_detail = Some(detail);
                    }
                }
                Ok(Some(self.snapshot(run_id)?))
            }
            Ok(validated_context) => {
                let mut compiled = pending.compiled.clone();
                let authoritative_objective = self.store.run_human_thesis(run_id)?;
                if let Err(error) =
                    normalize_compiled_contract_lifecycle(&mut compiled, &authoritative_objective)
                {
                    self.pending_startup_activations.remove(run_id);
                    let _ = self.stop(run_id)?;
                    return Err(
                        error.context("recovered DRAFT contract lifecycle normalization failed")
                    );
                }
                let activation = self.activate_contract(
                    compiled,
                    &pending.draft_event_id,
                    None,
                    &validated_context,
                    pending.retrieval_trace.as_ref(),
                    &pending.additional_evidence_ids,
                    &pending.independently_fresh_evidence_ids,
                );
                let compiled = match activation {
                    Ok(compiled) => compiled,
                    Err(error) => {
                        self.pending_startup_activations.remove(run_id);
                        let stopped = self.stop(run_id)?;
                        let _ = error;
                        return Ok(Some(stopped));
                    }
                };
                self.pending_startup_activations.remove(run_id);
                let human_thesis = compiled.hypothesis.original_prompt.clone();
                self.commit_initial_contract_loop(run_id, &human_thesis, compiled)?;
                Ok(Some(self.snapshot(run_id)?))
            }
        }
    }

    fn continuation_context(&self, source_run_id: &str) -> Result<serde_json::Value> {
        let source_thesis = self
            .store
            .run_human_thesis(source_run_id)?
            .chars()
            .take(2_000)
            .collect::<String>();
        let source_events = self.store.recent_continuation_events(source_run_id, 96)?;
        let mut selected = source_events.iter().collect::<Vec<_>>();
        if selected.len() > 32 {
            selected.drain(1..selected.len() - 31);
        }
        let mut evidence_ids = Vec::new();
        let records = selected
            .into_iter()
            .map(|stored| {
                let event = &stored.event;
                evidence_ids.push(event.id.clone());
                evidence_ids.push(event.aggregate_id.clone());
                let payload = &event.payload;
                let record = payload.get("record").unwrap_or(&serde_json::Value::Null);
                if let Some(id) = record.get("id").and_then(serde_json::Value::as_str) {
                    evidence_ids.push(id.to_owned());
                }
                let summary = payload
                    .get("summary")
                    .or_else(|| payload.get("detail"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or(&event.kind)
                    .chars()
                    .take(600)
                    .collect::<String>();
                let mut facts = serde_json::Map::new();
                for key in [
                    "humanThesis", "thesis", "strategyMechanism", "instruments", "timeframe",
                    "status", "action", "stage", "confidence", "rationale", "classification",
                    "direction", "state", "symbol", "filledQuantity", "rejectionReasons",
                ] {
                    if let Some(value) = record.get(key).filter(|value| !value.is_null()) {
                        let value = match value {
                            serde_json::Value::String(text) => serde_json::Value::String(
                                text.chars().take(1200).collect::<String>(),
                            ),
                            serde_json::Value::Array(items) => {
                                serde_json::Value::Array(items.iter().take(12).cloned().collect())
                            }
                            _ => value.clone(),
                        };
                        facts.insert(key.into(), value);
                    }
                }
                if facts.is_empty() {
                    if let Some(thesis) = payload.get("humanThesis") {
                        facts.insert("humanThesis".into(), thesis.clone());
                    }
                    if let Some(result) = payload.get("result") {
                        facts.insert("result".into(), result.clone());
                    }
                }
                if event.kind == "context_record_ingested" {
                    for key in ["title", "text", "sourceClass", "trustLevel", "provenanceUri"] {
                        if let Some(value) = record.get(key).filter(|value| !value.is_null()) {
                            let value = value
                                .as_str()
                                .map(|text| serde_json::Value::String(
                                    text.chars().take(if key == "text" { 1400 } else { 300 }).collect(),
                                ))
                                .unwrap_or_else(|| value.clone());
                            facts.insert(key.into(), value);
                        }
                    }
                }
                if let Some(proposal) = record
                    .get("contract")
                    .and_then(|contract| contract.get("proposal"))
                {
                    let mut contract_summary = serde_json::Map::new();
                    for key in [
                        "thesis", "mechanism", "expectedBehavior", "timeframe",
                        "supportingEvidenceIds", "contradictoryEvidenceIds", "invalidationConditions",
                        "reviewTriggers", "stopLimits", "jev1Objective", "jev2Objective",
                    ] {
                        if let Some(value) = proposal.get(key).filter(|value| !value.is_null()) {
                            let value = match value {
                                serde_json::Value::String(text) => serde_json::Value::String(
                                    text.chars().take(1200).collect::<String>(),
                                ),
                                serde_json::Value::Array(items) => {
                                    serde_json::Value::Array(items.iter().take(8).cloned().collect())
                                }
                                _ => value.clone(),
                            };
                            contract_summary.insert(key.into(), value);
                        }
                    }
                    if !contract_summary.is_empty() {
                        facts.insert("contractSummary".into(), serde_json::Value::Object(contract_summary));
                    }
                }
                json!({
                    "eventId":event.id,
                    "canonicalId":event.aggregate_id,
                    "kind":event.kind,
                    "occurredAt":event.occurred_at,
                    "summary":summary,
                    "facts":facts
                })
            })
            .collect::<Vec<_>>();
        evidence_ids.sort();
        evidence_ids.dedup();
        Ok(json!({
            "sourceRunId":source_run_id,
            "sourceObjective":source_thesis,
            "historicalEvidenceOnly":true,
            "records":records,
            "availableEvidenceIds":evidence_ids,
            "instruction":"Use this as historical context for the new objective and reason freely from it. Listed IDs are optional citation aids; do not force citations. Espeon will keep citations it can resolve and discard unavailable IDs without rejecting the proposal. Historical prices, account values, positions, and orders are not current execution context."
        }))
    }

    fn start_with_id(
        &mut self,
        human_thesis: &str,
        run_id: String,
        source_run_id: Option<&str>,
        continuation_context: Option<serde_json::Value>,
    ) -> Result<RunSnapshot> {
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
            json!({ "humanThesis": human_thesis, "internalRetrievalAvailable": self.context_pool.is_some(), "continuedFromRunId":source_run_id }),
        );
        self.store.create_run(&run_id, human_thesis, &run_event)?;

        let startup = match self.world_model.formulate_with_continuation(
            &run_id,
            human_thesis,
            self.context_pool
                .as_ref()
                .map(|pool| pool as &dyn ContextRetriever),
            continuation_context.as_ref(),
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
                    json!({"stoppedAt":stopped_at,"startupError":format!("{error:#}")}),
                );
                self.store.stop_run(&run_id, stopped_at, &stop_event)?;
                return Err(error.context("world model could not formulate the run"));
            }
        };
        let WorldModelStartupOutput {
            contract: contract_draft,
            retrieval_trace: startup_retrieval_trace,
            web_research_unavailable,
            broker_context_unavailable,
        } = startup;
        let mut formulation_causation_event_id = run_event_id.clone();
        if web_research_unavailable {
            let research_event_id = Uuid::new_v4().to_string();
            let research_event = event(
                &research_event_id,
                &run_id,
                None,
                "external_research_unavailable",
                "world_model",
                &run_id,
                Some(&run_event_id),
                "World model continued formulation without requested external research",
                json!({
                    "status": "temporarily_unavailable",
                    "formulationContinued": true,
                    "stateChangingReview": "held_when_external_evidence_is_required"
                }),
            );
            self.store.append_event(&research_event)?;
            formulation_causation_event_id = research_event_id;
        }
        if let Some(failure) = broker_context_unavailable {
            let broker_event_id = Uuid::new_v4().to_string();
            let broker_event = event(
                &broker_event_id,
                &run_id,
                None,
                "broker_context_unavailable",
                "world_model",
                &run_id,
                Some(&formulation_causation_event_id),
                "World model continued formulation after a failed cTrader MCP read",
                json!({
                    "requestedCapabilities": failure.requested_capabilities,
                    "error": failure.error,
                    "recoveryReason": failure.recovery_reason,
                    "recoveryAttempted": true,
                    "formulationContinued": true
                }),
            );
            self.store.append_event(&broker_event)?;
            formulation_causation_event_id = broker_event_id;
        }
        let continuation_evidence_ids = continuation_context
            .as_ref()
            .and_then(|context| context["availableEvidenceIds"].as_array())
            .map(|ids| ids.iter().filter_map(|id| id.as_str().map(str::to_owned)).collect::<Vec<_>>())
            .unwrap_or_default();
        let (compiled, draft_event_id) = self.persist_validated_contract_draft(
            contract_draft,
            &run_id,
            1,
            None,
            None,
            None,
            1,
            1,
            "world-model HypothesisContract",
            None,
            Some(&formulation_causation_event_id),
            startup_retrieval_trace.as_ref(),
            &continuation_evidence_ids,
            &[],
        )?;
        let activation_context =
            match self.preflight_contract_context(&compiled, "startup-context-preflight") {
                Ok(context) => context,
                Err(error) => {
                    return self.persist_pending_startup(
                        &run_id,
                        human_thesis,
                        compiled,
                        draft_event_id,
                        startup_retrieval_trace,
                        &format!("{error:#}"),
                    )
                }
            };
        let compiled = self.activate_contract(
            compiled,
            &draft_event_id,
            None,
            &activation_context,
            startup_retrieval_trace.as_ref(),
            &continuation_evidence_ids,
            &[],
        )?;
        self.commit_initial_contract_loop(&run_id, human_thesis, compiled)?;
        if let Some(source_run_id) = source_run_id {
            let continued_event_id = Uuid::new_v4().to_string();
            let continued_event = event(
                &continued_event_id,
                &run_id,
                None,
                "run_continued_from",
                "run",
                &run_id,
                Some(&run_event_id),
                "New run was formulated with history from a prior run",
                json!({"sourceRunId":source_run_id,"sourceRunRemainsUnchanged":true,"carriedEvidenceIds":continuation_evidence_ids}),
            );
            self.store.append_event(&continued_event)?;
            if let Some(active) = self.active_runs.get_mut(&run_id) {
                active.last_event_id = continued_event_id;
            }
        }
        self.snapshot(&run_id)
    }

    fn spawn_loop_for_active_contract(
        &self,
        hypothesis: &HypothesisDefinition,
        activation_event_id: &str,
        loop_state: &LoopView,
        loop_event: &HarnessEvent,
    ) -> Result<()> {
        let contract = hypothesis.contract.as_ref();
        if hypothesis.status != "ACTIVE"
            || contract.map(|item| item.state) != Some(crate::contracts::ContractState::Active)
            || contract.map(|item| item.id.as_str()) != Some(hypothesis.id.as_str())
            || contract.map(|item| item.run_id.as_str()) != Some(hypothesis.run_id.as_str())
        {
            bail!("Jev loop creation requires a validated ACTIVE HypothesisContract");
        }
        if loop_state.run_id != hypothesis.run_id
            || loop_state.hypothesis_id != hypothesis.id
            || loop_event.run_id != loop_state.run_id
            || loop_event.aggregate_id != loop_state.id
            || loop_event.kind != "loop_spawned"
            || loop_event.causation_event_id.as_deref() != Some(activation_event_id)
            || loop_event.payload.get("record") != Some(&serde_json::to_value(loop_state)?)
        {
            bail!("Jev loop creation does not match its ACTIVE contract and activation event");
        }
        self.store.spawn_loop(loop_state, loop_event)?;
        Ok(())
    }

    fn commit_initial_contract_loop(
        &mut self,
        run_id: &str,
        human_thesis: &str,
        compiled: crate::contracts::CompiledContract,
    ) -> Result<()> {
        let thesis = compiled.thesis;
        let context_definition = compiled.context_definition;
        let context = compiled.context;
        let hypothesis = compiled.hypothesis;
        let activation_event_id = compiled.activated_by_event_id;

        let primary_loop_event_id = Uuid::new_v4().to_string();
        let primary_loop = LoopView {
            id: Uuid::new_v4().to_string(),
            run_id: run_id.to_owned(),
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
            Some(&activation_event_id),
            "Primary Jev loop spawned",
            json!({ "record": primary_loop }),
        );
        self.spawn_loop_for_active_contract(
            &hypothesis,
            &activation_event_id,
            &primary_loop,
            &primary_loop_event,
        )?;

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
            run_id: run_id.to_owned(),
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

        self.active_runs.insert(
            run_id.to_owned(),
            ActiveRun {
                thesis: human_thesis.to_owned(),
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
        // The first Jev and broker cycle runs in the supervised worker after
        // Start returns, so failures cannot strand an unregistered position.
        self.immediate_cycle_runs.insert(run_id.to_owned());
        Ok(())
    }

    #[allow(dead_code)]
    fn current_account_open_exposure(&self) -> Result<f64> {
        self.current_account_open_exposure_in_deposit_currency(
            None,
            &self.risk.config.paper_account_currency,
        )
    }

    fn current_account_open_exposure_in_deposit_currency(
        &self,
        snapshot: Option<&crate::ports::BrokerRiskSnapshot>,
        static_account_currency: &str,
    ) -> Result<f64> {
        let mut broker_position_ids = std::collections::HashSet::new();
        let mut local_position_ids = std::collections::HashSet::new();
        let mut exposure = 0.0;

        for (run_id, active) in &self.active_runs {
            for position in active
                .positions
                .iter()
                .filter(|position| position_may_have_exposure(position))
            {
                if let Some(broker_position_id) = position.broker_position_id.as_deref() {
                    if !broker_position_ids.insert(broker_position_id) {
                        bail!("broker position {broker_position_id} is attributed to multiple active runs; account exposure is ambiguous");
                    }
                } else if !local_position_ids.insert(position.id.as_str()) {
                    bail!("local position {} is duplicated across active runs; account exposure is ambiguous", position.id);
                }

                let control = active
                    .position_controls
                    .iter()
                    .find(|control| control.position_id == position.id)
                    .with_context(|| {
                        format!(
                            "open position {} in run {run_id} has no risk control record",
                            position.id
                        )
                    })?;
                if !control.notional.is_finite() || control.notional <= 0.0 {
                    bail!(
                        "open position {} in run {run_id} has invalid notional exposure",
                        position.id
                    );
                }
                let notional = if let Some(snapshot) = snapshot {
                    let normalized_instrument = normalize_instrument_code(&control.instrument);
                    let mut matching_rates = snapshot
                        .quote_to_deposit
                        .iter()
                        .filter(|(instrument, _)| {
                            normalize_instrument_code(instrument) == normalized_instrument
                        })
                        .map(|(_, rate)| *rate);
                    let conversion = matching_rates
                        .next()
                        .filter(|rate| rate.is_finite() && *rate > 0.0)
                        .filter(|rate| {
                            matching_rates.all(|other| {
                                other.is_finite()
                                    && other > 0.0
                                    && (other - *rate).abs() <= f64::EPSILON * rate.abs().max(1.0)
                            })
                        })
                        .with_context(|| {
                            format!(
                                "no unique quote-to-deposit conversion for open position {} ({})",
                                position.id, control.instrument
                            )
                        })?;
                    control.notional * conversion
                } else {
                    let quote_asset = crate::risk::instrument_quote_asset(&control.instrument)
                        .with_context(|| {
                            format!(
                                "cannot determine quote asset for simulated position {} ({})",
                                position.id, control.instrument
                            )
                        })?;
                    if !quote_asset.eq_ignore_ascii_case(static_account_currency) {
                        bail!(
                            "simulated account currency {static_account_currency} cannot value {} exposure in {quote_asset}",
                            control.instrument
                        );
                    }
                    let mark = self
                        .broker
                        .reference_price(&control.instrument)?
                        .filter(|price| price.is_finite() && *price > 0.0)
                        .with_context(|| {
                            format!(
                                "current broker mark unavailable for open position {} ({})",
                                position.id, control.instrument
                            )
                        })?;
                    control.quantity * mark
                };
                exposure += notional;
                if !exposure.is_finite() {
                    bail!("account-wide open exposure is non-finite");
                }
            }
        }

        Ok(exposure)
    }

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
        if let Some(reason) = self.recovery_blocked_runs.get(run_id).cloned() {
            return match self.stop(run_id) {
                Ok(snapshot) => {
                    self.recovery_blocked_runs.remove(run_id);
                    Ok(snapshot)
                }
                Err(error) => Err(error.context(format!(
                    "recovery-blocked run is awaiting fail-safe stop: {reason}"
                ))),
            };
        }
        if self.pending_startup_activations.contains_key(run_id) {
            return self.snapshot(run_id);
        }
        for (service, detail) in self.market_data.drain_health_events() {
            let event_id = Uuid::new_v4().to_string();
            let health_event = event(
                &event_id,
                run_id,
                None,
                "market_service_state_changed",
                "market_service",
                &service,
                None,
                &format!("Market service {service}: {detail}"),
                json!({"service":service,"detail":detail}),
            );
            self.store.append_event(&health_event)?;
        }
        // Mark deterministic lifecycle caps before connector checks so the
        // loop cannot continue to Jev. Final stop completion waits for broker
        // reconciliation to prove that all exposure has been found.
        self.enforce_contract_stop_limits(run_id)?;
        let broker_state_ready = self.reconcile_broker(run_id)?;
        ensure_cycle_active(cancellation)?;
        if !broker_state_ready {
            return self.snapshot(run_id);
        }
        self.complete_pending_contract_stops(run_id)?;
        if self.stop_run_if_all_loops_stopped(run_id)? {
            return self.run_snapshot(run_id);
        }
        let pending_review_stops = self
            .active_runs
            .get(run_id)
            .ok_or_else(|| anyhow::anyhow!("run is not active"))?
            .loops
            .iter()
            .filter(|loop_state| loop_state.state == "review-stop-pending")
            .map(|loop_state| loop_state.id.clone())
            .collect::<Vec<_>>();
        for loop_id in pending_review_stops {
            self.complete_pending_review_stop(run_id, &loop_id)?;
        }
        if self.stop_run_if_all_loops_stopped(run_id)? {
            return self.run_snapshot(run_id);
        }
        if self.active_runs.get(run_id).is_some_and(|active| {
            active
                .loops
                .iter()
                .any(|loop_state| loop_state.state == "run-stop-pending")
        }) {
            return self.stop(run_id);
        }
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
                if item.state == "stopped"
                    || item.state.starts_with("review-")
                    || item.state == "run-stop-pending"
                    || item.state == "contract-stop-pending"
                {
                    return false;
                }
                let contract_is_active = self
                    .active_runs
                    .get(run_id)
                    .and_then(|active| {
                        active
                            .hypotheses
                            .iter()
                            .find(|hypothesis| hypothesis.id == item.hypothesis_id)
                    })
                    .is_some_and(|hypothesis| {
                        hypothesis.status == "ACTIVE"
                            && hypothesis.contract.as_ref().is_some_and(|contract| {
                                contract.state == crate::contracts::ContractState::Active
                            })
                    });
                if !contract_is_active {
                    return false;
                }
                let loop_is_flat = !self
                    .active_runs
                    .get(run_id)
                    .map(|active| {
                        active.positions.iter().any(|position| {
                            position.loop_id == item.id && position_may_have_exposure(position)
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
            let (mut loop_state, hypothesis, thesis, context, position, user_objective, causation) = {
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
                if hypothesis.status != "ACTIVE"
                    || hypothesis.contract.as_ref().map(|contract| contract.state)
                        != Some(crate::contracts::ContractState::Active)
                {
                    bail!(
                        "Jev cycle rejected: loop does not reference an ACTIVE validated contract"
                    );
                }
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
                    .find(|item| item.loop_id == loop_id && position_may_have_exposure(item))
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
                    active.thesis.clone(),
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
                if let Some(price) = self.broker.reference_price(&control.instrument)? {
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
            let resolved_context = match self.resolve_live_jev_state(
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
            let active_contract = hypothesis
                .contract
                .as_ref()
                .context("ACTIVE Jev loop is missing its HypothesisContract")?;
            let compiled_jev = match crate::jev_compiler::compile(active_contract, resolved_context)
            {
                Ok(compiled) => compiled,
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
            let resolved = compiled_jev.state;
            let jev1_question = format!(
                "The user's original trading instruction is authoritative and must be followed. If it explicitly restricts direction, instrument, or other entry conditions, do not contradict it. Original instruction: {}\n\nValidated loop entry objective: {}",
                user_objective, compiled_jev.jev1_question
            );
            let jev2_question = compiled_jev.jev2_question;
            let cycle_reference_price = resolved
                .live_context_snapshot
                .as_ref()
                .map(|snapshot| snapshot.quote.mid)
                .context("resolved live context omitted its quote envelope")?;
            let decision_event_id = Uuid::new_v4().to_string();
            let mut last_event_id = decision_event_id.clone();
            if let Some(mut position) = position {
                let management = match self.jev.manage_position(&resolved, &jev2_question) {
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
                let entry = match self.jev.decide_entry(&resolved, &jev1_question) {
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
                let instrument = hypothesis
                    .instruments
                    .first()
                    .map(String::as_str)
                    .unwrap_or("UNSPECIFIED");
                let mut risk_instruments = vec![instrument.to_owned()];
                for active in self.active_runs.values() {
                    for position in active
                        .positions
                        .iter()
                        .filter(|position| position_may_have_exposure(position))
                    {
                        if let Some(control) = active
                            .position_controls
                            .iter()
                            .find(|control| control.position_id == position.id)
                        {
                            risk_instruments.push(control.instrument.clone());
                        }
                    }
                }
                let mut requested_instruments = std::collections::HashMap::new();
                for requested in risk_instruments {
                    requested_instruments
                        .entry(normalize_instrument_code(&requested))
                        .or_insert(requested);
                }
                let mut risk_instruments = requested_instruments.into_values().collect::<Vec<_>>();
                risk_instruments.sort();
                let (mut broker_risk_snapshot, mut broker_risk_snapshot_error) =
                    match self.broker.risk_snapshot(&risk_instruments) {
                        Ok(Some(snapshot)) => (Some(snapshot), None),
                        Ok(None) => (self.manual_risk_snapshot.take(), None),
                        Err(error) => (
                            self.manual_risk_snapshot.take(),
                            Some(format!("broker risk snapshot failed: {error:#}")),
                        ),
                    };
                if let Some(snapshot) = broker_risk_snapshot.as_ref() {
                    let identity_matches =
                        self.broker.risk_account_identity().is_some_and(|expected| {
                            snapshot.account_id.trim() == expected.account_id.trim()
                                && snapshot
                                    .environment
                                    .trim()
                                    .eq_ignore_ascii_case(expected.environment.trim())
                        });
                    if !identity_matches {
                        broker_risk_snapshot_error = Some(
                            "broker risk snapshot account or environment differs from configured FIX identity".into(),
                        );
                        broker_risk_snapshot = None;
                    }
                }
                let (current_total_exposure, open_positions_in_loop) = {
                    let active = self.active_runs.get(run_id).unwrap();
                    let count = active
                        .positions
                        .iter()
                        .filter(|position| {
                            position.loop_id == loop_id && position_may_have_exposure(position)
                        })
                        .count();
                    if let Some(snapshot) = broker_risk_snapshot.as_ref() {
                        if !snapshot.account_open_exposure.is_finite()
                            || snapshot.account_open_exposure < 0.0
                        {
                            broker_risk_snapshot_error =
                                Some("broker-wide open exposure is missing or invalid".into());
                            broker_risk_snapshot = None;
                            (0.0, count)
                        } else {
                            (snapshot.account_open_exposure, count)
                        }
                    } else {
                        match self.current_account_open_exposure_in_deposit_currency(
                            None,
                            &self.risk.config.paper_account_currency,
                        ) {
                            Ok(exposure) => (exposure, count),
                            Err(error) => {
                                broker_risk_snapshot_error = Some(format!(
                                    "account-wide exposure could not be converted safely: {error:#}"
                                ));
                                (0.0, count)
                            }
                        }
                    }
                };
                let side = match entry.action {
                    Jev1Action::Long => "BUY",
                    Jev1Action::Short => "SELL",
                    Jev1Action::NoTrade => "NO_TRADE",
                };
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
                let verified_volume_limits =
                    self.manual_entry_volume_limits
                        .take()
                        .filter(|(symbol, _, _)| {
                            normalize_instrument_code(symbol)
                                == normalize_instrument_code(instrument)
                        });
                let (broker_volume_minimum, broker_volume_step) = match verified_volume_limits {
                    Some((_, minimum, step)) => (Some(minimum), Some(step)),
                    None => (None, None),
                };
                let demo_fixed_quantity = self.broker.demo_fixed_entry_quantity(instrument);
                let (order, gate) = self.risk.evaluate_entry(EntryRiskInput {
                    run_id,
                    loop_id: &loop_id,
                    decision_id: &record.id,
                    action: entry.action.clone(),
                    direction_constraint: explicit_direction_constraint(&user_objective),
                    confidence: entry.confidence,
                    signal_at: record.created_at,
                    instrument,
                    reference_price,
                    allocated_fraction: loop_state.allocated_fraction,
                    current_total_exposure,
                    open_positions_in_loop,
                    duplicate: self.store.order_exists(&duplicate_key)?,
                    minimum_confidence: self.minimum_confidence,
                    broker_volume_minimum,
                    broker_volume_step,
                    demo_fixed_quantity,
                    allow_demo_minimum_step: self.manual_demo_minimum_step.as_deref().is_some_and(
                        |approved_symbol| {
                            normalize_instrument_code(approved_symbol)
                                == normalize_instrument_code(instrument)
                        },
                    ),
                    broker_risk_snapshot: broker_risk_snapshot.as_ref(),
                    broker_risk_snapshot_error: broker_risk_snapshot_error.as_deref(),
                    allow_static_risk_policy: self.broker.allows_static_risk_policy(),
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

    fn enforce_contract_stop_limits(&mut self, run_id: &str) -> Result<()> {
        let state = replay::replay_run(&self.store, run_id)?;
        let now = Utc::now();
        let due = state
            .loops
            .iter()
            .filter(|loop_state| {
                !matches!(
                    loop_state.state.as_str(),
                    "stopped"
                        | "contract-stop-pending"
                        | "review-stop-pending"
                        | "review-modify-pending"
                        | "run-stop-pending"
                )
            })
            .filter_map(|loop_state| {
                let hypothesis = state
                    .hypotheses
                    .iter()
                    .find(|hypothesis| hypothesis.id == loop_state.hypothesis_id)?;
                let contract = hypothesis.contract.as_ref()?;
                if contract.state != crate::contracts::ContractState::Active {
                    return None;
                }
                let completed_trades = state
                    .trade_outcomes
                    .iter()
                    .filter(|outcome| outcome.loop_id == loop_state.id)
                    .count();
                let elapsed_seconds = now
                    .signed_duration_since(loop_state.created_at)
                    .num_seconds()
                    .max(0) as u64;
                let mut reasons = Vec::new();
                if contract
                    .proposal
                    .stop_limits
                    .maximum_elapsed_seconds
                    .is_some_and(|limit| elapsed_seconds >= limit)
                    || contract
                        .proposal
                        .expires_at
                        .is_some_and(|expiry| now >= expiry)
                {
                    reasons.push("maximum_elapsed_time".to_owned());
                }
                if contract
                    .proposal
                    .stop_limits
                    .maximum_completed_trades
                    .is_some_and(|limit| completed_trades >= limit as usize)
                {
                    reasons.push("maximum_completed_trades".to_owned());
                }
                (!reasons.is_empty()).then(|| {
                    (
                        loop_state.clone(),
                        reasons,
                        elapsed_seconds,
                        completed_trades,
                    )
                })
            })
            .collect::<Vec<_>>();

        for (mut loop_state, reasons, elapsed_seconds, completed_trades) in due {
            if loop_state.state != "contract-stop-pending" {
                let trigger_id = Uuid::new_v4().to_string();
                let trigger = event(
                    &trigger_id,
                    run_id,
                    Some(&loop_state.id),
                    "contract_stop_limit_triggered",
                    "hypothesis_contract",
                    &loop_state.hypothesis_id,
                    Some(&loop_state.created_by_event_id),
                    "Deterministic lifecycle skill limit reached; block new Jev decisions",
                    json!({
                        "reasons": reasons,
                        "elapsedSeconds": elapsed_seconds,
                        "completedTrades": completed_trades,
                        "observedAt": now
                    }),
                );
                self.store.append_event(&trigger)?;
                loop_state.state = "contract-stop-pending".into();
                let transition_id = Uuid::new_v4().to_string();
                let pending_event = event(
                    &transition_id,
                    run_id,
                    Some(&loop_state.id),
                    "loop_state_transitioned",
                    "loop",
                    &loop_state.id,
                    Some(&trigger_id),
                    "Contract stop is flattening any open exposure before loop closure",
                    json!({"record":loop_state}),
                );
                self.store.transition_loop(&loop_state, &pending_event)?;
                if let Some(active) = self.active_runs.get_mut(run_id) {
                    if let Some(existing) = active
                        .loops
                        .iter_mut()
                        .find(|item| item.id == loop_state.id)
                    {
                        *existing = loop_state.clone();
                    }
                    active.last_event_id = transition_id;
                }
            }
        }
        Ok(())
    }

    fn complete_pending_contract_stops(&mut self, run_id: &str) -> Result<()> {
        let pending = self
            .active_runs
            .get(run_id)
            .context("run is not active")?
            .loops
            .iter()
            .filter(|loop_state| loop_state.state == "contract-stop-pending")
            .map(|loop_state| loop_state.id.clone())
            .collect::<Vec<_>>();
        for loop_id in pending {
            self.close_positions_for_lifecycle(run_id, Some(&loop_id))?;
            self.materialize_trade_outcomes(run_id)?;
            let active = self.active_runs.get(run_id).context("run is not active")?;
            if active
                .positions
                .iter()
                .any(|position| position.loop_id == loop_id && position_may_have_exposure(position))
            {
                bail!("contract stop for loop {loop_id} cannot finalize while position exposure remains");
            }
            let mut loop_state = active
                .loops
                .iter()
                .find(|item| item.id == loop_id)
                .cloned()
                .context("pending contract stop references a missing loop")?;
            if loop_state.state != "contract-stop-pending" {
                bail!("pending contract stop loop changed state before flattening completed");
            }
            let trigger_id = self
                .store
                .stored_events(run_id)?
                .into_iter()
                .rev()
                .find(|stored| {
                    stored.event.kind == "loop_state_transitioned"
                        && stored.event.aggregate_id == loop_id
                        && stored.event.payload["record"]["state"] == "contract-stop-pending"
                })
                .map(|stored| stored.event.id)
                .context("pending contract stop has no canonical transition")?;
            let stopped_at = Utc::now();
            loop_state.state = "stopped".into();
            loop_state.stopped_at = Some(stopped_at);
            let stopped_id = Uuid::new_v4().to_string();
            let stopped_event = event(
                &stopped_id,
                run_id,
                Some(&loop_id),
                "loop_stopped",
                "loop",
                &loop_id,
                Some(&trigger_id),
                "Loop stopped after broker reconciliation and confirmed exposure flattening",
                json!({"record":loop_state,"recovery":"contract-stop-completed"}),
            );
            self.store.stop_loop(&loop_id, stopped_at, &stopped_event)?;
            let active = self.active_runs.get_mut(run_id).unwrap();
            if let Some(existing) = active.loops.iter_mut().find(|item| item.id == loop_id) {
                *existing = loop_state;
            }
            active.last_event_id = stopped_id;
        }
        Ok(())
    }

    /// Run deadline/trade-count stop checks immediately after the scheduler
    /// wakes, before a market refresh can delay the stop path. The regular
    /// cycle check remains as a second guard after broker reconciliation.
    pub fn enforce_due_contract_stops(&mut self, run_id: &str) -> Result<()> {
        if !self.active_runs.contains_key(run_id) {
            bail!("run is not active");
        }
        self.enforce_contract_stop_limits(run_id)?;
        let pending_contract = self
            .active_runs
            .get(run_id)
            .unwrap()
            .loops
            .iter()
            .any(|loop_state| loop_state.state == "contract-stop-pending");
        let pending_review = self
            .active_runs
            .get(run_id)
            .unwrap()
            .loops
            .iter()
            .any(|loop_state| loop_state.state == "review-stop-pending");
        if pending_contract || pending_review {
            if !self.reconcile_broker(run_id)? {
                bail!("lifecycle stop remains pending until a complete broker reconciliation is available");
            }
            self.complete_pending_contract_stops(run_id)?;
            let pending_review_stops = self
                .active_runs
                .get(run_id)
                .unwrap()
                .loops
                .iter()
                .filter(|loop_state| loop_state.state == "review-stop-pending")
                .map(|loop_state| loop_state.id.clone())
                .collect::<Vec<_>>();
            for loop_id in pending_review_stops {
                self.complete_pending_review_stop(run_id, &loop_id)?;
            }
        } else if self
            .active_runs
            .get(run_id)
            .unwrap()
            .loops
            .iter()
            .any(|loop_state| loop_state.state == "run-stop-pending")
        {
            self.stop(run_id)?;
            return Ok(());
        }
        self.stop_run_if_all_loops_stopped(run_id)?;
        Ok(())
    }

    pub fn seconds_until_contract_stop(&self, run_id: &str) -> Result<Option<u64>> {
        let active = self.active_runs.get(run_id).context("run is not active")?;
        let now = Utc::now();
        let mut nearest: Option<u64> = None;
        for loop_state in active.loops.iter().filter(|item| item.state != "stopped") {
            let Some(contract) = active
                .hypotheses
                .iter()
                .find(|hypothesis| hypothesis.id == loop_state.hypothesis_id)
                .and_then(|hypothesis| hypothesis.contract.as_ref())
                .filter(|contract| contract.state == crate::contracts::ContractState::Active)
            else {
                continue;
            };
            let stop_at = contract
                .proposal
                .expires_at
                .into_iter()
                .chain(
                    contract
                        .proposal
                        .stop_limits
                        .maximum_elapsed_seconds
                        .map(|seconds| {
                            loop_state.created_at + chrono::Duration::seconds(seconds as i64)
                        }),
                )
                .min();
            if let Some(stop_at) = stop_at {
                let seconds = stop_at.signed_duration_since(now).num_seconds().max(0) as u64;
                nearest = Some(nearest.map_or(seconds, |previous| previous.min(seconds)));
            }
        }
        Ok(nearest)
    }

    fn stop_run_if_all_loops_stopped(&mut self, run_id: &str) -> Result<bool> {
        if self.pending_startup_activations.contains_key(run_id) {
            return Ok(false);
        }
        let all_stopped = self
            .active_runs
            .get(run_id)
            .context("run is not active")?
            .loops
            .iter()
            .all(|loop_state| loop_state.state == "stopped");
        if !all_stopped {
            return Ok(false);
        }
        self.stop(run_id)?;
        Ok(true)
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
            let contract_triggers = replayed
                .hypotheses
                .iter()
                .find(|hypothesis| hypothesis.id == loop_state.hypothesis_id)
                .and_then(|hypothesis| hypothesis.contract.as_ref())
                .map(|contract| &contract.proposal.review_triggers);
            let no_trade_threshold = contract_triggers
                .map(|triggers| triggers.no_trade_decisions as usize)
                .unwrap_or_else(|| no_trade_review_threshold(cadence, &self.review_policy));
            let loss_threshold = contract_triggers
                .map(|triggers| triggers.consecutive_losses as usize)
                .unwrap_or(self.review_policy.consecutive_loss_threshold);
            let periodic_trade_threshold = contract_triggers
                .map(|triggers| triggers.completed_trades as usize)
                .unwrap_or(self.review_policy.periodic_trade_threshold);
            let mut kinds = Vec::new();
            if consecutive_losses >= loss_threshold {
                kinds.push(AutonomousReviewTriggerKind::LossStreak);
            }
            if no_trade_streak >= no_trade_threshold {
                kinds.push(AutonomousReviewTriggerKind::NoTradeStreak);
            }
            if outcomes.len() >= periodic_trade_threshold {
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
                loss_threshold,
                completed_trades_since_review: outcomes.len(),
                periodic_trade_threshold,
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
            &format!(
                "Fresh market observations resolved the versioned live-context formulas ({})",
                snapshot.quality_state
            ),
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
                json!({"record":decision,"reviewPackageId":job.package.id,"reviewPackageEventId":job.package.created_by_event_id,"actionAccepted":false}),
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
        if matches!(decision.action, HypothesisAction::Modify) {
            let proposal = decision
                .proposed_contract
                .as_ref()
                .context("MODIFY rejected: a complete HypothesisContract proposal is required")?;
            if proposal.mechanism.trim().is_empty() || proposal.timeframe.horizon_minutes == 0 {
                bail!("MODIFY rejected: replacement mechanism and timeframe are required");
            }
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

    fn restore_parent_loop_after_failed_modify(
        &mut self,
        run_id: &str,
        parent_loop: &LoopView,
        causation: &str,
    ) -> Result<()> {
        let restored = parent_loop.clone();
        let restore_event_id = Uuid::new_v4().to_string();
        let restore_event = event(
            &restore_event_id,
            run_id,
            Some(&restored.id),
            "loop_state_transitioned",
            "loop",
            &restored.id,
            Some(causation),
            "Rejected replacement contract; preserved the previous ACTIVE loop",
            json!({"record":restored}),
        );
        self.store.transition_loop(&restored, &restore_event)?;
        if let Some(active) = self.active_runs.get_mut(run_id) {
            if let Some(loop_state) = active.loops.iter_mut().find(|item| item.id == restored.id) {
                *loop_state = restored;
            }
            active.last_event_id = restore_event_id;
        }
        Ok(())
    }

    fn apply_review_decision(
        &mut self,
        request: &HypothesisReviewRequest,
        package: &WorldModelReviewPackage,
        decision: HypothesisReviewDecision,
    ) -> Result<RunSnapshot> {
        if matches!(
            decision.action,
            HypothesisAction::Modify | HypothesisAction::Split
        ) && decision.proposed_contract.is_none()
        {
            bail!("MODIFY/SPLIT requires a complete HypothesisContract proposal");
        }
        if decision.action == HypothesisAction::Split {
            let proposal = decision.proposed_contract.as_ref().expect("checked above");
            let routing = decision
                .routing
                .as_ref()
                .context("SPLIT requires model routing metadata")?;
            if !routing.escalated {
                bail!("SPLIT rejected: escalation is mandatory");
            }
            if decision.decision_confidence < self.review_policy.split_confidence_threshold {
                bail!("SPLIT rejected: confidence is below the configured threshold");
            }
            let active_count = self
                .active_runs
                .get(&request.run_id)
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
            let distinct_support = package
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
            if proposal
                .mechanism
                .trim()
                .eq_ignore_ascii_case(package.current_hypothesis.strategy_mechanism.trim())
            {
                bail!("SPLIT rejected: candidate contract mechanism is not materially distinct");
            }
        }
        let (current, parent_loop, last_event_id, next_thesis_version, next_context_version) = {
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
            )
        };
        if matches!(
            decision.action,
            HypothesisAction::Modify | HypothesisAction::Split
        ) && (current.status != "ACTIVE"
            || current.contract.as_ref().map(|contract| contract.state)
                != Some(crate::contracts::ContractState::Active))
        {
            bail!("MODIFY/SPLIT rejected: current hypothesis is not an ACTIVE contract");
        }
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
            json!({ "record": decision, "reviewPackageId": package.id, "reviewPackageEventId": package.created_by_event_id }),
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
                let active = self.active_runs.get_mut(&request.run_id).unwrap();
                active.last_event_id = loop_stop_event_id;
                if let Some(loop_state) = active
                    .loops
                    .iter_mut()
                    .find(|item| item.id == stopped_loop.id)
                {
                    *loop_state = stopped_loop;
                }
            }
            HypothesisAction::Modify | HypothesisAction::Split => {
                let is_split = decision.action == HypothesisAction::Split;
                let proposal = decision
                    .proposed_contract
                    .clone()
                    .context("review action omitted its HypothesisContract proposal")?;
                let mut additional_evidence_ids = decision.evidence_canonical_ids.clone();
                additional_evidence_ids.extend(package.exact_canonical_ids.iter().cloned());
                additional_evidence_ids.sort();
                additional_evidence_ids.dedup();
                let independently_fresh_evidence_ids = decision
                    .web_evidence
                    .iter()
                    .filter(|evidence| evidence.primary_eligible)
                    .map(|evidence| evidence.id.clone())
                    .collect::<Vec<_>>();
                let parent_contract = current
                    .contract
                    .as_ref()
                    .context("cannot revise a hypothesis without an active contract")?;
                let contract_version = if is_split {
                    1
                } else {
                    parent_contract
                        .version
                        .checked_add(1)
                        .context("contract version exceeds supported range")?
                };
                let persisted_draft = self.persist_validated_contract_draft(
                    proposal,
                    &request.run_id,
                    contract_version,
                    Some(parent_contract.id.clone()),
                    Some(current.id.clone()),
                    if is_split {
                        None
                    } else {
                        Some(current.root_hypothesis_id.clone())
                    },
                    next_thesis_version,
                    next_context_version,
                    &format!("world-model {:?} review {}", decision.action, decision.id),
                    Some(&parent_loop.id),
                    Some(&causation),
                    package.historical_retrieval.as_ref(),
                    &additional_evidence_ids,
                    &independently_fresh_evidence_ids,
                );
                let (compiled, draft_event_id) = match persisted_draft {
                    Ok(value) => value,
                    Err(error) => {
                        if !is_split {
                            self.restore_parent_loop_after_failed_modify(
                                &request.run_id,
                                &parent_loop,
                                &causation,
                            )?;
                        }
                        return Err(error.context(
                            "review replacement exhausted bounded contract validation repair",
                        ));
                    }
                };
                let activation_context =
                    match self.preflight_contract_context(&compiled, &parent_loop.id) {
                        Ok(context) => context,
                        Err(error) => {
                            self.reject_persisted_contract(
                                &compiled,
                                &draft_event_id,
                                Some(&parent_loop.id),
                                &error,
                            )?;
                            if !is_split {
                                self.restore_parent_loop_after_failed_modify(
                                    &request.run_id,
                                    &parent_loop,
                                    &causation,
                                )?;
                            }
                            return Err(error
                                .context("replacement contract required context is unavailable"));
                        }
                    };
                let compiled = match self.activate_contract(
                    compiled,
                    &draft_event_id,
                    Some(&parent_loop.id),
                    &activation_context,
                    package.historical_retrieval.as_ref(),
                    &additional_evidence_ids,
                    &independently_fresh_evidence_ids,
                ) {
                    Ok(compiled) => compiled,
                    Err(error) => {
                        if !is_split {
                            self.restore_parent_loop_after_failed_modify(
                                &request.run_id,
                                &parent_loop,
                                &causation,
                            )?;
                        }
                        return Err(error.context(
                            "review replacement did not pass the shared contract activation gate",
                        ));
                    }
                };
                let thesis = compiled.thesis;
                let context = compiled.context;
                let revised = compiled.hypothesis;
                let activation_event_id = compiled.activated_by_event_id;
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
                        Some(&activation_event_id),
                        "Harness replaced the prior loop after the revised ACTIVE contract passed validation",
                        json!({"record":stopped_loop}),
                    );
                    self.store
                        .stop_loop(&stopped_loop.id, stopped_at, &stop_event)?;
                    if let Some(active) = self.active_runs.get_mut(&request.run_id) {
                        if let Some(loop_state) = active
                            .loops
                            .iter_mut()
                            .find(|item| item.id == stopped_loop.id)
                        {
                            *loop_state = stopped_loop;
                        }
                        active.last_event_id = stop_event_id;
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
                    created_at: Utc::now(),
                    stopped_at: None,
                };
                let loop_event = event(
                    &loop_event_id,
                    &request.run_id,
                    Some(&loop_state.id),
                    "loop_spawned",
                    "loop",
                    &loop_state.id,
                    Some(&activation_event_id),
                    "Harness spawned a loop from a validated ACTIVE HypothesisContract",
                    json!({"record":loop_state}),
                );
                self.spawn_loop_for_active_contract(
                    &revised,
                    &activation_event_id,
                    &loop_state,
                    &loop_event,
                )?;
                let cadence = cadence_for(&loop_state.id, &revised);
                let cadence_event = event(
                    &cadence.created_by_event_id,
                    &request.run_id,
                    Some(&loop_state.id),
                    "loop_cadence_mapped",
                    "loop_cadence",
                    &loop_state.id,
                    Some(&loop_event_id),
                    "Harness mapped the validated contract timeframe to Jev cadence",
                    json!({"record":cadence}),
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
                position_may_have_exposure(position)
                    && only_loop_id
                        .map(|id| position.loop_id == id)
                        .unwrap_or(true)
            })
            .cloned()
            .collect::<Vec<_>>();
        for mut position in open_positions {
            let stop_key_prefix = format!("run-stop:{}:", position.id);
            let prior_close_orders = self
                .store
                .stored_events(run_id)?
                .into_iter()
                .map(|stored| stored.event)
                .filter(|event| event.kind == "order_evaluated")
                .map(|event| {
                    serde_json::from_value::<OrderRecord>(
                        event
                            .payload
                            .get("record")
                            .cloned()
                            .context("order_evaluated event omitted its order record")?,
                    )
                    .context("order_evaluated event contains a malformed order")
                })
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .filter(|order| order.idempotency_key.starts_with(&stop_key_prefix))
                .collect::<Vec<_>>();
            let prior_close_attempts = prior_close_orders.len();
            if let Some(previous_order) = prior_close_orders.last() {
                if !self.reconcile_broker(run_id)? {
                    bail!("cannot retry lifecycle close until a complete broker reconciliation resolves the prior close attempt for position {}", position.id);
                }
                let Some(current_position) = self
                    .active_runs
                    .get(run_id)
                    .and_then(|active| active.positions.iter().find(|item| item.id == position.id))
                    .cloned()
                else {
                    bail!(
                        "prior lifecycle close reconciliation lost position {}",
                        position.id
                    );
                };
                if !position_may_have_exposure(&current_position) {
                    continue;
                }
                position = current_position;
                let mut resolution = self
                    .broker
                    .resolve_order_status(previous_order, position.broker_position_id.as_deref())?;
                resolution.created_by_event_id = Uuid::new_v4().to_string();
                let resolution_status = resolution.status.clone();
                let resolution_event = event(
                    &resolution.created_by_event_id,
                    run_id,
                    Some(&position.loop_id),
                    "execution_recorded",
                    "execution",
                    &resolution.execution_id,
                    Some(&previous_order.created_by_event_id),
                    "Broker order-status request resolved a prior lifecycle close attempt",
                    json!({"record":resolution}),
                );
                self.store
                    .record_execution(&resolution, &resolution_event)?;
                if !matches!(
                    resolution_status.as_str(),
                    "cancelled" | "expired" | "rejected"
                ) {
                    bail!("prior lifecycle close order {} is not proven terminal (status: {}); no replacement close will be submitted", previous_order.id, resolution_status);
                }
                // The order-status response may report fills that happened after
                // the position snapshot above. Refresh broker truth before sizing
                // a replacement close, otherwise a partially filled cancelled
                // order could be retried at the original (now excessive) quantity.
                if !self.reconcile_broker(run_id)? {
                    bail!("cannot retry lifecycle close until a complete broker reconciliation reflects the terminal status of prior close order {}", previous_order.id);
                }
                let Some(current_position) = self
                    .active_runs
                    .get(run_id)
                    .and_then(|active| active.positions.iter().find(|item| item.id == position.id))
                    .cloned()
                else {
                    bail!("post-status reconciliation lost position {}", position.id);
                };
                if !position_may_have_exposure(&current_position) {
                    continue;
                }
                position = current_position;
            }
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
                .reference_price(&control.instrument)?
                .context("broker close reference price is unavailable")?;
            let order_event_id = Uuid::new_v4().to_string();
            let order = OrderRecord {
                id: Uuid::new_v4().to_string(),
                run_id: run_id.into(),
                loop_id: loop_state.id.clone(),
                decision_id: decision.id.clone(),
                idempotency_key: format!(
                    "run-stop:{}:{}",
                    position.id,
                    prior_close_attempts.saturating_add(1)
                ),
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

    fn complete_pending_review_stop(&mut self, run_id: &str, loop_id: &str) -> Result<()> {
        self.close_positions_for_lifecycle(run_id, Some(loop_id))?;
        self.materialize_trade_outcomes(run_id)?;
        let current_loop = self
            .active_runs
            .get(run_id)
            .and_then(|active| active.loops.iter().find(|item| item.id == loop_id))
            .cloned()
            .context("pending review STOP references a missing loop")?;
        if current_loop.state != "review-stop-pending" {
            bail!("pending review STOP loop changed state before flattening completed");
        }
        let decision_event_id = self
            .store
            .stored_events(run_id)?
            .into_iter()
            .rev()
            .find(|stored| {
                stored.event.kind == "loop_state_transitioned"
                    && stored.event.aggregate_id == loop_id
                    && stored.event.payload["record"]["state"] == "review-stop-pending"
            })
            .map(|stored| stored.event.id)
            .context("pending review STOP has no canonical pending transition")?;
        let stopped_at = Utc::now();
        let mut stopped_loop = current_loop;
        stopped_loop.state = "stopped".into();
        stopped_loop.stopped_at = Some(stopped_at);
        let event_id = Uuid::new_v4().to_string();
        let stop_event = event(
            &event_id,
            run_id,
            Some(loop_id),
            "loop_stopped",
            "loop",
            loop_id,
            Some(&decision_event_id),
            "Recovered a pending world-model STOP after confirming its broker exposure was flattened",
            json!({"record":stopped_loop,"recovery":"pending-review-stop-completed"}),
        );
        self.store.stop_loop(loop_id, stopped_at, &stop_event)?;
        let active = self
            .active_runs
            .get_mut(run_id)
            .context("run is not active")?;
        if let Some(current) = active.loops.iter_mut().find(|item| item.id == loop_id) {
            *current = stopped_loop;
        }
        active.last_event_id = event_id;
        Ok(())
    }

    pub fn stop(&mut self, run_id: &str) -> Result<RunSnapshot> {
        if let Some(pending) = self.pending_startup_activations.remove(run_id) {
            let error = anyhow::anyhow!("startup warm-up was cancelled by operator");
            self.reject_persisted_contract(
                &pending.compiled,
                &pending.draft_event_id,
                None,
                &error,
            )?;
            let mut rejected = pending.compiled.hypothesis;
            rejected.status = "REJECTED".into();
            if let Some(contract) = rejected.contract.as_mut() {
                contract.state = crate::contracts::ContractState::Rejected;
            }
            if let Some(active) = self.active_runs.get_mut(run_id) {
                if let Some(hypothesis) = active
                    .hypotheses
                    .iter_mut()
                    .find(|item| item.id == rejected.id)
                {
                    *hypothesis = rejected;
                }
            }
        }
        let pending = self
            .active_runs
            .get(run_id)
            .context("run is not active")?
            .loops
            .iter()
            .filter(|loop_state| {
                loop_state.state != "stopped" && loop_state.state != "run-stop-pending"
            })
            .cloned()
            .collect::<Vec<_>>();
        for mut loop_state in pending {
            loop_state.state = "run-stop-pending".into();
            let causation = self.active_runs.get(run_id).unwrap().last_event_id.clone();
            let event_id = Uuid::new_v4().to_string();
            let pending_event = event(
                &event_id,
                run_id,
                Some(&loop_state.id),
                "loop_state_transitioned",
                "loop",
                &loop_state.id,
                Some(&causation),
                "Operator stop blocked new entries while broker positions are flattened",
                json!({"record":loop_state}),
            );
            self.store.transition_loop(&loop_state, &pending_event)?;
            if let Some(active) = self.active_runs.get_mut(run_id) {
                if let Some(existing) = active
                    .loops
                    .iter_mut()
                    .find(|item| item.id == loop_state.id)
                {
                    *existing = loop_state;
                }
                active.last_event_id = event_id;
            }
        }
        if !self.reconcile_broker(run_id)? {
            bail!("run stop remains pending until a complete broker reconciliation is available");
        }
        self.close_positions_for_lifecycle(run_id, None)?;
        if self
            .active_runs
            .get(run_id)
            .context("run is not active")?
            .positions
            .iter()
            .any(position_may_have_exposure)
        {
            bail!("run stop remains pending while canonical position exposure is unresolved");
        }
        self.materialize_trade_outcomes(run_id)?;
        let wrapup_hypotheses = self
            .active_runs
            .get(run_id)
            .unwrap()
            .loops
            .iter()
            .map(|loop_state| loop_state.hypothesis_id.clone())
            .collect::<Vec<_>>();
        let mut wrapups = Vec::new();
        for hypothesis_id in wrapup_hypotheses {
            let request = HypothesisReviewRequest { run_id: run_id.into(), hypothesis_id,
                evidence_query: "Final retrospective after operator stop and confirmed broker flattening. Summarize observed decisions and outcomes; this response is archival only and cannot restart trading.".into() };
            // A stop retrospective must not depend on Qdrant availability.
            let context_pool = self.context_pool.take();
            let package_result = self.assemble_review_package(&request);
            self.context_pool = context_pool;
            match package_result {
                Ok(package) => wrapups.push(package),
                Err(error) => eprintln!("Run {run_id} stop wrap-up package unavailable: {error:#}"),
            }
        }
        let mut active = self
            .active_runs
            .get(run_id)
            .cloned()
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
            if let Some(current) = self.active_runs.get_mut(run_id) {
                if let Some(existing) = current
                    .loops
                    .iter_mut()
                    .find(|item| item.id == loop_state.id)
                {
                    *existing = loop_state.clone();
                }
                current.last_event_id = stop_event_id.clone();
            }
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
        self.active_runs.remove(run_id);
        self.pending_stop_wrapups.insert(run_id.into(), wrapups);
        if let Some(context_pool) = &self.context_pool {
            if let Err(error) = context_pool.sync_pending_events(&self.store, run_id) {
                eprintln!("Run {run_id} stopped; deferred search indexing failed: {error:#}");
            }
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

    pub fn take_stop_wrapups(&mut self, run_id: &str) -> Vec<WorldModelReviewPackage> {
        self.pending_stop_wrapups.remove(run_id).unwrap_or_default()
    }

    pub fn record_stop_wrapup(
        &mut self,
        package: &WorldModelReviewPackage,
        result: Result<HypothesisReviewDecision>,
    ) -> Result<()> {
        let event_id = Uuid::new_v4().to_string();
        let (kind, summary, payload) = match result {
            Ok(decision) => (
                "world_model_stop_wrapup_recorded",
                "World model retrospectively reviewed a stopped loop; no action applied",
                json!({"reviewPackageId":package.id,"decision":decision,"archivalOnly":true}),
            ),
            Err(error) => (
                "world_model_stop_wrapup_failed",
                "World model retrospective failed after the run was safely stopped",
                json!({"reviewPackageId":package.id,"error":format!("{error:#}"),"archivalOnly":true}),
            ),
        };
        let wrapup_event = event(
            &event_id,
            &package.run_id,
            Some(&package.current_loop.id),
            kind,
            "world_model_stop_wrapup",
            &package.id,
            Some(&package.created_by_event_id),
            summary,
            payload,
        );
        self.store.append_event(&wrapup_event)?;
        Ok(())
    }

    pub fn search(&self, query: &str) -> Result<Vec<SearchHit>> {
        self.store.search(query, 25)
    }

    pub fn is_active(&self, run_id: &str) -> bool {
        self.active_runs.contains_key(run_id)
    }

    pub fn cycle_interval_seconds(&self, run_id: &str) -> Result<u64> {
        if self.pending_startup_activations.contains_key(run_id) {
            return Ok(5);
        }
        let active = self
            .active_runs
            .get(run_id)
            .ok_or_else(|| anyhow::anyhow!("run is not active"))?;
        if active
            .loops
            .iter()
            .any(|loop_state| loop_state.state == "run-stop-pending")
        {
            // Stop recovery does not need a market-data cadence; keep retrying
            // broker reconciliation and flattening on a short bounded interval.
            return Ok(5);
        }
        if active
            .loops
            .iter()
            .all(|loop_state| loop_state.state == "stopped")
        {
            return Ok(0);
        }
        let cadence_interval = active
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
            .context("active run has no Jev cadence")?
            .max(1);
        let stop_interval = self.seconds_until_contract_stop(run_id)?;
        Ok(stop_interval
            .map(|remaining| cadence_interval.min(remaining.max(1)))
            .unwrap_or(cadence_interval))
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

    pub fn market_data_statuses(&self) -> Vec<IntegrationStatus> {
        self.market_data.health_statuses()
    }

    pub fn pending_indexer(&self) -> Option<(QdrantContextPool, CanonicalStore)> {
        self.context_pool
            .clone()
            .map(|pool| (pool, self.store.clone()))
    }

    pub fn canonical_store_handle(&self) -> CanonicalStore {
        self.store.clone()
    }

    pub fn record_worker_failure(
        &mut self,
        run_id: &str,
        stage: &str,
        message: &str,
    ) -> Result<()> {
        let causation = self
            .active_runs
            .get(run_id)
            .context("run is not active")?
            .last_event_id
            .clone();
        let event_id = Uuid::new_v4().to_string();
        let failure = event(
            &event_id,
            run_id,
            None,
            "harness_worker_failed",
            "run",
            run_id,
            Some(&causation),
            "Harness worker could not complete a scheduled step",
            json!({"stage":stage,"error":message}),
        );
        self.store.append_event(&failure)?;
        if let Some(active) = self.active_runs.get_mut(run_id) {
            active.last_event_id = event_id;
        }
        Ok(())
    }

    pub fn market_requests(&self, run_id: &str) -> Result<Vec<MarketDataRequest>> {
        let active = self.active_runs.get(run_id).context("run is not active")?;
        let mut requests = Vec::new();
        for loop_state in active.loops.iter().filter(|item| item.state != "stopped") {
            let Some(spec) = active
                .hypotheses
                .iter()
                .find(|hypothesis| hypothesis.id == loop_state.hypothesis_id)
                .and_then(|hypothesis| hypothesis.live_context_spec.as_ref())
            else {
                continue;
            };
            requests.push(MarketDataRequest {
                instrument: spec.instrument.clone(),
                series: crate::market_data::requirements(spec)?,
                quote_max_age_seconds: 15,
            });
        }
        if let Some(pending) = self.pending_startup_activations.get(run_id) {
            let spec = &pending.compiled.contract.proposal.live_context_spec;
            if !requests
                .iter()
                .any(|request| request.instrument.eq_ignore_ascii_case(&spec.instrument))
            {
                requests.push(MarketDataRequest {
                    instrument: spec.instrument.clone(),
                    series: crate::market_data::requirements(spec)?,
                    quote_max_age_seconds: pending
                        .compiled
                        .contract
                        .proposal
                        .context_requirements
                        .iter()
                        .filter(|requirement| {
                            requirement.value_type == crate::contracts::ContextValueType::Quote
                        })
                        .map(|requirement| requirement.maximum_age_seconds)
                        .min()
                        .unwrap_or(15),
                });
            }
        }
        Ok(requests)
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

    fn recover_interrupted_review_contract_drafts(
        &mut self,
        run_id: &str,
        state: &mut crate::domain::ReplayState,
        events: &[crate::domain::StoredEvent],
    ) -> Result<()> {
        let orphaned = state
            .hypotheses
            .iter()
            .filter(|hypothesis| {
                hypothesis.status == "DRAFT"
                    && hypothesis.contract.as_ref().is_some_and(|contract| {
                        contract.state == crate::contracts::ContractState::Draft
                    })
                    && !is_recoverable_initial_startup_draft(hypothesis, state, events, run_id)
            })
            .cloned()
            .collect::<Vec<_>>();

        // Resolve and validate all recovery evidence before writing rejection
        // events, so incomplete lineage fails visibly without half-recovery.
        let mut recoveries = Vec::new();
        for mut hypothesis in orphaned {
            let contract = hypothesis
                .contract
                .as_ref()
                .context("interrupted revision DRAFT is missing its contract")?;
            let draft_event = events
                .iter()
                .find(|stored| {
                    stored.event.kind == "hypothesis_version_created"
                        && stored.event.aggregate_id == contract.id
                })
                .context("interrupted revision DRAFT is missing its creation event")?;
            let mut ancestor = draft_event;
            for expected_kind in [
                "context_version_created",
                "context_definition_created",
                "thesis_version_created",
                "hypothesis_reviewed",
            ] {
                let ancestor_id = ancestor
                    .event
                    .causation_event_id
                    .as_deref()
                    .context("interrupted revision DRAFT has an incomplete causal chain")?;
                ancestor = events
                    .iter()
                    .find(|stored| stored.event.id == ancestor_id)
                    .with_context(|| {
                        format!("interrupted revision DRAFT is missing {expected_kind} ancestry")
                    })?;
                if ancestor.event.kind != expected_kind
                    || ancestor.event.run_id != run_id
                    || ancestor.event.loop_id != draft_event.event.loop_id
                {
                    bail!("interrupted revision DRAFT causal chain has invalid event ownership or order");
                }
            }
            let decision_event = ancestor;
            let draft_record: HypothesisDefinition = serde_json::from_value(
                draft_event
                    .event
                    .payload
                    .get("record")
                    .cloned()
                    .context("interrupted revision DRAFT omitted its hypothesis record")?,
            )
            .context("interrupted revision DRAFT hypothesis record is malformed")?;
            if draft_event.event.aggregate_type != "hypothesis_contract"
                || draft_event.event.aggregate_id != contract.id
                || draft_record.id != hypothesis.id
                || draft_record.run_id != run_id
                || draft_record.status != "DRAFT"
                || draft_record
                    .contract
                    .as_ref()
                    .is_none_or(|record| record.id != contract.id)
            {
                bail!("interrupted revision DRAFT event payload or aggregate identity is inconsistent");
            }
            let context_event = events
                .iter()
                .find(|stored| {
                    Some(stored.event.id.as_str())
                        == draft_event.event.causation_event_id.as_deref()
                })
                .context("interrupted revision is missing its context-version event")?;
            let context: ContextVersion = serde_json::from_value(
                context_event
                    .event
                    .payload
                    .get("record")
                    .cloned()
                    .context("interrupted revision context event omitted its record")?,
            )?;
            if context.id != hypothesis.context_version_id
                || context.run_id != run_id
                || context_event.event.aggregate_type != "context_version"
                || context_event.event.aggregate_id != context.id
                || context.created_by_event_id != context_event.event.id
            {
                bail!("interrupted revision context-version payload or aggregate identity is inconsistent");
            }
            let definition_event = events
                .iter()
                .find(|stored| {
                    Some(stored.event.id.as_str())
                        == context_event.event.causation_event_id.as_deref()
                })
                .context("interrupted revision is missing its context-definition event")?;
            let definition: ContextDefinition = serde_json::from_value(
                definition_event
                    .event
                    .payload
                    .get("record")
                    .cloned()
                    .context("interrupted revision context-definition event omitted its record")?,
            )?;
            if definition.id != context.definition_id
                || definition.run_id != run_id
                || definition_event.event.aggregate_type != "context_definition"
                || definition_event.event.aggregate_id != definition.id
                || definition.created_by_event_id != definition_event.event.id
            {
                bail!("interrupted revision context-definition payload or aggregate identity is inconsistent");
            }
            let thesis_event = events
                .iter()
                .find(|stored| {
                    Some(stored.event.id.as_str())
                        == definition_event.event.causation_event_id.as_deref()
                })
                .context("interrupted revision is missing its thesis-version event")?;
            let thesis: ThesisVersion = serde_json::from_value(
                thesis_event
                    .event
                    .payload
                    .get("record")
                    .cloned()
                    .context("interrupted revision thesis event omitted its record")?,
            )?;
            if thesis.id != hypothesis.thesis_version_id
                || thesis.run_id != run_id
                || thesis_event.event.aggregate_type != "thesis_version"
                || thesis_event.event.aggregate_id != thesis.id
                || thesis.created_by_event_id != thesis_event.event.id
            {
                bail!("interrupted revision thesis-version payload or aggregate identity is inconsistent");
            }
            let decision_event_id = decision_event.event.id.as_str();
            if decision_event.event.aggregate_type != "hypothesis_review"
                || decision_event.event.aggregate_id
                    != decision_event.event.payload["record"]["id"]
                        .as_str()
                        .unwrap_or_default()
            {
                bail!("interrupted revision review event aggregate identity is inconsistent");
            }
            let decision: HypothesisReviewDecision = serde_json::from_value(
                decision_event
                    .event
                    .payload
                    .get("record")
                    .cloned()
                    .context("interrupted revision review event omitted its record")?,
            )
            .context("interrupted revision review event is malformed")?;
            if !matches!(
                decision.action.clone(),
                HypothesisAction::Modify | HypothesisAction::Split
            ) {
                bail!("interrupted revision DRAFT is not linked to a MODIFY or SPLIT decision");
            }
            if decision.run_id != run_id
                || decision.hypothesis_id
                    != hypothesis
                        .parent_hypothesis_id
                        .as_deref()
                        .unwrap_or_default()
            {
                bail!("interrupted revision DRAFT review decision does not match its parent hypothesis");
            }
            let parent_contract_id = contract
                .parent_contract_id
                .as_deref()
                .context("interrupted review revision has no parent contract identity")?;
            let parent_hypothesis = state
                .hypotheses
                .iter()
                .find(|item| item.id == decision.hypothesis_id)
                .context("interrupted revision review references an unknown parent hypothesis")?;
            if parent_hypothesis
                .contract
                .as_ref()
                .map(|parent| parent.id.as_str())
                != Some(parent_contract_id)
                || !state.loops.iter().any(|loop_state| {
                    Some(loop_state.id.as_str()) == draft_event.event.loop_id.as_deref()
                        && loop_state.hypothesis_id == decision.hypothesis_id
                })
            {
                bail!(
                    "interrupted revision DRAFT parent contract or loop ownership is inconsistent"
                );
            }
            let restore_loop = if decision.action == HypothesisAction::Modify {
                let loop_id = draft_event
                    .event
                    .loop_id
                    .as_deref()
                    .context("interrupted MODIFY DRAFT is missing its parent loop")?;
                let pending = events
                    .iter()
                    .find(|stored| {
                        stored.event.kind == "loop_state_transitioned"
                            && stored.event.aggregate_id == loop_id
                            && stored.event.causation_event_id.as_deref() == Some(decision_event_id)
                            && stored.event.payload["record"]["state"] == "review-modify-pending"
                    })
                    .context(
                        "interrupted MODIFY is missing its exposure-blocking loop transition",
                    )?;
                let current_loop_state = state
                    .loops
                    .iter()
                    .find(|item| item.id == loop_id)
                    .map(|item| item.state.as_str())
                    .context("interrupted MODIFY parent loop is absent from replay state")?;
                if current_loop_state != "review-modify-pending" {
                    let already_restored = events.iter().any(|stored| {
                        stored.sequence > pending.sequence
                            && stored.event.aggregate_id == loop_id
                            && stored.event.kind == "loop_state_transitioned"
                            && stored.event.payload["recovery"]
                                == "interrupted-modify-loop-restored"
                    });
                    if !already_restored {
                        bail!("interrupted MODIFY parent loop has an unexpected state; recovery will not guess")
                    }
                }
                let previous = events
                    .iter()
                    .filter(|stored| {
                        stored.sequence < pending.sequence
                            && stored.event.aggregate_id == loop_id
                            && matches!(
                                stored.event.kind.as_str(),
                                "loop_spawned" | "loop_state_transitioned"
                            )
                    })
                    .max_by_key(|stored| stored.sequence)
                    .context("interrupted MODIFY has no prior parent-loop state to restore")?;
                if current_loop_state != "review-modify-pending" {
                    None
                } else {
                    Some(serde_json::from_value::<LoopView>(
                        previous
                            .event
                            .payload
                            .get("record")
                            .cloned()
                            .context("prior parent-loop event omitted its record")?,
                    )?)
                }
            } else {
                None
            };
            hypothesis.status = "REJECTED".into();
            if let Some(contract) = hypothesis.contract.as_mut() {
                contract.state = crate::contracts::ContractState::Rejected;
            }
            recoveries.push((hypothesis, draft_event.event.id.clone(), restore_loop));
        }

        for (hypothesis, draft_event_id, restore_loop) in recoveries {
            let contract_id = hypothesis
                .contract
                .as_ref()
                .map(|contract| contract.id.as_str())
                .unwrap_or(hypothesis.id.as_str());
            let rejection_id = Uuid::new_v4().to_string();
            let rejection = event(
                &rejection_id,
                run_id,
                None,
                "hypothesis_contract_rejected",
                "hypothesis_contract",
                contract_id,
                Some(&draft_event_id),
                "Interrupted review revision was rejected during recovery; the prior active loop is retained",
                json!({"record":hypothesis,"recovery":"interrupted-review-revision"}),
            );
            if let Some(restored) = restore_loop {
                let restore_id = Uuid::new_v4().to_string();
                let restored_event = event(
                    &restore_id,
                    run_id,
                    Some(&restored.id),
                    "loop_state_transitioned",
                    "loop",
                    &restored.id,
                    Some(&rejection_id),
                    "Recovered interrupted MODIFY by rejecting its unactivated replacement and restoring the previous loop state",
                    json!({"record":restored,"recovery":"interrupted-modify-loop-restored"}),
                );
                self.store.transition_loop(&restored, &restored_event)?;
                if let Some(current) = state.loops.iter_mut().find(|item| item.id == restored.id) {
                    *current = restored;
                }
            }
            self.store.reject_hypothesis(&hypothesis, &rejection)?;
            if let Some(current) = state
                .hypotheses
                .iter_mut()
                .find(|current| current.id == hypothesis.id)
            {
                *current = hypothesis;
            }
        }
        Ok(())
    }

    fn validate_recovered_active_contract(
        &self,
        active: &ActiveRun,
        hypothesis: &HypothesisDefinition,
        stored_events: &[crate::domain::StoredEvent],
    ) -> Result<()> {
        let contract = hypothesis
            .contract
            .as_ref()
            .context("executable hypothesis has no persisted contract")?;
        if hypothesis.status != "ACTIVE"
            || contract.state != crate::contracts::ContractState::Active
        {
            bail!("executable hypothesis and contract are not both ACTIVE");
        }
        if contract.run_id != hypothesis.run_id
            || contract.id != hypothesis.id
            || contract.version as i64 != hypothesis.version
        {
            bail!("persisted contract identity does not match its hypothesis projection");
        }
        if contract.proposal.user_objective != active.thesis
            || hypothesis.original_prompt != active.thesis
        {
            bail!("persisted contract objective is not the canonical run prompt");
        }
        if hypothesis.instruments != contract.proposal.instruments
            || hypothesis.strategy_mechanism != contract.proposal.mechanism
            || hypothesis.timeframe.label != contract.proposal.timeframe.label
            || hypothesis.timeframe.horizon_minutes != contract.proposal.timeframe.horizon_minutes
            || hypothesis.timeframe.source != contract.proposal.timeframe.source
            || hypothesis.timeframe.rationale != contract.proposal.timeframe.rationale
            || hypothesis.jev_question != contract.proposal.jev1_objective
            || hypothesis.live_context_spec.as_ref().is_none_or(|spec| {
                serde_json::to_value(spec).ok()
                    != serde_json::to_value(&contract.proposal.live_context_spec).ok()
            })
        {
            bail!("persisted hypothesis projections differ from the ACTIVE contract proposal");
        }
        if hypothesis.thesis_version_id.trim().is_empty()
            || hypothesis.context_version_id.trim().is_empty()
        {
            bail!("persisted ACTIVE hypothesis is missing its thesis/context projection links");
        }
        let thesis = active
            .thesis_versions
            .iter()
            .find(|item| item.id == hypothesis.thesis_version_id)
            .context("ACTIVE contract thesis projection is missing")?;
        let thesis_event = stored_events
            .iter()
            .find(|stored| stored.event.id == thesis.created_by_event_id)
            .context("ACTIVE contract thesis creation event is missing")?;
        if thesis_event.event.kind != "thesis_version_created"
            || thesis_event.event.aggregate_type != "thesis_version"
            || thesis_event.event.aggregate_id != thesis.id
            || thesis_event.event.run_id != hypothesis.run_id
            || thesis_event.event.payload.get("record") != Some(&serde_json::to_value(thesis)?)
        {
            bail!("ACTIVE contract thesis creation event does not match its projection");
        }
        if thesis.run_id != hypothesis.run_id || thesis.thesis != contract.proposal.thesis {
            bail!("ACTIVE contract thesis projection does not match its proposal");
        }
        let context = active
            .context_versions
            .iter()
            .find(|item| item.id == hypothesis.context_version_id)
            .context("ACTIVE contract context projection is missing")?;
        if context.run_id != hypothesis.run_id
            || context.definition_id.trim().is_empty()
            || context.items.len() != contract.proposal.context_requirements.len()
        {
            bail!("ACTIVE contract context projection has mismatched identity or fields");
        }
        for (item, requirement) in context
            .items
            .iter()
            .zip(&contract.proposal.context_requirements)
        {
            let expected_content = format!(
                "{:?} context from {:?}; period={:?}; lookback={:?}; max age={}s; required={}",
                requirement.value_type,
                requirement.source,
                requirement.period,
                requirement.lookback,
                requirement.maximum_age_seconds,
                requirement.required
            );
            if item.source != format!("contract-context://{:?}", requirement.source)
                || item.source_id != requirement.id
                || item.content != expected_content
            {
                bail!("ACTIVE contract context projection differs from declared requirements");
            }
        }
        let definition_event = stored_events
            .iter()
            .find(|stored| {
                stored.event.kind == "context_definition_created"
                    && stored.event.aggregate_id == context.definition_id
            })
            .context("ACTIVE contract context definition event is missing")?;
        let definition: ContextDefinition = serde_json::from_value(
            definition_event
                .event
                .payload
                .get("record")
                .cloned()
                .context("ACTIVE contract context definition event omitted its record")?,
        )
        .context("ACTIVE contract context definition record is malformed")?;
        if definition_event.event.run_id != hypothesis.run_id
            || definition_event.event.aggregate_type != "context_definition"
            || definition_event.event.id != definition.created_by_event_id
            || definition_event.event.causation_event_id.as_deref()
                != Some(thesis_event.event.id.as_str())
            || definition_event.event.loop_id != thesis_event.event.loop_id
            || definition_event.event.payload.get("record")
                != Some(&serde_json::to_value(&definition)?)
        {
            bail!("ACTIVE contract context definition event is not causally linked to its thesis");
        }
        let expected_description = contract
            .proposal
            .context_requirements
            .iter()
            .map(|item| format!("{} ({:?}, {:?})", item.id, item.source, item.value_type))
            .collect::<Vec<_>>()
            .join("; ");
        if definition.id != context.definition_id
            || definition.run_id != hypothesis.run_id
            || definition.name != format!("contract-context-v{}", contract.version)
            || definition.description != expected_description
        {
            bail!("ACTIVE contract context definition differs from its proposal projection");
        }
        let context_event = stored_events
            .iter()
            .find(|stored| stored.event.id == context.created_by_event_id)
            .context("ACTIVE contract context-version creation event is missing")?;
        if context_event.event.kind != "context_version_created"
            || context_event.event.aggregate_type != "context_version"
            || context_event.event.aggregate_id != context.id
            || context_event.event.run_id != hypothesis.run_id
            || context_event.event.causation_event_id.as_deref()
                != Some(definition_event.event.id.as_str())
            || context_event.event.loop_id != thesis_event.event.loop_id
            || context_event.event.payload.get("record") != Some(&serde_json::to_value(context)?)
        {
            bail!("ACTIVE contract context-version event is not causally linked to its definition");
        }

        let draft_event = stored_events
            .iter()
            .find(|stored| {
                stored.event.kind == "hypothesis_version_created"
                    && stored.event.aggregate_id == contract.id
                    && stored.event.id == hypothesis.created_by_event_id
            })
            .context("ACTIVE contract has no persisted source DRAFT event")?;
        if draft_event.event.run_id != hypothesis.run_id
            || draft_event.event.aggregate_type != "hypothesis_contract"
            || draft_event.event.causation_event_id.as_deref()
                != Some(context_event.event.id.as_str())
            || draft_event.event.loop_id != context_event.event.loop_id
        {
            bail!("ACTIVE contract DRAFT event is not causally linked to its context projection");
        }
        let mut draft_hypothesis: HypothesisDefinition = serde_json::from_value(
            draft_event
                .event
                .payload
                .get("record")
                .cloned()
                .context("ACTIVE contract DRAFT event omitted its hypothesis record")?,
        )
        .context("ACTIVE contract DRAFT hypothesis record is malformed")?;
        draft_hypothesis.status = hypothesis.status.clone();
        if let Some(draft_contract) = draft_hypothesis.contract.as_mut() {
            draft_contract.state = contract.state;
        }
        if serde_json::to_value(&draft_hypothesis)? != serde_json::to_value(hypothesis)? {
            bail!("ACTIVE hypothesis differs from the record persisted at DRAFT creation");
        }
        let (retrieval_trace, additional_evidence_ids, independently_fresh_ids) =
            parse_contract_evidence_provenance(&draft_event.event.payload)?;
        let verified_fresh_ids = validate_contract_review_evidence_provenance(
            stored_events,
            &thesis_event.event,
            &hypothesis.run_id,
            hypothesis.parent_hypothesis_id.as_deref(),
            &contract.proposal,
            retrieval_trace.as_ref(),
            &additional_evidence_ids,
            &independently_fresh_ids,
        )?;
        let validation_context = self.contract_validation_context(
            &contract.proposal,
            retrieval_trace.as_ref(),
            &additional_evidence_ids,
        )?;
        let now = Utc::now();
        crate::contracts::validate_contract(&contract.proposal, &validation_context, now)
            .context("persisted ACTIVE contract failed full deterministic validation")?;
        crate::contracts::validate_contract_timeframe_against_objective(
            &contract.proposal,
            &active.thesis,
        )
        .context("persisted ACTIVE contract timeframe differs from the canonical run prompt")?;
        crate::contracts::validate_contract_lifecycle_against_objective(
            &contract.proposal,
            &active.thesis,
        )
        .context("persisted ACTIVE contract lifecycle differs from the canonical run prompt")?;
        crate::contracts::validate_evidence_freshness(
            &contract.proposal,
            retrieval_trace.as_ref(),
            &verified_fresh_ids,
            now,
        )
        .context("persisted ACTIVE contract evidence is no longer fresh")?;
        Ok(())
    }

    pub fn restore_active_runs(&mut self) -> Result<Vec<String>> {
        let run_ids = self.store.active_run_ids()?;
        for run_id in &run_ids {
            if self.active_runs.contains_key(run_id) {
                continue;
            }
            let mut state = replay::replay_run(&self.store, run_id)?;
            let stored_events = self.store.stored_events(run_id)?;
            let mut recovery_block_detail = None;
            for decision in state.hypothesis_reviews.iter().filter(|decision| {
                matches!(
                    decision.action,
                    HypothesisAction::Modify | HypothesisAction::Split
                )
            }) {
                let decision_event_id = &decision.created_by_event_id;
                let already_marked = stored_events.iter().any(|stored| {
                    stored.event.kind == "review_recovery_aborted"
                        && stored.event.payload["decisionEventId"] == *decision_event_id
                });
                if already_marked {
                    continue;
                }
                let has_draft_descendant = stored_events
                    .iter()
                    .filter(|stored| stored.event.kind == "hypothesis_version_created")
                    .any(|draft| {
                        let mut ancestor = draft;
                        for expected_kind in [
                            "context_version_created",
                            "context_definition_created",
                            "thesis_version_created",
                        ] {
                            let Some(cause_id) = ancestor.event.causation_event_id.as_deref()
                            else {
                                return false;
                            };
                            let Some(previous) = stored_events
                                .iter()
                                .find(|candidate| candidate.event.id == cause_id)
                            else {
                                return false;
                            };
                            if previous.event.kind != expected_kind
                                || previous.event.run_id != *run_id
                                || previous.event.loop_id != draft.event.loop_id
                            {
                                return false;
                            }
                            ancestor = previous;
                        }
                        ancestor.event.causation_event_id.as_deref()
                            == Some(decision_event_id.as_str())
                    });
                if !has_draft_descendant {
                    let event_id = Uuid::new_v4().to_string();
                    let event = event(
                        &event_id,
                        run_id,
                        None,
                        "review_recovery_aborted",
                        "hypothesis_review",
                        &decision.id,
                        Some(decision_event_id),
                        "Interrupted MODIFY/SPLIT review action had no recoverable revision DRAFT and was abandoned during recovery",
                        json!({"decisionEventId":decision_event_id,"action":decision.action,"recovery":"no-recoverable-revision-draft"}),
                    );
                    self.store.append_event(&event)?;
                }
            }
            if let Err(error) =
                self.recover_interrupted_review_contract_drafts(run_id, &mut state, &stored_events)
            {
                recovery_block_detail = Some(format!(
                    "interrupted review revision could not be safely reconciled: {error:#}"
                ));
            }
            let events = self.store.events_for_run(run_id)?;
            let last_event_id = events
                .last()
                .map(|event| event.id.clone())
                .context("active run has no canonical events")?;
            let pending_draft = state
                .hypotheses
                .iter()
                .filter(|hypothesis| {
                    is_recoverable_initial_startup_draft(hypothesis, &state, &stored_events, run_id)
                })
                .max_by_key(|hypothesis| {
                    hypothesis
                        .contract
                        .as_ref()
                        .map_or(0, |contract| contract.version)
                })
                .cloned();
            if state.context_definitions.is_empty()
                || state.context_versions.is_empty()
                || state.thesis_versions.is_empty()
                || state.hypotheses.is_empty()
                || (state.loops.is_empty() && pending_draft.is_none())
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
            let pending_thesis_versions = state.thesis_versions.clone();
            let pending_context_versions = state.context_versions.clone();
            let pending_context_definitions = state.context_definitions.clone();
            let restored_allocations = state.capital_allocations.clone();
            let context_definition = pending_draft
                .as_ref()
                .and_then(|hypothesis| {
                    state
                        .context_versions
                        .iter()
                        .find(|item| item.id == hypothesis.context_version_id)
                })
                .and_then(|context| {
                    state
                        .context_definitions
                        .iter()
                        .find(|item| item.id == context.definition_id)
                })
                .or_else(|| state.context_definitions.first())
                .cloned()
                .unwrap();
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
            let incomplete_revision = recovery_block_detail.or_else(|| self.active_runs.get(run_id).and_then(|active| {
                if let Some(position) = active.positions.iter().find(|position| {
                    position_may_have_exposure(position)
                        && !active
                            .loops
                            .iter()
                            .any(|loop_state| loop_state.id == position.loop_id)
                }) {
                    return Some(format!(
                        "position {} may have exposure but references a missing loop",
                        position.id
                    ));
                }
                for loop_state in active.loops.iter().filter(|loop_state| {
                    loop_state.state != "stopped"
                        || active.positions.iter().any(|position| {
                            position.loop_id == loop_state.id
                                && position_may_have_exposure(position)
                        })
                }) {
                    let Some(hypothesis) = active
                        .hypotheses
                        .iter()
                        .find(|hypothesis| hypothesis.id == loop_state.hypothesis_id)
                    else {
                        return Some(format!(
                            "executable loop {} references a missing hypothesis",
                            loop_state.id
                        ));
                    };
                    if hypothesis.status != "ACTIVE" {
                        return Some(format!(
                            "executable loop {} references non-ACTIVE hypothesis {}",
                            loop_state.id, hypothesis.id
                        ));
                    }
                    if loop_state.thesis_version_id != hypothesis.thesis_version_id
                        || loop_state.context_version_id != hypothesis.context_version_id
                        || loop_state.run_id != hypothesis.run_id
                        || loop_state.id.trim().is_empty()
                    {
                        return Some(format!(
                            "executable loop {} references thesis/context projections that differ from hypothesis {}",
                            loop_state.id, hypothesis.id
                        ));
                    }
                    let linked_thesis = active
                        .thesis_versions
                        .iter()
                        .find(|item| item.id == loop_state.thesis_version_id);
                    let linked_context = active
                        .context_versions
                        .iter()
                        .find(|item| item.id == loop_state.context_version_id);
                    if linked_thesis.is_none_or(|item| item.version != loop_state.thesis_version)
                        || linked_context
                            .is_none_or(|item| item.version != loop_state.context_version)
                    {
                        return Some(format!(
                            "executable loop {} version numbers differ from linked projections",
                            loop_state.id
                        ));
                    }
                    let spawn_event = stored_events
                        .iter()
                        .find(|stored| stored.event.id == loop_state.created_by_event_id);
                    if spawn_event.is_none_or(|stored| {
                        stored.event.kind != "loop_spawned"
                            || stored.event.aggregate_type != "loop"
                            || stored.event.aggregate_id != loop_state.id
                            || stored.event.run_id != hypothesis.run_id
                    }) {
                        return Some(format!(
                            "executable loop {} has no matching canonical spawn event",
                            loop_state.id
                        ));
                    }
                    if hypothesis.contract.is_none() {
                        return Some(format!(
                            "executable hypothesis {} has no persisted validated contract",
                            hypothesis.id,
                        ));
                    }
                    if let Err(error) =
                        self.validate_recovered_active_contract(active, hypothesis, &stored_events)
                    {
                        return Some(format!(
                            "executable hypothesis {} failed ACTIVE contract recovery validation: {error:#}",
                            hypothesis.id,
                        ));
                    }
                }
                if active
                    .loops
                    .iter()
                    .any(|loop_state| {
                        matches!(
                            loop_state.state.as_str(),
                            "review-modify-pending" | "run-stop-pending"
                        )
                    })
                {
                    return Some(
                        "a review replacement or run stop was interrupted before recovery completed"
                            .to_owned(),
                    );
                }
                active
                    .hypotheses
                    .iter()
                    .filter(|hypothesis| {
                        hypothesis.status == "ACTIVE"
                            && hypothesis
                                .contract
                                .as_ref()
                                .is_some_and(|contract| contract.parent_contract_id.is_some())
                    })
                    .find_map(|hypothesis| {
                        let Some(loop_state) = active
                            .loops
                            .iter()
                            .find(|loop_state| loop_state.hypothesis_id == hypothesis.id)
                        else {
                            return Some(format!(
                                "activated review revision {} has no persisted loop",
                                hypothesis.id
                            ));
                        };
                        if !active
                            .cadences
                            .iter()
                            .any(|cadence| cadence.loop_id == loop_state.id)
                        {
                            return Some(format!(
                                "activated review revision {} has no persisted loop cadence",
                                hypothesis.id
                            ));
                        }
                        if !restored_allocations.iter().rev().any(|allocation| {
                            allocation
                                .allocations
                                .iter()
                                .any(|entry| entry.loop_id == loop_state.id)
                        }) {
                            return Some(format!(
                                "activated review revision {} is absent from persisted capital allocation",
                                hypothesis.id
                            ));
                        }
                        None
                    })
            }));
            if let Some(reason) = incomplete_revision {
                let recovery_id = Uuid::new_v4().to_string();
                let recovery_event = event(
                    &recovery_id,
                    run_id,
                    None,
                    "run_recovery_blocked",
                    "run",
                    run_id,
                    self.active_runs
                        .get(run_id)
                        .map(|active| active.last_event_id.as_str()),
                    "An interrupted contract revision could not be proven complete; run is being stopped fail-safe",
                    json!({"reason":reason.clone(),"action":"stop_and_flatten"}),
                );
                self.store.append_event(&recovery_event)?;
                if let Some(active) = self.active_runs.get_mut(run_id) {
                    active.last_event_id = recovery_id;
                }
                self.recovery_blocked_runs.insert(run_id.clone(), reason);
                self.immediate_cycle_runs.insert(run_id.clone());
                continue;
            }
            if let Some(hypothesis) = pending_draft {
                let contract = hypothesis
                    .contract
                    .clone()
                    .context("pending DRAFT hypothesis is missing its contract")?;
                let thesis = pending_thesis_versions
                    .iter()
                    .find(|item| item.id == hypothesis.thesis_version_id)
                    .cloned()
                    .context("pending DRAFT contract is missing its thesis projection")?;
                let context = pending_context_versions
                    .iter()
                    .find(|item| item.id == hypothesis.context_version_id)
                    .cloned()
                    .context("pending DRAFT contract is missing its context projection")?;
                let definition = pending_context_definitions
                    .iter()
                    .find(|item| item.id == context.definition_id)
                    .cloned()
                    .context("pending DRAFT contract is missing its context definition")?;
                let stored_events = self.store.stored_events(run_id)?;
                let draft_event = stored_events
                    .iter()
                    .find(|stored| {
                        stored.event.kind == "hypothesis_version_created"
                            && stored.event.aggregate_id == contract.id
                            && stored.event.id == hypothesis.created_by_event_id
                    })
                    .context("pending DRAFT contract is missing its canonical creation event")?;
                if draft_event.event.run_id != *run_id
                    || draft_event.event.aggregate_type != "hypothesis_contract"
                {
                    bail!("pending DRAFT contract creation event has inconsistent identity");
                }
                let persisted_draft: HypothesisDefinition = serde_json::from_value(
                    draft_event
                        .event
                        .payload
                        .get("record")
                        .cloned()
                        .context("pending DRAFT event omitted its hypothesis record")?,
                )
                .context("pending DRAFT event contains a malformed hypothesis record")?;
                if serde_json::to_value(&persisted_draft)? != serde_json::to_value(&hypothesis)?
                    || self
                        .active_runs
                        .get(run_id)
                        .is_none_or(|active| contract.proposal.user_objective != active.thesis)
                    || thesis.thesis != contract.proposal.thesis
                    || context.definition_id != definition.id
                    || context.run_id != *run_id
                    || definition.run_id != *run_id
                {
                    bail!("pending DRAFT contract or its projections differ from their canonical records");
                }
                let thesis_event = stored_events
                    .iter()
                    .find(|stored| stored.event.id == thesis.created_by_event_id)
                    .context("pending DRAFT thesis creation event is missing")?;
                let definition_event = stored_events
                    .iter()
                    .find(|stored| stored.event.id == definition.created_by_event_id)
                    .context("pending DRAFT context-definition event is missing")?;
                let context_event = stored_events
                    .iter()
                    .find(|stored| stored.event.id == context.created_by_event_id)
                    .context("pending DRAFT context-version event is missing")?;
                if thesis_event.event.kind != "thesis_version_created"
                    || thesis_event.event.payload.get("record")
                        != Some(&serde_json::to_value(&thesis)?)
                    || definition_event.event.kind != "context_definition_created"
                    || definition_event.event.causation_event_id.as_deref()
                        != Some(thesis_event.event.id.as_str())
                    || definition_event.event.payload.get("record")
                        != Some(&serde_json::to_value(&definition)?)
                    || context_event.event.kind != "context_version_created"
                    || context_event.event.causation_event_id.as_deref()
                        != Some(definition_event.event.id.as_str())
                    || context_event.event.payload.get("record")
                        != Some(&serde_json::to_value(&context)?)
                    || draft_event.event.causation_event_id.as_deref()
                        != Some(context_event.event.id.as_str())
                {
                    bail!("pending DRAFT contract projections are not linked by their canonical event chain");
                }
                let (retrieval_trace, additional_evidence_ids, independently_fresh_evidence_ids) =
                    parse_contract_evidence_provenance(&draft_event.event.payload)?;
                let verified_fresh_ids = validate_contract_review_evidence_provenance(
                    &stored_events,
                    &thesis_event.event,
                    run_id,
                    hypothesis.parent_hypothesis_id.as_deref(),
                    &contract.proposal,
                    retrieval_trace.as_ref(),
                    &additional_evidence_ids,
                    &independently_fresh_evidence_ids,
                )?;
                let warmup_detail = self
                    .store
                    .stored_events(run_id)?
                    .into_iter()
                    .rev()
                    .find(|stored| {
                        stored.event.kind == "live_context_resolution_failed"
                            && stored
                                .event
                                .payload
                                .get("startupWarmup")
                                .and_then(serde_json::Value::as_bool)
                                == Some(true)
                    })
                    .and_then(|stored| {
                        stored
                            .event
                            .payload
                            .get("error")
                            .and_then(serde_json::Value::as_str)
                            .map(str::to_owned)
                    });
                let compiled = crate::contracts::CompiledContract {
                    contract,
                    thesis,
                    context_definition: definition,
                    context,
                    hypothesis,
                    activated_by_event_id: String::new(),
                };
                self.pending_startup_activations.insert(
                    run_id.clone(),
                    PendingStartupActivation {
                        draft_event_id: draft_event.event.id.clone(),
                        compiled,
                        retrieval_trace,
                        additional_evidence_ids,
                        independently_fresh_evidence_ids: verified_fresh_ids,
                        last_warmup_detail: warmup_detail,
                    },
                );
            }
            // Network reconciliation and secondary indexing run after the
            // window exists. The first scheduled cycle reconciles broker truth
            // before any Jev decision or new entry.
            self.immediate_cycle_runs.insert(run_id.clone());
        }
        Ok(run_ids
            .into_iter()
            .filter(|id| self.active_runs.contains_key(id))
            .collect())
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
            let matching_loops = active
                .loops
                .iter()
                .filter(|loop_state| loop_state.state != "stopped")
                .filter(|loop_state| {
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
                .collect::<Vec<_>>();
            if matching_loops.len() != 1 {
                bail!(
                    "cannot safely attribute broker-only position {} for {}: found {} active matching loops",
                    broker_position.broker_position_id,
                    broker_position.instrument,
                    matching_loops.len()
                );
            }
            let loop_state = matching_loops[0].clone();
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
            raw_fix_report: None,
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

    pub fn reconcile_broker(&mut self, run_id: &str) -> Result<bool> {
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
                return Ok(false);
            }
        };
        if !snapshot.connected || !snapshot.complete {
            let reason = if !snapshot.connected {
                "broker reported disconnected"
            } else {
                "broker position snapshot was incomplete"
            };
            let degraded = event(
                &event_id,
                run_id,
                None,
                "broker_sync_degraded",
                "broker_sync",
                &event_id,
                Some(&causation),
                "Incomplete broker snapshot recorded without mutating canonical positions",
                json!({"reason":reason,"record":snapshot}),
            );
            self.store.append_event(&degraded)?;
            self.active_runs.get_mut(run_id).unwrap().last_event_id = event_id;
            return Ok(false);
        }
        let known_broker_ids = self
            .active_runs
            .values()
            .flat_map(|active| active.positions.iter())
            .filter(|position| position_may_have_exposure(position))
            .filter_map(|position| position.broker_position_id.as_deref())
            .collect::<std::collections::HashSet<_>>();
        let mut seen_broker_ids = std::collections::HashSet::new();
        let invalid_position = snapshot.positions.iter().find_map(|position| {
            let invalid = if position.broker_position_id.trim().is_empty() {
                Some("broker position omitted its ID")
            } else if !seen_broker_ids.insert(position.broker_position_id.as_str()) {
                Some("broker snapshot contains a duplicate position ID")
            } else if position.instrument.trim().is_empty() {
                Some("broker position omitted its instrument")
            } else if !matches!(position.side.as_str(), "BUY" | "SELL") {
                Some("broker position has an unsupported side")
            } else if !position.quantity.is_finite() || position.quantity <= 0.0 {
                Some("broker position has invalid quantity")
            } else if position
                .average_price
                .is_some_and(|price| !price.is_finite() || price <= 0.0)
            {
                Some("broker position has invalid average entry price")
            } else if !known_broker_ids.contains(position.broker_position_id.as_str())
                && position.average_price.is_none()
            {
                Some("untracked broker position omitted a valid average entry price")
            } else {
                None
            };
            invalid.map(|reason| (position.broker_position_id.as_str(), reason))
        });
        if let Some((broker_position_id, reason)) = invalid_position {
            let degraded = event(
                &event_id,
                run_id,
                None,
                "broker_sync_degraded",
                "broker_sync",
                &event_id,
                Some(&causation),
                "Broker snapshot failed position validation; Jev and order evaluation were skipped",
                json!({"reason":reason,"brokerPositionId":broker_position_id,"record":snapshot}),
            );
            self.store.append_event(&degraded)?;
            self.active_runs.get_mut(run_id).unwrap().last_event_id = event_id;
            return Ok(false);
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
                                && position_may_have_exposure(position)
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
                    position_may_have_exposure(position)
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
                .filter(|position| position_may_have_exposure(position))
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
        Ok(true)
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

fn validate_required_contract_context(
    proposal: &crate::contracts::HypothesisContractDraft,
    snapshot: &ResolvedContextSnapshot,
    allow_simulated_source: bool,
) -> Result<()> {
    let now = Utc::now();
    for requirement in proposal
        .context_requirements
        .iter()
        .filter(|item| item.required)
    {
        match requirement.value_type {
            crate::contracts::ContextValueType::Quote => {
                if !crate::freshness::is_fresh(
                    now,
                    snapshot.quote.received_at,
                    requirement.maximum_age_seconds,
                ) {
                    bail!(
                        "required quote context {} is stale or future-dated",
                        requirement.id
                    );
                }
            }
            crate::contracts::ContextValueType::Candle => {
                let period = requirement
                    .period
                    .as_ref()
                    .context("required candle context has no period")?;
                let expected_source = match requirement.source {
                    crate::contracts::ContextSource::CTraderFix => "ctrader-fix",
                    crate::contracts::ContextSource::TwelveDataRest => "twelve-data-rest",
                    _ => bail!(
                        "required candle context {} uses an unsupported source",
                        requirement.id
                    ),
                };
                let explicit_source = proposal
                    .live_context_spec
                    .series_sources
                    .get(&format!("{:?}", period))
                    .map(String::as_str);
                let mut candles = snapshot
                    .candles
                    .iter()
                    .filter(|candle| {
                        let is_fix_source = candle.provenance.contains("ctrader-fix")
                            || candle.provenance.contains("ctrader-open-api");
                        let is_requested_source = if expected_source == "ctrader-fix" {
                            is_fix_source
                        } else if explicit_source.is_none() {
                            candle.provenance.contains("twelve-data-rest") || is_fix_source
                        } else {
                            candle.provenance.contains(expected_source)
                        };
                        candle.period == *period
                            && candle.closed
                            && (is_requested_source
                                || (allow_simulated_source
                                    && candle.provenance.contains("simulated")))
                    })
                    .collect::<Vec<_>>();
                candles.sort_by_key(|candle| candle.open_time);
                if candles.len() < requirement.lookback.unwrap_or(u32::MAX) as usize {
                    bail!(
                        "required candle context {} lacks its declared closed-bar lookback from {}",
                        requirement.id,
                        expected_source
                    );
                }
                let latest_close = candles
                    .last()
                    .map(|candle| candle.open_time + chrono::Duration::seconds(period.seconds()))
                    .context(format!(
                        "required candle context {} has no completed bars",
                        requirement.id
                    ))?;
                if !crate::freshness::is_fresh(now, latest_close, requirement.maximum_age_seconds) {
                    bail!(
                        "required candle context {} is stale or future-dated",
                        requirement.id
                    );
                }
            }
            crate::contracts::ContextValueType::Indicator => {
                let field = snapshot
                    .fields
                    .iter()
                    .find(|field| field.field_id == requirement.id)
                    .with_context(|| {
                        format!(
                            "required indicator context {} was not resolved",
                            requirement.id
                        )
                    })?;
                if !crate::freshness::is_fresh(
                    now,
                    field.observed_at,
                    requirement.maximum_age_seconds,
                ) {
                    bail!(
                        "required indicator context {} is stale or future-dated",
                        requirement.id
                    );
                }
                let expected_source = match requirement.source {
                    crate::contracts::ContextSource::CTraderFix => "ctrader-fix",
                    crate::contracts::ContextSource::TwelveDataRest => "twelve-data-rest",
                    _ => bail!(
                        "required indicator context {} uses an unsupported source",
                        requirement.id
                    ),
                };
                let period = requirement
                    .period
                    .as_ref()
                    .expect("indicator period validated above");
                let explicit_source = proposal
                    .live_context_spec
                    .series_sources
                    .get(&format!("{:?}", period))
                    .map(String::as_str);
                let source_resolved = field.provenance.iter().any(|source| {
                    if expected_source == "ctrader-fix" {
                        source.contains("ctrader-fix") || source.contains("ctrader-open-api")
                    } else if explicit_source.is_none() {
                        source.contains("twelve-data-rest")
                            || source.contains("ctrader-fix")
                            || source.contains("ctrader-open-api")
                    } else {
                        source.contains(expected_source)
                    }
                });
                if !source_resolved
                    && !(allow_simulated_source
                        && field
                            .provenance
                            .iter()
                            .any(|source| source.contains("simulated")))
                {
                    bail!(
                        "required indicator context {} was not resolved from {}",
                        requirement.id,
                        expected_source
                    );
                }
            }
            crate::contracts::ContextValueType::Evidence => {
                let is_cited = proposal
                    .supporting_evidence_ids
                    .iter()
                    .chain(&proposal.contradictory_evidence_ids)
                    .any(|id| id == &requirement.id);
                if !is_cited {
                    bail!(
                        "required evidence context {} is not cited by the contract",
                        requirement.id
                    );
                }
            }
            crate::contracts::ContextValueType::Account
            | crate::contracts::ContextValueType::Position => {
                bail!("required {} context {} has no runtime resolver for Jev; the contract cannot activate", format!("{:?}", requirement.value_type).to_ascii_lowercase(), requirement.id);
            }
        }
    }
    Ok(())
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
        raw_fix_report: None,
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
        && execution.filled_quantity + ORDER_FILL_QUANTITY_TOLERANCE >= order.quantity
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

    #[derive(Clone)]
    struct HumanVerifiedCycleProbeBroker {
        approvals: Arc<std::sync::atomic::AtomicUsize>,
        executions: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl ExecutionBroker for HumanVerifiedCycleProbeBroker {
        fn execute(&self, request: &TradeRequest) -> Result<ExecutionReceipt> {
            self.executions.fetch_add(1, Ordering::SeqCst);
            SimulatedBroker.execute(request)
        }

        fn close(
            &self,
            position: &PositionRecord,
            order: &OrderRecord,
            caused_by_decision_id: &str,
            execution_event_id: &str,
        ) -> Result<ExecutionReceipt> {
            SimulatedBroker.close(position, order, caused_by_decision_id, execution_event_id)
        }

        fn reference_price(&self, _instrument: &str) -> Result<Option<f64>> {
            Ok(Some(50_000.0))
        }

        fn reconcile(&self) -> Result<BrokerSnapshot> {
            SimulatedBroker.reconcile()
        }

        fn risk_account_identity(&self) -> Option<BrokerAccountIdentity> {
            Some(BrokerAccountIdentity {
                account_id: "synthetic-demo-account-id".into(),
                environment: "demo".into(),
            })
        }

        fn approve_manual_entry_limits(
            &self,
            _instrument: &str,
            _minimum: f64,
            _step: f64,
        ) -> Result<()> {
            self.approvals.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn approve_demo_entry_without_volume_metadata(&self, _instrument: &str) -> Result<()> {
            self.approvals.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn human_verified_cycle_uses_account_snapshot_for_one_simulated_entry() {
        let runtime = tempfile::tempdir().expect("isolated harness runtime");
        let store = CanonicalStore::open(runtime.path()).expect("open isolated canonical store");
        let approvals = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let executions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let broker = HumanVerifiedCycleProbeBroker {
            approvals: Arc::clone(&approvals),
            executions: Arc::clone(&executions),
        };
        let mut risk_policy = RiskPolicyConfig::default();
        risk_policy.quantity_step = 0.01;
        let mut harness = HarnessController::with_optional_retrieval_adapters(
            store.clone(),
            0.60,
            None,
            Box::new(SimulatedWorldModel),
            Box::new(SimulatedJev),
            Box::new(broker),
            risk_policy,
        );
        harness.configure_market_data(Arc::new(
            crate::market_data::SimulatedMarketDataProvider::new(),
        ));
        let started = harness
            .start("Test a BTCUSD long strategy using completed candles")
            .expect("start simulated harness");
        let snapshot = BrokerRiskSnapshot {
            account_id: "synthetic-demo-account-id".into(),
            environment: "demo".into(),
            equity: 999.0,
            free_margin: 999.0,
            deposit_asset_id: "USD".into(),
            deposit_currency_code: "USD".into(),
            observed_at: Utc::now(),
            account_open_exposure: 0.0,
            quote_to_deposit: HashMap::from([("BTCUSD".into(), 1.0)]),
        };

        let result = harness
            .run_human_verified_live_cycle(&started.run_id, snapshot, None, None, true)
            .expect("verified snapshot should authorize one simulated decision cycle");
        assert_eq!(approvals.load(Ordering::SeqCst), 1);
        assert_eq!(executions.load(Ordering::SeqCst), 1);
        assert_eq!(result.positions.len(), 1);

        let events = store.stored_events(&started.run_id).unwrap();
        assert!(events
            .iter()
            .any(|stored| stored.event.kind == "human_live_risk_verified"));
        let evaluated = events
            .iter()
            .find(|stored| stored.event.kind == "order_evaluated")
            .expect("deterministic risk engine must evaluate the verified entry");
        assert_eq!(evaluated.event.payload["result"]["accepted"], true);
        assert!(
            evaluated.event.payload["record"]["quantity"]
                .as_f64()
                .unwrap()
                > 0.0
        );
        assert!(events.iter().any(|stored| {
            stored.event.kind == "execution_recorded"
                && stored.event.payload["record"]["status"] == "simulated-filled"
        }));
    }

    struct MissingLifecycleSkillWorldModel;

    impl WorldModel for MissingLifecycleSkillWorldModel {
        fn formulate(
            &self,
            run_id: &str,
            human_thesis: &str,
            retriever: Option<&dyn ContextRetriever>,
        ) -> Result<WorldModelStartupOutput> {
            let mut output = SimulatedWorldModel.formulate(run_id, human_thesis, retriever)?;
            output.contract.skill_invocations.clear();
            Ok(output)
        }

        fn review_hypothesis(
            &self,
            package: &WorldModelReviewPackage,
        ) -> Result<HypothesisReviewDecision> {
            SimulatedWorldModel.review_hypothesis(package)
        }

        fn critique(
            &self,
            thesis: &ThesisVersion,
            context: &ContextVersion,
            evidence: &str,
        ) -> Result<String> {
            SimulatedWorldModel.critique(thesis, context, evidence)
        }

        fn research(
            &self,
            retriever: &dyn ContextRetriever,
            request: &RetrievalRequest,
        ) -> Result<RetrievalTrace> {
            SimulatedWorldModel.research(retriever, request)
        }
    }

    #[test]
    fn harness_normalizes_omitted_lifecycle_skill_before_persisting_contract() {
        let runtime = tempfile::tempdir().expect("isolated harness runtime");
        let store = CanonicalStore::open(runtime.path()).expect("open isolated canonical store");
        let mut harness = HarnessController::with_runtime_adapters(
            store,
            0.60,
            Box::new(MissingLifecycleSkillWorldModel),
            Box::new(SimulatedJev),
        );

        let snapshot = harness
            .start("Test BTCUSD and stop after 3 completed trades or 4 days")
            .expect("omitted optional model skill is supplied at the harness boundary");

        let contract = snapshot.hypotheses[0]
            .contract
            .as_ref()
            .expect("compiled hypothesis contract");
        assert_eq!(contract.proposal.skill_invocations.len(), 1);
        assert_eq!(
            contract.proposal.stop_limits.maximum_completed_trades,
            Some(3)
        );
        assert_eq!(
            contract.proposal.stop_limits.maximum_elapsed_seconds,
            Some(4 * 24 * 60 * 60)
        );
        assert_eq!(contract.proposal.review_triggers.no_trade_decisions, 12);
    }

    #[test]
    fn simulated_startup_activates_contract_before_spawning_a_loop() {
        let runtime = tempfile::tempdir().expect("isolated harness runtime");
        let store = CanonicalStore::open(runtime.path()).expect("open isolated canonical store");
        let mut harness = HarnessController::new(store, 0.60);

        // This calls only the simulated formulation, simulated market provider,
        // and canonical lifecycle path. It does not run a Jev cycle or broker.
        let snapshot = harness
            .start("BTCUSD momentum continuation for a short intraday horizon")
            .expect("simulated contract startup");

        assert_eq!(snapshot.hypotheses.len(), 1);
        assert_eq!(snapshot.loops.len(), 1);
        assert!(snapshot.positions.is_empty());
        let hypothesis = &snapshot.hypotheses[0];
        assert_eq!(hypothesis.status, "ACTIVE");
        assert_eq!(
            hypothesis.contract.as_ref().map(|contract| contract.state),
            Some(crate::contracts::ContractState::Active)
        );

        let activation = snapshot
            .events
            .iter()
            .find(|event| event.kind == "hypothesis_contract_activated")
            .expect("canonical ACTIVE transition");
        let spawned = snapshot
            .events
            .iter()
            .find(|event| event.kind == "loop_spawned")
            .expect("canonical Jev loop spawn");
        assert_eq!(
            spawned.causation_event_id.as_deref(),
            Some(activation.id.as_str())
        );
        let draft = snapshot
            .events
            .iter()
            .find(|event| {
                event.kind == "hypothesis_version_created" && event.aggregate_id == hypothesis.id
            })
            .expect("persisted DRAFT proposal");
        assert_eq!(
            activation.causation_event_id.as_deref(),
            Some(draft.id.as_str())
        );
        assert!(
            snapshot
                .events
                .iter()
                .position(|event| event.id == activation.id)
                < snapshot
                    .events
                    .iter()
                    .position(|event| event.id == spawned.id),
            "loop spawn must follow ACTIVE contract activation"
        );
        let mut draft_hypothesis = hypothesis.clone();
        draft_hypothesis.status = "DRAFT".into();
        draft_hypothesis.contract.as_mut().unwrap().state = crate::contracts::ContractState::Draft;
        let mut forbidden_loop = snapshot.loops[0].clone();
        forbidden_loop.id = "unauthorized-draft-loop".into();
        forbidden_loop.created_by_event_id = "unauthorized-draft-loop-event".into();
        let forbidden_event = event(
            &forbidden_loop.created_by_event_id,
            &snapshot.run_id,
            Some(&forbidden_loop.id),
            "loop_spawned",
            "loop",
            &forbidden_loop.id,
            Some(&activation.id),
            "must not persist a loop for a DRAFT contract",
            json!({"record":forbidden_loop}),
        );
        let events_before_rejected_spawn =
            harness.store.stored_events(&snapshot.run_id).unwrap().len();
        assert!(harness
            .spawn_loop_for_active_contract(
                &draft_hypothesis,
                &activation.id,
                &forbidden_loop,
                &forbidden_event,
            )
            .is_err());
        assert_eq!(
            harness.store.stored_events(&snapshot.run_id).unwrap().len(),
            events_before_rejected_spawn,
            "the DRAFT rejection must not persist an event or loop"
        );
        assert!(!snapshot.events.iter().any(|event| {
            matches!(
                event.kind.as_str(),
                "jev1_decision_recorded" | "order_evaluated" | "execution_recorded"
            )
        }));

        let replayed = replay::replay_run(&harness.store, &snapshot.run_id)
            .expect("new contract and loop replay from canonical events");
        assert_eq!(replayed.hypotheses[0].status, "ACTIVE");
        assert_eq!(replayed.loops.len(), 1);

        drop(harness);
        let store = CanonicalStore::open(runtime.path()).expect("reopen isolated canonical store");
        let mut recovered = HarnessController::new(store, 0.60);
        assert_eq!(
            recovered.restore_active_runs().unwrap(),
            vec![snapshot.run_id.clone()]
        );
        let restored = recovered.run_snapshot(&snapshot.run_id).unwrap();
        assert_eq!(restored.hypotheses[0].status, "ACTIVE");
        assert_eq!(restored.loops.len(), 1);
    }

    #[test]
    fn simulated_modify_reenters_activation_gate_before_spawning_revision_loop() {
        let runtime = tempfile::tempdir().expect("isolated harness runtime");
        let store = CanonicalStore::open(runtime.path()).expect("open isolated canonical store");
        let mut harness = HarnessController::new(store, 0.60);
        let initial = harness
            .start("BTCUSD momentum continuation for a short intraday horizon")
            .expect("simulated initial contract startup");
        let original_loop_id = initial.loops[0].id.clone();
        let original_hypothesis_id = initial.hypotheses[0].id.clone();

        let revised = harness
            .review_hypothesis(&HypothesisReviewRequest {
                run_id: initial.run_id.clone(),
                hypothesis_id: original_hypothesis_id,
                evidence_query: "modify the timeframe to test a longer horizon".into(),
            })
            .expect("simulated MODIFY contract proposal");

        let revised_hypothesis = revised
            .hypotheses
            .iter()
            .find(|hypothesis| hypothesis.id != initial.hypotheses[0].id)
            .expect("new hypothesis version");
        assert_eq!(revised_hypothesis.status, "ACTIVE");
        assert_eq!(
            revised_hypothesis
                .contract
                .as_ref()
                .map(|contract| contract.state),
            Some(crate::contracts::ContractState::Active)
        );
        let old_loop = revised
            .loops
            .iter()
            .find(|loop_state| loop_state.id == original_loop_id)
            .expect("prior loop retained for replay");
        assert_eq!(old_loop.state, "stopped");
        let new_loop = revised
            .loops
            .iter()
            .find(|loop_state| loop_state.hypothesis_id == revised_hypothesis.id)
            .expect("revision loop");
        let activation = revised
            .events
            .iter()
            .find(|event| {
                event.kind == "hypothesis_contract_activated"
                    && event.aggregate_id == revised_hypothesis.id
            })
            .expect("revision activation event");
        let spawn = revised
            .events
            .iter()
            .find(|event| event.kind == "loop_spawned" && event.aggregate_id == new_loop.id)
            .expect("revision loop spawn event");
        assert_eq!(
            spawn.causation_event_id.as_deref(),
            Some(activation.id.as_str())
        );
        let replayed = replay::replay_run(&harness.store, &initial.run_id)
            .expect("revision lifecycle replays from canonical events");
        assert!(replayed
            .hypotheses
            .iter()
            .any(|item| item.id == revised_hypothesis.id));
        assert!(replayed.loops.iter().any(|item| item.id == new_loop.id));
        assert!(!revised.events.iter().any(|event| {
            matches!(
                event.kind.as_str(),
                "jev1_decision_recorded" | "order_evaluated" | "execution_recorded"
            )
        }));
    }

    #[test]
    fn split_candidate_reenters_activation_gate_before_spawning_candidate_loop() {
        let runtime = tempfile::tempdir().expect("isolated harness runtime");
        let store = CanonicalStore::open(runtime.path()).expect("open isolated canonical store");
        let mut harness = HarnessController::new(store, 0.60);
        let initial = harness
            .start("BTCUSD momentum continuation for a short intraday horizon")
            .expect("simulated initial contract startup");
        let request = HypothesisReviewRequest {
            run_id: initial.run_id.clone(),
            hypothesis_id: initial.hypotheses[0].id.clone(),
            evidence_query: "test a distinct competing mechanism".into(),
        };
        let mut package = harness
            .assemble_review_package(&request)
            .expect("assemble split review package");
        for index in 0..2 {
            package.evidence.push(ReviewEvidenceItem {
                relationship: EvidenceRelationship::Supporting,
                hit: RetrievalHit {
                    point_id: format!("point-{index}"),
                    score: 0.99,
                    id: format!("evidence-{index}"),
                    run_id: initial.run_id.clone(),
                    title: format!("Trusted support {index}"),
                    text: "Distinct canonical support for competing mechanism".into(),
                    source_class: "internal_canonical".into(),
                    trust_level: "internal".into(),
                    provenance_uri: format!("canonical://event/{index}"),
                    publisher: "harness".into(),
                    observed_at: Utc::now().to_rfc3339(),
                    ingested_at: Utc::now().to_rfc3339(),
                    content_sha256: format!("sha-{index}"),
                    canonical_entity_type: "experiment".into(),
                    canonical_entity_id: format!("support-{index}"),
                    canonical_event_id: format!("support-event-{index}"),
                    tags: Vec::new(),
                    metadata: json!({}),
                },
            });
        }
        let mut proposal = package
            .current_hypothesis
            .contract
            .as_ref()
            .unwrap()
            .proposal
            .clone();
        proposal.mechanism = "competing mean-reversion mechanism".into();
        proposal.thesis = "Test whether competing mean reversion explains BTCUSD movement.".into();
        proposal.expected_behavior = "Moves toward the prior range after an extension.".into();
        proposal.jev1_objective =
            "Does evidence support the competing mean-reversion hypothesis?".into();
        let decision = HypothesisReviewDecision {
            id: Uuid::new_v4().to_string(),
            run_id: initial.run_id.clone(),
            hypothesis_id: request.hypothesis_id.clone(),
            action: HypothesisAction::Split,
            rationale: "Test a materially distinct mechanism.".into(),
            diagnosis: "A competing explanation has independent support.".into(),
            problem_severity: "medium".into(),
            continuation_rationale: "Keep the existing hypothesis while testing the candidate."
                .into(),
            decision_confidence: 0.95,
            evidence_canonical_ids: vec!["support-0".into(), "support-1".into()],
            proposed_mechanism: Some(proposal.mechanism.clone()),
            proposed_timeframe: Some(proposal.timeframe.clone()),
            candidate_hypothesis: None,
            proposed_contract: Some(proposal),
            routing: Some(WorldModelRoutingMetadata {
                provider: "test".into(),
                base_model: "test-base".into(),
                selected_model: "test-escalation".into(),
                escalated: true,
                escalation_reasons: vec!["new competing mechanism".into()],
                base_confidence: 0.50,
                request_ids: Vec::new(),
                internet_research_used: false,
            }),
            web_evidence: Vec::new(),
            created_by_event_id: Uuid::new_v4().to_string(),
            created_at: Utc::now(),
        };
        let split = harness
            .apply_review_decision(&request, &package, decision)
            .expect("activate and spawn SPLIT candidate through shared gate");

        let candidate = split
            .hypotheses
            .iter()
            .find(|hypothesis| hypothesis.id != request.hypothesis_id)
            .expect("candidate contract");
        assert_eq!(candidate.status, "ACTIVE");
        assert_eq!(
            candidate.contract.as_ref().map(|contract| contract.state),
            Some(crate::contracts::ContractState::Active)
        );
        assert_eq!(
            candidate
                .contract
                .as_ref()
                .and_then(|contract| contract.parent_contract_id.as_deref()),
            Some(request.hypothesis_id.as_str())
        );
        assert_eq!(
            split
                .loops
                .iter()
                .filter(|loop_state| loop_state.state != "stopped")
                .count(),
            2
        );
        let candidate_loop = split
            .loops
            .iter()
            .find(|loop_state| loop_state.hypothesis_id == candidate.id)
            .expect("candidate Jev loop");
        let activation = split
            .events
            .iter()
            .find(|event| {
                event.kind == "hypothesis_contract_activated" && event.aggregate_id == candidate.id
            })
            .expect("candidate activation event");
        let spawn = split
            .events
            .iter()
            .find(|event| event.kind == "loop_spawned" && event.aggregate_id == candidate_loop.id)
            .expect("candidate loop spawn event");
        assert_eq!(
            spawn.causation_event_id.as_deref(),
            Some(activation.id.as_str())
        );
        let replayed = replay::replay_run(&harness.store, &initial.run_id)
            .expect("SPLIT candidate replays from canonical events");
        assert!(replayed
            .loops
            .iter()
            .any(|item| item.id == candidate_loop.id));
        assert!(!split.events.iter().any(|event| {
            matches!(
                event.kind.as_str(),
                "jev1_decision_recorded" | "order_evaluated" | "execution_recorded"
            )
        }));
    }

    #[test]
    fn pending_context_warmup_recovers_after_restart_before_loop_spawn() {
        let runtime = tempfile::tempdir().expect("isolated harness runtime");
        let store = CanonicalStore::open(runtime.path()).expect("open isolated canonical store");
        let mut harness = HarnessController::new(store, 0.60);
        harness.configure_market_data(Arc::new(crate::market_data::UnavailableMarketDataProvider(
            "test context is still warming".into(),
        )));
        let pending = harness
            .start("BTCUSD momentum continuation for a short intraday horizon")
            .expect("pending DRAFT warm-up is a recoverable startup state");
        assert!(pending.loops.is_empty());
        assert_eq!(pending.hypotheses[0].status, "DRAFT");
        assert_eq!(
            pending.hypotheses[0]
                .contract
                .as_ref()
                .map(|contract| contract.state),
            Some(crate::contracts::ContractState::Draft)
        );
        assert!(harness
            .store
            .stored_events(&pending.run_id)
            .unwrap()
            .iter()
            .any(|stored| {
                stored.event.kind == "live_context_resolution_failed"
                    && stored.event.payload["startupWarmup"] == true
                    && stored.event.payload["jevCalled"] == false
            }));
        drop(harness);

        let store = CanonicalStore::open(runtime.path()).expect("reopen DRAFT warm-up runtime");
        let mut recovered = HarnessController::new(store, 0.60);
        assert_eq!(
            recovered.restore_active_runs().unwrap(),
            vec![pending.run_id.clone()]
        );
        assert!(recovered.has_pending_startup_activation(&pending.run_id));
        recovered.configure_market_data(Arc::new(
            crate::market_data::SimulatedMarketDataProvider::new(),
        ));
        let activated = recovered
            .advance_pending_startup_activation(&pending.run_id)
            .unwrap()
            .expect("recovered warm-up activation");
        assert_eq!(activated.hypotheses[0].status, "ACTIVE");
        assert_eq!(activated.loops.len(), 1);
        assert!(activated.positions.is_empty());
        assert!(!activated.events.iter().any(|event| {
            matches!(
                event.kind.as_str(),
                "jev1_decision_recorded" | "order_evaluated" | "execution_recorded"
            )
        }));
        let replayed = replay::replay_run(&recovered.store, &pending.run_id)
            .expect("warm-up lifecycle replays after restart");
        assert_eq!(replayed.hypotheses[0].status, "ACTIVE");
        assert_eq!(replayed.loops.len(), 1);
    }

    #[test]
    fn deterministic_elapsed_limit_stops_simulated_loop_without_a_jev_cycle() {
        let runtime = tempfile::tempdir().expect("isolated harness runtime");
        let store = CanonicalStore::open(runtime.path()).expect("open isolated canonical store");
        let mut harness = HarnessController::new(store, 0.60);
        let started = harness
            .start("BTCUSD momentum continuation; stop after 1 second")
            .expect("simulated time-limited startup");
        assert_eq!(
            started.hypotheses[0]
                .contract
                .as_ref()
                .unwrap()
                .proposal
                .stop_limits
                .maximum_elapsed_seconds,
            Some(1)
        );
        std::thread::sleep(std::time::Duration::from_millis(1_100));
        harness.enforce_due_contract_stops(&started.run_id).unwrap();

        let stopped = harness.run_snapshot(&started.run_id).unwrap();
        assert_eq!(stopped.loops[0].state, "stopped");
        assert_eq!(stopped.hypotheses[0].status, "ACTIVE");
        assert!(harness
            .store
            .stored_events(&started.run_id)
            .unwrap()
            .iter()
            .any(|stored| {
                stored.event.kind == "contract_stop_limit_triggered"
                    && stored.event.payload["reasons"]
                        .as_array()
                        .is_some_and(|reasons| {
                            reasons
                                .iter()
                                .any(|reason| reason == "maximum_elapsed_time")
                        })
            }));
        assert!(!stopped.events.iter().any(|event| {
            matches!(
                event.kind.as_str(),
                "jev1_decision_recorded" | "order_evaluated" | "execution_recorded"
            )
        }));
    }

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

    #[test]
    fn account_exposure_includes_open_positions_from_other_active_runs() {
        let runtime = tempfile::tempdir().expect("isolated harness runtime");
        let store = CanonicalStore::open(runtime.path()).expect("open isolated canonical store");
        let mut harness = HarnessController::new(store, 0.60);
        let first = harness
            .start("First BTCUSD campaign")
            .expect("start first run");
        let second = harness
            .start("Second ETHUSD campaign")
            .expect("start second run");

        for (snapshot, position_id, broker_id, notional) in [
            (&first, "position-a", "account-position-a", 15_000.0),
            (&second, "position-b", "account-position-b", 20_000.0),
        ] {
            let loop_id = snapshot.loops[0].id.clone();
            let active = harness.active_runs.get_mut(&snapshot.run_id).unwrap();
            active.positions.push(PositionRecord {
                id: position_id.into(),
                run_id: snapshot.run_id.clone(),
                loop_id,
                opened_by_execution_id: format!("execution-{position_id}"),
                broker_position_id: Some(broker_id.into()),
                direction: "Long".into(),
                state: "open".into(),
                opened_at: Utc::now(),
                closed_by_execution_id: None,
                closed_at: None,
                last_event_id: format!("event-{position_id}"),
            });
            active.position_controls.push(PositionControlRecord {
                position_id: position_id.into(),
                order_id: format!("order-{position_id}"),
                instrument: "BTCUSD".into(),
                quantity: 0.15,
                entry_price: 100_000.0,
                notional,
                stop_loss_price: 99_000.0,
                created_by_event_id: format!("control-{position_id}"),
                created_at: Utc::now(),
            });
        }

        // Simulated positions are marked at the current broker reference
        // price (100), not at their historical entry/notional fields.
        assert_eq!(harness.current_account_open_exposure().unwrap(), 30.0);

        let second_active = harness.active_runs.get_mut(&second.run_id).unwrap();
        second_active.positions[0].broker_position_id = Some("account-position-a".into());
        assert!(harness.current_account_open_exposure().is_err());
    }

    #[test]
    fn final_lifecycle_stop_also_stops_the_run_and_avoids_missing_cadence() {
        let runtime = tempfile::tempdir().expect("isolated harness runtime");
        let store = CanonicalStore::open(runtime.path()).expect("open isolated canonical store");
        let mut harness = HarnessController::new(store, 0.60);
        let started = harness
            .start("Test BTCUSD and stop after 1 second")
            .expect("start short lifecycle run");

        std::thread::sleep(std::time::Duration::from_millis(1_100));
        harness
            .enforce_due_contract_stops(&started.run_id)
            .expect("enforce due contract limit");

        assert!(!harness.is_active(&started.run_id));
        let stopped = harness
            .run_snapshot(&started.run_id)
            .expect("read stopped canonical run");
        assert_eq!(stopped.status, "stopped");
        assert!(stopped
            .loops
            .iter()
            .all(|loop_state| loop_state.state == "stopped"));
        let wrapups = harness.take_stop_wrapups(&started.run_id);
        assert_eq!(wrapups.len(), 1);
        assert_eq!(wrapups[0].run_id, started.run_id);
        assert_eq!(wrapups[0].current_loop.id, started.loops[0].id);
        assert_eq!(wrapups[0].current_loop.state, "stopped");
    }
}

#[cfg(test)]
mod broker_reconciliation_tests {
    use super::*;

    #[derive(Clone, Copy)]
    enum ReconciliationReply {
        Error,
        Incomplete,
        MissingAveragePrice,
        InvalidAveragePrice,
        DuplicatePositionId,
        SimulatedInvalidPosition,
    }

    struct ReconciliationProbeBroker(ReconciliationReply);

    impl ExecutionBroker for ReconciliationProbeBroker {
        fn execute(&self, _request: &TradeRequest) -> Result<ExecutionReceipt> {
            bail!("probe broker must not execute an order")
        }

        fn close(
            &self,
            _position: &PositionRecord,
            _order: &OrderRecord,
            _caused_by_decision_id: &str,
            _execution_event_id: &str,
        ) -> Result<ExecutionReceipt> {
            bail!("probe broker must not close a position")
        }

        fn reference_price(&self, _instrument: &str) -> Result<Option<f64>> {
            Ok(Some(100.0))
        }

        fn supports_instrument(&self, _instrument: &str) -> Result<bool> {
            Ok(true)
        }

        fn reconcile(&self) -> Result<BrokerSnapshot> {
            match self.0 {
                ReconciliationReply::Error => bail!("simulated broker snapshot transport failure"),
                ReconciliationReply::Incomplete => Ok(BrokerSnapshot {
                    adapter: "ctrader-fix".into(),
                    connected: true,
                    complete: false,
                    positions: Vec::new(),
                    observed_at: Utc::now(),
                }),
                ReconciliationReply::MissingAveragePrice => Ok(BrokerSnapshot {
                    adapter: "ctrader-fix".into(),
                    connected: true,
                    complete: true,
                    positions: vec![BrokerPosition {
                        broker_position_id: "untracked-position".into(),
                        instrument: "BTCUSD".into(),
                        side: "BUY".into(),
                        quantity: 0.5,
                        average_price: None,
                    }],
                    observed_at: Utc::now(),
                }),
                ReconciliationReply::InvalidAveragePrice => Ok(BrokerSnapshot {
                    adapter: "ctrader-fix".into(),
                    connected: true,
                    complete: true,
                    positions: vec![BrokerPosition {
                        broker_position_id: "untracked-position".into(),
                        instrument: "BTCUSD".into(),
                        side: "BUY".into(),
                        quantity: 0.5,
                        average_price: Some(-1.0),
                    }],
                    observed_at: Utc::now(),
                }),
                ReconciliationReply::DuplicatePositionId => Ok(BrokerSnapshot {
                    adapter: "ctrader-fix".into(),
                    connected: true,
                    complete: true,
                    positions: vec![
                        BrokerPosition {
                            broker_position_id: "duplicate-position".into(),
                            instrument: "BTCUSD".into(),
                            side: "BUY".into(),
                            quantity: 0.5,
                            average_price: Some(100.0),
                        },
                        BrokerPosition {
                            broker_position_id: "duplicate-position".into(),
                            instrument: "BTCUSD".into(),
                            side: "BUY".into(),
                            quantity: 0.25,
                            average_price: Some(100.0),
                        },
                    ],
                    observed_at: Utc::now(),
                }),
                ReconciliationReply::SimulatedInvalidPosition => Ok(BrokerSnapshot {
                    adapter: "simulated".into(),
                    connected: true,
                    complete: true,
                    positions: vec![BrokerPosition {
                        broker_position_id: String::new(),
                        instrument: "BTCUSD".into(),
                        side: "BUY".into(),
                        quantity: 0.5,
                        average_price: Some(100.0),
                    }],
                    observed_at: Utc::now(),
                }),
            }
        }
    }

    #[test]
    fn broker_reconciliation_failures_stop_the_cycle_before_jev_or_orders() {
        for (reply, expected_event) in [
            (ReconciliationReply::Error, "broker_sync_failed"),
            (ReconciliationReply::Incomplete, "broker_sync_degraded"),
            (
                ReconciliationReply::MissingAveragePrice,
                "broker_sync_degraded",
            ),
            (
                ReconciliationReply::InvalidAveragePrice,
                "broker_sync_degraded",
            ),
            (
                ReconciliationReply::DuplicatePositionId,
                "broker_sync_degraded",
            ),
            (
                ReconciliationReply::SimulatedInvalidPosition,
                "broker_sync_degraded",
            ),
        ] {
            let runtime = tempfile::tempdir().expect("isolated harness runtime");
            let store = CanonicalStore::open(runtime.path()).expect("open canonical store");
            let mut harness = HarnessController::new(store, 0.60);
            let started = harness
                .start("BTCUSD momentum continuation for a short intraday horizon")
                .expect("simulated contract startup");
            harness.broker = Box::new(ReconciliationProbeBroker(reply));

            let after_cycle = harness
                .run_cycle(&started.run_id)
                .expect("failed reconciliation is reported without running a trade decision");

            assert!(after_cycle.positions.is_empty());
            assert!(after_cycle
                .events
                .iter()
                .any(|event| event.kind == expected_event));
            assert!(!after_cycle.events.iter().any(|event| {
                matches!(
                    event.kind.as_str(),
                    "jev1_decision_recorded"
                        | "jev2_decision_recorded"
                        | "order_evaluated"
                        | "execution_recorded"
                        | "broker_position_imported"
                )
            }));
            if matches!(reply, ReconciliationReply::MissingAveragePrice) {
                assert!(!after_cycle
                    .events
                    .iter()
                    .any(|event| event.kind == "broker_state_synchronized"));
            }
        }
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
