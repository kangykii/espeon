use crate::domain::*;
use crate::ports::*;
use anyhow::{bail, Result};
use chrono::Utc;
use uuid::Uuid;

pub struct SimulatedWorldModel;

pub struct UnavailableWorldModel(pub String);

impl WorldModel for UnavailableWorldModel {
    fn formulate(
        &self,
        _: &str,
        _: &str,
        _: Option<&dyn ContextRetriever>,
    ) -> Result<WorldModelStartupOutput> {
        bail!(self.0.clone())
    }
    fn review_hypothesis(&self, _: &WorldModelReviewPackage) -> Result<HypothesisReviewDecision> {
        bail!(self.0.clone())
    }
    fn critique(&self, _: &ThesisVersion, _: &ContextVersion, _: &str) -> Result<String> {
        bail!(self.0.clone())
    }
    fn research(&self, _: &dyn ContextRetriever, _: &RetrievalRequest) -> Result<RetrievalTrace> {
        bail!(self.0.clone())
    }
}

pub struct UnavailableJev(pub String);

impl JevEngine for UnavailableJev {
    fn decide_entry(&self, _: &ResolvedJevState, _: &str) -> Result<Jev1Decision> {
        bail!(self.0.clone())
    }
    fn manage_position(&self, _: &ResolvedJevState, _: &str) -> Result<Jev2Decision> {
        bail!(self.0.clone())
    }
}

pub struct UnavailableBroker(pub String);

impl ExecutionBroker for UnavailableBroker {
    fn execute(&self, _: &TradeRequest) -> Result<ExecutionReceipt> {
        bail!(self.0.clone())
    }
    fn close(
        &self,
        _: &PositionRecord,
        _: &OrderRecord,
        _: &str,
        _: &str,
    ) -> Result<ExecutionReceipt> {
        bail!(self.0.clone())
    }
    fn reference_price(&self, _: &str) -> Result<Option<f64>> {
        bail!(self.0.clone())
    }
    fn supports_instrument(&self, _: &str) -> Result<bool> {
        Ok(false)
    }
    fn reconcile(&self) -> Result<BrokerSnapshot> {
        bail!(self.0.clone())
    }
}

pub const WORLD_MODEL_SYSTEM_PROMPT: &str = include_str!("../../prompts/world-model-system.md");

impl WorldModel for SimulatedWorldModel {
    fn formulate(
        &self,
        run_id: &str,
        human_thesis: &str,
        retriever: Option<&dyn ContextRetriever>,
    ) -> Result<WorldModelStartupOutput> {
        let instruments = infer_instruments(human_thesis);
        let mechanism = infer_mechanism(human_thesis);
        let timeframe = infer_timeframe(human_thesis);
        let lower = human_thesis.to_ascii_lowercase();
        let requires_grounding = ["current", "latest", "today", "news", "live", "official"]
            .iter()
            .any(|needle| lower.contains(needle));
        let retrieval_trace = if requires_grounding {
            retriever
                .map(|service| {
                    service.retrieve(&RetrievalRequest {
                        question: format!(
                            "Ground this proposed trading hypothesis: {human_thesis}"
                        ),
                        required_source_classes: vec!["live_official".into()],
                        exact_canonical_ids: Vec::new(),
                        limit: 8,
                        max_rounds: 3,
                        filters: RetrievalFilters::default(),
                    })
                })
                .transpose()?
        } else {
            None
        };
        let live_context_spec = crate::market_data::default_live_context_spec(
            instruments.first().map(String::as_str).unwrap_or("BTCUSD"),
        );
        let explicit_stop_limits = crate::contracts::explicit_user_stop_limits(human_thesis)?;
        let skill_invocations = vec![crate::contracts::SkillInvocation {
            skill_id: "loop.lifecycle_limits".into(),
            arguments: serde_json::json!({
                "maximumElapsedSeconds":explicit_stop_limits.maximum_elapsed_seconds,
                "maximumCompletedTrades":explicit_stop_limits.maximum_completed_trades,
                "noTradeDecisions":12,
                "consecutiveLosses":3,
                "completedTrades":10
            }),
        }];
        let evidence_ids = retrieval_trace
            .as_ref()
            .map(|trace| {
                trace
                    .hits
                    .iter()
                    .map(|hit| hit.canonical_entity_id.clone())
                    .collect()
            })
            .unwrap_or_default();
        let mut contract_draft = crate::contracts::HypothesisContractDraft {
            user_objective: human_thesis.into(),
            thesis: format!(
                "Test whether {} on {} produces directional continuation over {}.",
                mechanism,
                instruments.join(", "),
                timeframe.label
            ),
            instruments: instruments.clone(),
            mechanism: mechanism.clone(),
            expected_behavior: format!("Directional continuation consistent with {mechanism} over {}.", timeframe.label),
            timeframe: timeframe.clone(),
            expires_at: None,
            supporting_evidence_ids: evidence_ids,
            contradictory_evidence_ids: Vec::new(),
            key_assumptions: vec!["The selected instrument remains tradable and live context stays fresh.".into()],
            alternative_explanation: "Observed movement may be noise or a different market regime.".into(),
            context_requirements: crate::contracts::context_requirements_from_live_spec(&live_context_spec)?,
            live_context_spec,
            invalidation_conditions: vec!["The selected mechanism is contradicted by the resolved live evidence.".into()],
            review_triggers: crate::contracts::ContractReviewTriggers {
                no_trade_decisions: 0,
                consecutive_losses: 0,
                completed_trades: 0,
            },
            stop_limits: crate::contracts::ContractStopLimits::default(),
            jev1_objective: format!(
                "Does current evidence support entering {} for the {} hypothesis over {}?",
                instruments.join(", "),
                mechanism,
                timeframe.label
            ),
            jev2_objective: "Manage the open position using current live context; hold, close, or request separate Jev1 confirmation to add exposure.".into(),
            skill_invocations: skill_invocations.clone(),
        };
        crate::skills::SkillRegistry::discover_builtin()
            .normalize_and_apply_contract_against_objective(&mut contract_draft, human_thesis)?;
        Ok(WorldModelStartupOutput {
            contract: contract_draft,
            retrieval_trace,
            web_research_unavailable: false,
            broker_context_unavailable: None,
        })
    }

    fn review_hypothesis(
        &self,
        package: &WorldModelReviewPackage,
    ) -> Result<HypothesisReviewDecision> {
        let hypothesis = &package.current_hypothesis;
        let lower = package.evidence_query.to_ascii_lowercase();
        let action =
            if lower.contains("stop") || lower.contains("invalidat") || lower.contains("terminate")
            {
                HypothesisAction::Stop
            } else if lower.contains("split")
                || lower.contains("compet")
                || lower.contains("alternative")
            {
                HypothesisAction::Split
            } else if lower.contains("modify")
                || lower.contains("change")
                || lower.contains("different timeframe")
            {
                HypothesisAction::Modify
            } else {
                HypothesisAction::Keep
            };
        let split_requires_escalation = action == HypothesisAction::Split;
        let proposed_timeframe =
            matches!(action, HypothesisAction::Modify | HypothesisAction::Split).then(|| {
                TimeframeDefinition {
                    label: format!(
                        "{} minute",
                        hypothesis.timeframe.horizon_minutes.saturating_mul(2)
                    ),
                    horizon_minutes: hypothesis.timeframe.horizon_minutes.saturating_mul(2),
                    source: "world-model-review".into(),
                    rationale: "Evidence warrants testing a revised or competing horizon.".into(),
                }
            });
        let mut evidence_ids: Vec<String> = package
            .historical_retrieval
            .as_ref()
            .map(|trace| {
                trace
                    .hits
                    .iter()
                    .map(|hit| hit.canonical_entity_id.clone())
                    .collect()
            })
            .unwrap_or_default();
        evidence_ids.sort();
        evidence_ids.dedup();
        let candidate_hypothesis =
            matches!(action, HypothesisAction::Split).then(|| CandidateHypothesis {
                instruments: hypothesis.instruments.clone(),
                strategy_mechanism: format!("competing {}", hypothesis.strategy_mechanism),
                timeframe: proposed_timeframe.clone().unwrap(),
                rationale: "Historical evidence warrants a competing test candidate.".into(),
                competing_with_hypothesis_id: hypothesis.id.clone(),
            });
        let proposed_contract =
            matches!(action, HypothesisAction::Modify | HypothesisAction::Split)
                .then(|| {
                    let mut proposal = hypothesis.contract.as_ref()?.proposal.clone();
                    let mechanism = if action == HypothesisAction::Split {
                        format!("competing {}", hypothesis.strategy_mechanism)
                    } else {
                        hypothesis.strategy_mechanism.clone()
                    };
                    proposal.mechanism = mechanism.clone();
                    if let Some(timeframe) = &proposed_timeframe {
                        proposal.timeframe = timeframe.clone();
                    }
                    proposal.thesis = format!(
                        "Test whether {mechanism} on {} produces directional continuation over {}.",
                        proposal.instruments.join(", "),
                        proposal.timeframe.label
                    );
                    proposal.expected_behavior = format!(
                        "Directional continuation consistent with {mechanism} over {}.",
                        proposal.timeframe.label
                    );
                    proposal.jev1_objective = format!(
                        "Does current evidence support the {mechanism} hypothesis over {}?",
                        proposal.timeframe.label
                    );
                    Some(proposal)
                })
                .flatten();
        Ok(HypothesisReviewDecision {
            id: Uuid::new_v4().to_string(),
            run_id: hypothesis.run_id.clone(),
            hypothesis_id: hypothesis.id.clone(),
            action: action.clone(),
            rationale: format!(
                "World-model {:?} decision after reviewing: {}",
                action, package.evidence_query
            ),
            diagnosis: "Simulated trigger-aware diagnosis".into(),
            problem_severity: "none".into(),
            continuation_rationale:
                "Continue unless the selected lifecycle action changes the loop.".into(),
            decision_confidence: 1.0,
            evidence_canonical_ids: evidence_ids,
            proposed_mechanism: matches!(action, HypothesisAction::Split)
                .then(|| format!("competing {}", hypothesis.strategy_mechanism)),
            proposed_timeframe,
            candidate_hypothesis,
            proposed_contract,
            routing: Some(WorldModelRoutingMetadata {
                provider: "simulated".into(),
                base_model: "simulated".into(),
                selected_model: "simulated".into(),
                escalated: split_requires_escalation,
                escalation_reasons: if split_requires_escalation {
                    vec!["split_requires_escalation".into()]
                } else {
                    Vec::new()
                },
                base_confidence: 1.0,
                request_ids: Vec::new(),
                internet_research_used: false,
            }),
            web_evidence: Vec::new(),
            created_by_event_id: Uuid::new_v4().to_string(),
            created_at: Utc::now(),
        })
    }

    fn critique(
        &self,
        thesis: &ThesisVersion,
        _context: &ContextVersion,
        evidence: &str,
    ) -> Result<String> {
        Ok(format!(
            "Foundation critique retained thesis {} version {} after reviewing {}.",
            thesis.id, thesis.version, evidence
        ))
    }

    fn research(
        &self,
        retriever: &dyn ContextRetriever,
        request: &RetrievalRequest,
    ) -> Result<RetrievalTrace> {
        retriever.retrieve(request)
    }
}

fn infer_instruments(prompt: &str) -> Vec<String> {
    let upper = prompt.to_ascii_uppercase();
    let tokens: Vec<&str> = upper
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty())
        .collect();
    let known = ["BTCUSD", "ETHUSD", "EURUSD", "GBPUSD", "TSLA", "AAPL", "ES"];
    let mut found: Vec<String> = known
        .iter()
        .filter(|symbol| tokens.iter().any(|token| token == *symbol))
        .map(|symbol| (*symbol).to_owned())
        .collect();
    if found.is_empty() && tokens.iter().any(|token| *token == "BTC") {
        found.push("BTCUSD".into());
    }
    if found.is_empty() && tokens.iter().any(|token| *token == "ETH") {
        found.push("ETHUSD".into());
    }
    if found.is_empty() {
        found.push("BTCUSD".into());
    }
    found
}

fn infer_mechanism(prompt: &str) -> String {
    let lower = prompt.to_ascii_lowercase();
    for mechanism in [
        "mean reversion",
        "breakout",
        "momentum",
        "continuation",
        "pairs spread",
    ] {
        if lower.contains(mechanism) {
            return mechanism.to_owned();
        }
    }
    "regime-filtered momentum continuation".into()
}

fn infer_timeframe(prompt: &str) -> TimeframeDefinition {
    let normalized = prompt.to_ascii_lowercase().replace('-', " ");
    let tokens: Vec<&str> = normalized.split_whitespace().collect();
    for (index, token) in tokens.iter().enumerate() {
        let (digits, suffix): (String, String) = token
            .chars()
            .partition(|character| character.is_ascii_digit());
        let adjacent_unit = tokens.get(index + 1).copied().unwrap_or("");
        if let Ok(value) = digits.parse::<u64>() {
            let unit = if suffix.is_empty() {
                adjacent_unit
            } else {
                suffix.as_str()
            };
            let minutes = if unit.starts_with('d') {
                value * 1440
            } else if unit.starts_with('h') {
                value * 60
            } else if unit.starts_with('m') {
                value
            } else {
                continue;
            };
            return TimeframeDefinition {
                label: format!("{value} {unit}"),
                horizon_minutes: minutes,
                source: "user-explicit".into(),
                rationale: "Preserved the user-supplied timeframe as a strong starting constraint."
                    .into(),
            };
        }
    }
    TimeframeDefinition {
        label: "60 minute".into(),
        horizon_minutes: 60,
        source: "world-model-selected".into(),
        rationale: "Selected an intraday horizon because the prompt did not prescribe one.".into(),
    }
}

pub struct SimulatedJev;

impl JevEngine for SimulatedJev {
    fn decide_entry(&self, _state: &ResolvedJevState, _question: &str) -> Result<Jev1Decision> {
        Ok(Jev1Decision {
            action: Jev1Action::Long,
            confidence: 0.82,
            rationale: "Simulated Jev1 entry decision for the connected foundation flow.".into(),
            inference: JevInferenceMetadata {
                provider: "simulated".into(),
                requested_model: "simulated-jev".into(),
                returned_model: "simulated-jev".into(),
                question_id: "entry_action".into(),
                answer_type: "choice".into(),
                probabilities: [("LONG".into(), 0.91), ("SHORT".into(), 0.09)].into(),
                usage: JevUsage::default(),
                request_id: None,
            },
        })
    }

    fn manage_position(&self, _state: &ResolvedJevState, _question: &str) -> Result<Jev2Decision> {
        Ok(Jev2Decision {
            action: Jev2Action::Hold,
            confidence: 0.76,
            rationale: "Simulated Jev2 management decision after position open.".into(),
            inference: JevInferenceMetadata {
                provider: "simulated".into(),
                requested_model: "simulated-jev".into(),
                returned_model: "simulated-jev".into(),
                question_id: "position_action".into(),
                answer_type: "choice".into(),
                probabilities: [
                    ("HOLD".into(), 0.76),
                    ("SELL".into(), 0.14),
                    ("BUY MORE".into(), 0.10),
                ]
                .into(),
                usage: JevUsage::default(),
                request_id: None,
            },
        })
    }
}

pub struct SimulatedBroker;

impl ExecutionBroker for SimulatedBroker {
    fn allows_static_risk_policy(&self) -> bool {
        true
    }

    fn execute(&self, request: &TradeRequest) -> Result<ExecutionReceipt> {
        if request.order.status != "approved"
            || request.order.run_id != request.run_id
            || request.order.loop_id != request.loop_id
            || request.order.decision_id != request.decision_id
            || request.order.quantity <= 0.0
            || request.order.notional <= 0.0
            || matches!(request.action, Jev1Action::NoTrade)
        {
            bail!(
                "execution rejected: request did not carry a matching approved deterministic order"
            );
        }
        let execution_id = Uuid::new_v4().to_string();
        Ok(ExecutionReceipt {
            execution_id: execution_id.clone(),
            run_id: request.run_id.clone(),
            loop_id: request.loop_id.clone(),
            caused_by_decision_id: request.decision_id.clone(),
            action: request.action.clone(),
            execution_kind: "open".into(),
            status: "simulated-filled".into(),
            broker_reference: format!("sim-{execution_id}"),
            broker_position_id: Some(format!("sim-position-{execution_id}")),
            filled_quantity: request.order.quantity,
            average_price: Some(request.order.reference_price),
            rejection_reason: None,
            raw_fix_report: None,
            created_by_event_id: request.execution_event_id.clone(),
            executed_at: Utc::now(),
        })
    }

    fn close(
        &self,
        position: &PositionRecord,
        order: &OrderRecord,
        caused_by_decision_id: &str,
        execution_event_id: &str,
    ) -> Result<ExecutionReceipt> {
        if order.status != "approved"
            || order.order_kind != "market_close" && order.order_kind != "stop_close"
            || order.run_id != position.run_id
            || order.loop_id != position.loop_id
            || order.decision_id != caused_by_decision_id
            || order.quantity <= 0.0
        {
            bail!("close rejected: request did not carry a matching approved deterministic order");
        }
        let execution_id = Uuid::new_v4().to_string();
        Ok(ExecutionReceipt {
            execution_id: execution_id.clone(),
            run_id: position.run_id.clone(),
            loop_id: position.loop_id.clone(),
            caused_by_decision_id: caused_by_decision_id.into(),
            action: if position.direction.to_ascii_lowercase().contains("short") {
                Jev1Action::Short
            } else {
                Jev1Action::Long
            },
            execution_kind: "close".into(),
            status: "simulated-filled".into(),
            broker_reference: format!("sim-close-{execution_id}"),
            broker_position_id: position.broker_position_id.clone(),
            filled_quantity: order.quantity,
            average_price: Some(order.reference_price),
            rejection_reason: None,
            raw_fix_report: None,
            created_by_event_id: execution_event_id.into(),
            executed_at: Utc::now(),
        })
    }

    fn reference_price(&self, _instrument: &str) -> Result<Option<f64>> {
        Ok(Some(100.0))
    }

    fn supports_instrument(&self, _instrument: &str) -> Result<bool> {
        Ok(true)
    }

    fn reconcile(&self) -> Result<BrokerSnapshot> {
        Ok(BrokerSnapshot {
            adapter: "simulated".into(),
            connected: true,
            complete: true,
            positions: Vec::new(),
            observed_at: Utc::now(),
        })
    }
}

pub struct EqualCapitalAllocator;

impl CapitalAllocator for EqualCapitalAllocator {
    fn allocation_for(&self, active_loop_count: usize) -> f64 {
        if active_loop_count == 0 {
            0.0
        } else {
            1.0 / active_loop_count as f64
        }
    }
}
