//! Typed hypothesis contracts and deterministic validation primitives.
//!
//! This module deliberately contains no model calls. Model output is a proposal;
//! the harness validates it and is the only component allowed to activate it.

use crate::domain::{
    ContextDefinition, ContextItem, ContextVersion, HypothesisDefinition, HypothesisReviewRules,
    LiveContextSpec, MarketDataPeriod, ThesisVersion, TimeframeDefinition,
};
use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ContractState {
    Draft,
    Active,
    Rejected,
}

impl Default for ContractState {
    fn default() -> Self {
        Self::Draft
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContextSource {
    CTraderFix,
    TwelveDataRest,
    CanonicalEvidence,
    CTraderMcpReadOnly,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContextValueType {
    Quote,
    Candle,
    Indicator,
    Account,
    Position,
    Evidence,
}

/// A machine-readable context dependency. Formula-specific requirements live in
/// `live_context_spec`; this declaration records the contract's source, type,
/// lookback, freshness and required/optional intent for deterministic checks.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextRequirement {
    pub id: String,
    pub source: ContextSource,
    pub value_type: ContextValueType,
    pub period: Option<MarketDataPeriod>,
    pub lookback: Option<u32>,
    pub maximum_age_seconds: u64,
    pub required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ContractReviewTriggers {
    pub no_trade_decisions: u32,
    pub consecutive_losses: u32,
    pub completed_trades: u32,
}

/// Hard loop limits selected by the world model within user-specified maxima,
/// then enforced by the deterministic harness. `None` means no cap was selected.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ContractStopLimits {
    pub maximum_elapsed_seconds: Option<u64>,
    pub maximum_completed_trades: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillInvocation {
    pub skill_id: String,
    #[serde(default)]
    pub arguments: Value,
}

/// The sole typed proposal shape used for initial formulation and material
/// revisions. The surrounding hypothesis/thesis/context records are projections
/// compiled from this contract only after it passes validation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HypothesisContractDraft {
    pub user_objective: String,
    pub thesis: String,
    pub instruments: Vec<String>,
    pub mechanism: String,
    pub expected_behavior: String,
    pub timeframe: TimeframeDefinition,
    pub expires_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub supporting_evidence_ids: Vec<String>,
    #[serde(default)]
    pub contradictory_evidence_ids: Vec<String>,
    pub key_assumptions: Vec<String>,
    pub alternative_explanation: String,
    pub context_requirements: Vec<ContextRequirement>,
    pub live_context_spec: LiveContextSpec,
    pub invalidation_conditions: Vec<String>,
    pub review_triggers: ContractReviewTriggers,
    pub stop_limits: ContractStopLimits,
    pub jev1_objective: String,
    pub jev2_objective: String,
    #[serde(default)]
    pub skill_invocations: Vec<SkillInvocation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HypothesisContract {
    pub id: String,
    pub run_id: String,
    pub version: u32,
    pub parent_contract_id: Option<String>,
    #[serde(default)]
    pub state: ContractState,
    pub proposal: HypothesisContractDraft,
    pub created_by_event_id: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct CompiledContract {
    pub contract: HypothesisContract,
    pub thesis: ThesisVersion,
    pub context_definition: ContextDefinition,
    pub context: ContextVersion,
    pub hypothesis: HypothesisDefinition,
    pub activated_by_event_id: String,
}

/// Deterministically projects one contract draft into the existing Jev and
/// persistence records. This function makes no model calls and is shared by
/// initial activation and material review revisions.
pub fn compile_contract(
    draft: HypothesisContractDraft,
    run_id: &str,
    version: u32,
    parent_contract_id: Option<String>,
    parent_hypothesis_id: Option<String>,
    root_hypothesis_id: Option<String>,
    thesis_version: i64,
    context_version: i64,
    provenance: &str,
    now: DateTime<Utc>,
) -> CompiledContract {
    let contract_id = Uuid::new_v4().to_string();
    let event_id = Uuid::new_v4().to_string();
    let thesis_id = Uuid::new_v4().to_string();
    let context_definition_id = Uuid::new_v4().to_string();
    let context_id = Uuid::new_v4().to_string();
    let thesis = ThesisVersion {
        id: thesis_id.clone(),
        run_id: run_id.into(),
        version: thesis_version,
        thesis: draft.thesis.clone(),
        provenance: provenance.into(),
        created_by_event_id: Uuid::new_v4().to_string(),
        created_at: now,
    };
    let context_definition = ContextDefinition {
        id: context_definition_id.clone(),
        run_id: run_id.into(),
        name: format!("contract-context-v{version}"),
        description: draft
            .context_requirements
            .iter()
            .map(|item| format!("{} ({:?}, {:?})", item.id, item.source, item.value_type))
            .collect::<Vec<_>>()
            .join("; "),
        created_by_event_id: Uuid::new_v4().to_string(),
        created_at: now,
    };
    let context = ContextVersion {
        id: context_id,
        definition_id: context_definition_id,
        run_id: run_id.into(),
        version: context_version,
        items: draft
            .context_requirements
            .iter()
            .map(|item| ContextItem {
                source: format!("contract-context://{:?}", item.source),
                source_id: item.id.clone(),
                observed_at: now,
                content: format!(
                    "{:?} context from {:?}; period={:?}; lookback={:?}; max age={}s; required={}",
                    item.value_type,
                    item.source,
                    item.period,
                    item.lookback,
                    item.maximum_age_seconds,
                    item.required
                ),
            })
            .collect(),
        created_by_event_id: Uuid::new_v4().to_string(),
        created_at: now,
    };
    let hypothesis_id = contract_id.clone();
    let hypothesis_event_id = event_id.clone();
    let hypothesis = HypothesisDefinition {
        id: hypothesis_id.clone(),
        root_hypothesis_id: root_hypothesis_id.unwrap_or_else(|| hypothesis_id.clone()),
        run_id: run_id.into(),
        version: version as i64,
        parent_hypothesis_id,
        original_prompt: draft.user_objective.clone(),
        instruments: draft.instruments.clone(),
        strategy_mechanism: draft.mechanism.clone(),
        timeframe: draft.timeframe.clone(),
        deterministic_context: draft
            .context_requirements
            .iter()
            .filter(|item| item.required)
            .map(|item| item.id.clone())
            .collect(),
        live_context_spec: Some(draft.live_context_spec.clone()),
        jev_question: draft.jev1_objective.clone(),
        review_rules: HypothesisReviewRules {
            support_evidence: draft.supporting_evidence_ids.clone(),
            weaken_evidence: draft.contradictory_evidence_ids.clone(),
            invalidate_evidence: draft.invalidation_conditions.clone(),
            modify_when: vec!["The validated contract's key assumptions change.".into()],
            split_when: vec![draft.alternative_explanation.clone()],
            stop_when: draft.invalidation_conditions.clone(),
        },
        thesis_version_id: thesis.id.clone(),
        context_version_id: context.id.clone(),
        status: "DRAFT".into(),
        contract: Some(HypothesisContract {
            id: contract_id,
            run_id: run_id.into(),
            version,
            parent_contract_id,
            state: ContractState::Draft,
            proposal: draft,
            created_by_event_id: event_id,
            created_at: now,
        }),
        created_by_event_id: hypothesis_event_id,
        created_at: now,
    };
    CompiledContract {
        contract: hypothesis.contract.clone().expect("compiler sets contract"),
        thesis,
        context_definition,
        context,
        hypothesis,
        activated_by_event_id: String::new(),
    }
}

#[derive(Debug, Clone, Default)]
pub struct ContractValidationContext {
    pub tradable_instruments: HashSet<String>,
    pub known_evidence_ids: HashSet<String>,
}

/// Reject stale cited records when the user's objective is explicitly about
/// current or recent events. External web IDs may be included only after the
/// web-evidence validator has established that they are date-eligible.
pub fn validate_evidence_freshness(
    proposal: &HypothesisContractDraft,
    retrieval_trace: Option<&crate::domain::RetrievalTrace>,
    independently_fresh_ids: &[String],
    now: DateTime<Utc>,
) -> Result<()> {
    let objective = proposal.user_objective.to_ascii_lowercase();
    let recency_required = [
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
    if !recency_required {
        return Ok(());
    }
    let claims = [
        proposal.thesis.as_str(),
        proposal.mechanism.as_str(),
        proposal.expected_behavior.as_str(),
        proposal.alternative_explanation.as_str(),
    ]
    .into_iter()
    .chain(proposal.key_assumptions.iter().map(String::as_str))
    .chain(proposal.invalidation_conditions.iter().map(String::as_str))
    .collect::<Vec<_>>()
    .join(" ")
    .to_ascii_lowercase();
    let contract_asserts_time_sensitive_facts = [
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
        "breaking",
        "recently",
    ]
    .iter()
    .any(|term| claims.contains(term));
    if !contract_asserts_time_sensitive_facts
        && proposal.supporting_evidence_ids.is_empty()
        && proposal.contradictory_evidence_ids.is_empty()
    {
        return Ok(());
    }
    if proposal.supporting_evidence_ids.is_empty() {
        bail!("time-sensitive contract needs at least one fresh supporting evidence reference");
    }
    let maximum_age = if proposal.timeframe.horizon_minutes <= 1_440 {
        chrono::Duration::hours(72)
    } else if proposal.timeframe.horizon_minutes <= 10_080 {
        chrono::Duration::days(14)
    } else {
        chrono::Duration::days(30)
    };
    let fresh_external: HashSet<&str> =
        independently_fresh_ids.iter().map(String::as_str).collect();
    let hits = retrieval_trace
        .map(|trace| trace.hits.as_slice())
        .unwrap_or_default();
    for id in proposal
        .supporting_evidence_ids
        .iter()
        .chain(proposal.contradictory_evidence_ids.iter())
    {
        if fresh_external.contains(id.as_str()) {
            continue;
        }
        let hit = hits
            .iter()
            .find(|hit| {
                hit.id == *id || hit.canonical_entity_id == *id || hit.canonical_event_id == *id
            })
            .with_context(|| {
                format!("time-sensitive evidence {id} has no dated canonical provenance")
            })?;
        let observed = DateTime::parse_from_rfc3339(&hit.observed_at)
            .map(|date| date.with_timezone(&Utc))
            .with_context(|| {
                format!("time-sensitive evidence {id} has an invalid observation date")
            })?;
        if observed > now + chrono::Duration::hours(24)
            || now.signed_duration_since(observed) > maximum_age
        {
            bail!(
                "time-sensitive evidence {id} is stale or future-dated for this contract timeframe"
            );
        }
    }
    Ok(())
}

pub fn context_requirements_from_live_spec(
    spec: &LiveContextSpec,
) -> Result<Vec<ContextRequirement>> {
    let series = crate::market_data::requirements(spec)?;
    let mut requirements = vec![ContextRequirement {
        id: "current_quote".into(),
        source: ContextSource::CTraderFix,
        value_type: ContextValueType::Quote,
        period: None,
        lookback: None,
        maximum_age_seconds: 15,
        required: true,
    }];
    for requirement in series {
        let source = requirement
            .source
            .as_deref()
            .or_else(|| {
                spec.series_sources
                    .get(&format!("{:?}", requirement.period))
                    .map(String::as_str)
            })
            .unwrap_or("twelve-data-rest");
        let source = match source {
            "twelve-data-rest" => ContextSource::TwelveDataRest,
            "ctrader-fix-price-only" => ContextSource::CTraderFix,
            _ => bail!("unsupported live-series source {source}"),
        };
        requirements.push(ContextRequirement {
            id: format!("candles:{:?}", requirement.period),
            source,
            value_type: ContextValueType::Candle,
            period: Some(requirement.period.clone()),
            lookback: Some(requirement.bars as u32),
            maximum_age_seconds: (requirement.period.seconds().max(1) as u64).saturating_mul(2),
            required: true,
        });
    }
    Ok(requirements)
}

/// Ensure the machine-readable context dependencies include every executable
/// formula input. These are derived from the AST/source map and cannot be
/// omitted by a world-model proposal.
pub fn normalize_derived_context_requirements(
    spec: &LiveContextSpec,
    requirements: &mut Vec<ContextRequirement>,
) -> Result<()> {
    for expected in context_requirements_from_live_spec(spec)? {
        if let Some(existing) = requirements.iter().find(|item| item.id == expected.id) {
            if existing.source != expected.source
                || existing.value_type != expected.value_type
                || existing.period != expected.period
                || existing.lookback != expected.lookback
                || existing.maximum_age_seconds != expected.maximum_age_seconds
                || existing.required != expected.required
            {
                bail!(
                    "derived context requirement {} conflicts with its live formula/source definition",
                    expected.id
                );
            }
        } else {
            requirements.push(expected);
        }
    }
    Ok(())
}

/// Validates objective structural and provenance requirements. It does not
/// score a thesis's quality or predict whether it will be profitable.
pub fn validate_contract(
    contract: &HypothesisContractDraft,
    context: &ContractValidationContext,
    now: DateTime<Utc>,
) -> Result<()> {
    let non_empty = [
        ("userObjective", contract.user_objective.as_str()),
        ("thesis", contract.thesis.as_str()),
        ("mechanism", contract.mechanism.as_str()),
        ("expectedBehavior", contract.expected_behavior.as_str()),
        (
            "alternativeExplanation",
            contract.alternative_explanation.as_str(),
        ),
        ("jev1Objective", contract.jev1_objective.as_str()),
        ("jev2Objective", contract.jev2_objective.as_str()),
    ];
    for (field, value) in non_empty {
        if value.trim().is_empty() {
            bail!("contract field {field} must not be empty");
        }
    }
    if contract.timeframe.horizon_minutes == 0 || contract.timeframe.horizon_minutes > 365 * 24 * 60
    {
        bail!("contract timeframe must be between one minute and one year");
    }
    if contract.expires_at.is_some_and(|expiry| expiry <= now) {
        bail!("contract expiry must be in the future");
    }
    validate_single_instrument_binding(contract)?;
    for instrument in &contract.instruments {
        if instrument.trim().is_empty() {
            bail!("contract contains an empty instrument");
        }
        if !context
            .tradable_instruments
            .contains(&instrument.trim().to_ascii_uppercase())
        {
            bail!("instrument {instrument} is not present in the validated tradable-symbol set");
        }
    }
    for (label, ids) in [
        ("supporting", &contract.supporting_evidence_ids),
        ("contradictory", &contract.contradictory_evidence_ids),
    ] {
        let mut unique = HashSet::new();
        for id in ids {
            if id.trim().is_empty() || !unique.insert(id) {
                bail!("{label} evidence IDs must be non-empty and unique");
            }
            if !context.known_evidence_ids.contains(id) {
                bail!("{label} evidence ID {id} does not reference a known canonical record");
            }
        }
    }
    if contract.key_assumptions.is_empty()
        || contract
            .key_assumptions
            .iter()
            .any(|value| value.trim().is_empty())
        || contract.invalidation_conditions.is_empty()
        || contract
            .invalidation_conditions
            .iter()
            .any(|value| value.trim().is_empty())
    {
        bail!("contract assumptions and invalidation conditions must be explicit");
    }
    if contract.context_requirements.is_empty() {
        bail!("contract must declare machine-readable context requirements");
    }
    let mut requirement_ids = HashSet::new();
    for requirement in &contract.context_requirements {
        if requirement.id.trim().is_empty() || !requirement_ids.insert(&requirement.id) {
            bail!("context requirement IDs must be non-empty and unique");
        }
        if requirement.maximum_age_seconds == 0 {
            bail!(
                "context requirement {} has no freshness limit",
                requirement.id
            );
        }
        if requirement.value_type == ContextValueType::Candle
            && (requirement.period.is_none() || requirement.lookback.is_none_or(|n| n == 0))
        {
            bail!(
                "candle requirement {} needs a period and positive lookback",
                requirement.id
            );
        }
        if let (Some(period), Some(lookback)) = (&requirement.period, requirement.lookback) {
            if lookback > 1_000
                || period.seconds() <= 0
                || requirement.maximum_age_seconds > (period.seconds() as u64).saturating_mul(2)
            {
                bail!(
                    "context requirement {} exceeds supported series bounds",
                    requirement.id
                );
            }
        }
        let source_matches_type = matches!(
            (&requirement.source, &requirement.value_type),
            (ContextSource::CTraderFix, ContextValueType::Quote)
                | (ContextSource::CTraderFix, ContextValueType::Candle)
                | (ContextSource::TwelveDataRest, ContextValueType::Candle)
                | (ContextSource::CTraderFix, ContextValueType::Indicator)
                | (ContextSource::TwelveDataRest, ContextValueType::Indicator)
                | (ContextSource::CTraderMcpReadOnly, ContextValueType::Account)
                | (
                    ContextSource::CTraderMcpReadOnly,
                    ContextValueType::Position
                )
                | (ContextSource::CanonicalEvidence, ContextValueType::Evidence)
        );
        if !source_matches_type {
            bail!(
                "context requirement {} pairs an unsupported source and value type",
                requirement.id
            );
        }
        if requirement.required
            && matches!(
                requirement.value_type,
                ContextValueType::Account | ContextValueType::Position,
            )
        {
            bail!(
                "required {} context {} has no executable Jev context resolver",
                format!("{:?}", requirement.value_type).to_ascii_lowercase(),
                requirement.id
            );
        }
        if requirement.required
            && requirement.value_type == ContextValueType::Indicator
            && !contract
                .live_context_spec
                .fields
                .iter()
                .any(|field| field.field_id == requirement.id)
        {
            bail!(
                "required indicator context {} is not produced by a live-context formula",
                requirement.id
            );
        }
        if requirement.required
            && requirement.value_type == ContextValueType::Indicator
            && requirement.period.is_none()
        {
            bail!(
                "required indicator context {} must declare its source period",
                requirement.id
            );
        }
        if requirement.required && requirement.value_type == ContextValueType::Evidence {
            let is_cited = contract
                .supporting_evidence_ids
                .iter()
                .chain(&contract.contradictory_evidence_ids)
                .any(|id| id == &requirement.id);
            if !is_cited || !context.known_evidence_ids.contains(&requirement.id) {
                bail!(
                    "required evidence context {} is not a known cited canonical record",
                    requirement.id
                );
            }
        }
    }
    let series = crate::market_data::requirements(&contract.live_context_spec)
        .context("contract live-context requirements are not executable")?;
    for series in &series {
        let matched = contract.context_requirements.iter().any(|item| {
            item.required
                && item.value_type == ContextValueType::Candle
                && item.period.as_ref() == Some(&series.period)
                && item
                    .lookback
                    .is_some_and(|count| count as usize >= series.bars)
                && matches!(
                    (&item.source, series.source.as_deref()),
                    (ContextSource::TwelveDataRest, Some("twelve-data-rest"))
                        | (ContextSource::CTraderFix, Some("ctrader-fix-price-only"))
                        | (ContextSource::TwelveDataRest, None)
                )
        });
        if !matched {
            bail!("required candle context {:?} is missing or its source/lookback differs from the executable formulas", series.period);
        }
    }
    for requirement in contract
        .context_requirements
        .iter()
        .filter(|item| item.required && item.value_type == ContextValueType::Candle)
    {
        let period = requirement
            .period
            .as_ref()
            .expect("candle period checked above");
        let formula_requirement = series
            .iter()
            .find(|series| &series.period == period)
            .context(format!(
            "required candle context {} is not requested by any executable live-context formula",
            requirement.id
        ))?;
        let requested_source = match requirement.source {
            ContextSource::CTraderFix => Some("ctrader-fix-price-only"),
            ContextSource::TwelveDataRest => Some("twelve-data-rest"),
            _ => None,
        };
        let formula_source = formula_requirement.source.as_deref().or_else(|| {
            contract
                .live_context_spec
                .series_sources
                .get(&format!("{:?}", period))
                .map(String::as_str)
        });
        let source_matches = formula_source == requested_source
            || (formula_source.is_none() && requirement.source == ContextSource::TwelveDataRest);
        // A declared lookback may be larger than the formula's minimum input
        // window. The resolver can provide the requested history while the
        // executable formula consumes only the bars it declares. Rejecting a
        // larger window here contradicted the check above, which correctly
        // requires at least the formula's bar count, and caused otherwise
        // executable contracts (for example 30 declared bars for a 13-bar
        // formula) to fail validation. Keep the source match strict: extra
        // history is acceptable, data from the wrong provider is not.
        if !source_matches {
            bail!(
                "required candle context {} uses a different source than the executable formula",
                requirement.id
            );
        }
    }
    if !contract.context_requirements.iter().any(|item| {
        item.id == "current_quote"
            && item.source == ContextSource::CTraderFix
            && item.value_type == ContextValueType::Quote
            && item.required
            && item.maximum_age_seconds <= 15
    }) {
        bail!("contract requires a fresh FIX quote context requirement");
    }
    let triggers = &contract.review_triggers;
    if !(1..=12).contains(&triggers.no_trade_decisions)
        || !(1..=10).contains(&triggers.consecutive_losses)
        || !(1..=100).contains(&triggers.completed_trades)
    {
        bail!("review trigger thresholds are outside supported deterministic bounds");
    }
    if contract
        .stop_limits
        .maximum_elapsed_seconds
        .is_some_and(|value| value == 0 || value > 365 * 24 * 60 * 60)
        || contract
            .stop_limits
            .maximum_completed_trades
            .is_some_and(|value| value == 0 || value > 10_000)
    {
        bail!("contract stop limits are outside supported bounds");
    }
    validate_contract_lifecycle(contract)?;
    Ok(())
}

/// Enforce the current one-snapshot, one-executable-symbol contract boundary.
/// This is also called for persisted ACTIVE contracts during run recovery.
pub fn validate_single_instrument_binding(contract: &HypothesisContractDraft) -> Result<()> {
    if contract.instruments.len() != 1 {
        bail!(
            "contract must name exactly one instrument because live Jev context and execution are bound to one symbol"
        );
    }
    let instrument = contract.instruments[0].trim();
    if instrument.is_empty() {
        bail!("contract contains an empty instrument");
    }
    if contract
        .live_context_spec
        .instrument
        .trim()
        .to_ascii_uppercase()
        != instrument.to_ascii_uppercase()
    {
        bail!("live context instrument must match the contract's instrument");
    }
    Ok(())
}

/// Validate persisted lifecycle limits. Older contracts may not contain the
/// optional skill invocation record; their persisted thresholds remain usable
/// when in range and their hard stops do not exceed canonical prompt limits.
pub fn validate_contract_lifecycle(contract: &HypothesisContractDraft) -> Result<()> {
    validate_contract_lifecycle_against_objective(contract, &contract.user_objective)
}

/// Validate lifecycle caps against the authoritative objective persisted for
/// the run. Recovery uses this form because a proposal's own objective is not
/// an authority for weakening a stop cap requested in the original prompt.
pub fn validate_contract_lifecycle_against_objective(
    contract: &HypothesisContractDraft,
    authoritative_objective: &str,
) -> Result<()> {
    for skill in &contract.skill_invocations {
        if skill.skill_id.trim().is_empty() || !skill.arguments.is_object() {
            bail!("skill invocations need a registered ID and an object argument payload");
        }
    }
    let mut compiled = contract.clone();
    crate::skills::SkillRegistry::discover_builtin()
        .apply_invocations(&contract.skill_invocations, &mut compiled)
        .context("contract requested a skill that is unavailable or has invalid arguments")?;
    if compiled.stop_limits.maximum_elapsed_seconds != contract.stop_limits.maximum_elapsed_seconds
        || compiled.stop_limits.maximum_completed_trades
            != contract.stop_limits.maximum_completed_trades
        || compiled.review_triggers != contract.review_triggers
    {
        bail!("contract lifecycle values differ from the registered skill output");
    }
    let triggers = &contract.review_triggers;
    if !(1..=12).contains(&triggers.no_trade_decisions)
        || !(1..=10).contains(&triggers.consecutive_losses)
        || !(1..=100).contains(&triggers.completed_trades)
        || contract
            .stop_limits
            .maximum_elapsed_seconds
            .is_some_and(|value| value == 0 || value > 365 * 24 * 60 * 60)
        || contract
            .stop_limits
            .maximum_completed_trades
            .is_some_and(|value| value == 0 || value > 10_000)
    {
        bail!("persisted lifecycle limits are outside supported bounds");
    }
    let explicit_limits = explicit_user_stop_limits(authoritative_objective)?;
    crate::skills::validate_proposed_stop_limits(
        &explicit_limits,
        contract.stop_limits.maximum_elapsed_seconds,
        contract.stop_limits.maximum_completed_trades,
    )?;
    Ok(())
}

#[cfg(test)]
mod lifecycle_model_cap_tests {
    use super::*;

    fn draft() -> HypothesisContractDraft {
        HypothesisContractDraft {
            user_objective: "Trade BTCUSD aggressively".into(),
            thesis: "test".into(),
            instruments: vec!["BTCUSD".into()],
            mechanism: "test".into(),
            expected_behavior: "test".into(),
            timeframe: crate::domain::TimeframeDefinition {
                label: "1 hour".into(),
                horizon_minutes: 60,
                source: "world-model-selected".into(),
                rationale: "test".into(),
            },
            expires_at: None,
            supporting_evidence_ids: vec![],
            contradictory_evidence_ids: vec![],
            key_assumptions: vec![],
            alternative_explanation: "test".into(),
            context_requirements: vec![],
            live_context_spec: crate::market_data::default_live_context_spec("BTCUSD"),
            invalidation_conditions: vec!["test".into()],
            review_triggers: ContractReviewTriggers {
                no_trade_decisions: 6,
                consecutive_losses: 2,
                completed_trades: 8,
            },
            stop_limits: ContractStopLimits::default(),
            jev1_objective: "test".into(),
            jev2_objective: "test".into(),
            skill_invocations: vec![],
        }
    }

    #[test]
    fn persisted_lifecycle_accepts_model_selected_caps_without_user_caps() {
        let registry = crate::skills::SkillRegistry::discover_builtin();
        let mut contract = draft();
        contract.skill_invocations = vec![SkillInvocation {
            skill_id: "loop.lifecycle_limits".into(),
            arguments: serde_json::json!({
                "maximumElapsedSeconds":1800,
                "maximumCompletedTrades":6,
                "noTradeDecisions":6,
                "consecutiveLosses":2,
                "completedTrades":8
            }),
        }];
        registry
            .normalize_and_apply_contract(&mut contract)
            .unwrap();
        validate_contract_lifecycle_against_objective(&contract, "Trade BTCUSD aggressively")
            .unwrap();
    }
}

/// Find an exact trading horizon only when the objective states one directly.
/// Candle periods, stop limits, upper bounds, and vague/compound durations are
/// intentionally not treated as a model-selected strategy horizon.
pub fn explicit_user_timeframe_minutes(objective: &str) -> Result<Option<u64>> {
    let lowercase = objective.to_ascii_lowercase();
    let characters = lowercase.chars().collect::<Vec<_>>();
    let mut normalized = String::with_capacity(lowercase.len());
    for (index, character) in characters.iter().copied().enumerate() {
        let previous_non_whitespace = characters[..index]
            .iter()
            .rev()
            .find(|candidate| !candidate.is_whitespace());
        let next_non_whitespace = characters[index.saturating_add(1)..]
            .iter()
            .find(|candidate| !candidate.is_whitespace());
        let adjacent_digits = previous_non_whitespace.is_some_and(|value| value.is_ascii_digit())
            && next_non_whitespace.is_some_and(|value| value.is_ascii_digit());
        if character == '.' && adjacent_digits {
            normalized.push_str(" decimalmarker ");
        } else if matches!(character, '-' | '–' | '—' | '−') && adjacent_digits {
            normalized.push_str(" rangemarker ");
        } else if matches!(character, '<' | '>' | '≤' | '≥' | '+') {
            normalized.push_str(" boundmarker ");
        } else if character == '~' {
            normalized.push_str(" approxmarker ");
        } else if character.is_ascii_alphanumeric() {
            normalized.push(character);
        } else {
            normalized.push(' ');
        }
    }
    let tokens = normalized
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let word_number = |token: &str| -> Option<u64> {
        token.parse::<u64>().ok().or_else(|| match token {
            "one" => Some(1),
            "two" => Some(2),
            "three" => Some(3),
            "four" => Some(4),
            "five" => Some(5),
            "six" => Some(6),
            "seven" => Some(7),
            "eight" => Some(8),
            "nine" => Some(9),
            "ten" => Some(10),
            _ => None,
        })
    };
    let unit_minutes = |unit: &str| -> Option<u64> {
        match unit {
            "m" | "min" | "mins" | "minute" | "minutes" => Some(1),
            "h" | "hr" | "hrs" | "hour" | "hours" => Some(60),
            "d" | "day" | "days" => Some(60 * 24),
            "w" | "week" | "weeks" => Some(60 * 24 * 7),
            "y" | "yr" | "yrs" | "year" | "years" => Some(60 * 24 * 365),
            _ => None,
        }
    };
    let candle_cues = [
        "candle", "candles", "bar", "bars", "chart", "data", "lookback", "interval", "window",
    ];
    let mut candidates = std::collections::HashSet::new();

    for unit_index in 0..tokens.len() {
        let unit = &tokens[unit_index];
        if is_lifecycle_duration(&tokens, unit_index) {
            continue;
        }
        if unit.ends_with("ish") && unit.chars().any(|character| character.is_ascii_digit()) {
            bail!("explicit strategy timeframe is approximate; one exact horizon is required");
        }
        let Some(multiplier) = unit_minutes(unit) else {
            // Also recognize compact forms such as 15m, 1h, and 2d.
            if let Some(minutes) = parse_compact_timeframe(unit) {
                let nearby_start = unit_index.saturating_sub(5);
                let nearby_end = (unit_index + 8).min(tokens.len());
                let nearby = &tokens[nearby_start..nearby_end];
                if has_timeframe_range_cue(nearby)
                    || has_ambiguous_timeframe_quantity(&tokens, unit_index)
                    || nearby.iter().any(|token| {
                        matches!(token.as_str(), "ish" | "thereabouts") || token.ends_with("ish")
                    })
                {
                    bail!("explicit strategy timeframe is bounded or approximate; one exact horizon is required");
                }
                if exact_horizon_cue(&tokens, unit_index, unit_index + 1, &candle_cues) {
                    if minutes == 0 || minutes > 365 * 24 * 60 {
                        bail!(
                            "explicit strategy timeframe must be between one minute and one year"
                        );
                    }
                    candidates.insert(minutes);
                }
                continue;
            }
            let (digits, suffix): (String, String) = unit
                .chars()
                .partition(|character| character.is_ascii_digit());
            if digits.is_empty() {
                continue;
            }
            let Some(multiplier) = unit_minutes(&suffix) else {
                continue;
            };
            let nearby_start = unit_index.saturating_sub(4);
            let nearby_end = (unit_index + 3).min(tokens.len());
            let nearby = &tokens[nearby_start..nearby_end];
            let describes_candles = nearby
                .iter()
                .any(|token| candle_cues.contains(&token.as_str()));
            if nearby.iter().any(|token| {
                matches!(token.as_str(), "ish" | "thereabouts") || token.ends_with("ish")
            }) {
                bail!("explicit strategy timeframe is approximate; one exact horizon is required");
            }
            if has_timeframe_range_cue(nearby)
                || (!describes_candles
                    && unit_index > 0
                    && matches!(
                        tokens[unit_index - 1].as_str(),
                        "decimalmarker" | "rangemarker" | "to" | "or"
                    ))
            {
                bail!("explicit strategy timeframe is a range, alternative, or unsupported decimal; one exact horizon is required");
            }
            if !exact_horizon_cue(&tokens, unit_index, unit_index + 1, &candle_cues) {
                continue;
            }
            let amount = digits
                .parse::<u64>()
                .context("explicit strategy timeframe number is outside the supported range")?;
            let minutes = amount
                .checked_mul(multiplier)
                .filter(|minutes| *minutes > 0 && *minutes <= 365 * 24 * 60)
                .context("explicit strategy timeframe must be between one minute and one year")?;
            candidates.insert(minutes);
            continue;
        };

        let unit_nearby_start = unit_index.saturating_sub(5);
        let unit_nearby_end = (unit_index + 3).min(tokens.len());
        let unit_nearby = &tokens[unit_nearby_start..unit_nearby_end];
        if unit_nearby.iter().any(|token| {
            matches!(
                token.as_str(),
                "couple"
                    | "few"
                    | "several"
                    | "many"
                    | "half"
                    | "ish"
                    | "some"
                    | "multiple"
                    | "handful"
                    | "various"
            ) || token.ends_with("ish")
        }) {
            bail!("explicit strategy timeframe is imprecise; one exact horizon is required");
        }

        let compound_unit =
            unit_index > 0 && matches!(tokens[unit_index - 1].as_str(), "trading" | "business");
        if compound_unit && matches!(unit.as_str(), "day" | "days" | "week" | "weeks") {
            let quantity_index = unit_index.saturating_sub(2);
            let has_explicit_quantity = word_number(&tokens[quantity_index]).is_some()
                || matches!(tokens[quantity_index].as_str(), "next" | "a" | "an")
                || (quantity_index >= 2
                    && tokens[quantity_index - 1] == "decimalmarker"
                    && word_number(&tokens[quantity_index - 2]).is_some());
            if has_explicit_quantity {
                bail!("trading-day and business-day horizons need an explicit market-hours convention");
            }
        }
        let amount_slot = unit_index.saturating_sub(usize::from(compound_unit) + 1);
        let amount = if unit_index > 0
            && unit_index >= usize::from(compound_unit) + 1
            && amount_slot > 0
            && tokens[amount_slot - 1] == "decimalmarker"
            && amount_slot >= 2
        {
            let whole = word_number(&tokens[amount_slot - 2]);
            let fractional = tokens[amount_slot].parse::<u64>().ok();
            whole
                .zip(fractional)
                .map(|(whole, fraction)| {
                    let digits = tokens[amount_slot].len();
                    let scale = 10_u64.checked_pow(digits as u32).unwrap_or(u64::MAX);
                    let numerator = whole
                        .checked_mul(scale)
                        .and_then(|value| value.checked_add(fraction))
                        .unwrap_or(u64::MAX);
                    (numerator, scale, amount_slot - 2)
                })
                .map(|(numerator, scale, start)| ((numerator, scale), start))
        } else if unit_index > usize::from(compound_unit) {
            let index = amount_slot;
            word_number(&tokens[index]).map(|amount| ((amount, 1), index))
        } else {
            None
        }
        .or_else(|| {
            tokens[..unit_index]
                .iter()
                .enumerate()
                .rposition(|(index, token)| {
                    matches!(token.as_str(), "next" | "a" | "an")
                        && unit_index.saturating_sub(index) <= 2
                        && tokens[index + 1..unit_index]
                            .iter()
                            .all(|token| matches!(token.as_str(), "full" | "calendar"))
                })
                .map(|index| ((1, 1), index))
        });
        let Some(((numerator, denominator), amount_index)) = amount else {
            continue;
        };
        if unit_index.saturating_sub(amount_index) > 3 {
            continue;
        }
        let nearby_start = amount_index.saturating_sub(4);
        let nearby_end = (unit_index + 3).min(tokens.len());
        let nearby = &tokens[nearby_start..nearby_end];
        if has_ambiguous_timeframe_quantity(&tokens, amount_index)
            || has_timeframe_range_cue(nearby)
        {
            bail!("explicit strategy timeframe is a range or alternative; one exact horizon is required");
        }
        if !exact_horizon_cue(&tokens, amount_index, unit_index + 1, &candle_cues) {
            continue;
        }
        let scaled_minutes = numerator as f64 * multiplier as f64 / denominator as f64;
        if !scaled_minutes.is_finite() || scaled_minutes.fract().abs() > 1e-9 {
            bail!("explicit strategy timeframe cannot be represented as whole minutes");
        }
        let minutes = scaled_minutes as u64;
        if minutes == 0 || minutes > 365 * 24 * 60 {
            bail!("explicit strategy timeframe must be between one minute and one year");
        }
        candidates.insert(minutes);
    }

    if candidates.len() > 1 {
        bail!("objective contains conflicting explicit strategy timeframes");
    }
    Ok(candidates.into_iter().next())
}

fn has_ambiguous_timeframe_quantity(tokens: &[String], amount_index: usize) -> bool {
    if amount_index == 0 {
        return false;
    }
    matches!(
        tokens[amount_index - 1].as_str(),
        "rangemarker" | "to" | "or"
    ) || (amount_index >= 2
        && matches!(tokens[amount_index - 1].as_str(), "to" | "or")
        && (tokens[amount_index - 2].parse::<u64>().is_ok()
            || matches!(
                tokens[amount_index - 2].as_str(),
                "one" | "two" | "three" | "four" | "five"
            )))
        || (amount_index >= 2
            && tokens[amount_index - 1] == "decimalmarker"
            && word_number_token(&tokens[amount_index - 2]))
}

fn word_number_token(token: &str) -> bool {
    token.parse::<u64>().is_ok()
        || matches!(
            token,
            "one" | "two" | "three" | "four" | "five" | "six" | "seven" | "eight" | "nine" | "ten"
        )
}

fn parse_compact_timeframe(token: &str) -> Option<u64> {
    let bytes = token.as_bytes();
    let mut cursor = 0;
    let mut total = 0u64;
    let mut components = 0usize;
    while cursor < bytes.len() {
        let number_start = cursor;
        while cursor < bytes.len() && bytes[cursor].is_ascii_digit() {
            cursor += 1;
        }
        if number_start == cursor {
            return None;
        }
        let amount = token[number_start..cursor].parse::<u64>().ok()?;
        let unit_start = cursor;
        while cursor < bytes.len() && bytes[cursor].is_ascii_alphabetic() {
            cursor += 1;
        }
        if unit_start == cursor {
            return None;
        }
        let multiplier = match &token[unit_start..cursor] {
            "m" | "min" => 1,
            "h" | "hr" => 60,
            "d" => 60 * 24,
            "w" => 60 * 24 * 7,
            "y" => 60 * 24 * 365,
            _ => return None,
        };
        total = total.checked_add(amount.checked_mul(multiplier)?)?;
        components += 1;
    }
    (components >= 2).then_some(total)
}

fn exact_horizon_cue(
    tokens: &[String],
    amount_index: usize,
    after_unit_index: usize,
    candle_cues: &[&str],
) -> bool {
    let before_start = amount_index.saturating_sub(4);
    let before_end = (amount_index + 1).min(tokens.len());
    let before = &tokens[before_start..before_end];
    let local_before_start = amount_index.saturating_sub(2);
    let local_before = &tokens[local_before_start..before_end];
    let after_end = (after_unit_index + 8).min(tokens.len());
    let after = &tokens[after_unit_index..after_end];
    let candle_interval_cue = after
        .iter()
        .take(2)
        .any(|token| candle_cues.contains(&token.as_str()));
    let direct_horizon =
        has_direct_horizon_cue_before(tokens, amount_index) && !candle_interval_cue;
    let explicit_strategy_horizon = direct_horizon
        || after
            .iter()
            .take(3)
            .any(|token| matches!(token.as_str(), "horizon" | "duration"));
    let mentions_candle_data = before
        .iter()
        .chain(after)
        .any(|token| candle_cues.contains(&token.as_str()));
    if has_timeframe_range_cue(local_before) || (mentions_candle_data && !explicit_strategy_horizon)
    {
        return false;
    }
    direct_horizon
        || amount_index
            .checked_sub(1)
            .is_some_and(|index| tokens[index] == "timeframe")
        || after
            .iter()
            .take(3)
            .any(|token| matches!(token.as_str(), "timeframe" | "horizon" | "duration"))
}

fn has_direct_horizon_cue_before(tokens: &[String], amount_index: usize) -> bool {
    let Some(previous) = amount_index
        .checked_sub(1)
        .map(|index| tokens[index].as_str())
    else {
        return false;
    };
    if matches!(previous, "for" | "over" | "next" | "horizon" | "duration") {
        return true;
    }
    if previous == "timeframe"
        && amount_index >= 2
        && matches!(
            tokens[amount_index - 2].as_str(),
            "strategy" | "run" | "trading"
        )
    {
        return true;
    }
    previous == "of"
        && amount_index >= 2
        && matches!(tokens[amount_index - 2].as_str(), "horizon" | "duration")
}

fn has_timeframe_range_cue(tokens: &[String]) -> bool {
    let words = tokens.iter().map(String::as_str).collect::<Vec<_>>();
    [
        "under",
        "maximum",
        "max",
        "minimum",
        "min",
        "within",
        "boundmarker",
        "approxmarker",
        "approx",
        "approximate",
        "approximately",
        "about",
        "around",
        "roughly",
        "thereabouts",
        "between",
    ]
    .iter()
    .any(|cue| words.contains(cue))
        || words.windows(2).any(|pair| {
            matches!(
                pair,
                ["less", "than"]
                    | ["longer", "than"]
                    | ["shorter", "than"]
                    | ["greater", "than"]
                    | ["at", "most"]
                    | ["at", "least"]
                    | ["up", "to"]
                    | ["no", "less"]
                    | ["or", "more"]
                    | ["or", "less"]
                    | ["or", "greater"]
                    | ["or", "fewer"]
                    | ["or", "longer"]
                    | ["or", "so"]
                    | ["not", "exceeding"]
                    | ["not", "exceed"]
                    | ["no", "longer"]
                    | ["from", "one"]
                    | ["from", "two"]
                    | ["from", "three"]
            )
        })
        || words.windows(3).any(|triple| {
            matches!(
                triple,
                ["no", "more", "than"]
                    | ["more", "than", _]
                    | ["at", "or", "below"]
                    | ["at", "or", "above"]
            )
        })
}

pub fn normalize_contract_timeframe(
    contract: &mut HypothesisContractDraft,
    authoritative_objective: &str,
) -> Result<()> {
    match explicit_user_timeframe_minutes(authoritative_objective)? {
        Some(minutes) => {
            if contract.timeframe.horizon_minutes != minutes {
                contract.timeframe.horizon_minutes = minutes;
                contract.timeframe.label = canonical_timeframe_label(minutes)?;
            }
            contract.timeframe.source = "user-explicit".into();
            contract.timeframe.rationale =
                "Normalized to the exact timeframe stated by the user.".into();
        }
        None => contract.timeframe.source = "world-model-selected".into(),
    }
    Ok(())
}

fn is_lifecycle_duration(tokens: &[String], unit_index: usize) -> bool {
    let start = unit_index.saturating_sub(9);
    for index in start..unit_index {
        let cue = tokens[index].as_str();
        let next = tokens
            .get(index + 1)
            .map(String::as_str)
            .unwrap_or_default();
        if matches!(cue, "stop" | "end" | "terminate" | "quit" | "halt" | "exit")
            && matches!(next, "after" | "in" | "at" | "once" | "when")
        {
            if index + 1 >= unit_index {
                continue;
            }
            let between = &tokens[(index + 2).min(unit_index)..unit_index];
            if !between.iter().any(|token| {
                matches!(
                    token.as_str(),
                    "candle" | "candles" | "bar" | "bars" | "timeframe" | "horizon" | "lookback"
                )
            }) {
                return true;
            }
        }
    }
    false
}

/// Produce a stable display label from the numeric horizon so the model cannot
/// return a label that contradicts the executable timeframe value.
pub fn canonical_timeframe_label(minutes: u64) -> Result<String> {
    if minutes == 0 || minutes > 365 * 24 * 60 {
        bail!("contract timeframe must be between one minute and one year");
    }
    let (value, unit) = if minutes % (60 * 24 * 7) == 0 {
        (minutes / (60 * 24 * 7), "week")
    } else if minutes % (60 * 24) == 0 {
        (minutes / (60 * 24), "day")
    } else if minutes % 60 == 0 {
        (minutes / 60, "hour")
    } else {
        (minutes, "minute")
    };
    Ok(format!(
        "{value} {unit}{}",
        if value == 1 { "" } else { "s" }
    ))
}

pub fn validate_contract_timeframe_against_objective(
    contract: &HypothesisContractDraft,
    authoritative_objective: &str,
) -> Result<()> {
    let mut normalized = contract.clone();
    normalize_contract_timeframe(&mut normalized, authoritative_objective)?;
    if normalized.timeframe.source != contract.timeframe.source {
        bail!("contract timeframe source does not match the canonical user objective");
    }
    Ok(())
}

pub fn explicit_user_stop_limits(objective: &str) -> Result<ContractStopLimits> {
    let normalized_objective = objective
        .to_ascii_lowercase()
        .replace("don't", "dont")
        .replace("doesn't", "doesnt")
        .replace("can't", "cant")
        .replace("won't", "wont")
        .replace("cannot", "cant");
    const SENTENCE_BOUNDARY: &str = "__sentence_boundary__";
    const CLAUSE_BOUNDARY: &str = "__clause_boundary__";
    const COMMA_BOUNDARY: &str = "__comma_boundary__";
    const DECIMAL_POINT: &str = "decimalpoint";
    const SLASH_MARKER: &str = "slashmarker";
    // Keep a decimal point inside a number visible to the parser instead of
    // treating it as sentence punctuation (which could otherwise turn 1.5
    // into an unrelated `1` and `5`).
    let mut objective_with_decimal_markers = String::with_capacity(normalized_objective.len());
    let characters = normalized_objective.chars().collect::<Vec<_>>();
    for (index, character) in characters.iter().copied().enumerate() {
        if character == '.'
            && index > 0
            && index + 1 < characters.len()
            && characters[index - 1].is_ascii_digit()
            && characters[index + 1].is_ascii_digit()
        {
            objective_with_decimal_markers.push(' ');
            objective_with_decimal_markers.push_str(DECIMAL_POINT);
            objective_with_decimal_markers.push(' ');
        } else if character == '/' {
            objective_with_decimal_markers.push(' ');
            objective_with_decimal_markers.push_str(SLASH_MARKER);
            objective_with_decimal_markers.push(' ');
        } else {
            objective_with_decimal_markers.push(character);
        }
    }
    let mut tokens = Vec::new();
    for sentence in objective_with_decimal_markers
        .split(|character| matches!(character, '.' | '!' | '?' | ';' | '\n' | '\r'))
    {
        let sentence = sentence.replace(',', " __comma_boundary__ ");
        let sentence_tokens = sentence
            .split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
            .filter(|token| !token.is_empty())
            .map(str::to_owned)
            .collect::<Vec<_>>();
        for (index, token) in sentence_tokens.iter().enumerate() {
            let coordinated_action = token == "and"
                && sentence_tokens.get(index + 1).is_some_and(|next| {
                    matches!(
                        next.as_str(),
                        "review"
                            | "check"
                            | "inspect"
                            | "analyze"
                            | "analyse"
                            | "reassess"
                            | "wait"
                            | "monitor"
                            | "observe"
                            | "evaluate"
                    )
                });
            if token == COMMA_BOUNDARY {
                tokens.push(COMMA_BOUNDARY.into());
            } else if token == "then" || coordinated_action {
                tokens.push(CLAUSE_BOUNDARY.into());
            } else {
                tokens.push(token.clone());
            }
        }
        tokens.push(SENTENCE_BOUNDARY.into());
    }
    if tokens
        .last()
        .is_some_and(|token| token == SENTENCE_BOUNDARY)
    {
        tokens.pop();
    }
    let parse_number = |token: &str| -> Option<u64> {
        if let Ok(number) = token.parse::<u64>() {
            return Some(number);
        }
        Some(match token {
            "one" | "a" | "an" => 1,
            "two" => 2,
            "three" => 3,
            "four" => 4,
            "five" => 5,
            "six" => 6,
            "seven" => 7,
            "eight" => 8,
            "nine" => 9,
            "ten" => 10,
            "eleven" => 11,
            "twelve" => 12,
            "thirteen" => 13,
            "fourteen" => 14,
            "fifteen" => 15,
            "sixteen" => 16,
            "seventeen" => 17,
            "eighteen" => 18,
            "nineteen" => 19,
            "twenty" => 20,
            _ => return None,
        })
    };
    let is_number_component = |token: &str| {
        token.bytes().all(|byte| byte.is_ascii_digit())
            || matches!(
                token,
                "one"
                    | "a"
                    | "an"
                    | "two"
                    | "three"
                    | "four"
                    | "five"
                    | "six"
                    | "seven"
                    | "eight"
                    | "nine"
                    | "ten"
                    | "eleven"
                    | "twelve"
                    | "thirteen"
                    | "fourteen"
                    | "fifteen"
                    | "sixteen"
                    | "seventeen"
                    | "eighteen"
                    | "nineteen"
                    | "twenty"
                    | "thirty"
                    | "forty"
                    | "fifty"
                    | "sixty"
                    | "seventy"
                    | "eighty"
                    | "ninety"
                    | "hundred"
                    | "thousand"
                    | "million"
                    | "billion"
                    | "trillion"
                    | "dozen"
                    | "couple"
                    | "few"
                    | "several"
                    | "many"
            )
    };
    let is_lifecycle_quantity_unit = |token: &str| {
        matches!(
            token,
            "trade"
                | "trades"
                | "second"
                | "seconds"
                | "minute"
                | "minutes"
                | "hour"
                | "hours"
                | "day"
                | "days"
                | "week"
                | "weeks"
                | "order"
                | "orders"
                | "loss"
                | "losses"
                | "signal"
                | "signals"
                | "session"
                | "sessions"
                | "decision"
                | "decisions"
                | "loop"
                | "loops"
                | "position"
                | "positions"
                | "lot"
                | "lots"
                | "bar"
                | "bars"
                | "entry"
                | "entries"
                | "setup"
                | "setups"
                | "percent"
                | "percentage"
                | "equity"
                | "return"
                | "drawdown"
                | "pips"
                | "pip"
                | "rr"
                | "risk"
                | "month"
                | "months"
                | "year"
                | "years"
                | "trading"
                | "business"
        )
    };
    let is_quantity_qualifier = |token: &str| {
        matches!(
            token,
            "completed"
                | "total"
                | "calendar"
                | "consecutive"
                | "winning"
                | "losing"
                | "profitable"
        )
    };
    let has_lifecycle_cue = |sentence_start: usize, quantity_start: usize, quantity_end: usize| {
        let last_boundary = tokens[sentence_start..quantity_start]
            .iter()
            .rposition(|word| word == CLAUSE_BOUNDARY || word == COMMA_BOUNDARY)
            .map(|position| sentence_start + position);
        let coordinated_comma = last_boundary.filter(|boundary| {
            let continuation = &tokens[*boundary + 1..quantity_start];
            tokens[*boundary] == COMMA_BOUNDARY
                && continuation
                    .first()
                    .is_some_and(|word| matches!(word.as_str(), "and" | "or"))
                && continuation.iter().skip(1).all(|word| {
                    matches!(
                        word.as_str(),
                        "and"
                            | "or"
                            | "after"
                            | "at"
                            | "once"
                            | "when"
                            | "upon"
                            | "following"
                            | "only"
                            | "exactly"
                            | "just"
                            | "the"
                            | "this"
                    )
                })
        });
        let scoped_start = coordinated_comma
            .and_then(|comma| {
                tokens[sentence_start..comma]
                    .iter()
                    .rposition(|word| word == CLAUSE_BOUNDARY || word == COMMA_BOUNDARY)
                    .map(|position| sentence_start + position + 1)
                    .or(Some(sentence_start))
            })
            .or_else(|| last_boundary.map(|position| position + 1))
            .unwrap_or(sentence_start);
        let sentence_end = quantity_end.min(quantity_start).min(tokens.len());
        let explicit_limit = (scoped_start..sentence_end).any(|position| {
            let word = tokens[position].as_str();
            let limit_word = matches!(
                word,
                "maximum" | "max" | "limit" | "capped" | "atmost" | "upto" | "exceeding"
            ) || (word == "up"
                && tokens.get(position + 1).is_some_and(|next| next == "to"));
            let no_intervening_quantity = tokens[position + 1..quantity_start.min(tokens.len())]
                .iter()
                .all(|between| {
                    !is_number_component(between) && !is_lifecycle_quantity_unit(between)
                });
            (limit_word && position < quantity_start && no_intervening_quantity)
                || (word == "than"
                    && position > scoped_start
                    && tokens[position - 1] == "more"
                    && ((position >= scoped_start + 2
                        && tokens[position - 2] == "no"
                        && (position - 2 == scoped_start
                            || (position >= scoped_start + 3
                                && matches!(
                                    tokens[position - 3].as_str(),
                                    "please"
                                        | "must"
                                        | "should"
                                        | "can"
                                        | "will"
                                        | "allow"
                                        | "permit"
                                        | "trade"
                                        | "trading"
                                        | "execute"
                                        | "open"
                                        | "make"
                                        | "place"
                                ))))
                        || (position >= scoped_start + 3
                            && tokens[position - 3] == "not"
                            && tokens[position - 2] == "trade"))
                    && position < quantity_start
                    && tokens[position + 1..quantity_start]
                        .iter()
                        .all(|between| !is_number_component(between)))
        });
        if explicit_limit {
            return true;
        }
        (scoped_start..sentence_end).any(|position| {
            let word = tokens[position].as_str();
            let stop_is_part_of_stop_loss = word == "stop"
                && (tokens
                    .get(position + 1)
                    .is_some_and(|next| next == "loss" || next == "losses")
                    || (position > sentence_start
                        && matches!(tokens[position - 1].as_str(), "loss" | "losses")));
            if stop_is_part_of_stop_loss
                || !matches!(
                    word,
                    "stop" | "halt" | "terminate" | "end" | "cease" | "quit" | "pause"
                )
            {
                return false;
            }
            if position >= quantity_start {
                return false;
            }
            let between = &tokens[position + 1..quantity_start];
            let previous_cap = between.iter().enumerate().any(|(offset, word)| {
                if !is_number_component(word) {
                    return false;
                }
                between[offset + 1..]
                    .iter()
                    .take(5)
                    .enumerate()
                    .any(|(unit_offset, unit)| {
                        is_lifecycle_quantity_unit(unit)
                            && between[offset + 1..offset + 1 + unit_offset]
                                .iter()
                                .all(|modifier| is_quantity_qualifier(modifier))
                    })
            });
            if previous_cap {
                let previous_unit_end = between
                    .iter()
                    .rposition(|word| is_lifecycle_quantity_unit(word))
                    .map(|unit| unit + 1)
                    .unwrap_or(0);
                if !between[previous_unit_end..].iter().all(|word| {
                    matches!(
                        word.as_str(),
                        "and"
                            | "or"
                            | COMMA_BOUNDARY
                            | "after"
                            | "at"
                            | "once"
                            | "when"
                            | "upon"
                            | "following"
                            | "only"
                            | "exactly"
                            | "just"
                            | "the"
                            | "this"
                    )
                }) {
                    return false;
                }
            }
            let has_connector = between.iter().any(|connector| {
                matches!(
                    connector.as_str(),
                    "after" | "at" | "once" | "when" | "upon" | "following"
                )
            });
            let identifies_trading_target = between.iter().any(|word| {
                matches!(
                    word.as_str(),
                    "trade"
                        | "trades"
                        | "trading"
                        | "run"
                        | "loop"
                        | "strategy"
                        | "campaign"
                        | "bot"
                        | "system"
                        | "account"
                        | "operation"
                        | "operations"
                        | "activity"
                        | "session"
                )
            });
            let only_connective_fillers_precede = between
                .iter()
                .take_while(|word| {
                    !matches!(
                        word.as_str(),
                        "after" | "at" | "once" | "when" | "upon" | "following"
                    )
                })
                .all(|word| matches!(word.as_str(), "only" | "exactly" | "just" | "the" | "this"));
            has_connector && (identifies_trading_target || only_connective_fillers_precede)
        })
    };
    let has_unitless_lifecycle_cue = |sentence_start: usize, quantity_start: usize| {
        let scoped_start = tokens[sentence_start..quantity_start]
            .iter()
            .rposition(|word| word == CLAUSE_BOUNDARY || word == COMMA_BOUNDARY)
            .map(|position| sentence_start + position + 1)
            .unwrap_or(sentence_start);
        let connected_stop = (scoped_start..quantity_start).any(|position| {
            if !matches!(
                tokens[position].as_str(),
                "stop" | "halt" | "terminate" | "end" | "cease" | "quit" | "pause"
            ) {
                return false;
            }
            let between = &tokens[position + 1..quantity_start];
            let has_connector = between.iter().any(|word| {
                matches!(
                    word.as_str(),
                    "after" | "at" | "once" | "when" | "upon" | "following"
                )
            });
            let identifies_trading_target = between.iter().any(|word| {
                matches!(
                    word.as_str(),
                    "trade"
                        | "trades"
                        | "trading"
                        | "run"
                        | "loop"
                        | "strategy"
                        | "campaign"
                        | "bot"
                        | "system"
                        | "account"
                        | "operation"
                        | "operations"
                        | "activity"
                        | "session"
                )
            });
            let only_connective_fillers_precede = between
                .iter()
                .take_while(|word| {
                    !matches!(
                        word.as_str(),
                        "after" | "at" | "once" | "when" | "upon" | "following"
                    )
                })
                .all(|word| matches!(word.as_str(), "only" | "exactly" | "just" | "the" | "this"));
            has_connector && (identifies_trading_target || only_connective_fillers_precede)
        });
        let comparison_with_trade_verb = (scoped_start..quantity_start).any(|position| {
            position + 3 < quantity_start
                && tokens[position] == "not"
                && tokens[position + 1] == "trade"
                && tokens[position + 2] == "more"
                && tokens[position + 3] == "than"
        });
        let bare_comparison_at_end = quantity_start + 1 == tokens.len()
            && quantity_start >= scoped_start + 3
            && tokens[quantity_start - 1] == "than"
            && tokens[quantity_start - 2] == "more"
            && tokens[quantity_start - 3] == "no";
        connected_stop || comparison_with_trade_verb || bare_comparison_at_end
    };
    for (index, token) in tokens.iter().enumerate() {
        if !is_number_component(token) {
            continue;
        }
        let sentence_start = tokens[..index]
            .iter()
            .rposition(|word| word == SENTENCE_BOUNDARY)
            .map(|position| position + 1)
            .unwrap_or(0);
        let next_end = index.saturating_add(5).min(tokens.len());
        let following = &tokens[index + 1..next_end];
        let has_quantity_unit = following.iter().enumerate().any(|(offset, next)| {
            is_lifecycle_quantity_unit(next)
                && following[..offset]
                    .iter()
                    .all(|modifier| is_quantity_qualifier(modifier))
        });
        if !has_quantity_unit && has_unitless_lifecycle_cue(sentence_start, index) {
            bail!("lifecycle stop instruction has a quantity without a supported unit");
        }
    }
    let mut expected_trade_caps = HashSet::new();
    let mut expected_duration_caps = HashSet::new();
    for (index, token) in tokens.iter().enumerate().skip(1) {
        let direct_amount = parse_number(&tokens[index - 1]).map(|amount| (amount, index - 1));
        let mut qualified_index = index.checked_sub(1);
        while qualified_index.is_some_and(|candidate| {
            matches!(
                tokens[candidate].as_str(),
                "completed"
                    | "total"
                    | "calendar"
                    | "consecutive"
                    | "winning"
                    | "losing"
                    | "profitable"
            )
        }) {
            qualified_index = qualified_index.and_then(|candidate| candidate.checked_sub(1));
        }
        let qualified_amount = qualified_index.and_then(|amount_index| {
            parse_number(&tokens[amount_index]).map(|amount| (amount, amount_index))
        });
        let Some((amount, amount_index)) = direct_amount.or(qualified_amount) else {
            // If a lifecycle cue and a number-like token are present next to a
            // supported cap unit, reject unsupported/overflowing number forms
            // instead of silently treating the objective as having no cap.
            let local_start = tokens[..index]
                .iter()
                .rposition(|word| word == SENTENCE_BOUNDARY)
                .map(|position| position + 1)
                .unwrap_or(0);
            let local = &tokens[local_start..index];
            let quantity_start = (local_start..index)
                .rev()
                .find(|position| is_number_component(&tokens[*position]))
                .unwrap_or(index);
            let has_stop_cue =
                has_lifecycle_cue(local_start, quantity_start, index.saturating_add(2));
            let has_nearby_number = local.iter().any(|word| is_number_component(word));
            let is_cap_unit = matches!(
                token.as_str(),
                "trade"
                    | "trades"
                    | "second"
                    | "seconds"
                    | "minute"
                    | "minutes"
                    | "hour"
                    | "hours"
                    | "day"
                    | "days"
                    | "week"
                    | "weeks"
                    | "order"
                    | "orders"
                    | "loss"
                    | "losses"
                    | "signal"
                    | "signals"
                    | "session"
                    | "sessions"
                    | "decision"
                    | "decisions"
                    | "loop"
                    | "loops"
                    | "position"
                    | "positions"
                    | "lot"
                    | "lots"
                    | "bar"
                    | "bars"
                    | "entry"
                    | "entries"
                    | "setup"
                    | "setups"
                    | "percent"
                    | "percentage"
                    | "equity"
                    | "return"
                    | "drawdown"
                    | "pips"
                    | "pip"
                    | "rr"
                    | "risk"
                    | "month"
                    | "months"
                    | "year"
                    | "years"
                    | "trading"
                    | "business"
            );
            if has_stop_cue && has_nearby_number && is_cap_unit {
                bail!("unsupported or out-of-range lifecycle quantity is ambiguous");
            }
            continue;
        };
        let clause_start = tokens[..amount_index]
            .iter()
            .rposition(|word| word == SENTENCE_BOUNDARY || word == CLAUSE_BOUNDARY)
            .map(|position| position + 1)
            .unwrap_or(0);
        let mut clause_end = tokens[index + 1..]
            .iter()
            .position(|word| {
                word == SENTENCE_BOUNDARY || word == CLAUSE_BOUNDARY || word == COMMA_BOUNDARY
            })
            .map(|position| index + 1 + position)
            .unwrap_or(tokens.len());
        if tokens
            .get(clause_end)
            .is_some_and(|word| word == COMMA_BOUNDARY)
        {
            let modifier_start = clause_end + 1;
            let is_postposed_modifier = |word: &str| {
                matches!(
                    word,
                    "per"
                        | "each"
                        | "every"
                        | "daily"
                        | "weekly"
                        | "monthly"
                        | "hourly"
                        | "perday"
                        | "perweek"
                        | "permonth"
                        | "perhour"
                        | "in"
                        | "a"
                        | "an"
                        | "once"
                        | "twice"
                        | "not"
                        | "never"
                        | "dont"
                        | "doesnt"
                        | "cant"
                        | "wont"
                        | "without"
                        | "would"
                        | "could"
                        | "might"
                        | "hypothetical"
                        | "hypothetically"
                        | "unless"
                        | "except"
                        | "if"
                        | "but"
                        | "although"
                        | "however"
                        | "provided"
                        | "providing"
                        | "whereas"
                )
            };
            if tokens
                .get(modifier_start)
                .is_some_and(|word| is_postposed_modifier(word))
            {
                clause_end = tokens[modifier_start..]
                    .iter()
                    .position(|word| {
                        word == SENTENCE_BOUNDARY
                            || word == CLAUSE_BOUNDARY
                            || word == COMMA_BOUNDARY
                    })
                    .map(|position| modifier_start + position)
                    .unwrap_or(tokens.len());
            }
        }
        let clause = &tokens[clause_start..clause_end];
        let is_stop_clause = has_lifecycle_cue(clause_start, amount_index, index.saturating_add(2));
        if !is_stop_clause {
            continue;
        }
        let has_compound_connector = amount_index >= 2
            && tokens[amount_index - 1] == "and"
            && is_number_component(&tokens[amount_index - 2]);
        if (amount_index > 0 && is_number_component(&tokens[amount_index - 1]))
            || has_compound_connector
        {
            bail!("composite lifecycle quantities are unsupported; refusing to guess a cap");
        }
        if clause.iter().any(|word| word == DECIMAL_POINT) {
            bail!("decimal lifecycle limits are ambiguous and must not be silently rounded");
        }
        let cadence_qualifiers = [
            ["per", "day"].as_slice(),
            ["each", "day"].as_slice(),
            ["every", "day"].as_slice(),
            ["per", "week"].as_slice(),
            ["each", "week"].as_slice(),
            ["every", "week"].as_slice(),
            ["per", "month"].as_slice(),
            ["each", "month"].as_slice(),
            ["every", "month"].as_slice(),
            ["per", "hour"].as_slice(),
            ["each", "hour"].as_slice(),
            ["every", "hour"].as_slice(),
            ["per", "session"].as_slice(),
            ["each", "session"].as_slice(),
            ["every", "session"].as_slice(),
            ["a", "day"].as_slice(),
            ["an", "hour"].as_slice(),
            ["a", "week"].as_slice(),
            ["a", "month"].as_slice(),
        ];
        let cadence_word = clause.iter().any(|word| {
            matches!(
                word.as_str(),
                "daily"
                    | "weekly"
                    | "monthly"
                    | "hourly"
                    | "perday"
                    | "perweek"
                    | "permonth"
                    | "perhour"
            )
        });
        let cadence_after_count = clause.windows(3).any(|window| {
            matches!(window[0].as_str(), "per" | "each" | "every" | "in")
                && (is_number_component(&window[1])
                    || matches!(window[1].as_str(), "trading" | "a" | "an"))
                && matches!(
                    window[2].as_str(),
                    "day"
                        | "days"
                        | "week"
                        | "weeks"
                        | "month"
                        | "months"
                        | "hour"
                        | "hours"
                        | "session"
                        | "sessions"
                )
        });
        let cadence_with_article = clause.windows(2).any(|window| {
            matches!(window[0].as_str(), "a" | "an" | "one")
                && matches!(
                    window[1].as_str(),
                    "day"
                        | "days"
                        | "week"
                        | "weeks"
                        | "month"
                        | "months"
                        | "hour"
                        | "hours"
                        | "session"
                        | "sessions"
                )
        });
        let cadence_with_other_day = clause.windows(3).any(|window| {
            matches!(window[0].as_str(), "every" | "each")
                && matches!(window[1].as_str(), "other" | "alternate" | "single")
                && matches!(window[2].as_str(), "day" | "week" | "month")
        }) || clause.windows(3).any(|window| {
            matches!(window[0].as_str(), "twice" | "once")
                && matches!(window[1].as_str(), "a" | "an" | "per")
                && matches!(window[2].as_str(), "day" | "week" | "month" | "hour")
        });
        let slash_cadence = clause.windows(3).any(|window| {
            matches!(
                window[0].as_str(),
                "trade" | "trades" | "day" | "days" | "week" | "weeks"
            ) && window[1] == SLASH_MARKER
                && matches!(
                    window[2].as_str(),
                    "day"
                        | "days"
                        | "week"
                        | "weeks"
                        | "month"
                        | "months"
                        | "hour"
                        | "hours"
                        | "session"
                        | "sessions"
                )
        });
        if cadence_word
            || cadence_after_count
            || cadence_with_article
            || cadence_with_other_day
            || slash_cadence
            || cadence_qualifiers.iter().any(|qualifier| {
                clause.windows(qualifier.len()).any(|window| {
                    window
                        .iter()
                        .zip(qualifier.iter())
                        .all(|(word, expected)| word.as_str() == *expected)
                })
            })
        {
            bail!("cadence-qualified lifecycle caps are unsupported; refusing to convert them into run-wide caps");
        }
        let is_positive_comparison_cap = clause
            .windows(2)
            .any(|pair| matches!(pair[0].as_str(), "no" | "not") && pair[1] == "more")
            || clause.windows(4).any(|words| {
                words[0] == "not" && words[1] == "trade" && words[2] == "more" && words[3] == "than"
            })
            || clause
                .windows(3)
                .any(|triple| triple[0] == "no" && triple[1] == "more" && triple[2] == "than")
            || clause
                .windows(2)
                .any(|pair| pair[0] == "up" && pair[1] == "to")
            || clause
                .windows(2)
                .any(|pair| pair[0] == "without" && pair[1] == "exceeding");
        let has_negation = clause.iter().any(|word| {
            matches!(
                word.as_str(),
                "not" | "never" | "dont" | "doesnt" | "cant" | "wont" | "without"
            )
        });
        let negates_stop_instruction = clause.iter().enumerate().any(|(offset, word)| {
            if !matches!(
                word.as_str(),
                "not" | "never" | "dont" | "doesnt" | "cant" | "wont" | "without"
            ) {
                return false;
            }
            clause
                .iter()
                .enumerate()
                .skip(offset + 1)
                .any(|(later_offset, later)| {
                    let token_index = clause_start + later_offset;
                    let stop_is_part_of_stop_loss = later == "stop"
                        && (tokens
                            .get(token_index + 1)
                            .is_some_and(|next| next == "loss" || next == "losses")
                            || (token_index > 0
                                && matches!(tokens[token_index - 1].as_str(), "loss" | "losses")));
                    !stop_is_part_of_stop_loss
                        && matches!(
                            later.as_str(),
                            "stop" | "halt" | "terminate" | "end" | "cease" | "quit" | "pause"
                        )
                })
        });
        if negates_stop_instruction || (has_negation && !is_positive_comparison_cap) {
            bail!("negated lifecycle stop language near an explicit quantity is ambiguous");
        }
        if clause.iter().any(|word| {
            matches!(
                word.as_str(),
                "hypothetical" | "hypothetically" | "would" | "could" | "might"
            )
        }) {
            bail!("hypothetical lifecycle language near an explicit quantity is ambiguous");
        }
        let conditional_after_comma_coordination = if tokens
            .get(clause_end)
            .is_some_and(|word| word == COMMA_BOUNDARY)
            && tokens
                .get(clause_end + 1)
                .is_some_and(|word| matches!(word.as_str(), "and" | "or"))
        {
            let continuation_start = clause_end + 2;
            let continuation_end = tokens[continuation_start..]
                .iter()
                .position(|word| {
                    word == SENTENCE_BOUNDARY || word == CLAUSE_BOUNDARY || word == COMMA_BOUNDARY
                })
                .map(|position| continuation_start + position)
                .unwrap_or(tokens.len());
            let continuation = &tokens[continuation_start..continuation_end];
            continuation.first().is_some_and(|word| {
                matches!(
                    word.as_str(),
                    "if" | "unless"
                        | "except"
                        | "provided"
                        | "providing"
                        | "but"
                        | "although"
                        | "however"
                        | "whereas"
                ) || (word == "only" && continuation.get(1).is_some_and(|next| next == "if"))
            })
        } else {
            false
        };
        if conditional_after_comma_coordination
            || tokens[index + 1..clause_end].iter().any(|word| {
                matches!(
                    word.as_str(),
                    "if" | "unless"
                        | "except"
                        | "provided"
                        | "providing"
                        | "but"
                        | "although"
                        | "however"
                        | "whereas"
                )
            })
        {
            bail!("conditional or contrastive lifecycle qualifier is ambiguous");
        }
        if matches!(token.as_str(), "trade" | "trades") {
            if tokens[amount_index + 1..index].iter().any(|qualifier| {
                matches!(
                    qualifier.as_str(),
                    "loss"
                        | "losses"
                        | "losing"
                        | "winning"
                        | "win"
                        | "wins"
                        | "profitable"
                        | "consecutive"
                )
            }) || (amount_index > 0
                && matches!(
                    tokens[amount_index - 1].as_str(),
                    "loss" | "losses" | "losing"
                ))
            {
                bail!("qualified trade stops are not total completed-trade caps");
            }
            expected_trade_caps.insert(amount);
            continue;
        }
        let seconds_per_unit = match token.as_str() {
            "second" | "seconds" => Some(1),
            "minute" | "minutes" => Some(60),
            "hour" | "hours" => Some(3_600),
            "day" | "days" => Some(86_400),
            "week" | "weeks" => Some(604_800),
            _ => None,
        };
        if let Some(multiplier) = seconds_per_unit {
            expected_duration_caps.insert(
                amount
                    .checked_mul(multiplier)
                    .context("explicit elapsed-time limit exceeds supported range")?,
            );
        } else if matches!(
            token.as_str(),
            "order"
                | "orders"
                | "loss"
                | "losses"
                | "signal"
                | "signals"
                | "session"
                | "sessions"
                | "decision"
                | "decisions"
                | "loop"
                | "loops"
                | "position"
                | "positions"
                | "lot"
                | "lots"
                | "bar"
                | "bars"
                | "entry"
                | "entries"
                | "setup"
                | "setups"
                | "percent"
                | "percentage"
                | "equity"
                | "return"
                | "drawdown"
                | "pips"
                | "pip"
                | "rr"
                | "risk"
                | "month"
                | "months"
                | "year"
                | "years"
                | "trading"
                | "business"
        ) {
            bail!("explicit lifecycle stop unit {token} is not supported; refusing to silently ignore it");
        }
    }
    if expected_trade_caps.len() > 1 || expected_duration_caps.len() > 1 {
        bail!("user objective states conflicting explicit lifecycle limits");
    }
    Ok(ContractStopLimits {
        maximum_elapsed_seconds: expected_duration_caps.into_iter().next(),
        maximum_completed_trades: expected_trade_caps
            .into_iter()
            .next()
            .map(u32::try_from)
            .transpose()
            .context("explicit trade cap exceeds supported range")?,
    })
}

pub fn validate_and_activate(
    contract: &mut HypothesisContract,
    context: &ContractValidationContext,
    now: DateTime<Utc>,
) -> Result<()> {
    if contract.state != ContractState::Draft {
        bail!("only DRAFT may transition through the activation gate");
    }
    if contract.id.trim().is_empty()
        || contract.run_id.trim().is_empty()
        || contract.version == 0
        || contract.created_by_event_id.trim().is_empty()
    {
        bail!("contract identity and version are required before activation");
    }
    validate_contract(&contract.proposal, context, now)?;
    contract.state = ContractState::Active;
    Ok(())
}

#[cfg(test)]
mod lifecycle_limit_tests {
    use super::*;

    #[test]
    fn explicit_trade_and_elapsed_time_limits_are_parsed_separately() {
        let limits = explicit_user_stop_limits("Stop after 3 trades or 4 days").unwrap();
        assert_eq!(limits.maximum_completed_trades, Some(3));
        assert_eq!(limits.maximum_elapsed_seconds, Some(4 * 24 * 60 * 60));
        let limits = explicit_user_stop_limits("Stop after 3 trades, or 4 days").unwrap();
        assert_eq!(limits.maximum_completed_trades, Some(3));
        assert_eq!(limits.maximum_elapsed_seconds, Some(4 * 24 * 60 * 60));
        let limits = explicit_user_stop_limits("Stop after 3 trades, or after 4 days").unwrap();
        assert_eq!(limits.maximum_completed_trades, Some(3));
        assert_eq!(limits.maximum_elapsed_seconds, Some(4 * 24 * 60 * 60));
        let limits = explicit_user_stop_limits("Stop after 3 trades, and 4 days").unwrap();
        assert_eq!(limits.maximum_completed_trades, Some(3));
        assert_eq!(limits.maximum_elapsed_seconds, Some(4 * 24 * 60 * 60));
        assert_eq!(
            explicit_user_stop_limits("Stop after 3 completed trades")
                .unwrap()
                .maximum_completed_trades,
            Some(3)
        );
    }

    #[test]
    fn stop_deadlines_are_not_mistaken_for_strategy_timeframes() {
        assert_eq!(
            explicit_user_timeframe_minutes(
                "Test BTCUSD and stop after 3 completed trades or 4 days"
            )
            .unwrap(),
            None
        );
        assert_eq!(
            explicit_user_timeframe_minutes("Test BTCUSD over 1 hour and stop after 4 days")
                .unwrap(),
            Some(60)
        );
    }

    #[test]
    fn lifecycle_cues_do_not_bleed_into_later_clauses_or_discussion_text() {
        let limits =
            explicit_user_stop_limits("Stop after 3 trades and review data after 4 days").unwrap();
        assert_eq!(limits.maximum_completed_trades, Some(3));
        assert_eq!(limits.maximum_elapsed_seconds, None);

        let limits =
            explicit_user_stop_limits("Set a maximum of 3 indicators, then review after 4 days")
                .unwrap();
        assert_eq!(limits.maximum_completed_trades, None);
        assert_eq!(limits.maximum_elapsed_seconds, None);

        let limits = explicit_user_stop_limits("Stop discussing after 3 indicators").unwrap();
        assert_eq!(limits.maximum_completed_trades, None);
        assert_eq!(limits.maximum_elapsed_seconds, None);
    }

    #[test]
    fn conflicting_explicit_trade_caps_are_rejected() {
        assert!(
            explicit_user_stop_limits("Stop after 3 trades, with a maximum of 5 trades").is_err()
        );
    }

    #[test]
    fn decimal_and_overflowing_time_caps_fail_closed() {
        assert!(explicit_user_stop_limits("Stop after 1.5 days").is_err());
        assert!(explicit_user_stop_limits("Stop after 18446744073709551615 days").is_err());
        assert!(explicit_user_stop_limits("Stop after 18446744073709551616 days").is_err());
        assert!(explicit_user_stop_limits("Stop after one hundred trades").is_err());
        assert!(explicit_user_stop_limits("Stop after one million trades").is_err());
        assert!(explicit_user_stop_limits("Stop after a dozen trades").is_err());
        assert!(explicit_user_stop_limits("Stop after many trades").is_err());
        assert!(explicit_user_stop_limits("No more than 3 trades per day").is_err());
        assert!(
            explicit_user_stop_limits("Do not trade more than 3 completed trades per day").is_err()
        );
        assert!(explicit_user_stop_limits("No more than 3 trades daily").is_err());
        assert!(explicit_user_stop_limits("Stop after 3 trades, per day").is_err());
        assert!(explicit_user_stop_limits("No more than 3 trades, daily").is_err());
        assert!(explicit_user_stop_limits("Stop after 3 trades, every day").is_err());
        assert!(explicit_user_stop_limits("Stop after 3 trades, unless profitable").is_err());
        assert!(explicit_user_stop_limits("Stop after 3 trades, if the market is calm").is_err());
        assert!(explicit_user_stop_limits("Stop after 3 trades, but only if profitable").is_err());
        assert!(explicit_user_stop_limits("Stop after 3 trades, or if profitable").is_err());
        assert_eq!(
            explicit_user_stop_limits("Stop after 3 trades, or review if profitable")
                .unwrap()
                .maximum_completed_trades,
            Some(3)
        );
        assert!(
            explicit_user_stop_limits("Stop after 3 trades, provided conditions hold").is_err()
        );
        assert!(explicit_user_stop_limits("Stop after 3 trades, whereas conditions hold").is_err());
        assert!(explicit_user_stop_limits("Stop after 3 trades/day").is_err());
        assert!(explicit_user_stop_limits("Stop after 3 trades per 24 hours").is_err());
        assert!(explicit_user_stop_limits("Stop after 3 trades every trading day").is_err());
        assert!(explicit_user_stop_limits("Stop after 3 trades a day").is_err());
        assert!(explicit_user_stop_limits("Stop after 3 trades every other day").is_err());
        assert!(explicit_user_stop_limits("Stop after 3 trades in a day").is_err());
        assert!(explicit_user_stop_limits("Stop after 3").is_err());
        assert!(explicit_user_stop_limits("Do not trade more than 3").is_err());
        assert!(explicit_user_stop_limits("No more than 3").is_err());
        assert!(explicit_user_stop_limits("Stop after 3 indicators and 4 days").is_err());
        assert_eq!(
            explicit_user_stop_limits("The strategy has a limit of 3 indicators")
                .unwrap()
                .maximum_completed_trades,
            None
        );
        assert!(explicit_user_stop_limits("Stop after twenty one trades").is_err());
        assert!(explicit_user_stop_limits("Stop after twenty and one trades").is_err());
        assert!(explicit_user_stop_limits("Stop after 18446744073709551616 months").is_err());
        assert!(
            explicit_user_stop_limits("Set a stop loss of 18446744073709551616 trades")
                .unwrap()
                .maximum_completed_trades
                .is_none()
        );
        assert_eq!(
            explicit_user_stop_limits("Stop after 3 total completed trades")
                .unwrap()
                .maximum_completed_trades,
            Some(3)
        );
        assert_eq!(
            explicit_user_stop_limits("Use model v1.5. Stop after 3 trades")
                .unwrap()
                .maximum_completed_trades,
            Some(3)
        );
        assert_eq!(
            explicit_user_stop_limits(
                "Stop trading for risk management and account stability purposes after 3 trades"
            )
            .unwrap()
            .maximum_completed_trades,
            Some(3)
        );
        assert_eq!(
            explicit_user_stop_limits("Stop discussing 3 trades")
                .unwrap()
                .maximum_completed_trades,
            None
        );
        assert_eq!(
            explicit_user_stop_limits("Stop discussing after 3 trades")
                .unwrap()
                .maximum_completed_trades,
            None
        );
        assert_eq!(
            explicit_user_stop_limits("We discussed more than 3 trades today")
                .unwrap()
                .maximum_completed_trades,
            None
        );
        assert_eq!(
            explicit_user_stop_limits("No more than 3 trades")
                .unwrap()
                .maximum_completed_trades,
            Some(3)
        );
        assert_eq!(
            explicit_user_stop_limits("We discussed no more than 3 trades")
                .unwrap()
                .maximum_completed_trades,
            None
        );
        assert_eq!(
            explicit_user_stop_limits("Stop discussing no more than 3 trades")
                .unwrap()
                .maximum_completed_trades,
            None
        );
    }

    #[test]
    fn negated_or_unsupported_stop_language_is_not_silently_dropped() {
        assert!(explicit_user_stop_limits("Don't stop after 3 trades").is_err());
        assert!(explicit_user_stop_limits("Stop after 3 losing trades").is_err());
        assert!(explicit_user_stop_limits("Stop after 4 sessions").is_err());
        assert_eq!(
            explicit_user_stop_limits("Use a 3 day context window")
                .unwrap()
                .maximum_elapsed_seconds,
            None
        );
    }

    #[test]
    fn stop_cap_parser_handles_comparison_caps_and_avoids_stop_loss_false_positives() {
        assert_eq!(
            explicit_user_stop_limits("Do not trade more than 3 completed trades")
                .unwrap()
                .maximum_completed_trades,
            Some(3)
        );
        assert_eq!(
            explicit_user_stop_limits("Use a 2% stop loss and a maximum of 3 trades")
                .unwrap()
                .maximum_completed_trades,
            Some(3)
        );
        assert!(explicit_user_stop_limits("Stop after 5% campaign return").is_err());
        assert!(explicit_user_stop_limits("Stop after 2 entries").is_err());
        assert!(explicit_user_stop_limits("Hypothetically stop after 3 trades").is_err());
        assert!(explicit_user_stop_limits("Do not stop after more than 3 trades").is_err());
        assert!(explicit_user_stop_limits("Do not stop after no more than 3 trades").is_err());
        assert_eq!(
            explicit_user_stop_limits(
                "Hypothetically, the market may reverse today. Stop after 3 trades"
            )
            .unwrap()
            .maximum_completed_trades,
            Some(3)
        );
    }
}
