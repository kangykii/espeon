//! Discoverable, permission-scoped capabilities for world-model proposals.
//!
//! Skills are compiled Rust handlers, not arbitrary model tools. Each handler
//! publishes a manifest to the model and validates its own typed arguments.

use crate::contracts::{
    ContractReviewTriggers, ContractStopLimits, HypothesisContractDraft, SkillInvocation,
};
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashSet};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillManifest {
    pub id: String,
    pub name: String,
    pub description: String,
    pub authority: String,
    pub input_schema: Value,
}

pub trait WorldModelSkill: Send + Sync {
    fn manifest(&self) -> SkillManifest;
    fn apply(&self, arguments: &Value, contract: &mut HypothesisContractDraft) -> Result<()>;
}

#[derive(Default)]
pub struct SkillRegistry {
    entries: BTreeMap<String, Box<dyn WorldModelSkill>>,
}

impl SkillRegistry {
    pub fn discover_builtin() -> Self {
        let mut registry = Self::default();
        registry.register(Box::new(LoopLifecycleSkill));
        registry
    }

    pub fn register(&mut self, skill: Box<dyn WorldModelSkill>) {
        let id = skill.manifest().id;
        self.entries.insert(id, skill);
    }

    pub fn manifests(&self) -> Vec<SkillManifest> {
        self.entries
            .values()
            .map(|skill| skill.manifest())
            .collect()
    }

    /// Build the strict JSON-schema union used for model-authored invocations.
    /// Each branch binds one discovered skill ID to that skill's own argument
    /// schema, so adding a handler also makes its typed inputs model-visible.
    pub fn invocation_schema(&self) -> Value {
        let variants = self
            .entries
            .iter()
            .map(|(id, skill)| {
                let mut arguments = skill.manifest().input_schema;
                make_strict_schema(&mut arguments);
                json!({
                    "type":"object",
                    "additionalProperties":false,
                    "required":["skillId","arguments"],
                    "properties":{
                        "skillId":{"type":"string","enum":[id]},
                        "arguments":arguments
                    }
                })
            })
            .collect::<Vec<_>>();
        json!({"anyOf":variants})
    }

    /// One discovery catalog for handler-backed contract skills and the
    /// existing read-only tool capabilities. The latter retain their own
    /// typed request channels and do not become arbitrary contract handlers.
    pub fn catalog_json(
        &self,
        web_research_available: bool,
        broker_context_available: bool,
    ) -> Value {
        json!({
            "contractSkills": self.manifests(),
            "readOnlyCapabilities": [
                {
                    "id":"research.web_search",
                    "name":"Controlled web research",
                    "available":web_research_available,
                    "authority":"Read-only source search and fetch; no trade recommendations or execution.",
                    "requestChannel":"controlledResearchTools",
                    "selectionProtocol":"When enabled for the request, the controlled research stage exposes only the documented web_search and web_fetch tools. Use those native tool schemas; do not return this capability as a skill invocation."
                },
                {
                    "id":"broker_context.ctrader_read_only",
                    "name":"cTrader broker context",
                    "available":broker_context_available,
                    "authority":"Read-only account, positions, and symbol metadata. No order placement or modification.",
                    "requestChannel":"brokerContextRequests",
                    "inputSchema":{"type":"array","uniqueItems":true,"items":{"type":"string","enum":["account","positions","symbol_details"]}},
                    "selectionProtocol":"If available and genuinely required, select only values from the brokerContextRequests response field. After one retrieval, request no further broker context."
                }
            ]
        })
    }

    pub fn apply_invocations(
        &self,
        invocations: &[SkillInvocation],
        contract: &mut HypothesisContractDraft,
    ) -> Result<()> {
        let mut seen = HashSet::new();
        for invocation in invocations {
            if !seen.insert(invocation.skill_id.as_str()) {
                bail!("skill {} was requested more than once", invocation.skill_id);
            }
            let skill = self.entries.get(&invocation.skill_id).with_context(|| {
                format!(
                    "world model requested undiscovered skill {}",
                    invocation.skill_id
                )
            })?;
            skill.apply(&invocation.arguments, contract)?;
        }
        Ok(())
    }

    /// Normalize and apply required lifecycle behavior for every model-authored
    /// contract. Missing lifecycle invocation gets prompt stop caps and policy
    /// defaults; supplied caps may tighten, but never relax, user-specified caps.
    pub fn normalize_and_apply_contract(
        &self,
        contract: &mut HypothesisContractDraft,
    ) -> Result<()> {
        let authoritative_objective = contract.user_objective.clone();
        self.normalize_and_apply_contract_against_objective(contract, &authoritative_objective)
    }

    pub fn normalize_and_apply_contract_against_objective(
        &self,
        contract: &mut HypothesisContractDraft,
        authoritative_objective: &str,
    ) -> Result<()> {
        let explicit_limits = crate::contracts::explicit_user_stop_limits(authoritative_objective)?;
        let lifecycle = contract
            .skill_invocations
            .iter()
            .filter(|invocation| invocation.skill_id == "loop.lifecycle_limits")
            .collect::<Vec<_>>();

        if lifecycle.is_empty() {
            contract.skill_invocations.push(SkillInvocation {
                skill_id: "loop.lifecycle_limits".into(),
                arguments: json!({
                    "maximumElapsedSeconds":explicit_limits.maximum_elapsed_seconds,
                    "maximumCompletedTrades":explicit_limits.maximum_completed_trades,
                    "noTradeDecisions":12,
                    "consecutiveLosses":3,
                    "completedTrades":10
                }),
            });
        } else {
            if lifecycle.len() > 1 {
                bail!("loop.lifecycle_limits was requested more than once");
            }
            let arguments = &lifecycle[0].arguments;
            let elapsed_seconds = arguments
                .get("maximumElapsedSeconds")
                .and_then(Value::as_u64);
            let completed_trades = arguments
                .get("maximumCompletedTrades")
                .and_then(Value::as_u64)
                .map(u32::try_from)
                .transpose()
                .context("maximumCompletedTrades exceeds the supported range")?;
            validate_proposed_stop_limits(&explicit_limits, elapsed_seconds, completed_trades)?;
        }

        self.apply_invocations(&contract.skill_invocations.clone(), contract)
    }
}

pub(crate) fn validate_proposed_stop_limits(
    explicit_limits: &crate::contracts::ContractStopLimits,
    elapsed_seconds: Option<u64>,
    completed_trades: Option<u32>,
) -> Result<()> {
    if explicit_limits
        .maximum_elapsed_seconds
        .is_some_and(|limit| elapsed_seconds.is_none_or(|proposed| proposed > limit))
        || explicit_limits
            .maximum_completed_trades
            .is_some_and(|limit| completed_trades.is_none_or(|proposed| proposed > limit))
    {
        bail!("loop.lifecycle_limits may tighten explicit user stop caps but cannot remove or exceed them");
    }
    Ok(())
}

/// OpenAI strict structured outputs requires closed objects and every declared
/// property to be required. Optional handler inputs must represent absence as
/// a nullable value in their manifest schema.
fn make_strict_schema(schema: &mut Value) {
    match schema {
        Value::Object(object) => {
            if object.get("type").and_then(Value::as_str) == Some("object") {
                object.insert("additionalProperties".into(), Value::Bool(false));
                if let Some(properties) = object.get("properties").and_then(Value::as_object) {
                    let required = properties.keys().cloned().map(Value::String).collect();
                    object.insert("required".into(), Value::Array(required));
                }
            }
            for child in object.values_mut() {
                make_strict_schema(child);
            }
        }
        Value::Array(items) => {
            for child in items {
                make_strict_schema(child);
            }
        }
        _ => {}
    }
}

struct LoopLifecycleSkill;

impl WorldModelSkill for LoopLifecycleSkill {
    fn manifest(&self) -> SkillManifest {
        SkillManifest {
            id: "loop.lifecycle_limits".into(),
            name: "Loop lifecycle limits".into(),
            description: "Choose bounded deterministic stop caps for the strategy and select per-loop review thresholds for no-trade decisions, consecutive losses, and completed trades. You may propose a cap even when the user did not specify one, but must keep every cap within any explicit user-specified maximum. The harness enforces these values; you cannot stop or trade directly.".into(),
            authority: "May propose bounded loop review thresholds and stop limits within explicit user limits; Rust validates, persists, times, and applies them.".into(),
            input_schema: json!({
                "type":"object",
                "additionalProperties":false,
                "required":["maximumElapsedSeconds","maximumCompletedTrades","noTradeDecisions","consecutiveLosses","completedTrades"],
                "properties":{
                    "maximumElapsedSeconds":{"type":["integer","null"],"minimum":1,"maximum":31536000},
                    "maximumCompletedTrades":{"type":["integer","null"],"minimum":1,"maximum":10000},
                    "noTradeDecisions":{"type":"integer","minimum":1,"maximum":12},
                    "consecutiveLosses":{"type":"integer","minimum":1,"maximum":10},
                    "completedTrades":{"type":"integer","minimum":1,"maximum":100}
                }
            }),
        }
    }

    fn apply(&self, arguments: &Value, contract: &mut HypothesisContractDraft) -> Result<()> {
        let schema = self.manifest().input_schema;
        let object = arguments
            .as_object()
            .context("loop.lifecycle_limits arguments must be an object")?;
        if object.len() != 5
            || object
                .keys()
                .any(|key| schema["properties"].get(key).is_none())
        {
            bail!("loop.lifecycle_limits arguments do not match the discovered schema");
        }
        let optional_u64 = |key: &str| -> Result<Option<u64>> {
            match arguments.get(key) {
                None | Some(Value::Null) => Ok(None),
                Some(value) => value
                    .as_u64()
                    .filter(|number| *number > 0)
                    .map(Some)
                    .with_context(|| format!("{key} must be null or a positive integer")),
            }
        };
        let bounded = |key: &str, minimum: u64, maximum: u64| -> Result<u32> {
            arguments
                .get(key)
                .and_then(Value::as_u64)
                .filter(|number| (minimum..=maximum).contains(number))
                .map(|number| number as u32)
                .with_context(|| format!("{key} is outside {minimum}..={maximum}"))
        };
        let maximum_elapsed_seconds = optional_u64("maximumElapsedSeconds")?;
        if maximum_elapsed_seconds.is_some_and(|value| value > 365 * 24 * 60 * 60) {
            bail!("maximumElapsedSeconds exceeds one year");
        }
        let maximum_completed_trades = optional_u64("maximumCompletedTrades")?;
        let maximum_completed_trades = maximum_completed_trades
            .map(u32::try_from)
            .transpose()
            .context("maximumCompletedTrades exceeds the supported range")?;
        if maximum_completed_trades.is_some_and(|value| value > 10_000) {
            bail!("maximumCompletedTrades exceeds 10000");
        }
        contract.stop_limits = ContractStopLimits {
            maximum_elapsed_seconds,
            maximum_completed_trades,
        };
        contract.review_triggers = ContractReviewTriggers {
            no_trade_decisions: bounded("noTradeDecisions", 1, 12)?,
            consecutive_losses: bounded("consecutiveLosses", 1, 10)?,
            completed_trades: bounded("completedTrades", 1, 100)?,
        };
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::{
        ContextRequirement, ContractReviewTriggers, ContractStopLimits, HypothesisContractDraft,
    };
    use crate::domain::TimeframeDefinition;

    fn draft() -> HypothesisContractDraft {
        let live_context_spec = crate::market_data::default_live_context_spec("BTCUSD");
        HypothesisContractDraft {
            user_objective: "test lifecycle limits".into(),
            thesis: "test thesis".into(),
            instruments: vec!["BTCUSD".into()],
            mechanism: "test mechanism".into(),
            expected_behavior: "test behavior".into(),
            timeframe: TimeframeDefinition {
                label: "one hour".into(),
                horizon_minutes: 60,
                source: "test".into(),
                rationale: "test".into(),
            },
            expires_at: None,
            supporting_evidence_ids: Vec::new(),
            contradictory_evidence_ids: Vec::new(),
            key_assumptions: vec!["test assumption".into()],
            alternative_explanation: "test alternative".into(),
            context_requirements: vec![ContextRequirement {
                id: "test".into(),
                source: crate::contracts::ContextSource::CanonicalEvidence,
                value_type: crate::contracts::ContextValueType::Evidence,
                period: None,
                lookback: None,
                maximum_age_seconds: 60,
                required: false,
            }],
            live_context_spec,
            invalidation_conditions: vec!["test invalidation".into()],
            review_triggers: ContractReviewTriggers {
                no_trade_decisions: 0,
                consecutive_losses: 0,
                completed_trades: 0,
            },
            stop_limits: ContractStopLimits::default(),
            jev1_objective: "test Jev1".into(),
            jev2_objective: "test Jev2".into(),
            skill_invocations: Vec::new(),
        }
    }

    #[test]
    fn discovered_lifecycle_skill_is_applied_to_contract_limits() {
        let registry = SkillRegistry::discover_builtin();
        let mut contract = draft();
        let invocations = vec![SkillInvocation {
            skill_id: "loop.lifecycle_limits".into(),
            arguments: json!({
                "maximumElapsedSeconds":345600,
                "maximumCompletedTrades":3,
                "noTradeDecisions":2,
                "consecutiveLosses":3,
                "completedTrades":10
            }),
        }];
        registry
            .apply_invocations(&invocations, &mut contract)
            .unwrap();
        assert_eq!(contract.stop_limits.maximum_elapsed_seconds, Some(345_600));
        assert_eq!(contract.stop_limits.maximum_completed_trades, Some(3));
        assert_eq!(contract.review_triggers.no_trade_decisions, 2);
        assert_eq!(contract.review_triggers.consecutive_losses, 3);
        assert_eq!(contract.review_triggers.completed_trades, 10);
    }

    #[test]
    fn omitted_lifecycle_skill_is_normalized_from_user_caps() {
        let registry = SkillRegistry::discover_builtin();
        let mut contract = draft();
        contract.user_objective = "Stop after 3 trades or 4 days".into();
        registry
            .normalize_and_apply_contract(&mut contract)
            .unwrap();
        assert_eq!(contract.skill_invocations.len(), 1);
        assert_eq!(contract.stop_limits.maximum_completed_trades, Some(3));
        assert_eq!(contract.stop_limits.maximum_elapsed_seconds, Some(345_600));
        assert_eq!(contract.review_triggers.no_trade_decisions, 12);
        assert_eq!(contract.review_triggers.consecutive_losses, 3);
        assert_eq!(contract.review_triggers.completed_trades, 10);
    }

    #[test]
    fn lifecycle_skill_allows_stop_caps_absent_from_user_prompt() {
        let registry = SkillRegistry::discover_builtin();
        let mut contract = draft();
        contract.skill_invocations = vec![SkillInvocation {
            skill_id: "loop.lifecycle_limits".into(),
            arguments: json!({
                "maximumElapsedSeconds":null,
                "maximumCompletedTrades":3,
                "noTradeDecisions":12,
                "consecutiveLosses":3,
                "completedTrades":10
            }),
        }];
        registry
            .normalize_and_apply_contract(&mut contract)
            .unwrap();
        assert_eq!(contract.stop_limits.maximum_completed_trades, Some(3));
        assert_eq!(contract.stop_limits.maximum_elapsed_seconds, None);
    }

    #[test]
    fn lifecycle_skill_allows_model_caps_and_tightens_user_duration() {
        let registry = SkillRegistry::discover_builtin();
        let mut contract = draft();
        contract.user_objective = "Trade BTCUSD over an hour".into();
        contract.skill_invocations = vec![SkillInvocation {
            skill_id: "loop.lifecycle_limits".into(),
            arguments: json!({
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
        assert_eq!(contract.stop_limits.maximum_elapsed_seconds, Some(1800));
        assert_eq!(contract.stop_limits.maximum_completed_trades, Some(6));
    }

    #[test]
    fn model_capability_catalog_discovers_skills_and_scoped_tools() {
        let registry = SkillRegistry::discover_builtin();
        let catalog = registry.catalog_json(true, false);
        assert_eq!(catalog["contractSkills"][0]["id"], "loop.lifecycle_limits");
        assert_eq!(catalog["readOnlyCapabilities"][0]["available"], true);
        assert_eq!(catalog["readOnlyCapabilities"][1]["available"], false);
        assert_eq!(
            catalog["readOnlyCapabilities"][1]["requestChannel"],
            "brokerContextRequests"
        );
    }

    #[test]
    fn invocation_schema_binds_discovered_ids_to_closed_typed_arguments() {
        let registry = SkillRegistry::discover_builtin();
        let schema = registry.invocation_schema();
        let lifecycle = &schema["anyOf"][0];
        assert_eq!(
            lifecycle["properties"]["skillId"]["enum"][0],
            "loop.lifecycle_limits"
        );
        assert_eq!(
            lifecycle["properties"]["arguments"]["additionalProperties"],
            false
        );
        assert_eq!(
            lifecycle["properties"]["arguments"]["required"]
                .as_array()
                .unwrap()
                .len(),
            5
        );
        assert!(
            lifecycle["properties"]["arguments"]["properties"]["maximumElapsedSeconds"].is_object()
        );
    }
}
