use crate::domain::*;
use crate::ports::ContextResolver;
use anyhow::{bail, Result};
use chrono::Utc;
use serde_json::json;
use std::collections::HashSet;

const MAX_JEV_STATE_JSON_BYTES: usize = 7_500;
const MAX_JEV_CONTEXT_FIELDS: usize = 8;
const MAX_JEV_CONTEXT_TEXT_CHARS: usize = 3_000;
const MAX_JEV_FIELD_TEXT_CHARS: usize = 700;

pub struct StructuredContextResolver;

impl ContextResolver for StructuredContextResolver {
    fn resolve(
        &self,
        hypothesis: &HypothesisDefinition,
        thesis: &ThesisVersion,
        context: &ContextVersion,
        position: Option<&PositionRecord>,
    ) -> Result<ResolvedJevState> {
        if context.items.is_empty() {
            bail!("context resolution failed: context version has no deterministic fields");
        }
        let mut fields = Vec::with_capacity(context.items.len());
        for item in &context.items {
            if item.source.trim().is_empty()
                || item.source_id.trim().is_empty()
                || item.content.trim().is_empty()
            {
                bail!("context resolution failed: source, source ID and value are required");
            }
            fields.push(ResolvedContextField {
                name: item.source_id.clone(),
                value: json!(item.content),
                source_id: item.source_id.clone(),
                source_uri: item.source.clone(),
                observed_at: item.observed_at,
            });
        }
        let position = position.map(|position| {
            json!({
                "positionId": position.id,
                "direction": position.direction,
                "state": position.state,
                "openedAt": position.opened_at,
                "openedByExecutionId": position.opened_by_execution_id,
            })
        });
        Ok(ResolvedJevState {
            thesis: truncate_chars(&thesis.thesis, 1_400),
            thesis_version_id: thesis.id.clone(),
            context_version_id: context.id.clone(),
            instruments: hypothesis.instruments.clone(),
            timeframe: hypothesis.timeframe.clone(),
            market_state: json!({
                "strategyMechanism": truncate_chars(&hypothesis.strategy_mechanism, 1_200),
                "canonicalContextFieldCount": context.items.len(),
            }),
            context: fields,
            position,
            live_context_snapshot: None,
            resolved_at: Utc::now(),
        })
    }
}

pub(crate) fn compact_for_jev(state: &mut ResolvedJevState) {
    state.thesis = truncate_chars(&state.thesis, 1_400);
    if let Some(mechanism) = state
        .market_state
        .get_mut("strategyMechanism")
        .and_then(|value| value.as_str())
        .map(|value| truncate_chars(value, 1_200))
    {
        state.market_state["strategyMechanism"] = json!(mechanism);
    }
    let available = state.context.len();
    let mut remaining_chars = MAX_JEV_CONTEXT_TEXT_CHARS;
    let mut seen = HashSet::new();
    let mut compacted = Vec::new();

    for (index, mut field) in state.context.drain(..).enumerate() {
        if compacted.len() >= MAX_JEV_CONTEXT_FIELDS || remaining_chars == 0 {
            break;
        }
        let raw = field
            .value
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| field.value.to_string());
        let normalized = raw.split_whitespace().collect::<Vec<_>>().join(" ");
        if normalized.is_empty() || !seen.insert(normalized.clone()) {
            continue;
        }
        let limit = remaining_chars.min(MAX_JEV_FIELD_TEXT_CHARS);
        let value = truncate_chars(&normalized, limit);
        remaining_chars = remaining_chars.saturating_sub(value.chars().count());
        if field.name.chars().count() > 80 || field.name == field.source_id {
            field.name = format!("context_{:02}", index + 1);
        } else {
            field.name = truncate_chars(&field.name, 80);
        }
        field.source_id = truncate_chars(&field.source_id, 180);
        field.source_uri = truncate_chars(&field.source_uri, 320);
        field.value = json!(value);
        compacted.push(field);
    }
    state.context = compacted;
    annotate_selection(state, available);

    while serde_json::to_vec(state)
        .map(|bytes| bytes.len() > MAX_JEV_STATE_JSON_BYTES)
        .unwrap_or(false)
        && state.context.len() > 1
    {
        state.context.pop();
        annotate_selection(state, available);
    }
}

fn annotate_selection(state: &mut ResolvedJevState, available: usize) {
    if let Some(object) = state.market_state.as_object_mut() {
        object.insert(
            "jevContextSelection".into(),
            json!({
                "includedFields": state.context.len(),
                "availableFields": available,
                "compacted": state.context.len() < available,
                "canonicalContextVersionId": state.context_version_id,
            }),
        );
    }
}

fn truncate_chars(value: &str, limit: usize) -> String {
    let mut chars = value.chars();
    let prefix: String = chars.by_ref().take(limit).collect();
    if chars.next().is_some() {
        format!("{prefix}…")
    } else {
        prefix
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::TimeframeDefinition;

    #[test]
    fn final_jev_state_is_deduplicated_and_bounded_after_retrieval() {
        let now = Utc::now();
        let mut state = ResolvedJevState {
            thesis: "thesis ".repeat(1_000),
            thesis_version_id: "thesis-1".into(),
            context_version_id: "context-1".into(),
            instruments: vec!["BTCUSD".into()],
            timeframe: TimeframeDefinition {
                label: "60 minutes".into(),
                horizon_minutes: 60,
                source: "world-model-selected".into(),
                rationale: "test".into(),
            },
            context: (0..20)
                .map(|index| ResolvedContextField {
                    name: "an excessively long generated context field name ".repeat(4),
                    value: json!(format!("evidence-{index} {}", "market detail ".repeat(500))),
                    source_id: format!("canonical-{index}"),
                    source_uri: "context://test".into(),
                    observed_at: now,
                })
                .collect(),
            market_state: json!({"strategyMechanism":"breakout"}),
            position: None,
            live_context_snapshot: None,
            resolved_at: now,
        };

        compact_for_jev(&mut state);

        assert!(serde_json::to_vec(&state).unwrap().len() <= MAX_JEV_STATE_JSON_BYTES);
        assert!(state.context.len() <= MAX_JEV_CONTEXT_FIELDS);
        assert_eq!(state.context_version_id, "context-1");
        assert_eq!(
            state.market_state["jevContextSelection"]["availableFields"],
            20
        );
        assert_eq!(state.market_state["jevContextSelection"]["compacted"], true);
    }
}
