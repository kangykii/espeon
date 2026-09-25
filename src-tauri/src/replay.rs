use crate::domain::*;
use crate::storage::CanonicalStore;
use anyhow::{bail, Context, Result};
use std::collections::HashSet;

pub fn replay_run(store: &CanonicalStore, run_id: &str) -> Result<ReplayState> {
    let events = store.stored_events(run_id)?;
    if events.is_empty() {
        bail!("run has no canonical events");
    }

    let mut state = ReplayState::empty(run_id);
    for stored in &events {
        if stored.event.run_id != run_id {
            bail!("event {} belongs to a different run", stored.event.id);
        }
        state.last_sequence = stored.sequence;
        match stored.event.kind.as_str() {
            "run_started" => {
                state.status = "active".into();
                state.human_thesis = stored
                    .event
                    .payload
                    .get("humanThesis")
                    .and_then(|value| value.as_str())
                    .context("run_started missing humanThesis")?
                    .to_owned();
            }
            "thesis_version_created" => state
                .thesis_versions
                .push(record(&stored.event.payload, "thesis version")?),
            "context_definition_created" => state
                .context_definitions
                .push(record(&stored.event.payload, "context definition")?),
            "context_version_created" => state
                .context_versions
                .push(record(&stored.event.payload, "context version")?),
            "hypothesis_version_created"
            | "hypothesis_modified"
            | "hypothesis_split"
            | "hypothesis_stopped" => state
                .hypotheses
                .push(record(&stored.event.payload, "hypothesis definition")?),
            "loop_spawned" => state.loops.push(record(&stored.event.payload, "loop")?),
            "loop_cadence_mapped" => state
                .cadences
                .push(record(&stored.event.payload, "loop cadence")?),
            "hypothesis_reviewed" => state
                .hypothesis_reviews
                .push(record(&stored.event.payload, "hypothesis review")?),
            "world_model_review_package_assembled" | "autonomous_review_package_assembled" => state
                .review_packages
                .push(record(&stored.event.payload, "world model review package")?),
            "capital_allocation_changed" => {
                let allocation: CapitalAllocationRecord =
                    record(&stored.event.payload, "capital allocation")?;
                for entry in &allocation.allocations {
                    let loop_state = state
                        .loops
                        .iter_mut()
                        .find(|loop_state| loop_state.id == entry.loop_id)
                        .with_context(|| {
                            format!("allocation references unknown loop {}", entry.loop_id)
                        })?;
                    loop_state.allocated_fraction = entry.fraction;
                }
                state.capital_allocations.push(allocation);
            }
            "jev1_decision_recorded"
            | "jev2_decision_recorded"
            | "deterministic_stop_decision_recorded"
            | "broker_position_import_decision_recorded" => state
                .decisions
                .push(record(&stored.event.payload, "decision")?),
            "live_context_resolved" => state
                .resolved_context_snapshots
                .push(record(&stored.event.payload, "resolved context snapshot")?),
            "execution_recorded" | "broker_position_imported" => state
                .executions
                .push(record(&stored.event.payload, "execution")?),
            "order_evaluated" | "broker_position_import_order_recorded" => {
                state.orders.push(record(&stored.event.payload, "order")?)
            }
            "position_control_created" => state
                .position_controls
                .push(record(&stored.event.payload, "position control")?),
            "position_partially_closed" | "position_quantity_reconciled" => {
                let position: PositionRecord = record(&stored.event.payload, "position")?;
                let control: PositionControlRecord = serde_json::from_value(
                    stored
                        .event
                        .payload
                        .get("control")
                        .context("partial close missing position control")?
                        .clone(),
                )?;
                let existing_position = state
                    .positions
                    .iter_mut()
                    .find(|item| item.id == position.id)
                    .context("partial close references unknown position")?;
                *existing_position = position;
                let existing_control = state
                    .position_controls
                    .iter_mut()
                    .find(|item| item.position_id == control.position_id)
                    .context("partial close references unknown position control")?;
                *existing_control = control;
            }
            "loop_failure_state_changed" => {
                let failure: LoopFailureState = record(&stored.event.payload, "failure state")?;
                state
                    .failure_states
                    .retain(|item| item.loop_id != failure.loop_id);
                state.failure_states.push(failure);
            }
            "position_opened" | "position_reconciled_open" => state
                .positions
                .push(record(&stored.event.payload, "position")?),
            "position_closed" | "position_reconciled_closed" => {
                let position: PositionRecord = record(&stored.event.payload, "closed position")?;
                let existing = state
                    .positions
                    .iter_mut()
                    .find(|item| item.id == position.id)
                    .context("position_closed references unknown position")?;
                *existing = position;
            }
            "memory_indexed" => state
                .memory_documents
                .push(record(&stored.event.payload, "memory document")?),
            "context_record_ingested" => state
                .context_pool_records
                .push(record(&stored.event.payload, "context pool record")?),
            "world_model_reviewed" | "startup_memory_review_recorded" => state
                .reviews
                .push(record(&stored.event.payload, "world-model review")?),
            "trade_outcome_recorded" => state
                .trade_outcomes
                .push(record(&stored.event.payload, "trade outcome")?),
            "autonomous_review_triggered"
            | "autonomous_review_started"
            | "autonomous_review_failed"
            | "autonomous_review_completed"
            | "autonomous_review_cancelled"
            | "autonomous_review_action_rejected" => state
                .autonomous_review_triggers
                .push(record(&stored.event.payload, "autonomous review trigger")?),
            "loop_stopped" => {
                let loop_state: LoopView = record(&stored.event.payload, "stopped loop")?;
                let existing = state
                    .loops
                    .iter_mut()
                    .find(|existing| existing.id == loop_state.id)
                    .context("loop_stopped references unknown loop")?;
                *existing = loop_state;
            }
            "loop_state_transitioned" => {
                let loop_state: LoopView = record(&stored.event.payload, "transitioned loop")?;
                let existing = state
                    .loops
                    .iter_mut()
                    .find(|item| item.id == loop_state.id)
                    .context("loop_state_transitioned references unknown loop")?;
                *existing = loop_state;
            }
            "run_stopped" => state.status = "stopped".into(),
            "guardrail_evaluated"
            | "jev_inference_failed"
            | "live_context_resolution_failed"
            | "broker_state_synchronized"
            | "broker_sync_failed"
            | "broker_sync_degraded" => {}
            other => bail!("unsupported canonical event kind during replay: {other}"),
        }
    }
    validate(&state, &events)?;
    Ok(state)
}

fn record<T: serde::de::DeserializeOwned>(
    payload: &serde_json::Value,
    description: &str,
) -> Result<T> {
    serde_json::from_value(
        payload
            .get("record")
            .with_context(|| format!("event missing {description} record"))?
            .clone(),
    )
    .with_context(|| format!("decode {description} record"))
}

fn validate(state: &ReplayState, events: &[StoredEvent]) -> Result<()> {
    let event_ids: HashSet<&str> = events
        .iter()
        .map(|stored| stored.event.id.as_str())
        .collect();
    let thesis_ids: HashSet<&str> = state
        .thesis_versions
        .iter()
        .map(|record| record.id.as_str())
        .collect();
    let context_ids: HashSet<&str> = state
        .context_versions
        .iter()
        .map(|record| record.id.as_str())
        .collect();
    let loop_ids: HashSet<&str> = state
        .loops
        .iter()
        .map(|record| record.id.as_str())
        .collect();
    let hypothesis_ids: HashSet<&str> = state
        .hypotheses
        .iter()
        .map(|record| record.id.as_str())
        .collect();
    let decision_ids: HashSet<&str> = state
        .decisions
        .iter()
        .map(|record| record.id.as_str())
        .collect();
    let resolved_snapshot_ids: HashSet<&str> = state
        .resolved_context_snapshots
        .iter()
        .map(|record| record.id.as_str())
        .collect();
    let execution_ids: HashSet<&str> = state
        .executions
        .iter()
        .map(|record| record.execution_id.as_str())
        .collect();
    let order_ids: HashSet<&str> = state
        .orders
        .iter()
        .map(|record| record.id.as_str())
        .collect();
    let position_ids: HashSet<&str> = state
        .positions
        .iter()
        .map(|record| record.id.as_str())
        .collect();
    let review_ids: HashSet<&str> = state
        .reviews
        .iter()
        .map(|record| record.id.as_str())
        .collect();

    for loop_state in &state.loops {
        require(
            &hypothesis_ids,
            &loop_state.hypothesis_id,
            "loop hypothesis",
        )?;
        require(
            &thesis_ids,
            &loop_state.thesis_version_id,
            "loop thesis version",
        )?;
        require(
            &context_ids,
            &loop_state.context_version_id,
            "loop context version",
        )?;
        require(
            &event_ids,
            &loop_state.created_by_event_id,
            "loop creation event",
        )?;
        if let Some(parent_loop_id) = &loop_state.parent_loop_id {
            require(&loop_ids, parent_loop_id, "parent loop")?;
            let parent_thesis = loop_state
                .parent_thesis_version_id
                .as_ref()
                .context("spawned loop is missing parent thesis version")?;
            require(&thesis_ids, parent_thesis, "parent thesis version")?;
        }
    }
    for hypothesis in &state.hypotheses {
        require(
            &thesis_ids,
            &hypothesis.thesis_version_id,
            "hypothesis thesis version",
        )?;
        require(
            &context_ids,
            &hypothesis.context_version_id,
            "hypothesis context version",
        )?;
        require(
            &event_ids,
            &hypothesis.created_by_event_id,
            "hypothesis creation event",
        )?;
        if hypothesis.instruments.is_empty()
            || hypothesis.strategy_mechanism.is_empty()
            || hypothesis.jev_question.is_empty()
            || hypothesis.timeframe.horizon_minutes == 0
        {
            bail!("hypothesis {} is not executable", hypothesis.id);
        }
    }
    for cadence in &state.cadences {
        require(&loop_ids, &cadence.loop_id, "cadence loop")?;
        require(
            &hypothesis_ids,
            &cadence.hypothesis_id,
            "cadence hypothesis",
        )?;
        require(
            &event_ids,
            &cadence.created_by_event_id,
            "cadence creation event",
        )?;
    }
    for review in &state.hypothesis_reviews {
        require(
            &hypothesis_ids,
            &review.hypothesis_id,
            "hypothesis review target",
        )?;
        require(
            &event_ids,
            &review.created_by_event_id,
            "hypothesis review event",
        )?;
        if let Some(candidate) = &review.candidate_hypothesis {
            require(
                &hypothesis_ids,
                &candidate.competing_with_hypothesis_id,
                "candidate competing hypothesis",
            )?;
        }
    }
    for package in &state.review_packages {
        require(
            &hypothesis_ids,
            &package.current_hypothesis.id,
            "review package hypothesis",
        )?;
        require(
            &thesis_ids,
            &package.current_thesis.id,
            "review package thesis",
        )?;
        require(
            &context_ids,
            &package.current_context.id,
            "review package context",
        )?;
        require(&loop_ids, &package.current_loop.id, "review package loop")?;
        require(
            &event_ids,
            &package.created_by_event_id,
            "review package event",
        )?;
        for decision in &package.recent_jev_decisions {
            require(&decision_ids, &decision.id, "review package decision")?;
        }
        for execution in &package.recent_executions {
            require(
                &execution_ids,
                &execution.execution_id,
                "review package execution",
            )?;
        }
    }
    for decision in &state.decisions {
        require(&loop_ids, &decision.loop_id, "decision loop")?;
        require(
            &thesis_ids,
            &decision.thesis_version_id,
            "decision thesis version",
        )?;
        require(
            &context_ids,
            &decision.context_version_id,
            "decision context version",
        )?;
        require(
            &event_ids,
            &decision.created_by_event_id,
            "decision creation event",
        )?;
        if let Some(position_id) = &decision.position_id {
            require(&position_ids, position_id, "decision position")?;
        }
        if let Some(snapshot) = &decision.resolved_state.live_context_snapshot {
            require(
                &resolved_snapshot_ids,
                &snapshot.id,
                "decision resolved context snapshot",
            )?;
            if snapshot.thesis_version_id != decision.thesis_version_id
                || snapshot.context_version_id != decision.context_version_id
                || snapshot.loop_id != decision.loop_id
            {
                bail!(
                    "decision {} references a mismatched live context snapshot",
                    decision.id
                );
            }
        }
    }
    for snapshot in &state.resolved_context_snapshots {
        require(&loop_ids, &snapshot.loop_id, "resolved context loop")?;
        require(
            &thesis_ids,
            &snapshot.thesis_version_id,
            "resolved context thesis",
        )?;
        require(
            &context_ids,
            &snapshot.context_version_id,
            "resolved context version",
        )?;
        require(
            &event_ids,
            &snapshot.created_by_event_id,
            "resolved context event",
        )?;
        if snapshot.freshness_state != "fresh"
            || snapshot.quote.id.is_empty()
            || snapshot.fields.is_empty()
            || snapshot.candles.iter().any(|candle| !candle.closed)
        {
            bail!(
                "resolved context snapshot {} is incomplete or stale",
                snapshot.id
            );
        }
    }
    for execution in &state.executions {
        require(
            &decision_ids,
            &execution.caused_by_decision_id,
            "execution causal decision",
        )?;
        require(
            &event_ids,
            &execution.created_by_event_id,
            "execution creation event",
        )?;
    }
    for order in &state.orders {
        require(&decision_ids, &order.decision_id, "order decision")?;
        require(&loop_ids, &order.loop_id, "order loop")?;
        require(&event_ids, &order.created_by_event_id, "order event")?;
        if order.status == "approved"
            && (order.quantity <= 0.0 || order.notional <= 0.0 || order.reference_price <= 0.0)
        {
            bail!(
                "approved order {} has invalid deterministic sizing",
                order.id
            );
        }
    }
    for control in &state.position_controls {
        require(
            &position_ids,
            &control.position_id,
            "position control position",
        )?;
        require(&order_ids, &control.order_id, "position control order")?;
        require(
            &event_ids,
            &control.created_by_event_id,
            "position control event",
        )?;
    }
    for position in &state.positions {
        require(
            &execution_ids,
            &position.opened_by_execution_id,
            "position opening execution",
        )?;
        require(&event_ids, &position.last_event_id, "position event")?;
        if position.state == "closed" || position.state == "closed-on-stop" {
            let closing_execution = position
                .closed_by_execution_id
                .as_ref()
                .context("closed position is missing closing execution")?;
            require(
                &execution_ids,
                closing_execution,
                "position closing execution",
            )?;
        }
    }
    for outcome in &state.trade_outcomes {
        require(&loop_ids, &outcome.loop_id, "trade outcome loop")?;
        require(
            &position_ids,
            &outcome.position_id,
            "trade outcome position",
        )?;
        require(
            &thesis_ids,
            &outcome.thesis_version_id,
            "trade outcome thesis version",
        )?;
        require(
            &context_ids,
            &outcome.context_version_id,
            "trade outcome context version",
        )?;
        require(
            &event_ids,
            &outcome.created_by_event_id,
            "trade outcome event",
        )?;
        for execution_id in outcome
            .entry_execution_ids
            .iter()
            .chain(&outcome.exit_execution_ids)
        {
            require(&execution_ids, execution_id, "trade outcome execution")?;
        }
    }
    for trigger in &state.autonomous_review_triggers {
        require(
            &loop_ids,
            &trigger.loop_id,
            "autonomous review trigger loop",
        )?;
        require(
            &hypothesis_ids,
            &trigger.hypothesis_id,
            "autonomous review trigger hypothesis",
        )?;
        require(
            &event_ids,
            &trigger.created_by_event_id,
            "autonomous review trigger event",
        )?;
    }
    for memory in &state.memory_documents {
        require(
            &event_ids,
            &memory.canonical_event_id,
            "memory canonical event",
        )?;
        require(
            &event_ids,
            &memory.indexed_by_event_id,
            "memory indexing event",
        )?;
        let exists = match memory.canonical_entity_type.as_str() {
            "decision" => decision_ids.contains(memory.canonical_entity_id.as_str()),
            "execution" => execution_ids.contains(memory.canonical_entity_id.as_str()),
            "position" => position_ids.contains(memory.canonical_entity_id.as_str()),
            "review" => review_ids.contains(memory.canonical_entity_id.as_str()),
            "thesis_version" => thesis_ids.contains(memory.canonical_entity_id.as_str()),
            "context_version" => context_ids.contains(memory.canonical_entity_id.as_str()),
            _ => false,
        };
        if !exists {
            bail!(
                "memory {} references missing canonical {} {}",
                memory.id,
                memory.canonical_entity_type,
                memory.canonical_entity_id
            );
        }
    }
    for context_record in &state.context_pool_records {
        require(
            &event_ids,
            &context_record.canonical_event_id,
            "context record canonical event",
        )?;
        if context_record.provenance_uri.is_empty()
            || context_record.publisher.is_empty()
            || context_record.content_sha256.is_empty()
        {
            bail!(
                "context record {} is missing required provenance metadata",
                context_record.id
            );
        }
    }
    Ok(())
}

fn require<'a>(ids: &HashSet<&'a str>, id: &str, description: &str) -> Result<()> {
    if !ids.contains(id) {
        bail!("{description} {id} is missing from replay");
    }
    Ok(())
}
