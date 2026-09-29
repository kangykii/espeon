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
    let mut seen_run_started = false;
    for stored in &events {
        if stored.event.run_id != run_id {
            bail!("event {} belongs to a different run", stored.event.id);
        }
        if !seen_run_started && stored.event.kind != "run_started" {
            bail!("run_started must be the first canonical run event");
        }
        state.last_sequence = stored.sequence;
        match stored.event.kind.as_str() {
            "run_started" => {
                if seen_run_started {
                    bail!("run contains more than one canonical run_started event");
                }
                if stored.event.aggregate_type != "run" || stored.event.aggregate_id != run_id {
                    bail!("run_started event has an invalid run aggregate identity");
                }
                seen_run_started = true;
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
            "hypothesis_contract_activated" => {
                let hypothesis: HypothesisDefinition =
                    record(&stored.event.payload, "activated hypothesis contract")?;
                if hypothesis.status != "ACTIVE"
                    || hypothesis.contract.as_ref().map(|contract| contract.state)
                        != Some(crate::contracts::ContractState::Active)
                {
                    bail!(
                        "hypothesis contract activation event did not contain an ACTIVE contract"
                    );
                }
                let existing = state
                    .hypotheses
                    .iter_mut()
                    .find(|item| item.id == hypothesis.id)
                    .context("contract activation references unknown DRAFT hypothesis")?;
                *existing = hypothesis;
            }
            "hypothesis_contract_rejected" => {
                let hypothesis: HypothesisDefinition =
                    record(&stored.event.payload, "rejected hypothesis contract")?;
                if hypothesis.status != "REJECTED"
                    || hypothesis.contract.as_ref().map(|contract| contract.state)
                        != Some(crate::contracts::ContractState::Rejected)
                {
                    bail!(
                        "hypothesis contract rejection event did not contain a REJECTED contract"
                    );
                }
                let existing = state
                    .hypotheses
                    .iter_mut()
                    .find(|item| item.id == hypothesis.id)
                    .context("contract rejection references unknown DRAFT hypothesis")?;
                *existing = hypothesis;
            }
            "loop_spawned" => {
                let loop_state: LoopView = record(&stored.event.payload, "loop")?;
                if state
                    .hypotheses
                    .iter()
                    .find(|hypothesis| hypothesis.id == loop_state.hypothesis_id)
                    .and_then(|hypothesis| hypothesis.contract.as_ref())
                    .is_some_and(|contract| {
                        contract.state != crate::contracts::ContractState::Active
                    })
                {
                    bail!("loop_spawned references a non-ACTIVE hypothesis contract");
                }
                state.loops.push(loop_state);
            }
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
            "context_record_ingested" | "web_evidence_ingested" => state
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
                let existing_index = state
                    .loops
                    .iter()
                    .position(|existing| {
                        stored
                            .event
                            .payload
                            .get("record")
                            .and_then(|record| record.get("id"))
                            .and_then(serde_json::Value::as_str)
                            == Some(existing.id.as_str())
                    })
                    .context("loop_stopped references unknown loop")?;
                let loop_state = loop_transition_record(
                    &stored.event.payload,
                    &state.loops[existing_index],
                    "stopped loop",
                )?;
                state.loops[existing_index] = loop_state;
            }
            "loop_state_transitioned" => {
                let existing_index = state
                    .loops
                    .iter()
                    .position(|item| {
                        stored
                            .event
                            .payload
                            .get("record")
                            .and_then(|record| record.get("id"))
                            .and_then(serde_json::Value::as_str)
                            == Some(item.id.as_str())
                    })
                    .context("loop_state_transitioned references unknown loop")?;
                let loop_state = loop_transition_record(
                    &stored.event.payload,
                    &state.loops[existing_index],
                    "transitioned loop",
                )?;
                state.loops[existing_index] = loop_state;
            }
            "run_stopped" => state.status = "stopped".into(),
            // Continuation metadata links this run to its source run; it
            // does not alter the replayed state of the new run.
            "run_continued_from" => {},
            "guardrail_evaluated"
            | "jev_inference_failed"
            | "live_context_resolution_failed"
            | "market_service_state_changed"
            | "contract_stop_limit_triggered"
            | "broker_state_synchronized"
            | "broker_sync_failed"
            | "broker_sync_degraded"
            | "broker_context_unavailable"
            | "external_research_unavailable"
            | "human_live_risk_verified"
            | "harness_worker_failed"
            | "run_recovery_blocked"
            | "review_recovery_aborted"
            // Stop wrap-ups are archival-only retrospectives recorded after
            // the run is already stopped. They do not mutate workspace state,
            // regardless of whether the model review succeeded or failed.
            | "world_model_stop_wrapup_recorded"
            | "world_model_stop_wrapup_failed" => {}
            other => bail!("unsupported canonical event kind during replay: {other}"),
        }
    }
    if !seen_run_started {
        bail!("run has no canonical run_started event");
    }
    let authoritative_thesis = store.run_human_thesis(run_id)?;
    if state.human_thesis != authoritative_thesis {
        bail!("run_started prompt differs from the authoritative canonical_runs prompt");
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

fn loop_transition_record(
    payload: &serde_json::Value,
    existing: &LoopView,
    description: &str,
) -> Result<LoopView> {
    let mut value = payload
        .get("record")
        .with_context(|| format!("event missing {description} record"))?
        .clone();
    let fields = value
        .as_object_mut()
        .with_context(|| format!("{description} record is not an object"))?;
    let previous = serde_json::to_value(existing)?;
    let previous_fields = previous
        .as_object()
        .context("previous loop state did not serialize as an object")?;
    // Older stop/transition records omitted fields added to LoopView later.
    // Keep the last complete loop projection and apply the fields this event
    // actually carried, so historical activity remains replayable.
    for (key, fallback) in previous_fields {
        fields
            .entry(key.clone())
            .or_insert_with(|| fallback.clone());
    }
    serde_json::from_value(value).with_context(|| format!("decode {description} record"))
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

#[cfg(test)]
mod legacy_contract_replay_tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;

    fn event(
        id: &str,
        run_id: &str,
        kind: &str,
        aggregate_type: &str,
        aggregate_id: &str,
        payload: serde_json::Value,
    ) -> HarnessEvent {
        HarnessEvent {
            id: id.into(),
            run_id: run_id.into(),
            loop_id: None,
            kind: kind.into(),
            aggregate_type: aggregate_type.into(),
            aggregate_id: aggregate_id.into(),
            causation_event_id: None,
            occurred_at: Utc::now(),
            payload,
        }
    }

    #[test]
    fn legacy_active_hypothesis_without_contract_still_replays_for_history() {
        let runtime = tempfile::tempdir().expect("temporary legacy replay database");
        let store = CanonicalStore::open(runtime.path()).expect("open legacy replay database");
        let run_id = "legacy-run";
        let run_event = event(
            "legacy-run-start",
            run_id,
            "run_started",
            "run",
            run_id,
            json!({"humanThesis":"Historical BTC experiment"}),
        );
        store
            .create_run(run_id, "Historical BTC experiment", &run_event)
            .unwrap();

        let now = Utc::now();
        let thesis = ThesisVersion {
            id: "legacy-thesis".into(),
            run_id: run_id.into(),
            version: 1,
            thesis: "Historical thesis".into(),
            provenance: "legacy".into(),
            created_by_event_id: "legacy-thesis-event".into(),
            created_at: now,
        };
        store
            .append_event(&event(
                "legacy-thesis-event",
                run_id,
                "thesis_version_created",
                "thesis_version",
                &thesis.id,
                json!({"record":thesis}),
            ))
            .unwrap();
        let definition = ContextDefinition {
            id: "legacy-context-definition".into(),
            run_id: run_id.into(),
            name: "legacy context".into(),
            description: "historical context".into(),
            created_by_event_id: "legacy-context-definition-event".into(),
            created_at: now,
        };
        store
            .append_event(&event(
                "legacy-context-definition-event",
                run_id,
                "context_definition_created",
                "context_definition",
                &definition.id,
                json!({"record":definition}),
            ))
            .unwrap();
        let context = ContextVersion {
            id: "legacy-context".into(),
            definition_id: definition.id.clone(),
            run_id: run_id.into(),
            version: 1,
            items: Vec::new(),
            created_by_event_id: "legacy-context-event".into(),
            created_at: now,
        };
        store
            .append_event(&event(
                "legacy-context-event",
                run_id,
                "context_version_created",
                "context_version",
                &context.id,
                json!({"record":context}),
            ))
            .unwrap();
        let hypothesis = HypothesisDefinition {
            id: "legacy-hypothesis".into(),
            root_hypothesis_id: "legacy-hypothesis".into(),
            run_id: run_id.into(),
            version: 1,
            parent_hypothesis_id: None,
            original_prompt: "Historical BTC experiment".into(),
            instruments: vec!["BTCUSD".into()],
            strategy_mechanism: "momentum".into(),
            timeframe: TimeframeDefinition {
                label: "one hour".into(),
                horizon_minutes: 60,
                source: "legacy".into(),
                rationale: "historical".into(),
            },
            deterministic_context: Vec::new(),
            live_context_spec: None,
            jev_question: "Historical question".into(),
            review_rules: HypothesisReviewRules {
                support_evidence: Vec::new(),
                weaken_evidence: Vec::new(),
                invalidate_evidence: Vec::new(),
                modify_when: Vec::new(),
                split_when: Vec::new(),
                stop_when: Vec::new(),
            },
            thesis_version_id: thesis.id.clone(),
            context_version_id: context.id.clone(),
            status: "active".into(),
            contract: None,
            created_by_event_id: "legacy-hypothesis-event".into(),
            created_at: now,
        };
        store
            .append_event(&event(
                "legacy-hypothesis-event",
                run_id,
                "hypothesis_version_created",
                "hypothesis",
                &hypothesis.id,
                json!({"record":hypothesis}),
            ))
            .unwrap();
        let loop_state = LoopView {
            id: "legacy-loop".into(),
            run_id: run_id.into(),
            parent_loop_id: None,
            parent_thesis_version_id: None,
            hypothesis_id: "legacy-hypothesis".into(),
            thesis_version_id: thesis.id,
            context_version_id: context.id,
            thesis_version: 1,
            context_version: 1,
            state: "stopped".into(),
            allocated_fraction: 1.0,
            created_by_event_id: "legacy-loop-event".into(),
            created_at: now,
            stopped_at: Some(now),
        };
        store
            .append_event(&event(
                "legacy-loop-event",
                run_id,
                "loop_spawned",
                "loop",
                &loop_state.id,
                json!({"record":loop_state}),
            ))
            .unwrap();

        let replayed =
            replay_run(&store, run_id).expect("legacy canonical history remains replayable");
        assert_eq!(replayed.hypotheses.len(), 1);
        assert!(replayed.hypotheses[0].contract.is_none());
        assert_eq!(replayed.loops.len(), 1);
    }
}

fn require<'a>(ids: &HashSet<&'a str>, id: &str, description: &str) -> Result<()> {
    if !ids.contains(id) {
        bail!("{description} {id} is missing from replay");
    }
    Ok(())
}
