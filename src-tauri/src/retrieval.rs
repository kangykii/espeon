use crate::domain::*;
use crate::ports::ContextRetriever;
use crate::replay;
use crate::storage::CanonicalStore;
use anyhow::{bail, Context, Result};
use chrono::Utc;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

#[derive(Clone)]
pub struct RetrievalConfig {
    pub python_path: PathBuf,
    pub bridge_path: PathBuf,
    pub storage_path: PathBuf,
    pub models_path: PathBuf,
    pub collection: String,
}

impl RetrievalConfig {
    pub fn load(project_root: &Path) -> Result<Self> {
        let _ = dotenvy::from_path(project_root.join(".env"));
        let runtime_root = std::env::var("JEV_RUNTIME_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| project_root.join("runtime-harness"));
        let storage_path = std::env::var("QDRANT_LOCAL_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| runtime_root.join("qdrant"));
        let configured_collection = std::env::var("QDRANT_COLLECTION")
            .unwrap_or_else(|_| "jev_context_pool".into());
        let collection = if configured_collection.ends_with("_evidence_v2") {
            configured_collection
        } else {
            format!("{configured_collection}_evidence_v2")
        };
        Ok(Self {
            python_path: project_root
                .join(".venv")
                .join("Scripts")
                .join("python.exe"),
            bridge_path: project_root
                .join("retrieval")
                .join("qdrant_local_bridge.py"),
            storage_path,
            models_path: runtime_root.join("models"),
            collection,
        })
    }
}

#[derive(Clone)]
pub struct QdrantContextPool {
    config: RetrievalConfig,
    bridge_lock: Arc<Mutex<()>>,
}

#[derive(Debug, Deserialize)]
struct SearchResponse {
    hits: Vec<RetrievalHit>,
}

#[derive(Debug, Deserialize)]
struct InspectResponse {
    records: Vec<Value>,
}

#[derive(Serialize)]
struct BridgeSearchRequest<'a> {
    action: &'static str,
    query: &'a str,
    mode: &'static str,
    limit: usize,
    filters: &'a RetrievalFilters,
    canonical_entity_id: Option<&'a str>,
}

impl QdrantContextPool {
    pub fn new(config: RetrievalConfig) -> Result<Self> {
        if !config.python_path.is_file() {
            bail!(
                "local retrieval Python is missing at {}",
                config.python_path.display()
            );
        }
        let pool = Self {
            config,
            bridge_lock: Arc::new(Mutex::new(())),
        };
        let response = pool.execute(json!({ "action": "init" }))?;
        if response.get("ready").and_then(Value::as_bool) != Some(true) {
            bail!("Qdrant local bridge did not report ready");
        }
        Ok(pool)
    }

    pub fn create_record(
        run_id: &str,
        title: &str,
        text: &str,
        source_class: ContextSourceClass,
        trust_level: TrustLevel,
        provenance_uri: &str,
        publisher: &str,
        canonical_entity_type: &str,
        canonical_entity_id: &str,
        canonical_event_id: &str,
        tags: Vec<String>,
        metadata: Value,
    ) -> ContextPoolRecord {
        let mut hasher = Sha256::new();
        hasher.update(text.as_bytes());
        ContextPoolRecord {
            id: Uuid::new_v4().to_string(),
            run_id: run_id.to_owned(),
            title: title.to_owned(),
            text: text.to_owned(),
            source_class,
            trust_level,
            provenance_uri: provenance_uri.to_owned(),
            publisher: publisher.to_owned(),
            observed_at: Utc::now(),
            ingested_at: Utc::now(),
            content_sha256: format!("{:x}", hasher.finalize()),
            canonical_entity_type: canonical_entity_type.to_owned(),
            canonical_entity_id: canonical_entity_id.to_owned(),
            canonical_event_id: canonical_event_id.to_owned(),
            tags,
            metadata,
        }
    }

    pub fn upsert(&self, records: &[ContextPoolRecord]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let mut index_records = Vec::new();
        for record in records {
            for (point_id, text, chunk_index, chunk_count) in
                crate::evidence::index_chunks(&record.text, &record.id)
            {
                let mut chunk = record.clone();
                chunk.id = point_id;
                chunk.text = text;
                chunk.content_sha256 = format!("{:x}", Sha256::digest(chunk.text.as_bytes()));
                chunk.metadata = crate::evidence::compact_index_value(&chunk.metadata);
                if !chunk.metadata.is_object() {
                    chunk.metadata = json!({});
                }
                chunk.metadata["indexChunk"] = json!({
                    "index": chunk_index,
                    "count": chunk_count,
                    "version": 2
                });
                index_records.push(chunk);
            }
        }
        let payloads: Vec<Value> = index_records.iter().map(payload).collect();
        let response = self.execute(json!({ "action": "upsert", "records": payloads }))?;
        let count = response
            .get("upserted")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        if count != index_records.len() as u64 {
            bail!(
                "Qdrant acknowledged {count} of {} indexed evidence chunks",
                index_records.len()
            );
        }
        Ok(())
    }

    pub fn sync_pending_events(&self, store: &CanonicalStore, run_id: &str) -> Result<usize> {
        let pending = store.pending_retrieval_events(run_id, 500)?;
        let state = replay::replay_run(store, run_id)?;
        let records: Vec<ContextPoolRecord> = pending
            .iter()
            .map(|stored| event_record(stored, &state))
            .collect();
        self.upsert(&records)?;
        store.mark_retrieval_events_indexed(
            &pending
                .iter()
                .map(|stored| stored.event.id.clone())
                .collect::<Vec<_>>(),
        )?;
        Ok(records.len())
    }

    /// Backfill the versioned compact index once. The ready marker is written
    /// only after every canonical event has been indexed successfully, so a
    /// crash retries the idempotent deterministic point IDs on next launch.
    pub fn rebuild_if_needed(&self, store: &CanonicalStore) -> Result<bool> {
        let marker = self.config.storage_path.join(format!(
            ".{}.source-index-v2.ready",
            self.config.collection
        ));
        if marker.is_file() {
            return Ok(false);
        }

        for run in store.run_summaries()? {
            let events = store.stored_events(&run.run_id)?;
            if events.is_empty() {
                continue;
            }
            let state = replay::replay_run(store, &run.run_id)?;
            const EVENT_BATCH: usize = 48;
            for batch in events.chunks(EVENT_BATCH) {
                let records = batch
                    .iter()
                    .map(|stored| event_record(stored, &state))
                    .collect::<Vec<_>>();
                self.upsert(&records)?;
            }
            store.mark_retrieval_events_indexed(
                &events
                    .iter()
                    .map(|stored| stored.event.id.clone())
                    .collect::<Vec<_>>(),
            )?;
        }

        std::fs::create_dir_all(&self.config.storage_path)?;
        let temporary = marker.with_extension("ready.tmp");
        std::fs::write(&temporary, "evidence-index-v2\n")?;
        std::fs::rename(&temporary, &marker)?;
        Ok(true)
    }

    fn search(
        &self,
        query: &str,
        mode: SearchMode,
        limit: usize,
        filters: &RetrievalFilters,
        canonical_entity_id: Option<&str>,
    ) -> Result<Vec<RetrievalHit>> {
        let query = query.trim();
        if query.is_empty() {
            bail!("retrieval search query must not be empty");
        }
        let response = self.execute(BridgeSearchRequest {
            action: "search",
            query,
            mode: mode_name(&mode),
            limit: limit.max(1),
            filters,
            canonical_entity_id,
        })?;
        Ok(serde_json::from_value::<SearchResponse>(response)?.hits)
    }

    fn inspect(&self, ids: &[String]) -> Result<Vec<Value>> {
        let response = self.execute(json!({ "action": "inspect", "ids": ids }))?;
        Ok(serde_json::from_value::<InspectResponse>(response)?.records)
    }

    fn execute(&self, request: impl Serialize) -> Result<Value> {
        let _bridge_guard = self.bridge_lock.lock();
        let mut command = Command::new(&self.config.python_path);
        command
            .arg(&self.config.bridge_path)
            .arg("--storage")
            .arg(&self.config.storage_path)
            .arg("--models")
            .arg(&self.config.models_path)
            .arg("--collection")
            .arg(&self.config.collection)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;

            const CREATE_NO_WINDOW: u32 = 0x0800_0000;
            command.creation_flags(CREATE_NO_WINDOW);
        }

        let mut child = command.spawn().with_context(|| {
            format!(
                "start Qdrant bridge with {}",
                self.config.python_path.display()
            )
        })?;
        child
            .stdin
            .take()
            .context("open Qdrant bridge stdin")?
            .write_all(serde_json::to_string(&request)?.as_bytes())?;
        let mut stdout = child.stdout.take().context("open Qdrant bridge stdout")?;
        let mut stderr = child.stderr.take().context("open Qdrant bridge stderr")?;
        let stdout_reader = std::thread::spawn(move || {
            let mut bytes = Vec::new();
            stdout.read_to_end(&mut bytes).map(|_| bytes)
        });
        let stderr_reader = std::thread::spawn(move || {
            let mut bytes = Vec::new();
            stderr.read_to_end(&mut bytes).map(|_| bytes)
        });
        let deadline = Instant::now() + Duration::from_secs(12);
        let status = loop {
            if let Some(status) = child.try_wait()? {
                break status;
            }
            if Instant::now() >= deadline {
                child.kill().ok();
                child.wait().ok();
                bail!("Qdrant bridge exceeded its 12-second operation limit");
            }
            std::thread::sleep(Duration::from_millis(50));
        };
        let output = stdout_reader
            .join()
            .map_err(|_| anyhow::anyhow!("Qdrant stdout reader failed"))??;
        let error_output = stderr_reader
            .join()
            .map_err(|_| anyhow::anyhow!("Qdrant stderr reader failed"))??;
        if !status.success() {
            bail!(
                "Qdrant bridge failed: {}",
                String::from_utf8_lossy(&error_output).trim()
            );
        }
        serde_json::from_slice(&output).context("decode Qdrant bridge response")
    }
}

impl ContextRetriever for QdrantContextPool {
    fn retrieve(&self, request: &RetrievalRequest) -> Result<RetrievalTrace> {
        let mut steps = Vec::new();
        let mut hits_by_id: HashMap<String, RetrievalHit> = HashMap::new();
        let mut round = 1;

        for canonical_id in &request.exact_canonical_ids {
            let exact_hits = self.search(
                canonical_id,
                SearchMode::Exact,
                request.limit,
                &request.filters,
                Some(canonical_id),
            )?;
            steps.push(RetrievalStep {
                round,
                action: "search".into(),
                query: canonical_id.clone(),
                mode: SearchMode::Exact,
                filter: request.filters.clone(),
                result_ids: exact_hits.iter().map(|hit| hit.id.clone()).collect(),
                finding: "Exact canonical lookup completed.".into(),
            });
            for hit in exact_hits {
                hits_by_id.insert(hit.id.clone(), hit);
            }
        }

        let initial_hits = self.search(
            &request.question,
            SearchMode::Hybrid,
            request.limit,
            &request.filters,
            None,
        )?;
        steps.push(RetrievalStep {
            round,
            action: "search".into(),
            query: request.question.clone(),
            mode: SearchMode::Hybrid,
            filter: request.filters.clone(),
            result_ids: initial_hits.iter().map(|hit| hit.id.clone()).collect(),
            finding: "Hybrid semantic and BM25 retrieval completed.".into(),
        });
        for hit in initial_hits {
            hits_by_id.insert(hit.id.clone(), hit);
        }

        let inspect_ids: Vec<String> = hits_by_id.keys().take(request.limit).cloned().collect();
        let inspected = self.inspect(&inspect_ids)?;
        steps.push(RetrievalStep {
            round,
            action: "inspect".into(),
            query: request.question.clone(),
            mode: SearchMode::Hybrid,
            filter: request.filters.clone(),
            result_ids: inspect_ids,
            finding: format!("Inspected {} full provenance records.", inspected.len()),
        });

        let mut missing = missing_classes(&request.required_source_classes, hits_by_id.values());
        steps.push(RetrievalStep {
            round,
            action: "decide_sufficiency".into(),
            query: request.question.clone(),
            mode: SearchMode::Hybrid,
            filter: request.filters.clone(),
            result_ids: hits_by_id.keys().cloned().collect(),
            finding: if missing.is_empty() {
                "Evidence covers all required source classes.".into()
            } else {
                format!("Evidence is insufficient; missing {}.", missing.join(", "))
            },
        });

        while !missing.is_empty() && round < request.max_rounds.max(1) {
            round += 1;
            for source_class in missing.clone() {
                let mut filter = request.filters.clone();
                filter.source_class = Some(source_class.clone());
                let follow_up_query =
                    format!("{} evidence from {}", request.question, source_class);
                let follow_up = self.search(
                    &follow_up_query,
                    SearchMode::Semantic,
                    request.limit,
                    &filter,
                    None,
                )?;
                steps.push(RetrievalStep {
                    round,
                    action: "search_again".into(),
                    query: follow_up_query,
                    mode: SearchMode::Semantic,
                    filter,
                    result_ids: follow_up.iter().map(|hit| hit.id.clone()).collect(),
                    finding: format!("Targeted missing source class {source_class}."),
                });
                for hit in follow_up {
                    hits_by_id.insert(hit.id.clone(), hit);
                }
            }
            let ids: Vec<String> = hits_by_id.keys().take(request.limit).cloned().collect();
            let inspected = self.inspect(&ids)?;
            steps.push(RetrievalStep {
                round,
                action: "inspect".into(),
                query: request.question.clone(),
                mode: SearchMode::Semantic,
                filter: request.filters.clone(),
                result_ids: ids,
                finding: format!(
                    "Inspected {} records after follow-up search.",
                    inspected.len()
                ),
            });
            missing = missing_classes(&request.required_source_classes, hits_by_id.values());
            steps.push(RetrievalStep {
                round,
                action: "decide_sufficiency".into(),
                query: request.question.clone(),
                mode: SearchMode::Hybrid,
                filter: request.filters.clone(),
                result_ids: hits_by_id.keys().cloned().collect(),
                finding: if missing.is_empty() {
                    "Follow-up evidence is sufficient.".into()
                } else {
                    format!("Still missing {}.", missing.join(", "))
                },
            });
        }

        let mut hits: Vec<RetrievalHit> = hits_by_id.into_values().collect();
        hits.sort_by(|left, right| right.score.total_cmp(&left.score));
        let limit = request.limit.max(1);
        let mut selected = Vec::new();
        for source_class in &request.required_source_classes {
            if let Some(hit) = hits
                .iter()
                .find(|hit| &hit.source_class == source_class)
                .cloned()
            {
                if !selected
                    .iter()
                    .any(|selected_hit: &RetrievalHit| selected_hit.id == hit.id)
                {
                    selected.push(hit);
                }
            }
        }
        for canonical_id in &request.exact_canonical_ids {
            if let Some(hit) = hits
                .iter()
                .find(|hit| &hit.canonical_entity_id == canonical_id)
                .cloned()
            {
                if !selected
                    .iter()
                    .any(|selected_hit: &RetrievalHit| selected_hit.id == hit.id)
                {
                    selected.push(hit);
                }
            }
        }
        for hit in hits {
            if selected.len() >= limit {
                break;
            }
            if !selected
                .iter()
                .any(|selected_hit: &RetrievalHit| selected_hit.id == hit.id)
            {
                selected.push(hit);
            }
        }
        selected.truncate(limit);
        selected.sort_by(|left, right| left.observed_at.cmp(&right.observed_at));
        Ok(RetrievalTrace {
            question: request.question.clone(),
            sufficient: missing.is_empty(),
            missing_source_classes: missing.clone(),
            steps,
            hits: selected,
            final_action: if missing.is_empty() {
                "act_with_evidence".into()
            } else {
                "act_with_explicit_evidence_gap".into()
            },
        })
    }
}

fn payload(record: &ContextPoolRecord) -> Value {
    json!({
        "id": record.id,
        "run_id": record.run_id,
        "title": record.title,
        "text": record.text,
        "source_class": record.source_class.as_str(),
        "trust_level": record.trust_level.as_str(),
        "provenance_uri": record.provenance_uri,
        "publisher": record.publisher,
        "observed_at": record.observed_at.to_rfc3339(),
        "ingested_at": record.ingested_at.to_rfc3339(),
        "content_sha256": record.content_sha256,
        "canonical_entity_type": record.canonical_entity_type,
        "canonical_entity_id": record.canonical_entity_id,
        "canonical_event_id": record.canonical_event_id,
        "tags": record.tags,
        "metadata": record.metadata,
    })
}

fn event_record(stored: &StoredEvent, state: &ReplayState) -> ContextPoolRecord {
    if stored.event.kind == "context_record_ingested" {
        if let Some(record) = stored.event.payload.get("record") {
            if let Ok(mut record) = serde_json::from_value::<ContextPoolRecord>(record.clone()) {
                record.observed_at = stored.event.occurred_at;
                return record;
            }
        }
    }
    let dimensions = memory_dimensions(stored, state);
    let instruments = dimensions["instruments"]
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_default();
    let outcome = dimensions["outcome"].as_str().unwrap_or("recorded");
    let timeframe = dimensions["timeframe"]
        .get("label")
        .and_then(Value::as_str)
        .unwrap_or("unspecified");
    let mut tags = vec![
        "system-log".into(),
        stored.event.kind.clone(),
        outcome.into(),
    ];
    if is_experiment_memory(&stored.event.kind) {
        tags.push("experiment-memory".into());
    }
    let index_payload = crate::evidence::compact_index_value(&stored.event.payload);
    let mut record = QdrantContextPool::create_record(
        &stored.event.run_id,
        &format!("{} | {} | {}", stored.event.kind, instruments, timeframe),
        &format!(
            "{} Outcome: {}. Instruments: {}. Timeframe: {}. Canonical record: {} {}. Evidence: {}",
            stored
                .event
                .payload
                .get("summary")
                .and_then(Value::as_str)
                .unwrap_or(""),
            outcome,
            instruments,
            timeframe,
            stored.event.aggregate_type,
            stored.event.aggregate_id,
            index_payload
        ),
        ContextSourceClass::InternalCanonical,
        TrustLevel::Internal,
        &format!("canonical-event://{}", stored.event.id),
        "jev-harness",
        &stored.event.aggregate_type,
        &stored.event.aggregate_id,
        &stored.event.id,
        tags,
        json!({
            "sequence": stored.sequence,
            "event_kind": stored.event.kind,
            "loop_id": dimensions["loop_id"],
            "thesis_version_id": dimensions["thesis_version_id"],
            "context_version_id": dimensions["context_version_id"],
            "instruments": dimensions["instruments"],
            "timeframe": dimensions["timeframe"],
            "decision_id": dimensions["decision_id"],
            "position_id": dimensions["position_id"],
            "outcome": dimensions["outcome"],
            "causation_event_id": stored.event.causation_event_id,
            "canonical_record_uri": format!("canonical-record://{}/{}", stored.event.aggregate_type, stored.event.aggregate_id),
            "canonical_event_uri": format!("canonical-event://{}", stored.event.id),
        }),
    );
    record.id = stored.event.id.clone();
    record.observed_at = stored.event.occurred_at;
    record
}

fn memory_dimensions(stored: &StoredEvent, state: &ReplayState) -> Value {
    let event = &stored.event;
    let payload_record = event.payload.get("record");
    let mut loop_id = event.loop_id.clone();
    let mut thesis_id = payload_record
        .and_then(|record| record.get("thesisVersionId"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let mut context_id = payload_record
        .and_then(|record| record.get("contextVersionId"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let mut decision_id = None;
    let mut position_id = payload_record
        .and_then(|record| record.get("positionId"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let mut instruments = Vec::new();
    let mut timeframe = None;
    let mut outcome = payload_record
        .and_then(|record| {
            record
                .get("status")
                .or_else(|| record.get("state"))
                .or_else(|| record.get("action"))
                .or_else(|| record.get("classification"))
                .or_else(|| record.get("outcome"))
        })
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| event.kind.clone());

    let decision = if event.aggregate_type == "decision" {
        state
            .decisions
            .iter()
            .find(|item| item.id == event.aggregate_id)
    } else if event.aggregate_type == "order" {
        state
            .orders
            .iter()
            .find(|item| item.id == event.aggregate_id)
            .and_then(|order| {
                outcome = order.status.clone();
                decision_id = Some(order.decision_id.clone());
                state
                    .decisions
                    .iter()
                    .find(|item| item.id == order.decision_id)
            })
    } else if event.aggregate_type == "execution" {
        state
            .executions
            .iter()
            .find(|item| item.execution_id == event.aggregate_id)
            .and_then(|execution| {
                outcome = execution.status.clone();
                decision_id = Some(execution.caused_by_decision_id.clone());
                state
                    .decisions
                    .iter()
                    .find(|item| item.id == execution.caused_by_decision_id)
            })
    } else if event.aggregate_type == "position" {
        state
            .positions
            .iter()
            .find(|item| item.id == event.aggregate_id)
            .and_then(|position| {
                position_id = Some(position.id.clone());
                outcome = position.state.clone();
                state
                    .executions
                    .iter()
                    .find(|item| item.execution_id == position.opened_by_execution_id)
                    .and_then(|execution| {
                        decision_id = Some(execution.caused_by_decision_id.clone());
                        state
                            .decisions
                            .iter()
                            .find(|item| item.id == execution.caused_by_decision_id)
                    })
            })
    } else {
        None
    };
    if let Some(decision) = decision {
        loop_id = Some(decision.loop_id.clone());
        thesis_id = Some(decision.thesis_version_id.clone());
        context_id = Some(decision.context_version_id.clone());
        decision_id = Some(decision.id.clone());
        instruments = decision.resolved_state.instruments.clone();
        timeframe = Some(decision.resolved_state.timeframe.clone());
        if event.aggregate_type == "decision" {
            outcome = decision.action.clone();
        }
    }
    let referenced_hypothesis_id = payload_record
        .and_then(|record| record.get("hypothesisId"))
        .and_then(Value::as_str);
    let hypothesis = payload_record
        .and_then(|record| record.get("currentHypothesis"))
        .and_then(|value| serde_json::from_value::<HypothesisDefinition>(value.clone()).ok())
        .or_else(|| {
            state
                .hypotheses
                .iter()
                .find(|item| {
                    item.id == event.aggregate_id
                        || referenced_hypothesis_id == Some(item.id.as_str())
                })
                .cloned()
        })
        .or_else(|| {
            state
                .hypotheses
                .iter()
                .find(|item| {
                    thesis_id.as_deref() == Some(item.thesis_version_id.as_str())
                        && context_id.as_deref() == Some(item.context_version_id.as_str())
                })
                .cloned()
        })
        .or_else(|| {
            loop_id.as_ref().and_then(|id| {
                state
                    .loops
                    .iter()
                    .find(|item| &item.id == id)
                    .and_then(|loop_state| {
                        state
                            .hypotheses
                            .iter()
                            .find(|item| item.id == loop_state.hypothesis_id)
                    })
                    .cloned()
            })
        });
    if let Some(hypothesis) = hypothesis {
        if instruments.is_empty() {
            instruments = hypothesis.instruments;
        }
        timeframe.get_or_insert(hypothesis.timeframe);
        thesis_id.get_or_insert(hypothesis.thesis_version_id);
        context_id.get_or_insert(hypothesis.context_version_id);
        if loop_id.is_none() {
            loop_id = state
                .loops
                .iter()
                .find(|item| item.hypothesis_id == hypothesis.id)
                .map(|item| item.id.clone());
        }
    }
    json!({
        "loop_id": loop_id,
        "thesis_version_id": thesis_id,
        "context_version_id": context_id,
        "instruments": instruments,
        "timeframe": timeframe,
        "decision_id": decision_id,
        "position_id": position_id,
        "outcome": outcome,
    })
}

fn is_experiment_memory(kind: &str) -> bool {
    matches!(
        kind,
        "jev1_decision_recorded"
            | "jev2_decision_recorded"
            | "order_evaluated"
            | "execution_recorded"
            | "position_opened"
            | "position_closed"
            | "position_reconciled_closed"
            | "world_model_reviewed"
            | "startup_memory_review_recorded"
            | "trade_outcome_recorded"
            | "autonomous_review_triggered"
            | "autonomous_review_completed"
            | "autonomous_review_package_assembled"
            | "hypothesis_reviewed"
            | "world_model_review_package_assembled"
            | "hypothesis_modified"
            | "hypothesis_split"
            | "hypothesis_stopped"
            | "loop_stopped"
    )
}

fn missing_classes<'a>(
    required: &[String],
    hits: impl Iterator<Item = &'a RetrievalHit>,
) -> Vec<String> {
    let present: HashSet<String> = hits.map(|hit| hit.source_class.clone()).collect();
    required
        .iter()
        .filter(|required| !present.contains(*required))
        .cloned()
        .collect()
}

fn mode_name(mode: &SearchMode) -> &'static str {
    match mode {
        SearchMode::Hybrid => "hybrid",
        SearchMode::Semantic => "semantic",
        SearchMode::Keyword => "keyword",
        SearchMode::Exact => "exact",
    }
}
