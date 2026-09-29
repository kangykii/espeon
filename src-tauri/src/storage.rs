use crate::domain::*;
use crate::ports::SearchableContext;
use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Clone)]
pub struct CanonicalStore {
    connection: Arc<Mutex<Connection>>,
    event_log_path: PathBuf,
}

impl CanonicalStore {
    pub fn open(runtime_dir: &Path) -> Result<Self> {
        fs::create_dir_all(runtime_dir)
            .with_context(|| format!("create runtime directory {}", runtime_dir.display()))?;
        let connection = Connection::open(runtime_dir.join("harness.sqlite3"))?;
        connection.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA foreign_keys=ON;
             CREATE TABLE IF NOT EXISTS canonical_runs (
               id TEXT PRIMARY KEY,
               human_thesis TEXT NOT NULL,
               status TEXT NOT NULL CHECK(status IN ('active','stopped')),
               started_at TEXT NOT NULL,
               stopped_at TEXT
             );
             CREATE TABLE IF NOT EXISTS run_presentation (
               run_id TEXT PRIMARY KEY REFERENCES canonical_runs(id),
               display_name TEXT,
               archived INTEGER NOT NULL DEFAULT 0 CHECK(archived IN (0,1))
             );
             CREATE TABLE IF NOT EXISTS canonical_events (
               sequence INTEGER PRIMARY KEY AUTOINCREMENT,
               id TEXT UNIQUE NOT NULL,
               run_id TEXT NOT NULL REFERENCES canonical_runs(id),
               loop_id TEXT,
               kind TEXT NOT NULL,
               aggregate_type TEXT NOT NULL,
               aggregate_id TEXT NOT NULL,
               causation_event_id TEXT REFERENCES canonical_events(id),
               occurred_at TEXT NOT NULL,
               payload_json TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS idx_canonical_events_run_sequence
               ON canonical_events(run_id, sequence);
             CREATE TABLE IF NOT EXISTS canonical_thesis_versions (
               id TEXT PRIMARY KEY,
               run_id TEXT NOT NULL REFERENCES canonical_runs(id),
               version INTEGER NOT NULL,
               thesis TEXT NOT NULL,
               provenance TEXT NOT NULL,
               created_by_event_id TEXT UNIQUE NOT NULL REFERENCES canonical_events(id),
               created_at TEXT NOT NULL,
               UNIQUE(run_id, version)
             );
             CREATE TABLE IF NOT EXISTS canonical_context_definitions (
               id TEXT PRIMARY KEY,
               run_id TEXT NOT NULL REFERENCES canonical_runs(id),
               name TEXT NOT NULL,
               description TEXT NOT NULL,
               created_by_event_id TEXT UNIQUE NOT NULL REFERENCES canonical_events(id),
               created_at TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS canonical_context_versions (
               id TEXT PRIMARY KEY,
               definition_id TEXT NOT NULL REFERENCES canonical_context_definitions(id),
               run_id TEXT NOT NULL REFERENCES canonical_runs(id),
               version INTEGER NOT NULL,
               items_json TEXT NOT NULL,
               created_by_event_id TEXT UNIQUE NOT NULL REFERENCES canonical_events(id),
               created_at TEXT NOT NULL,
               UNIQUE(definition_id, version)
             );
             CREATE TABLE IF NOT EXISTS canonical_hypothesis_definitions (
               id TEXT PRIMARY KEY,
               root_hypothesis_id TEXT NOT NULL,
               run_id TEXT NOT NULL REFERENCES canonical_runs(id),
               version INTEGER NOT NULL,
               parent_hypothesis_id TEXT,
               thesis_version_id TEXT NOT NULL REFERENCES canonical_thesis_versions(id),
               context_version_id TEXT NOT NULL REFERENCES canonical_context_versions(id),
               status TEXT NOT NULL,
               definition_json TEXT NOT NULL,
               created_by_event_id TEXT UNIQUE NOT NULL REFERENCES canonical_events(id),
               created_at TEXT NOT NULL,
               UNIQUE(root_hypothesis_id, version)
             );
             CREATE TABLE IF NOT EXISTS canonical_loops (
               id TEXT PRIMARY KEY,
               run_id TEXT NOT NULL REFERENCES canonical_runs(id),
               parent_loop_id TEXT REFERENCES canonical_loops(id),
               parent_thesis_version_id TEXT REFERENCES canonical_thesis_versions(id),
               thesis_version_id TEXT NOT NULL REFERENCES canonical_thesis_versions(id),
               context_version_id TEXT NOT NULL REFERENCES canonical_context_versions(id),
               state TEXT NOT NULL,
               allocated_fraction REAL NOT NULL,
               created_by_event_id TEXT UNIQUE NOT NULL REFERENCES canonical_events(id),
               created_at TEXT NOT NULL,
               stopped_at TEXT
             );
             CREATE TABLE IF NOT EXISTS canonical_loop_hypotheses (
               loop_id TEXT PRIMARY KEY REFERENCES canonical_loops(id),
               hypothesis_id TEXT NOT NULL REFERENCES canonical_hypothesis_definitions(id),
               linked_by_event_id TEXT NOT NULL REFERENCES canonical_events(id)
             );
             CREATE TABLE IF NOT EXISTS canonical_loop_cadences (
               loop_id TEXT PRIMARY KEY REFERENCES canonical_loops(id),
               hypothesis_id TEXT NOT NULL REFERENCES canonical_hypothesis_definitions(id),
               cadence_json TEXT NOT NULL,
               created_by_event_id TEXT UNIQUE NOT NULL REFERENCES canonical_events(id)
             );
             CREATE TABLE IF NOT EXISTS canonical_decisions (
               id TEXT PRIMARY KEY,
               run_id TEXT NOT NULL REFERENCES canonical_runs(id),
               loop_id TEXT NOT NULL REFERENCES canonical_loops(id),
               stage TEXT NOT NULL CHECK(stage IN ('jev1','jev2')),
               thesis_version_id TEXT NOT NULL REFERENCES canonical_thesis_versions(id),
               context_version_id TEXT NOT NULL REFERENCES canonical_context_versions(id),
               position_id TEXT,
               action TEXT NOT NULL,
               confidence REAL NOT NULL,
               rationale TEXT NOT NULL,
               created_by_event_id TEXT UNIQUE NOT NULL REFERENCES canonical_events(id),
               created_at TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS canonical_jev_inferences (
               decision_id TEXT PRIMARY KEY REFERENCES canonical_decisions(id),
               inference_json TEXT NOT NULL,
               resolved_state_json TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS canonical_market_quotes (
               id TEXT PRIMARY KEY,
               symbol TEXT NOT NULL,
               observation_json TEXT NOT NULL,
               source_timestamp TEXT NOT NULL,
               received_at TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS canonical_market_candles (
               id TEXT PRIMARY KEY,
               symbol TEXT NOT NULL,
               period TEXT NOT NULL,
               open_time TEXT NOT NULL,
               observation_json TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS canonical_resolved_context_snapshots (
               id TEXT PRIMARY KEY,
               run_id TEXT NOT NULL REFERENCES canonical_runs(id),
               loop_id TEXT NOT NULL REFERENCES canonical_loops(id),
               thesis_version_id TEXT NOT NULL REFERENCES canonical_thesis_versions(id),
               context_version_id TEXT NOT NULL REFERENCES canonical_context_versions(id),
               spec_id TEXT NOT NULL,
               spec_version INTEGER NOT NULL,
               snapshot_json TEXT NOT NULL,
               created_by_event_id TEXT UNIQUE NOT NULL REFERENCES canonical_events(id),
               resolved_at TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS canonical_executions (
               id TEXT PRIMARY KEY,
               run_id TEXT NOT NULL REFERENCES canonical_runs(id),
               loop_id TEXT NOT NULL REFERENCES canonical_loops(id),
               caused_by_decision_id TEXT NOT NULL REFERENCES canonical_decisions(id),
               action TEXT NOT NULL,
               status TEXT NOT NULL,
               broker_reference TEXT NOT NULL,
               created_by_event_id TEXT UNIQUE NOT NULL REFERENCES canonical_events(id),
               executed_at TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS canonical_execution_metadata (
               execution_id TEXT PRIMARY KEY REFERENCES canonical_executions(id),
               execution_kind TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS canonical_orders (
               id TEXT PRIMARY KEY,
               run_id TEXT NOT NULL REFERENCES canonical_runs(id),
               loop_id TEXT NOT NULL REFERENCES canonical_loops(id),
               decision_id TEXT NOT NULL REFERENCES canonical_decisions(id),
               idempotency_key TEXT UNIQUE NOT NULL,
               status TEXT NOT NULL,
               order_json TEXT NOT NULL,
               created_by_event_id TEXT UNIQUE NOT NULL REFERENCES canonical_events(id),
               created_at TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS canonical_position_controls (
               position_id TEXT PRIMARY KEY REFERENCES canonical_positions(id),
               order_id TEXT NOT NULL REFERENCES canonical_orders(id),
               control_json TEXT NOT NULL,
               created_by_event_id TEXT UNIQUE NOT NULL REFERENCES canonical_events(id)
             );
             CREATE TABLE IF NOT EXISTS canonical_loop_failure_history (
               event_id TEXT PRIMARY KEY REFERENCES canonical_events(id),
               loop_id TEXT NOT NULL REFERENCES canonical_loops(id),
               state_json TEXT NOT NULL,
               created_at TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS canonical_positions (
               id TEXT PRIMARY KEY,
               run_id TEXT NOT NULL REFERENCES canonical_runs(id),
               loop_id TEXT NOT NULL REFERENCES canonical_loops(id),
               opened_by_execution_id TEXT UNIQUE NOT NULL REFERENCES canonical_executions(id),
               broker_position_id TEXT,
               direction TEXT NOT NULL,
               state TEXT NOT NULL,
               opened_at TEXT NOT NULL,
               closed_by_execution_id TEXT REFERENCES canonical_executions(id),
               closed_at TEXT,
               last_event_id TEXT NOT NULL REFERENCES canonical_events(id)
             );
             CREATE TABLE IF NOT EXISTS canonical_world_model_reviews (
               id TEXT PRIMARY KEY,
               run_id TEXT NOT NULL REFERENCES canonical_runs(id),
               thesis_version_id TEXT NOT NULL REFERENCES canonical_thesis_versions(id),
               context_version_id TEXT NOT NULL REFERENCES canonical_context_versions(id),
               evidence_canonical_ids_json TEXT NOT NULL,
               critique TEXT NOT NULL,
               outcome TEXT NOT NULL,
               created_by_event_id TEXT UNIQUE NOT NULL REFERENCES canonical_events(id),
               created_at TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS canonical_hypothesis_reviews (
               id TEXT PRIMARY KEY,
               run_id TEXT NOT NULL REFERENCES canonical_runs(id),
               hypothesis_id TEXT NOT NULL REFERENCES canonical_hypothesis_definitions(id),
               action TEXT NOT NULL,
               decision_json TEXT NOT NULL,
               created_by_event_id TEXT UNIQUE NOT NULL REFERENCES canonical_events(id),
               created_at TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS canonical_trade_outcomes (
               id TEXT PRIMARY KEY,
               run_id TEXT NOT NULL REFERENCES canonical_runs(id),
               loop_id TEXT NOT NULL REFERENCES canonical_loops(id),
               position_id TEXT UNIQUE NOT NULL REFERENCES canonical_positions(id),
               classification TEXT NOT NULL,
               outcome_json TEXT NOT NULL,
               created_by_event_id TEXT UNIQUE NOT NULL REFERENCES canonical_events(id),
               completed_at TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS canonical_autonomous_review_trigger_history (
               event_id TEXT PRIMARY KEY REFERENCES canonical_events(id),
               trigger_id TEXT NOT NULL,
               run_id TEXT NOT NULL REFERENCES canonical_runs(id),
               loop_id TEXT NOT NULL REFERENCES canonical_loops(id),
               status TEXT NOT NULL,
               trigger_json TEXT NOT NULL,
               created_at TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS idx_review_trigger_loop
               ON canonical_autonomous_review_trigger_history(loop_id, created_at);
             CREATE TABLE IF NOT EXISTS canonical_capital_allocations (
               id TEXT PRIMARY KEY,
               run_id TEXT NOT NULL REFERENCES canonical_runs(id),
               reason TEXT NOT NULL,
               allocations_json TEXT NOT NULL,
               created_by_event_id TEXT UNIQUE NOT NULL REFERENCES canonical_events(id),
               created_at TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS search_documents (
               id TEXT PRIMARY KEY,
               run_id TEXT NOT NULL REFERENCES canonical_runs(id),
               canonical_entity_type TEXT NOT NULL,
               canonical_entity_id TEXT NOT NULL,
               canonical_event_id TEXT NOT NULL REFERENCES canonical_events(id),
               text TEXT NOT NULL,
               indexed_by_event_id TEXT UNIQUE NOT NULL REFERENCES canonical_events(id),
               created_at TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS idx_search_documents_text ON search_documents(text);
             CREATE TABLE IF NOT EXISTS canonical_context_pool_records (
               id TEXT PRIMARY KEY,
               run_id TEXT NOT NULL REFERENCES canonical_runs(id),
               title TEXT NOT NULL,
               text TEXT NOT NULL,
               source_class TEXT NOT NULL,
               trust_level TEXT NOT NULL,
               provenance_uri TEXT NOT NULL,
               publisher TEXT NOT NULL,
               observed_at TEXT NOT NULL,
               ingested_at TEXT NOT NULL,
               content_sha256 TEXT NOT NULL,
               canonical_entity_type TEXT NOT NULL,
               canonical_entity_id TEXT NOT NULL,
               canonical_event_id TEXT UNIQUE NOT NULL REFERENCES canonical_events(id),
               tags_json TEXT NOT NULL,
               metadata_json TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS retrieval_outbox (
               event_id TEXT PRIMARY KEY REFERENCES canonical_events(id),
               run_id TEXT NOT NULL REFERENCES canonical_runs(id),
               status TEXT NOT NULL DEFAULT 'pending',
               indexed_at TEXT
             );
             CREATE TRIGGER IF NOT EXISTS canonical_events_no_update
               BEFORE UPDATE ON canonical_events BEGIN SELECT RAISE(ABORT, 'canonical events are append-only'); END;
             CREATE TRIGGER IF NOT EXISTS canonical_events_no_delete
               BEFORE DELETE ON canonical_events BEGIN SELECT RAISE(ABORT, 'canonical events are append-only'); END;
             CREATE TRIGGER IF NOT EXISTS thesis_versions_no_update
               BEFORE UPDATE ON canonical_thesis_versions BEGIN SELECT RAISE(ABORT, 'thesis versions are immutable'); END;
             CREATE TRIGGER IF NOT EXISTS thesis_versions_no_delete
               BEFORE DELETE ON canonical_thesis_versions BEGIN SELECT RAISE(ABORT, 'thesis versions are immutable'); END;
             CREATE TRIGGER IF NOT EXISTS context_definitions_no_update
               BEFORE UPDATE ON canonical_context_definitions BEGIN SELECT RAISE(ABORT, 'context definitions are immutable'); END;
             CREATE TRIGGER IF NOT EXISTS context_definitions_no_delete
               BEFORE DELETE ON canonical_context_definitions BEGIN SELECT RAISE(ABORT, 'context definitions are immutable'); END;
             CREATE TRIGGER IF NOT EXISTS context_versions_no_update
               BEFORE UPDATE ON canonical_context_versions BEGIN SELECT RAISE(ABORT, 'context versions are immutable'); END;
             CREATE TRIGGER IF NOT EXISTS context_versions_no_delete
               BEFORE DELETE ON canonical_context_versions BEGIN SELECT RAISE(ABORT, 'context versions are immutable'); END;
             DROP TRIGGER IF EXISTS hypotheses_no_update;
             CREATE TRIGGER hypotheses_no_update
               BEFORE UPDATE ON canonical_hypothesis_definitions
               WHEN NOT COALESCE((
                 OLD.status = 'DRAFT'
                 AND NEW.status IN ('ACTIVE','REJECTED')
                 AND OLD.id IS NEW.id
                 AND OLD.root_hypothesis_id IS NEW.root_hypothesis_id
                 AND OLD.run_id IS NEW.run_id
                 AND OLD.version IS NEW.version
                 AND OLD.parent_hypothesis_id IS NEW.parent_hypothesis_id
                 AND OLD.thesis_version_id IS NEW.thesis_version_id
                 AND OLD.context_version_id IS NEW.context_version_id
                 AND OLD.created_by_event_id IS NEW.created_by_event_id
                 AND OLD.created_at IS NEW.created_at
                 AND json_valid(OLD.definition_json)
                 AND json_valid(NEW.definition_json)
                 AND json_extract(OLD.definition_json, '$.status') = 'DRAFT'
                 AND json_extract(OLD.definition_json, '$.contract.state') = 'DRAFT'
                 AND json_extract(NEW.definition_json, '$.status') = NEW.status
                 AND json_extract(NEW.definition_json, '$.contract.state') = NEW.status
                 AND json_remove(OLD.definition_json, '$.status', '$.contract.state')
                     = json_remove(NEW.definition_json, '$.status', '$.contract.state')
               ), 0)
               BEGIN SELECT RAISE(ABORT, 'hypothesis versions are immutable except for the DRAFT lifecycle transition'); END;
             CREATE TRIGGER IF NOT EXISTS hypotheses_no_delete
               BEFORE DELETE ON canonical_hypothesis_definitions BEGIN SELECT RAISE(ABORT, 'hypothesis versions are immutable'); END;
             CREATE TRIGGER IF NOT EXISTS hypothesis_reviews_no_update
               BEFORE UPDATE ON canonical_hypothesis_reviews BEGIN SELECT RAISE(ABORT, 'hypothesis reviews are immutable'); END;
             CREATE TRIGGER IF NOT EXISTS hypothesis_reviews_no_delete
               BEFORE DELETE ON canonical_hypothesis_reviews BEGIN SELECT RAISE(ABORT, 'hypothesis reviews are immutable'); END;
             CREATE TRIGGER IF NOT EXISTS decisions_no_update
               BEFORE UPDATE ON canonical_decisions BEGIN SELECT RAISE(ABORT, 'decisions are immutable'); END;
             CREATE TRIGGER IF NOT EXISTS decisions_no_delete
               BEFORE DELETE ON canonical_decisions BEGIN SELECT RAISE(ABORT, 'decisions are immutable'); END;
             CREATE TRIGGER IF NOT EXISTS market_quotes_no_update
               BEFORE UPDATE ON canonical_market_quotes BEGIN SELECT RAISE(ABORT, 'market quotes are immutable'); END;
             CREATE TRIGGER IF NOT EXISTS market_quotes_no_delete
               BEFORE DELETE ON canonical_market_quotes BEGIN SELECT RAISE(ABORT, 'market quotes are immutable'); END;
             CREATE TRIGGER IF NOT EXISTS market_candles_no_update
               BEFORE UPDATE ON canonical_market_candles BEGIN SELECT RAISE(ABORT, 'market candles are immutable'); END;
             CREATE TRIGGER IF NOT EXISTS market_candles_no_delete
               BEFORE DELETE ON canonical_market_candles BEGIN SELECT RAISE(ABORT, 'market candles are immutable'); END;
             CREATE TRIGGER IF NOT EXISTS resolved_context_snapshots_no_update
               BEFORE UPDATE ON canonical_resolved_context_snapshots BEGIN SELECT RAISE(ABORT, 'resolved context snapshots are immutable'); END;
             CREATE TRIGGER IF NOT EXISTS resolved_context_snapshots_no_delete
               BEFORE DELETE ON canonical_resolved_context_snapshots BEGIN SELECT RAISE(ABORT, 'resolved context snapshots are immutable'); END;
             CREATE TRIGGER IF NOT EXISTS jev_inferences_no_update
               BEFORE UPDATE ON canonical_jev_inferences BEGIN SELECT RAISE(ABORT, 'Jev inference records are immutable'); END;
             CREATE TRIGGER IF NOT EXISTS jev_inferences_no_delete
               BEFORE DELETE ON canonical_jev_inferences BEGIN SELECT RAISE(ABORT, 'Jev inference records are immutable'); END;
             CREATE TRIGGER IF NOT EXISTS orders_no_update
               BEFORE UPDATE ON canonical_orders BEGIN SELECT RAISE(ABORT, 'orders are immutable'); END;
             CREATE TRIGGER IF NOT EXISTS orders_no_delete
               BEFORE DELETE ON canonical_orders BEGIN SELECT RAISE(ABORT, 'orders are immutable'); END;
             CREATE TRIGGER IF NOT EXISTS position_controls_no_update
               BEFORE UPDATE ON canonical_position_controls BEGIN SELECT RAISE(ABORT, 'position controls are immutable'); END;
             CREATE TRIGGER IF NOT EXISTS position_controls_no_delete
               BEFORE DELETE ON canonical_position_controls BEGIN SELECT RAISE(ABORT, 'position controls are immutable'); END;
             CREATE TRIGGER IF NOT EXISTS executions_no_update
               BEFORE UPDATE ON canonical_executions BEGIN SELECT RAISE(ABORT, 'executions are immutable'); END;
             CREATE TRIGGER IF NOT EXISTS executions_no_delete
               BEFORE DELETE ON canonical_executions BEGIN SELECT RAISE(ABORT, 'executions are immutable'); END;
             CREATE TRIGGER IF NOT EXISTS reviews_no_update
               BEFORE UPDATE ON canonical_world_model_reviews BEGIN SELECT RAISE(ABORT, 'reviews are immutable'); END;
             CREATE TRIGGER IF NOT EXISTS reviews_no_delete
               BEFORE DELETE ON canonical_world_model_reviews BEGIN SELECT RAISE(ABORT, 'reviews are immutable'); END;
             CREATE TRIGGER IF NOT EXISTS trade_outcomes_no_update
               BEFORE UPDATE ON canonical_trade_outcomes BEGIN SELECT RAISE(ABORT, 'trade outcomes are immutable'); END;
             CREATE TRIGGER IF NOT EXISTS trade_outcomes_no_delete
               BEFORE DELETE ON canonical_trade_outcomes BEGIN SELECT RAISE(ABORT, 'trade outcomes are immutable'); END;
             CREATE TRIGGER IF NOT EXISTS autonomous_review_triggers_no_update
               BEFORE UPDATE ON canonical_autonomous_review_trigger_history BEGIN SELECT RAISE(ABORT, 'review trigger history is immutable'); END;
             CREATE TRIGGER IF NOT EXISTS autonomous_review_triggers_no_delete
               BEFORE DELETE ON canonical_autonomous_review_trigger_history BEGIN SELECT RAISE(ABORT, 'review trigger history is immutable'); END;
             CREATE TRIGGER IF NOT EXISTS allocations_no_update
               BEFORE UPDATE ON canonical_capital_allocations BEGIN SELECT RAISE(ABORT, 'allocation history is immutable'); END;
             CREATE TRIGGER IF NOT EXISTS allocations_no_delete
               BEFORE DELETE ON canonical_capital_allocations BEGIN SELECT RAISE(ABORT, 'allocation history is immutable'); END;
             CREATE TRIGGER IF NOT EXISTS context_pool_records_no_update
               BEFORE UPDATE ON canonical_context_pool_records BEGIN SELECT RAISE(ABORT, 'context pool records are immutable'); END;
             CREATE TRIGGER IF NOT EXISTS context_pool_records_no_delete
               BEFORE DELETE ON canonical_context_pool_records BEGIN SELECT RAISE(ABORT, 'context pool records are immutable'); END;",
        )?;
        let _ = connection.execute(
            "ALTER TABLE canonical_positions ADD COLUMN broker_position_id TEXT",
            [],
        );
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            event_log_path: runtime_dir.join("canonical-events.jsonl"),
        })
    }

    pub fn create_run(&self, run_id: &str, thesis: &str, event: &HarnessEvent) -> Result<i64> {
        let mut connection = self.connection.lock();
        let transaction = connection.transaction()?;
        transaction.execute(
            "INSERT INTO canonical_runs(id, human_thesis, status, started_at) VALUES (?1, ?2, 'active', ?3)",
            params![run_id, thesis, event.occurred_at.to_rfc3339()],
        )?;
        let sequence = Self::insert_event(&transaction, event)?;
        transaction.commit()?;
        self.mirror_event(sequence, event);
        Ok(sequence)
    }

    pub fn active_run_ids(&self) -> Result<Vec<String>> {
        let connection = self.connection.lock();
        let mut statement = connection
            .prepare("SELECT id FROM canonical_runs WHERE status='active' ORDER BY started_at")?;
        let run_ids = statement
            .query_map([], |row| row.get(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(run_ids)
    }

    pub fn run_human_thesis(&self, run_id: &str) -> Result<String> {
        let connection = self.connection.lock();
        connection
            .query_row(
                "SELECT human_thesis FROM canonical_runs WHERE id=?1",
                params![run_id],
                |row| row.get(0),
            )
            .optional()?
            .with_context(|| format!("canonical run {run_id} is missing its authoritative prompt"))
    }

    pub fn record_thesis(&self, record: &ThesisVersion, event: &HarnessEvent) -> Result<i64> {
        self.transact(event, |transaction| {
            transaction.execute(
                "INSERT INTO canonical_thesis_versions VALUES (?1,?2,?3,?4,?5,?6,?7)",
                params![
                    record.id,
                    record.run_id,
                    record.version,
                    record.thesis,
                    record.provenance,
                    record.created_by_event_id,
                    record.created_at.to_rfc3339()
                ],
            )?;
            Ok(())
        })
    }

    pub fn record_context_definition(
        &self,
        record: &ContextDefinition,
        event: &HarnessEvent,
    ) -> Result<i64> {
        self.transact(event, |transaction| {
            transaction.execute(
                "INSERT INTO canonical_context_definitions VALUES (?1,?2,?3,?4,?5,?6)",
                params![
                    record.id,
                    record.run_id,
                    record.name,
                    record.description,
                    record.created_by_event_id,
                    record.created_at.to_rfc3339()
                ],
            )?;
            Ok(())
        })
    }

    pub fn record_context_version(
        &self,
        record: &ContextVersion,
        event: &HarnessEvent,
    ) -> Result<i64> {
        self.transact(event, |transaction| {
            transaction.execute(
                "INSERT INTO canonical_context_versions VALUES (?1,?2,?3,?4,?5,?6,?7)",
                params![
                    record.id,
                    record.definition_id,
                    record.run_id,
                    record.version,
                    serde_json::to_string(&record.items)?,
                    record.created_by_event_id,
                    record.created_at.to_rfc3339()
                ],
            )?;
            Ok(())
        })
    }

    pub fn record_hypothesis(
        &self,
        record: &HypothesisDefinition,
        event: &HarnessEvent,
    ) -> Result<i64> {
        if record.status != "DRAFT"
            || record.contract.as_ref().map(|contract| contract.state)
                != Some(crate::contracts::ContractState::Draft)
        {
            bail!("new hypothesis records must enter canonical storage as a DRAFT contract");
        }
        self.transact(event, |transaction| {
            transaction.execute(
                "INSERT INTO canonical_hypothesis_definitions VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
                params![
                    record.id,
                    record.root_hypothesis_id,
                    record.run_id,
                    record.version,
                    record.parent_hypothesis_id,
                    record.thesis_version_id,
                    record.context_version_id,
                    record.status,
                    serde_json::to_string(record)?,
                    record.created_by_event_id,
                    record.created_at.to_rfc3339()
                ],
            )?;
            Ok(())
        })
    }

    pub fn spawn_loop(&self, record: &LoopView, event: &HarnessEvent) -> Result<i64> {
        self.transact(event, |transaction| {
            let (status, definition_json): (String, String) = transaction.query_row(
                "SELECT status,definition_json FROM canonical_hypothesis_definitions WHERE id=?1",
                params![record.hypothesis_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            ).context("loop spawn references a hypothesis absent from canonical storage")?;
            let contract: Option<crate::contracts::HypothesisContract> =
                serde_json::from_str::<HypothesisDefinition>(&definition_json)?.contract;
            if status != "ACTIVE"
                || contract.as_ref().map(|item| item.state)
                    != Some(crate::contracts::ContractState::Active)
            {
                bail!("loop spawn rejected: referenced hypothesis contract is not ACTIVE");
            }
            transaction.execute(
                "INSERT INTO canonical_loops VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,NULL)",
                params![
                    record.id,
                    record.run_id,
                    record.parent_loop_id,
                    record.parent_thesis_version_id,
                    record.thesis_version_id,
                    record.context_version_id,
                    record.state,
                    record.allocated_fraction,
                    record.created_by_event_id,
                    record.created_at.to_rfc3339()
                ],
            )?;
            transaction.execute(
                "INSERT INTO canonical_loop_hypotheses VALUES (?1,?2,?3)",
                params![record.id, record.hypothesis_id, event.id],
            )?;
            Ok(())
        })
    }

    pub fn activate_hypothesis(
        &self,
        record: &HypothesisDefinition,
        event: &HarnessEvent,
    ) -> Result<i64> {
        if record.status != "ACTIVE"
            || record.contract.as_ref().map(|contract| contract.state)
                != Some(crate::contracts::ContractState::Active)
        {
            bail!("activation update must contain an ACTIVE contract");
        }
        self.transact(event, |transaction| {
            let updated = transaction.execute(
                "UPDATE canonical_hypothesis_definitions SET status=?2,definition_json=?3 WHERE id=?1 AND status='DRAFT'",
                params![record.id, record.status, serde_json::to_string(record)?],
            )?;
            if updated != 1 {
                bail!("contract activation requires one existing DRAFT hypothesis record");
            }
            Ok(())
        })
    }

    pub fn reject_hypothesis(
        &self,
        record: &HypothesisDefinition,
        event: &HarnessEvent,
    ) -> Result<i64> {
        if record.status != "REJECTED"
            || record.contract.as_ref().map(|contract| contract.state)
                != Some(crate::contracts::ContractState::Rejected)
        {
            bail!("rejection update must contain a REJECTED contract");
        }
        self.transact(event, |transaction| {
            let updated = transaction.execute(
                "UPDATE canonical_hypothesis_definitions SET status=?2,definition_json=?3 WHERE id=?1 AND status='DRAFT'",
                params![record.id, record.status, serde_json::to_string(record)?],
            )?;
            if updated != 1 {
                bail!("contract rejection requires one existing DRAFT hypothesis record");
            }
            Ok(())
        })
    }

    pub fn record_cadence(&self, record: &LoopCadence, event: &HarnessEvent) -> Result<i64> {
        self.transact(event, |transaction| {
            transaction.execute(
                "INSERT INTO canonical_loop_cadences VALUES (?1,?2,?3,?4)",
                params![
                    record.loop_id,
                    record.hypothesis_id,
                    serde_json::to_string(record)?,
                    record.created_by_event_id
                ],
            )?;
            Ok(())
        })
    }

    pub fn record_allocation(
        &self,
        record: &CapitalAllocationRecord,
        event: &HarnessEvent,
    ) -> Result<i64> {
        self.transact(event, |transaction| {
            transaction.execute(
                "INSERT INTO canonical_capital_allocations VALUES (?1,?2,?3,?4,?5,?6)",
                params![
                    record.id,
                    record.run_id,
                    record.reason,
                    serde_json::to_string(&record.allocations)?,
                    record.created_by_event_id,
                    record.created_at.to_rfc3339()
                ],
            )?;
            for allocation in &record.allocations {
                transaction.execute(
                    "UPDATE canonical_loops SET allocated_fraction=?2 WHERE id=?1 AND run_id=?3",
                    params![allocation.loop_id, allocation.fraction, record.run_id],
                )?;
            }
            Ok(())
        })
    }

    pub fn record_decision(&self, record: &DecisionRecord, event: &HarnessEvent) -> Result<i64> {
        self.transact(event, |transaction| {
            transaction.execute(
                "INSERT INTO canonical_decisions VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
                params![
                    record.id,
                    record.run_id,
                    record.loop_id,
                    record.stage,
                    record.thesis_version_id,
                    record.context_version_id,
                    record.position_id,
                    record.action,
                    record.confidence,
                    record.rationale,
                    record.created_by_event_id,
                    record.created_at.to_rfc3339()
                ],
            )?;
            transaction.execute(
                "INSERT INTO canonical_jev_inferences VALUES (?1,?2,?3)",
                params![
                    record.id,
                    serde_json::to_string(&record.inference)?,
                    serde_json::to_string(&record.resolved_state)?
                ],
            )?;
            Ok(())
        })
    }

    pub fn record_resolved_context_snapshot(
        &self,
        record: &ResolvedContextSnapshot,
        event: &HarnessEvent,
    ) -> Result<i64> {
        self.transact(event, |transaction| {
            transaction.execute(
                "INSERT OR IGNORE INTO canonical_market_quotes VALUES (?1,?2,?3,?4,?5)",
                params![record.quote.id, record.quote.symbol, serde_json::to_string(&record.quote)?, record.quote.source_timestamp.to_rfc3339(), record.quote.received_at.to_rfc3339()],
            )?;
            for candle in &record.candles {
                transaction.execute(
                    "INSERT OR IGNORE INTO canonical_market_candles VALUES (?1,?2,?3,?4,?5)",
                    params![candle.id, candle.symbol, format!("{:?}", candle.period), candle.open_time.to_rfc3339(), serde_json::to_string(candle)?],
                )?;
            }
            transaction.execute(
                "INSERT INTO canonical_resolved_context_snapshots VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
                params![record.id, record.run_id, record.loop_id, record.thesis_version_id, record.context_version_id, record.live_context_spec_id, record.live_context_spec_version, serde_json::to_string(record)?, record.created_by_event_id, record.resolved_at.to_rfc3339()],
            )?;
            Ok(())
        })
    }

    pub fn record_execution(&self, record: &ExecutionReceipt, event: &HarnessEvent) -> Result<i64> {
        self.transact(event, |transaction| {
            transaction.execute(
                "INSERT INTO canonical_executions VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
                params![
                    record.execution_id,
                    record.run_id,
                    record.loop_id,
                    record.caused_by_decision_id,
                    format!("{:?}", record.action),
                    record.status,
                    record.broker_reference,
                    record.created_by_event_id,
                    record.executed_at.to_rfc3339()
                ],
            )?;
            transaction.execute(
                "INSERT INTO canonical_execution_metadata VALUES (?1,?2)",
                params![record.execution_id, record.execution_kind],
            )?;
            Ok(())
        })
    }

    pub fn record_trade_outcome(
        &self,
        record: &TradeOutcomeRecord,
        event: &HarnessEvent,
    ) -> Result<i64> {
        self.transact(event, |transaction| {
            transaction.execute(
                "INSERT INTO canonical_trade_outcomes VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
                params![
                    record.id,
                    record.run_id,
                    record.loop_id,
                    record.position_id,
                    format!("{:?}", record.classification).to_ascii_lowercase(),
                    serde_json::to_string(record)?,
                    record.created_by_event_id,
                    record.completed_at.to_rfc3339()
                ],
            )?;
            Ok(())
        })
    }

    pub fn record_autonomous_review_trigger(
        &self,
        record: &AutonomousReviewTriggerRecord,
        event: &HarnessEvent,
    ) -> Result<i64> {
        self.transact(event, |transaction| {
            transaction.execute(
                "INSERT INTO canonical_autonomous_review_trigger_history VALUES (?1,?2,?3,?4,?5,?6,?7)",
                params![
                    event.id,
                    record.trigger_id,
                    record.run_id,
                    record.loop_id,
                    record.status,
                    serde_json::to_string(record)?,
                    record.created_at.to_rfc3339()
                ],
            )?;
            Ok(())
        })
    }

    pub fn record_order(&self, record: &OrderRecord, event: &HarnessEvent) -> Result<i64> {
        self.transact(event, |transaction| {
            transaction.execute(
                "INSERT INTO canonical_orders VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
                params![
                    record.id,
                    record.run_id,
                    record.loop_id,
                    record.decision_id,
                    record.idempotency_key,
                    record.status,
                    serde_json::to_string(record)?,
                    record.created_by_event_id,
                    record.created_at.to_rfc3339()
                ],
            )?;
            Ok(())
        })
    }

    pub fn order_exists(&self, idempotency_key: &str) -> Result<bool> {
        let connection = self.connection.lock();
        let found: Option<String> = connection
            .query_row(
                "SELECT id FROM canonical_orders WHERE idempotency_key=?1",
                params![idempotency_key],
                |row| row.get(0),
            )
            .optional()?;
        Ok(found.is_some())
    }

    pub fn record_position_control(
        &self,
        record: &PositionControlRecord,
        event: &HarnessEvent,
    ) -> Result<i64> {
        self.transact(event, |transaction| {
            transaction.execute(
                "INSERT INTO canonical_position_controls VALUES (?1,?2,?3,?4)",
                params![
                    record.position_id,
                    record.order_id,
                    serde_json::to_string(record)?,
                    record.created_by_event_id
                ],
            )?;
            Ok(())
        })
    }

    pub fn record_partial_close(
        &self,
        position: &PositionRecord,
        control: &PositionControlRecord,
        event: &HarnessEvent,
    ) -> Result<i64> {
        self.transact(event, |transaction| {
            let position_changed = transaction.execute(
                "UPDATE canonical_positions SET state=?2,last_event_id=?3 WHERE id=?1",
                params![position.id, position.state, position.last_event_id],
            )?;
            if position_changed != 1 {
                bail!("partial close references missing canonical position state");
            }
            // Position-control records are immutable. The adjusted control is versioned in
            // this append-only event and becomes authoritative during replay.
            let _ = control;
            Ok(())
        })
    }

    pub fn record_failure_state(
        &self,
        record: &LoopFailureState,
        event: &HarnessEvent,
    ) -> Result<i64> {
        self.transact(event, |transaction| {
            transaction.execute(
                "INSERT INTO canonical_loop_failure_history VALUES (?1,?2,?3,?4)",
                params![
                    event.id,
                    record.loop_id,
                    serde_json::to_string(record)?,
                    record.updated_at.to_rfc3339()
                ],
            )?;
            Ok(())
        })
    }

    pub fn open_position(&self, record: &PositionRecord, event: &HarnessEvent) -> Result<i64> {
        self.transact(event, |transaction| {
            transaction.execute(
                "INSERT INTO canonical_positions(id,run_id,loop_id,opened_by_execution_id,broker_position_id,direction,state,opened_at,closed_by_execution_id,closed_at,last_event_id) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,NULL,NULL,?9)",
                params![
                    record.id,
                    record.run_id,
                    record.loop_id,
                    record.opened_by_execution_id,
                    record.broker_position_id,
                    record.direction,
                    record.state,
                    record.opened_at.to_rfc3339(),
                    record.last_event_id
                ],
            )?;
            transaction.execute(
                "UPDATE canonical_loops SET state='jev2-hold' WHERE id=?1",
                params![record.loop_id],
            )?;
            Ok(())
        })
    }

    pub fn close_position(&self, record: &PositionRecord, event: &HarnessEvent) -> Result<i64> {
        self.transact(event, |transaction| {
            transaction.execute(
                "UPDATE canonical_positions SET state='closed', closed_by_execution_id=?2, closed_at=?3, last_event_id=?4 WHERE id=?1",
                params![
                    record.id,
                    record.closed_by_execution_id,
                    record.closed_at.map(|value| value.to_rfc3339()),
                    record.last_event_id
                ],
            )?;
            Ok(())
        })
    }

    pub fn transition_loop(&self, record: &LoopView, event: &HarnessEvent) -> Result<i64> {
        self.transact(event, |transaction| {
            transaction.execute(
                "UPDATE canonical_loops SET state=?2 WHERE id=?1",
                params![record.id, record.state],
            )?;
            Ok(())
        })
    }

    pub fn record_review(
        &self,
        record: &WorldModelReviewRecord,
        event: &HarnessEvent,
    ) -> Result<i64> {
        self.transact(event, |transaction| {
            transaction.execute(
                "INSERT INTO canonical_world_model_reviews VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
                params![
                    record.id,
                    record.run_id,
                    record.thesis_version_id,
                    record.context_version_id,
                    serde_json::to_string(&record.evidence_canonical_ids)?,
                    record.critique,
                    record.outcome,
                    record.created_by_event_id,
                    record.created_at.to_rfc3339()
                ],
            )?;
            Ok(())
        })
    }

    pub fn record_hypothesis_review(
        &self,
        record: &HypothesisReviewDecision,
        event: &HarnessEvent,
    ) -> Result<i64> {
        self.transact(event, |transaction| {
            transaction.execute(
                "INSERT INTO canonical_hypothesis_reviews VALUES (?1,?2,?3,?4,?5,?6,?7)",
                params![
                    record.id,
                    record.run_id,
                    record.hypothesis_id,
                    format!("{:?}", record.action),
                    serde_json::to_string(record)?,
                    record.created_by_event_id,
                    record.created_at.to_rfc3339()
                ],
            )?;
            Ok(())
        })
    }

    pub fn index_memory(&self, record: &MemoryDocument, event: &HarnessEvent) -> Result<i64> {
        let mut connection = self.connection.lock();
        Self::ensure_canonical_reference(
            &connection,
            &record.canonical_entity_type,
            &record.canonical_entity_id,
        )?;
        let transaction = connection.transaction()?;
        let sequence = Self::insert_event(&transaction, event)?;
        transaction.execute(
            "INSERT INTO search_documents VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
            params![
                record.id,
                record.run_id,
                record.canonical_entity_type,
                record.canonical_entity_id,
                record.canonical_event_id,
                record.text,
                record.indexed_by_event_id,
                record.created_at.to_rfc3339()
            ],
        )?;
        transaction.commit()?;
        self.mirror_event(sequence, event);
        Ok(sequence)
    }

    pub fn record_context_pool_record(
        &self,
        record: &ContextPoolRecord,
        event: &HarnessEvent,
    ) -> Result<i64> {
        self.transact(event, |transaction| {
            transaction.execute(
                "INSERT INTO canonical_context_pool_records VALUES (
                   ?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16
                 )",
                params![
                    record.id,
                    record.run_id,
                    record.title,
                    record.text,
                    record.source_class.as_str(),
                    record.trust_level.as_str(),
                    record.provenance_uri,
                    record.publisher,
                    record.observed_at.to_rfc3339(),
                    record.ingested_at.to_rfc3339(),
                    record.content_sha256,
                    record.canonical_entity_type,
                    record.canonical_entity_id,
                    record.canonical_event_id,
                    serde_json::to_string(&record.tags)?,
                    serde_json::to_string(&record.metadata)?,
                ],
            )?;
            Ok(())
        })
    }

    pub fn append_event(&self, event: &HarnessEvent) -> Result<i64> {
        self.transact(event, |_| Ok(()))
    }

    pub fn stop_loop(
        &self,
        loop_id: &str,
        stopped_at: DateTime<Utc>,
        event: &HarnessEvent,
    ) -> Result<i64> {
        self.transact(event, |transaction| {
            transaction.execute(
                "UPDATE canonical_loops SET state='stopped', stopped_at=?2 WHERE id=?1",
                params![loop_id, stopped_at.to_rfc3339()],
            )?;
            Ok(())
        })
    }

    pub fn stop_run(
        &self,
        run_id: &str,
        stopped_at: DateTime<Utc>,
        event: &HarnessEvent,
    ) -> Result<i64> {
        self.transact(event, |transaction| {
            transaction.execute(
                "UPDATE canonical_runs SET status='stopped', stopped_at=?2 WHERE id=?1",
                params![run_id, stopped_at.to_rfc3339()],
            )?;
            Ok(())
        })
    }

    pub fn stored_events(&self, run_id: &str) -> Result<Vec<StoredEvent>> {
        let connection = self.connection.lock();
        let mut statement = connection.prepare(
            "SELECT sequence,id,run_id,loop_id,kind,aggregate_type,aggregate_id,
                    causation_event_id,occurred_at,payload_json
             FROM canonical_events WHERE run_id=?1 ORDER BY sequence",
        )?;
        let rows = statement.query_map(params![run_id], |row| {
            let occurred_at: String = row.get(8)?;
            let payload_json: String = row.get(9)?;
            Ok(StoredEvent {
                sequence: row.get(0)?,
                event: HarnessEvent {
                    id: row.get(1)?,
                    run_id: row.get(2)?,
                    loop_id: row.get(3)?,
                    kind: row.get(4)?,
                    aggregate_type: row.get(5)?,
                    aggregate_id: row.get(6)?,
                    causation_event_id: row.get(7)?,
                    occurred_at: parse_time(&occurred_at)?,
                    payload: serde_json::from_str(&payload_json).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            9,
                            rusqlite::types::Type::Text,
                            Box::new(error),
                        )
                    })?,
                },
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn recent_continuation_events(
        &self,
        run_id: &str,
        limit: usize,
    ) -> Result<Vec<StoredEvent>> {
        let connection = self.connection.lock();
        let mut statement = connection.prepare(
            "SELECT sequence,id,run_id,loop_id,kind,aggregate_type,aggregate_id,
                    causation_event_id,occurred_at,payload_json
             FROM canonical_events
             WHERE run_id=?1 AND kind IN (
               'context_record_ingested','thesis_version_created','hypothesis_version_created',
               'hypothesis_contract_activated','hypothesis_contract_rejected',
               'jev1_decision_recorded','jev2_decision_recorded','order_evaluated',
               'execution_recorded','position_opened','position_closed','trade_outcome_recorded',
               'world_model_reviewed','world_model_stop_wrapup_recorded'
             )
             ORDER BY sequence DESC LIMIT ?2",
        )?;
        let rows = statement.query_map(params![run_id, limit as i64], |row| {
            let occurred_at: String = row.get(8)?;
            let payload_json: String = row.get(9)?;
            Ok(StoredEvent {
                sequence: row.get(0)?,
                event: HarnessEvent {
                    id: row.get(1)?,
                    run_id: row.get(2)?,
                    loop_id: row.get(3)?,
                    kind: row.get(4)?,
                    aggregate_type: row.get(5)?,
                    aggregate_id: row.get(6)?,
                    causation_event_id: row.get(7)?,
                    occurred_at: parse_time(&occurred_at)?,
                    payload: serde_json::from_str(&payload_json).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            9,
                            rusqlite::types::Type::Text,
                            Box::new(error),
                        )
                    })?,
                },
            })
        })?;
        let mut events = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        events.reverse();
        Ok(events)
    }

    pub fn events_for_run(&self, run_id: &str) -> Result<Vec<EventView>> {
        Ok(self
            .stored_events(run_id)?
            .into_iter()
            .map(|stored| {
                let detail = match stored.event.kind.as_str() {
                    "live_context_resolution_failed" => stored
                        .event
                        .payload
                        .get("error")
                        .and_then(|value| value.as_str())
                        .map(str::to_owned),
                    "run_steering_queued" => stored
                        .event
                        .payload
                        .get("instruction")
                        .and_then(|value| value.as_str())
                        .map(str::to_owned),
                    "run_steering_applied" => Some("Delivered to the Jev decision prompt".into()),
                    "market_service_state_changed" => stored
                        .event
                        .payload
                        .get("detail")
                        .and_then(|value| value.as_str())
                        .map(str::to_owned),
                    "harness_worker_failed" => {
                        let stage = stored
                            .event
                            .payload
                            .get("stage")
                            .and_then(|value| value.as_str())
                            .unwrap_or("worker");
                        stored
                            .event
                            .payload
                            .get("error")
                            .and_then(|value| value.as_str())
                            .map(|error| format!("{stage}: {error}"))
                    }
                    "broker_sync_failed" => stored
                        .event
                        .payload
                        .get("error")
                        .and_then(|value| value.as_str())
                        .map(str::to_owned),
                    "broker_sync_degraded" => stored
                        .event
                        .payload
                        .get("reason")
                        .or_else(|| stored.event.payload.get("error"))
                        .and_then(|value| value.as_str())
                        .map(str::to_owned),
                    "live_context_resolved" => stored
                        .event
                        .payload
                        .get("record")
                        .and_then(|record| record.get("qualityState"))
                        .and_then(|value| value.as_str())
                        .map(str::to_owned),
                    "order_evaluated" | "guardrail_evaluated" => {
                        let result = stored.event.payload.get("result");
                        let accepted = result
                            .and_then(|value| value.get("accepted"))
                            .and_then(serde_json::Value::as_bool);
                        let reason = result
                            .and_then(|value| value.get("reason"))
                            .and_then(serde_json::Value::as_str);
                        match (accepted, reason) {
                            (Some(true), Some(reason)) => Some(format!("Approved: {reason}")),
                            (Some(false), Some(reason)) => Some(format!("Rejected: {reason}")),
                            (Some(true), None) => Some("Approved".into()),
                            (Some(false), None) => Some("Rejected".into()),
                            _ => None,
                        }
                    }
                    "external_research_unavailable" => stored
                        .event
                        .payload
                        .get("status")
                        .and_then(|value| value.as_str())
                        .map(str::to_owned),
                    "broker_context_unavailable" => stored
                        .event
                        .payload
                        .get("error")
                        .and_then(|value| value.as_str())
                        .map(str::to_owned),
                    _ => None,
                };
                EventView {
                    sequence: stored.sequence,
                    id: stored.event.id,
                    kind: stored.event.kind,
                    aggregate_type: stored.event.aggregate_type,
                    aggregate_id: stored.event.aggregate_id,
                    loop_id: stored.event.loop_id,
                    causation_event_id: stored.event.causation_event_id,
                    occurred_at: stored.event.occurred_at,
                    summary: stored
                        .event
                        .payload
                        .get("summary")
                        .and_then(|value| value.as_str())
                        .unwrap_or("")
                        .to_owned(),
                    detail,
                }
            })
            .collect())
    }

    pub fn run_summaries(&self) -> Result<Vec<RunSummary>> {
        let connection = self.connection.lock();
        let mut statement = connection.prepare(
            "SELECT r.id,r.status,COALESCE(p.display_name,r.human_thesis),
                    r.started_at,r.stopped_at,COALESCE(p.archived,0)
             FROM canonical_runs r
             LEFT JOIN run_presentation p ON p.run_id=r.id
             ORDER BY r.started_at DESC",
        )?;
        let rows = statement.query_map([], |row| {
            let started_at: String = row.get(3)?;
            let stopped_at: Option<String> = row.get(4)?;
            Ok(RunSummary {
                run_id: row.get(0)?,
                status: row.get(1)?,
                thesis: row.get(2)?,
                archived: row.get::<_, i64>(5)? != 0,
                started_at: parse_time(&started_at)?,
                stopped_at: stopped_at.as_deref().map(parse_time).transpose()?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn rename_run(&self, run_id: &str, name: &str) -> Result<()> {
        let name = name.trim();
        if name.is_empty() || name.chars().count() > 120 {
            anyhow::bail!("experiment name must be between 1 and 120 characters");
        }
        let connection = self.connection.lock();
        let exists = connection
            .query_row(
                "SELECT 1 FROM canonical_runs WHERE id=?1",
                params![run_id],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if !exists {
            anyhow::bail!("experiment not found");
        }
        connection.execute(
            "INSERT INTO run_presentation(run_id,display_name) VALUES (?1,?2)
             ON CONFLICT(run_id) DO UPDATE SET display_name=excluded.display_name",
            params![run_id, name],
        )?;
        Ok(())
    }

    pub fn set_run_archived(&self, run_id: &str, archived: bool) -> Result<()> {
        let connection = self.connection.lock();
        let status: Option<String> = connection
            .query_row(
                "SELECT status FROM canonical_runs WHERE id=?1",
                params![run_id],
                |row| row.get(0),
            )
            .optional()?;
        match status.as_deref() {
            Some("stopped") => {}
            Some(_) => anyhow::bail!("stop the experiment before archiving it"),
            None => anyhow::bail!("experiment not found"),
        }
        connection.execute(
            "INSERT INTO run_presentation(run_id,archived) VALUES (?1,?2)
             ON CONFLICT(run_id) DO UPDATE SET archived=excluded.archived",
            params![run_id, archived],
        )?;
        Ok(())
    }

    pub fn pending_retrieval_events(&self, run_id: &str, limit: usize) -> Result<Vec<StoredEvent>> {
        let connection = self.connection.lock();
        let mut statement = connection.prepare(
            "SELECT e.sequence,e.id,e.run_id,e.loop_id,e.kind,e.aggregate_type,e.aggregate_id,
                    e.causation_event_id,e.occurred_at,e.payload_json
             FROM canonical_events e
             JOIN retrieval_outbox o ON o.event_id=e.id
             WHERE o.run_id=?1 AND o.status='pending'
             ORDER BY e.sequence LIMIT ?2",
        )?;
        let rows = statement.query_map(params![run_id, limit as i64], |row| {
            let occurred_at: String = row.get(8)?;
            let payload_json: String = row.get(9)?;
            Ok(StoredEvent {
                sequence: row.get(0)?,
                event: HarnessEvent {
                    id: row.get(1)?,
                    run_id: row.get(2)?,
                    loop_id: row.get(3)?,
                    kind: row.get(4)?,
                    aggregate_type: row.get(5)?,
                    aggregate_id: row.get(6)?,
                    causation_event_id: row.get(7)?,
                    occurred_at: parse_time(&occurred_at)?,
                    payload: serde_json::from_str(&payload_json).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            9,
                            rusqlite::types::Type::Text,
                            Box::new(error),
                        )
                    })?,
                },
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn mark_retrieval_events_indexed(&self, event_ids: &[String]) -> Result<()> {
        if event_ids.is_empty() {
            return Ok(());
        }
        let mut connection = self.connection.lock();
        let transaction = connection.transaction()?;
        for event_id in event_ids {
            transaction.execute(
                "UPDATE retrieval_outbox SET status='indexed', indexed_at=?2 WHERE event_id=?1",
                params![event_id, Utc::now().to_rfc3339()],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    fn transact<F>(&self, event: &HarnessEvent, projection: F) -> Result<i64>
    where
        F: FnOnce(&Transaction<'_>) -> Result<()>,
    {
        let mut connection = self.connection.lock();
        let transaction = connection.transaction()?;
        let sequence = Self::insert_event(&transaction, event)?;
        projection(&transaction)?;
        transaction.commit()?;
        self.mirror_event(sequence, event);
        Ok(sequence)
    }

    fn insert_event(transaction: &Transaction<'_>, event: &HarnessEvent) -> Result<i64> {
        transaction.execute(
            "INSERT INTO canonical_events(
               id,run_id,loop_id,kind,aggregate_type,aggregate_id,
               causation_event_id,occurred_at,payload_json
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            params![
                event.id,
                event.run_id,
                event.loop_id,
                event.kind,
                event.aggregate_type,
                event.aggregate_id,
                event.causation_event_id,
                event.occurred_at.to_rfc3339(),
                serde_json::to_string(&event.payload)?
            ],
        )?;
        let sequence = transaction.last_insert_rowid();
        transaction.execute(
            "INSERT INTO retrieval_outbox(event_id,run_id,status) VALUES (?1,?2,'pending')",
            params![event.id, event.run_id],
        )?;
        Ok(sequence)
    }

    fn ensure_canonical_reference(
        connection: &Connection,
        entity_type: &str,
        entity_id: &str,
    ) -> Result<()> {
        let table = match entity_type {
            "decision" => "canonical_decisions",
            "execution" => "canonical_executions",
            "position" => "canonical_positions",
            "review" => "canonical_world_model_reviews",
            "thesis_version" => "canonical_thesis_versions",
            "context_version" => "canonical_context_versions",
            other => bail!("unsupported canonical memory entity type: {other}"),
        };
        let sql = format!("SELECT id FROM {table} WHERE id=?1");
        let found: Option<String> = connection
            .query_row(&sql, params![entity_id], |row| row.get(0))
            .optional()?;
        if found.is_none() {
            bail!("indexed memory must reference an existing canonical {entity_type} record");
        }
        Ok(())
    }

    fn mirror_event(&self, sequence: i64, event: &HarnessEvent) {
        let line = serde_json::json!({ "sequence": sequence, "event": event });
        if let Ok(mut file) = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.event_log_path)
        {
            let _ = writeln!(file, "{}", line);
            let _ = file.flush();
        }
    }
}

impl SearchableContext for CanonicalStore {
    fn search(&self, query: &str, limit: usize) -> Result<Vec<SearchHit>> {
        let connection = self.connection.lock();
        let mut hits = {
            let mut statement = connection.prepare(
                "SELECT id,canonical_entity_type,canonical_entity_id,canonical_event_id,text,created_at
                 FROM search_documents
                 WHERE lower(text) LIKE '%' || lower(?1) || '%'
                 ORDER BY created_at DESC LIMIT ?2",
            )?;
            let rows = statement.query_map(params![query, limit as i64], |row| {
                let timestamp: String = row.get(5)?;
                Ok(SearchHit {
                    document_id: row.get(0)?,
                    canonical_entity_type: row.get(1)?,
                    canonical_entity_id: row.get(2)?,
                    canonical_event_id: row.get(3)?,
                    text: row.get(4)?,
                    created_at: parse_time(&timestamp)?,
                })
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };

        // The text index is a rebuildable projection. Keep exact execution
        // references discoverable from the immutable event log if indexing is
        // disabled, delayed, or has been cleared.
        if hits.len() < limit {
            let mut statement = connection.prepare(
                "SELECT id,payload_json FROM canonical_events
                 WHERE kind IN ('execution_recorded','broker_position_imported')
                 ORDER BY sequence DESC",
            )?;
            let rows = statement.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            let query = query.trim().to_lowercase();
            for row in rows {
                let (event_id, payload) = row?;
                let payload: serde_json::Value = serde_json::from_str(&payload)?;
                let Some(record) = payload.get("record") else {
                    continue;
                };
                let execution: ExecutionReceipt = serde_json::from_value(record.clone())?;
                if hits.iter().any(|hit| {
                    hit.canonical_entity_type == "execution"
                        && hit.canonical_entity_id == execution.execution_id
                }) {
                    continue;
                }
                let text = format!(
                    "Decision {} caused execution {}: {:?} {} quantity {} status {}.",
                    execution.caused_by_decision_id,
                    execution.execution_id,
                    execution.action,
                    execution.execution_kind,
                    execution.filled_quantity,
                    execution.status
                );
                if !text.to_lowercase().contains(&query) {
                    continue;
                }
                hits.push(SearchHit {
                    document_id: format!("canonical-execution:{}", execution.execution_id),
                    canonical_entity_type: "execution".into(),
                    canonical_entity_id: execution.execution_id,
                    canonical_event_id: if execution.created_by_event_id.is_empty() {
                        event_id
                    } else {
                        execution.created_by_event_id
                    },
                    text,
                    created_at: execution.executed_at,
                });
                if hits.len() >= limit {
                    break;
                }
            }
        }
        hits.sort_by(|left, right| right.created_at.cmp(&left.created_at));
        hits.truncate(limit);
        Ok(hits)
    }
}

fn parse_time(value: &str) -> rusqlite::Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })
}
