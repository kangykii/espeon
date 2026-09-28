use crate::domain::*;
use anyhow::{bail, Result};
use chrono::{DateTime, Utc};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct BrokerRiskSnapshot {
    pub account_id: String,
    pub environment: String,
    pub equity: f64,
    pub free_margin: f64,
    pub deposit_asset_id: String,
    /// Supported fiat/crypto currency code shared by equity, free margin,
    /// account exposure, and every quote-to-deposit multiplier in this snapshot.
    pub deposit_currency_code: String,
    pub observed_at: DateTime<Utc>,
    /// Broker-wide gross notional exposure in deposit currency, including all
    /// open positions and working entry orders for the selected account.
    pub account_open_exposure: f64,
    /// Conservative conversion multipliers from each instrument's quote asset
    /// into the account deposit asset.
    pub quote_to_deposit: HashMap<String, f64>,
}

#[derive(Debug, Clone)]
pub struct BrokerAccountIdentity {
    pub account_id: String,
    pub environment: String,
}

pub trait WorldModel: Send + Sync {
    fn formulate(
        &self,
        run_id: &str,
        human_thesis: &str,
        retriever: Option<&dyn ContextRetriever>,
    ) -> Result<WorldModelStartupOutput>;
    fn formulate_with_continuation(
        &self,
        run_id: &str,
        human_thesis: &str,
        retriever: Option<&dyn ContextRetriever>,
        _continuation_context: Option<&serde_json::Value>,
    ) -> Result<WorldModelStartupOutput> {
        self.formulate(run_id, human_thesis, retriever)
    }
    fn repair_contract(
        &self,
        _user_objective: &str,
        _rejected_proposal: &crate::contracts::HypothesisContractDraft,
        _validation_error: &str,
        _escalate: bool,
        _retrieval_trace: Option<&RetrievalTrace>,
    ) -> Result<crate::contracts::HypothesisContractDraft> {
        anyhow::bail!("world-model adapter does not support bounded contract repair")
    }
    fn review_hypothesis(
        &self,
        package: &WorldModelReviewPackage,
    ) -> Result<HypothesisReviewDecision>;
    fn critique(
        &self,
        thesis: &ThesisVersion,
        context: &ContextVersion,
        evidence: &str,
    ) -> Result<String>;
    fn research(
        &self,
        retriever: &dyn ContextRetriever,
        request: &RetrievalRequest,
    ) -> Result<RetrievalTrace>;
}

pub trait JevEngine: Send + Sync {
    fn decide_entry(&self, state: &ResolvedJevState, question: &str) -> Result<Jev1Decision>;
    fn manage_position(&self, state: &ResolvedJevState, question: &str) -> Result<Jev2Decision>;
}

pub trait ContextResolver: Send + Sync {
    fn resolve(
        &self,
        hypothesis: &HypothesisDefinition,
        thesis: &ThesisVersion,
        context: &ContextVersion,
        position: Option<&PositionRecord>,
    ) -> Result<ResolvedJevState>;
}

pub trait MarketDataProvider: Send + Sync {
    fn snapshot(&self, request: &MarketDataRequest) -> Result<MarketDataSnapshot>;
    fn refresh(&self, _request: &MarketDataRequest) -> Result<()> {
        Ok(())
    }
    fn is_simulated(&self) -> bool {
        false
    }
    fn drain_health_events(&self) -> Vec<(String, String)> {
        Vec::new()
    }
    fn health_statuses(&self) -> Vec<IntegrationStatus> {
        Vec::new()
    }
}

pub trait ExecutionBroker: Send + Sync {
    fn execute(&self, request: &TradeRequest) -> Result<ExecutionReceipt>;
    fn close(
        &self,
        position: &PositionRecord,
        order: &OrderRecord,
        caused_by_decision_id: &str,
        execution_event_id: &str,
    ) -> Result<ExecutionReceipt>;
    /// Resolve an order whose submission result may have been lost. Returning
    /// a terminal status is required before a replacement close is submitted.
    fn resolve_order_status(
        &self,
        _order: &OrderRecord,
        _broker_position_id: Option<&str>,
    ) -> Result<ExecutionReceipt> {
        bail!("broker adapter cannot resolve an uncertain order status")
    }
    fn reference_price(&self, instrument: &str) -> Result<Option<f64>>;
    fn supports_instrument(&self, instrument: &str) -> Result<bool> {
        Ok(self.reference_price(instrument)?.is_some())
    }
    fn reconcile(&self) -> Result<BrokerSnapshot>;
    /// Static `paperAccountCapital` is permitted only for an explicitly
    /// simulated execution adapter. Real and unavailable brokers fail closed
    /// until they provide a fresh account risk snapshot.
    fn allows_static_risk_policy(&self) -> bool {
        false
    }
    fn risk_snapshot(&self, _instruments: &[String]) -> Result<Option<BrokerRiskSnapshot>> {
        Ok(None)
    }
    fn risk_account_identity(&self) -> Option<BrokerAccountIdentity> {
        None
    }
    /// Optional fixed entry size for explicitly configured Demo instruments.
    /// Live brokers must not return a fallback quantity.
    fn demo_fixed_entry_quantity(&self, _instrument: &str) -> Option<f64> {
        None
    }
    /// Install one human-verified symbol minimum/increment for the next live
    /// order when broker metadata is unavailable. Adapters may reject this.
    fn approve_manual_entry_limits(
        &self,
        _instrument: &str,
        _minimum: f64,
        _step: f64,
    ) -> Result<()> {
        bail!("broker does not support human-verified manual entry limits")
    }
    /// Allow exactly one human-approved Demo entry without broker volume
    /// metadata. Live accounts must never use this fallback.
    fn approve_demo_entry_without_volume_metadata(&self, _instrument: &str) -> Result<()> {
        bail!("broker does not support an unverified Demo entry")
    }
    fn clear_demo_entry_without_volume_metadata(&self, _instrument: &str) {}
}

pub trait CapitalAllocator: Send + Sync {
    fn allocation_for(&self, active_loop_count: usize) -> f64;
}

pub trait SearchableContext: Send + Sync {
    fn search(&self, query: &str, limit: usize) -> Result<Vec<SearchHit>>;
}

pub trait ContextRetriever: Send + Sync {
    fn retrieve(&self, request: &RetrievalRequest) -> Result<RetrievalTrace>;
}
