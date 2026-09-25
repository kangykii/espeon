use crate::domain::*;
use anyhow::Result;

pub trait WorldModel: Send + Sync {
    fn formulate(
        &self,
        run_id: &str,
        human_thesis: &str,
        retriever: Option<&dyn ContextRetriever>,
    ) -> Result<WorldModelStartupOutput>;
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
    fn reference_price(&self, instrument: &str) -> Result<Option<f64>>;
    fn reconcile(&self) -> Result<BrokerSnapshot>;
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
