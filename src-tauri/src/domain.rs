use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub const ORDER_FILL_QUANTITY_TOLERANCE: f64 = 1e-7;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThesisVersion {
    pub id: String,
    pub run_id: String,
    pub version: i64,
    pub thesis: String,
    pub provenance: String,
    pub created_by_event_id: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextDefinition {
    pub id: String,
    pub run_id: String,
    pub name: String,
    pub description: String,
    pub created_by_event_id: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextItem {
    pub source: String,
    pub source_id: String,
    pub observed_at: DateTime<Utc>,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextVersion {
    pub id: String,
    pub definition_id: String,
    pub run_id: String,
    pub version: i64,
    pub items: Vec<ContextItem>,
    pub created_by_event_id: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TimeframeDefinition {
    pub label: String,
    pub horizon_minutes: u64,
    pub source: String,
    pub rationale: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HypothesisReviewRules {
    pub support_evidence: Vec<String>,
    pub weaken_evidence: Vec<String>,
    pub invalidate_evidence: Vec<String>,
    pub modify_when: Vec<String>,
    pub split_when: Vec<String>,
    pub stop_when: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HypothesisDefinition {
    pub id: String,
    pub root_hypothesis_id: String,
    pub run_id: String,
    pub version: i64,
    pub parent_hypothesis_id: Option<String>,
    pub original_prompt: String,
    pub instruments: Vec<String>,
    pub strategy_mechanism: String,
    pub timeframe: TimeframeDefinition,
    pub deterministic_context: Vec<String>,
    #[serde(default)]
    pub live_context_spec: Option<LiveContextSpec>,
    pub jev_question: String,
    pub review_rules: HypothesisReviewRules,
    pub thesis_version_id: String,
    pub context_version_id: String,
    pub status: String,
    /// Absent in historical runs. New loops require a validated ACTIVE contract.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contract: Option<crate::contracts::HypothesisContract>,
    pub created_by_event_id: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MarketDataPeriod {
    M1,
    M5,
    M15,
    M30,
    H1,
    H4,
    D1,
}

impl MarketDataPeriod {
    pub fn seconds(&self) -> i64 {
        match self {
            Self::M1 => 60,
            Self::M5 => 300,
            Self::M15 => 900,
            Self::M30 => 1_800,
            Self::H1 => 3_600,
            Self::H4 => 14_400,
            Self::D1 => 86_400,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LiveValueType {
    Number,
    Boolean,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum LiveExpression {
    Constant {
        value: f64,
    },
    CurrentBid,
    CurrentAsk,
    CurrentMid,
    CurrentSpread,
    Series {
        period: MarketDataPeriod,
        column: String,
        #[serde(default)]
        lag: usize,
    },
    Add {
        args: Vec<LiveExpression>,
    },
    Subtract {
        left: Box<LiveExpression>,
        right: Box<LiveExpression>,
    },
    Multiply {
        args: Vec<LiveExpression>,
    },
    Divide {
        numerator: Box<LiveExpression>,
        denominator: Box<LiveExpression>,
    },
    GreaterThan {
        left: Box<LiveExpression>,
        right: Box<LiveExpression>,
    },
    LessThan {
        left: Box<LiveExpression>,
        right: Box<LiveExpression>,
    },
    And {
        args: Vec<LiveExpression>,
    },
    Or {
        args: Vec<LiveExpression>,
    },
    Not {
        value: Box<LiveExpression>,
    },
    Change {
        period: MarketDataPeriod,
        column: String,
        lag: usize,
    },
    PercentChange {
        period: MarketDataPeriod,
        column: String,
        lag: usize,
    },
    RollingMin {
        period: MarketDataPeriod,
        column: String,
        window: usize,
    },
    RollingMax {
        period: MarketDataPeriod,
        column: String,
        window: usize,
    },
    RollingMean {
        period: MarketDataPeriod,
        column: String,
        window: usize,
    },
    RollingSum {
        period: MarketDataPeriod,
        column: String,
        window: usize,
    },
    RollingStdDev {
        period: MarketDataPeriod,
        column: String,
        window: usize,
    },
    Ema {
        period: MarketDataPeriod,
        column: String,
        window: usize,
    },
    Rsi {
        period: MarketDataPeriod,
        window: usize,
    },
    TrueRange {
        period: MarketDataPeriod,
        lag: usize,
    },
    Atr {
        period: MarketDataPeriod,
        window: usize,
    },
    Crossover {
        left: Box<LiveExpression>,
        right: Box<LiveExpression>,
        period: MarketDataPeriod,
    },
    CrossUnder {
        left: Box<LiveExpression>,
        right: Box<LiveExpression>,
        period: MarketDataPeriod,
    },
    RangePosition {
        value: Box<LiveExpression>,
        low: Box<LiveExpression>,
        high: Box<LiveExpression>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LiveContextFieldSpec {
    pub field_id: String,
    pub label: String,
    pub value_type: LiveValueType,
    pub required: bool,
    pub maximum_age_seconds: u64,
    pub expression: LiveExpression,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LiveContextSpec {
    pub id: String,
    pub version: i64,
    pub instrument: String,
    #[serde(default)]
    pub series_sources: std::collections::HashMap<String, String>,
    pub fields: Vec<LiveContextFieldSpec>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuoteSnapshot {
    pub id: String,
    pub symbol: String,
    pub bid: f64,
    pub ask: f64,
    pub mid: f64,
    pub spread: f64,
    pub source_timestamp: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
    pub provenance: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Candle {
    pub id: String,
    pub symbol: String,
    pub period: MarketDataPeriod,
    pub open_time: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub tick_volume: u64,
    #[serde(default)]
    pub provider_volume: Option<f64>,
    #[serde(default)]
    pub volume_kind: Option<String>,
    #[serde(default)]
    pub received_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub source_observation_ids: Vec<String>,
    pub closed: bool,
    pub provenance: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SeriesRequirement {
    pub period: MarketDataPeriod,
    pub bars: usize,
    #[serde(default)]
    pub source: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketDataRequest {
    pub instrument: String,
    pub series: Vec<SeriesRequirement>,
    pub quote_max_age_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketDataSnapshot {
    pub quote: QuoteSnapshot,
    pub candles: Vec<Candle>,
    pub captured_at: DateTime<Utc>,
    #[serde(default)]
    pub quality_state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedLiveField {
    pub field_id: String,
    pub label: String,
    pub value: Value,
    pub value_type: LiveValueType,
    pub formula: LiveExpression,
    pub observed_at: DateTime<Utc>,
    pub source_observation_ids: Vec<String>,
    pub provenance: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedContextSnapshot {
    pub id: String,
    pub run_id: String,
    pub loop_id: String,
    pub thesis_version_id: String,
    pub context_version_id: String,
    pub live_context_spec_id: String,
    pub live_context_spec_version: i64,
    pub quote: QuoteSnapshot,
    pub candles: Vec<Candle>,
    pub fields: Vec<ResolvedLiveField>,
    pub freshness_state: String,
    #[serde(default)]
    pub quality_state: String,
    pub resolved_at: DateTime<Utc>,
    pub created_by_event_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoopCadence {
    pub loop_id: String,
    pub hypothesis_id: String,
    pub timeframe_horizon_minutes: u64,
    pub jev1_interval_seconds: u64,
    pub jev2_interval_seconds: u64,
    pub mapping_rule: String,
    pub created_by_event_id: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HypothesisAction {
    Keep,
    Modify,
    Split,
    Stop,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HypothesisReviewRequest {
    pub run_id: String,
    pub hypothesis_id: String,
    pub evidence_query: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AutonomousReviewTriggerKind {
    LossStreak,
    NoTradeStreak,
    PeriodicTradeCount,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AutonomousReviewTriggerRecord {
    pub trigger_id: String,
    pub run_id: String,
    pub loop_id: String,
    pub hypothesis_id: String,
    pub kinds: Vec<AutonomousReviewTriggerKind>,
    pub status: String,
    pub attempts: u32,
    pub no_trade_streak: usize,
    pub no_trade_threshold: usize,
    pub consecutive_losses: usize,
    pub loss_threshold: usize,
    pub completed_trades_since_review: usize,
    pub periodic_trade_threshold: usize,
    pub last_decision_id: Option<String>,
    pub last_trade_outcome_id: Option<String>,
    pub review_id: Option<String>,
    pub next_retry_at: Option<DateTime<Utc>>,
    pub created_by_event_id: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradeOutcomeClassification {
    Win,
    Loss,
    Breakeven,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TradeOutcomeRecord {
    pub id: String,
    pub run_id: String,
    pub loop_id: String,
    pub position_id: String,
    pub thesis_version_id: String,
    pub context_version_id: String,
    pub instrument: String,
    pub direction: String,
    pub quantity: f64,
    pub entry_notional: Option<f64>,
    pub exit_notional: Option<f64>,
    pub gross_realized_pnl: Option<f64>,
    pub gross_return: Option<f64>,
    pub fees_available: bool,
    pub classification: TradeOutcomeClassification,
    pub entry_execution_ids: Vec<String>,
    pub exit_execution_ids: Vec<String>,
    pub created_by_event_id: String,
    pub completed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HypothesisReviewDecision {
    pub id: String,
    pub run_id: String,
    pub hypothesis_id: String,
    pub action: HypothesisAction,
    pub rationale: String,
    #[serde(default)]
    pub diagnosis: String,
    #[serde(default)]
    pub problem_severity: String,
    #[serde(default)]
    pub continuation_rationale: String,
    #[serde(default)]
    pub decision_confidence: f64,
    pub evidence_canonical_ids: Vec<String>,
    pub proposed_mechanism: Option<String>,
    pub proposed_timeframe: Option<TimeframeDefinition>,
    pub candidate_hypothesis: Option<CandidateHypothesis>,
    /// Full DRAFT proposal for MODIFY/SPLIT. Legacy review events omit this.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proposed_contract: Option<crate::contracts::HypothesisContractDraft>,
    #[serde(default)]
    pub routing: Option<WorldModelRoutingMetadata>,
    #[serde(default)]
    pub web_evidence: Vec<WebEvidenceRecord>,
    pub created_by_event_id: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct WorldModelRoutingMetadata {
    pub provider: String,
    pub base_model: String,
    pub selected_model: String,
    pub escalated: bool,
    pub escalation_reasons: Vec<String>,
    pub base_confidence: f64,
    pub request_ids: Vec<String>,
    pub internet_research_used: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WebEvidenceRecord {
    pub id: String,
    pub url: String,
    pub title: String,
    pub publisher: String,
    pub claim: String,
    pub publication_date: Option<DateTime<Utc>>,
    pub event_date: Option<DateTime<Utc>>,
    pub retrieved_at: DateTime<Utc>,
    pub date_verified: bool,
    pub recency_required: bool,
    pub used_as_primary: bool,
    pub primary_eligible: bool,
    pub recency_reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CandidateHypothesis {
    pub instruments: Vec<String>,
    pub strategy_mechanism: String,
    pub timeframe: TimeframeDefinition,
    pub rationale: String,
    pub competing_with_hypothesis_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceRelationship {
    Supporting,
    Contradictory,
    Related,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReviewEvidenceItem {
    pub relationship: EvidenceRelationship,
    pub hit: RetrievalHit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorldModelReviewPackage {
    pub id: String,
    pub run_id: String,
    pub evidence_query: String,
    pub current_hypothesis: HypothesisDefinition,
    pub current_thesis: ThesisVersion,
    pub current_context: ContextVersion,
    pub current_loop: LoopView,
    pub recent_jev_decisions: Vec<DecisionRecord>,
    pub recent_executions: Vec<ExecutionReceipt>,
    pub current_positions: Vec<PositionRecord>,
    pub prior_reviews: Vec<HypothesisReviewDecision>,
    pub prior_spawned_hypotheses: Vec<HypothesisDefinition>,
    pub historical_retrieval: Option<RetrievalTrace>,
    pub evidence: Vec<ReviewEvidenceItem>,
    pub exact_canonical_ids: Vec<String>,
    #[serde(default)]
    pub autonomous_trigger: Option<AutonomousReviewTriggerRecord>,
    #[serde(default)]
    pub recent_trade_outcomes: Vec<TradeOutcomeRecord>,
    pub created_by_event_id: String,
    pub assembled_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct WorldModelStartupOutput {
    pub contract: crate::contracts::HypothesisContractDraft,
    pub retrieval_trace: Option<RetrievalTrace>,
    pub web_research_unavailable: bool,
    pub broker_context_unavailable: Option<BrokerContextFailure>,
}

#[derive(Debug, Clone)]
pub struct BrokerContextFailure {
    pub requested_capabilities: Vec<String>,
    pub error: String,
    pub recovery_reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Jev1Action {
    Long,
    Short,
    NoTrade,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Jev2Action {
    Hold,
    Sell,
    BuyMore,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Jev1Decision {
    pub action: Jev1Action,
    pub confidence: f64,
    pub rationale: String,
    pub inference: JevInferenceMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Jev2Decision {
    pub action: Jev2Action,
    pub confidence: f64,
    pub rationale: String,
    pub inference: JevInferenceMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct JevUsage {
    #[serde(alias = "input_tokens")]
    pub input_tokens: u64,
    #[serde(alias = "output_tokens")]
    pub output_tokens: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct JevInferenceMetadata {
    pub provider: String,
    pub requested_model: String,
    pub returned_model: String,
    pub question_id: String,
    pub answer_type: String,
    pub probabilities: BTreeMap<String, f64>,
    pub usage: JevUsage,
    pub request_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedContextField {
    pub name: String,
    pub value: Value,
    pub source_id: String,
    pub source_uri: String,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedJevState {
    pub thesis: String,
    pub thesis_version_id: String,
    pub context_version_id: String,
    pub instruments: Vec<String>,
    pub timeframe: TimeframeDefinition,
    pub context: Vec<ResolvedContextField>,
    pub market_state: Value,
    pub position: Option<Value>,
    #[serde(default)]
    pub live_context_snapshot: Option<ResolvedContextSnapshot>,
    pub resolved_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DecisionRecord {
    pub id: String,
    pub run_id: String,
    pub loop_id: String,
    pub stage: String,
    pub thesis_version_id: String,
    pub context_version_id: String,
    pub position_id: Option<String>,
    pub action: String,
    pub confidence: f64,
    pub rationale: String,
    pub inference: JevInferenceMetadata,
    pub resolved_state: ResolvedJevState,
    pub created_by_event_id: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TradeRequest {
    pub run_id: String,
    pub loop_id: String,
    pub decision_id: String,
    pub execution_event_id: String,
    pub action: Jev1Action,
    pub confidence: f64,
    pub order: OrderRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GuardrailDecision {
    pub accepted: bool,
    pub effective_action: Jev1Action,
    pub reason: String,
    pub order_id: String,
    pub checks: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RiskPolicyConfig {
    pub paper_account_capital: f64,
    #[serde(default = "default_paper_account_currency")]
    pub paper_account_currency: String,
    pub max_total_exposure_fraction: f64,
    pub max_position_fraction_of_loop: f64,
    pub minimum_order_notional: f64,
    pub quantity_step: f64,
    pub stop_loss_basis_points: u64,
    pub max_signal_age_seconds: u64,
    pub duplicate_order_window_seconds: u64,
    pub max_open_positions_per_loop: usize,
    pub max_consecutive_failures: u32,
    pub retry_base_seconds: u64,
    pub paper_reference_price: f64,
}

fn default_paper_account_currency() -> String {
    "USD".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[serde(rename_all = "camelCase")]
pub struct AutonomousReviewPolicy {
    pub enabled: bool,
    pub no_trade_horizon_mode: String,
    pub no_trade_min_decisions: usize,
    pub no_trade_max_decisions: usize,
    pub consecutive_loss_threshold: usize,
    pub periodic_trade_threshold: usize,
    pub max_active_loops: usize,
    pub split_confidence_threshold: f64,
    pub retry_attempts: u32,
    pub retry_base_seconds: u64,
}

impl Default for AutonomousReviewPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            no_trade_horizon_mode: "hypothesis_horizon".into(),
            no_trade_min_decisions: 6,
            no_trade_max_decisions: 20,
            consecutive_loss_threshold: 3,
            periodic_trade_threshold: 10,
            max_active_loops: 3,
            split_confidence_threshold: 0.80,
            retry_attempts: 3,
            retry_base_seconds: 5,
        }
    }
}

impl Default for RiskPolicyConfig {
    fn default() -> Self {
        Self {
            paper_account_capital: 100_000.0,
            paper_account_currency: "USD".into(),
            max_total_exposure_fraction: 0.8,
            max_position_fraction_of_loop: 0.5,
            minimum_order_notional: 25.0,
            quantity_step: 0.0001,
            stop_loss_basis_points: 200,
            max_signal_age_seconds: 300,
            duplicate_order_window_seconds: 300,
            max_open_positions_per_loop: 2,
            max_consecutive_failures: 3,
            retry_base_seconds: 5,
            paper_reference_price: 100.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderRecord {
    pub id: String,
    pub run_id: String,
    pub loop_id: String,
    pub decision_id: String,
    pub idempotency_key: String,
    pub order_kind: String,
    pub instrument: String,
    pub side: String,
    pub quantity: f64,
    pub reference_price: f64,
    pub notional: f64,
    pub stop_loss_price: Option<f64>,
    pub signal_at: DateTime<Utc>,
    pub status: String,
    pub rejection_reasons: Vec<String>,
    pub created_by_event_id: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PositionControlRecord {
    pub position_id: String,
    pub order_id: String,
    pub instrument: String,
    pub quantity: f64,
    pub entry_price: f64,
    pub notional: f64,
    pub stop_loss_price: f64,
    pub created_by_event_id: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoopFailureState {
    pub loop_id: String,
    pub consecutive_failures: u32,
    pub next_retry_at: Option<DateTime<Utc>>,
    pub paused: bool,
    pub last_error: Option<String>,
    pub updated_by_event_id: String,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionReceipt {
    pub execution_id: String,
    pub run_id: String,
    pub loop_id: String,
    pub caused_by_decision_id: String,
    pub action: Jev1Action,
    pub execution_kind: String,
    pub status: String,
    pub broker_reference: String,
    pub broker_position_id: Option<String>,
    pub filled_quantity: f64,
    pub average_price: Option<f64>,
    pub rejection_reason: Option<String>,
    #[serde(default)]
    pub raw_fix_report: Option<Vec<(String, String)>>,
    pub created_by_event_id: String,
    pub executed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PositionRecord {
    pub id: String,
    pub run_id: String,
    pub loop_id: String,
    pub opened_by_execution_id: String,
    pub broker_position_id: Option<String>,
    pub direction: String,
    pub state: String,
    pub opened_at: DateTime<Utc>,
    pub closed_by_execution_id: Option<String>,
    pub closed_at: Option<DateTime<Utc>>,
    pub last_event_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerPosition {
    pub broker_position_id: String,
    pub instrument: String,
    pub side: String,
    pub quantity: f64,
    pub average_price: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerSnapshot {
    pub adapter: String,
    pub connected: bool,
    pub complete: bool,
    pub positions: Vec<BrokerPosition>,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoopView {
    pub id: String,
    pub run_id: String,
    pub parent_loop_id: Option<String>,
    pub parent_thesis_version_id: Option<String>,
    pub hypothesis_id: String,
    pub thesis_version_id: String,
    pub context_version_id: String,
    pub thesis_version: i64,
    pub context_version: i64,
    pub state: String,
    pub allocated_fraction: f64,
    pub created_by_event_id: String,
    pub created_at: DateTime<Utc>,
    pub stopped_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AllocationEntry {
    pub loop_id: String,
    pub fraction: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CapitalAllocationRecord {
    pub id: String,
    pub run_id: String,
    pub reason: String,
    pub allocations: Vec<AllocationEntry>,
    pub created_by_event_id: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorldModelReviewRecord {
    pub id: String,
    pub run_id: String,
    pub thesis_version_id: String,
    pub context_version_id: String,
    pub evidence_canonical_ids: Vec<String>,
    pub critique: String,
    pub outcome: String,
    pub created_by_event_id: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryDocument {
    pub id: String,
    pub run_id: String,
    pub canonical_entity_type: String,
    pub canonical_entity_id: String,
    pub canonical_event_id: String,
    pub text: String,
    pub indexed_by_event_id: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextSourceClass {
    LiveOfficial,
    HistoricalRecorded,
    ExternalWebResearch,
    InternalCanonical,
}

impl ContextSourceClass {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::LiveOfficial => "live_official",
            Self::HistoricalRecorded => "historical_recorded",
            Self::ExternalWebResearch => "external_web_research",
            Self::InternalCanonical => "internal_canonical",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrustLevel {
    Verified,
    Internal,
    Untrusted,
}

impl TrustLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Verified => "verified",
            Self::Internal => "internal",
            Self::Untrusted => "untrusted",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextPoolRecord {
    pub id: String,
    pub run_id: String,
    pub title: String,
    pub text: String,
    pub source_class: ContextSourceClass,
    pub trust_level: TrustLevel,
    pub provenance_uri: String,
    pub publisher: String,
    pub observed_at: DateTime<Utc>,
    pub ingested_at: DateTime<Utc>,
    pub content_sha256: String,
    pub canonical_entity_type: String,
    pub canonical_entity_id: String,
    pub canonical_event_id: String,
    pub tags: Vec<String>,
    pub metadata: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextIngestRequest {
    pub run_id: String,
    pub title: String,
    pub text: String,
    pub source_class: ContextSourceClass,
    pub trust_level: TrustLevel,
    pub provenance_uri: String,
    pub publisher: String,
    pub tags: Vec<String>,
    pub metadata: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchMode {
    Hybrid,
    Semantic,
    Keyword,
    Exact,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct RetrievalFilters {
    pub run_id: Option<String>,
    pub source_class: Option<String>,
    pub trust_level: Option<String>,
    pub canonical_entity_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetrievalRequest {
    pub question: String,
    pub required_source_classes: Vec<String>,
    pub exact_canonical_ids: Vec<String>,
    pub limit: usize,
    pub max_rounds: usize,
    pub filters: RetrievalFilters,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RetrievalHit {
    pub point_id: String,
    pub score: f64,
    pub id: String,
    pub run_id: String,
    pub title: String,
    pub text: String,
    pub source_class: String,
    pub trust_level: String,
    pub provenance_uri: String,
    pub publisher: String,
    pub observed_at: String,
    pub ingested_at: String,
    pub content_sha256: String,
    pub canonical_entity_type: String,
    pub canonical_entity_id: String,
    pub canonical_event_id: String,
    pub tags: Vec<String>,
    pub metadata: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetrievalStep {
    pub round: usize,
    pub action: String,
    pub query: String,
    pub mode: SearchMode,
    pub filter: RetrievalFilters,
    pub result_ids: Vec<String>,
    pub finding: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetrievalTrace {
    pub question: String,
    pub sufficient: bool,
    pub missing_source_classes: Vec<String>,
    pub steps: Vec<RetrievalStep>,
    pub hits: Vec<RetrievalHit>,
    pub final_action: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventView {
    pub sequence: i64,
    pub id: String,
    pub kind: String,
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub loop_id: Option<String>,
    pub causation_event_id: Option<String>,
    pub occurred_at: DateTime<Utc>,
    pub summary: String,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunSnapshot {
    pub run_id: String,
    pub status: String,
    pub thesis: String,
    pub hypotheses: Vec<HypothesisDefinition>,
    pub cadences: Vec<LoopCadence>,
    pub loops: Vec<LoopView>,
    pub positions: Vec<PositionRecord>,
    pub events: Vec<EventView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunSummary {
    pub run_id: String,
    pub status: String,
    pub thesis: String,
    pub archived: bool,
    pub started_at: DateTime<Utc>,
    pub stopped_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IntegrationStatus {
    pub id: String,
    pub label: String,
    pub state: String,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceSnapshot {
    pub active_runs: Vec<RunSnapshot>,
    pub run_history: Vec<RunSummary>,
    pub integrations: Vec<IntegrationStatus>,
    pub hydrated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchHit {
    pub document_id: String,
    pub canonical_entity_type: String,
    pub canonical_entity_id: String,
    pub canonical_event_id: String,
    pub text: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HarnessEvent {
    pub id: String,
    pub run_id: String,
    pub loop_id: Option<String>,
    pub kind: String,
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub causation_event_id: Option<String>,
    pub occurred_at: DateTime<Utc>,
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredEvent {
    pub sequence: i64,
    pub event: HarnessEvent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplayState {
    pub run_id: String,
    pub status: String,
    pub human_thesis: String,
    pub last_sequence: i64,
    pub thesis_versions: Vec<ThesisVersion>,
    pub context_definitions: Vec<ContextDefinition>,
    pub context_versions: Vec<ContextVersion>,
    pub hypotheses: Vec<HypothesisDefinition>,
    pub hypothesis_reviews: Vec<HypothesisReviewDecision>,
    pub autonomous_review_triggers: Vec<AutonomousReviewTriggerRecord>,
    pub trade_outcomes: Vec<TradeOutcomeRecord>,
    pub review_packages: Vec<WorldModelReviewPackage>,
    pub cadences: Vec<LoopCadence>,
    pub loops: Vec<LoopView>,
    pub decisions: Vec<DecisionRecord>,
    pub resolved_context_snapshots: Vec<ResolvedContextSnapshot>,
    pub executions: Vec<ExecutionReceipt>,
    pub positions: Vec<PositionRecord>,
    pub orders: Vec<OrderRecord>,
    pub position_controls: Vec<PositionControlRecord>,
    pub failure_states: Vec<LoopFailureState>,
    pub reviews: Vec<WorldModelReviewRecord>,
    pub capital_allocations: Vec<CapitalAllocationRecord>,
    pub memory_documents: Vec<MemoryDocument>,
    pub context_pool_records: Vec<ContextPoolRecord>,
}

impl ReplayState {
    pub fn empty(run_id: &str) -> Self {
        Self {
            run_id: run_id.to_owned(),
            status: "unknown".into(),
            human_thesis: String::new(),
            last_sequence: 0,
            thesis_versions: Vec::new(),
            context_definitions: Vec::new(),
            context_versions: Vec::new(),
            hypotheses: Vec::new(),
            hypothesis_reviews: Vec::new(),
            autonomous_review_triggers: Vec::new(),
            trade_outcomes: Vec::new(),
            review_packages: Vec::new(),
            cadences: Vec::new(),
            loops: Vec::new(),
            decisions: Vec::new(),
            resolved_context_snapshots: Vec::new(),
            executions: Vec::new(),
            positions: Vec::new(),
            orders: Vec::new(),
            position_controls: Vec::new(),
            failure_states: Vec::new(),
            reviews: Vec::new(),
            capital_allocations: Vec::new(),
            memory_documents: Vec::new(),
            context_pool_records: Vec::new(),
        }
    }
}
