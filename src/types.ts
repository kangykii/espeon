export type Timeframe = {
  label: string;
  horizonMinutes: number;
  source: string;
  rationale: string;
};

export type Hypothesis = {
  id: string;
  rootHypothesisId: string;
  parentHypothesisId: string | null;
  originalPrompt: string;
  instruments: string[];
  strategyMechanism: string;
  timeframe: Timeframe;
  deterministicContext: string[];
  liveContextSpec?: {
    id: string;
    version: number;
    instrument: string;
    fields: Array<{ fieldId: string; label: string; valueType: string; required: boolean; maximumAgeSeconds: number; expression: Record<string, unknown>; description: string }>;
  } | null;
  jevQuestion: string;
  thesisVersionId: string;
  contextVersionId: string;
  status: string;
  createdAt: string;
};

export type LoopView = {
  id: string;
  runId: string;
  parentLoopId: string | null;
  parentThesisVersionId: string | null;
  hypothesisId: string;
  thesisVersionId: string;
  contextVersionId: string;
  thesisVersion: number;
  contextVersion: number;
  state: string;
  allocatedFraction: number;
  createdAt: string;
  stoppedAt: string | null;
};

export type EventView = {
  sequence: number;
  id: string;
  kind: string;
  aggregateType: string;
  aggregateId: string;
  loopId?: string | null;
  causationEventId: string | null;
  occurredAt: string;
  summary: string;
  detail?: string | null;
};

export type Position = {
  id: string;
  runId: string;
  loopId: string;
  openedByExecutionId: string;
  brokerPositionId: string | null;
  direction: string;
  state: string;
  openedAt: string;
  closedByExecutionId: string | null;
  closedAt: string | null;
  lastEventId: string;
};

export type RunSnapshot = {
  runId: string;
  status: string;
  thesis: string;
  hypotheses: Hypothesis[];
  cadences: Array<Record<string, unknown>>;
  loops: LoopView[];
  positions: Position[];
  events: EventView[];
};

export type RunSummary = {
  runId: string;
  status: string;
  thesis: string;
  archived: boolean;
  startedAt: string;
  stoppedAt: string | null;
};

export type UpdateStatus = {
  state: "checking" | "current" | "waiting" | "installed" | "authRequired" | "error";
  message: string;
  version: string | null;
};

export type IntegrationStatus = {
  id: string;
  label: string;
  state: "connected" | "configured" | "disabled" | "degraded" | string;
  detail: string;
};

export type ConnectorField = {
  key: string;
  label: string;
  value: string;
  kind: "text" | "secret";
  configured: boolean;
  required: boolean;
  placeholder: string;
};

export type ConnectorSection = {
  id: string;
  title: string;
  description: string;
  fields: ConnectorField[];
};

export type ConnectorSettings = {
  worldModelAdapter: string;
  jevAdapter: string;
  brokerAdapter: string;
  riskReadiness: string;
  sections: ConnectorSection[];
  restartRequired: boolean;
};

export type ConnectorSettingsUpdate = {
  worldModelAdapter: string;
  jevAdapter: string;
  brokerAdapter: string;
  values: Record<string, string>;
};

export type WorkspaceSnapshot = {
  activeRuns: RunSnapshot[];
  runHistory: RunSummary[];
  integrations: IntegrationStatus[];
  hydratedAt: string;
};

export type Decision = {
  id: string;
  loopId: string;
  stage: string;
  action: string;
  confidence: number;
  rationale: string;
  thesisVersionId: string;
  contextVersionId: string;
  positionId: string | null;
  resolvedState: {
    instruments?: string[];
    context?: Array<Record<string, unknown>>;
    marketState?: Record<string, unknown>;
    resolvedAt?: string;
    liveContextSnapshot?: {
      id: string;
      freshnessState: string;
      qualityState?: string;
      resolvedAt: string;
      quote: { bid: number; ask: number; mid: number; spread: number; sourceTimestamp: string; receivedAt: string; provenance: string };
      candles: Array<{ period: string; openTime: string; open: number; high: number; low: number; close: number; tickVolume: number; providerVolume?: number | null; volumeKind?: string | null; provenance: string }>;
      fields: Array<{ fieldId: string; label: string; value: unknown; valueType: string; formula: Record<string, unknown>; observedAt: string; provenance: string[]; sourceObservationIds?: string[] }>;
    } | null;
  };
  createdAt: string;
};

export type Execution = {
  executionId: string;
  loopId: string;
  causedByDecisionId: string;
  action: string;
  executionKind: string;
  status: string;
  brokerReference: string;
  brokerPositionId: string | null;
  filledQuantity: number;
  averagePrice: number | null;
  rejectionReason: string | null;
  executedAt: string;
};

export type Order = {
  id: string;
  loopId: string;
  decisionId: string;
  instrument: string;
  side: string;
  quantity: number;
  referencePrice: number;
  notional: number;
  stopLossPrice: number | null;
  status: string;
  rejectionReasons: string[];
  createdAt: string;
};

export type ReviewRouting = {
  provider: string;
  baseModel: string;
  selectedModel: string;
  escalated: boolean;
  escalationReasons: string[];
  baseConfidence: number;
  requestIds: string[];
  internetResearchUsed: boolean;
};

export type WebEvidence = {
  id: string;
  url: string;
  title: string;
  publisher: string;
  claim: string;
  publicationDate: string | null;
  eventDate: string | null;
  retrievedAt: string;
  dateVerified: boolean;
  recencyRequired: boolean;
  usedAsPrimary: boolean;
  primaryEligible: boolean;
  recencyReason: string;
};

export type HypothesisReview = {
  id: string;
  hypothesisId: string;
  action: "keep" | "modify" | "split" | "stop";
  rationale: string;
  diagnosis: string;
  problemSeverity: string;
  continuationRationale: string;
  decisionConfidence: number;
  evidenceCanonicalIds: string[];
  proposedMechanism: string | null;
  routing: ReviewRouting | null;
  webEvidence: WebEvidence[];
  createdAt: string;
};

export type TradeOutcome = {
  id: string;
  loopId: string;
  positionId: string;
  instrument: string;
  direction: string;
  quantity: number;
  grossRealizedPnl: number | null;
  grossReturn: number | null;
  classification: "win" | "loss" | "breakeven" | "unknown";
  entryExecutionIds: string[];
  exitExecutionIds: string[];
  completedAt: string;
};

export type AutonomousReviewTrigger = {
  triggerId: string;
  loopId: string;
  hypothesisId: string;
  kinds: Array<"loss_streak" | "no_trade_streak" | "periodic_trade_count">;
  status: string;
  attempts: number;
  noTradeStreak: number;
  noTradeThreshold: number;
  consecutiveLosses: number;
  lossThreshold: number;
  completedTradesSinceReview: number;
  periodicTradeThreshold: number;
  lastDecisionId: string | null;
  lastTradeOutcomeId: string | null;
  reviewId: string | null;
  nextRetryAt: string | null;
  createdAt: string;
};

export type ContextRecord = {
  id: string;
  title: string;
  text: string;
  sourceClass: string;
  trustLevel: string;
  provenanceUri: string;
  publisher: string;
  observedAt: string;
  ingestedAt: string;
  canonicalEntityType: string;
  canonicalEntityId: string;
  canonicalEventId: string;
  tags: string[];
  metadata: Record<string, unknown>;
};

export type ReplayState = {
  runId: string;
  status: string;
  humanThesis: string;
  lastSequence: number;
  thesisVersions: Array<{ id: string; version: number; thesis: string; provenance: string; createdAt: string }>;
  contextVersions: Array<{ id: string; version: number; items: Array<{ source: string; sourceId: string; observedAt: string; content: string }> }>;
  hypotheses: Hypothesis[];
  hypothesisReviews: HypothesisReview[];
  reviewPackages: Array<Record<string, unknown>>;
  autonomousReviewTriggers: AutonomousReviewTrigger[];
  tradeOutcomes: TradeOutcome[];
  loops: LoopView[];
  cadences: Array<{ loopId: string; hypothesisId: string; timeframeHorizonMinutes: number; jev1IntervalSeconds: number; jev2IntervalSeconds: number }>;
  decisions: Decision[];
  executions: Execution[];
  positions: Position[];
  orders: Order[];
  capitalAllocations: Array<{ id: string; reason: string; allocations: Array<{ loopId: string; fraction: number }>; createdAt: string }>;
  contextPoolRecords: ContextRecord[];
};

export type SearchHit = {
  documentId: string;
  canonicalEntityType: string;
  canonicalEntityId: string;
  canonicalEventId: string;
  text: string;
  createdAt: string;
};
