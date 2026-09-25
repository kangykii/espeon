# AUTONOMOUS JEV TRADING HARNESS

## Architecture Specification

### Purpose

The harness is a persistent autonomous trading system that starts from a human-supplied thesis, then runs hands-free until the user stops it or a deterministic system stop condition is reached. The world model is not the live trader. It interprets and tests hypotheses. Jev runs the live decision loops. Deterministic harness logic controls capital, risk, execution and logging.

### Core architecture

#### 1. World model

The world model is invoked sparsely rather than continuously. At startup it interprets the user's thesis, chooses what deterministic context should be supplied to Jev, and defines the qualitative thesis/question Jev should evaluate.

The world model may later wake on meaningful review events, inspect accumulated evidence and logs, critique the active hypothesis, change the thesis, change the context, or spawn a competing Jev loop. It is allowed to stray from the user's original thesis when evidence supports a better hypothesis. The original user input remains provenance, not an immutable constraint.

The world model does not directly trade, size positions, control capital, or bypass guardrails.

#### 2. Context + thesis

Each Jev loop is defined by two things.

Context is deterministic information supplied to the loop. It can include live market data, official data feeds, selected internet or news records, historical records, recorded research, prior Jev decisions, trade logs and other indexed sources.

Thesis is the qualitative question or proposition Jev is asked to evaluate.

The world model can modify either independently. This allows live experiments such as keeping the same thesis but changing context, or keeping the same context while testing a modified thesis.

#### 3. Jev loop

Each active hypothesis runs in its own Jev loop.

Jev1 decides whether to open new risk. Its effective outcomes are LONG, SHORT or NO TRADE. A deterministic confidence gate can convert insufficient-confidence decisions into NO TRADE without Jev needing to know the rule.

Once a position is open, control moves to Jev2. Jev2 has only three actions: HOLD, SELL or BUY MORE.

HOLD keeps control in Jev2 and the position remains open.

SELL closes the position and returns control to Jev1 to search for the next opening.

BUY MORE is a request for additional exposure. Control returns to Jev1, which independently decides whether the additional buy should be accepted.

The loop continues until stopped. Jev does not know account capital, position sizing, capital allocation between hypotheses, or hidden guardrails. Those are irrelevant to its qualitative decision task.

#### 4. Deterministic harness

The harness sits between Jev and the broker. It applies rules that are intentionally outside model awareness.

Its responsibilities include:

- capital allocation between active Jev loops;
- position sizing and order construction;
- confidence gates such as NO TRADE below a configured threshold;
- stop losses and hard risk limits;
- execution constraints and broker interaction;
- loop lifecycle control;
- immutable event logging.

The harness is the system authority. Model outputs are requests interpreted by deterministic rules, not direct broker commands.

### Hypothesis spawning and capital splitting

The world model can test a modified thesis or modified context by spawning another Jev loop.

Capital is split automatically and equally across active loops unless the deterministic harness later specifies another fixed allocation policy. The Jev loops never see this capital amount.

Example:

One active loop receives 100% of available capital.

Two active loops each receive 50%.

Three active loops each receive approximately one third.

This makes hypothesis testing a harness-level portfolio operation rather than part of Jev reasoning.

### Memory and feedback loop

Every meaningful event is logged: the loop identity, thesis, context used, Jev decisions and confidences, execution result, market outcome, timestamps and experiment status.

Logs are then indexed back into the context pool. The world model can retrieve them later alongside live and historical sources. This creates a closed research loop:

human thesis -> world model -> context + thesis -> Jev trading -> logs -> indexed context -> world-model critique -> modified or competing hypothesis -> new Jev loop.

The vector index is not the source of truth. Exact trades, timestamps, decisions and outcomes remain in structured canonical storage. Semantic indexing exists to help the world model retrieve relevant prior situations and research efficiently.

### System boundaries

The most important boundaries are:

- world model can change thesis/context and manage experiments, but cannot directly trade;
- Jev can make qualitative trading decisions, but cannot see or control capital;
- deterministic harness owns risk, sizing, broker execution and hidden guardrails;
- historical logs are append-only and cannot be rewritten by the world model;
- all retrieved context carries provenance and timestamps.

The result is a simple three-part intelligence split:

World model: hypothesis formation and critique.

Jev: fast live decision loop.

Harness: deterministic control, capital, risk, execution and memory.

### Live Jev context

Each hypothesis owns an immutable, typed `LiveContextSpec`. Its named fields are formula
ASTs over current bid/ask/mid/spread and completed cTrader candles. The formula definition
is stable for the hypothesis version; Rust reevaluates it from fresh observations before
every Jev1 or Jev2 call.

The production market-data boundary combines a persistent cTrader FIX price session with
cTrader Open API trendbar backfill. FIX quotes update the live envelope and locally formed
UTC bars; Open API repairs startup and reconnect gaps. Partial candles never enter signal
formulas. Every consumed quote, completed candle, formula result, timestamp and provenance
reference is captured in an immutable `ResolvedContextSnapshot` and linked to the decision.

If a required observation is missing, stale, malformed, or lacks sufficient history, the
harness records `live_context_resolution_failed` and skips Jev. It does not translate an
operational data failure into `NO TRADE`. Deterministic stop evaluation remains outside the
model path and continues to use the broker price boundary.
