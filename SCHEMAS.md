> **BTC update:** The current bot targets BTCUSD with 24/7 eligibility. Read [BTC_SETUP.md](BTC_SETUP.md) first; it supersedes the FX schedule, pip-cost defaults and runtime paths described below.

# Contracts and audit semantics

All timestamps are UTC ISO-8601. C# local contract property names are PascalCase exactly as shown in the source. TypeSafe request/response names are lower-case exactly as documented. Numbers are finite JSON numbers; missing evidence is null, never zero or a fabricated observation. The C# DTOs in `JevPullbackBot.cs` are the executable input/output schema, while this document defines semantics and joins.

## Snapshot v1

| Field | Type / meaning |
|---|---|
| Schema | constant `snapshot-v1` |
| Id | `symbol-yyyyMMddTHHmmss-v1`, scheduled closed-bar time, stable within experiment |
| Symbol | configured broker symbol string |
| DecisionUtc / ObservedUtc | scheduled bar close / first actual snapshot observation |
| Bid / Ask / PipSize | entry-side quote prices and pip unit |
| M15 / H1 / H4 | Frame object, exactly 32 closed Candle objects in time order |
| Frame.Bars[].OpenUtc | opening time; close time derived using frame duration |
| Candle.O/H/L/C/TickVolume | unadjusted broker OHLC, broker tick count |
| Frame.Atr | mean of last 14 true ranges, price units |
| Frame.Efficiency | absolute 12-bar close displacement / sum of adjacent absolute changes |
| Frame.NetAtr | signed 12-bar displacement / ATR |
| Frame.RangeLow/RangeHigh | extremes of last 12 bars |
| TickVwap | nullable price, UTC-day typical-price/tick-volume proxy |
| RelativeTickActivity | nullable ratio vs previous 20 M15 bars |
| VolumeKind/VwapKind | explicit provenance labels |
| Depth/OrderFlow | null in V1 |
| Calendar | nullable CalendarSnapshot including as-of, coverage, provider, events |
| CalendarError | nullable read/parse error class |
| ConfigHash/PromptHash | SHA-256 of serialized configuration / exact prompt file |

The wire `state` is the market subset plus current spread in pips; setup adds context answers. Raw evidence is retained even if a later hard gate rejects it. A failed capture produces EVALUATION_STARTED, EVALUATION_ERROR and FINAL rather than a partially valid Snapshot. The error names the missing/invalid input; raw broker tick/bar archives remain necessary to diagnose unavailable data. An expected M15 grid must be outer-joined with these events to distinguish no callback during outage from evaluated abstention.

## Jev contract

Request: `{ "model": "pinned-version", "state": {...}, "questions": { "questionId": { "type":"choice", "instructions":"...", "criteria": {"OPTION":"definition"} } } }`.

Response: `{ "model":"pinned-version", "answers": { "questionId": { "type":"choice", "choice":"OPTION", "confidence":0.65, "probabilities":{"OPTION":0.8,"OTHER":0.2} } }, "usage": {...} }`.

Required answer keys must exactly match the stage questions. Every expected option must exist in the distribution; all probabilities/confidence lie in [0,1], sum within 0.001 of one, and the selected option must attain the maximum probability. A returned model mismatch is fatal for that evaluation. These checks protect the adapter, not model correctness. `Judgement.Direction` is the combined policy result; `Judgement.Confidence` is raw setup-direction confidence even when a context veto forces NONE. For analysis, use raw `Setup.Reply.answers.direction.choice` to interpret that confidence.

`Stage`: exact Request string, Raw response string, parsed Reply, StartedUtc and EndedUtc (wall-clock API latency). `Judgement`: Id, Context, Setup, Error, Direction, Confidence, Reasons. API timestamps use machine UTC; execution uses server UTC. Compare/log clock skew before relying on latency; expiry is enforced using server time.

## Calendar contract

`AsOfUtc`, `CoverageFromUtc`, `CoverageToUtc`, `Symbol`, `Source`, `Events[]` are required. Events have `Id`, `TimeUtc`, `KnownAtUtc`, `Impact` (`HIGH` for blocking events), and `Name`. Coverage refers to the whole relevant event universe for the configured symbol, including both currencies. A null list, future as-of, unknown source or insufficient coverage is rejected. The historical file must be the vintage known at that decision, not the final revised schedule. Realized release values are not used.

## Candidate / final decision

Candidate: Direction LONG/SHORT/NONE, StopAnchor (price), Obstacle (price; zero means none observed), StopPips, TargetPips and Reason. A NONE candidate can still carry computed geometry, which must not be interpreted as tradable. Final: Id, Direction and Reasons[]. Reasons retain **all** gate failures reached, not just the first. FINAL is the authoritative action outcome, while JEV and Candidate describe independent analytical opinions. ORDER_ERROR_RECONCILE is not proof no fill happened.

## Append-only storage

Every journal line is `{Schema:"journal-v1", Run:<uuid>, Seq:<increasing per run>, Utc:<server time>, Kind:<event>, Data:<typed payload>}`. `(Run, Seq)` is the primary key. Semantic events are flushed before execution; never overwrite a past model response. Keep raw JSONL as the source of truth; SQLite is a rebuildable index with per-payload SHA-256 and idempotent import. Hashes detect changed ingested payloads, not malicious rewriting of the complete original archive. Archive daily files/backups and a manifest hash outside the runtime directory if stronger provenance is required.

| Kind | Contents / use |
|---|---|
| START | complete config, hashes, symbol units, account currency, run mode, model availability |
| EVALUATION_STARTED | every callback, scheduled time and observed quote |
| EVALUATION | full snapshot, deterministic candidate, current hard reasons |
| EVALUATION_ERROR | capture error; join to started record, never silently exclude |
| JEV | both raw stages, distributions, confidences, semantic reason codes, errors |
| EXECUTION_CALENDAR | fresh calendar vintage used in final execution gate |
| FINAL | final direction and complete rejection codes |
| ORDER_INTENT | evaluation ID, requested side/price/volume, stop, target and cash risk budget |
| FILL | actual position ID, entry, requested quote, adverse signed slippage, spread, protection |
| EXIT_INTENT | position ID, request quote and requested exit reason |
| EXIT | position totals, all available historical closing deals, requested and broker reasons |
| QUOTE | every observed bid/ask tick in the active process; source for path reconstruction |
| FORWARD | 15/30/45/60/90-minute long/short markouts, observed lag and cumulative excursions |
| FORWARD_CENSORED | orderly stop interrupted a path |
| DAY_RESET / KILL / STOP | risk lifecycle and operational audit |
| RECOVERED_POSITION | broker position discovered at restart |

Join evaluation to order via Id/comment, order to fill/exit via position ID, and all events to the versioned START record via Run. Use historical trade deals for closing prices and account-currency fees; an exit callback may precede history population (`HistoryPending=true`). Reconcile those cases against a later broker statement before calculating realized results. Do not infer a closing fill from the current quote. [cTrader historical trade contract](https://help.ctrader.com/ctrader-algo/references/Trading/History/HistoricalTrade/)

Broker `NetProfit` is authoritative for net P&L; `GrossProfit`, signed `Commissions` and `Swap` are recorded separately. Check their reconciliation on the actual broker; do not subtract commission again from NetProfit. Additional broker charges need statement reconciliation. Entry slippage: long `(fill-request)/pip`, short `(request-fill)/pip`. Exit slippage: adverse difference between requested executable exit-side quote and actual closing price; for stop/target use the trigger level as a separate gap/slippage reference. Spread is the contemporaneous ask minus bid, not a second charge applied to bid/ask returns.

## Outcome labels

For each evaluation, including NONE/rejected, first observed quote is the markout origin. Long horizon return in price units is future bid minus origin ask; short is origin bid minus future ask. Bps divide by origin ask/bid respectively. First quote at/after the horizon is used; lag >30 seconds is invalid. The offline tool additionally censors paths with >30-second quote gaps. A no-tick interval cannot establish whether the market was quiet or the feed failed; conservatively flag it.

MFE/MAE are extrema of executable-side mark-to-market change from origin through the given horizon, MFE floored at zero and MAE capped at zero. They are sampled-tick excursions, not exchange-perfect extrema. Bar OHLC alone cannot determine stop-versus-target ordering. Actual-trade MFE/MAE must instead run from actual fill to actual close; the offline `actuals.py` tool reconstructs these separately. Quote returns include spread but exclude commission; fees and slippage are added for policy net labels. NONE produces two hypothetical sides, never a fictional realized trade.

Forward labels live in separate events/tables and must never be joined into model feature exports. Interrupted paths remain missing/censored, not zero-return losers. Across restarts, reconstruct from continuous archived broker quotes where available; do not pretend the in-memory watcher survived. Measure missingness by reason, regime, session, confidence and selected/rejected status.
