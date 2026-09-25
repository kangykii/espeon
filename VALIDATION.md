> **BTC update:** The current bot targets BTCUSD with 24/7 eligibility. Read [BTC_SETUP.md](BTC_SETUP.md) first; it supersedes the FX schedule, pip-cost defaults and runtime paths described below.

# Validation, walk-forward protocol and falsification

## Status of this delivery

Source and design only. No restore, build, compiler, unit tests, Python scripts, cBot, broker connection, TypeSafe request, backtest or deployment was run. Documentation was checked against primary sources and code was statically reviewed. Tests below are supplied for the implementer, not reported as passing. Current API signatures still require compilation in your installed cTrader version.

## Freeze the experiment before collecting confirmatory outcomes

Record the symbol, broker, account type/currency, fixed UTC session, feature definitions, chosen model ID, exact prompt bytes, settings, commission schedule, slippage estimate, sample end date, hypotheses and review dates. Keep a research change ledger. A change to prompt, model, feature, threshold, event rules, exits or symbol starts a new experimental version. Do not silently pool models. Do not choose the initial instrument or session because it looked best in the final holdout.

Primary hypothesis H1: mean daily **Jev portfolio minus baseline portfolio** net P&L is positive. Secondary H2: net expectancy increases with raw setup-direction confidence for comparable mechanical candidates. The proposed 0.60 threshold is a frozen initial abstention heuristic. It is not a 60% profit forecast and may prove useless.

Use three to six months of exploration/data quality work, followed by rolling monthly unseen test windows and a final untouched holdout of at least three months where feasible. These are proposed research durations, not a promise that the resulting sample is sufficient. Target at least 200 Jev-selected OOS trades, 500 eligible labeled candidates and 60 independent trading days; also inspect the number of independent weeks. If this takes longer, extend according to a rule written before results are inspected, not until a desirable p-value appears.

## Data integrity before economic analysis

Outer-join the intended M15 decision grid with EVALUATION_STARTED, EVALUATION, JEV and FINAL. Report absent callbacks, data failures, unknown calendar, invalid schema, timeouts, model-busy, final reasons, missing fills and incomplete exit history. Missing model confidence is its own category. Confirm every fill has a prior durable intent, a unique position mapping and matching broker statement. Confirm every candidate label uses only observations available at the decision and every HTF bar closes no later than that decision. Spot-check overlapping labels and time zones around weekends/DST.

The runtime supplies tick activity and a VWAP proxy, not exchange participation. Stratify diagnostics by missing VWAP/participation. Tick tape can be lost during downtime; never forward-fill that into a tradable price path. Censor labels spanning a >30-second recorded gap. Report missingness separately by accepted and rejected decisions. If censoring differs materially by group, stop making edge claims until coverage is repaired.

Retrospective Jev evaluation can exploit knowledge encoded in its training, even when your market features are point-in-time. Avoid descriptive future news text, asset-event labels and outcome-revealing fields. Treat historical Jev replay as exploratory. Forward-recorded, unseen demo/shadow decisions are the confirmatory evidence; timestamps hidden from a model do not eliminate all contamination.

## Comparators and identification

1. **B0: no-trade/cash**. Does the strategy family add positive net value at all?
2. **B1: deterministic candidate + identical risk/execution**. This is the primary comparator. Jev adds only the qualitative veto.
3. **J1: same candidate, Jev context/setup veto, fixed confidence floor**. No different stop, target, time exit or sizing.
4. **Matched-thinning control**. Randomly retain B1 opportunities at J1's selection rate within predefined session/direction strata; replay the portfolio constraints for each of 1,000 fixed-seed draws. This optional sensitivity study distinguishes better selection from merely trading less. Do not sample individual final P&Ls as though portfolio conflicts did not exist.
5. Predeclared context-only and setup-only ablations, if sample size permits. They are secondary; adjust inference for the declared comparison family or label them exploratory.

The included runner implements B1/J1, all-candidate independent labels, common latency and doubled cost stress. B0 is identically zero. Matched-thinning, secondary ablations and power estimation are protocol extensions, not claimed as implemented outputs. The core claim must first survive B1.

Use two complementary views. Independent candidate labels answer which opportunities Jev selects/rejects; overlapping labels are not a portfolio. Separate chronological replays enforce each policy's own position occupancy, cooldown, daily cap and risk latches. A veto changes subsequent availability, so subtraction of isolated trade means is not the primary economic test. Keep session/direction/volatility strata fixed rather than searching hundreds of subgroups.

## Replay mechanics

The Python runner reads the saved journal only. Both policies wait the configured TTL (20 seconds) after the scheduled close, using the first quote within two seconds of that point. J1 requires the model result to have been logged by then. This fixed delay gives a clean latency-controlled comparison; it intentionally differs from the runtime's earliest-valid-response execution. Confirmatory promotion also requires a cTrader demo or a higher-fidelity broker replay using actual response availability, and a faster deterministic-baseline sensitivity test. A fast baseline may do better; do not hide that.

Use executable ask-to-bid prices for longs and bid-to-ask prices for shorts. Entry and exit receive adverse fixed slippage. Stops and targets are checked on each quote; gap fills use the observed executable quote, not the ideal stop. Time exit uses first available quote at/after the deadline. Tick ordering resolves stop/target sequence. Spread is already paid through the bid/ask convention. The cost input in this initial runner is the configured reserve used as a conservative round-trip fee proxy; replace it with a separately versioned measured fee schedule before interpreting results. The reserve is also used in the sizing denominator, but it is deducted from P&L only once.

The runner uses continuous risk-normalized size, not exact broker units, margin, minimum fees, historical pip conversion, rejects, partial fills, financing or overnight charges. Those are material fidelity limitations. V1 avoids overnight holds; actual fees and swap are logged. Estimated normalized returns are insufficient for deployment until actual demo fills reconcile with the simulator and measured live-like costs. Recheck targets that gap favorably: the current quote-fill assumption can give improvement; include a conservative target-capped sensitivity in final broker replay.

Markout horizons are 15/30/45/60/90 minutes from observed evaluation time. The primary strategy exit is 60 minutes or stop/target first. Do not pick the best horizon after viewing results. For each trade record net R, account-normalized P&L, MFE/MAE, holding time, exit reason, fill costs and risk exposure. Report expectancy, return per calendar/session day, traded volume/turnover, drawdown, loss tails and cost sensitivity. Add API charges separately at the strategy-day level: both context and setup calls, including rejected opportunities, consume budget.

## Walk-forward implementation

`folds.example.json` defines nonoverlapping test windows with an embargo of at least 90 minutes after the prior training cutoff. Features may use earlier closed prices; training labels may not extend into validation/test. Purge any training sample whose 90-minute label interval touches the next evaluation window. For adaptive calibration, fit only on train, select on validation, and score once on untouched test. Initial V1 deliberately fits nothing: fixed rules and confidence floor are evaluated sequentially, reducing overfitting opportunities.

Each fold starts flat with normalized equity one and new risk state; report this reset convention. Do not concatenate fold equities and call that an uninterrupted account backtest. Also run a continuous holdout replay for deployment-like risk behavior after freezing the policy. Exclude/censor positions that would cross a fold's end. Advance model versions only between experiments; cached decisions are immutable and never recomputed to replace disliked answers.

Commands for the implementer, not executed in this delivery:

```powershell
python -m unittest discover -s tests -p "test_*.py"
python research.py runtime/events.jsonl --out analysis --folds folds.json
python actuals.py runtime/events.jsonl --out analysis/actual_trades.json
```

The output contains independent candidate labels, portfolio trades, confidence bins, censor counts, net statistics and a paired five-day moving-block bootstrap for daily J1-minus-B1 P&L. It includes observed zero-trade days. Completely missing days must be identified by the expected-grid audit; they are not silently inserted as genuine zero returns. Bootstrap intervals are approximate, not valid evidence with too few independent blocks or major nonstationarity. Recheck using weekly clustering and a 10-day block sensitivity. Do not repeatedly inspect intervals until one excludes zero.

## Confidence analysis

Use fixed bins [0,.4), [.4,.6), [.6,.7), [.7,.8), [.8,.9), [.9,1]. For raw directional choices matching the mechanical direction, calculate **net mean R** and day/week-clustered uncertainty for each bin, including hypothetical outcomes for candidates rejected only by confidence or context. Log NONE separately: confidence in NONE does not share the meaning of confidence in LONG/SHORT. Also retain the continuous raw confidence and class probabilities.

Compare selection coverage, average net R, median, loss tail, cost and win rate by bin; win rate is secondary. Test a prespecified monotonic slope or rank trend with day/week clustering, adjusting for the small predefined session/direction/volatility strata. Check that the result persists across folds rather than being one outlier month. If desired, fit isotonic confidence-to-net-expectancy on training data and score it on the next test fold; never describe that mapping as a probability model. Brier score/ECE only become appropriate after separately fitting a probability-of-positive-net-return model with a clearly defined target.

Do not multiply answers' confidences: the stages and questions are correlated. Do not retune 0.60 to the historical best number. At most preregister 0.50/0.60/0.70 as sensitivity analyses with multiplicity disclosure; only 0.60 is the V1 trading rule. The code enforces >=0.60 for execution.

## Kill switches and falsification

Immediate operational kill: missing protection; position/order reconciliation mismatch; corrupt or unwritable audit; unexpected model version; repeated API/schema failures; manual KILL file; daily/global equity breach. Stale quotes/calendar/model result reject the evaluation. A disconnected process cannot guarantee flattening; broker protection and operator visibility are essential.

Freeze these research review criteria before the holdout:

- **Promotion**: positive J1 net expectancy and positive paired daily increment whose prespecified 95% block interval excludes zero; no material data-quality bias; sufficient independent days/trades; positive net expectancy under doubled measured fee/slippage stress; no single month or handful of trades accounting for essentially all incremental profit. This is an evidence threshold, not proof of future profitability.
- **Falsified incremental edge**: at the terminal preregistered sample, the upper bound for the daily incremental effect is <=0, or the effect's upper bound is below a prespecified economically worthwhile minimum after API and operational costs. Stop V1 rather than mining reasons for a rescue subgroup.
- **Inconclusive**: interval spans both material benefit and harm. Keep execution disabled; collect only the preallowed extension, or end the study as inconclusive. A non-significant result does not establish no edge.
- **Confidence hypothesis fails**: no stable positive OOS confidence/expectancy relationship or clear reversal. Do not use confidence for sizing; reject the 0.60 floor as a validated profitability filter even if qualitative veto value survives. A revised policy needs a fresh holdout.
- **Strategy family fails**: both B1 and J1 have negative net expectancy after realistic costs. Do not celebrate J1 for losing less unless that was explicitly the sole research objective.
- **Data/behavior invalidates study**: >1% eligible decisions missing without explanation, unresolved fills, or material differential censoring. Pause conclusions and repair the experiment. Threshold is a proposed engineering acceptance rule, not a statistical theorem.

Runtime drawdown stops are engineering guardrails, not statistical falsification. A 2% drawdown can occur with or without edge. Conversely, staying under the drawdown limit does not validate the strategy.

## Acceptance work before any implementation is considered ready

Compile in installed cTrader; run provided core and Python tests; confirm demo account guard and observe-only dual switches. Test real broker symbol minimum stop distance, unit rounding, margin, commission, server timezone, expiry and stop-trigger conventions. Inject malformed/future/partial HTF bars, stale calendars, invalid Jev response and returned model change. Test exactly-at-threshold confidence, stale model arrival, weekend bar event, wide spread and price drift. Test crash before intent, after intent/before fill log, restart with an open position, missing stop, disk full and failed close. Confirm no duplicate entry after ambiguous submission. Verify model requests contain no API key or account secrets in logs.

Replay a known quote path through both the pure policy and cTrader demo, then manually reconcile all fills and fees. Do not call the live API inside accelerated backtests. The supplied unit-test projects deliberately require an explicit local cTrader API assembly path; no unpinned package restore is hidden in the deliverable.
