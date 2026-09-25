> **BTC update:** The current bot targets BTCUSD with 24/7 eligibility. Read [BTC_SETUP.md](BTC_SETUP.md) first; it supersedes the FX schedule, pip-cost defaults and runtime paths described below.

# Implementation handoff (instructions only)

Nothing in this delivery has been run. Do not interpret source delivery as a passing test or broker certification.

1. Read DESIGN.md, SCHEMAS.md and VALIDATION.md. Choose the one broker symbol/session and measured costs. The example is EURUSD with a fixed UTC session; it is not a recommendation of the most profitable instrument.
2. Create a C# cBot in cTrader Algo on .NET 6 or later. Replace its default source with JevPullbackBot.cs. This is the complete runtime source; all helper classes are in that file. Set the directory parameter to your local project directory.
3. Copy settings.example.json to settings.json and supply a genuine calendar.json under the documented contract. Keep both execution switches false for shadow data collection. The calendar sample is not factual and will be rejected by its source field. Configure a currently available exact Jev version. The included example version was seen in TypeSafe documentation, but your account's entitlement must be checked; never silently substitute `latest`.
4. Supply TYPESAFE_API_KEY to the local process environment before starting cTrader. Do not put it in source, journal or settings. Missing key logs API_UNCONFIGURED and prevents orders. The API client is implemented; no bridge service or chat-completion adapter is required.
5. Build the cBot and the pure-core tests yourself. For core tests: `dotnet run --project tests/CoreTests.csproj /p:ApiAssemblyPath="<absolute installed cAlgo.API.dll path>"`. Match the target framework to the installed assembly if needed. This command is supplied, not executed.
6. Run Python standard-library tests and offline fixture/data analyses as described in VALIDATION.md. Unit tests cover invariants, not live-platform correctness. Perform the platform failure-injection acceptance work before enabling demo execution.
7. On a dedicated hedging demo account and M15 time chart, start observe-only. The cBot reads bars/quotes, records every evaluation and calls Jev, but both switches are required for order entry. Note: risk management of previously owned labeled positions still operates even if new entries are disabled. FullAccess is needed for local journal files and the environment key; restrict access to the directory yourself.
8. When the separately specified demo acceptance work is complete, enabling both switches permits demo execution only. The source explicitly refuses live accounts and historical cTrader running modes. Use offline saved-decision replay for historical studies. Do not remove the live guard as part of the V1 evidence-gathering workflow.

## Daily operation after you implement it

Before the session: verify calendar coverage/provider vintage, available disk, UTC clock, account flatness and execution mode. During the session: observe logs, broker connection and model-failure rate. At session end: reconcile broker statement against intents/fills/exits, archive immutable logs and compute missingness; then compute outcomes without feeding them back into prompts.

After any restart: the cBot creates runtime/RECONCILE.required. Compare pending intent and recorded fills with actual positions and History. Retain risk day/high-water mark and cooldown. Resolve any pending order uncertainty; record a manual reconciliation note in a separate audit file before editing risk state. Only then remove the reconciliation sentinel. Global loss/error latches require investigation; do not blindly reset them. Existing positions have original entry times and time/stop supervision, but forward watchers are censored across a crash unless externally reconstructed.

To stop new risk and attempt flattening: create runtime/KILL. Verify the broker account actually becomes flat. A disconnected bot cannot ensure a close or a time exit. Never delete runtime state/logs while positions or ambiguous intents exist.

## Boundaries that are intentional

- One host, one runtime directory, one symbol and dedicated account. The local file lock does not coordinate separate machines or directories.
- Calendar adapter is a validated file contract, not a supplied licensed news feed. Archive vintages externally if the provider replaces them.
- Quote tape covers observations received by this cBot. It is not an exchange tick recorder. Missing-path labels are censored.
- Actual History can populate after the exit callback. HistoryPending means obtain/reconcile a broker statement; the tool cannot manufacture fees or fills.
- Offline portfolio calculations use normalized continuous R size and a fee proxy. Broker rounding/conversion, actual fees and rejection rates must be reconciled before any economic conclusion.
- This code is designed for liquid FX time bars. Session-based equities/futures/CFDs require calendar, units and volume semantics changes before use.
