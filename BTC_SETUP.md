# Current profile: BTCUSD, 24/7, demo

This document supersedes the original FX-specific settings in DESIGN.md, IMPLEMENTATION.md, SCHEMAS.md and VALIDATION.md. The strategy remains trend-continuation pullbacks; the target is now BTCUSD on a dedicated demo account. Code and tests have been edited, not compiled or executed.

## What changed

- No London-style UTC trading session, Saturday/Sunday exclusion, midnight entry exclusion, or fixed session exit. The bot evaluates all M15 closes, including weekends and across midnight.
- The broker's `Symbol.MarketHours` is still authoritative. Entries are rejected when closed or if a scheduled closed minute occurs within the holding period plus two minutes. Open positions attempt to close two minutes before a scheduled closure. A broker's BTCUSD instrument need not offer uninterrupted exchange-style trading. These checks use cTrader's published MarketHours API: https://help.ctrader.com/ctrader-algo/references/MarketData/Symbols/MarketHours/.
- BTC gaps are never excused as FX weekends. All 32 bars in each timeframe must be contiguous; after a missing H4 bar this conservative warmup can take roughly 128 hours. It may therefore be unsuitable for a broker with frequent maintenance gaps until a separate gap-aware feature policy is designed. No missing prices are synthesized.
- Cost and spread inputs are basis points of current BTC price. One basis point is 0.01%. Entry sizing and market-range submission convert those values to the broker's pip unit using `Symbol.PipSize` at the current entry quote. This does not assume one pip is one dollar.
- The new configuration requires `Profile: BTC_24X7_V1`; old FX settings cannot silently load. Separate `runtime-btc` files prevent mixing research datasets. Do not use the new directory to reset existing account loss limits: if this account has already traded under the previous bot, reconcile and migrate the risk history first.
- Existing File alias fix is retained. Both the deliverable source and the existing cTrader project source were updated.

## Configuration now present

`settings.json` was created with BTCUSD and AllowOrders=false because no active settings file existed in the source directory. `settings.btc.example.json` and `settings.example.json` have the same BTC defaults. Keep source/config directory `C:\JevCTraderV1`, exact symbol `BTCUSD`, timeframe M15, execution disabled while verifying setup. Reload the cTrader editor from disk before rebuilding so an old editor buffer does not overwrite the changes.

The provisional defaults are MaxSpreadBps=5, SlippageBps=1, CostReserveBps=10, and MaxSpreadAtr=0.10. These are engineering examples, not measured broker costs or optimized parameters. At a hypothetical BTC price of $100,000, 5 bps is $50 of price spread, 1 bp is $10 of price slippage, and 10 bps is $100 of price-equivalent cost reserve per BTC-equivalent exposure. Actual cash charges depend on contract units, quantity and conversion. The reserve must cover round-trip fees and relevant financing/adverse cost allowance; it does not replace actual fee logging.

`MaxVolumeUnits` is deliberately zero until the contract specification is known. This allows observation but returns BROKER_VOLUME_CAP_UNSET for an execution attempt. Do not copy the old 10,000 FX-unit cap. Obtain the broker name and BTCUSD specification: volume units per lot, minimum/step size, commission and any rollover/financing charges. Set a verified positive cap in broker volume units; the bot also rounds down and checks equity risk and margin. A minimum permitted trade that exceeds the risk budget is rejected, never rounded up.

The demo-only account guard remains. The existing 0.10% risk, 0.50% UTC daily loss cap, 2% equity drawdown limit, maximum four entries/day, 75-minute cooldown, 60-minute time exit and 0.60 confidence floor remain. 24/7 eligibility does not mean a trade every bar. UTC daily counters reset at midnight; open positions retain their original entry time and stop/target. The VWAP proxy also resets at UTC midnight as an analytical anchor, not a market closing event.

Jev access and a verified BTCUSD event-calendar file are still required. The calendar must cover relevant major macroeconomic and known crypto events for the holding window; an unverified empty file is not a valid substitute. The sample calendar remains illustrative. A missing calendar blocks entries, not model observation. Do not disable the event filter merely because the instrument trades on weekends.

## Research and audit

New logs are in `runtime-btc/events.jsonl`; manual kill and restart reconciliation files are also in `runtime-btc`. Preserve old `runtime` contents. Never combine FX and BTC runs in one study. The replay accepts BTC basis-point settings, with backwards compatibility for old FX journals. It handles open-position UTC midnight risk resets and attributes realized trade P&L to exit day; its daily bootstrap therefore measures realized daily P&L, not an exact mark-to-market daily return series. The same continuous-size/fee-proxy limitations remain, and crypto financing across rollover requires separate statement reconciliation.

Authored tests now cover BTC pip conversion invariance, wide spreads, weekend/midnight bar continuity, and missing BTC bars. They have not been run. First rebuild, then complete demo platform checks; do not treat these changes as validated execution readiness.
