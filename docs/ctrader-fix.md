# cTrader FIX adapter

The deterministic harness owns both cTrader FIX connections. Neither the world model nor Jev can access them.

- `PRICE` logs on through the price credentials and supplies current bid/ask midpoint data for sizing and stop evaluation.
- `TRADE` submits FIX 4.4 market orders, waits for final execution reports, and requests position reports for reconciliation.
- `CTRADER_MCP_*` is a separate, optional read-only context boundary. It is never consulted by order execution.

Copy the price and trade values from **cTrader Settings → FIX API** into the matching `.env` fields. Set `CTRADER_FIX_SYMBOL_MAP` to comma-separated `SYMBOL:FIX_ID` entries, for example `EURUSD:1,GBPUSD:2`. Symbol IDs are broker-specific.

The adapter accepts plain TCP or TLS according to each `*_SSL` value. It reconnects and performs a fresh reset-sequence logon for failed connection attempts. Missing credentials fail application startup only while `brokerAdapter` is `ctrader-fix`.

After configuring demo credentials, verify both sessions without placing an order:

```powershell
cd C:\JevCTraderV1\src-tauri
cargo test --lib live_ctrader_demo_sessions_log_on_quote_and_reconcile -- --ignored --nocapture
```
