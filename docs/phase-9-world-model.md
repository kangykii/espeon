# Phase 9 World-Model Routing and Research

The selected `openrouter` adapter contains all model selection. The rest of the harness continues to depend only on the `WorldModel` port and the provider-neutral review package/action types.

## Routing

- Base: `openai/gpt-6-luna-pro`
- Escalation: `anthropic/claude-opus-5.5`
- Deterministic escalation triggers: explicit base request, confidence below 0.70, material contradiction, two or more prior MODIFY/SPLIT revisions, regime/causal change, a genuinely new hypothesis, unexplained failure, or stale/undated primary web evidence.
- Both models receive the identical canonical review package and return only KEEP/MODIFY/SPLIT/STOP through the existing schema. Capital, orders, broker access, stops, and Jev remain outside this adapter.

## Internet research and recency

Time-sensitive or explicitly web-oriented reviews first use OpenRouter server tools (`web_search`, `web_fetch`, and `datetime`) to build a research dossier. A second schema-constrained call converts that dossier into the normal review response. This separation avoids provider tool-output formatting from changing the action contract.

Every returned web item records its URL, publisher, claim, publication date, event/effective date, retrieval timestamp, date-verification flag, and deterministic primary-eligibility result. The maximum evidence age is tied to the hypothesis horizon: 72 hours for intraday, 14 days through one week, and 30 days for longer horizons. A missing/unverified date, stale date, or implausibly future date cannot support a state-changing action as primary evidence.

Web items are stored as immutable `external_web_research` / `untrusted` canonical context records and indexed into Qdrant. Review decisions link back to eligible evidence IDs. Retrieval and web request IDs plus routing reasons are retained in decision metadata.

## Configuration

`OPENROUTER_API_KEY` is the only required world-model secret. `JEV_API_KEY` remains separate for TypeSafe Jev. Model IDs, OpenRouter base URL, and web-search enablement are configurable through `.env` without changing the harness.

## Evaluation

The regression set covers routine non-escalation, low-confidence and new-hypothesis escalation, contradiction and repeated revisions, narrow internet tool selection, structured SPLIT output, provenance fields, recency eligibility, stale/undated rejection, response parsing, fixed model IDs, and authority-boundary preservation. Separate ignored smoke tests exercise the live base model, live search/recency flow, and live base-to-Opus escalation.
