# Historical memory and review packages

Canonical SQLite records and append-only events remain authoritative. Qdrant contains searchable projections only.

Every completed decision, deterministic order, execution, position outcome, hypothesis lifecycle action and world-model review is automatically projected with:

- canonical record and event URIs;
- run and loop identity;
- thesis and context version IDs;
- instrument and timeframe;
- decision, position and outcome metadata;
- original event time;
- internal provenance and trust classification.

Review retrieval runs across experiments rather than being restricted to the current run. Exact current IDs are retrieved alongside semantically and lexically related history, and the final evidence list is chronologically ordered.

Before a review model is invoked, the harness records a provider-independent review package containing current versions, recent Jev decisions, executions, positions, prior reviews, spawned hypotheses, historical retrieval and supporting/contradictory evidence classifications. The package can be inspected independently through the `prepare_review_package` Tauri command.

KEEP, MODIFY, SPLIT and STOP remain the lifecycle actions. SPLIT can carry a structured competing-hypothesis candidate. Provider selection and escalation are deliberately outside this package and remain Phase 9 work.
