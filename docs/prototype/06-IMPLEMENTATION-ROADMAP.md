# Implementation and qualification roadmap

The package implements the integrated runtime. Qualification gates remain distinct from code completion.

| Milestone | Implementation and required evidence |
|---|---|
| M0 Contracts/dependencies | Python 3.14 install/imports, immutable contracts, provider schemas, pinned CPU checkpoint |
| M1 Vertical slice | Real Redis admission/pop, telemetry reduction, state, frozen batch, fixture gate |
| M2 Reducers/state | Conversation completeness, synthetic buffering, revisions, activity, caps, lifecycle authority |
| M3 CPU router | Local Laya, tokenizer preflight, histories/fan-out, reviewed corpus and frozen held-out report |
| M4 DREX gate | Closed questions, exact request packing, whole-decision validation, scheduler, status ACT |
| M5 System 2 | One-call profiles without token budgets, captured evidence, correlation, stale/late handling |
| M6 Qualification | Conformance suite, complete fixture workflow, CPU quality/performance, ten hosted-model runs |

## Reproducibility

`uv.lock` pins the tested dependency set. The Laya checkpoint is pinned to a revision with artifact/tokenizer hashes. Keep model-provider IDs, library versions, registry/prompt hashes, dataset hash, call counts, usage, latencies, losses, and pass/fail evidence in reports. No inference-quality claim follows from dependency installation alone.

## Live gates

1. Preserve original held-out recall, precision, exact-set and no-destination targets and CPU throughput qualification. The historical P0 routing-accuracy exception is limited to the recorded error rate and does not waive throughput. `evaluate-router` still reports the original targets.
2. Explicit hosted probes must verify strict outputs, DREX choices, and all three thinking profiles against the configured models.
3. Ten independent live scenarios must produce at least nine combined runtime-and-factual successes, with zero lifecycle/authority violations. Track missing lifecycle coverage separately.
4. Measure repeat suppression and thirty-second idle behavior separately; include every model tier, not merely escalation episodes.

A failed model gate leaves the fixture runtime useful for contract testing but does not make live mode qualified. Tune only development data; freeze changes before a fresh held-out evaluation. Do not introduce another router, silent fallback, durable queue, or wider action scope to conceal failure.

See [historical measurements](../qualification/2026-10-12.md) and [release verification](../qualification/release-0.1.0a1.md). Historical source hashes do not certify subsequent cleanup or prompt changes.
