# Prototype scope

The P0 is a single-operator local Python service and CLI, with local transient Redis, CPU Laya routing, and hosted inference. Configuration selects fixture or live mode explicitly.

| Feature | Required behavior |
|---|---|
| F01 Transport | Immutable JSON/binary signals; bounded atomic admission; destructive Redis pop |
| F02 Routing | One local Laya prediction per valid consumed signal; zero through three destinations; bounded history |
| F03 Reduction | Three independent workers with source protocols, bounded buffers, hosted summaries, and duplicate suppression |
| F04 Observation state | Atomic isolated updates, snapshots, events, revisions, activity, and frozen batches |
| F05 Lifecycle | Separate consumption/resolution, gate-only dispositions, domain evidence checks |
| F06 System 1 | Typed DREX decisions, scheduler, read-only status ACT, whole-decision validation |
| F07 System 2 | One asynchronous captured-context completion; gate-selected thinking; correlated result |
| F08 Operation | CLI, diagnostics, fixture/live demo, failure tests, and qualification reports |

## Boundaries

No real audio/video decoding, image observations, external remediation, production connectors, web UI, public application API, accounts, durable recovery, replay, distributed state, dynamic reducer/tool creation, subagents, long memory, or vector store. Simulated video carries JSON bytes and never establishes real visual accuracy.

The fixed reducers are `conversation`, `telemetry`, and `simulated_video`. Source strings identify logical streams; they do not authenticate producers. Registry/model/prompt changes require renewed routing evaluation.

The hosted models are configurable, but readiness rejects incompatible capabilities. System 2 uses the same model for three thinking settings, with no application token budget. There is one active System 1 evaluation, one active System 2 escalation, and one inference slot per reducer.

See [workflows](05-USER-WORKFLOWS.md), [architecture](04-TECHNICAL-ARCHITECTURE.md), and [build specification](08-PROTOTYPE-BUILD-SPEC.md).
