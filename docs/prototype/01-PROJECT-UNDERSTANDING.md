# Project understanding — implemented P0

Signal Reducers turns repetitive telemetry, conversation fragments, and simulated video packets into bounded observations. A local CPU decision model routes each accepted signal to zero or more independent reducers. Reducers use hosted summaries; deterministic state holds their observations. A hosted decision gate decides acknowledgement, resolution, read-only status requests, and whether one larger LLM call is warranted.

The operator is a developer testing the architecture with synthetic inputs. The first useful workflow monitors `demo.worker`, answers a submitted question about degradation, and keeps the underlying health incident open until new healthy evidence arrives.

## Model allocation

| Responsibility | Model | Placement |
|---|---|---|
| Signal routing | `convaiinnovations/laya-typed-decisions` | CPU, explicit device, pinned checkpoint |
| System 1 | `nace-ai/drex-v1.5` | OpenRouter typed `/systemone` decisions |
| Reducer summaries | `openai/gpt-4.1-mini` | OpenRouter, non-reasoning |
| System 2 answer | `openai/gpt-5.1` | One OpenRouter completion with none/low/high effort |

System 2 has no application completion-token budget. System 1 chooses its thinking level. Only the validated gate runtime can acknowledge or change resolution.

## Source inventory and precedence

The [README](../../README.md) is the current entry point. These eight prototype documents describe the approved implementation, with [08-PROTOTYPE-BUILD-SPEC.md](08-PROTOTYPE-BUILD-SPEC.md) taking precedence. Earlier conceptual drafts are retained only in a private local archive and are excluded from release artifacts. [Historical qualification](../qualification/2026-10-12.md) and [current release verification](../qualification/release-0.1.0a1.md) distinguish measured source from later changes.

Success requires a working fixture workflow, actual CPU routing qualification, hosted-model qualification, reproducible setup, and visible failures. Passing mock tests alone does not qualify live mode.
