# Documentation audit and decision record

Earlier conceptual drafts mixed architectural requirements, proposed technologies, and future extensions. Those drafts are privately archived. The current implementation resolves their material ambiguities as follows.

| Issue | Resolution |
|---|---|
| Framework alternatives and broad autonomous execution | Use plain async Python and one System 2 request; no planning/tool loop |
| Local LLM serving versus hosted inference | OpenRouter for reducers, DREX, and System 2; only Laya routing is local CPU inference |
| Generative gate proposals | DREX answers closed questions; an adapter compiles answers into a validated decision |
| Router deployment and training | Pin local Laya weights; evaluate reviewed development/held-out families; no prototype fine-tuning |
| Model-selected thinking | Three exact levels: none, some, extra; map to none, low, high |
| Completion budgets | Omit all System 2 completion/reasoning token-limit fields; retain the deadline and result contract |
| Repeated telemetry | Suppress identical source facts before reducer inference; refresh freshness only |
| Conversation completeness | Only an ordered full-known message followed by explicit submit can initiate work |
| Delayed media | Synthetic metadata packets demonstrate buffering/revisions, without real perception claims |
| Lifecycle conflation | Presentation, acknowledgement, and resolution are independent |
| Result authority | Correlated results return through an ordinary gate batch and cannot write dispositions |
| Health diagnosis versus recovery | A successful explanation resolves the question; healthy same-subject evidence is required for health resolution |
| Revision ambiguity | Same-area live-leaf lineage, no rollback, no overwrite of a newer unrelated snapshot |
| Best-effort delivery | Redis destructive pop, expiry and visible loss; no retries/replay guarantees |
| Batch starvation | Exact request-size packing and progress-dependent pagination include deferred recovery assessments |
| Immutable JSON | Canonical bytes inside frozen contracts; detached boundary copies |
| Unbounded work | Queue, mailbox, assembly, observation, operation, context, and trace caps |
| Unsupported live capabilities | Readiness and qualification fail explicitly; fixture adapters are never automatic fallbacks |

Historical framework/model proposals do not override the approved P0. Public documentation links only the current contracts and curated qualification evidence; generated states, traces, credentials, and downloaded weights remain private.

[Build specification](08-PROTOTYPE-BUILD-SPEC.md) and [risk register](07-OPEN-QUESTIONS-AND-RISKS.md) distinguish implemented mechanisms from measured model quality.
