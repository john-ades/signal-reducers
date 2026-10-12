# Open questions and risks

Model allocation and bounded execution are settled: CPU Laya routing, hosted DREX decisions, hosted reducers/System 2, three thinking levels, and no System 2 completion budgets. This is an experimental alpha; implementation completion and artifact verification do not imply full live qualification.

The [2026-10-12 historical record](../qualification/2026-10-12.md) retains the frozen pre-cleanup measurements: 205 conformance tests, 9/10 runtime workflows, 6/10 combined factual successes, one five-second routing timeout, .878483 signals/second, and 50 expired reports in the simultaneous 100-report burst. A prototype-specific routing-accuracy exception covers only the accepted historical error rate. CPU throughput and factual quality remain open. [Current release verification](../qualification/release-0.1.0a1.md) records subsequent checks separately.

| Risk | Required behavior or evidence |
|---|---|
| Laya routing accuracy | Preserve original targets and measured pinned-checkpoint results; no silent rule/model fallback. The recorded P0 exception does not apply broadly to future models or registry changes. |
| CPU latency and expiry | Retain the five-second deadline and unhealthy-router shutdown. Measure throughput and drops separately from paced workflow completion. |
| Context truncation or option collapse | Exact tokenizer preflight and native diagnostics rejection; preserve captured metadata/history/definitions. |
| Native thread hangs | Stop pops on unhealthy routing; terminating the operator process may be necessary for a stuck native thread. |
| Conflicting DREX answers | Captured-only candidates and complete atomic validation before any dispositions or operation reservation. |
| Gate request overhead | Pack complete facts/provenance within the whole-request bound; defer complete groups rather than truncate evidence. |
| Generated numbers conflict with source facts | Declare fractional units and typed-fact precedence in model prompts; test contradictory summaries and assess actual answers independently of citation membership. |
| Invented root cause, trends, or policies | Captured evidence bounds claims; unknown cause stays unknown and internal reducer activity is distinct from monitored service facts. |
| Diagnosis incorrectly resolves health | Require newer current same-subject healthy evidence or withdrawal; record missing lifecycle coverage explicitly. |
| Hosted failures and late results | Deadlines, strict result bounds, correlation/current applicability, one System 2 attempt, terminal deduplication, and safe failures. |
| Hidden repeated inference | Count native predictions and every hosted tier; independently measure repeat suppression and thirty-second idle behavior. |
| State or mailbox pressure | Bound subjects/payloads/records/operations, pin outstanding evidence, and reject overflow atomically. |
| Redis destructive loss | Document best effort, unknown admission outcomes, expiry/mailbox drops, and no replay or durable recovery. |
| Dependencies and downloaded weights | Python 3.14/native compatibility and artifact checks on the supported host; preserve separate third-party licenses. |
| Secret or data disclosure | Environment credentials, private generated outputs, synthetic public examples, and no raw provider bodies or reasoning in normal diagnostics. |

One sample per thinking level cannot establish general model quality. Valid schemas, correlation, citations, and lifecycle checks cannot establish numerical accuracy. Requalification requires new frozen workflow/factual and CPU evidence for the source being claimed. Real media, guaranteed delivery, external writes, or multiple operators would require a new scope decision.
