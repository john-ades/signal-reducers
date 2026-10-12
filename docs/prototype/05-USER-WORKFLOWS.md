# User workflows

Run commands from the project root after `uv sync --locked`. Live mode requires `OPENROUTER_API_KEY` in the environment. Fixture mode is explicit and never a live fallback.

## W01 Setup and readiness

Start a dedicated Redis with persistence disabled, 256 MiB maximum memory, and no eviction. `check` validates configuration/catalog without paid inference. `check --probe-models` explicitly requests bounded model probes. `evaluate-router` loads the pinned CPU checkpoint and evaluates held-out cases.

## W02 Repeat suppression

Publish 100 identical ordered healthy telemetry signals. Each nonexpired signal reaches the router; telemetry produces one initial snapshot and one hosted summary. Later identical reports refresh freshness without creating semantic updates.

## W03 Delayed independent processing

Three agreeing contiguous synthetic-video packets must enter intake within one second, measured by their intake-owned `received_at` span. Routing may deliver them more slowly; assembly waits only until the first intake expiry, bounded by the existing signal age limit. Exceeding the input span and exhausting downstream waiting produce separate safe errors, without an asserted scene.

Fixture mode publishes degradation beside the first packet, then supplies the remaining two while telemetry completes independently. Live mode first verifies degradation, publishes all three video packets in one admission, captures their BUFFERING activity, and then submits the question. Complete segments enter delayed processing; both schedules preserve the same one-second input protocol.

## W04 Fragmented submitted question

Send `replace`, ordered `append`, and empty-text `submit` records with one message ID. Only complete coverage creates a committed request. Bare text, gaps, expiry, or an initial append cannot authorize an operation.

## W05 Diagnosis and result review

DREX selects one submitted request and none/some/extra thinking. The runtime reserves it before starting one System 2 call. A correlated result enters a later batch. DREX may resolve the answered request while the degraded health event remains unresolved.

## W06 Recovery

A newer committed healthy telemetry snapshot for the same subject supplies resolution evidence. The gate explicitly resolves the earlier unhealthy records. A diagnosis alone cannot do this.

## W07 Correction and retraction

A complete simulated reflection segment corrects prior stalled snapshot/event lineages. A subsequent retraction targets the corrected live leaves. Both remain separate semantic records; a retraction never restores an older snapshot.

## W08 Status and failure

A submitted processing-status request can select ACT and read current reducer activity. Invalid model output, unsupported routing context, missing fragments, full mailboxes, expired queue records, and provider errors remain visible without fabricated success.

## W09 Shutdown and restart

Stop intake, cancel bounded outstanding tasks, preserve terminal results where possible, close clients/executors, and delete run keys only while still owning them. Restart with a new UUID; diagnostics do not restore state.

```mermaid
sequenceDiagram
    participant P as Producer
    participant S as Observation state
    participant G as System 1 DREX
    participant L as System 2
    P->>S: Reduced degradation and submitted request
    S->>G: Immutable batch
    G->>S: Validated unresolved disposition and reservation
    G->>L: One captured-context call with selected thinking
    L->>S: Correlated answer result
    S->>G: Result, original request, relevant observations
    G->>S: Resolve request; retain unhealthy incident
    P->>S: New healthy telemetry observation
    S->>G: Recovery evidence and unresolved incident
    G->>S: Explicitly resolve incident
```

The [run guide](../../RUNNING.md) contains commands and publisher examples.
