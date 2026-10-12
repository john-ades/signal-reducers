# Running Signal Reducers

The first supported development target is macOS on Apple Silicon with Python 3.14, `uv`, and a `redis-server` executable on `PATH`. Other platforms are unqualified. Native Laya routing always uses CPU; hosted inference uses OpenRouter. Fixture and live modes are explicit.

## Install and try the fixture

Install `uv` and Redis through your preferred package manager, then run from the checkout:

```sh
uv python install 3.14
uv sync --locked
uv run signal-reducers demo --mode fixture --output outputs/demo
```

The demo starts and stops an isolated Redis process. Reports are `summary.json`, `trace.jsonl`, and `final-state.json` under the selected output directory. Model doubles exercise production transport, reduction, state, and lifecycle behavior without hosted calls or a checkpoint download.

For an installed wheel or extracted source distribution, `signal-reducers --help` exposes the same CLI. The routing evaluation corpus is package data, so its default does not depend on a repository-relative `datasets/` directory.

## Live credentials and Redis

The service reads `OPENROUTER_API_KEY` from its process environment. It does not automatically load `.env`. Set the variable through your usual secret-management method; keep credentials out of TOML, publisher records, logs, and committed files.

For explicit live verification scripts, copy `.env.example` to the ignored `.env` and replace the placeholder locally. Those scripts accept `OPENROUTER_API_KEY` and the legacy alias `OPENROUTER_KEY`. They parse the assignment without executing shell code and map it to the canonical variable only in the test process. Live checks make paid calls only when their command explicitly requests inference.

Start dedicated transient Redis in another terminal:

```sh
redis-server --bind 127.0.0.1 --port 6380 --save '' --appendonly no \
  --maxmemory 256mb --maxmemory-policy noeviction
```

Then check the configuration:

```sh
uv run signal-reducers check --config config.example.toml
uv run signal-reducers check --config config.example.toml --probe-models
uv run signal-reducers evaluate-router --config config.example.toml \
  --output outputs/router-evaluation.json
uv run signal-reducers demo --mode live --output outputs/live-demo
```

`check` verifies authenticated access and model capabilities without inference. `--probe-models` explicitly calls hosted models and loads Laya. Authentication failure stops readiness before inference. The first native load downloads approximately 842 MB of checkpoint files into the Hugging Face user cache; standard Hugging Face cache environment variables can select another location. Model weights are neither committed nor included in release archives.

`evaluate-router` makes no OpenRouter calls. It evaluates complete-context routing, predicted history, and CPU quality/performance, and exits unsuccessfully when an original threshold fails. The historical routing-accuracy tolerance for P0 does not change these recorded thresholds or waive throughput. See [historical results](docs/qualification/2026-10-12.md).

Live demos pace the 100 identical reports at one per second; the separate burst script admits them simultaneously. Paced runs take at least 100 seconds and do not establish lossless burst handling. Every escalation makes one HTTP attempt at the gate-selected thinking level. A client timeout or cancellation does not guarantee that the provider stops inference or reverses charges.

## Persistent service and publishers

Choose a fresh run UUID and share it with publishers:

```sh
uv run signal-reducers run --config config.example.toml \
  --run-id 3b7df725-569d-4475-993e-48ef5f33db2f
```

Interactive commands are `status`, `recheck`, and `quit`; `Ctrl-C` also shuts down. Active namespaces cannot reuse a UUID. After a crash, use a new UUID; transient keys expire rather than resume work.

Save a publisher file such as `signals.json`:

```json
[
  {
    "source": "worker.telemetry",
    "source_seq": 1,
    "modality": "structured",
    "mimetype": "application/json",
    "payload_json": {
      "type": "telemetry",
      "service": "worker",
      "status": "degraded",
      "metrics": {"error_rate": 0.4, "queue_depth": 50}
    }
  }
]
```

`error_rate` is a fraction: `0.4` means 40%. Exactly one of `payload_json` and `payload_base64` is required.

```sh
uv run signal-reducers publish --config config.example.toml \
  --run-id 3b7df725-569d-4475-993e-48ef5f33db2f --file signals.json
```

An accepted receipt does not guarantee processing. Destructive pops, age expiry, and full destination mailboxes can lose signals; an `outcome_unknown` must not be retried automatically. Native router timeout marks the router unhealthy and stops further pops. Source strings identify streams but do not authenticate producers.

## Verification and reports

Conformance tests use model doubles or mocked HTTP plus actual loopback Redis. They do not establish live model quality:

```sh
uv run --locked ruff check .
uv run --locked ruff format --check .
uv run --locked pytest -q -ra
```

For artifact construction and installation smoke tests, follow the ordered [release procedure](docs/RELEASING.md). Build archives before invoking `scripts/validate_release.py dist`.

Explicit scripts are available for separately scoped live measurements:

```sh
uv run python scripts/live_probes.py --output outputs/live/probes.json
uv run python scripts/live_targeted.py --output outputs/live/targeted.json
uv run python scripts/live_gate_contexts.py --output outputs/live/gate-contexts.json
uv run python scripts/live_system2_profiles.py --context representative
uv run python scripts/live_numeric.py
uv run python scripts/live_idle.py
uv run python scripts/live_burst.py
uv run python scripts/live_workflows.py demo --mode live --runs 10 --output outputs/live/workflows
```

Targeted reducer/DREX cases and profile checks use synthetic captured state with real provider calls. Workflows use all live components. The offline `scripts/audit_live_workflows.py` checks saved invariants and can compare a manifest captured from the exact source used for that batch; an old historical manifest must not be used to certify modified code. The runtime workflow report does not independently grade numerical answer accuracy.

Generated reports can contain captured observation text and typed facts. Keep `outputs/` private, review before sharing, and publish only curated aggregate evidence. Credentials and raw reasoning are excluded from normal diagnostics. Reports are not replay or restart data.

The [2026-10-12 qualification record](docs/qualification/2026-10-12.md) describes the frozen pre-cleanup source: 205 conformance tests, 9/10 runtime workflows, 6/10 combined factual successes, and unmet CPU targets. It is not a new qualification of this alpha. Current artifact checks and any narrowly scoped new measurements belong in [release verification](docs/qualification/release-0.1.0a1.md).
