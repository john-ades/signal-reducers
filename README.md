# Signal Reducers

An experimental Python service that turns repeated telemetry, conversation fragments, and simulated video packets into immutable observations. Independent reducer workers summarize meaningful changes, and a validated decision gate chooses when a larger LLM call is useful.

**Version 0.1.0a1 is an experimental alpha.** macOS on Apple Silicon with Python 3.14 is the first supported development environment. Redis transport is best effort, and generated answers need factual review. Historical live testing found numerical-answer errors and CPU throughput below target; see the [qualification record](docs/qualification/2026-10-12.md) and [release verification](docs/qualification/release-0.1.0a1.md).

| Component | Model | Execution |
|---|---|---|
| Signal router | `convaiinnovations/laya-typed-decisions` | Local CPU, pinned checkpoint, four threads |
| System 1 gate | `nace-ai/drex-v1.5` | OpenRouter typed decisions |
| Three reducers | `openai/gpt-4.1-mini` | OpenRouter, no thinking |
| System 2 | `openai/gpt-5.1` | One OpenRouter call per escalation |

System 1 selects `none`, `some`, or `extra` thinking, mapped to OpenRouter effort `none`, `low`, or `high`. System 2 has no application-set completion budget, planning framework, tool loop, continuation, or automatic retry. Only validated System 1 decisions can acknowledge observations, resolve them, or reserve operations.

## Try the fixture demo

With [uv](https://docs.astral.sh/uv/) and `redis-server` installed, run from the checkout:

```sh
uv python install 3.14
uv sync --locked
uv run signal-reducers demo --mode fixture --output outputs/demo
```

The demo starts isolated Redis, exercises repeat suppression, diagnosis, recovery, revisions, and failures, and writes private reports. Fixture mode uses model doubles and makes no hosted calls or model download. It exercises the same transport, reducers, state, and lifecycle code used in live mode.

## Run live models

Set `OPENROUTER_API_KEY` in the process environment, then follow [RUNNING.md](RUNNING.md) for Redis setup and readiness checks. Live calls consume provider credits. The pinned router checkpoint downloads on first native load into the Hugging Face user cache; it is not bundled with this project.

```sh
uv run signal-reducers check --config config.example.toml
uv run signal-reducers demo --mode live --output outputs/live-demo
uv run signal-reducers evaluate-router --output outputs/router-evaluation.json
```

The three reducers are conversation, telemetry, and simulated video. Video input is synthetic JSON metadata carried as bytes; the prototype does not interpret real media. Accepted signals can expire or be dropped, and reports do not provide replay or crash recovery.

## Develop and release

```sh
uv run --locked pytest -q
```

Read the [architecture](docs/prototype/04-TECHNICAL-ARCHITECTURE.md), [build specification and acceptance criteria](docs/prototype/08-PROTOTYPE-BUILD-SPEC.md), [contribution guide](CONTRIBUTING.md), [security policy](SECURITY.md), and [release procedure](docs/RELEASING.md).

Original project code and documentation use the [Zero-Clause BSD license](LICENSE). Dependencies, downloaded model weights, and hosted services keep their own licenses and terms; see [third-party notices](THIRD_PARTY_NOTICES.md).
