# Contributing

Signal Reducers is an experimental alpha. Start with the [build specification](docs/prototype/08-PROTOTYPE-BUILD-SPEC.md) and [known limitations](docs/qualification/2026-10-12.md). Contributions to original project code and documentation use the [0BSD license](LICENSE).

## Development

Use Python 3.14 and install `uv` and `redis-server`. From the checkout:

```sh
uv sync --locked
uv run --locked ruff check .
uv run --locked ruff format --check .
uv run --locked pytest -q -ra
uv run signal-reducers demo --mode fixture --output outputs/contribution-demo
```

Keep fixture tests offline and deterministic. Redis integration tests must actually run; a missing `redis-server` skips that coverage locally. Hosted tests are explicit opt-in commands, require your own OpenRouter credential, and consume credits. Do not put live credentials into CI or enable paid calls in the default test suite.

## Changes and review

Explain the problem, resulting behavior, and meaningful validation. Preserve immutable captured context, whole-decision atomic validation, gate-only lifecycle authority, independent reducer workers, CPU-only routing, and one System 2 attempt per escalation. Add regression cases when a behavior change can violate those contracts.

Model, prompt, registry, tokenizer, corpus, and routing changes need fresh evidence for affected claims. Keep historical reports intact and distinguish contract tests, fixture behavior, live component checks, full workflows, factual review, and CPU performance. A successful runtime assertion is not proof that an answer is numerically correct.

Keep `.env`, generated outputs, model caches, personal IDE settings, and raw provider/captured data out of commits. The packaged routing corpus contains synthetic examples; contributions to it must not include personal or customer data. Follow the [release procedure](docs/RELEASING.md) to build archives before running `scripts/validate_release.py dist`; archive validation also checks fresh project installations using the frozen native dependencies.
