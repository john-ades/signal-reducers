# Preparing an experimental release

Version 0.1.0a1 is an experimental source/wheel release for macOS on Apple Silicon and Python 3.14. Local artifact preparation does not publish a repository, upload a package, or establish production/live qualification.

## Offline release checks

Run these from the checkout with `uv` and `redis-server` installed. Update the version/changelog first, review license metadata and notices, and keep `uv.lock` synchronized. Dependency setup can download packages; subsequent checks make no hosted calls or checkpoint downloads.

```sh
uv sync --locked
uv run --locked ruff check .
uv run --locked ruff format --check .
uv run --locked pytest -q -ra --junitxml=outputs/release-tests.xml
```

Inspect the test summary and reject a release run with skipped conformance tests. A missing Redis executable must not silently omit transport/runtime coverage. CI additionally asserts a zero skipped count from JUnit XML.

Run ten independent fixture workflows before building:

```sh
uv run --locked signal-reducers demo --mode fixture --runs 10 \
  --output outputs/release-fixture-workflows
```

Build **before** validating archives. Use a distribution directory containing exactly the intended wheel and source distribution; move older artifacts into private storage before building.

```sh
uv build --no-build-isolation
uv run --locked twine check --strict dist/*.whl dist/*.tar.gz
uv run --locked python scripts/validate_release.py dist
```

Twine checks artifact metadata only and does not upload. The validator requires one wheel and one source distribution, checks archive contents/version/license and the packaged 240-development/80-held-out corpus hash, then installs each artifact separately. It excludes credentials, private archives, outputs, downloaded weights, caches, IDE files, and personal absolute paths. `--archives-only` skips installation checks and is insufficient for a complete release pass.

For each install smoke, the validator creates a fresh temporary virtual environment and working directory, installs the project from the wheel or source archive, verifies both CLI entry points/version and package-resource origin/hash, then runs the fixture demo against isolated Redis. Source-archive installation builds the project afresh with build isolation disabled. Native and build dependencies are reused from the frozen development environment through a `.pth` file; installs use `--no-deps --no-build-isolation --offline`, and OpenRouter credentials are removed from the smoke environment.

This verifies fresh **project installation** and operation outside the checkout, including an independently built source archive. It is not a hermetic environment or fresh dependency resolution across platforms. The validator checks that the imported project is inside the temporary environment rather than the editable checkout. Default CI follows this same method.

Record commands, test counts/skips, fixture results, environment, and installation method in [release verification](qualification/release-0.1.0a1.md). Supply exact archive hashes in an accompanying `SHA256SUMS` release asset. Preserve [historical measurements](qualification/2026-10-12.md) separately. If source/docs change after a build, rebuild and revalidate the artifacts that will actually be distributed.

The 0.1.0a1 preparation additionally tested fully isolated environments using `uv export --locked --no-emit-project --format requirements.txt`, then `uv pip install --python <fresh-venv-python> --require-hashes -r <exported-file>`. Install each artifact with `uv pip install --python <fresh-venv-python> --no-deps --no-build-isolation --offline <artifact>` and run the CLI, fixture, and resource checks from a temporary working directory with provider credentials removed. This does not use the validator's parent dependency `.pth` shortcut. Registry downloads were needed for some exact locked dependency wheels; native-cache and fixture checks remained offline. A separately authorized native-cache smoke can select an existing cache through `HF_HUB_CACHE` and make one CPU routing prediction without hosted calls.

## Local source release artifacts

After inspecting publishable content, initialize local Git on `main` if needed, stage only project files, and verify that `.env`, outputs, weights, private archives, and environments are ignored. Commit the reviewed source and create the local `v0.1.0a1` tag. Prepare the GitHub source asset from that tag:

```sh
git archive --format=tar.gz --prefix=signal-reducers-0.1.0a1/ \
  --output=dist/signal-reducers-0.1.0a1-source.tar.gz v0.1.0a1
shasum -a 256 dist/signal_reducers-0.1.0a1-py3-none-any.whl \
  dist/signal_reducers-0.1.0a1.tar.gz \
  dist/signal-reducers-0.1.0a1-source.tar.gz
```

Inspect source-archive members with the same content exclusions as the distributions and save the checksum lines as `SHA256SUMS`. Keep the wheel/source distribution isolated from this additional asset when running Twine; its metadata checker accepts distributions only. Use the [experimental release notes](releases/v0.1.0a1.md) with the source, wheel, source distribution, and checksums. Local commits, tags, and archives do not publish anything.

## Explicit funded model checks

These commands load a locally supplied live credential, send requests to providers, and consume credits. They are separate from offline CI:

```sh
uv run --locked python scripts/live_numeric.py
uv run --locked python scripts/live_workflows.py demo --mode live --runs 10 \
  --output outputs/release-live-workflows
```

Review numeric answers against authoritative facts, and full workflow answers against captured evidence. Runtime-only workflow success cannot establish factual accuracy. The workflow wrapper captures a source manifest in its output directory. Compare it with the offline invariant audit:

```sh
uv run --locked python scripts/audit_live_workflows.py outputs/release-live-workflows \
  --manifest outputs/release-live-workflows/execution-manifest.json
```

This audit makes no provider calls and does not grade answer prose. New numeric checks or prompt fixes do not replace a frozen ten-workflow qualification or clear CPU throughput targets. Never copy raw states, traces, manifests, or provider bodies into public qualification evidence; curate aggregates after review.

## Publish separately

Review the concrete source commit and built artifacts before choosing a public repository or package destination. Configure real project URLs and a private security-reporting channel when that destination exists; do not invent them in package metadata. Tag and upload the exact reviewed artifacts only when publication has been authorized. If package-index publication is used, verify name availability and install the published artifact afterward.

Original code/docs use 0BSD. Dependencies, Redis, downloaded weights, and hosted services keep their own licenses and terms; this release does not bundle or relicense them.
