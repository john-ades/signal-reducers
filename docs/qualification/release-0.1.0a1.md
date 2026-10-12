# Release verification — 0.1.0a1

Checks performed on **2026-10-12**, on Apple Silicon macOS with **CPython 3.14.4**, for the cleaned **0.1.0a1** experimental alpha. The [pre-cleanup qualification](2026-10-12.md) remains frozen and separate. [Sanitized aggregate metrics](release-0.1.0a1.json) contain counts and evidence hashes; raw diagnostics, prompts, answers, traces, credentials, and weights remain private.

## Current-source results

| Check | Result | Scope |
|---|---|---|
| Ruff lint and formatting | Passed for source, scripts, and tests | E4/E7/E9/F/I; no runtime architecture change |
| Complete conformance suite | **250 passed**, 0 failed/errors/skips, 18.268 seconds | Includes actual isolated Redis, provider failure/cancellation contracts, resource/cache behavior, numeric input regressions, and attempt-history rejection |
| Fixture workflows | **10/10 passed**, zero observed lifecycle violations | Independent runs with deterministic model doubles |
| Live telemetry numeric checks | **10/10 materially correct** | Five checks each at fraction `0.45` (45%) and `0.0045` (0.45%) |
| Adversarial live System 2 checks | **6/6 materially correct** | Both rates across `none`, `some`, and `extra`; misleading summary prose loses to typed source facts |
| Integrated live workflow | **1/1 passed** runtime and material factual review; **32/32** saved-trace audit checks passed | All nine scenario stages and four lifecycle assertions covered, zero observed violations |

The offline numeric regressions cover fraction values `0`, `0.0045`, `0.01`, `0.45`, and `1`, plus contradictory summary prose. Reviewed failed, partial, timed-out, and cancelled operations retain attempted initiators in subsequent captured batches: rechecks offer no duplicate initiator and cannot reserve another System 2 call. These tests also preserve the authoritative runtime rejection of forged duplicate operations and include attempt metadata in request-size accounting.

Every targeted live call used one HTTP attempt. All six System 2 checks preserved captured context, valid evidence/correlation, selected effort `none`/`low`/`high`, `reasoning.exclude=true`, response bounds, and the absence of completion-budget fields and tools. Independent factual review checked the generated answers separately from API/schema controls. No failed response was automatically retried or replaced.

The integrated run used **117 CPU Laya predictions**, 117 admitted/popped signals, and zero expiries. Each reducer made three hosted calls. System 1 made **23 HTTP attempts: 22 succeeded and one was cancelled during shutdown after the scenario completed**. System 2 made one operation/result/HTTP attempt at `some` (`low`) thinking. Its diagnosis correctly reported 45%, queue count 50, the stalled synthetic scene, and an unknown precise root cause. The paced 100-report repeat stage used one telemetry call, one snapshot, two System 1 calls, and zero System 2 calls; pacing is distinct from a simultaneous burst.

Both live manifests match all **30** recorded executable/configuration input hashes. The aggregate records a fingerprint and selected hashes, rather than publishing raw manifests. Documentation updates after these calls do not alter executable inputs.

## Packaging and installation

Wheel and source distribution build successfully with metadata 2.4, 0BSD/license metadata, and unchanged production dependency versions. Strict Twine metadata checks pass. Archive inspection excludes credentials, private diagnostics, local archives, generated files, model weights, caches, IDE files, and personal absolute paths. Public Markdown links resolve, and both the pattern-based scan and a comparison against the locally supplied credential found no credential matches in publishable content.

Each artifact was installed outside the checkout in its own fresh virtual environment with **69 exact, hash-locked dependencies plus the project**. These environments have no parent `.pth` reuse; all 70 distributions resolve inside the fresh environment. Both console/module entrypoints, `--version`, help, fixture operation with isolated Redis and zero lifecycle violations, packaged corpus access, and native imports pass. All 15 package/resource files match the artifact bytes. The source-distribution install independently rebuilds the project.

Actual pinned Laya loaded on CPU in both isolated environments and made one successful telemetry routing prediction per installation. `HF_HUB_CACHE` selected the existing checkpoint cache; no weights were downloaded into the installation directory and no hosted inference calls were made. These two predictions establish installation/cache behavior, not routing accuracy or throughput qualification. The automated release validator also passes its separate fresh-project smoke method using reused frozen dependencies, as documented in the release procedure.

The synthetic corpus is unchanged: 320 cases, with 240 development and 80 held-out cases, SHA-256 `041ec6daef323b8cb69df13758b5eadd694d5fc82e47d383b383c924114d33e0`. Default access uses package resources, and explicit `--dataset` overrides remain supported. Native checkpoint download uses the standard Hugging Face user cache and environment overrides. The live workflow reused an existing cache through `HF_HUB_CACHE`; weights are excluded from release archives.

The [release procedure](../RELEASING.md) records commands and the automated smoke-test method. macOS CI performs locked dependency setup, Ruff checks, conformance with Redis and zero skips, metadata checks, and packaging smoke checks. It makes no hosted inference calls. Hosted CI has not been run because no public repository has been published. Local Git uses `main`; release artifacts and their exact hashes are supplied with `SHA256SUMS`. No external publication has occurred.

## Remaining experimental limits

These sixteen targeted calls and one workflow support the numerical fix, not general model accuracy or a fresh ten-workflow success rate. Three telemetry summaries added interpretive wording beyond the supplied count/status facts, and one System 2 response departed from the requested paragraph formatting while meeting runtime bounds. The integrated answer also used unspecified queue-item terminology. These qualitative caveats are retained in private reviews and generated prose still needs factual review.

Full live model and CPU qualification has not been re-established. Historical throughput was **0.878483 signals/second**, below the original target, with a router timeout and queue expiry under a simultaneous burst. The original combined factual-workflow target also remains open. The accepted historical Laya routing-accuracy exception does not waive either target. This release is limited to Apple Silicon macOS with CPython 3.14; other platforms and PyPI publication remain unqualified/deferred.
