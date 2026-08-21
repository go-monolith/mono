# Code Coverage

This page documents how test coverage is measured and enforced for the Monolith Framework. Coverage uses Go's built-in tooling in atomic mode, which is required for accurate counts under `-race`.

## Overview

The live coverage figure is published to Codecov — that dashboard, not this page, is the source of truth:

[![codecov](https://codecov.io/gh/go-monolith/mono/graph/badge.svg)](https://codecov.io/gh/go-monolith/mono)

| Metric | Value |
|--------|-------|
| **Enforced floor** | 80% statement coverage |
| **Enforced by** | `scripts/coverage-gate.sh`, run in CI on every pull request and every push to `main` |
| **Local command** | `make test-coverage-check` |
| **Dashboard** | [codecov.io/gh/go-monolith/mono](https://codecov.io/gh/go-monolith/mono) |

The floor is a hard gate: a pull request that drops in-scope coverage below 80% fails CI. It is expected to ratchet upward over time — lowering `COVERAGE_THRESHOLD` to turn a red build green is not the fix.

## Running Coverage Locally

```bash
# Produce a profile and enforce the 80% floor (what CI runs)
make test-coverage-check

# Profile only, no gate
make test-coverage-ci

# Profile plus an HTML report for browsing uncovered lines
make test-coverage
open coverage.html        # macOS
xdg-open coverage.html    # Linux

# Check against a stricter floor without editing anything
make COVERAGE_THRESHOLD=90 test-coverage-check
```

`make test-coverage-check` prints the full per-package breakdown before its verdict, so a failure names the packages that need tests.

## Coverage Exclusions

Three trees are excluded from the enforced figure. Including them would measure how much example and benchmark code the repository carries rather than how well the framework itself is tested — `examples/` alone drags the raw number from ~92% down to ~61%.

Expect the exact figure to move a little with the Go release you build under: the compiler's statement-counting granularity changes between versions, so the same tree reports 3807 in-scope statements under Go 1.26 and 5226 under Go 1.27 (91.2% and 92.4% respectively). The percentage barely shifts, but any check written against an absolute statement count has to tolerate the spread.

| Excluded | Why |
| --- | --- |
| `examples/` | Documentation programs, built and run by `make run-example-*` rather than unit tested |
| `bench/` | Performance benchmarks and the benchmark JSON parser |
| `test/` | Integration suite, run separately via `make test-integration` under `-tags=integration` |

This list lives in two places and the two must stay in lockstep:

- `EXCLUDE_RE` in [`scripts/coverage-gate.sh`](../../../scripts/coverage-gate.sh) — governs the enforced floor
- `ignore:` in [`codecov.yml`](../../../codecov.yml) — governs the badge and the dashboard

If they drift apart the badge and the gate report different numbers for the same commit, and nothing at runtime notices. That lockstep is enforced by [`internal/covergate`](../../../internal/covergate/exclusions_test.go), a test-only package that parses both files and fails when they name different directories. It runs under `make test`, `make test-short` and `make check`, so a mismatch fails CI rather than sitting undetected.

The same tests also check that both files agree with the module path in `go.mod`. `codecov.yml` needs its `fixes:` entry to strip the `github.com/go-monolith/mono/` import-path prefix that Go writes into every profile line — without it Codecov's root-anchored ignore globs match nothing, and every exclusion silently becomes a no-op.

Changing the exclusions therefore means editing both files together. If the shape of either changes enough that the guard can no longer parse it, the guard fails loudly and asks to be updated — that is deliberate, since a drift guard that quietly parses nothing is worse than no guard at all.

## Coverage in CI

Coverage runs on a single Go version leg of the pull-request matrix — the figure does not meaningfully vary by toolchain version, and running it on all six would send Codecov six duplicate uploads of the same commit. The push-to-`main` workflow uploads too, which is what gives Codecov a base report for per-pull-request deltas.

Each workflow runs three separate steps rather than one combined target:

1. **Coverage profile** — `make test-coverage-ci`
2. **Upload to Codecov** — `codecov/codecov-action@v5`, marked `continue-on-error` because pull requests from forks cannot read `CODECOV_TOKEN`
3. **Coverage floor** — `scripts/coverage-gate.sh`

The upload deliberately comes before the gate, so a run that fails the floor still publishes the numbers that explain why.

## Improving Coverage

When contributing to the framework, ensure:

1. **New code has tests** — all new public functions and methods should have corresponding tests
2. **Edge cases are covered** — include tests for error conditions and boundary cases
3. **Coverage doesn't decrease** — the Codecov comment on your pull request shows exactly which lines your diff left uncovered

## Related Documentation

- [Testing Guide](../guides/testing.md) — how to write tests for modules
- [Error Handling](../guides/error-handling.md) — error handling patterns to test
