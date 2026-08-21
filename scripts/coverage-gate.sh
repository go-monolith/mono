#!/usr/bin/env bash
#
# coverage-gate.sh — enforce a minimum statement-coverage floor.
#
# Reads a Go coverage profile, strips the packages that are out of scope for
# the floor, prints the per-package breakdown, and exits non-zero when the
# remaining figure is below the threshold.
#
# Usage:
#   scripts/coverage-gate.sh [profile] [threshold]
#   COVERAGE_FILE=coverage.out COVERAGE_THRESHOLD=80 scripts/coverage-gate.sh
#
# Normally invoked as `make test-coverage-check`, which produces the profile
# first.

set -euo pipefail

PROFILE="${1:-${COVERAGE_FILE:-coverage.out}}"
THRESHOLD="${2:-${COVERAGE_THRESHOLD:-80}}"
FILTERED="${COVERAGE_FILTERED:-coverage.filtered.out}"

# Packages excluded from the floor. Keep this list in lockstep with the
# `ignore:` list in codecov.yml — the two drifting apart is how the badge and
# the gate end up reporting different numbers. internal/covergate enforces
# that; it parses this assignment, so changing its shape fails those tests.
#
#   examples/  documentation programs, no tests by design
#   bench/     benchmarks and the benchmark JSON parser
#   test/      integration suite, run separately under -tags=integration
EXCLUDE_RE='^github\.com/go-monolith/mono/(examples|bench|test)/'

# A full unit-test run reports well over 5000 in-scope statements. A profile
# from a single package would be a few hundred and would still yield a
# plausible-looking percentage, so refuse to measure one at all rather than
# report a number computed over almost nothing.
MIN_STATEMENTS="${COVERAGE_MIN_STATEMENTS:-4000}"

if [ ! -f "$PROFILE" ]; then
	echo "coverage-gate: no coverage profile at '$PROFILE'" >&2
	echo "" >&2
	echo "Produce one with:" >&2
	echo "  make test-coverage-ci" >&2
	echo "or:" >&2
	echo "  go test -race -covermode=atomic -coverprofile=$PROFILE ./..." >&2
	exit 1
fi

# The first line of a profile is the `mode:` header; everything after it is one
# coverage block per line. Keep the header, drop the out-of-scope blocks.
head -n 1 "$PROFILE" >"$FILTERED"
grep -Ev "$EXCLUDE_RE" <(tail -n +2 "$PROFILE") >>"$FILTERED" || true

statements="$(awk 'NR > 1 { total += $2 } END { print total + 0 }' "$FILTERED")"
if [ "$statements" -lt "$MIN_STATEMENTS" ]; then
	echo "coverage-gate: only $statements in-scope statements in '$PROFILE' (expected at least $MIN_STATEMENTS)." >&2
	echo "That profile is truncated or stale — measuring it would report a meaningless percentage." >&2
	echo "Re-run a full profile with: make test-coverage-ci" >&2
	exit 1
fi

echo "Coverage by package (excluding examples/, bench/, test/):"
go tool cover -func="$FILTERED"
echo ""

total="$(go tool cover -func="$FILTERED" | awk '/^total:/ { gsub(/%/, "", $NF); print $NF }')"

if [ -z "$total" ]; then
	echo "coverage-gate: could not parse a total from 'go tool cover -func=$FILTERED'" >&2
	exit 1
fi

# awk rather than bc: bc is not guaranteed to be present in CI images.
if awk -v have="$total" -v want="$THRESHOLD" 'BEGIN { exit !(have + 0 < want + 0) }'; then
	echo "FAIL: coverage ${total}% is below the required ${THRESHOLD}% floor." >&2
	echo "$statements in-scope statements were measured; see the table above for the least-covered packages." >&2
	exit 1
fi

echo "OK: coverage ${total}% meets the ${THRESHOLD}% floor ($statements in-scope statements)."
