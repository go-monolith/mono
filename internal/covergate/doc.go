// Package covergate guards the coverage tooling's own configuration.
//
// It deliberately contains no runtime code. What it checks is not Go source
// but three files that have to agree with each other:
//
//   - scripts/coverage-gate.sh, whose EXCLUDE_RE decides which packages count
//     toward the enforced coverage floor
//   - codecov.yml, whose ignore globs decide which packages count toward the
//     figure on the badge and dashboard, and whose fixes entry strips the
//     module prefix that Go writes into every profile line
//   - go.mod, whose module path both of the above hardcode
//
// Nothing at runtime forces those to match. When they drift the failure is
// silent in the worst way: CI stays green while the badge and the gate report
// different numbers for the same commit. Comments in both files ask for
// lockstep; the tests in this package are what enforce it.
//
// Living here rather than in a shell script is deliberate. Neither CI workflow
// runs "make check", so a script wired to that target would never guard
// anything on a pull request. A Go test is picked up by "go list ./..." and so
// runs under "make test", "make test-short" and both workflows without any
// extra wiring.
//
// Every parser here fails loudly rather than returning an empty result. A
// drift guard that quietly parses nothing is worse than no guard at all, so
// the parsers are pure functions returning errors and are themselves covered
// by table-driven tests — the guard's own logic must not be allowed to rot.
package covergate
