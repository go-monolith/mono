---
paths: **/*_test.go
---

# Go Test File Rules (*_test.go)

When working with Go test files, follow these conventions based on package declaration choice.

## Package Declaration Strategy

Choose the appropriate package style based on testing intent:

### White-box Testing (Internal)
Use `package mypkg` (same as source) when:
- Testing unexported functions, variables, or internal logic
- Verifying implementation details not exposed in the public API
- Writing unit tests that need direct access to package internals

```go
// mypkg_test.go
package mypkg

func TestInternalHelper(t *testing.T) {
    result := internalHelper() // direct access to unexported function
    // ...
}
```

### Black-box Testing (External)
Use `package mypkg_test` (with `_test` suffix) when:
- Testing the package from a consumer's perspective
- Validating the public API ergonomics
- Ensuring tests don't rely on implementation details
- Avoiding circular import issues

```go
// mypkg_test.go
package mypkg_test

import (
    "testing"
    "path/to/mypkg"
)

func TestPublicAPI(t *testing.T) {
    result := mypkg.ExportedFunc() // only exported members accessible
    // ...
}
```

## Exposing Internals for External Tests

When using `package mypkg_test` but needing access to specific unexported members, create a bridge file:

1. Create `export_test.go` in the source package
2. Export only the specific internals needed for testing

```go
// export_test.go
package mypkg

// Expose unexported members for external tests
var InternalFuncForTest = internalFunc
var InternalVarForTest = internalVar
```

The external test can then access these via the exported aliases:

```go
// mypkg_test.go
package mypkg_test

func TestInternalViaExport(t *testing.T) {
    result := mypkg.InternalFuncForTest()
    // ...
}
```

## Decision Guide

| Scenario | Package Style | Rationale |
|----------|---------------|-----------|
| Unit testing private helpers | `package mypkg` | Needs unexported access |
| Integration/API tests | `package mypkg_test` | Tests consumer experience |
| Avoiding import cycles | `package mypkg_test` | Separate compilation unit |
| Testing one unexported func in API tests | `export_test.go` bridge | Controlled exposure |

## Naming Conventions

- Test files: `*_test.go` (e.g., `handler_test.go`)
- Test functions: `func TestXxx(t *testing.T)` where `Xxx` starts with capital letter
- Benchmark functions: `func BenchmarkXxx(b *testing.B)`
- Example functions: `func ExampleXxx()` with `// Output:` comments
- Export bridge files: `export_test.go` (conventional name)

## Default Preference

Prefer `package mypkg_test` (black-box) as the default approach. This encourages:
- Well-designed public APIs
- Tests that serve as usage documentation
- Decoupled test code that won't break from internal refactoring

Switch to `package mypkg` (white-box) only when internal access is genuinely required.