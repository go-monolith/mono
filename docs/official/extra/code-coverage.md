# Code Coverage

This page documents the test coverage statistics for the Monolith Framework. Coverage is measured using Go's built-in coverage tools with atomic mode for accurate results.

## Overview

| Metric | Value |
|--------|-------|
| **Overall Coverage** | 90.1% |
| **Test Command** | `go test -coverprofile=coverage.out -covermode=atomic ./...` |
| **Last Updated** | December 2024 |

## Coverage by Package

### Public API Packages (`pkg/`)

| Package | Coverage | Description |
|---------|----------|-------------|
| `pkg/types` | 100.0% | Core interfaces and type definitions |
| `pkg/errors` | 99.4% | Error handling utilities |
| `pkg/helper` | 99.1% | Helper functions for common tasks |

### Internal Implementation (`internal/`)

| Package | Coverage | Description |
|---------|----------|-------------|
| `internal/logger` | 100.0% | Logger implementation |
| `internal/middleware` | 100.0% | Middleware chain execution |
| `internal/eventbus` | 97.5% | EventBus implementation (NATS wrapper) |
| `internal/registry` | 97.7% | Module and event registry |
| `internal/app` | 96.9% | Framework application lifecycle |
| `internal/lifecycle` | 96.7% | Module lifecycle and startup order |
| `internal/container` | 96.2% | ServiceContainer implementation |
| `internal/nats` | 91.4% | NATS server setup and embedded server |

### Middleware (`middleware/`)

| Package | Coverage | Description |
|---------|----------|-------------|
| `middleware/requestid` | 100.0% | Request ID injection middleware |
| `middleware/accesslog` | 99.0% | Access logging middleware |
| `middleware/audit` | 99.0% | Audit logging middleware |

### Plugins (`plugin/`)

| Package | Coverage | Description |
|---------|----------|-------------|
| `plugin/fs-jetstream` | 99.3% | File storage plugin for JetStream |
| `plugin/kv-jetstream` | 79.8% | Key-Value storage plugin |

## Coverage Targets

The framework maintains the following coverage targets:

| Category | Target | Current Status |
|----------|--------|----------------|
| Public API (`pkg/`) | 95%+ | Achieved (99%+) |
| Internal Core (`internal/`) | 90%+ | Achieved (96%+) |
| Middleware | 95%+ | Achieved (99%+) |
| Plugins | 80%+ | Achieved |
| **Overall** | **85%+** | **Achieved (90.1%)** |

## Running Coverage Locally

To generate a coverage report locally:

```bash
# Run tests with coverage (excluding examples)
go test -coverprofile=coverage.out -covermode=atomic ./...

# View coverage summary
go tool cover -func=coverage.out

# Generate HTML report
go tool cover -html=coverage.out -o coverage.html

# Open HTML report in browser
open coverage.html  # macOS
xdg-open coverage.html  # Linux
```

## Coverage Exclusions

The following directories are excluded from coverage metrics:

- `examples/` - Example code for documentation purposes
- `bench/` - Performance benchmarks
- `test/integration/` - Integration tests (tests themselves are excluded)

## Improving Coverage

When contributing to the framework, ensure:

1. **New code has tests** - All new public functions and methods should have corresponding tests
2. **Edge cases are covered** - Include tests for error conditions and boundary cases
3. **Coverage doesn't decrease** - Pull requests should maintain or improve overall coverage

### Areas for Improvement

| Package | Current | Target | Notes |
|---------|---------|--------|-------|
| `plugin/kv-jetstream` | 79.8% | 90%+ | Additional edge case testing needed |
| `internal/nats` | 91.4% | 95%+ | More error path coverage needed |

## Related Documentation

- [Testing Guide](../guides/testing.md) - How to write tests for modules
- [Error Handling](../guides/error-handling.md) - Error handling patterns to test

---

Coverage statistics are updated with each major release. For the most current coverage data, run the tests locally using the commands above.
