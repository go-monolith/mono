# Integration Tests

This directory contains integration tests for the mono-framework that test the complete system with real NATS server instances.

## Running Integration Tests

Integration tests are skipped by default when running `go test`. To run them:

```bash
# Run all integration tests
go test -tags=integration ./test/integration/...

# Run with verbose output
go test -tags=integration -v ./test/integration/...

# Run with race detector
go test -tags=integration -race ./test/integration/...

# Run a specific test
go test -tags=integration -v -run TestIntegration_BasicFrameworkLifecycle ./test/integration/...
```

## Test Coverage

The integration tests cover:

1. **Basic Framework Lifecycle** - Framework initialization, module registration, start, stop
2. **Multi-Module Dependencies** - Multiple modules with dependency chains
3. **Event Publishing and Subscription** - NATS messaging between modules
4. **Graceful Shutdown** - Proper cleanup and module shutdown ordering
5. **Health Checks** - Framework and NATS health status aggregation

## Test Requirements

- **NATS Embedded Server**: Tests use the embedded NATS server (no external NATS required)
- **Time**: Integration tests may take 30+ seconds due to NATS startup times
- **Resources**: Tests create temporary NATS servers which are cleaned up automatically

## Skipping Integration Tests

Integration tests are automatically skipped when:
- Running `go test -short`
- Running without the `-tags=integration` flag

This allows fast unit test runs during development while still providing comprehensive integration testing for CI/CD pipelines.
