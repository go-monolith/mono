# 📖 Guides

This section contains practical how-to guides for common development tasks with the Monolith Framework. Each guide covers one task; pick the one that matches what you are doing:

- **[Error Handling](error-handling.md)** - Handling errors in service handlers, creating custom error types, and error propagation patterns
- **[Testing](testing.md)** - Domain tests with fakes, `monotest` tests without NATS, and integration testing with embedded NATS
- **[Module Boundaries](module-boundaries.md)** - Contract packages, ports and adapters, startup validation, the boundary report, strict mode, and the `monotest` conformance and import rule tests
- **[Clustering and Kubernetes](clustering.md)** - Clustered JetStream: server names, routes, route authentication, replicas, readiness and health, and a Kubernetes StatefulSet setup
- **[Upgrading from v0.1.x to v0.2.x](upgrading.md)** - What changes, what may need action, and how to roll back
- **[Logging](logging.md)** - Using the Logger interface, structured logging best practices, and log level selection
- **[Storage Backends](storage-backends.md)** - Working with the unified storage interface, capability detection, and backend comparison

## Common Patterns

### Error Handling Pattern

```go
// Handle error returned by dependency
result, err := someService.Do(ctx, data)
if err != nil {
    return fmt.Errorf("failed to do something: %w", err)
}

// Use result...
return nil
```

### Logging Pattern

```go
// Add context to logs
m.logger.Info("Processing request",
    "request_id", requestID,
    "customer_id", customerID,
    "amount", amount,
)
```

### Testing Pattern

Give every module a conformance test with `monotest.CheckModule`, which runs the module without NATS and reports every boundary rule it breaks. The test and its options are in the [Module Boundaries Guide](module-boundaries.md#testing-with-monotest); the [Testing Guide](testing.md) shows where it fits among the other tests.

## Best Practices Summary

### Error Handling

✓ **Do**
- Wrap errors with context using `fmt.Errorf` and `%w`
- Check errors immediately after function calls
- Propagate errors to the caller
- Log errors with debugging context

✗ **Don't**
- Ignore errors with blank identifier `_`
- Return generic error messages
- Lose error context by re-wrapping multiple times
- Panic in libraries (only in `main`)

### Testing

✓ **Do**
- Write unit tests for all public functions
- Use table-driven tests for multiple cases
- Mock external dependencies
- Add a `monotest.CheckModule` conformance test per module
- Test error scenarios
- Name test files with `_test.go` suffix

✗ **Don't**
- Skip error test cases
- Make tests dependent on execution order
- Leave test data files in the repository
- Use `time.Sleep()` for synchronization

### Logging

✓ **Do**
- Use structured logging with key-value pairs
- Include request IDs for tracing
- Log at appropriate levels
- Add context using `With()` method

✗ **Don't**
- Log sensitive data (passwords, tokens)
- Use generic messages without context
- Mix log levels arbitrarily
- Log the same information multiple times

## Related Documentation

- [Core Concepts - Modules](../core-concepts/modules.md)
- [Module Boundaries Guide](module-boundaries.md)
- [API Reference - Service Container](../api/container.md)
- [Middleware System](../middleware/README.md)
