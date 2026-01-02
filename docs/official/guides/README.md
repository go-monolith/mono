# 📖 Guides

This section contains practical how-to guides for common development tasks with the Monolith Framework.

## Overview

The guides in this section provide step-by-step instructions and best practices for:

- **[Error Handling](error-handling.md)** - Handling errors in service handlers, creating custom error types, and error propagation patterns
- **[Testing](testing.md)** - Unit testing modules, integration testing with embedded NATS, and mocking dependencies
- **[Logging](logging.md)** - Using the Logger interface, structured logging best practices, and log level selection
- **[Storage Backends](storage-backends.md)** - Working with the unified storage interface, capability detection, and backend comparison

## Quick Links

### Error Handling Guide

Learn how to properly handle errors in your service handlers:

- Handle errors returned by dependencies
- Create domain-specific error types
- Propagate errors through call chains
- Log errors with proper context
- Return appropriate error responses to clients

**Start here:** [Error Handling Guide](error-handling.md)

### Testing Guide

Learn how to write comprehensive tests for your modules:

- Write unit tests for service handlers
- Integration test with embedded NATS
- Mock dependencies for isolated testing
- Test concurrent message handling
- Verify error scenarios

**Start here:** [Testing Guide](testing.md)

### Logging Guide

Learn how to implement effective structured logging:

- Access the framework Logger
- Add contextual information to logs
- Choose appropriate log levels
- Structured logging with key-value pairs
- Integration with monitoring systems

**Start here:** [Logging Guide](logging.md)

### Storage Backends Guide

Learn how to work with the unified storage interface:

- Understand the storage interface hierarchy
- Detect backend capabilities using type assertions
- Choose between fs-jetstream and kv-jetstream
- Implement optimistic locking with revisions
- Handle storage errors properly

**Start here:** [Storage Backends Guide](storage-backends.md)

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

```go
// Unit test a handler
func TestHandleCreateOrder(t *testing.T) {
    module := &OrderModule{}
    ctx := context.Background()

    result, err := module.handleCreateOrder(ctx, &CreateOrderRequest{})
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }

    if result.ID == "" {
        t.Error("order ID should not be empty")
    }
}
```

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
- [API Reference - Service Container](../api/container.md)
- [Middleware System](../middleware/README.md)

---

For more in-depth information, see the complete guides below.
