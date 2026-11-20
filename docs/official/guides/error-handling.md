# Error Handling Guide

This guide explains what happens to an error a handler returns, which errors the framework itself returns, and how to design errors that callers can act on. Read it when you write service handlers or code that calls other modules' services.

**Time to complete:** 15 minutes

**Prerequisites:**
- Basic understanding of Go error handling
- Familiarity with [Module basics](../core-concepts/modules.md)

---

## Overview

The framework follows Go conventions (errors are values; check them immediately; wrap with `fmt.Errorf("...: %w", err)`, never `%v`, so `errors.Is` and `errors.As` keep working) and adds:

- a fixed way for request-reply handler errors to reach the caller (below),
- sentinel errors and error types in `pkg/errors` (see [Framework Error Types](#framework-error-types)),
- logging of every error a handler returns, so handlers need not log it themselves.

The snippets on this page import the framework errors as `monoerrors`:

```go
import (
    "errors"
    "fmt"

    "github.com/go-monolith/mono"
    monoerrors "github.com/go-monolith/mono/pkg/errors"
)
```

## How Handler Errors Reach the Caller

When a request-reply handler returns an error, the framework logs it at Error level (`RequestReply handler error`) and replies with these headers:

| Header | Value |
|--------|-------|
| `Mono-Error` | `true` |
| `Mono-Error-Message` | `err.Error()`: the full wrapped message |
| `Mono-Error-Type` | The type name of the returned error, lower-cased, without an `Error` suffix: `*OrderError` gives `order`, `errors.New` gives `errorstring`, `fmt.Errorf("...%w", err)` gives `wrap`. The framework's own values are `overloaded`, `handler-panic` and `shutting-down`. |

On the caller side:

- The framework's own types map to `ErrServiceOverloaded`, `ErrHandlerPanic` and `ErrServiceUnavailable`, so `errors.Is` works for them.
- Any other error becomes a `*monoerrors.RemoteError` whose `Unwrap` returns nil: **sentinel identity is lost** across the call. Inspect `.Message` (the handler's `err.Error()`) and `.ErrorType` instead.

```go
resp, err := client.Call(ctx, data)
if remote, ok := monoerrors.GetRemoteError(err); ok {
    // remote.Message is the handler's err.Error(), remote.ErrorType its type name
    if remote.ErrorType == "order" {
        // ...
    }
}
```

Consequences for handler code:

- Because the full wrapped message reaches the caller, keep internal details in logs, not in returned errors.
- To give callers a usable `ErrorType`, return the custom error type unwrapped.
- `errors.Is(err, ErrInsufficientFunds)` with a sentinel of your own package works only for local calls.

## Service Handler Error Patterns

### Request-Reply Handlers

```go
func (m *OrderModule) handleGetOrder(ctx context.Context, req *GetOrderRequest) (*Order, error) {
    if req.OrderID == "" {
        return nil, errors.New("order ID required")  // Client sees this message
    }

    order, err := m.findOrder(ctx, req.OrderID)
    if err != nil {
        return nil, fmt.Errorf("failed to find order: %w", err)
    }

    return order, nil
}
```

### Queue Group Handlers

Queue group handlers use fire-and-forget semantics: errors are **not** returned to the sender. The framework logs a returned error at Error level (`QueueGroup handler error`), so add context to the error rather than logging it again:

```go
func (m *OrderModule) handleProcessOrder(ctx context.Context, order *Order) error {
    if err := m.validateOrder(ctx, order); err != nil {
        // Logged by the framework, not propagated to sender
        return fmt.Errorf("validate order %s: %w", order.ID, err)
    }

    return nil
}
```

### Channel Service Handlers

Channel handlers own their error handling:

```go
func (m *OrderModule) listenForOrderUpdates(ctx context.Context, updateChan <-chan *OrderUpdate) {
    for update := range updateChan {
        if err := m.applyUpdate(ctx, update); err != nil {
            // Handle error (log, retry, or notify)
            m.logger.Error("Failed to apply update",
                "order_id", update.OrderID,
                "error", err,
            )
            // Decide: retry, skip, or stop listening?
        }
    }
}
```

### Handler Panics

A panic inside any handler (request-reply, queue group, event consumer, stream consumer, event stream consumer, cron) is recovered by the framework:

- It is logged at `Error` level with `panic=true` and the stack trace, and the subscription keeps running, so one bad message does not crash the process.
- A request-reply caller receives `ErrHandlerPanic` immediately instead of waiting for a timeout.
- Acknowledgement is unchanged: a cron occurrence is Nak'd, while a stream or event stream consumer batch is neither Ack'd nor Nak'd, so the messages the handler did not acknowledge are redelivered after `AckWait` (bounded by `MaxDeliver`).

```go
resp, err := client.Call(ctx, data)
if errors.Is(err, monoerrors.ErrHandlerPanic) {
    // The handler crashed on this request; it was logged with a stack trace on the handling instance.
}
```

Recovery is a safety net, not a substitute for returning errors; a handler that panics on every delivery still burns its redeliveries.

### Overloaded Services

Request-reply, queue group and event consumer handlers run on a bounded worker pool per subscription (request-reply pools several workers by default; queue groups and event consumers have one worker unless they opt in). When every worker is busy and the pending queue is full, a request-reply call or an ACK-mode queue group send fails fast with `ErrServiceOverloaded` instead of waiting. The message was **not** handled, so the caller may retry; with several instances the retry usually lands on one with capacity.

```go
resp, err := client.Call(ctx, data)
if errors.Is(err, monoerrors.ErrServiceOverloaded) {
    // This instance is saturated; back off and retry, or degrade gracefully
    return m.retryLater(ctx, data)
}
```

Events and optimistic queue group sends carry no reply subject, so they are not rejected: the subscription waits for a free slot instead. See [Handler Concurrency](../api/container.md#handler-concurrency-worker-pools) for the options.

## Handling Framework Errors

### Sentinel Error Checking

```go
// m.payment is the mono.ServiceLookup stored in SetDependencyServiceContainer.
func (m *OrderModule) chargeClient() (mono.RequestReplyServiceClient, error) {
    client, err := m.payment.GetRequestReplyService("charge")
    if err != nil {
        // Check if it's a missing service error
        if errors.Is(err, monoerrors.ErrServiceNotFound) {
            return nil, fmt.Errorf("payment service not configured")
        }
        return nil, fmt.Errorf("failed to get payment service: %w", err)
    }

    return client, nil
}
```

### Module Boundary and Contract Errors

Since v0.1.0 the framework reports boundary problems with three sentinel errors:

| Error | When |
|-------|------|
| `ErrServiceContractMismatch` | A contract names a different module or service kind than the registered service |
| `ErrMissingDependency` | A `ConsumeServices()` contract names a module that is not in `Dependencies()`, or a declared dependency is not registered |
| `ErrModuleBoundaryViolation` | A module crosses an undeclared boundary while `WithStrictModuleBoundaries` is on |

Contract problems surface from `app.Start`, wrapped in `ErrModuleStartFailed`:

```go
if err := app.Start(ctx); err != nil {
    switch {
    case errors.Is(err, monoerrors.ErrServiceNotFound):
        // a consumed service is not registered by its provider
    case errors.Is(err, monoerrors.ErrServiceContractMismatch):
        // a consumed service has a different kind than the contract
    case errors.Is(err, monoerrors.ErrMissingDependency):
        // a consumed contract's module is missing from Dependencies()
    }
    log.Fatalf("start: %v", err)
}
```

See the [Module Boundaries Guide](module-boundaries.md).

### Configuration Errors

Configuration errors occur during framework setup:

```go
app, err := mono.NewMonoApplication(
    mono.WithJetStreamStorageDir("/tmp/jetstream"), // also enables JetStream
)
if err != nil {
    // Could be ConfigurationError with specific option that failed
    var confErr *monoerrors.ConfigurationError
    if errors.As(err, &confErr) {
        fmt.Printf("Configuration option %s failed: %v\n",
            confErr.OptionName, confErr.Err)
    }
    return err
}
```

`ConfigurationError.OptionIndex` is currently always 0, so print `OptionName` and `Err` only.

### Framework Error Types

All in `monoerrors "github.com/go-monolith/mono/pkg/errors"`.

| Sentinel | Meaning |
|----------|---------|
| `ErrModuleNotFound`, `ErrModuleAlreadyRegistered` | Module lookup or registration |
| `ErrServiceNotFound`, `ErrServiceAlreadyRegistered` | Service lookup or registration |
| `ErrCircularDependency`, `ErrMissingDependency` | Dependency resolution |
| `ErrServiceUnavailable` | The handling instance is shutting down, or the service cannot be reached |
| `ErrServiceOverloaded` | The worker pool and its queue are full |
| `ErrHandlerPanic` | The handler panicked; the panic was recovered and logged |
| `ErrInvalidConfiguration` | An option rejected its value |
| `ErrModuleStartFailed`, `ErrModuleStopFailed`, `ErrModulePanic` | Module lifecycle failures |
| `ErrContainerNotBound` | Container used before it was bound to a module |
| `ErrPluginNotFound`, `ErrPluginAlreadyRegistered` | Plugin lookup or registration |
| `ErrServiceContractMismatch`, `ErrModuleBoundaryViolation` | Module boundaries (see above) |
| `ErrStreamConfigImmutable` | `Start`: a stream's storage type, or its retention to or from work-queue, differs from the existing stream's, or a setting that can only be switched on is configured off. See [Changing a Stream's Storage Type or Retention](../api/container.md#changing-a-streams-storage-type-or-retention) |
| `ErrWorkQueueConsumerConflict` | `Start`: a consumer on a work-queue stream whose subjects another consumer already covers (two registrations of the application, or the durable of a renamed module or service). See [Work-Queue Streams: One Consumer per Subject](../api/container.md#work-queue-streams-one-consumer-per-subject) |
| `ErrStreamSubjectsOverlap` | `Start`: a stream asks for subjects another stream holds (a renamed stream, or a service switched between stream consumer and cron). See [Renaming a Stream, or Switching a Service Between Stream Consumer and Cron](../api/container.md#renaming-a-stream-or-switching-a-service-between-stream-consumer-and-cron) |

| Type | Fields | Helpers |
|------|--------|---------|
| `*ModuleError` | `ModuleName`, `Operation`, `Err` | `IsModuleError` |
| `*ServiceError` | `ServiceName`, `ModuleName`, `ServiceType`, `Err` | `IsServiceError`, `GetServiceError` |
| `*DependencyError` | `Module`, `Dependency`, `Chain`, `Err` | `IsDependencyError` |
| `*ConfigurationError` | `OptionIndex` (currently always 0), `OptionName`, `Value`, `Err` | `IsConfigurationError`, `GetConfigurationError` |
| `*TimeoutError` | `Operation`, `Duration`, `Err`; `Timeout()` returns true | `IsTimeoutError` |
| `*EventStreamError` | `StreamName`, `Subject`, `Operation`, `Err` | `IsEventStreamError`, `GetEventStreamError` |
| `*RemoteError` | `Message`, `ServiceName`, `ModuleName`, `ErrorType`; `Unwrap` returns nil | `IsRemoteError`, `GetRemoteError` |

Every type except `RemoteError` unwraps to its `Err`, so `errors.Is(err, monoerrors.ErrServiceNotFound)` works through them. `AggregateErrors(errs)` joins several errors into one.

## Creating Custom Error Types

A custom type carries structured detail; sentinels let callers check specific failures:

```go
// OrderError represents an order-related error
type OrderError struct {
    OrderID string
    Code    string
    Message string
    Err     error
}

func (e *OrderError) Error() string {
    if e.Err != nil {
        return fmt.Sprintf("order error [%s] (%s): %v", e.Code, e.OrderID, e.Err)
    }
    return fmt.Sprintf("order error [%s] (%s): %s", e.Code, e.OrderID, e.Message)
}

func (e *OrderError) Unwrap() error {
    return e.Err
}

// Sentinel errors for specific order failures
var (
    ErrOrderNotFound = errors.New("order not found")
    ErrInvalidOrder  = errors.New("invalid order")
    ErrOrderLocked   = errors.New("order is locked")
)

func (m *OrderModule) findOrder(ctx context.Context, orderID string) (*Order, error) {
    order, found := m.orders[orderID]
    if !found {
        return nil, &OrderError{
            OrderID: orderID,
            Code:    "NOT_FOUND",
            Message: "Order does not exist",
            Err:     ErrOrderNotFound,
        }
    }

    return order, nil
}
```

Returned unwrapped from a request-reply handler, an `*OrderError` reaches the caller with `ErrorType` `order` (see [How Handler Errors Reach the Caller](#how-handler-errors-reach-the-caller)). Locally, check it with `errors.As`:

```go
var orderErr *OrderError
if errors.As(err, &orderErr) && orderErr.Code == "NOT_FOUND" {
    return nil, fmt.Errorf("order %s does not exist", req.OrderID)
}
```

## Error Propagation Patterns

- **Propagate as-is** an error that already has context (from a dependency); **wrap** one that needs local context.
- In **multi-step operations**, log intermediate failures at Warn, continue where possible, and return a summary error:

```go
func (m *OrderModule) processOrderBatch(ctx context.Context, orders []*Order) error {
    var processedCount int
    var lastErr error

    for _, order := range orders {
        if err := m.processOrder(ctx, order); err != nil {
            m.logger.Warn("Failed to process order",
                "order_id", order.ID,
                "error", err,
            )
            lastErr = err
            continue  // Process remaining orders
        }
        processedCount++
    }

    if lastErr != nil {
        return fmt.Errorf("batch processing failed (processed %d of %d): %w",
            processedCount, len(orders), lastErr)
    }

    return nil
}
```

- In **retries**, log each failed attempt at Warn (`m.logger.WithError(err).Warn("Operation failed, retrying", "attempt", attempt)`) and return the last error wrapped.

## Error Response Patterns

{% hint style="danger" %}
**Security:** Never expose internal error details (stack traces, database errors, file paths) to external clients. The full message of a returned error reaches the caller, so log the details and return a simple message.
{% endhint %}

```go
func (m *OrderModule) handleGetOrder(ctx context.Context, req *GetOrderRequest) (*Order, error) {
    order, err := m.findOrder(ctx, req.OrderID)

    if errors.Is(err, ErrOrderNotFound) {
        return nil, fmt.Errorf("order not found")
    }

    if err != nil {
        // Log the details, return a generic message
        m.logger.Error("Failed to retrieve order",
            "order_id", req.OrderID,
            "error", err,
        )
        return nil, errors.New("internal error")
    }

    return order, nil
}
```

For APIs, a short error code in the message (for example `errors.New("creation_failed")`, or `fmt.Errorf("validation_error: %w", err)`) lets clients categorize failures.

## Testing Error Cases

Test both success and error paths:

```go
func TestHandleCreateOrder_Validation(t *testing.T) {
    module := &OrderModule{logger: newTestLogger()}

    testCases := []struct {
        name    string
        req     *CreateOrderRequest
        wantErr bool
        errMsg  string
    }{
        {name: "missing customer ID", req: &CreateOrderRequest{Amount: 100}, wantErr: true, errMsg: "customer ID required"},
        {name: "invalid amount", req: &CreateOrderRequest{CustomerID: "c1", Amount: 0}, wantErr: true, errMsg: "amount must be positive"},
        {name: "valid request", req: &CreateOrderRequest{CustomerID: "c1", Amount: 100}, wantErr: false},
    }

    for _, tt := range testCases {
        t.Run(tt.name, func(t *testing.T) {
            _, err := module.handleCreateOrder(context.Background(), tt.req)

            if (err != nil) != tt.wantErr {
                t.Errorf("unexpected error: %v", err)
            }

            if tt.wantErr && !strings.Contains(err.Error(), tt.errMsg) {
                t.Errorf("error message %q does not contain %q", err.Error(), tt.errMsg)
            }
        })
    }
}
```

## Checklist

Before returning an error from a function:

- [ ] Is the error from a dependency that already has context? Propagate as-is.
- [ ] Is this a new error? Add context using `fmt.Errorf(...%w...)`.
- [ ] Should the caller know about this specific error? Create a sentinel error or custom type (and, across a service call, return the custom type unwrapped).
- [ ] Is this a handler error? Return it; the framework logs it. Don't log it as well.
- [ ] Is this a user-facing error? Remove internal details and provide a user-friendly message.

Don't ignore errors with `_`, and don't panic in libraries (reserved for `main` or unrecoverable errors).

## Related Documentation

- [Middleware System](../middleware/README.md)
- [Framework Error Types](#framework-error-types)
- [Testing Guide](testing.md)
- [Logging Guide](logging.md)
