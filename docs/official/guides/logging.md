# Logging Guide

This guide shows how to configure the framework logger, how to hand it to your modules, and how to write useful structured log lines: key-value context, log levels and common mistakes. Read it when you set up logging for an application or add logging to a module.

**Time to complete:** 15 minutes

**Prerequisites:**
- Familiarity with Go's `log/slog` package
- Understanding of [Module basics](../core-concepts/modules.md)

---

## Overview

The Monolith Framework provides a structured logging interface (`mono.Logger`) backed by Go's `log/slog` package.

- The framework creates one logger, configured with the options below, and returns it from `app.Logger()`.
- Modules do **not** receive a logger automatically: pass `app.Logger().WithModule(name)` to the module's constructor and keep it in a field (`m.logger`). The examples in this guide use that field.
- The framework already logs every error a handler returns (see [Framework Log Lines](#framework-log-lines)).

{% hint style="info" %}
**Structured logging:** Always use key-value pairs instead of string concatenation. This makes logs searchable and parseable by monitoring tools.
{% endhint %}

## Accessing the Logger

### Passing the Framework Logger to a Module

```go
type OrderModule struct {
    logger mono.Logger
}

func NewOrderModule(logger mono.Logger) *OrderModule {
    return &OrderModule{logger: logger.WithModule("orders")}
}

// In main.go
app, _ := mono.NewMonoApplication()
app.Logger().Info("Application created")
app.Register(NewOrderModule(app.Logger()))
```

### Using Go's slog Package

You can also call Go's standard `log/slog` package directly (`slog.Info("Order module starting")`). Package-level `slog` writes to slog's default logger: it is **not** affected by `WithLogLevel`, `WithLogFormat` or `WithLogOutput`.

## Configuration

### Logger Options

The framework logger is configured when the application is created; the level cannot be changed at runtime.

```go
app, err := mono.NewMonoApplication(
    mono.WithLogLevel(mono.LogLevelInfo),     // Default: Info
    mono.WithLogFormat(mono.LogFormatJSON),   // Default: mono.LogFormatText
    mono.WithLogOutput(os.Stderr),            // Default: os.Stdout
    mono.WithLogSource(true),                 // Default: false (adds file:line)
)
```

| Option | Description | Default |
|--------|-------------|---------|
| `WithLogLevel(level)` | `mono.LogLevelDebug`, `LogLevelInfo`, `LogLevelWarn`, `LogLevelError` | `LogLevelInfo` |
| `WithLogFormat(format)` | `mono.LogFormatText` or `mono.LogFormatJSON` | `LogFormatText` |
| `WithLogOutput(w)` | Output writer (nil rejected) | `os.Stdout` |
| `WithLogSource(enable)` | Include source file and line | `false` |
| `WithLogger(logger)` / `WithCustomLogger(logger)` | Use your own logger instead; it must implement `mono.Logger` (the level, format, output and source options are then ignored) | - |
| `WithNATSLogging(debug, trace, sysTrace)` | Route the embedded NATS server's debug/trace logs through the framework logger | all `false` |

### Framework Log Lines

The framework logs every error a handler returns, at Error level with an `error` field (and `panic`/`stack` fields for a recovered panic):

- `RequestReply handler error`
- `QueueGroup handler error`
- `Event consumer handler error`
- `Event stream consumer handler error`
- `Cron handler error`
- `Handler error` (stream consumer services)

A handler that returns an error does not need to log it as well: add context to the returned error instead.

## Logger Interface

`mono.Logger` provides four log levels and three context methods:

```go
type Logger interface {
    // Log messages at different levels
    Debug(msg string, args ...any)
    Info(msg string, args ...any)
    Warn(msg string, args ...any)
    Error(msg string, args ...any)

    // Add context to logger
    With(args ...any) Logger              // Add arbitrary context
    WithModule(moduleName string) Logger  // Set module name
    WithError(err error) Logger           // Attach error
}
```

## Structured Logging with Key-Value Pairs

### Basic Logging

```go
m.logger.Info("Processing order",
    "order_id", order.ID,
    "customer_id", customer.ID,
    "amount", order.Amount,
)
```

Output with `mono.WithLogFormat(mono.LogFormatJSON)` (the default format is text):

```json
{
  "time": "2025-01-15T10:30:45.123Z",
  "level": "INFO",
  "msg": "Processing order",
  "order_id": "ord-123",
  "customer_id": "cust-456",
  "amount": 10000
}
```

### Adding Request Context

With the [Request ID middleware](../middleware/requestid.md), read the request ID from the handler context:

```go
func (m *OrderModule) handleCreateOrder(ctx context.Context, req *CreateOrderRequest) (*Order, error) {
    requestID := requestid.GetRequestID(ctx)

    m.logger.Info("Creating order",
        "request_id", requestID,
        "customer_id", req.CustomerID,
        "amount", req.Amount,
    )

    // ...
}
```

### Adding Error Context

Put the context into the error you return; the handler that finally returns it is logged by the framework:

```go
func (m *OrderModule) processPayment(ctx context.Context, order *Order) error {
    result, err := m.paymentService.Charge(ctx, order.Amount)
    if err != nil {
        return fmt.Errorf("payment for order %s (amount %d) failed: %w", order.ID, order.Amount, err)
    }

    m.logger.Info("Payment processed successfully",
        "order_id", order.ID,
        "transaction_id", result.TransactionID,
    )

    return nil
}
```

## Using With() for Context

### Adding Persistent Context

`With()` returns a new logger with additional context:

```go
func (m *OrderModule) handleCreateOrder(ctx context.Context, customerID string) error {
    custLogger := m.logger.With("customer_id", customerID)

    custLogger.Info("Starting order creation")

    // All logs from custLogger include customer_id
    if err := m.validateCustomer(ctx, customerID); err != nil {
        return fmt.Errorf("validate customer %s: %w", customerID, err)
    }

    custLogger.Info("Order creation completed")
    return nil
}
```

### Using WithModule() and Chaining

`WithModule()` adds the `module` attribute. Call it once, on the framework logger (as in the constructor above); calling it again on a logger that already has it adds a second `module` attribute.

```go
logger := app.Logger().
    With("request_id", "req-123").
    With("customer_id", "cust-456").
    WithModule("orders")

logger.Info("Processing request")
// Text output: time=... level=INFO msg="Processing request" request_id=req-123 customer_id=cust-456 module=orders
```

## Choosing Log Levels

| Level | Use for |
|-------|---------|
| Debug | Detailed information about function execution, variable values and state transitions, entry/exit points; information only needed for troubleshooting. Typically disabled in production: don't rely on it for critical information. |
| Info | Successful completion of operations, significant state changes, important business events, module startup/shutdown |
| Warn | Recoverable errors that are handled, deprecated API usage, configuration issues with fallbacks, performance degradation |
| Error | Unexpected failures that impact functionality, external service failures, data integrity issues. Error logs should be actionable: Error implies "someone needs to investigate this"; if nothing can be done, consider Warn. |

### Debug

```go
func (m *OrderModule) validateOrder(order *Order) error {
    m.logger.Debug("Validating order",
        "order_id", order.ID,
        "has_items", len(order.Items) > 0,
    )

    if order.ID == "" {
        m.logger.Debug("Validation failed", "reason", "empty order ID")
        return errors.New("order ID required")
    }

    m.logger.Debug("Validation passed", "order_id", order.ID)
    return nil
}
```

### Info

```go
func (m *OrderModule) handleCreateOrder(ctx context.Context, req *CreateOrderRequest) (*Order, error) {
    order := &Order{ID: generateID()}

    if err := m.persistOrder(ctx, order); err != nil {
        // Logged by the framework as "RequestReply handler error"
        return nil, fmt.Errorf("persist order %s: %w", order.ID, err)
    }

    m.logger.Info("Order created successfully",
        "order_id", order.ID,
        "customer_id", req.CustomerID,
    )

    return order, nil
}
```

### Warn

A handled, recoverable failure: log it and carry on.

```go
func (m *OrderModule) processOrderBatch(ctx context.Context, orders []*Order) error {
    var failedOrders []string

    for _, order := range orders {
        if err := m.processOrder(ctx, order); err != nil {
            m.logger.Warn("Failed to process order",
                "order_id", order.ID,
                "error", err,
            )
            failedOrders = append(failedOrders, order.ID)
        }
    }

    if len(failedOrders) > 0 {
        m.logger.Warn("Batch processing completed with failures",
            "total", len(orders),
            "failed", len(failedOrders),
            "failed_orders", failedOrders,
        )
    }

    return nil
}
```

### Error

In a handler, an error you return is already logged at Error by the framework, so add context to the error instead of logging it again:

```go
func (m *OrderModule) handleGetOrder(ctx context.Context, orderID string) (*Order, error) {
    order, err := m.findOrder(ctx, orderID)
    if err != nil {
        // Logged by the framework as "RequestReply handler error"
        return nil, fmt.Errorf("failed to retrieve order %s: %w", orderID, err)
    }

    if order == nil {
        return nil, fmt.Errorf("order %s not found", orderID)
    }

    return order, nil
}
```

## Logging Patterns

### Request Lifecycle Logging

Log the start and the successful end of a request yourself; failures are logged by the framework with the error you return:

```go
func (m *OrderModule) handleCreateOrder(ctx context.Context, req *CreateOrderRequest) (*Order, error) {
    logger := m.logger.With("request_id", requestid.GetRequestID(ctx))

    startTime := time.Now()
    logger.Info("Request started",
        "operation", "create_order",
        "customer_id", req.CustomerID,
    )

    if err := m.validateRequest(req); err != nil {
        return nil, fmt.Errorf("validate request: %w", err)
    }

    order, err := m.createOrder(ctx, req)
    if err != nil {
        return nil, fmt.Errorf("create order: %w", err)
    }

    logger.Info("Request completed successfully",
        "order_id", order.ID,
        "duration_ms", time.Since(startTime).Milliseconds(),
    )

    return order, nil
}
```

### Error Recovery Logging

Log retries at Warn; the final error is returned (and logged once by whoever handles it):

```go
func (m *OrderModule) retryableOperation(ctx context.Context) error {
    var lastErr error

    for attempt := 1; attempt <= 3; attempt++ {
        if err := m.doOperation(ctx); err != nil {
            lastErr = err
            if attempt < 3 {
                m.logger.Warn("Operation failed, retrying",
                    "attempt", attempt,
                    "error", err,
                    "next_retry_in_seconds", attempt,
                )
                time.Sleep(time.Duration(attempt) * time.Second)
            }
            continue
        }

        m.logger.Info("Operation succeeded", "attempt", attempt)
        return nil
    }

    return fmt.Errorf("operation failed after 3 attempts: %w", lastErr)
}
```

### Performance Monitoring

```go
func (m *OrderModule) handleListOrders(ctx context.Context) ([]*Order, error) {
    startTime := time.Now()

    orders, err := m.fetchOrders(ctx)
    if err != nil {
        return nil, fmt.Errorf("fetch orders: %w", err)
    }

    durationMs := time.Since(startTime).Milliseconds()
    m.logger.Info("Orders fetched",
        "count", len(orders),
        "duration_ms", durationMs,
        "slow", durationMs > 100, // Flag slow queries
    )

    return orders, nil
}
```

## Integration with Monitoring Systems

### Log Structured Fields for Monitoring

```go
m.logger.Info("Service metrics",
    "service_name", m.Name(),
    "request_count", atomic.LoadInt64(&m.requestCount),
    "error_count", atomic.LoadInt64(&m.errorCount),
    "avg_latency_ms", m.calculateAverageLatency(),
)
```

### Combining Logs with Traces

```go
func (m *OrderModule) handleCreateOrder(ctx context.Context, req *CreateOrderRequest) (*Order, error) {
    logger := m.logger.With(
        "request_id", requestid.GetRequestID(ctx),
        "trace_id", extractTraceID(ctx), // your tracer's ID, if available
    )

    logger.Info("Creating order")

    // Framework's request ID middleware automatically propagates
    // request IDs to downstream calls for distributed tracing
    // (request-reply, queue group and stream consumer calls; not events)
    return m.createOrder(ctx, req)
}
```

### Health Status Logging

```go
// Health implements mono.HealthCheckableModule.
func (m *OrderModule) Health(ctx context.Context) mono.HealthStatus {
    // Check health...

    if healthy {
        m.logger.Debug("Health check passed")
        return mono.HealthStatus{Healthy: true, Message: "operational"}
    }
    m.logger.Warn("Health check failed",
        "reason", "database unavailable",
    )
    return mono.HealthStatus{Healthy: false, Message: "database unavailable"}
}
```

## Avoiding Common Logging Mistakes

{% hint style="danger" %}
**Security:** Never log passwords, API keys, tokens, credit card numbers, or other sensitive data. Even in debug logs. Logs are often stored and accessed by many systems.
{% endhint %}

```go
// ❌ BAD: Logging passwords
m.logger.Info("User login",
    "username", user.Username,
    "password", user.Password,  // ❌ SECURITY ISSUE
)

// ✅ GOOD: Only log safe information
m.logger.Info("User login attempt",
    "username", user.Username,
    "success", true,
)
```

```go
// ❌ BAD: The same error logged twice
if err := m.doSomething(); err != nil {
    m.logger.Error("Operation failed", "error", err)
    m.logger.Error("Critical error", "error", err)  // ❌ Duplicate
}

// ❌ BAD: Generic message without context
m.logger.Error("Failed")

// ✅ GOOD: Log once, with context
if err := m.doSomething(); err != nil {
    m.logger.Error("Failed to process order",
        "order_id", order.ID,
        "error", err,
    )
}
```

## Checklist

- [ ] Pass the framework logger to each module (`app.Logger().WithModule(name)`)
- [ ] Use structured key-value pairs, never string concatenation
- [ ] Include relevant context (IDs, amounts, users) and request IDs
- [ ] Use appropriate log levels; don't use Debug for critical information
- [ ] Log each error once: a handler returns it, the framework logs it
- [ ] Don't log sensitive information
- [ ] Log performance metrics where relevant

## Related Documentation

- [Error Handling Guide](error-handling.md)
- [Testing Guide](testing.md)
- [Access Log Middleware](../middleware/accesslog.md)
- [Request ID Middleware](../middleware/requestid.md)
- [Core Concepts - Modules](../core-concepts/modules.md)
