# Logging Guide

This guide covers structured logging best practices with the Monolith Framework's Logger interface, including using the logger, adding contextual information, choosing log levels, and integrating with monitoring systems.

**Time to complete:** 15 minutes

**What you'll learn:**
- Using the Logger interface effectively
- Structured logging with key-value pairs
- Choosing appropriate log levels
- Avoiding common logging mistakes

**Prerequisites:**
- Familiarity with Go's `log/slog` package
- Understanding of [Module basics](../core-concepts/modules.md)

---

## Overview

The Monolith Framework provides a structured logging interface compatible with Go's `log/slog` package. All modules receive a Logger instance from the framework, enabling consistent logging across your application.

{% hint style="info" %}
**Structured logging:** Always use key-value pairs instead of string concatenation. This makes logs searchable and parseable by monitoring tools.
{% endhint %}

## Logger Interface

The Logger interface provides four log levels and context methods:

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

## Accessing the Logger

### Using Go's slog Package

The recommended approach is to use Go's standard `log/slog` package directly:

```go
import "log/slog"

type OrderModule struct {}

func (m *OrderModule) Name() string {
    return "orders"
}

func (m *OrderModule) Start(ctx context.Context) error {
    slog.Info("Order module starting")

    // Initialize module...

    slog.Info("Order module started")
    return nil
}

func (m *OrderModule) Stop(ctx context.Context) error {
    slog.Info("Order module stopping")
    return nil
}
```

### Via Framework Logger

You can also access the framework's Logger via `app.Logger()`:

```go
// In main.go, after creating the application
app, _ := mono.NewMonoApplication()
logger := app.Logger()
logger.Info("Application created")
```

## Structured Logging with Key-Value Pairs

### Basic Logging

Use key-value pairs for context:

```go
m.logger.Info("Processing order",
    "order_id", order.ID,
    "customer_id", customer.ID,
    "amount", order.Amount,
)
```

Output in JSON format:
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

Add request IDs for tracing:

```go
func (m *OrderModule) handleCreateOrder(ctx context.Context, req *CreateOrderRequest) (*Order, error) {
    // Extract request ID from context (if using Request ID middleware)
    requestID := requestid.FromContext(ctx)

    // Log with request context
    m.logger.Info("Creating order",
        "request_id", requestID,
        "customer_id", req.CustomerID,
        "amount", req.Amount,
    )

    // ...
}
```

### Adding Error Context

Log errors with full context:

```go
func (m *OrderModule) processPayment(ctx context.Context, order *Order) error {
    result, err := m.paymentService.Charge(ctx, order.Amount)
    if err != nil {
        m.logger.Error("Payment processing failed",
            "order_id", order.ID,
            "amount", order.Amount,
            "error", err,
        )
        return fmt.Errorf("payment failed: %w", err)
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

The `With()` method returns a new logger with additional context:

```go
func (m *OrderModule) handleCreateOrder(ctx context.Context, customerID string) error {
    // Create a logger with customer context
    custLogger := m.logger.With("customer_id", customerID)

    custLogger.Info("Starting order creation")

    // All logs from custLogger include customer_id
    if err := m.validateCustomer(ctx, customerID); err != nil {
        custLogger.Error("Customer validation failed", "error", err)
        return err
    }

    custLogger.Info("Order creation completed")
    return nil
}
```

### Using WithModule()

Set the module name for logs:

```go
func (m *OrderModule) Start(ctx context.Context) error {
    // Create logger with module context
    logger := m.logger.WithModule(m.Name())

    logger.Info("Starting")
    // Output: module="orders", msg="Starting"

    return nil
}
```

### Chaining Context Methods

Chain multiple context additions:

```go
logger := m.logger.
    With("request_id", "req-123").
    With("customer_id", "cust-456").
    WithModule("orders")

logger.Info("Processing request")
// Output: request_id="req-123", customer_id="cust-456", module="orders"
```

## Choosing Log Levels

### Debug Level

{% hint style="info" %}
**Debug logs are typically disabled in production.** Use them freely for detailed tracing, but don't rely on them for critical information.
{% endhint %}

Use Debug for development and troubleshooting:

```go
func (m *OrderModule) validateOrder(order *Order) error {
    m.logger.Debug("Validating order",
        "order_id", order.ID,
        "has_items", len(order.Items) > 0,
        "total_amount", calculateTotal(order),
    )

    if order.ID == "" {
        m.logger.Debug("Validation failed", "reason", "empty order ID")
        return errors.New("order ID required")
    }

    m.logger.Debug("Validation passed", "order_id", order.ID)
    return nil
}
```

When to use Debug:
- Detailed information about function execution
- Variable values and state transitions
- Entry/exit points in complex functions
- Information only needed for troubleshooting

### Info Level

Use Info for important application events:

```go
func (m *OrderModule) handleCreateOrder(ctx context.Context, req *CreateOrderRequest) (*Order, error) {
    m.logger.Info("Creating order",
        "customer_id", req.CustomerID,
        "amount", req.Amount,
    )

    order := &Order{ID: generateID()}

    if err := m.persistOrder(ctx, order); err != nil {
        m.logger.Error("Failed to persist order", "error", err)
        return nil, err
    }

    m.logger.Info("Order created successfully",
        "order_id", order.ID,
        "customer_id", req.CustomerID,
    )

    return order, nil
}
```

When to use Info:
- Successful completion of operations
- Significant state changes
- Important business events
- Module startup/shutdown

### Warn Level

Use Warn for recoverable issues:

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

When to use Warn:
- Recoverable errors that are handled
- Deprecated API usage
- Configuration issues with fallbacks
- Performance degradation

### Error Level

{% hint style="warning" %}
**Error logs should be actionable.** If nothing can be done about it, consider Warn instead. Error implies "someone needs to investigate this."
{% endhint %}

Use Error for serious problems that need attention:

```go
func (m *OrderModule) handleGetOrder(ctx context.Context, orderID string) (*Order, error) {
    order, err := m.findOrder(ctx, orderID)
    if err != nil {
        m.logger.Error("Database query failed",
            "order_id", orderID,
            "error", err,
        )
        return nil, fmt.Errorf("failed to retrieve order: %w", err)
    }

    if order == nil {
        m.logger.Error("Order not found in database",
            "order_id", orderID,
        )
        return nil, fmt.Errorf("order not found")
    }

    return order, nil
}
```

When to use Error:
- Unexpected failures that impact functionality
- External service failures
- Data integrity issues
- Errors requiring operator attention

## Logging Patterns

### Request Lifecycle Logging

Log the full lifecycle of a request:

```go
func (m *OrderModule) handleCreateOrder(ctx context.Context, req *CreateOrderRequest) (*Order, error) {
    requestID := requestid.FromContext(ctx)
    logger := m.logger.With("request_id", requestID)

    startTime := time.Now()
    logger.Info("Request started",
        "operation", "create_order",
        "customer_id", req.CustomerID,
    )

    // Validate request
    if err := m.validateRequest(req); err != nil {
        logger.Warn("Request validation failed",
            "error", err,
            "duration_ms", time.Since(startTime).Milliseconds(),
        )
        return nil, err
    }

    // Process
    order, err := m.createOrder(ctx, req)
    if err != nil {
        logger.Error("Order creation failed",
            "error", err,
            "duration_ms", time.Since(startTime).Milliseconds(),
        )
        return nil, err
    }

    // Success
    logger.Info("Request completed successfully",
        "order_id", order.ID,
        "duration_ms", time.Since(startTime).Milliseconds(),
    )

    return order, nil
}
```

### Error Recovery Logging

Log when recovering from errors:

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
            } else {
                m.logger.Error("Operation failed after all retries",
                    "attempts", attempt,
                    "error", err,
                )
            }
            continue
        }

        m.logger.Info("Operation succeeded",
            "attempt", attempt,
        )
        return nil
    }

    return fmt.Errorf("operation failed: %w", lastErr)
}
```

### Performance Monitoring

Log performance metrics:

```go
func (m *OrderModule) handleListOrders(ctx context.Context) ([]*Order, error) {
    startTime := time.Now()

    orders, err := m.fetchOrders(ctx)
    if err != nil {
        m.logger.Error("Failed to fetch orders",
            "error", err,
            "duration_ms", time.Since(startTime).Milliseconds(),
        )
        return nil, err
    }

    duration := time.Since(startTime)
    durationMs := duration.Milliseconds()

    // Log performance metrics
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

Include fields that monitoring systems can parse:

```go
m.logger.Info("Service metrics",
    "service_name", m.Name(),
    "request_count", atomic.LoadInt64(&m.requestCount),
    "error_count", atomic.LoadInt64(&m.errorCount),
    "avg_latency_ms", m.calculateAverageLatency(),
)
```

### Combining Logs with Traces

Include trace information in logs:

```go
func (m *OrderModule) handleCreateOrder(ctx context.Context, req *CreateOrderRequest) (*Order, error) {
    requestID := requestid.FromContext(ctx)

    // Add trace ID if available
    traceID := extractTraceID(ctx)

    logger := m.logger.With(
        "request_id", requestID,
        "trace_id", traceID,
    )

    logger.Info("Creating order")

    // Framework's request ID middleware automatically propagates
    // request IDs to downstream calls for distributed tracing
    return m.createOrder(ctx, req)
}
```

### Health Status Logging

Log health status for monitoring:

```go
func (m *OrderModule) checkHealth(ctx context.Context) types.HealthStatus {
    // Check health...

    if healthy {
        m.logger.Debug("Health check passed")
        return types.HealthStatusHealthy
    } else {
        m.logger.Warn("Health check failed",
            "reason", "database unavailable",
        )
        return types.HealthStatusUnhealthy
    }
}
```

## Avoiding Common Logging Mistakes

{% hint style="danger" %}
**Security:** Never log passwords, API keys, tokens, credit card numbers, or other sensitive data. Even in debug logs. Logs are often stored and accessed by many systems.
{% endhint %}

### Don't Log Sensitive Information

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

### Don't Log at Multiple Levels

```go
// ❌ BAD: Logging and then logging the same error at a higher level
err := m.doSomething()
if err != nil {
    m.logger.Error("Operation failed", "error", err)
    m.logger.Error("Critical error", "error", err)  // ❌ Duplicate
}

// ✅ GOOD: Log once at appropriate level
if err := m.doSomething(); err != nil {
    m.logger.Error("Operation failed", "error", err)
}
```

### Don't Log Without Context

```go
// ❌ BAD: Generic error message
m.logger.Error("Failed")

// ✅ GOOD: Include context
m.logger.Error("Failed to process order",
    "order_id", order.ID,
    "error", err,
)
```

## Configuration

### Setting Log Level

The framework sets the default log level:

```go
app, err := mono.NewMonoApplication(
    mono.WithLogLevel(types.LogLevelInfo),  // Default to Info
)
```

Modules can change it:

```go
func (m *OrderModule) Start(ctx context.Context) error {
    // Logger instance is configured by the framework
    // and passed to modules during initialization
    return nil
}
```

## Checklist for Logging

When implementing logging in your module:

- [ ] Log significant events (start, stop, errors)
- [ ] Include relevant context (IDs, amounts, users)
- [ ] Use appropriate log levels
- [ ] Include request IDs for tracing
- [ ] Don't log sensitive information
- [ ] Use structured key-value pairs
- [ ] Log errors with full context
- [ ] Log performance metrics where relevant

## Best Practices Summary

✓ **Do**
- Use structured logging with key-value pairs
- Include request IDs for request tracing
- Add context using `With()` methods
- Log at appropriate levels
- Include operation IDs and entity IDs
- Log error details for debugging
- Log performance metrics
- Chain context methods for clarity

✗ **Don't**
- Log sensitive data (passwords, tokens, API keys)
- Use generic error messages
- Log the same information multiple times
- Ignore log levels
- Mix formatted and structured logging
- Log without context
- Use Debug logs for critical information

## Related Documentation

- [Error Handling Guide](error-handling.md)
- [Testing Guide](testing.md)
- [Access Log Middleware](../middleware/accesslog.md)
- [Request ID Middleware](../middleware/requestid.md)
- [Core Concepts - Modules](../core-concepts/modules.md)

---

For more information on Go's logging package, see [Go's log/slog documentation](https://pkg.go.dev/log/slog).
