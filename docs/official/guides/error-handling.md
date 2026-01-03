# Error Handling Guide

This guide covers error handling best practices in the Monolith Framework, including handling errors in service handlers, creating custom error types, and propagating errors through call chains.

**Time to complete:** 15 minutes

**What you'll learn:**
- How to check and propagate errors properly
- Creating custom domain-specific error types
- Error handling patterns for different service types
- Converting errors to user-friendly responses

**Prerequisites:**
- Basic understanding of Go error handling
- Familiarity with [Module basics](../core-concepts/modules.md)

---

## Overview

Go's error handling model emphasizes explicit error checking and proper propagation. The Monolith Framework follows Go conventions with additional context-aware error types for module, service, and dependency errors.

{% hint style="info" %}
**Go Philosophy:** Errors are values, not exceptions. Always check errors immediately after function calls and handle or propagate them explicitly.
{% endhint %}

## Error Handling Basics

### Checking Errors Immediately

Always check errors immediately after function calls:

```go
func (m *OrderModule) handleCreateOrder(ctx context.Context, req *CreateOrderRequest) (*Order, error) {
    // Check error immediately
    data, err := m.validateOrder(ctx, req)
    if err != nil {
        return nil, err  // Propagate to caller
    }

    // Use data...
    return &Order{}, nil
}
```

### Wrapping Errors with Context

Use `fmt.Errorf` with the `%w` verb to wrap errors and preserve the error chain:

{% hint style="warning" %}
**Always use `%w`** (not `%v`) when wrapping errors. Using `%v` loses the error chain, breaking `errors.Is()` and `errors.As()` checks.
{% endhint %}

```go
func (m *OrderModule) processPayment(ctx context.Context, amount int64) error {
    result, err := m.paymentService.Charge(ctx, amount)
    if err != nil {
        // Wrap with context
        return fmt.Errorf("failed to process payment: %w", err)
    }

    return nil
}
```

The `%w` verb preserves the original error, allowing callers to use `errors.Is()` and `errors.As()`:

```go
err := m.processPayment(ctx, 1000)
if errors.Is(err, paymentService.ErrInsufficientFunds) {
    // Handle specific error
}
```

## Service Handler Error Patterns

### Request-Reply Handlers

Request-Reply handlers return errors that propagate immediately to the caller:

```go
func (m *OrderModule) handleGetOrder(ctx context.Context, req *GetOrderRequest) (*Order, error) {
    // Validate request
    if req.OrderID == "" {
        return nil, errors.New("order ID required")  // Client sees this error
    }

    // Find order
    order, err := m.findOrder(ctx, req.OrderID)
    if err != nil {
        // Wrap with context
        return nil, fmt.Errorf("failed to find order: %w", err)
    }

    return order, nil
}
```

### Queue Group Handlers

{% hint style="info" %}
Queue group handlers use fire-and-forget semantics. Errors are logged but **not** returned to the sender.
{% endhint %}

Queue group handlers don't return errors to the sender (fire-and-forget pattern). Log errors instead:

```go
func (m *OrderModule) handleProcessOrder(ctx context.Context, order *Order) error {
    // Log error instead of propagating
    if err := m.validateOrder(ctx, order); err != nil {
        m.logger.Error("Failed to validate order",
            "order_id", order.ID,
            "error", err,
        )
        return err  // Logged, not propagated to sender
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

## Creating Custom Error Types

### Domain-Specific Errors

Create custom error types for domain-specific failures:

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
    ErrOrderNotFound   = errors.New("order not found")
    ErrInvalidOrder    = errors.New("invalid order")
    ErrOrderLocked     = errors.New("order is locked")
)
```

### Using Custom Errors

```go
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

### Handling Custom Errors

```go
func (m *OrderModule) handleGetOrder(ctx context.Context, req *GetOrderRequest) (*Order, error) {
    order, err := m.findOrder(ctx, req.OrderID)

    // Check for specific error type
    var orderErr *OrderError
    if errors.As(err, &orderErr) {
        if orderErr.Code == "NOT_FOUND" {
            // Return user-friendly error
            return nil, fmt.Errorf("order %s does not exist", req.OrderID)
        }
    }

    if err != nil {
        return nil, fmt.Errorf("failed to find order: %w", err)
    }

    return order, nil
}
```

## Error Propagation Patterns

### Simple Propagation

For straightforward cases, propagate errors directly:

```go
func (m *OrderModule) createOrder(ctx context.Context, req *CreateOrderRequest) (*Order, error) {
    // Error from dependency - propagate as-is (already has context)
    if err := m.validateRequest(ctx, req); err != nil {
        return nil, err
    }

    order := &Order{ID: generateID()}

    // Error needs local context - wrap it
    if err := m.persistOrder(ctx, order); err != nil {
        return nil, fmt.Errorf("failed to persist order: %w", err)
    }

    return order, nil
}
```

### Multi-Step Operations

For operations with multiple steps, log intermediate errors but continue if possible:

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

    m.logger.Info("Processed order batch",
        "total", len(orders),
        "succeeded", processedCount,
        "failed", len(orders)-processedCount,
    )

    if lastErr != nil {
        return fmt.Errorf("batch processing failed (processed %d of %d): %w",
            processedCount, len(orders), lastErr)
    }

    return nil
}
```

## Error Logging Best Practices

### Log Errors with Context

Include relevant information when logging errors:

```go
func (m *OrderModule) handleCreateOrder(ctx context.Context, req *CreateOrderRequest) (*Order, error) {
    order, err := m.createOrder(ctx, req)
    if err != nil {
        m.logger.Error("Failed to create order",
            "customer_id", req.CustomerID,
            "amount", req.Amount,
            "error", err,
        )
        return nil, err
    }

    m.logger.Info("Order created successfully",
        "order_id", order.ID,
        "customer_id", req.CustomerID,
        "amount", req.Amount,
    )

    return order, nil
}
```

### Log Error Chains

When logging wrapped errors, the error chain is preserved:

```go
err := m.someOperation()  // Returns: "operation failed: database error: connection timeout"
m.logger.Error("Operation failed", "error", err)

// Output: error="operation failed: database error: connection timeout"
```

### Use Error-Specific Logger Methods

Create loggers with error context:

```go
func (m *OrderModule) retryableOperation(ctx context.Context) error {
    var lastErr error

    for attempt := 1; attempt <= 3; attempt++ {
        if err := m.doOperation(ctx); err != nil {
            lastErr = err
            logger := m.logger.WithError(err)
            logger.Warn("Operation failed, retrying",
                "attempt", attempt,
            )
            time.Sleep(time.Duration(attempt) * time.Second)
            continue
        }

        return nil
    }

    return fmt.Errorf("operation failed after 3 attempts: %w", lastErr)
}
```

## Handling Framework Errors

### Sentinel Error Checking

The framework provides sentinel errors. Check for specific failures:

```go
import (
    "errors"
    mono "github.com/go-monolith/mono"
    monierr "github.com/go-monolith/mono/pkg/errors"
)

func (m *OrderModule) getDependency(ctx context.Context) (PaymentService, error) {
    service, err := m.container.RequestReplyService(ctx, "payment-service")
    if err != nil {
        // Check if it's a missing dependency error
        if errors.Is(err, monierr.ErrServiceNotFound) {
            return nil, fmt.Errorf("payment service not configured")
        }
        return nil, fmt.Errorf("failed to get payment service: %w", err)
    }

    return service.(PaymentService), nil
}
```

### Configuration Errors

Configuration errors occur during framework setup:

```go
app, err := mono.NewMonoApplication(
    mono.WithJetStreamEnabled(true),
    mono.WithJetStreamStorageDir("/tmp/jetstream"),
)
if err != nil {
    // Could be ConfigurationError with specific option that failed
    var confErr *monierr.ConfigurationError
    if errors.As(err, &confErr) {
        fmt.Printf("Configuration failed at option %d (%s): %v\n",
            confErr.OptionIndex, confErr.OptionName, confErr.Err)
    }
    return err
}
```

## Error Response Patterns

### Convert Errors to User-Friendly Messages

{% hint style="danger" %}
**Security:** Never expose internal error details (stack traces, database errors, file paths) to external clients. This can leak implementation details useful to attackers.
{% endhint %}

Don't expose internal error details to clients:

```go
func (m *OrderModule) handleGetOrder(ctx context.Context, req *GetOrderRequest) (*Order, error) {
    order, err := m.findOrder(ctx, req.OrderID)

    // Convert internal error to user-friendly message
    if errors.Is(err, ErrOrderNotFound) {
        // Return simple error message
        return nil, fmt.Errorf("order not found")
    }

    if err != nil {
        // Generic message for unexpected errors
        m.logger.Error("Failed to retrieve order",
            "order_id", req.OrderID,
            "error", err,
        )
        return nil, errors.New("internal error")
    }

    return order, nil
}
```

### Error Codes for API Responses

For APIs, use error codes for categorization:

```go
type ErrorResponse struct {
    Code    string `json:"code"`
    Message string `json:"message"`
}

func (m *OrderModule) handleCreateOrder(ctx context.Context, req *CreateOrderRequest) (*Order, error) {
    if err := m.validateRequest(req); err != nil {
        // Return error code along with message
        return nil, fmt.Errorf("validation_error: %w", err)
    }

    order, err := m.createOrder(ctx, req)
    if err != nil {
        m.logger.Error("Create order failed", "error", err)
        return nil, errors.New("creation_failed")
    }

    return order, nil
}
```

## Testing Error Cases

### Test Error Scenarios

Always test both success and error paths:

```go
func TestHandleCreateOrder_Validation(t *testing.T) {
    module := &OrderModule{logger: newTestLogger()}

    testCases := []struct {
        name    string
        req     *CreateOrderRequest
        wantErr bool
        errMsg  string
    }{
        {
            name:    "missing customer ID",
            req:     &CreateOrderRequest{Amount: 100},
            wantErr: true,
            errMsg:  "customer ID required",
        },
        {
            name:    "invalid amount",
            req:     &CreateOrderRequest{CustomerID: "c1", Amount: 0},
            wantErr: true,
            errMsg:  "amount must be positive",
        },
        {
            name:    "valid request",
            req:     &CreateOrderRequest{CustomerID: "c1", Amount: 100},
            wantErr: false,
        },
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
- [ ] Should the caller know about this specific error? Create a sentinel error or custom type.
- [ ] Is this error being logged? Include the error in the log message.
- [ ] Is this a user-facing error? Remove internal details and provide a user-friendly message.

## Best Practices Summary

✓ **Do**
- Check errors immediately after function calls
- Wrap errors with context using `fmt.Errorf` and `%w`
- Create domain-specific error types for important failures
- Log errors with debugging context
- Return user-friendly errors to clients

✗ **Don't**
- Ignore errors with blank identifier `_`
- Lose error context by not wrapping
- Expose internal error details to clients
- Panic in libraries (reserved for `main` or unrecoverable errors)
- Return generic errors without context

## Related Documentation

- [Middleware System](../middleware/README.md)
- [API Reference - Error Types](../api/README.md)
- [Testing Guide](testing.md)
- [Logging Guide](logging.md)

---

For more information on error handling in Go, see the [Go error handling blog post](https://go.dev/blog/error-handling-and-go).
