# Request ID Middleware

The Request ID Middleware provides automatic request ID generation and propagation for distributed tracing across module boundaries, enabling correlation of related requests through your application.

## Overview

The `requestid` middleware automatically:

- Generates a unique UUID for each request
- Extracts existing request IDs from incoming messages
- Injects request ID into handler execution context
- Propagates request ID to outgoing messages

This enables tracing requests through your entire application for debugging and monitoring.

{% hint style="info" %}
**Zero configuration required:** This middleware works out of the box with sensible defaults. Just register it and request IDs are automatically generated and propagated.
{% endhint %}

## Features

- **Automatic UUID Generation**: Creates UUIDs for requests without IDs
- **Request ID Extraction**: Reads X-Request-ID headers from incoming messages
- **Context Injection**: Makes request ID available to handlers via context
- **Automatic Propagation**: Forwards request ID to downstream service calls
- **Distributed Tracing**: Correlate requests across modules
- **Zero Configuration**: Works out of the box with sensible defaults

## Installation

Import the package:

```go
import "github.com/go-monolith/mono/v1/middleware/requestid"
```

## Signatures

### New

```go
func New() (MiddlewareModule, error)
```

Creates a new request ID middleware instance. No configuration options needed.

### FromContext

```go
func FromContext(ctx context.Context) string
```

Extracts the request ID from the context. Returns empty string if not found.

### WithContext

```go
func WithContext(ctx context.Context, requestID string) context.Context
```

Creates a new context with the specified request ID attached.

## Basic Usage

```go
package main

import (
    "context"
    "github.com/go-monolith/mono/v1"
    "github.com/go-monolith/mono/v1/middleware/requestid"
)

func main() {
    // Create request ID middleware
    requestIDMiddleware, err := requestid.New()
    if err != nil {
        panic(err)
    }

    // Create application
    app, _ := mono.NewMonoApplication()

    // Register middleware BEFORE other modules
    app.Register(requestIDMiddleware)

    // Register your modules
    app.Register(&OrderModule{})
    app.Register(&PaymentModule{})

    // Start application
    app.Start(context.Background())
}
```

## How It Works

### Request ID Lifecycle

```
Incoming Request
    ↓
[RequestID Middleware]
    ├─ Check X-Request-ID header
    ├─ Generate UUID if missing
    └─ Inject into context
    ↓
Handler Receives Request
    ├─ Access request ID from context
    └─ Use in logging
    ↓
Handler Calls Downstream Service
    ├─ RequestID Middleware intercepts
    ├─ Add X-Request-ID to outgoing message
    └─ Propagate to next module
    ↓
Downstream Handler
    └─ Receives same request ID
```

### Example Flow

```
Client Request (no request ID)
    ↓
[OrderModule Handler]
  request_id: "550e8400-e29b-41d4-a716-446655440000" (generated)
  logs: "Processing order" with request_id
  calls: paymentService.Process()
    ↓
[RequestID Middleware]
  adds X-Request-ID header to payment service call
    ↓
[PaymentModule Handler]
  request_id: "550e8400-e29b-41d4-a716-446655440000" (same ID!)
  logs: "Processing payment" with request_id
```

## Accessing Request ID in Handlers

### From Context

```go
func (m *OrderModule) handleCreateOrder(ctx context.Context, req *CreateOrderRequest) (*Order, error) {
    // Extract request ID from context
    requestID := requestid.FromContext(ctx)

    // Use in logging
    m.logger.Info("Creating order", "request_id", requestID, "order_id", req.OrderID)

    // Pass to downstream calls
    return m.paymentService.Process(ctx, &PaymentRequest{})
}
```

### Helper Function

```go
import "github.com/go-monolith/mono/v1/middleware/requestid"

func (m *OrderModule) handleOrder(ctx context.Context, order *Order) error {
    requestID := requestid.FromContext(ctx)
    // Use requestID for logging, tracing, etc.
    return nil
}
```

## Configuration Options

The Request ID Middleware requires no configuration - it works with zero options:

| Setting | Default |
|---------|---------|
| Configuration Options | None |
| UUID Generation | Automatic |
| Header Name | `X-Request-ID` |

## Default Config

```go
requestid.New()  // Works out of the box, no options needed
```

## Integration with Other Middleware

### With Access Log Middleware

The request ID automatically appears in access logs:

```go
app.Register(requestid.New())
app.Register(accesslog.New(accesslog.WithFormat(accesslog.FormatJSON)))
```

Access log output:
```json
{
  "module": "order",
  "service": "create-order",
  "status": "success",
  "duration_ms": 2.456,
  "request_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

### With Audit Middleware

Audit entries include request IDs for tracing:

```go
app.Register(requestid.New())
app.Register(audit.New(audit.WithOutput(auditFile)))
```

Audit log entry:
```json
{
  "event_type": "service_registration",
  "request_id": "550e8400-e29b-41d4-a716-446655440000",
  "details": {
    "module": "payment",
    "service": "process-payment"
  }
}
```

## Practical Examples

### Complete Setup

```go
func main() {
    app, _ := mono.NewMonoApplication()

    // Request ID first (other middleware uses it)
    app.Register(requestid.New())

    // Access logging with request IDs
    app.Register(accesslog.New(
        accesslog.WithFormat(accesslog.FormatJSON),
    ))

    // Audit logging with request IDs
    auditFile, _ := os.OpenFile("audit.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
    app.Register(audit.New(audit.WithOutput(auditFile)))

    // Application modules
    app.Register(&OrderModule{})
    app.Register(&PaymentModule{})

    app.Start(context.Background())
}
```

### Custom Request ID in Logs

```go
func (m *OrderModule) handleCreateOrder(ctx context.Context, req *CreateOrderRequest) (*Order, error) {
    requestID := requestid.FromContext(ctx)

    // Include request ID in all logs
    m.logger.Info("Order creation started",
        "request_id", requestID,
        "customer_id", req.CustomerID,
        "amount", req.Amount,
    )

    // Log before and after external calls
    m.logger.Info("Calling payment service",
        "request_id", requestID,
        "payment_amount", req.Amount,
    )

    response, err := m.paymentService.Process(ctx, &PaymentRequest{
        Amount: req.Amount,
    })

    m.logger.Info("Payment service response",
        "request_id", requestID,
        "success", err == nil,
        "transaction_id", response.TransactionID,
    )

    return &Order{ID: "ORD-001"}, nil
}
```

### Distributed Tracing Integration

```go
// Trace requests across services
func (m *OrderModule) CreateOrder(ctx context.Context, req *CreateOrderRequest) (*Order, error) {
    requestID := requestid.FromContext(ctx)

    // Start span with request ID
    span := tracer.StartSpan("create_order", trace.WithAttributes(
        attribute.String("request_id", requestID),
    ))
    defer span.End()

    // Create order
    order := &Order{ID: "ORD-001"}

    // Call payment (request ID propagated automatically)
    payment, err := m.paymentService.Process(ctx, &PaymentRequest{})

    return order, err
}
```

## Monitoring and Debugging

### Finding Related Requests

With request IDs, you can find all related logs:

```bash
# Find all logs for a specific request
grep "550e8400-e29b-41d4-a716-446655440000" *.log

# Find request IDs for a specific customer
jq 'select(.customer_id == "CUST-001") | .request_id' access.log | sort -u

# Trace request through all modules
grep "request_id.*550e8400" access.log audit.log | sort -t: -k1
```

### Performance Monitoring

Track request latencies by request ID:

```go
func (m *OrderModule) handleCreateOrder(ctx context.Context, req *CreateOrderRequest) (*Order, error) {
    requestID := requestid.FromContext(ctx)
    startTime := time.Now()

    // Process...
    result := m.createOrder(ctx, req)

    // Log performance
    duration := time.Since(startTime)
    m.metrics.RecordLatency("create_order", duration, requestID)

    return result, nil
}
```

## Common Pitfalls

### Proxy header conflicts

{% hint style="warning" %}
If you have a reverse proxy in front of your application, ensure it doesn't overwrite the `X-Request-ID` header, or configure it to pass through existing IDs.
{% endhint %}

```nginx
# Nginx: Pass through existing request ID or generate new one
proxy_set_header X-Request-ID $request_id;
```

### Losing request ID in goroutines

When spawning goroutines, you must pass the context to preserve the request ID:

```go
// ❌ WRONG: Losing request ID
go func() {
    m.doAsyncWork()  // No request ID!
}()

// ✅ CORRECT: Pass context to goroutine
go func(ctx context.Context) {
    requestID := requestid.FromContext(ctx)
    m.doAsyncWork(ctx)  // Request ID preserved
}(ctx)
```

### Not extracting request ID in handlers

```go
// ❌ WRONG: Not using request ID
func (m *Module) Handle(ctx context.Context, req *Request) error {
    m.logger.Info("Processing")  // No correlation!
    return nil
}

// ✅ CORRECT: Extract and use request ID
func (m *Module) Handle(ctx context.Context, req *Request) error {
    requestID := requestid.FromContext(ctx)
    m.logger.Info("Processing", "request_id", requestID)
    return nil
}
```

## Best Practices

✓ **Do**
- Register request ID middleware FIRST
- Use request ID in all logging
- Pass context to all downstream calls
- Include request ID in error responses
- Use request IDs for debugging and monitoring

✗ **Don't**
- Create new requests without request ID context
- Drop request ID when calling other modules
- Assume request ID is always present (use helper safely)
- Log raw request ID without context (log with operation name)

## Troubleshooting

### Request ID Not Propagating

**Cause**: RequestID middleware not registered first

**Solution**:
```go
app.Register(requestid.New())  // Must be FIRST
app.Register(accesslog.New())
app.Register(&OrderModule{})
```

### Request ID Not in Logs

**Cause**: Not extracting from context in handler

**Solution**:
```go
func (m *Module) Handle(ctx context.Context, req *Request) error {
    requestID := requestid.FromContext(ctx)  // Extract it!
    m.logger.Info("Processing", "request_id", requestID)
    return nil
}
```

## HTTP Gateway Integration

If you're adding an HTTP gateway in front of NATS:

```go
// HTTP Server
http.HandleFunc("/api/orders", func(w http.ResponseWriter, r *http.Request) {
    // Extract or generate request ID from HTTP request
    requestID := r.Header.Get("X-Request-ID")
    if requestID == "" {
        requestID = uuid.New().String()
    }

    // Pass to NATS service
    ctx := context.Background()
    ctx = requestid.WithContext(ctx, requestID)

    result, err := orderService.Create(ctx, &Order{})
})
```

## Related Documentation

- [Access Log Middleware](accesslog.md)
- [Audit Middleware](audit.md)
- [Middleware System](README.md)
- [Core Concepts - Modules](../core-concepts/modules.md)

---

For access logging with request IDs, see [Access Log Middleware](accesslog.md).
