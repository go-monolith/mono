# Request ID Middleware

The Request ID Middleware provides automatic request ID generation and propagation for distributed tracing across module boundaries, enabling correlation of related requests through your application.

## Overview

The `requestid` middleware automatically:

- Generates a unique UUID for each request
- Extracts existing request IDs from incoming messages
- Injects request ID into handler execution context
- Propagates request ID to outgoing request-reply calls, queue group sends and stream consumer publishes

For stream consumer and event stream consumer batches there is one ID per batch, taken from the first message or generated. Events (`EventDefinition.Publish`, `EventBus.Publish`) and channel services do not carry the ID.

This enables tracing requests through your entire application for debugging and monitoring.

{% hint style="info" %}
**Zero configuration required:** This middleware works out of the box with sensible defaults. Just register it and request IDs are automatically generated and propagated.
{% endhint %}

## Features

- **Automatic UUID Generation**: Creates UUIDs for requests without IDs
- **Request ID Extraction**: Reads X-Request-ID headers from incoming messages
- **Context Injection**: Makes request ID available to handlers via context
- **Automatic Propagation**: Forwards request ID to downstream service calls (not to events)
- **Distributed Tracing**: Correlate requests across modules
- **Zero Configuration**: Works out of the box with sensible defaults

## Installation

Import the package:

```go
import "github.com/go-monolith/mono/middleware/requestid"
```

## Signatures

### New

```go
func New(opts ...Option) (*RequestIDModule, error)
```

Creates a new request ID middleware instance. No options are needed; `WithHeaderName` changes the header.

### WithHeaderName

```go
func WithHeaderName(name string) Option
```

Sets the header the request ID is read from and written to (default `requestid.HeaderName`, `"X-Request-ID"`; empty is rejected).

### GetRequestID

```go
func GetRequestID(ctx context.Context) string
```

Extracts the request ID from the context. Returns empty string if not found. There is no function to put an ID into a context; send it as a header instead (see [HTTP Gateway Integration](#http-gateway-integration)).

## Basic Usage

```go
package main

import (
    "context"
    "github.com/go-monolith/mono"
    "github.com/go-monolith/mono/middleware/requestid"
)

func main() {
    // Create request ID middleware
    requestIDMiddleware, err := requestid.New()
    if err != nil {
        panic(err)
    }

    // Create application
    app, _ := mono.NewMonoApplication()

    // Register middleware (any position relative to modules)
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
    requestID := requestid.GetRequestID(ctx)

    // Use in logging
    m.logger.Info("Creating order", "request_id", requestID, "order_id", req.OrderID)

    // Pass to downstream calls
    return m.paymentService.Process(ctx, &PaymentRequest{})
}
```

### Helper Function

```go
import "github.com/go-monolith/mono/middleware/requestid"

func (m *OrderModule) handleOrder(ctx context.Context, order *Order) error {
    requestID := requestid.GetRequestID(ctx)
    // Use requestID for logging, tracing, etc.
    return nil
}
```

## Configuration Options

The Request ID Middleware works with zero options:

| Option | Description | Default |
|--------|-------------|---------|
| `WithHeaderName` | Header name for the request ID | `X-Request-ID` (`requestid.HeaderName`) |

UUIDs (v4) are generated automatically when the incoming message has no ID.

## Default Config

```go
requestid.New()  // Works out of the box, no options needed
```

## Integration with Other Middleware

### With Access Log Middleware

The request ID appears in access logs. Register `accesslog` **before** `requestid`: the last registered middleware runs first, and `requestid` must run before `accesslog` for a generated ID to reach the access log. If you change the header with `WithHeaderName`, pass the same name to `accesslog.WithRequestIDHeader`.

```go
accessModule, err := accesslog.New(
    accesslog.WithOutput(accessFile),
    accesslog.WithFormat(accesslog.FormatJSON),
)
if err != nil {
    return err
}
app.Register(accessModule)

requestIDModule, err := requestid.New()
if err != nil {
    return err
}
app.Register(requestIDModule)
```

Access log output:
```json
{"duration_ms":2,"module":"order","request_id":"550e8400-e29b-41d4-a716-446655440000","request_size":128,"response_size":256,"service":"create-order","service_type":"request_reply","status":"success","ts":"2025-01-15T10:30:00Z"}
```

The audit middleware does not record request IDs: it never sees service calls.

## Practical Examples

### Complete Setup

```go
func main() {
    app, _ := mono.NewMonoApplication()

    // Access logging first, so that requestid (registered next) runs before it
    accessFile, _ := os.OpenFile("access.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
    accessModule, err := accesslog.New(
        accesslog.WithOutput(accessFile),
        accesslog.WithFormat(accesslog.FormatJSON),
    )
    if err != nil {
        log.Fatal(err)
    }
    app.Register(accessModule)

    // Request ID second
    requestIDModule, err := requestid.New()
    if err != nil {
        log.Fatal(err)
    }
    app.Register(requestIDModule)

    // Audit logging (lifecycle and registration events)
    auditFile, _ := os.OpenFile("audit.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
    auditModule, err := audit.New(audit.WithOutput(auditFile))
    if err != nil {
        log.Fatal(err)
    }
    app.Register(auditModule)

    // Application modules
    app.Register(&OrderModule{})
    app.Register(&PaymentModule{})

    app.Start(context.Background())
}
```

### Custom Request ID in Logs

```go
func (m *OrderModule) handleCreateOrder(ctx context.Context, req *CreateOrderRequest) (*Order, error) {
    requestID := requestid.GetRequestID(ctx)

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
    requestID := requestid.GetRequestID(ctx)

    // Start span with request ID (pseudo-code for a third-party tracer)
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
    requestID := requestid.GetRequestID(ctx)
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
# Nginx: always sets a new ID ($request_id is generated per request)
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
    requestID := requestid.GetRequestID(ctx)
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
    requestID := requestid.GetRequestID(ctx)
    m.logger.Info("Processing", "request_id", requestID)
    return nil
}
```

## Best Practices

✓ **Do**
- Register request ID middleware after accesslog (so it runs first)
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

**Cause**: The message is an event or a channel service message (not propagated), or the downstream call does not use the handler's context

**Solution**: Pass the handler's `ctx` to the downstream request-reply call, queue group send or stream consumer publish; events and channel services do not carry the ID.

### Request ID Missing from Access Logs

**Cause**: `requestid` registered before `accesslog`

**Solution**:
```go
app.Register(accessModule)    // first
app.Register(requestIDModule) // second: runs first
app.Register(&OrderModule{})
```

### Request ID Not in Logs

**Cause**: Not extracting from context in handler

**Solution**:
```go
func (m *Module) Handle(ctx context.Context, req *Request) error {
    requestID := requestid.GetRequestID(ctx)  // Extract it!
    m.logger.Info("Processing", "request_id", requestID)
    return nil
}
```

## HTTP Gateway Integration

If you're adding an HTTP gateway in front of NATS:

Send the ID as a message header; the middleware reads it in the handler and does not overwrite an existing header on outgoing messages:

```go
// HTTP Server (orderClient is a mono.RequestReplyServiceClient)
http.HandleFunc("/api/orders", func(w http.ResponseWriter, r *http.Request) {
    // Extract or generate request ID from HTTP request
    requestID := r.Header.Get("X-Request-ID")
    if requestID == "" {
        requestID = uuid.New().String()
    }

    // Pass to NATS service as a header
    resp, err := orderClient.CallMsg(r.Context(), &mono.Msg{
        Header: mono.Header{requestid.HeaderName: {requestID}},
        Data:   body,
    })
    // ...
})
```

## Related Documentation

- [Access Log Middleware](accesslog.md)
- [Audit Middleware](audit.md)
- [Middleware System](README.md)
- [Core Concepts - Modules](../core-concepts/modules.md)

---

For access logging with request IDs, see [Access Log Middleware](accesslog.md).
