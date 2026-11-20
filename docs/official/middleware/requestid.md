# Request ID Middleware

The `requestid` middleware gives every handled message a request ID, puts it in the handler context, and passes it on to downstream service calls, so that related log lines across modules can be correlated.

Read this page to trace requests across modules, to read the ID in a handler, or to seed the ID from an HTTP gateway.

{% hint style="info" %}
**Zero configuration required:** register it and request IDs are generated and propagated. When you also use `accesslog`, register `requestid` **after** it (see [Ordering](README.md#ordering)).
{% endhint %}

## What It Does

- **Extracts** the ID from the `X-Request-ID` header of incoming messages
- **Generates** a UUID (v4) when the header is missing
- **Injects** the ID into the handler context (`requestid.GetRequestID(ctx)`)
- **Propagates** the ID to outgoing request-reply calls, queue group sends and stream consumer publishes made with that context; an existing header is not overwritten

Limits:

- Stream consumer and event stream consumer batches get one ID per batch, taken from the first message or generated.
- Events (`EventDefinition.Publish`, `EventBus.Publish`) and channel services do not carry the ID.

## Basic Usage

```go
package main

import (
    "context"

    "github.com/go-monolith/mono"
    "github.com/go-monolith/mono/middleware/requestid"
)

func main() {
    requestIDMiddleware, err := requestid.New()
    if err != nil {
        panic(err)
    }

    app, _ := mono.NewMonoApplication()

    // Register middleware (any position relative to modules)
    app.Register(requestIDMiddleware)
    app.Register(&OrderModule{})
    app.Register(&PaymentModule{})

    app.Start(context.Background())
}
```

## Configuration Options

`func New(opts ...Option) (*RequestIDModule, error)` works with zero options:

| Option | Description | Default |
|--------|-------------|---------|
| `WithHeaderName(name string)` | Header the request ID is read from and written to (empty is rejected) | `X-Request-ID` (`requestid.HeaderName`) |

If you change the header, pass the same name to `accesslog.WithRequestIDHeader`.

## How It Works

```
Client Request (no request ID)
    ↓
[RequestID Middleware]  checks X-Request-ID, generates a UUID if missing, injects it into ctx
    ↓
[OrderModule Handler]
  request_id: "550e8400-e29b-41d4-a716-446655440000" (generated)
  calls the payment service with ctx
    ↓
[RequestID Middleware]  adds X-Request-ID to the outgoing message
    ↓
[PaymentModule Handler]
  request_id: "550e8400-e29b-41d4-a716-446655440000" (same ID)
```

**An incoming ID is sanitised** (since v0.2.18). The header is written by the sender of the message, and the ID goes into the handler's context, every nested call and the logs:

- At most 128 bytes are kept.
- ASCII letters, digits and `-_.:/=+@~` are kept; every other byte (spaces, quotes, control characters, non-ASCII) becomes `_`.
- UUIDs, ULIDs, W3C trace IDs and base64 values pass unchanged. The access log applies the same rule, so both report the same value.
- The message's own header is not rewritten: a handler that reads `msg.Header` directly sees what was sent.

## Accessing the Request ID in Handlers

`func GetRequestID(ctx context.Context) string` returns the ID, or an empty string if there is none. There is no function to put an ID into a context; send it as a header instead (see [HTTP Gateway Integration](#http-gateway-integration)).

```go
func (m *OrderModule) handleCreateOrder(ctx context.Context, req *CreateOrderRequest) (*Order, error) {
    requestID := requestid.GetRequestID(ctx)

    m.logger.Info("Creating order", "request_id", requestID, "order_id", req.OrderID)

    // Pass ctx to downstream calls so the ID is propagated
    return m.paymentService.Process(ctx, &PaymentRequest{})
}
```

When you start a goroutine, pass the context to keep the ID:

```go
// ❌ WRONG: Losing request ID
go func() {
    m.doAsyncWork()  // No request ID!
}()

// ✅ CORRECT: Pass context to goroutine
go func(ctx context.Context) {
    m.doAsyncWork(ctx)  // Request ID preserved
}(ctx)
```

### Distributed Tracing Integration

```go
func (m *OrderModule) CreateOrder(ctx context.Context, req *CreateOrderRequest) (*Order, error) {
    requestID := requestid.GetRequestID(ctx)

    // Start span with request ID (pseudo-code for a third-party tracer)
    span := tracer.StartSpan("create_order", trace.WithAttributes(
        attribute.String("request_id", requestID),
    ))
    defer span.End()

    order := &Order{ID: "ORD-001"}

    // Call payment (request ID propagated automatically)
    _, err := m.paymentService.Process(ctx, &PaymentRequest{})

    return order, err
}
```

## HTTP Gateway Integration

If you're adding an HTTP gateway in front of NATS, send the ID as a message header. The middleware reads it in the handler and does not overwrite an existing header on outgoing messages:

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

{% hint style="warning" %}
If you have a reverse proxy in front of your application, ensure it doesn't overwrite the `X-Request-ID` header. For example, nginx's `proxy_set_header X-Request-ID $request_id;` always sets a new ID (`$request_id` is generated per request).
{% endhint %}

## Integration with Other Middleware

- **Access log**: the request ID appears in access log entries. Register `accesslog` first and `requestid` second; see [Ordering](README.md#ordering) and [Access Log: Integration with Request ID Middleware](accesslog.md#integration-with-request-id-middleware).
- **Audit**: the audit middleware does not record request IDs; it never sees service calls.

## Finding Related Requests

```bash
# Find all logs for a specific request
grep "550e8400-e29b-41d4-a716-446655440000" *.log

# List the access log entries of a request (JSON format)
jq 'select(.request_id == "550e8400-e29b-41d4-a716-446655440000")' access.log
```

## Best Practices

✓ **Do**
- Register request ID middleware after accesslog (so it runs first)
- Use the request ID in all logging
- Pass the handler's context to all downstream calls
- Include request ID in error responses

✗ **Don't**
- Create new requests without request ID context
- Assume request ID is always present (`GetRequestID` returns `""` when there is none)
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

### Request ID Not in Application Logs

**Cause**: The handler does not read the ID from its context

**Solution**: call `requestid.GetRequestID(ctx)` and add it to the log line, as in [Accessing the Request ID in Handlers](#accessing-the-request-id-in-handlers).

## Related Documentation

- [Access Log Middleware](accesslog.md)
- [Audit Middleware](audit.md)
- [Middleware System](README.md)
- [Core Concepts - Modules](../core-concepts/modules.md)
