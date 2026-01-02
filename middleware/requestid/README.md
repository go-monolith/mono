# Request ID Middleware

The `requestid` middleware provides automatic request ID generation and propagation for distributed tracing in the mono-framework. It ensures every request has a unique identifier that can be traced across service calls.

## Features

- **Automatic ID Generation**: Generates UUID for requests without an existing ID
- **Header Extraction**: Extracts existing request IDs from message headers (X-Request-ID)
- **Context Injection**: Injects request ID into handler context for use in logging
- **Automatic Propagation**: Propagates request ID to outgoing messages via OnOutgoingMessage hook
- **Configurable Header Name**: Use custom header name if needed
- **Zero-Copy**: Minimal performance overhead

## Quick Start

```go
package main

import (
    "github.com/go-monolith/mono/middleware/requestid"
    "github.com/go-monolith/mono"
)

func main() {
    // Create request ID middleware with default settings
    requestIDModule, _ := requestid.New()

    // Create and configure framework
    framework, _ := mono.NewMonoApplication()

    // Register request ID middleware FIRST (before other middleware)
    framework.Register(requestIDModule)
    framework.Start(ctx)
}
```

## Configuration Options

### Custom Header Name

```go
// Default header: X-Request-ID
requestid.New()

// Custom header name
requestid.New(
    requestid.WithHeaderName("X-Correlation-ID"),
)
```

## How It Works

### 1. Incoming Request Processing

When a request arrives at a service handler:

1. The middleware checks for an existing request ID in the message header (default: `X-Request-ID`)
2. If found, the existing ID is used
3. If not found, a new UUID is generated
4. The request ID is injected into the handler's `context.Context`

### 2. Outgoing Message Propagation

When a handler makes outgoing service calls (via EventBus.Publish, ServiceContainer calls, etc.):

1. The `OnOutgoingMessage` hook intercepts the outgoing message
2. It extracts the request ID from the context
3. It injects the request ID into the outgoing message's header

This ensures the same request ID flows through the entire call chain.

## Accessing Request ID in Handlers

Use the `GetRequestID` function to retrieve the request ID from context:

```go
import "github.com/go-monolith/mono/middleware/requestid"

func myHandler(ctx context.Context, req *types.Msg) ([]byte, error) {
    // Get request ID from context
    reqID := requestid.GetRequestID(ctx)

    // Use it in logging
    log.Printf("Processing request %s", reqID)

    // Process request...
    return response, nil
}
```

## Integration with Access Log Middleware

The requestid middleware is designed to work together with the accesslog middleware. **Important**: Register requestid BEFORE accesslog to ensure proper request ID extraction.

```go
// Correct order:
framework.Register(requestIDModule)  // 1. Generates/extracts request ID
framework.Register(accessLogModule)  // 2. Logs requests with request ID
framework.Register(businessModule)   // 3. Business modules

// Why this order matters:
// - requestid wraps handlers to inject request ID into message headers
// - accesslog extracts request ID from headers for logging
// - If registered in wrong order, accesslog won't see the request ID
```

### Middleware Chain Execution

```
Incoming Request
       │
       ▼
┌─────────────────────────┐
│  requestid middleware   │ ← Extracts/generates request ID
│  (wraps handler first)  │   Injects into context & headers
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│  accesslog middleware   │ ← Reads request ID from headers
│  (wraps handler second) │   Logs request with ID
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│  Business Handler       │ ← Can access request ID via
│                         │   requestid.GetRequestID(ctx)
└─────────────────────────┘
```

## Supported Service Types

| Service Type | Supported | Notes |
|--------------|-----------|-------|
| RequestReply | Yes | Full support |
| QueueGroup | Yes | Full support |
| StreamConsumer | Yes | Uses request ID from first message in batch |
| Channel | No | Channel services don't have context.Context |

## Example: Distributed Tracing

```go
// Service A receives a request
func (m *ModuleA) handleRequest(ctx context.Context, req *types.Msg) ([]byte, error) {
    reqID := requestid.GetRequestID(ctx)
    log.Printf("[%s] Service A processing", reqID)

    // Call Service B - request ID is automatically propagated
    response, _ := m.serviceBClient.Call(ctx, data)

    return response, nil
}

// Service B receives the same request ID
func (m *ModuleB) handleRequest(ctx context.Context, req *types.Msg) ([]byte, error) {
    reqID := requestid.GetRequestID(ctx)
    log.Printf("[%s] Service B processing", reqID)  // Same request ID!

    return response, nil
}
```

## Performance Considerations

- UUID generation: ~200ns per request (only when no existing ID)
- Header extraction: ~50ns per request
- Context injection: ~100ns per request
- Total overhead: < 1μs per request

## Thread Safety

The request ID module is fully thread-safe:
- Stateless after initialization
- Context values are immutable
- Header operations are atomic

## API Reference

### Functions

```go
// New creates a new request ID middleware module
func New(opts ...Option) (*RequestIDModule, error)

// GetRequestID retrieves the request ID from context
// Returns empty string if not found
func GetRequestID(ctx context.Context) string
```

### Options

```go
// WithHeaderName sets the header name for request ID
// Default: "X-Request-ID"
func WithHeaderName(name string) Option
```

### Constants

```go
// HeaderName is the default header name for request IDs
const HeaderName = "X-Request-ID"
```

## Best Practices

1. **Register First**: Always register requestid before other middleware (especially accesslog)
2. **Use in Logging**: Include request ID in all log messages for traceability
3. **Propagate to External Services**: When calling external APIs, forward the X-Request-ID header
4. **Store in Structured Logs**: Use structured logging to make request IDs searchable

## See Also

- [Access Log Middleware](../accesslog/README.md) - Logs requests with request IDs
- [Audit Middleware](../audit/README.md) - Security audit logging
- [Multi-Module Example](../../examples/multi-module/README.md) - Shows requestid + accesslog integration

## License

See the main framework LICENSE file.
