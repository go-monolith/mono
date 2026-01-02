# Access Log Middleware

The Access Log Middleware provides HTTP-style access logging for all service handlers, capturing request/response details including timing, sizes, status, and request IDs.

## Overview

The `accesslog` middleware automatically logs every service call with detailed information about the request and response. It's similar to HTTP access logging in traditional web servers.

{% hint style="info" %}
**Order matters:** Register the access log middleware **before** your application modules but **after** the Request ID middleware if you want request IDs included in your logs.
{% endhint %}

## Features

- **Automatic Logging**: Logs all service handler executions
- **Multiple Formats**: Text and JSON output formats
- **Performance Metrics**: Measures request duration and message sizes
- **Request ID Integration**: Automatically includes request IDs if available
- **Configurable Output**: Write to file, stdout, or custom writer
- **Low Overhead**: ~1-5µs per request depending on I/O

## Installation

Import the package:

```go
import "github.com/go-monolith/mono/v1/middleware/accesslog"
```

## Signatures

### New

```go
func New(opts ...Option) (MiddlewareModule, error)
```

Creates a new access log middleware instance with the given options.

### WithOutput

```go
func WithOutput(w io.Writer) Option
```

Sets the output destination for logs (file, stdout, etc.).

### WithFormat

```go
func WithFormat(format Format) Option
```

Sets the log format (Text or JSON).

## Basic Usage

```go
package main

import (
    "os"
    "context"
    "github.com/go-monolith/mono/v1"
    "github.com/go-monolith/mono/v1/middleware/accesslog"
)

func main() {
    // Create access log middleware
    accessModule, err := accesslog.New(
        accesslog.WithOutput(os.Stdout),
        accesslog.WithFormat(accesslog.FormatText),
    )
    if err != nil {
        panic(err)
    }

    // Create application
    app, _ := mono.NewMonoApplication()

    // Register middleware BEFORE other modules
    app.Register(accessModule)

    // Register your modules
    app.Register(&OrderModule{})
    app.Register(&PaymentModule{})

    // Start application
    app.Start(context.Background())
}
```

## Configuration Options

All configuration is done through functional options passed to `New()`:

| Option | Type | Description | Default |
|--------|------|-------------|---------|
| `WithOutput` | `io.Writer` | Output destination for logs | `os.Stdout` |
| `WithFormat` | `Format` | Log format (Text or JSON) | `FormatText` |

### WithOutput

Write to file:
```go
file, _ := os.OpenFile("access.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
accesslog.New(accesslog.WithOutput(file))
```

Write to stdout (default):
```go
accesslog.New(accesslog.WithOutput(os.Stdout))
```

### WithFormat

Text format (default, human-readable):
```go
accesslog.New(accesslog.WithFormat(accesslog.FormatText))
// Output: INFO [accesslog] module=payment service=process-payment status=success duration=1.234ms request_size=256B response_size=512B
```

JSON format (machine-readable):
```go
accesslog.New(accesslog.WithFormat(accesslog.FormatJSON))
// Output: {"module":"payment","service":"process-payment","status":"success","duration_ms":1.234,"request_size":256,"response_size":512}
```

## Default Config

```go
accesslog.New()  // Uses defaults: stdout, text format
```

## Log Entry Format

### Text Format Fields

```
[service-name] module=MODULE service=SERVICE status=STATUS duration=DURATIONms request_size=SIZEb response_size=SIZEb [request_id=ID]
```

| Field | Description |
|-------|-------------|
| `module` | Name of the module providing the service |
| `service` | Service name |
| `status` | success or error |
| `duration` | Request processing time in milliseconds |
| `request_size` | Size of request in bytes |
| `response_size` | Size of response in bytes |
| `request_id` | Request ID (if RequestID middleware is used) |

### JSON Format Fields

```json
{
  "module": "payment",
  "service": "process-payment",
  "status": "success",
  "duration_ms": 1.234,
  "request_size": 256,
  "response_size": 512,
  "request_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

## Integration with Request ID Middleware

The access log middleware automatically includes request IDs when using the Request ID middleware:

```go
app.Register(requestid.New())
app.Register(accesslog.New(accesslog.WithOutput(os.Stdout)))
```

Sample output:
```
[order] module=order service=create-order status=success duration=2.456ms request_size=128B response_size=256B request_id=550e8400-e29b-41d4-a716-446655440000
```

## Practical Examples

### Log to File with Rotation

```go
file, _ := os.OpenFile(
    "logs/access.log",
    os.O_APPEND|os.O_CREATE|os.O_WRONLY,
    0644,
)
defer file.Close()

accessModule, _ := accesslog.New(
    accesslog.WithOutput(file),
    accesslog.WithFormat(accesslog.FormatJSON), // Better for parsing
)
```

### Development vs Production

```go
var output io.Writer
if os.Getenv("ENV") == "production" {
    file, _ := os.OpenFile("logs/access.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    output = file
} else {
    output = os.Stdout
}

accessModule, _ := accesslog.New(
    accesslog.WithOutput(output),
    accesslog.WithFormat(accesslog.FormatText),
)
```

### Multiple Middleware

```go
app, _ := mono.NewMonoApplication()

// Register request ID first so access logs include IDs
app.Register(requestid.New())

// Register access logging
accessModule, _ := accesslog.New(
    accesslog.WithOutput(os.Stdout),
    accesslog.WithFormat(accesslog.FormatJSON),
)
app.Register(accessModule)

// Then register application modules
app.Register(&OrderModule{})
```

## Performance Characteristics

- **Memory**: Negligible overhead
- **CPU**: ~1-5µs per request (I/O bound)
- **I/O**: Single write per request (buffered)

{% hint style="warning" %}
**Performance Note:** Writing logs synchronously to slow I/O (network storage, slow disk) can impact request latency. Consider using buffered writers or async logging for high-throughput applications.
{% endhint %}

For high-performance applications, consider:
- Using JSON format for faster parsing
- Buffering log writes
- Separating access logs from application logs

## Common Pitfalls

### Registration order

{% hint style="danger" %}
Registering access log middleware **after** application modules means those modules won't be logged properly.
{% endhint %}

```go
// ❌ WRONG: Access log registered after modules
app.Register(&OrderModule{})
app.Register(accesslog.New())  // Too late!

// ✅ CORRECT: Access log registered before modules
app.Register(requestid.New())  // First: request IDs
app.Register(accesslog.New())  // Second: access logging
app.Register(&OrderModule{})   // Then: application modules
```

### Writing to slow destinations

Writing access logs to network storage or slow disks can add latency to every request. For production:

```go
// ❌ AVOID: Direct network writes
networkFile, _ := os.OpenFile("/mnt/nfs/access.log", ...)
accesslog.New(accesslog.WithOutput(networkFile))

// ✅ BETTER: Local fast storage with async shipping
localFile, _ := os.OpenFile("/var/log/access.log", ...)
accesslog.New(accesslog.WithOutput(localFile))
// Ship logs async with fluentd, filebeat, etc.
```

### Logging sensitive data in request IDs

If your request IDs contain user information or tokens, they'll appear in access logs. Use opaque UUIDs instead.

## Troubleshooting

### No logs appearing

1. Check middleware is registered before modules
2. Verify output destination is writable
3. Check that services are actually being called
4. Ensure the module/service names match what you expect

### Performance degradation

- Reduce output verbosity if possible
- Use JSON format instead of text
- Consider writing to a fast storage medium
- Profile to identify bottlenecks

## Related Documentation

- [Request ID Middleware](requestid.md)
- [Audit Middleware](audit.md)
- [Middleware System](README.md)
- [Core Concepts - Modules](../core-concepts/modules.md)

---

For security logging, see [Audit Middleware](audit.md).
