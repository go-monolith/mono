# Access Log Middleware

The Access Log Middleware provides HTTP-style access logging for all service handlers, capturing request/response details including timing, sizes, status, and request IDs.

## Overview

The `accesslog` middleware automatically logs every service call with detailed information about the request and response. It's similar to HTTP access logging in traditional web servers.

{% hint style="info" %}
**Order matters:** Register the access log middleware **before** the Request ID middleware. Middleware wrap handlers in registration order, so the last one registered runs first; `requestid` must run before `accesslog` for a generated ID to reach the access log. Queue group and stream consumer entries only show an ID that arrived in the `X-Request-ID` header. The position relative to application modules does not matter.
{% endhint %}

## Features

- **Automatic Logging**: Logs service handler executions (see [What Is Logged](#what-is-logged))
- **Multiple Formats**: Text and JSON output formats
- **Performance Metrics**: Measures request duration and message sizes
- **Request ID Integration**: Automatically includes request IDs if available
- **Configurable Output**: Write to a file or any `io.Writer`
- **Field Selection**: Choose which fields appear in each entry

## Installation

Import the package:

```go
import "github.com/go-monolith/mono/middleware/accesslog"
```

## Signatures

### New

```go
func New(opts ...Option) (*AccessLogModule, error)
```

Creates a new access log middleware instance with the given options. `WithOutput` is required: without it `New` returns the error `access log module requires WithOutput option`.

### WithOutput

```go
func WithOutput(w io.Writer) Option
```

Sets the output destination for logs (required). The module syncs and closes the writer on `Stop` when it implements `Sync()` / `io.Closer`.

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
    "github.com/go-monolith/mono"
    "github.com/go-monolith/mono/middleware/accesslog"
)

func main() {
    // Open the access log file (the module closes it on Stop)
    accessFile, err := os.OpenFile("access.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
    if err != nil {
        panic(err)
    }

    // Create access log middleware
    accessModule, err := accesslog.New(
        accesslog.WithOutput(accessFile),
        accesslog.WithFormat(accesslog.FormatText),
    )
    if err != nil {
        panic(err)
    }

    // Create application
    app, _ := mono.NewMonoApplication()

    // Register middleware (any position relative to modules)
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
| `WithOutput` | `io.Writer` | Output destination for logs | required |
| `WithFormat` | `Format` | Log format (`FormatText` or `FormatJSON`) | `FormatText` |
| `WithFields` | `[]Field` | Fields to include (nil or empty means all) | `AllFields()` |
| `WithRequestIDHeader` | `string` | Header the request ID is read from (empty is rejected) | `"X-Request-ID"` |

Field constants: `FieldTimestamp`, `FieldRequestID`, `FieldModule`, `FieldService`, `FieldServiceType`, `FieldDurationMS`, `FieldStatus`, `FieldRequestSize`, `FieldResponseSize`; `AllFields()` returns all of them and `FieldName(f)` returns the key used in the output. If you change the request ID header with `requestid.WithHeaderName`, pass the same name to `WithRequestIDHeader`.

{% hint style="warning" %}
**The module closes the writer on `Stop`.** Do not close it yourself, and do not pass `os.Stdout` if the process keeps writing to it after `Stop`.
{% endhint %}

### WithOutput

Write to file:
```go
file, _ := os.OpenFile("access.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
accesslog.New(accesslog.WithOutput(file))
```

Select fields:
```go
accesslog.New(
    accesslog.WithOutput(file),
    accesslog.WithFields([]accesslog.Field{
        accesslog.FieldTimestamp,
        accesslog.FieldService,
        accesslog.FieldDurationMS,
        accesslog.FieldStatus,
    }),
)
```

### WithFormat

Text format (default, human-readable):
```go
accesslog.New(accesslog.WithOutput(file), accesslog.WithFormat(accesslog.FormatText))
// Output: ts=2025-01-15T10:30:00Z request_id=550e8400-e29b-41d4-a716-446655440000 module=payment service=process-payment service_type=request_reply status=success duration_ms=1 request_size=256 response_size=512
```

JSON format (machine-readable):
```go
accesslog.New(accesslog.WithOutput(file), accesslog.WithFormat(accesslog.FormatJSON))
// Output: {"duration_ms":1,"module":"payment","request_id":"550e8400-e29b-41d4-a716-446655440000","request_size":256,"response_size":512,"service":"process-payment","service_type":"request_reply","status":"success","ts":"2025-01-15T10:30:00Z"}
```

## Default Config

```go
accesslog.New(accesslog.WithOutput(file))  // Defaults: text format, all fields, X-Request-ID header
```

## Log Entry Format

### Text Format Fields

```
ts=TIMESTAMP request_id=ID module=MODULE service=SERVICE service_type=TYPE status=STATUS duration_ms=MS request_size=BYTES response_size=BYTES
```

| Field | Description |
|-------|-------------|
| `ts` | UTC start time of the call (RFC 3339) |
| `request_id` | Request ID from the header (or from the context, see the ordering note); always present, empty when unknown |
| `module` | Name of the module providing the service |
| `service` | Service name (event name for event consumers) |
| `service_type` | `request_reply`, `queue_group`, `stream_consumer`, `event_consumer`, `event_stream_consumer` or `channel` |
| `status` | success or error |
| `duration_ms` | Handler time in whole milliseconds (sub-millisecond calls log 0) |
| `request_size` | Size of request in bytes (sum of the batch for stream consumers) |
| `response_size` | Size of response in bytes (0 except for request-reply and channel responses) |

### JSON Format Fields

The JSON format uses the same keys, sorted alphabetically:

```json
{
  "duration_ms": 1,
  "module": "payment",
  "request_id": "550e8400-e29b-41d4-a716-446655440000",
  "request_size": 256,
  "response_size": 512,
  "service": "process-payment",
  "service_type": "request_reply",
  "status": "success",
  "ts": "2025-01-15T10:30:00Z"
}
```

### What Is Logged

- **Request-reply, queue group, stream consumer, event consumer, event stream consumer**: one entry per handler call; batch handlers get one entry per batch, with the request ID of the first message.
- **Channel services**: separate entries for the inbound request and the outbound response, each with `duration_ms` 0.
- **Not logged**: cron handlers and plugin services.
- Write failures do not affect handlers; they are reported on stderr as `CRITICAL: Failed to write access log entry: <error>`.

## Integration with Request ID Middleware

The access log middleware automatically includes request IDs when using the Request ID middleware:

```go
accessModule, err := accesslog.New(accesslog.WithOutput(file))
if err != nil {
    return err
}
app.Register(accessModule) // first: accesslog

requestIDModule, err := requestid.New()
if err != nil {
    return err
}
app.Register(requestIDModule) // second: requestid runs before accesslog
```

Sample output:
```
ts=2025-01-15T10:30:00Z request_id=550e8400-e29b-41d4-a716-446655440000 module=order service=create-order service_type=request_reply status=success duration_ms=2 request_size=128 response_size=256
```

## Practical Examples

### Log to File with Rotation

```go
file, _ := os.OpenFile(
    "logs/access.log",
    os.O_APPEND|os.O_CREATE|os.O_WRONLY,
    0644,
)
// The module closes the file on Stop

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
    output = os.Stderr // stderr: the module closes the writer on Stop
}

accessModule, _ := accesslog.New(
    accesslog.WithOutput(output),
    accesslog.WithFormat(accesslog.FormatText),
)
```

### Multiple Middleware

```go
app, _ := mono.NewMonoApplication()

// Register access logging first
accessModule, err := accesslog.New(
    accesslog.WithOutput(accessFile),
    accesslog.WithFormat(accesslog.FormatJSON),
)
if err != nil {
    return err
}
app.Register(accessModule)

// Register request ID second, so it runs first and access logs include generated IDs
requestIDModule, err := requestid.New()
if err != nil {
    return err
}
app.Register(requestIDModule)

// Register application modules (any position)
app.Register(&OrderModule{})
```

## Performance Characteristics

- **I/O**: one unbuffered write per entry, serialized by a mutex

{% hint style="warning" %}
**Performance Note:** Writing logs synchronously to slow I/O (network storage, slow disk) can impact request latency. Consider using buffered writers or async logging for high-throughput applications.
{% endhint %}

For high-performance applications, consider:
- Buffering log writes (for example a `bufio.Writer`)
- Separating access logs from application logs

## Common Pitfalls

### Registration order

Generated request IDs only appear in the access log when `requestid` is registered **after** `accesslog` (see the ordering note at the top). The position relative to application modules does not matter.

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

1. Verify output destination is writable (write failures are reported on stderr)
2. Check that services are actually being called
3. Ensure the module/service names match what you expect
4. Cron handlers and plugin services are not logged

### Performance degradation

- Reduce output verbosity if possible (`WithFields`)
- Consider writing to a fast storage medium
- Profile to identify bottlenecks

## Related Documentation

- [Request ID Middleware](requestid.md)
- [Audit Middleware](audit.md)
- [Middleware System](README.md)
- [Core Concepts - Modules](../core-concepts/modules.md)

---

For security logging, see [Audit Middleware](audit.md).
