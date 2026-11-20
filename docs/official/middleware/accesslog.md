# Access Log Middleware

The `accesslog` middleware writes one access log entry per service handler call, with timing, request and response sizes, status and request ID, in text or JSON. It works like HTTP access logging in traditional web servers.

Read this page to set up access logging, to choose the fields and format, or to understand an entry.

{% hint style="info" %}
**Ordering:** register `accesslog` **before** `requestid` so that generated request IDs reach the access log. See [Ordering](README.md#ordering).
{% endhint %}

## Features

- **Automatic Logging**: Logs service handler executions (see [What Is Logged](#what-is-logged))
- **Multiple Formats**: Text and JSON output formats
- **Performance Metrics**: Measures request duration and message sizes
- **Request ID Integration**: Includes request IDs if available
- **Configurable Output**: Write to a file or any `io.Writer`
- **Field Selection**: Choose which fields appear in each entry

## Basic Usage

```go
package main

import (
    "context"
    "os"

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

    app, _ := mono.NewMonoApplication()

    // Register middleware (any position relative to modules)
    app.Register(accessModule)
    app.Register(&OrderModule{})
    app.Register(&PaymentModule{})

    app.Start(context.Background())
}
```

## Configuration Options

`func New(opts ...Option) (*AccessLogModule, error)` takes these options:

| Option | Type | Description | Default |
|--------|------|-------------|---------|
| `WithOutput` | `io.Writer` | Output destination for logs | required |
| `WithFormat` | `Format` | Log format (`FormatText` or `FormatJSON`) | `FormatText` |
| `WithFields` | `[]Field` | Fields to include (nil or empty means all) | `AllFields()` |
| `WithRequestIDHeader` | `string` | Header the request ID is read from (empty is rejected) | `"X-Request-ID"` |

- `WithOutput` is required: without it `New` returns the error `access log module requires WithOutput option`.
- Field constants: `FieldTimestamp`, `FieldRequestID`, `FieldModule`, `FieldService`, `FieldServiceType`, `FieldDurationMS`, `FieldStatus`, `FieldRequestSize`, `FieldResponseSize`. `AllFields()` returns all of them and `FieldName(f)` returns the key used in the output.
- If you change the request ID header with `requestid.WithHeaderName`, pass the same name to `WithRequestIDHeader`.

{% hint style="warning" %}
**The module closes the writer on `Stop`** (it calls `Sync()` and `Close()` when the writer implements them. `os.Stdout` and `os.Stderr` are never closed, and a pipe or terminal, which cannot be synced, does not fail `Stop`). Do not close it yourself.

A module whose log was closed cannot be started again: `Start` returns `accesslog.ErrOutputClosed`, so build a new module with `accesslog.New` for a new application. When a failed `Start` of the application is rolled back, the log is flushed and left open, and the module logs again after the retried `Start` (since v0.2.18).
{% endhint %}

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

## Log Entry Format

### Text Format

`accesslog.WithFormat(accesslog.FormatText)` (the default, human-readable):

```
ts=2025-01-15T10:30:00Z request_id=550e8400-e29b-41d4-a716-446655440000 module=payment service=process-payment service_type=request_reply status=success duration_ms=1 request_size=256 response_size=512
```

### JSON Format

`accesslog.WithFormat(accesslog.FormatJSON)` (machine-readable) uses the same keys, sorted alphabetically:

```json
{"duration_ms":1,"module":"payment","request_id":"550e8400-e29b-41d4-a716-446655440000","request_size":256,"response_size":512,"service":"process-payment","service_type":"request_reply","status":"success","ts":"2025-01-15T10:30:00Z"}
```

### Fields

| Field | Description |
|-------|-------------|
| `ts` | UTC start time of the call (RFC 3339) |
| `request_id` | Request ID from the header (or from the context, see [Ordering](README.md#ordering)); always present, empty when unknown. The header is written by the sender, so the value is sanitised: at most 128 bytes of ASCII letters, digits and `-_.:/=+@~`, every other byte replaced by `_` (since v0.2.18). A value with spaces can therefore not add fields to a text line |
| `module` | Name of the module providing the service |
| `service` | Service name (event name for event consumers) |
| `service_type` | `request_reply`, `queue_group`, `stream_consumer`, `event_consumer`, `event_stream_consumer` or `channel` |
| `status` | success or error |
| `duration_ms` | Handler time in whole milliseconds (sub-millisecond calls log 0) |
| `request_size` | Size of request in bytes (sum of the batch for stream consumers) |
| `response_size` | Size of response in bytes (0 except for request-reply and channel responses) |

### What Is Logged

- **Request-reply, queue group, stream consumer, event consumer, event stream consumer**: one entry per handler call. Batch handlers get one entry per batch, with the request ID of the first message.
- **Channel services**: separate entries for the inbound request and the outbound response, each with `duration_ms` 0.
- **Not logged**: cron handlers and plugin services.
- **Write failures** do not affect handlers; they are reported on stderr as `CRITICAL: Failed to write access log entry: <error>`.

## Integration with Request ID Middleware

Register `requestid` after `accesslog` (see [Ordering](README.md#ordering)):

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

### Log to a File in JSON

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
    output = os.Stderr // stderr stays open after Stop
}

accessModule, _ := accesslog.New(
    accesslog.WithOutput(output),
    accesslog.WithFormat(accesslog.FormatText),
)
```

## Performance

Each entry is one unbuffered write, serialized by a mutex. Writing synchronously to slow I/O (network storage, slow disk) adds latency to every request. For high-throughput applications:

- Buffer log writes (for example a `bufio.Writer`)
- Write to local fast storage and ship the logs asynchronously (fluentd, filebeat, etc.) instead of writing to network storage
- Keep access logs separate from application logs
- Reduce output with `WithFields`

## Common Pitfalls

- **Request IDs missing**: `requestid` must be registered **after** `accesslog` (see [Ordering](README.md#ordering)).
- **Sensitive data in request IDs**: if your request IDs contain user information or tokens, they appear in access logs. Use opaque UUIDs instead.
- **Closing the writer yourself**: the module already closes it on `Stop`.

## Troubleshooting

### No logs appearing

1. Verify output destination is writable (write failures are reported on stderr)
2. Check that services are actually being called
3. Ensure the module/service names match what you expect
4. Cron handlers and plugin services are not logged

## Related Documentation

- [Request ID Middleware](requestid.md)
- [Audit Middleware](audit.md)
- [Middleware System](README.md)
- [Core Concepts - Modules](../core-concepts/modules.md)
