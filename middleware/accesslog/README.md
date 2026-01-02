# Access Log Middleware

The `accesslog` middleware provides HTTP-style access logging for NATS-based service handlers in the mono-framework. It wraps service handlers to capture request/response details, timing, and status.

## Features

- **Handler Wrapping**: Intercepts RequestReply, QueueGroup, and StreamConsumer handlers
- **Configurable Fields**: Select which fields to include in logs
- **Multiple Formats**: Text (key-value) or JSON output
- **Request ID Tracking**: Extract request IDs from message headers
- **Thread-Safe**: Safe for concurrent use across multiple handlers
- **Zero-Copy**: Minimal performance overhead

## Quick Start

```go
package main

import (
    "os"

    "github.com/go-monolith/mono/middleware/accesslog"
    "github.com/go-monolith/mono"
)

func main() {
    // Create access log file
    accessFile, _ := os.OpenFile("access.log",
        os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    defer accessFile.Close()

    // Create access log middleware with default settings
    accessModule, _ := accesslog.New(
        accesslog.WithOutput(accessFile),
    )

    // Create and configure framework
    framework, _ := mono.NewMonoApplication()
    framework.Register(accessModule)  // Register as first module
    framework.Start(ctx)
}
```

## Configuration Options

### Output Format

```go
// Text format (default): ts=... request_id=... module=... service=...
accesslog.New(
    accesslog.WithOutput(os.Stdout),
    accesslog.WithFormat(accesslog.FormatText),
)

// JSON format: {"ts":"...","request_id":"...","module":"..."}
accesslog.New(
    accesslog.WithOutput(os.Stdout),
    accesslog.WithFormat(accesslog.FormatJSON),
)
```

### Field Selection

```go
// Log only specific fields
accesslog.New(
    accesslog.WithOutput(os.Stdout),
    accesslog.WithFields([]accesslog.Field{
        accesslog.FieldTimestamp,
        accesslog.FieldService,
        accesslog.FieldDurationMS,
        accesslog.FieldStatus,
    }),
)

// All available fields (default):
// - FieldTimestamp
// - FieldRequestID
// - FieldModule
// - FieldService
// - FieldMethod
// - FieldDurationMS
// - FieldStatus
// - FieldRequestSize
// - FieldResponseSize
```

### Custom Request ID Header

```go
// Default: X-Request-ID
accesslog.New(
    accesslog.WithOutput(os.Stdout),
    accesslog.WithRequestIDHeader("X-Correlation-ID"),
)
```

## Log Output Examples

### Text Format

```
ts=2024-01-15T10:30:00Z request_id=abc123 module=order service=place-order method=request_reply status=success duration_ms=45 request_size=1024 response_size=512
ts=2024-01-15T10:30:01Z request_id=xyz789 module=payment service=process-payment method=queue_group status=success duration_ms=120 request_size=2048 response_size=0
```

### JSON Format

```json
{"ts":"2024-01-15T10:30:00Z","request_id":"abc123","module":"order","service":"place-order","method":"request_reply","status":"success","duration_ms":45,"request_size":1024,"response_size":512}
{"ts":"2024-01-15T10:30:01Z","request_id":"xyz789","module":"payment","service":"process-payment","method":"queue_group","status":"success","duration_ms":120,"request_size":2048,"response_size":0}
```

## Log Fields

| Field | Description | Example Value |
|-------|-------------|---------------|
| `ts` | UTC timestamp in RFC3339 format | `2024-01-15T10:30:00Z` |
| `request_id` | Request ID from header (default: X-Request-ID) | `abc123` |
| `module` | Module name that registered the service | `order` |
| `service` | Service handler name | `place-order` |
| `method` | Service type: `request_reply`, `queue_group`, `stream_consumer` | `request_reply` |
| `duration_ms` | Handler execution time in milliseconds | `45` |
| `status` | Handler outcome: `success` or `error` | `success` |
| `request_size` | Request data size in bytes | `1024` |
| `response_size` | Response data size in bytes (0 for non-request-reply) | `512` |

## Service Types

The access log middleware wraps the following service types:

- **RequestReply**: Logs request size, response size, and timing
- **QueueGroup**: Logs request size and timing (response_size is 0)
- **StreamConsumer**: Logs total batch size and timing (response_size is 0)
- **Channel**: Passed through unchanged (in-process Go channels, not NATS)

## Best Practices

1. **Register Early**: Register the access log middleware as the first module to ensure it captures all service registrations
2. **Use Buffered Writers**: For high-throughput services, use buffered I/O to reduce syscall overhead
3. **Rotate Logs**: Implement log rotation (e.g., using `lumberjack`) for production deployments
4. **Field Selection**: Limit fields to only what you need for analysis to reduce log size
5. **Request ID**: Always include `X-Request-ID` header in requests for distributed tracing

## Thread Safety

The access log module is thread-safe:
- Concurrent writes are protected by mutex
- Shutdown coordination prevents race conditions during Stop()
- In-flight writes complete before writer is closed

## Performance Considerations

- Minimal overhead: ~1-2μs per log entry
- Single write syscall per entry (line + newline combined)
- No allocations in hot path (pre-computed formatters)
- Skips writes during shutdown to prevent blocking

## Error Handling

Write failures are logged to stderr but do not propagate to service handlers. This prevents logging failures from impacting application availability.

```
CRITICAL: Failed to write access log entry: write /var/log/access.log: no space left on device
```

## Integration with Other Tools

### Parsing Logs

**Text format with `grep`:**
```bash
grep "status=error" access.log
grep "duration_ms=[0-9]\{3,\}" access.log  # Find slow requests (100ms+)
```

**JSON format with `jq`:**
```bash
cat access.log | jq 'select(.status == "error")'
cat access.log | jq 'select(.duration_ms > 100)'
```

### Log Aggregation

The JSON format is compatible with:
- Elasticsearch/Kibana
- Prometheus (with json_exporter)
- Datadog
- Splunk
- CloudWatch Logs

## License

See the main framework LICENSE file.
