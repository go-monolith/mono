# Audit Middleware

The `audit` middleware provides tamper-evident security audit logging for the mono-framework. It captures framework events with optional cryptographic hash chaining to detect log tampering.

## Features

- **Event Logging**: Automatically logs module lifecycle and service registration events
- **Hash Chaining**: Optional SHA-256 hash chain for tamper detection
- **User Context**: Track which user triggered each event
- **Custom Audit Trail**: Channel service for modules to log custom audit entries
- **Observer Pattern**: Passes events through unchanged (non-intrusive)
- **JSON Output**: Structured JSON format for easy parsing and analysis

## Quick Start

```go
package main

import (
    "os"

    "github.com/go-monolith/mono/middleware/audit"
    "github.com/go-monolith/mono"
)

func main() {
    // Create audit log file with restricted permissions
    auditFile, _ := os.OpenFile("audit.log",
        os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)

    // Create audit middleware with hash chaining enabled
    auditModule, _ := audit.New(
        audit.WithOutput(auditFile),
        audit.WithHashChaining(""),  // Start new chain
    )

    // Create and configure framework
    framework, _ := mono.NewMonoApplication()
    framework.Register(auditModule)  // Register early to capture events
    framework.Start(ctx)
}
```

## Configuration Options

### Output Writer

```go
// Write to file
auditFile, _ := os.OpenFile("audit.log",
    os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
audit.New(audit.WithOutput(auditFile))

// Write to stdout
audit.New(audit.WithOutput(os.Stdout))
```

### Hash Chaining

Hash chaining creates a cryptographic chain where each entry contains:
- `prev_hash`: SHA-256 hash of the previous entry
- `entry_hash`: SHA-256 hash of the current entry

This allows detection of any tampering or deletion of log entries.

```go
// Start a new hash chain
audit.New(
    audit.WithOutput(auditFile),
    audit.WithHashChaining(""),
)

// Continue an existing chain (after restart)
lastHash := getLastHashFromPreviousSession()
audit.New(
    audit.WithOutput(auditFile),
    audit.WithHashChaining(lastHash),
)
```

### User Context

Track which user triggered each event:

```go
audit.New(
    audit.WithOutput(auditFile),
    audit.WithUserContext(func(ctx context.Context) string {
        if userID, ok := ctx.Value(userIDKey).(string); ok {
            return userID
        }
        return "system"
    }),
)
```

## Captured Events

The audit middleware automatically captures these framework events:

| Event Type | Description | Details Captured |
|------------|-------------|------------------|
| `module.started` | Module started successfully | duration_ms |
| `module.stopped` | Module stopped | error (if any) |
| `service.registered` | Service registered | service_type |
| `configuration.updated` | Configuration changed | option_name, old_value, new_value |
| `custom.audit_trail` | Custom entry from module | User-defined |

**Note**: `module.registered` is NOT captured because module registration occurs before the middleware chain is built (during `framework.Register()`, before `framework.Start()`).

## Log Output Format

Each log entry is a JSON object on a single line:

```json
{"timestamp":"2024-01-15T10:30:00Z","event_type":"module.started","module_name":"inventory","details":{"duration_ms":5},"user_context":"","prev_hash":"abc123...","entry_hash":"def456..."}
{"timestamp":"2024-01-15T10:30:00Z","event_type":"service.registered","service_name":"check-stock","module_name":"inventory","details":{"service_type":"request_reply"},"user_context":"","prev_hash":"def456...","entry_hash":"ghi789..."}
```

### Entry Fields

| Field | Type | Description |
|-------|------|-------------|
| `timestamp` | string | UTC timestamp in RFC3339 format |
| `event_type` | string | Type of security event |
| `module_name` | string | Module name (if applicable) |
| `service_name` | string | Service name (if applicable) |
| `details` | object | Event-specific structured data |
| `user_context` | string | User/request context |
| `prev_hash` | string | SHA-256 hash of previous entry |
| `entry_hash` | string | SHA-256 hash of this entry |

## Custom Audit Trail Service

Modules can log custom audit entries via the built-in channel service:

```go
// In your module, get the audit adapter
func (m *MyModule) SetDependencyServiceContainer(dep string, container types.ServiceContainer) {
    if dep == "audit" {
        m.auditAdapter = audit.NewAdapter(container)
    }
}

// Log custom audit entries
func (m *MyModule) handleSensitiveOperation(ctx context.Context) error {
    // Perform operation...

    // Log to audit trail
    err := m.auditAdapter.SaveEntry(ctx, audit.Entry{
        EventType:   "custom.user_access",
        ModuleName:  m.Name(),
        Details: map[string]any{
            "action":      "data_export",
            "record_count": 1000,
        },
    })

    return err
}
```

## Verifying the Hash Chain

Use the `VerifyChain` function to detect tampering:

```go
import "github.com/go-monolith/mono/middleware/audit"

// Read audit log entries
entries := readEntriesFromFile("audit.log")

// Verify the chain
valid, err := audit.VerifyChain(entries)
if err != nil {
    log.Printf("Chain verification error: %v", err)
}
if !valid {
    log.Printf("WARNING: Audit log may have been tampered with!")
}
```

## Sensitive Value Redaction

Configuration change events automatically redact sensitive values:

```go
// If a configuration option contains sensitive keywords like "password",
// "secret", "token", "key", the value is redacted:

// Original: {"option_name": "nats_password", "old_value": "secret123", "new_value": "newsecret"}
// Logged:   {"option_name": "nats_password", "old_value": "[REDACTED]", "new_value": "[REDACTED]"}
```

## Example Output

```json
{"timestamp":"2024-01-15T10:30:00Z","event_type":"module.started","module_name":"audit","details":{"duration_ms":1},"prev_hash":"","entry_hash":"a1b2c3..."}
{"timestamp":"2024-01-15T10:30:00Z","event_type":"module.started","module_name":"inventory","details":{"duration_ms":5},"prev_hash":"a1b2c3...","entry_hash":"d4e5f6..."}
{"timestamp":"2024-01-15T10:30:00Z","event_type":"service.registered","service_name":"check-stock","module_name":"inventory","details":{"service_type":"request_reply"},"prev_hash":"d4e5f6...","entry_hash":"g7h8i9..."}
{"timestamp":"2024-01-15T10:30:01Z","event_type":"module.stopped","module_name":"inventory","details":{},"prev_hash":"g7h8i9...","entry_hash":"j0k1l2..."}
```

## Security Best Practices

1. **File Permissions**: Use restrictive permissions (0600) for audit log files
2. **Enable Hash Chaining**: Always enable hash chaining in production
3. **Persist Last Hash**: Store the last hash externally to detect file truncation
4. **Regular Verification**: Periodically verify the hash chain integrity
5. **Backup Audit Logs**: Store audit logs on separate, secure storage
6. **Monitor for Gaps**: Alert on missing or out-of-sequence entries

## Thread Safety

The audit module is thread-safe:
- All writes are protected by mutex
- Hash chain updates are atomic
- Channel service uses buffered channels with proper shutdown coordination

## Performance Considerations

- Hash computation: ~1μs per entry (SHA-256)
- JSON encoding: ~500ns per entry
- File write: varies by storage
- Total overhead: typically < 5μs per event

The audit module is an observer - it does not modify events, so it adds minimal latency to the request path.

## API Reference

### Functions

```go
// New creates a new audit middleware module
func New(opts ...Option) (*AuditModule, error)

// NewAdapter creates a client adapter for the audit trail service
func NewAdapter(container types.ServiceContainer) *Adapter

// VerifyChain verifies the integrity of an audit log hash chain
func VerifyChain(entries []Entry) (bool, error)
```

### Options

```go
// WithOutput sets the output writer for audit logs
func WithOutput(w io.Writer) Option

// WithHashChaining enables hash chaining with optional initial hash
func WithHashChaining(lastSavedHash string) Option

// WithUserContext sets a function to extract user context
func WithUserContext(fn func(context.Context) string) Option
```

### Event Types

```go
const (
    EventModuleStarted       = "module.started"
    EventModuleStopped       = "module.stopped"
    EventServiceRegistered   = "service.registered"
    EventConfigurationUpdate = "configuration.updated"
    EventCustomAuditTrail    = "custom.audit_trail"
)
```

## Analyzing Audit Logs

```bash
# View all module lifecycle events
cat audit.log | jq 'select(.event_type | startswith("module."))'

# Find configuration changes
cat audit.log | jq 'select(.event_type == "configuration.updated")'

# Extract all service registrations
cat audit.log | jq 'select(.event_type == "service.registered") | {service: .service_name, module: .module_name, type: .details.service_type}'

# Verify hash chain with jq (simple check)
cat audit.log | jq -s 'reduce .[] as $item ([]; . + [$item.prev_hash, $item.entry_hash]) | unique | length'
```

## See Also

- [Access Log Middleware](../accesslog/README.md) - Request/response logging
- [Request ID Middleware](../requestid/README.md) - Request tracing
- [Multi-Module Example](../../examples/multi-module/README.md) - Integration examples

## License

See the main framework LICENSE file.
