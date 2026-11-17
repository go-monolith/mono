# Audit Middleware

The Audit Middleware provides tamper-evident audit logging with cryptographic hash chaining, designed for security and compliance scenarios where log integrity is critical.

## Overview

The `audit` middleware records module start/stop and service and event consumer registrations as JSON lines, optionally linked by a SHA-256 hash chain to detect tampering. It does not record service calls. It can be registered in any position: the framework collects middleware at `Start`, before any module starts.

## Features

- **Hash Chaining**: Optional cryptographic linkage between log entries (off by default)
- **Tamper Detection**: Detects if logs have been modified after creation (`audit.VerifyChain`)
- **Event Logging**: Captures module lifecycle and registration events
- **User Context**: A function you supply (`WithUserContext`) fills `user_context`; it is not called with request contexts

## Installation

Import the package:

```go
import "github.com/go-monolith/mono/middleware/audit"
```

## Signatures

### New

```go
func New(opts ...Option) (*AuditModule, error)
```

Creates a new audit middleware instance with the given options. `WithOutput` is required: without it `New` returns the error `audit module requires WithOutput option`.

### WithOutput

```go
func WithOutput(w io.Writer) Option
```

Sets the output destination for audit logs (typically a file opened with mode 0600). The module syncs and closes the writer on `Stop` when it implements `Sync()` / `io.Closer`; do not close it yourself.

### WithHashChaining

```go
func WithHashChaining(lastSavedHash string) Option
```

Enables hash chaining for tamper detection. Pass empty string to start new chain, or the last `entry_hash` of the previous log to continue it.

### WithUserContext

```go
func WithUserContext(fn func(context.Context) string) Option
```

Sets the function that fills `user_context` on each entry (nil is rejected; the default returns `""`). The framework calls it with the `Start`/`Stop` context or `context.Background()`, not with a request context.

## Basic Usage

```go
package main

import (
    "os"
    "context"
    "github.com/go-monolith/mono"
    "github.com/go-monolith/mono/middleware/audit"
)

func main() {
    // Open audit log file
    auditFile, err := os.OpenFile(
        "audit.log",
        os.O_APPEND|os.O_CREATE|os.O_WRONLY,
        0600, // Secure permissions
    )
    if err != nil {
        panic(err)
    }
    // The audit module closes the file on Stop

    // Create audit middleware
    auditModule, err := audit.New(
        audit.WithOutput(auditFile),
        audit.WithHashChaining(""), // Start new chain
    )
    if err != nil {
        panic(err)
    }

    // Create application
    app, _ := mono.NewMonoApplication()

    // Register audit middleware (any position)
    app.Register(auditModule)

    // Register your modules
    app.Register(&OrderModule{})

    // Start application
    app.Start(context.Background())
}
```

## Configuration Options

All configuration is done through functional options passed to `New()`:

| Option | Type | Description | Default |
|--------|------|-------------|---------|
| `WithOutput` | `io.Writer` | Output destination for audit logs | required |
| `WithHashChaining` | `string` | Enables chaining; last hash for chain continuity | disabled |
| `WithUserContext` | `func(context.Context) string` | Fills `user_context` | returns `""` |

### WithOutput

Write to file with secure permissions:
```go
file, _ := os.OpenFile("audit.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
audit.New(audit.WithOutput(file))
```

### WithHashChaining

Start a new chain:
```go
audit.New(audit.WithOutput(file), audit.WithHashChaining(""))  // New chain
```

Continue existing chain (for log rotation):
```go
lastHash := loadLastHashFromPreviousLog() // your helper: last entry_hash of the old file
audit.New(audit.WithOutput(file), audit.WithHashChaining(lastHash))  // Resume chain
```

## Default Config

```go
audit.New(audit.WithOutput(file))  // No hash chaining: prev_hash and entry_hash are written empty
```

Hash chaining is off by default; pass `audit.WithHashChaining("")` to start a chain, or the last `entry_hash` to continue one.

## Security Considerations

{% hint style="danger" %}
**Never** use permissions other than `0600` for audit log files. World-readable audit logs (`0644`) or group-readable logs (`0640`) can expose sensitive information.
{% endhint %}

- **Permissions**: Open audit log files with mode 0600 yourself; the module does not set permissions
- **Storage**: Store audit logs on separate filesystems if possible
- **Archival**: Archive and secure old logs away from application access
- **Verification**: Regularly verify hash chain integrity for tampering detection
- **Sensitive Data**: Audit logs may contain sensitive information; restrict access appropriately

## Common Pitfalls

### Breaking the hash chain

{% hint style="danger" %}
Manually editing audit log files or restarting without preserving the last hash breaks the chain and makes logs unverifiable.
{% endhint %}

```go
// ❌ WRONG: Starting fresh after rotation (breaks chain)
newFile, _ := os.OpenFile("audit.log", ...)
audit.New(audit.WithOutput(newFile), audit.WithHashChaining(""))

// ✅ CORRECT: Continue chain from last hash
lastHash := extractLastHashFromFile("audit.log.old") // your helper
newFile, _ := os.OpenFile("audit.log", ...)
audit.New(audit.WithOutput(newFile), audit.WithHashChaining(lastHash))
```

### Using insecure file permissions

```go
// ❌ WRONG: World-readable audit logs
file, _ := os.OpenFile("audit.log", os.O_CREATE|os.O_WRONLY, 0644)

// ✅ CORRECT: Secure permissions
file, _ := os.OpenFile("audit.log", os.O_CREATE|os.O_WRONLY, 0600)
```

### Logging PII without controls

Audit logs may capture user identifiers, IP addresses, or request details. Ensure compliance with data protection regulations (GDPR, CCPA) by:
- Implementing log retention policies
- Encrypting logs at rest
- Restricting access to authorized personnel only

## Audit Log Format

Each audit entry is one JSON line:

```json
{"timestamp":"2025-12-30T08:30:15.123456789Z","event_type":"module.started","module_name":"order","details":{"duration_ms":3},"prev_hash":"","entry_hash":"9f2c…"}
```

| Field | Description |
|-------|-------------|
| `timestamp` | When the event occurred (UTC, RFC 3339 with nanoseconds) |
| `event_type` | Type of event (see below) |
| `module_name` | Module name (omitted when empty) |
| `service_name` | Service or event name (omitted when empty) |
| `details` | Event-specific information (omitted when empty) |
| `user_context` | Value returned by the `WithUserContext` function (omitted when empty) |
| `prev_hash` | `entry_hash` of the previous entry (`""` for the first entry of a new chain, or when chaining is off) |
| `entry_hash` | Hex SHA-256 of timestamp, event_type, module_name, service_name, details, user_context and prev_hash (`""` when chaining is off) |

## Events Captured

The audit middleware captures these events (`audit.EventType` constants):

- `module.started` (`EventModuleStarted`) - a module started; `details.duration_ms`
- `module.stopped` (`EventModuleStopped`) - a module stopped; `details.error` when it failed
- `service.registered` (`EventServiceRegistered`) - a service, event consumer or event stream consumer was registered; `details.service_type`
- `configuration.updated` (`EventConfigurationUpdate`) - defined, but currently never emitted (the framework never calls `OnConfigurationChange`)
- `custom.audit_trail` (`EventCustomAuditTrail`) - custom entries sent through `audit.NewAuditAdapter`; the `audit-trail` channel service it needs is not registered in a running application (the framework does not call `RegisterServices` on middleware modules), so this path is currently not usable

Module registration, middleware modules themselves and plugins are not recorded.

## Hash Chaining Explained

Hash chaining provides tamper detection:

```
Entry 1: entry_hash=h1, prev_hash=""
Entry 2: entry_hash=h2, prev_hash=h1
Entry 3: entry_hash=h3, prev_hash=h2
Entry 4: entry_hash=h4, prev_hash=h3
```

If an attacker modifies Entry 2:
- Entry 2's hash changes
- But Entry 3's prev_hash still points to original h2
- Mismatch is detected when verifying chain

## Practical Examples

### Basic Audit Setup

```go
auditFile, _ := os.OpenFile("logs/audit.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
// The module closes auditFile on Stop

auditModule, err := audit.New(
    audit.WithOutput(auditFile),
    audit.WithHashChaining(""),
)
if err != nil {
    return err
}

app, _ := mono.NewMonoApplication()
app.Register(auditModule)
app.Register(&OrderModule{})
app.Start(context.Background())
```

### Log Rotation with Chain Continuity

The audit module cannot be replaced in a running application, so rotate at restart: read the last `entry_hash` of the old file and start the new process with `audit.WithHashChaining(lastHash)` on the new file.

```go
// At startup, after the previous process has stopped
lastHash := extractLastHashFromFile("logs/audit.log") // your helper
os.Rename("logs/audit.log", "logs/audit.log.1")

newFile, _ := os.OpenFile("logs/audit.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
auditModule, err := audit.New(
    audit.WithOutput(newFile),
    audit.WithHashChaining(lastHash), // Link to previous log
)
if err != nil {
    return err
}
app.Register(auditModule)
```

## Verifying Audit Log Integrity

### Manual Verification

```bash
# Extract hashes from log
jq '.entry_hash, .prev_hash' audit.log > hashes.txt
```

### Programmatic Verification

`audit.VerifyChain` checks both the linkage and each recomputed `entry_hash` (`audit.ComputeEntryHash`):

```go
func verifyAuditLog(filename string) error {
    file, err := os.Open(filename)
    if err != nil {
        return err
    }
    defer file.Close()

    var entries []audit.Entry
    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
        var entry audit.Entry
        if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
            return err
        }
        entries = append(entries, entry)
    }
    if err := scanner.Err(); err != nil {
        return err
    }

    return audit.VerifyChain(entries)
}
```

`VerifyChain` expects the first entry to have an empty `prev_hash`, so a file continued with `WithHashChaining(lastHash)` verifies only when its entries are concatenated after those of its predecessor.

Other exported helpers: `audit.NewHashChain(lastSavedHash)` / `HashChain.AddEntry`, the `audit.Entry` type and the `audit.EventType` constants.

## Performance Characteristics

- **Cost**: per registration or module lifecycle event, not per request
- **I/O**: two unbuffered writes per entry (the JSON and the newline), serialized by a mutex
- **Hash Algorithm**: SHA-256

## Compliance Use Cases

The module records framework lifecycle and registration events only. Records of access to data or of transactions (PCI DSS, HIPAA, financial systems) need application code.

## Best Practices

✓ **Do**
- Use secure file permissions (0600)
- Store audit logs on separate filesystem
- Archive and secure old logs
- Verify chain integrity regularly
- Document chain continuation strategy

✗ **Don't**
- Mix audit and access logs (use separate files)
- Modify audit log files manually
- Use weak permissions on audit files
- Ignore hash chain verification failures
- Lose the last hash when rotating logs

## Troubleshooting

### Chain Broken Error

**Cause**: Log file was edited, or hash chain wasn't properly continued

**Solution**:
1. Check file permissions (should be 0600)
2. Verify last hash was saved before rotation
3. Restore from backup if available

### Performance Issues

**Cause**: Slow output writer (entries are written synchronously)

**Solution**:
- Use faster storage for audit logs

## Related Documentation

- [Access Log Middleware](accesslog.md)
- [Request ID Middleware](requestid.md)
- [Middleware System](README.md)
- [Core Concepts - Modules](../core-concepts/modules.md)

---

For request tracking, see [Request ID Middleware](requestid.md).
