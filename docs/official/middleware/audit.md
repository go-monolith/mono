# Audit Middleware

The Audit Middleware provides tamper-evident audit logging with cryptographic hash chaining, designed for security and compliance scenarios where log integrity is critical.

## Overview

The `audit` middleware automatically logs all framework events with cryptographic signatures to detect tampering. It's designed for compliance requirements, security incident investigation, and maintaining integrity of audit trails.

{% hint style="warning" %}
**Critical:** Register audit middleware **first**, before any other modules. This ensures all framework events are captured in the audit trail.
{% endhint %}

## Features

- **Hash Chaining**: Cryptographic linkage between log entries
- **Tamper Detection**: Detects if logs have been modified after creation
- **Event Logging**: Captures framework lifecycle events
- **User Context**: Tracks user information from request context
- **Custom Audit Trails**: Hook into audit events
- **Secure Permissions**: Automatic file permission management (0600)

## Installation

Import the package:

```go
import "github.com/go-monolith/mono/v1/middleware/audit"
```

## Signatures

### New

```go
func New(opts ...Option) (MiddlewareModule, error)
```

Creates a new audit middleware instance with the given options.

### WithOutput

```go
func WithOutput(w io.Writer) Option
```

Sets the output destination for audit logs (typically a secure file).

### WithHashChaining

```go
func WithHashChaining(prevHash string) Option
```

Enables hash chaining for tamper detection. Pass empty string to start new chain, or previous hash for log rotation.

## Basic Usage

```go
package main

import (
    "os"
    "context"
    "github.com/go-monolith/mono/v1"
    "github.com/go-monolith/mono/v1/middleware/audit"
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
    defer auditFile.Close()

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

    // Register audit middleware FIRST (before other modules)
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
| `WithOutput` | `io.Writer` | Output destination for audit logs | - |
| `WithHashChaining` | `string` | Previous hash for chain continuity | "" |

### WithOutput

Write to file with secure permissions:
```go
file, _ := os.OpenFile("audit.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
audit.New(audit.WithOutput(file))
```

### WithHashChaining

Start a new chain:
```go
audit.New(audit.WithHashChaining(""))  // New chain
```

Continue existing chain (for log rotation):
```go
lastHash := loadLastHashFromPreviousLog()
audit.New(audit.WithHashChaining(lastHash))  // Resume chain
```

## Default Config

```go
audit.New(audit.WithOutput(file))  // Starts new chain by default
```

## Security Considerations

{% hint style="danger" %}
**Never** use permissions other than `0600` for audit log files. World-readable audit logs (`0644`) or group-readable logs (`0640`) can expose sensitive information.
{% endhint %}

- **Permissions**: Always use secure file permissions (0600) for audit logs
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
lastHash := extractLastHashFromFile("audit.log.old")
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

Each audit entry includes:

```json
{
  "timestamp": "2025-12-30T08:30:15.123Z",
  "event_type": "module_started",
  "details": {
    "module_name": "order",
    "status": "success"
  },
  "hash": "sha256:abc123...",
  "prev_hash": "sha256:def456..."
}
```

| Field | Description |
|-------|-------------|
| `timestamp` | When the event occurred (ISO 8601) |
| `event_type` | Type of framework event (module_started, etc.) |
| `details` | Event-specific information |
| `hash` | SHA-256 hash of current entry |
| `prev_hash` | SHA-256 hash of previous entry |

## Events Captured

The audit middleware captures these framework events:

- `framework_started` - Application framework initialized
- `framework_stopped` - Application framework shut down
- `module_registered` - Module registered with framework
- `module_started` - Module initialization completed
- `module_stopped` - Module shutdown completed
- `module_lifecycle_error` - Error during module lifecycle
- `configuration_event` - Configuration change detected
- `service_registration` - Service registered by module
- `outgoing_message` - Message sent to NATS

## Hash Chaining Explained

Hash chaining provides tamper detection:

```
Entry 1: hash=h1, prev_hash=null
Entry 2: hash=h2, prev_hash=h1
Entry 3: hash=h3, prev_hash=h2
Entry 4: hash=h4, prev_hash=h3
```

If an attacker modifies Entry 2:
- Entry 2's hash changes
- But Entry 3's prev_hash still points to original h2
- Mismatch is detected when verifying chain

## Practical Examples

### Basic Audit Setup

```go
auditFile, _ := os.OpenFile("logs/audit.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
defer auditFile.Close()

auditModule, _ := audit.New(
    audit.WithOutput(auditFile),
    audit.WithHashChaining(""),
)

app, _ := mono.NewMonoApplication()
app.Register(auditModule) // Register FIRST
app.Register(&OrderModule{})
app.Start(context.Background())
```

### Log Rotation with Chain Continuity

```go
// On log rotation
func rotateAuditLog() error {
    // Get last hash from current log
    lastHash := extractLastHashFromFile("logs/audit.log")

    // Close old file
    currentAuditFile.Close()

    // Rotate file
    os.Rename("logs/audit.log", "logs/audit.log.1")

    // Create new file
    newFile, _ := os.OpenFile("logs/audit.log", os.O_CREATE|os.O_WRONLY, 0600)

    // Continue chain with last hash
    newAuditModule, _ := audit.New(
        audit.WithOutput(newFile),
        audit.WithHashChaining(lastHash), // Link to previous log
    )

    app.swapAuditModule(newAuditModule)
    return nil
}
```

### Integration with Request ID Middleware

```go
app.Register(requestid.New())

auditFile, _ := os.OpenFile("audit.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
auditModule, _ := audit.New(
    audit.WithOutput(auditFile),
    audit.WithHashChaining(""),
)
app.Register(auditModule)

app.Register(&OrderModule{})
```

Audit entries will include request IDs for tracing.

## Verifying Audit Log Integrity

### Manual Verification

```bash
# Extract hashes from log
jq '.hash, .prev_hash' audit.log > hashes.txt

# Verify chain
python3 verify_chain.py audit.log
```

### Programmatic Verification

```go
func verifyAuditLog(filename string) error {
    file, _ := os.Open(filename)
    defer file.Close()

    scanner := bufio.NewScanner(file)
    var prevHash string

    for scanner.Scan() {
        var entry AuditEntry
        json.Unmarshal(scanner.Bytes(), &entry)

        // Verify chain linkage
        if prevHash != "" && entry.PrevHash != prevHash {
            return fmt.Errorf("chain broken at entry: %v", entry.Timestamp)
        }

        prevHash = entry.Hash
    }

    return nil
}
```

## Performance Characteristics

- **Memory**: ~1KB per entry
- **CPU**: ~10-50µs per event (due to hashing)
- **I/O**: Single write per event (buffered)
- **Hash Algorithm**: SHA-256

For high-volume applications, consider:
- Batching audit entries
- Using faster storage
- Separating critical events from less important ones

## Compliance Use Cases

### PCI DSS

Log all access to payment systems and verify integrity:

```go
auditFile, _ := os.OpenFile("logs/pci-audit.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
auditModule, _ := audit.New(
    audit.WithOutput(auditFile),
    audit.WithHashChaining(""),
)
```

### HIPAA

Maintain tamper-proof audit trail of PHI access:

```go
// Automatic audit of all events with hash chaining
app.Register(auditModule)
```

### Financial Systems

Immutable record of transactions and state changes:

```go
// Chain continues across log rotations
lastHash := getLastHashFromArchive()
auditModule, _ := audit.New(
    audit.WithOutput(newLogFile),
    audit.WithHashChaining(lastHash),
)
```

## Best Practices

✓ **Do**
- Register audit middleware FIRST, before other modules
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

**Cause**: Hash computation overhead on high-volume systems

**Solution**:
- Use dedicated audit thread
- Batch audit entries
- Use faster storage for audit logs

## Related Documentation

- [Access Log Middleware](accesslog.md)
- [Request ID Middleware](requestid.md)
- [Middleware System](README.md)
- [Core Concepts - Modules](../core-concepts/modules.md)

---

For request tracking, see [Request ID Middleware](requestid.md).
