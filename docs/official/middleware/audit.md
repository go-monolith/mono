# Audit Middleware

The `audit` middleware records module start/stop and service and event consumer registrations as JSON lines, optionally linked by a SHA-256 hash chain to detect tampering; it does **not** record service calls or data access.

Read this page to set up an audit log, to read its format, or to verify its hash chain.

It can be registered in any position: the framework collects middleware at `Start`, before any module starts (see [Ordering](README.md#ordering)).

## Features

- **Hash Chaining**: Optional cryptographic linkage between log entries (off by default)
- **Tamper Detection**: Detects if logs have been modified after creation (`audit.VerifyChain`)
- **Event Logging**: Captures module lifecycle and registration events
- **User Context**: A function you supply (`WithUserContext`) fills `user_context`; it is not called with request contexts

## Basic Usage

```go
package main

import (
    "context"
    "os"

    "github.com/go-monolith/mono"
    "github.com/go-monolith/mono/middleware/audit"
)

func main() {
    // Open audit log file with secure permissions (the module closes it on Stop)
    auditFile, err := os.OpenFile("audit.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
    if err != nil {
        panic(err)
    }

    auditModule, err := audit.New(
        audit.WithOutput(auditFile),
        audit.WithHashChaining(""), // Start new chain
    )
    if err != nil {
        panic(err)
    }

    app, _ := mono.NewMonoApplication()
    app.Register(auditModule) // any position
    app.Register(&OrderModule{})

    app.Start(context.Background())
}
```

## Configuration Options

`func New(opts ...Option) (*AuditModule, error)` takes these options:

| Option | Type | Description | Default |
|--------|------|-------------|---------|
| `WithOutput` | `io.Writer` | Output destination for audit logs | required |
| `WithHashChaining` | `string` | Enables chaining; `""` starts a new chain, the last `entry_hash` of the previous log continues it | disabled |
| `WithUserContext` | `func(context.Context) string` | Fills `user_context` (nil is rejected) | returns `""` |

- `WithOutput` is required: without it `New` returns the error `audit module requires WithOutput option`.
- **The module closes the writer on `Stop`** (it calls `Sync()` and `Close()` when the writer implements them. `os.Stdout` and `os.Stderr` are never closed, and a pipe or terminal, which cannot be synced, does not fail `Stop`); do not close it yourself.
- **A module whose log was closed cannot be started again**: `Start` returns `audit.ErrOutputClosed`. Build a new module with `audit.New` for a new application. When a failed `Start` of the application is rolled back, the log is flushed and left open, so a retried `Start` works (since v0.2.18).
- **Hash chaining is off by default**: `audit.New(audit.WithOutput(file))` writes `prev_hash` and `entry_hash` empty.
- The `WithUserContext` function is called with the `Start`/`Stop` context or `context.Background()`, not with a request context.

Continue an existing chain (for log rotation, see [below](#log-rotation-with-chain-continuity)):

```go
lastHash := loadLastHashFromPreviousLog() // your helper: last entry_hash of the old file
audit.New(audit.WithOutput(file), audit.WithHashChaining(lastHash))
```

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

The `audit.EventType` constants:

| `event_type` | Constant | When |
|--------------|----------|------|
| `module.started` | `EventModuleStarted` | A module started; `details.duration_ms` |
| `module.stopped` | `EventModuleStopped` | A module stopped; `details.error` when it failed |
| `service.registered` | `EventServiceRegistered` | A service, event consumer or event stream consumer was registered; `details.service_type` |
| `configuration.updated` | `EventConfigurationUpdate` | Deprecated, never emitted: the framework's configuration does not change at runtime, so it never calls `OnConfigurationChange` |
| `custom.audit_trail` | `EventCustomAuditTrail` | Custom entries a module sends through `audit.NewAuditAdapter`: list `"audit"` in its `Dependencies()` and build the adapter from the container passed to `SetDependencyServiceContainer` for it (see the `NewAuditAdapter` godoc). `SaveAuditTrail` waits for the confirmation of each entry, `AsyncSaveAuditTrail` does not (see [Custom Audit Entries](#custom-audit-entries)) |

Not recorded: module registration, middleware modules themselves, plugins, and service calls. Records of access to data or of transactions (PCI DSS, HIPAA, financial systems) need application code.

## Custom Audit Entries

A module that lists `"audit"` in its `Dependencies()` saves its own entries through the adapter:

```go
saved, err := m.audit.SaveAuditTrail(ctx, []audit.Entry{{
    EventType:  audit.EventCustomAuditTrail,
    ModuleName: "orders",
    Details:    map[string]any{"action": "refund", "order_id": "o-1"},
}})
```

- **`SaveAuditTrail` returns how many entries are in the log.** An entry the module could not write is not counted, and the first error is returned. Any number of entries can be saved in one call.
- **Pass a context with a deadline.** When it ends first, the call returns the number confirmed so far and an error; entries already sent may still be written. After the audit module was stopped no confirmation arrives, so the call returns when the context ends.
- **`AsyncSaveAuditTrail` does not wait** and cannot report a failed write. Entries it handed over before `Stop` are written by `Stop`; entries sent after `Stop` are not written.
- **A failed write** is also reported on stderr (`CRITICAL: Failed to write audit entry`). The hash chain does not advance over an entry that is not in the log, so the entries that are still verify.

## Hash Chaining Explained

```
Entry 1: entry_hash=h1, prev_hash=""
Entry 2: entry_hash=h2, prev_hash=h1
Entry 3: entry_hash=h3, prev_hash=h2
```

If an attacker modifies Entry 2, its recomputed hash no longer matches `h2`, and Entry 3's `prev_hash` still points to the original `h2`, so verification detects the change. Manually editing the file, or restarting without preserving the last hash, breaks the chain.

### Log Rotation with Chain Continuity

The audit module cannot be replaced in a running application, so rotate at restart: read the last `entry_hash` of the old file and start the new process with `audit.WithHashChaining(lastHash)` on the new file. Starting the new file with `WithHashChaining("")` breaks the chain.

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

`VerifyChain` expects the first entry to have an empty `prev_hash`. Verify a file continued with `WithHashChaining(lastHash)` with `audit.VerifyChainFrom(entries, lastHash)`, where `lastHash` is the last `entry_hash` of its predecessor.

An entry is hashed in the form a reader of the log gets it in (`audit.CanonicalDetails`), so the log verifies after it was written and parsed again whatever Go values the details held: empty details are hashed like missing ones, and a struct like the map it is parsed as. Up to v0.2.16 an entry with empty details, which every clean `module.stopped` entry is, never verified, so `VerifyChain` rejected the untampered log of a normal run. Logs written by those versions verify now (the hash they gave such an entry is accepted), and the hashes of entries that verified before are unchanged.

For a quick look at the hashes: `jq '.entry_hash, .prev_hash' audit.log`.

Other exported helpers: `audit.NewHashChain(lastSavedHash)` / `HashChain.AddEntry`, the `audit.Entry` type and the `audit.EventType` constants.

## Security Considerations

{% hint style="danger" %}
**Never** use permissions other than `0600` for audit log files. World-readable (`0644`) or group-readable (`0640`) audit logs can expose sensitive information. The module does not set permissions: open the file with mode 0600 yourself.
{% endhint %}

- **Storage**: Store audit logs on separate filesystems if possible, and in separate files from access logs
- **Archival**: Archive and secure old logs away from application access
- **Verification**: Regularly verify hash chain integrity, and do not ignore failures
- **Rotation**: Never lose the last hash when rotating logs; document your chain continuation strategy
- **Sensitive Data**: Audit logs may contain user identifiers or other sensitive information. Follow data protection regulations (GDPR, CCPA): retention policies, encryption at rest, access restricted to authorized personnel

## Performance

- **Cost**: per registration or module lifecycle event, not per request
- **I/O**: one unbuffered write per entry (the JSON line), serialized by a mutex; entries are written synchronously, so a slow output writer slows the events being recorded
- **Hash Algorithm**: SHA-256

## Troubleshooting

### Chain Broken Error

**Cause**: Log file was edited, or hash chain wasn't properly continued

**Solution**:
1. Check file permissions (should be 0600)
2. Verify last hash was saved before rotation
3. Restore from backup if available

## Related Documentation

- [Access Log Middleware](accesslog.md)
- [Request ID Middleware](requestid.md)
- [Middleware System](README.md)
- [Core Concepts - Modules](../core-concepts/modules.md)
