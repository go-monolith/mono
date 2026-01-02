# 🧬 Middleware

Middleware modules provide cross-cutting functionality that runs alongside your application modules. They intercept framework events and service handlers to add features like logging, auditing, and request tracking.

## What is Middleware?

Middleware modules implement the `MiddlewareModule` interface to:

- Intercept service handler execution
- Hook into framework lifecycle events
- Add cross-cutting concerns without modifying module code
- Maintain separation of concerns

## Built-in Middleware

The framework comes with three built-in middleware modules:

### 1. Access Log Middleware

HTTP-style access logging for all service handlers.

- **Package**: `github.com/go-monolith/mono/v1/middleware/accesslog`
- **Purpose**: Log request/response details including timing, sizes, and status
- **Features**:
  - Text and JSON output formats
  - Request ID tracking integration
  - Configurable output destination
  - Performance metrics

**Quick Start**:
```go
accessModule, _ := accesslog.New(
    accesslog.WithOutput(os.Stdout),
    accesslog.WithFormat(accesslog.FormatJSON),
)

app.Register(accessModule)
```

Learn more: [Access Log Middleware](accesslog.md)

### 2. Audit Middleware

Tamper-evident audit logging with cryptographic hash chaining.

- **Package**: `github.com/go-monolith/mono/v1/middleware/audit`
- **Purpose**: Log all framework events with cryptographic integrity protection
- **Features**:
  - Hash chaining for tamper detection
  - User context tracking
  - Custom audit trail hooks
  - Secure log file permissions

**Quick Start**:
```go
auditFile, _ := os.OpenFile("audit.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
auditModule, _ := audit.New(
    audit.WithOutput(auditFile),
    audit.WithHashChaining(""),
)

app.Register(auditModule)
```

Learn more: [Audit Middleware](audit.md)

### 3. Request ID Middleware

Automatic request ID generation and propagation for distributed tracing.

- **Package**: `github.com/go-monolith/mono/v1/middleware/requestid`
- **Purpose**: Track requests across module boundaries for debugging and tracing
- **Features**:
  - Automatic UUID generation
  - Request ID extraction from headers
  - Context injection for handlers
  - Automatic propagation to outgoing messages

**Quick Start**:
```go
requestIDMiddleware, _ := requestid.New()
app.Register(requestIDMiddleware)
```

Learn more: [Request ID Middleware](requestid.md)

## Middleware Lifecycle

Middleware modules follow the standard module lifecycle:

```
1. Module Registration (earliest)
   └─ Middleware can register and prepare

2. Other Module Startup
   └─ Middleware may be called during module initialization

3. Service Handler Execution
   └─ Middleware wraps handlers and logs events

4. Framework Events
   └─ Middleware hooks into framework lifecycle

5. Application Shutdown
   └─ Middleware cleans up resources (last)
```

## Using Multiple Middleware

You can register multiple middleware modules together:

```go
app, _ := mono.NewMonoApplication()

// Register middleware
app.Register(requestid.New())
app.Register(accesslog.New(accesslog.WithFormat(accesslog.FormatText)))
app.Register(audit.New(audit.WithOutput(auditFile)))

// Register application modules
app.Register(&OrderModule{})
app.Register(&PaymentModule{})

// Start
app.Start(context.Background())
```

Middleware are processed in registration order. The `requestid` middleware is often registered first since other middleware may use request IDs.

## Middleware vs Modules

| Aspect | Middleware | Module |
|--------|-----------|--------|
| **Purpose** | Cross-cutting concerns | Business logic |
| **Registration** | Called before modules | Called alongside modules |
| **Interface** | MiddlewareModule | Module |
| **Lifecycle Hooks** | Framework events | Module lifecycle |
| **Dependencies** | Can depend on modules | Independent |
| **Examples** | Logging, tracing, security | Order, Payment, Notification |

## Common Patterns

### Conditional Middleware

Enable middleware based on configuration:

```go
app, _ := mono.NewMonoApplication(mono.WithLogLevel(mono.LogLevelDebug))

if config.EnableAudit {
    auditModule, _ := audit.New()
    app.Register(auditModule)
}

if config.EnableAccessLog {
    accessModule, _ := accesslog.New()
    app.Register(accessModule)
}
```

### Middleware Chain

Multiple middleware working together:

```
Request
  ├→ RequestID Middleware (generates ID)
  ├→ AccessLog Middleware (logs request)
  ├→ Audit Middleware (security logging)
  └→ Handler (processes request)
```

### Custom Middleware

Create your own middleware by implementing `MiddlewareModule`:

```go
type MyMiddleware struct{}

func (m *MyMiddleware) Name() string { return "my-middleware" }

func (m *MyMiddleware) OnModuleLifecycleEvent(ctx context.Context, event *types.ModuleLifecycleEvent) error {
    // Hook into module lifecycle
    return nil
}

func (m *MyMiddleware) OnServiceRegistration(ctx context.Context, sr *types.ServiceRegistration) error {
    // Hook into service registration
    return nil
}

// More hooks...
```

## Performance Considerations

Middleware adds processing overhead. Consider:

- **Request ID**: Minimal overhead (~100ns per request)
- **Access Log**: Moderate overhead (~1-5µs per request, depends on I/O)
- **Audit**: Higher overhead (~10-50µs per request, due to hashing)

For high-performance applications, be selective about which middleware you enable.

## Best Practices

✓ **Do**
- Register middleware early before modules
- Keep middleware focused on single concerns
- Use middleware for cross-cutting concerns
- Configure output files with appropriate permissions (especially audit)

✗ **Don't**
- Put business logic in middleware
- Have middleware depend on specific modules
- Use middleware for request transformation (use handlers instead)
- Log sensitive data (use the framework's redaction features)

## Related Documentation

- [Access Log Middleware](accesslog.md)
- [Audit Middleware](audit.md)
- [Request ID Middleware](requestid.md)
- [Core Concepts - Modules](../core-concepts/modules.md)

---

Ready to explore specific middleware? Continue with [Access Log Middleware](accesslog.md).
