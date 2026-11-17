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

- **Package**: `github.com/go-monolith/mono/middleware/accesslog`
- **Purpose**: Log request/response details including timing, sizes, and status
- **Features**:
  - Text and JSON output formats
  - Request ID tracking integration
  - Configurable output destination
  - Performance metrics

**Quick Start**:
```go
accessFile, _ := os.OpenFile("access.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
accessModule, err := accesslog.New(
    accesslog.WithOutput(accessFile), // closed by the module on Stop
    accesslog.WithFormat(accesslog.FormatJSON),
)
if err != nil {
    return err
}

app.Register(accessModule)
```

Learn more: [Access Log Middleware](accesslog.md)

### 2. Audit Middleware

Tamper-evident audit logging with cryptographic hash chaining.

- **Package**: `github.com/go-monolith/mono/middleware/audit`
- **Purpose**: Log module start/stop and service and event consumer registrations, optionally hash-chained
- **Features**:
  - Optional hash chaining for tamper detection
  - User context field filled by a function you supply

**Quick Start**:
```go
auditFile, _ := os.OpenFile("audit.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
auditModule, err := audit.New(
    audit.WithOutput(auditFile),
    audit.WithHashChaining(""),
)
if err != nil {
    return err
}

app.Register(auditModule)
```

Learn more: [Audit Middleware](audit.md)

### 3. Request ID Middleware

Automatic request ID generation and propagation for distributed tracing.

- **Package**: `github.com/go-monolith/mono/middleware/requestid`
- **Purpose**: Track requests across module boundaries for debugging and tracing
- **Features**:
  - Automatic UUID generation
  - Request ID extraction from headers
  - Context injection for handlers
  - Automatic propagation to outgoing request-reply calls, queue group sends and stream consumer publishes (not to events or channel services)

**Quick Start**:
```go
requestIDMiddleware, err := requestid.New()
if err != nil {
    return err
}
app.Register(requestIDMiddleware)
```

Learn more: [Request ID Middleware](requestid.md)

## Middleware Lifecycle

Middleware modules can be registered in any position relative to modules: the framework collects them at `Start`, starts them after plugins and before modules, and stops them after modules. Only the order *among* middleware matters (see below). The framework calls only `Start`, `Stop` and the hooks on a middleware module; it does not call `Dependencies`, `SetDependencyServiceContainer`, `SetEventBus`, `RegisterServices` or `RegisterEventConsumers`.

```
1. app.Start
   └─ Plugins start, then middleware start (in registration order)

2. Module Startup
   └─ Hooks fire for each module's service and event consumer registrations,
      and when each module has started

3. Service Handler Execution
   └─ Wrapped handlers run; outgoing service-client messages pass OnOutgoingMessage

4. Application Shutdown
   └─ Modules stop (a hook fires for each), then middleware stop (reverse order)
```

The hooks fire for module started/stopped events, service and event consumer registrations, and outgoing service-client messages. `OnConfigurationChange` is currently never called.

## Using Multiple Middleware

You can register multiple middleware modules together:

```go
app, _ := mono.NewMonoApplication()

// Register middleware: accesslog before requestid (see the note below)
accessModule, err := accesslog.New(
    accesslog.WithOutput(accessFile),
    accesslog.WithFormat(accesslog.FormatText),
)
if err != nil {
    return err
}
app.Register(accessModule)

requestIDModule, err := requestid.New()
if err != nil {
    return err
}
app.Register(requestIDModule)

auditModule, err := audit.New(audit.WithOutput(auditFile))
if err != nil {
    return err
}
app.Register(auditModule)

// Register application modules
app.Register(&OrderModule{})
app.Register(&PaymentModule{})

// Start
app.Start(context.Background())
```

Middleware are processed in registration order: each one wraps the handler produced by the previous one, so the **last** registered middleware runs **first** when a message arrives. Register `accesslog` **before** `requestid`: `requestid` must run before `accesslog` for a generated ID to reach the access log. Queue group and stream consumer entries only show an ID that arrived in the `X-Request-ID` header.

## Middleware vs Modules

| Aspect | Middleware | Module |
|--------|-----------|--------|
| **Purpose** | Cross-cutting concerns | Business logic |
| **Lifecycle** | Started after plugins and before modules, stopped after modules | Started in dependency order |
| **Interface** | MiddlewareModule | Module |
| **Lifecycle Hooks** | Framework events | Module lifecycle |
| **Dependencies** | None (only `Start`/`Stop` and the hooks are called) | Declares dependencies on other modules |
| **Examples** | Logging, tracing, security | Order, Payment, Notification |

## Common Patterns

### Conditional Middleware

Enable middleware based on configuration:

```go
app, _ := mono.NewMonoApplication(mono.WithLogLevel(mono.LogLevelDebug))

if config.EnableAudit {
    auditModule, err := audit.New(audit.WithOutput(auditFile))
    if err != nil {
        return err
    }
    app.Register(auditModule)
}

if config.EnableAccessLog {
    accessModule, err := accesslog.New(accesslog.WithOutput(accessFile))
    if err != nil {
        return err
    }
    app.Register(accessModule)
}
```

### Middleware Chain

Multiple middleware working together:

With `accesslog` registered first and `requestid` second, a request runs through:

```
Request
  ├→ RequestID Middleware (registered last, runs first: extracts or generates ID)
  ├→ AccessLog Middleware (logs request, sees the ID in the context)
  └→ Handler (processes request)
```

The audit middleware is not on the request path: it only records registrations and module lifecycle events.

### Custom Middleware

Create your own middleware by implementing `MiddlewareModule`:

```go
type MyMiddleware struct{}

var _ mono.MiddlewareModule = (*MyMiddleware)(nil)

func (m *MyMiddleware) Name() string                    { return "my-middleware" }
func (m *MyMiddleware) Start(ctx context.Context) error { return nil }
func (m *MyMiddleware) Stop(ctx context.Context) error  { return nil }

// Each hook receives a value and returns the (possibly modified) value
// for the next middleware in the chain.
func (m *MyMiddleware) OnModuleLifecycle(ctx context.Context, e mono.ModuleLifecycleEvent) mono.ModuleLifecycleEvent {
    return e
}

func (m *MyMiddleware) OnServiceRegistration(ctx context.Context, r mono.ServiceRegistration) mono.ServiceRegistration {
    return r // wrap r.RequestHandler etc. here
}

// Currently never called by the framework.
func (m *MyMiddleware) OnConfigurationChange(ctx context.Context, e mono.ConfigurationEvent) mono.ConfigurationEvent {
    return e
}

func (m *MyMiddleware) OnOutgoingMessage(o mono.OutgoingMessageContext) mono.OutgoingMessageContext {
    return o
}

func (m *MyMiddleware) OnEventConsumerRegistration(ctx context.Context, e mono.EventConsumerEntry) mono.EventConsumerEntry {
    return e
}

func (m *MyMiddleware) OnEventStreamConsumerRegistration(ctx context.Context, e mono.EventStreamConsumerEntry) mono.EventStreamConsumerEntry {
    return e
}
```

## Performance Considerations

Middleware adds processing overhead. Consider:

- **Request ID**: one context value per handler call
- **Access Log**: one unbuffered write per entry under a mutex; wrap the writer in a `bufio.Writer` if needed
- **Audit**: cost per registration or lifecycle event, not per request

For high-performance applications, be selective about which middleware you enable.

## Best Practices

✓ **Do**
- Keep middleware focused on single concerns
- Use middleware for cross-cutting concerns
- Configure output files with appropriate permissions (especially audit)

✗ **Don't**
- Put business logic in middleware
- Have middleware depend on specific modules
- Use middleware for request transformation (use handlers instead)
- Log sensitive data; the framework does not redact log output

## Related Documentation

- [Access Log Middleware](accesslog.md)
- [Audit Middleware](audit.md)
- [Request ID Middleware](requestid.md)
- [Core Concepts - Modules](../core-concepts/modules.md)

---

Ready to explore specific middleware? Continue with [Access Log Middleware](accesslog.md).
