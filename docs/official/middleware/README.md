# 🧬 Middleware

Middleware modules add cross-cutting behaviour (logging, auditing, request tracking) without changing your module code. They intercept service and event consumer registrations, module lifecycle events and outgoing service-client messages.

Read this page to choose a built-in middleware, to learn the **ordering rule**, or to write your own middleware.

## What is Middleware?

A middleware module implements `mono.MiddlewareModule`: the `Module` methods (`Name`, `Start`, `Stop`) plus six hooks. Each hook receives a value and returns the (possibly modified) value for the next middleware in the chain. Middleware use the hooks to:

- Wrap service and event consumer handlers
- Observe module started/stopped events
- Modify outgoing service-client messages (for example, add headers)

## Built-in Middleware

| Middleware | Package | What it does | Page |
|------------|---------|--------------|------|
| Access log | `github.com/go-monolith/mono/middleware/accesslog` | One entry per handler call with timing, sizes, status and request ID; text or JSON | [Access Log](accesslog.md) |
| Audit | `github.com/go-monolith/mono/middleware/audit` | Records module start/stop and service and event consumer registrations, optionally hash-chained; does not record service calls | [Audit](audit.md) |
| Request ID | `github.com/go-monolith/mono/middleware/requestid` | Extracts or generates a request ID, puts it in the handler context, and propagates it to outgoing request-reply calls, queue group sends and stream consumer publishes (not to events or channel services) | [Request ID](requestid.md) |

Registering all three:

```go
app, _ := mono.NewMonoApplication()

// accesslog before requestid (see Ordering below)
accessFile, _ := os.OpenFile("access.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
accessModule, err := accesslog.New(
    accesslog.WithOutput(accessFile), // closed by the module on Stop
    accesslog.WithFormat(accesslog.FormatJSON),
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

auditFile, _ := os.OpenFile("audit.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
auditModule, err := audit.New(
    audit.WithOutput(auditFile),
    audit.WithHashChaining(""),
)
if err != nil {
    return err
}
app.Register(auditModule)

// Application modules (any position)
app.Register(&OrderModule{})
app.Register(&PaymentModule{})

app.Start(context.Background())
```

## Ordering

**Relative to modules, position does not matter.** The framework collects middleware at `Start`, starts them after plugins and before modules, and stops them after modules.

**Among middleware, order matters.** Each middleware wraps the handler produced by the previous one, so the **last** registered middleware runs **first** when a message arrives.

For the built-in middleware this means: register `accesslog` **before** `requestid`. `requestid` must run before `accesslog` for a generated ID to reach the access log. Queue group and stream consumer entries only show an ID that arrived in the `X-Request-ID` header.

With `accesslog` registered first and `requestid` second, a request runs through:

```
Request
  ├→ RequestID Middleware (registered last, runs first: extracts or generates ID)
  ├→ AccessLog Middleware (logs request, sees the ID in the context)
  └→ Handler (processes request)
```

The audit middleware is not on the request path: it only records registrations and module lifecycle events.

## Middleware Lifecycle

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

- The framework calls `Start`, `Stop`, the hooks and, when the middleware implements `ServiceProviderModule`, `RegisterServices` on a bound container of its own (before `Start`; the middleware's own registrations are not passed through the chain). A module that lists the middleware in `Dependencies()` receives that container in `SetDependencyServiceContainer`, as it would a module's. The framework does not call `SetDependencyServiceContainer`, `SetEventBus` or `RegisterEventConsumers` on a middleware.
- `OnConfigurationChange` is deprecated and never called: the framework's configuration does not change at runtime. Implement it as a pass-through.
- **A failed `Start` of the application** stops the middleware that had started, and the application may be started again with the same middleware. A middleware whose `Stop` gives up something it cannot get back (the built-in audit and access log middleware close their log file) can implement the optional `types.RollbackAwareMiddleware` (since v0.2.18): the framework then calls `RollbackStart(ctx)` instead of `Stop` for that rollback, and `Stop` only when the application stops.

```go
// RollbackStart stops the middleware after a failed application Start and
// leaves it able to be started again.
func (m *MyMiddleware) RollbackStart(ctx context.Context) error {
    return m.flush() // keep the output open; Stop closes it
}
```

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

### Custom Middleware

Implement all of `mono.MiddlewareModule`:

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

// Deprecated hook, never called by the framework: pass the event through.
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

- **Request ID**: one context value per handler call
- **Access Log**: one unbuffered write per entry under a mutex; wrap the writer in a `bufio.Writer` if needed
- **Audit**: cost per registration or lifecycle event, not per request

For high-performance applications, be selective about which middleware you enable.

## Best Practices

✓ **Do**
- Keep middleware focused on a single cross-cutting concern
- Open output files with appropriate permissions (especially audit)

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
