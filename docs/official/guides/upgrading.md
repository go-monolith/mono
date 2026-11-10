# Upgrading to v0.1.0

This page lists what changes when you upgrade to **v0.1.0**, including the breaking changes that shipped in v0.0.15 to v0.0.19 without a minor version bump.

## Who Is Affected

| You have | Affected by |
|----------|-------------|
| Only modules that use the framework's own container and event bus | Behaviour changes only. No code change is required |
| A type that wraps or implements `ServiceContainer` (decorators, test fakes) | `RegisterRequestReplyService` signature |
| A type that implements `EventRegistry` (test fakes) | `RegisterEventConsumerWithOptions` |
| A type that implements `MonoApplication` without embedding it | `ModuleBoundaries()` |

## Compile-Time Changes

### `RegisterRequestReplyService` takes options (since v0.0.15)

```go
// Before
RegisterRequestReplyService(name string, handler RequestReplyHandler) error

// After
RegisterRequestReplyService(name string, handler RequestReplyHandler, opts ...ServiceOption) error
```

Callers are unaffected. A type that overrides or implements the method must accept the options, and a wrapper must forward them, or per-service worker pool options are silently dropped:

```go
func (c myContainer) RegisterRequestReplyService(name string, handler mono.RequestReplyHandler, opts ...mono.ServiceOption) error {
    return c.ServiceContainer.RegisterRequestReplyService(name, wrap(handler), opts...)
}
```

### `EventRegistry` gains `RegisterEventConsumerWithOptions` (since v0.0.15)

```go
RegisterEventConsumerWithOptions(def BaseEventDefinition, handler EventConsumerHandler, module Module, opts EventConsumerOptions) error
```

Add the method to hand-written registry fakes.

### `MonoApplication` gains `ModuleBoundaries()` (v0.1.0)

```go
ModuleBoundaries() []ModuleBoundaryReport
```

A type that embeds `mono.MonoApplication` still compiles. A full implementation must add the method.

{% hint style="info" %}
**Prefer `monotest` to hand-written fakes.** `monotest.NewContainer` is the framework's real container, so it follows interface changes for you. See the [Testing Guide](testing.md#testing-without-nats-monotest).
{% endhint %}

## Behaviour Changes

### Request-reply handlers run on a worker pool (since v0.0.15)

Request-reply handlers of one service now run concurrently, up to `runtime.GOMAXPROCS(0)*4` at once. Queue group services and event consumers stay serial unless their registration sets `MaxConcurrency`. Use `mono.WithDefaultServiceConcurrency(1)` to restore fully serial handling. See [Handler Concurrency](../api/container.md#handler-concurrency-worker-pools).

### Request-reply handlers receive the caller's deadline (since v0.0.17)

The handler context now ends at the caller's deadline and is cancelled when the handler returns. A handler that starts background work must derive from `context.WithoutCancel(ctx)`.

### Handler panics are recovered (since v0.0.16)

A panic in a handler no longer crashes the process. It is logged with its stack, and a request-reply caller receives `ErrHandlerPanic`.

### Dependency containers and event buses are wrapped (v0.1.0)

- The container passed to `SetDependencyServiceContainer` is a read-only view of the provider's container.
- The bus passed to `SetEventBus` is scoped to the module. `app.EventBus(name)` returns that scoped bus for a started module.
- Identity comparisons with `app.Services(name)` or the shared bus no longer hold.
- `EventBusWithConn[*nats.Conn]` assertions keep working.

Calls that cross a boundary still succeed by default, but now log a warning and appear in `app.ModuleBoundaries()`:

- registering on, or unregistering from, a dependency's container;
- a raw publish or request to `services.<module>.<service>` of a module that is not a declared dependency;
- subscribing to another module's service subject.

### More log lines at shutdown (v0.1.0)

`Stop` logs one Info line per declared dependency that was never looked up, and one Warn line per boundary violation kind.

### Start errors keep their cause (v0.1.0)

`errors.Is(err, ErrServiceNotFound)` and similar checks (sentinels from `github.com/go-monolith/mono/pkg/errors`) now work on the error returned by `app.Start`. The message text is unchanged.

## Recommended Next Steps

Nothing below is required to upgrade. It is the recommended path for new and existing modules:

1. Move each module's DTOs and event definitions into a `contract` sub-package and declare typed service contracts.
2. Implement `ConsumeServices()` on modules that call other modules.
3. Add a `monotest.CheckModule` test per module.
4. Read `app.ModuleBoundaries()`, fix what it reports, then enable `mono.WithStrictModuleBoundaries()`.

See the [Module Boundaries Guide](module-boundaries.md).
