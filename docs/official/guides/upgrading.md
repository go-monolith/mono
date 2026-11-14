# Upgrading to v0.1.0 and v0.2.0

This page lists what changes when you upgrade to **v0.1.0**, including the breaking changes that shipped in v0.0.15 to v0.0.19 without a minor version bump, and what changes in **v0.2.0** (see [Upgrading to v0.2.0](#upgrading-to-v020)).

## Who Is Affected

| You have | Affected by |
|----------|-------------|
| Only modules that use the framework's own container and event bus | Behaviour changes only. No code change is required |
| A type that wraps or implements `ServiceContainer` (decorators, test fakes) | `RegisterRequestReplyService` signature |
| A type that implements `EventRegistry` (test fakes) | `RegisterEventConsumerWithOptions` |
| A type that implements `MonoApplication` without embedding it | `SubscriptionStats()` (v0.0.18) and `ModuleBoundaries()` (v0.1.0) |
| A short `WithShutdownTimeout`, or modules with a slow `Stop` | "`WithShutdownTimeout` is enforced" below |
| Polyglot clients, or middleware that inspects message headers | "Request-reply handlers receive the caller's deadline" below |
| Request-reply services that receive bursts faster than they dispatch | "Request-reply client buffer is bounded" below |
| Alerts or log parsing on framework messages | "Handler panics are recovered" and "NATS client errors go to the framework logger" below |
| `WithStrictModuleBoundaries()` together with modules that implement `ConsumeServices()` | "`ConsumeServices()` is a complete list" (v0.2.0) |
| Module code that reads handlers or channels from a dependency's `Entries()` | "`Entries()` on a dependency container returns descriptions" (v0.2.0) |
| `monotest.CheckModule` tests | "`monotest.CheckModule` has a rule 9" (v0.2.0) |

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

### `MonoApplication` gains `SubscriptionStats()` (since v0.0.18)

```go
SubscriptionStats() []SubscriptionStats
```

### `MonoApplication` gains `ModuleBoundaries()` (v0.1.0)

```go
ModuleBoundaries() []ModuleBoundaryReport
```

A type that embeds `mono.MonoApplication` still compiles. A full implementation written against v0.0.14 must add **both** `SubscriptionStats()` and `ModuleBoundaries()`.

{% hint style="info" %}
**Prefer `monotest` to hand-written fakes.** `monotest.NewContainer` is the framework's real container, so it follows interface changes for you. See the [Testing Guide](testing.md#testing-without-nats-monotest).
{% endhint %}

## Behaviour Changes

### Request-reply handlers run on a worker pool (since v0.0.15)

Request-reply handlers of one service now run concurrently, up to `runtime.GOMAXPROCS(0)*4` at once. Queue group services and event consumers stay serial unless their registration sets `MaxConcurrency`. Use `mono.WithDefaultServiceConcurrency(1)` to restore fully serial handling. See [Handler Concurrency](../api/container.md#handler-concurrency-worker-pools).

### Handler panics are recovered (since v0.0.16)

A panic in a handler no longer crashes the process. It is logged with its stack, and a request-reply caller receives `ErrHandlerPanic`.

The error text of a recovered cron handler panic changed from `cron handler panicked: <value>` to `handler panicked: <value>`, the same text as every other handler. Update alerts or log queries that match the old text.

### Request-reply handlers receive the caller's deadline (since v0.0.17)

- **New headers.** Request-reply calls, and queue group sends in ACK mode, carry two headers: `Mono-Deadline` (absolute deadline, Unix milliseconds) and `Mono-Timeout` (time remaining, milliseconds). Polyglot consumers and header-inspecting middleware see them. Removing both opts a request out of server-side expiry.
- **Expired requests are dropped.** A request whose deadline passed while it waited in the queue, by more than the clock-skew allowance (`WithDeadlineSkewAllowance`, default 250ms), is dropped: the handler does not run and no reply is sent. Queue group messages and events are never dropped.
- **Handler context.** The handler context now ends at the caller's deadline and is cancelled when the handler returns. It used to be the long-lived runtime context. A handler that starts background work must derive from `context.WithoutCancel(ctx)`.

### `WithShutdownTimeout` is enforced (since v0.0.17)

- `Stop` used to ignore `WithShutdownTimeout`. It now bounds `Stop` when the caller's context has no deadline. The default is 30s, so a `Stop(context.Background())` that used to wait indefinitely now gives up after 30s.
- The drain of subscriptions and in-flight handlers runs inside that bound and is capped to leave time for module `Stop`. With `WithShutdownTimeout(5*time.Second)`, in-flight handlers get about 3.75s.
- The new `WithShutdownDrainTimeout` must not exceed `WithShutdownTimeout`; `NewMonoApplication` returns an error otherwise.

Modules with a slow `Stop` and a short configured timeout are affected. See [WithShutdownTimeout](../api/framework.md#withshutdowntimeout) and [WithShutdownDrainTimeout](../api/framework.md#withshutdowndraintimeout).

### Request-reply client buffer is bounded (since v0.0.18)

The NATS client buffer of each request-reply subscription is now bounded at 10,000 messages and 16 MB, down from nats.go's defaults of about 500,000 messages and 64 MB. When the dispatcher falls that far behind, nats.go drops further messages as a slow consumer, and the caller sees a timeout. The framework logs the drop at Error level.

Raise or remove the bound per registration:

```go
container.RegisterRequestReplyService("check-stock", handler, mono.WithPendingLimits(100_000, 64<<20)) // -1 means unlimited
```

Queue group services and event consumers keep nats.go's defaults.

### NATS client errors go to the framework logger (since v0.0.18)

NATS client errors and connection events (disconnect, reconnect, closed) are logged through the framework logger instead of being printed to stderr. The framework's client connection now reports the name `mono` in `/connz` and server logs. Change it with [`WithNATSClientName`](../api/framework.md#withnatsclientname), or override the handlers with [`WithNATSClientOptions`](../api/framework.md#withnatsclientoptions).

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

## Upgrading to v0.2.0

v0.2.0 tightens the module boundary checks introduced in v0.1.0 and adds an import rule check. One change applies to every application: `Entries()` on a dependency container returns descriptions (below). Applications whose modules do not read a dependency's entries and that do not use `ConsumeServices()`, strict mode or `monotest.CheckModule` are not affected.

### `ConsumeServices()` is a complete list

A module that implements `ConsumeServices()` and uses a request-reply, queue group or stream consumer service of a dependency that is **not** in the list now commits an `undeclared-service-use` boundary violation. A use is a lookup on the dependency's container, or a publish or request on the module's event bus to the service subject `services.<dependency>.<service>`.

| Mode | Effect |
|------|--------|
| Default | The call goes through. One warning is logged, and the use appears in `app.ModuleBoundaries()` |
| `WithStrictModuleBoundaries()` | The lookup, publish or request fails with `ErrModuleBoundaryViolation` |

Lookups happen when a module makes the call, so in strict mode an incomplete list fails on the first call at runtime, not at startup. The exception is a service the module looks up in `SetDependencyServiceContainer`: in strict mode one that is not in the list fails the start of the module. Before enabling or keeping strict mode, run in the default mode, read the report, and add every contract the module calls to `ConsumeServices()`. Modules that do not implement the interface, channel services and plugin services are not checked.

A lookup of a service the dependency does not provide:

| Mode | Effect |
|------|--------|
| Default | `ErrServiceNotFound`, as before. It is not a violation, so a module may ask for an optional service and fall back |
| `WithStrictModuleBoundaries()` | `ErrModuleBoundaryViolation` instead of `ErrServiceNotFound`: the module asked for something it did not declare |

`ConsumeServices()` is called once per start, after the module's dependency containers are set, and that one list is both validated and enforced. A module may build it from what it was wired with.

### `Entries()` on a dependency container returns descriptions

`Entries()` on the container passed to `SetDependencyServiceContainer` returns copies that describe the dependency's services (name, type, subject, queue groups, options, stream and cron configuration, contract). The handlers and channels are left out (nil), in the default mode as well as in strict mode. The copies are deep, so changing one, including the subjects or metadata of its stream configuration, has no effect on the provider. `app.Services(name).Entries()` in the host program is unchanged.

A module that read `RequestHandler`, `QueueHandlers[i].Handler`, `InChannel` or `OutChannel` from a dependency's entry now finds nil: calling the handler panics and a send on the channel blocks. Reach the service through `GetRequestReplyService`, `GetQueueGroupService`, `GetStreamConsumerService` or `GetChannelService` instead. Reading the entries is not a boundary violation, so it is not in the boundary report. The framework logs one warning for the first `Entries()` call of a module on each dependency:

```text
Entries() of a dependency container returns descriptions without handlers and channels; reach its services through Get* (the host reads the real entries with MonoFramework.Services) module=order dependency=inventory
```

### `monotest.CheckModule` has a rule 9

A `ConsumeServices()` contract that is never used during `Start` or the exercise (looked up on the dependency's container, or reached on the event bus) fails the conformance test. Call it from `WithExercise`, remove it from the list, or exempt it with `monotest.WithUnexercisedContract(def)`.

### `ServiceLookup`

`mono.ServiceLookup` is the read side of `ServiceContainer` (`Get*` and `Has`), and `ServiceContainer` embeds it. The method set of `ServiceContainer` is unchanged, so implementations and fakes still compile. `helper.CallRequestReplyService`, `SendQueueGroupService`, `PublishStreamConsumerService` and the typed contracts' `Call` / `Send` / `Publish` accept a `ServiceLookup`; callers that pass a `ServiceContainer` are unaffected.

### More in the boundary report

`ModuleBoundaryReport` gains `UnusedContracts`: the consumed contracts not used so far. `Stop` logs one Info line for each. Like `Observed`, `UnusedDependencies` and `Violations` it is an empty list when there is nothing to list, so `unused_contracts` is always present in the JSON form.

### New: `monotest.CheckImports`

`monotest.CheckImports(t)` checks the contract package import rule on the import graph of the application. Nothing changes until you add the test.

Two things to know when you add it:

- Put the test in the root directory of the application. The check fails, instead of passing with nothing checked, when it finds no package to check, no module, or no package of a module.
- Every directory named `contract` marks its parent as a module. If you have a package with that name that is something else, such as `legal/contract` for customer contracts or a helper `order/internal/contract`, name it with `monotest.WithNonContractPackages`.

See [Checking Imports](module-boundaries.md#checking-imports).

## Recommended Next Steps

Nothing below is required to upgrade. It is the recommended path for new and existing modules:

1. Move each module's DTOs and event definitions into a `contract` sub-package and declare typed service contracts.
2. Implement `ConsumeServices()` on modules that call other modules.
3. Add a `monotest.CheckModule` test per module, and one `monotest.CheckImports` test for the application (v0.2.0+).
4. Read `app.ModuleBoundaries()`, fix what it reports, then enable `mono.WithStrictModuleBoundaries()`.

See the [Module Boundaries Guide](module-boundaries.md).
