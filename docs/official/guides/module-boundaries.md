# Module Boundaries Guide

This guide shows the recommended way to connect modules since **v0.1.0**. It makes the hexagonal (Ports and Adapters) pattern checkable: a typed port both sides share, startup validation of what a module consumes, a runtime report of how modules really use their boundaries, and a NATS-free conformance test per module.

**Time to complete:** 25 minutes

**What you'll learn:**
- Publishing a module's port as a `contract` package
- Registering and calling services through typed contracts
- Declaring consumed services so startup fails fast
- Reading the boundary report and turning on strict mode
- Testing a module without NATS using `monotest`

**Prerequisites:**
- [Modules](../core-concepts/modules.md) and [Services](../core-concepts/services.md)
- [Inter-Module Communication](../core-concepts/inter-module-communication.md)

**Working code:** the [multi-module example](../../../examples/multi-module/README.md) follows this guide, with a conformance test per module.

---

## The Recommended Path

1. Give every module that exposes services or events a `contract` sub-package.
2. Register and call through contracts; never write a service name as a string in module code.
3. In consumers, put driven ports in `ports.go` and their adapters in `adapter.go`.
4. Implement `ConsumeServices()` on every module that calls another module.
5. Add one `monotest.CheckModule` test per module.
6. Run the app in the default mode and read `app.ModuleBoundaries()` (or the `Stop` log).
7. Fix what it reports, then turn on `mono.WithStrictModuleBoundaries()`, and keep it on in tests.

## Module Layout

```
mymodule/
├── contract/
│   └── contract.go   # Published port: DTOs + typed service/event definitions
├── ports.go          # Driven ports this module needs (interfaces it owns)
├── adapter.go        # Driven adapters: ports implemented over other modules' contracts
├── module.go         # Domain logic + driving adapter (RegisterServices)
└── module_test.go    # monotest.CheckModule conformance test
```

| File | Role in the pattern | Who imports it |
|------|---------------------|----------------|
| `contract/` | Published port | Any module, `main.go`, tests |
| `ports.go` | Driven ports, in the providers' contract DTOs | The module and its tests |
| `adapter.go` | Driven adapters | The module only |
| `module.go` | Domain + driving adapter | `main.go` only |

**Contract import rule:** a `contract` package imports only the standard library, `github.com/go-monolith/mono/pkg/types` and `.../pkg/helper`. Never its own module package, never another module's non-contract package. Consumers import `<provider>/contract` and nothing else of the provider. Events live in the **emitter's** contract package, so no shared `events` package is needed and no import cycle can arise.

## Typed Service Contracts

```go
// inventory/contract/contract.go
package contract

import "github.com/go-monolith/mono/pkg/helper"

type CheckStockRequest struct {
    ProductID string `json:"product_id"`
    Quantity  int    `json:"quantity"`
}

type CheckStockResponse struct {
    Available bool `json:"available"`
    Stock     int  `json:"stock"`
}

var CheckStock = helper.RequestReplyService[CheckStockRequest, CheckStockResponse]("inventory", "check-stock")
```

| Constructor | Provider side | Consumer side |
|-------------|---------------|---------------|
| `helper.RequestReplyService[Req, Resp](module, name)` | `def.Register(container, handler, opts...)` | `def.Call(ctx, container, req) (Resp, error)` |
| `helper.QueueGroupService[T](module, name)` | `def.Register(container, pairs...)` with `types.TypedQGHP[T]` | `def.Send(ctx, container, payload) error` |
| `helper.StreamConsumerService[T](module, name)` | `def.Register(container, config, handler)` | `def.Publish(ctx, container, payload) (MsgPubAck, error)` |

- JSON by default; `WithMarshaler` / `WithUnmarshaler` return a copy with another codec.
- `def.ToBase()` returns the non-generic `types.BaseServiceDefinition`.
- Constructors panic on a non-kebab-case module or service name, like `helper.EventDefinition`. Declare contracts as package-level variables so this shows at process start.
- `Call` propagates the caller's context deadline and wraps errors with the contract identity (`contract inventory/check-stock (request_reply): ...`).
- Channel services have no contract type.

**Provider (driving adapter):**

```go
// inventory/module.go
func (m *Module) RegisterServices(container mono.ServiceContainer) error {
    return contract.CheckStock.Register(container, m.checkStock)
}

func (m *Module) checkStock(ctx context.Context, req contract.CheckStockRequest, _ *mono.Msg) (contract.CheckStockResponse, error) {
    // typed, pure domain logic
}
```

**Consumer (port + driven adapter):**

```go
// order/ports.go: owned by the consumer
type InventoryPort interface {
    CheckStock(ctx context.Context, req inventorycontract.CheckStockRequest) (inventorycontract.CheckStockResponse, error)
}

// order/adapter.go
type inventoryAdapter struct{ container mono.ServiceContainer }

func (a inventoryAdapter) CheckStock(ctx context.Context, req inventorycontract.CheckStockRequest) (inventorycontract.CheckStockResponse, error) {
    return inventorycontract.CheckStock.Call(ctx, a.container, req)
}

// order/module.go
func (m *Module) SetDependencyServiceContainer(dep string, container mono.ServiceContainer) {
    if dep == "inventory" {
        m.inventory = inventoryAdapter{container: container}
    }
}
```

## ServiceConsumerModule: Startup Validation

```go
import "github.com/go-monolith/mono/pkg/types" // no mono.* alias yet

var _ types.ServiceConsumerModule = (*Module)(nil)

func (m *Module) ConsumeServices() []types.BaseServiceDefinition {
    return []types.BaseServiceDefinition{
        inventorycontract.CheckStock.ToBase(),
        paymentcontract.Process.ToBase(),
    }
}
```

At module start, after dependency containers are set and before `RegisterServices`, the framework checks each contract:

- its module is in `Dependencies()`, or is a registered plugin;
- that provider registered the service;
- the registered service kind matches.

A failure stops startup with `ErrModuleStartFailed` wrapping `ErrMissingDependency`, `ErrServiceNotFound` or `ErrServiceContractMismatch` (all inspectable with `errors.Is`), and already-started modules are rolled back.

## Runtime Boundaries

What a module receives is wrapped:

- `SetDependencyServiceContainer` gets a **read-only view** of the provider's container. Every lookup is attributed to the consumer.
- `SetEventBus` gets a **module-scoped bus**. `events.*` and custom subjects pass untouched.

| Violation kind | What the module did |
|----------------|---------------------|
| `register-on-dependency` | Called `Register*`, `Unregister`, `BindModule`, `Set*`, `StartChannelRouters` or `RegisterContract` on a dependency's container |
| `channel-consumer-mismatch` | Called `GetChannelService` with a `consumerModule` that is not its own name |
| `undeclared-service-subject` | Published to or requested `services.<module>.<service>` of a module that is not itself, a declared dependency or a plugin (bypassing the port) |
| `foreign-service-subscription` | Subscribed to another module's service subject |

**Default mode:** the call goes through, is warned once, and is reported.

```go
for _, r := range app.ModuleBoundaries() { // nil unless the app is running
    // r.Module, r.Declared, r.Consumed
    // r.Observed            dependency services looked up, with counts
    // r.UnusedDependencies  declared but never looked up
    // r.Violations          kind, dependency, subject, count, first seen
}
```

`Stop` logs the same summary: one Warn per violation kind, one Info per declared dependency that was never looked up. A dependency declared only for start ordering is legitimate.

{% hint style="info" %}
**Existing applications keep working.** In the default mode nothing is rejected. The report shows where modules step outside their declared boundaries, so you can fix them before turning strict mode on.
{% endhint %}

**Strict mode:**

```go
app, err := mono.NewMonoApplication(mono.WithStrictModuleBoundaries())
```

Violations fail with `ErrModuleBoundaryViolation`. Void container methods become no-ops, and `MustGetChannelService` panics. Nothing is recorded in the report.

## Testing with monotest

`github.com/go-monolith/mono/monotest` runs a module without NATS, through the framework's lifecycle order, with the same container views and scoped bus as production.

**Conformance test (one per module):**

```go
func TestOrderModuleConformance(t *testing.T) {
    bus := monotest.NewEventBus()
    inventory := monotest.NewContainer("inventory", bus)
    // Register the dependency's service through its own contract...
    err := inventorycontract.CheckStock.Register(inventory,
        func(context.Context, inventorycontract.CheckStockRequest, *types.Msg) (inventorycontract.CheckStockResponse, error) {
            return inventorycontract.CheckStockResponse{Available: true, Stock: 5}, nil
        })
    if err != nil {
        t.Fatal(err)
    }
    // ...or with a JSON stub: monotest.StubRequestReply / monotest.StubQueueGroup.

    monotest.CheckModule(t, order.NewModule(),
        monotest.WithDependency("inventory", inventory),
        monotest.WithExercise(func(ctx context.Context, h *monotest.Harness) error {
            _, err := contract.PlaceOrder.Call(ctx, h.Container(), contract.PlaceOrderRequest{ProductID: "laptop", Quantity: 1})
            return err
        }))
}
```

`CheckModule` reports each broken rule as its own failure:

1. `Name()` is non-empty and kebab-case.
2. `Dependencies()` has no self reference or duplicate, and each has a stand-in (`WithDependency`).
3. A `ServiceProviderModule` registers at least one service with a valid subject.
4. Every `ConsumeServices()` contract is provided by a declared dependency's stand-in with the same kind.
5. No boundary violation was recorded.
6. Every declared dependency was looked up at least once.
7. Emitted events have valid subjects, and every `events.*` subject the module published is in `EmitEvents()`.
8. `Start` and `Stop` succeed, and `Stop` returns within the stop timeout.

| Option | Use |
|--------|-----|
| `WithDependency(name, container)` | Stand-in for a dependency. All containers must share one `EventBus` |
| `WithExercise(fn)` | Drive the module after `Start`. Needed for rule 6, since adapters look services up at call time |
| `WithOrderingOnlyDependency(name)` | Exempt a start-ordering dependency from rule 6 |
| `WithEmittedEvents(defs...)` | Events of other modules, for consumers that use `registry.GetEventByName` |
| `WithPlugin(alias, plugin)` | Inject a plugin. Give it a container first with `plugin.SetContainer(monotest.NewContainer(...))` |
| `WithStopTimeout(d)` | Bound `Stop` (default 5s) |
| `Strict()` | Reject violations, like `WithStrictModuleBoundaries`. Ignored by `CheckModule` |

**Driving a module directly:**

```go
h := monotest.New(t, order.NewModule(), monotest.WithDependency("inventory", inventory))
if err := h.Start(ctx); err != nil { t.Fatal(err) }
defer h.Stop(ctx)

resp := monotest.Call[contract.PlaceOrderRequest, contract.PlaceOrderResponse](t, h, "place-order", req)
inventory.Calls("check-stock") // handler runs on the stand-in
h.Report()                     // the module's boundary report
h.ModulePublished()            // subjects the module itself published
h.EventBus()                   // publish consumed events here with def.Publish
```

Limits: no JetStream (stream consumer, event stream consumer, cron, `EventStream()`), no middleware chain, no worker pools. Services are dispatched only after `Start`, as in the framework, so a call to a dependency from `Start` fails with `ErrServiceUnavailable`.

## What Is Not Checked

- **Imports.** Nothing stops a module importing another module's internal package. Keep the contract import rule by review.
- **Direct references.** A module handed another module's object in its constructor bypasses every check.
- **Host escape hatches.** `app.Services(name)` and `app.EventBus(name)` are for `main.go`, HTTP handlers and tests, not for module code.
- **JetStream.** Publishes through `EventStream()` are not observed.
- **Domain purity.** The framework cannot see whether domain code avoids infrastructure.

## Migrating Existing Modules

1. Move request/response types and event definitions into `<module>/contract`, and add a typed definition per service.
2. Provider: replace `container.RegisterRequestReplyService("name", ...)` / `helper.RegisterTyped*Service(...)` with `contract.X.Register(container, handler)`.
3. Consumer: replace `helper.CallRequestReplyService(ctx, c, "name", json.Marshal, json.Unmarshal, &req, &resp)` with `resp, err := contract.X.Call(ctx, c, req)`.
4. Replace provider-owned adapters (`provider.NewXAdapter(container)`) with a consumer-owned port and adapter.
5. Replace raw `eventBus.Request("services.other.svc", ...)` with a declared dependency and a contract call.
6. Add `ConsumeServices()` and the `CheckModule` test.

Interface changes that custom containers and test fakes must follow are listed in [Upgrading to v0.1.0](upgrading.md).

## Best Practices

✓ **Do**

- Declare contracts as package-level variables in `<module>/contract`
- Keep ports small and owned by the consumer
- Call `Register` / `Call` / `Send` / `Publish` on the contract
- Declare every consumed contract in `ConsumeServices()`
- Use `WithOrderingOnlyDependency` for start-ordering dependencies in `CheckModule`
- Enable `WithStrictModuleBoundaries()` once the report is clean

✗ **Don't**

- Import another module's package other than its `contract`
- Write service names as string literals in module code
- Call `Register*` or `Unregister` on a dependency's container
- Request or subscribe to another module's `services.*` subject on the event bus
- Hold the application object inside a module to reach `app.Services(...)`

## Related Documentation

- [Testing Guide](testing.md)
- [Upgrading to v0.1.0](upgrading.md)
- [API Reference - Modules](../api/module.md#serviceconsumermodule)
- [API Reference - Framework](../api/framework.md) (`ModuleBoundaries`, `WithStrictModuleBoundaries`)
- [API Reference - Service Container](../api/container.md#typed-service-contracts)
