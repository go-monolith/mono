# Module Boundaries Guide

This guide shows the recommended way to connect modules since **v0.1.0**, with the checks added in **v0.2.0** (the import rule check, the complete `ConsumeServices()` list and `mono.ServiceLookup`). It makes the hexagonal (Ports and Adapters) pattern checkable: a typed port both sides share, startup validation of what a module consumes, a runtime report of how modules really use their boundaries, and a NATS-free conformance test per module.

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
3. In consumers, put driven ports in `ports.go` (in the module's own types), the domain in `service.go` and the adapters in `adapter.go`.
4. Implement `ConsumeServices()` on every module that calls another module, listing every contract it calls.
5. Add one `monotest.CheckModule` test per module, and one `monotest.CheckImports` test for the application.
6. Run the app in the default mode and read `app.ModuleBoundaries()` (or the `Stop` log).
7. Fix what it reports, then turn on `mono.WithStrictModuleBoundaries()`, and keep it on in tests.

## Module Layout

```
mymodule/
├── contract/
│   └── contract.go   # Published contract: DTOs + typed service/event definitions
├── ports.go          # Driven ports this module needs (interfaces it owns, in its own types)
├── service.go        # Domain logic: plain Go over ports.go
├── adapter.go        # Driven adapters: ports implemented over other modules' contracts
├── module.go         # mono.Module: wiring + driving adapter (RegisterServices)
├── service_test.go   # Domain tests with fake ports
└── module_test.go    # monotest.CheckModule conformance test
```

| File | Role in the pattern | Who imports it |
|------|---------------------|----------------|
| `contract/` | Published contract | Any module, `main.go`, tests |
| `ports.go` | Driven ports, in the module's own types | The module and its tests |
| `service.go` | Domain. Imports only the standard library | The module only |
| `adapter.go` | Driven adapters. Translate between the module's types and the providers' DTOs | The module only |
| `module.go` | Wiring + driving adapter | `main.go` only |

The direction of the dependencies is the point: adapters and `module.go` know the domain, and the domain knows only its ports. A module with little logic can leave `service.go` out and keep the logic in its handler.

**Contract import rule:** a `contract` package imports only the standard library, `github.com/go-monolith/mono/pkg/types`, `.../pkg/helper` and other modules' contract packages. Never its own module package, never another module's non-contract package. Consumers import `<provider>/contract` and nothing else of the provider. Events live in the **emitter's** contract package, so no shared `events` package is needed and no import cycle can arise. `monotest.CheckImports` checks the rule (see [Checking Imports](#checking-imports)).

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
    // typed handler: no serialization. Holds the logic in a small module,
    // delegates to the domain service in a larger one.
}
```

**Consumer (port + domain + driven adapter):**

```go
// order/ports.go: owned by the consumer, in its own types
type InventoryPort interface {
    Available(ctx context.Context, productID string, quantity int) (bool, error)
}

// order/service.go: the domain, plain Go over the ports
type Service struct{ inventory InventoryPort /* ... */ }

func (s *Service) PlaceOrder(ctx context.Context, order NewOrder) (Result, error) {
    available, err := s.inventory.Available(ctx, order.ProductID, order.Quantity)
    // ...
}

// order/adapter.go: translates the port to the provider's contract
type inventoryAdapter struct{ services mono.ServiceLookup }

func (a inventoryAdapter) Available(ctx context.Context, productID string, quantity int) (bool, error) {
    resp, err := inventorycontract.CheckStock.Call(ctx, a.services,
        inventorycontract.CheckStockRequest{ProductID: productID, Quantity: quantity})
    if err != nil {
        return false, fmt.Errorf("inventory check failed: %w", err)
    }
    return resp.Available, nil
}

// order/module.go: wiring
func (m *Module) SetDependencyServiceContainer(dep string, container mono.ServiceContainer) {
    if dep == "inventory" {
        m.inventory = inventoryAdapter{services: container}
    }
}

func (m *Module) Start(context.Context) error {
    service, err := NewService(m.inventory /* ... */)
    m.service = service
    return err
}
```

- **Ports use the module's own types.** When the provider changes its DTO, only the adapter changes. A port may use the provider's DTOs directly when the two models are the same; the adapter is then a pass-through, and a DTO change reaches the domain.
- **Adapters hold a `mono.ServiceLookup` (v0.2.0+).** It is the read side of a `ServiceContainer` (`Get*` and `Has`). Registering on a dependency is then a compile error. `Call`, `Send` and `Publish` accept it, and any `ServiceContainer` is one.
- **Events go behind a port too.** The domain calls `events.OrderPlaced(...)`; an adapter over the event bus calls `contract.OrderCreatedV1.Publish(...)`.
- **Build the domain in `Start`.** The framework has set every dependency container and the event bus by then.

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

`ConsumeServices()` is called once per start, after the dependency containers are set, so a module may build the list from what it was wired with.

**The list is complete (v0.2.0+).** A module that implements `ConsumeServices()` and uses a request-reply, queue group or stream consumer service of a dependency that is not in the list commits an `undeclared-service-use` violation (see below). Both ways of reaching a service count:

- a lookup on the dependency's container (`Get*`, which every `Call` / `Send` / `Publish` does);
- a publish or request on the module's event bus to `services.<dependency>.<service>`.

Not checked: modules that do not implement the interface, channel services (they have no contract type), and plugin services.

Asking for a service the dependency does not provide is not a violation in the default mode: the lookup fails with `ErrServiceNotFound`, so a module can probe for an optional service and fall back. In strict mode the lookup of anything outside the list fails with `ErrModuleBoundaryViolation`.

## Runtime Boundaries

What a module receives is wrapped:

- `SetDependencyServiceContainer` gets a **read-only view** of the provider's container. Every lookup is attributed to the consumer. `Entries()` on the view returns descriptions of the services: copies without the provider's handlers and channels (nil, in both modes). Changing a description does not change the provider. Reading them is not a violation; the first call per dependency is logged once at Warn, because code written against the real entries would find nil handlers.
- `SetEventBus` gets a **module-scoped bus**. `events.*` and custom subjects pass untouched. Service subjects are checked: see the table.

| Violation kind | What the module did |
|----------------|---------------------|
| `register-on-dependency` | Called `Register*`, `Unregister`, `BindModule`, `Set*`, `StartChannelRouters` or `RegisterContract` on a dependency's container |
| `channel-consumer-mismatch` | Called `GetChannelService` with a `consumerModule` that is not its own name |
| `undeclared-service-subject` | Published to or requested `services.<module>.<service>` of a module that is not itself, a declared dependency or a plugin (bypassing the port) |
| `foreign-service-subscription` | Subscribed to another module's service subject |
| `undeclared-service-use` | Implements `ConsumeServices()` and looked up a dependency service that is not in the list, or published to or requested its subject on the event bus |

**Default mode:** the call goes through, is warned once, and is reported. The report keeps up to 1024 distinct violations per module; beyond that, new ones are counted together per kind in an entry with `*` as dependency and subject.

```go
for _, r := range app.ModuleBoundaries() { // nil unless the app is running
    // r.Module, r.Declared, r.Consumed
    // r.Observed            dependency services looked up, with counts
    // r.UnusedDependencies  declared but never looked up
    // r.UnusedContracts     in ConsumeServices() but not looked up so far
    // r.Violations          kind, dependency, subject, count, first seen
}
```

The lists are empty, never nil, when there is nothing to list, so a JSON export of the report always has `observed`, `unused_dependencies`, `unused_contracts` and `violations`.

`Stop` logs the same summary: one Warn per violation kind, one Info per declared dependency and per consumed contract that was never looked up. A dependency declared only for start ordering is legitimate, and so is a contract that was not called during this run.

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
5. No boundary violation was recorded. This includes a dependency service called without being in `ConsumeServices()`, through the container or over the event bus.
6. Every declared dependency was looked up at least once.
7. Emitted events have valid subjects, and every `events.*` subject the module published is in `EmitEvents()`.
8. `Start` and `Stop` succeed, and `Stop` returns within the stop timeout.
9. Every `ConsumeServices()` contract of a dependency was used at least once (looked up, or reached on the event bus). With rule 5 the list is exact for everything the module does through its container and bus: nothing used is undeclared, nothing declared is unused. JetStream publishes through `EventStream()` are not observed.

| Option | Use |
|--------|-----|
| `WithDependency(name, container)` | Stand-in for a dependency. All containers must share one `EventBus` |
| `WithExercise(fn)` | Drive the module after `Start`. Needed for rules 6 and 9, since adapters look services up at call time |
| `WithOrderingOnlyDependency(name)` | Exempt a start-ordering dependency from rule 6 |
| `WithUnexercisedContract(def)` | Exempt a contract the exercise cannot reach from rule 9 |
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
h.Report()                     // the module's boundary report (empty lists before Start)
h.ModulePublished()            // subjects the module itself published
h.EventBus()                   // publish consumed events here with def.Publish
```

Limits: no JetStream (stream consumer, event stream consumer, cron, `EventStream()`), no middleware chain, no worker pools. Services are dispatched only after `Start`, as in the framework, so a call to a dependency from `Start` fails with `ErrServiceUnavailable`.

## Checking Imports

Since v0.2.0. The runtime checks see what a module does through the container and the bus it is handed. A module that imports another module's package and calls it directly uses neither, so `monotest.CheckImports` reads the import graph instead (it runs `go list`):

```go
// One test for the whole application, in its root directory,
// so that "./..." covers every module.
func TestImportRules(t *testing.T) {
    monotest.CheckImports(t)
}
```

A test placed elsewhere, such as beside `cmd/app/main.go`, points the check at the root with `monotest.WithImportDir("../..")`. The check **fails** when the patterns match no package, or only `package main` and host packages, and when the application has no module at all (no `contract` package and no `WithModuleRoots`): a wrong directory cannot pass by accident. It also fails when `./...` matches packages but none of a module, as in a `cmd/app` directory with helper packages of its own. `WithImportPatterns` turns that last check off: naming the patterns says those packages are the ones to check.

| Rule | What it reports |
|------|-----------------|
| `contract-import` | A `contract` package that imports anything but the standard library, `pkg/types`, `pkg/helper` and other contract packages |
| `module-import` | A package outside a module that imports a package of that module other than its `contract` |

A module is a package with a `contract` sub-package, together with every package below it. Packages of one module import each other freely. `package main` is exempt: the host program imports module packages to construct and register them.

Every directory named `contract` counts. A package with that name that is not a module's contract (`legal/contract` for customer contracts, a helper `order/internal/contract`) would turn its parent into a module and be held to the contract rule. Exclude it with `WithNonContractPackages`, by import path.

| Option | Use |
|--------|-----|
| `WithHostPackages(paths...)` | A composition root that is not `package main` |
| `WithModuleRoots(paths...)` | A module without a `contract` sub-package |
| `WithNonContractPackages(paths...)` | A package named `contract` that is not a module's contract, such as `legal/contract` or `order/internal/contract`. Its parent is then not a module because of it |
| `WithAllowedContractImports(paths...)` | Extra imports for contract packages, such as a Protobuf runtime or a shared value type |
| `WithImportDir(dir)` | Run in another directory (default: the test's directory) |
| `WithImportPatterns(patterns...)` | Packages to check (default `./...`). The modules of the whole application are still known |

Limits:

- Imports of test files are not checked, so an integration test may wire real modules.
- Packages are listed for the default build context. Files behind build tags or for another operating system are not checked.
- In a Go workspace, every workspace module counts as part of the application.
- The `go` command must be on `PATH`, which it is under `go test`.

`monotest.ImportViolations(ctx, opts...)` returns the same findings without a test, for a command or a CI step.

## What Is Not Checked

- **Direct references.** A module handed another module's object in its constructor bypasses every runtime check. `CheckImports` catches the import that makes it possible, unless the object is passed as an interface.
- **Host escape hatches.** `app.Services(name)` and `app.EventBus(name)` are for `main.go`, HTTP handlers and tests, not for module code.
- **JetStream.** Publishes through `EventStream()` are not observed.
- **Raw NATS access.** `Conn()` on the event bus gives the NATS connection, which is not scoped.
- **Events at runtime.** Publishing an `events.*` subject that is missing from `EmitEvents()` is reported by `CheckModule` only.
- **Domain purity.** The framework cannot see whether domain code avoids infrastructure. Keep `service.go` and `ports.go` on standard-library imports.

## Migrating Existing Modules

1. Move request/response types and event definitions into `<module>/contract`, and add a typed definition per service.
2. Provider: replace `container.RegisterRequestReplyService("name", ...)` / `helper.RegisterTyped*Service(...)` with `contract.X.Register(container, handler)`.
3. Consumer: replace `helper.CallRequestReplyService(ctx, c, "name", json.Marshal, json.Unmarshal, &req, &resp)` with `resp, err := contract.X.Call(ctx, c, req)`.
4. Replace provider-owned adapters (`provider.NewXAdapter(container)`) with a consumer-owned port and adapter.
5. Replace raw `eventBus.Request("services.other.svc", ...)` with a declared dependency and a contract call.
6. Add `ConsumeServices()` with every contract the module calls, and the `CheckModule` test.
7. Add one `CheckImports` test in the root directory of the application.
8. Where a module has real rules, move them into `service.go` behind ports in its own types.

Interface changes that custom containers and test fakes must follow are listed in [Upgrading to v0.1.0 and v0.2.0](upgrading.md).

## Best Practices

✓ **Do**

- Declare contracts as package-level variables in `<module>/contract`
- Keep ports small, owned by the consumer and in its own types
- Keep the domain (`service.go`) on standard-library imports
- Hold a dependency in an adapter as `mono.ServiceLookup`
- Call `Register` / `Call` / `Send` / `Publish` on the contract
- Declare every consumed contract in `ConsumeServices()`
- Run `monotest.CheckImports` in one test for the application
- Use `WithOrderingOnlyDependency` for start-ordering dependencies in `CheckModule`
- Enable `WithStrictModuleBoundaries()` once the report is clean

✗ **Don't**

- Import another module's package other than its `contract`
- Write service names as string literals in module code
- Call `Register*` or `Unregister` on a dependency's container
- Request or subscribe to another module's `services.*` subject on the event bus
- Hold the application object inside a module to reach `app.Services(...)`
- Build another module's DTOs or publish on the event bus from domain code

## Related Documentation

- [Testing Guide](testing.md)
- [Upgrading to v0.1.0 and v0.2.0](upgrading.md)
- [API Reference - Modules](../api/module.md#serviceconsumermodule)
- [API Reference - Framework](../api/framework.md) (`ModuleBoundaries`, `WithStrictModuleBoundaries`)
- [API Reference - Service Container](../api/container.md#typed-service-contracts)
