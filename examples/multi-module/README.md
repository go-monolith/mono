# Multi-Module Example

An e-commerce order flow built from four modules that talk only through the framework: **Inventory** (stock), **Payment** (charges, with a simulated 90% success rate), **Notification** (via a queue group service and an event) and **Order** (orchestrates the other three).
It shows module dependencies, request-reply and queue group services, events, typed service contracts, the Ports and Adapters layout with `ConsumeServices()` validated at startup, and the request ID and access log middleware.

## Running the Example

```bash
cd examples/multi-module
go run .

# Or, from the repository root (stops after 5 s):
make run-example-2
```

The program runs 4 scenarios, then waits for Ctrl+C:

1. Successful order (all steps complete)
2. Out of stock (inventory check fails)
3. Another order (may succeed or fail payment due to the 90% success rate)
4. Third order attempt

Every service call is logged to `access.log` in the working directory.

Run the tests:

```bash
go test ./examples/multi-module/...
```

Three kinds of test, from fastest to widest:

- **Domain** (`order/service_test.go`): the order rules with fake ports. The file imports only the `order` package and the standard library.
- **Conformance** (`*/module_test.go`): each module through `monotest.CheckModule`. The order test stands in for its three dependencies with `monotest.NewContainer` and the providers' contracts, then exercises `PlaceOrder`; a module that registered on a dependency's container, bypassed a port with a raw request, called a service missing from `ConsumeServices()`, or never used a declared dependency or contract would fail it.
- **Import rule** (`imports_test.go`): `monotest.CheckImports` fails when a contract package imports more than it may, or a module imports another module's package instead of its `contract`.

## Code Structure

Every module follows the hexagonal layout from [`docs/architect/hexagonal-architecture.md`](https://github.com/go-monolith/mono/blob/main/docs/architect/hexagonal-architecture.md): a `contract` sub-package is the only thing other modules import.

```
examples/multi-module/
├── main.go                    # Application orchestration (calls order through its contract)
├── imports_test.go            # monotest.CheckImports: the contract import rule
├── README.md                  # This file
├── inventory/
│   ├── contract/contract.go  # Port: CheckStock contract + DTOs
│   └── module.go             # Domain logic, RegisterServices via contract
├── payment/
│   ├── contract/contract.go  # Port: Process contract + DTOs
│   └── module.go             # Domain logic
├── notification/
│   ├── contract/contract.go  # Port: OnOrderCreated queue group contract + DTO
│   ├── module.go             # Queue group handler + typed event consumer
│   └── types.go              # Module-internal types (NotificationLog)
└── order/
    ├── contract/
    │   ├── contract.go       # Port: PlaceOrder contract + DTOs
    │   └── events.go         # OrderCreatedEvent + OrderCreatedV1 (consumed by notification)
    ├── ports.go              # Driven ports the order domain needs, in its own terms (InventoryPort, ...)
    ├── service.go            # Domain: order placement rules, plain Go over the ports
    ├── adapter.go            # Adapters translating the ports to the providers' contracts
    ├── module.go             # mono.Module: wiring, driving adapter, Dependencies(), ConsumeServices()
    ├── service_test.go       # Domain tests with fake ports (no framework, no NATS)
    └── module_test.go        # monotest.CheckModule conformance test
```

Inventory, payment and notification are small enough to keep their logic in the handler. The order module has rules and dependencies, so it shows the full layout: `service.go` and `ports.go` import only the standard library.

## Key Patterns

### Dependencies and Communication

```
Inventory (no deps) ─┐
Payment (no deps)   ─┼─→ Order (depends on all three)
Notification (no deps)─┘
```

The framework starts modules in dependency order: Inventory, Payment and Notification first, then Order once all three are running.

| From → To | Kind | Service / event | Subject |
|---|---|---|---|
| Order → Inventory | request-reply (Order waits for the reply) | `check-stock` | `services.inventory.check-stock` |
| Order → Payment | request-reply (Order waits for the reply) | `process` | `services.payment.process` |
| Order → Notification | queue group `notification-workers` (fire-and-forget) | `on-order-created` | `services.notification.on-order-created` |
| Order → all subscribers | event (broadcast) | `order/contract.OrderCreatedV1` | `events.order.v1.order-created` |

Notification imports `order/contract.OrderCreatedV1` and registers a typed consumer, so it receives every order creation event.

### Typed Service Contracts

A service is declared once in the provider's `contract` package and used on both sides:

```go
// inventory/contract/contract.go — declared once, used by both sides
var CheckStock = helper.RequestReplyService[CheckStockRequest, CheckStockResponse]("inventory", "check-stock")

// inventory/module.go — provider side: the driving adapter binds the typed domain handler
func (m *InventoryModule) RegisterServices(container mono.ServiceContainer) error {
    return contract.CheckStock.Register(container, m.checkStock)
}

// order/adapter.go — consumer side (the container received in SetDependencyServiceContainer)
resp, err := inventorycontract.CheckStock.Call(ctx, a.services, req)
```

Service name, subject, kind and serialization are written once; a consumer that imports a renamed contract fails to compile, and a consumer whose declared contract is not provided fails at startup.

The other providers follow the same pattern:

```go
// payment: simulates payment processing with transaction ID generation
var Process = helper.RequestReplyService[ProcessPaymentRequest, ProcessPaymentResponse]("payment", "process")

func (m *PaymentModule) RegisterServices(container mono.ServiceContainer) error {
    return contract.Process.Register(container, m.process)
}

// notification: a queue group service plus a typed event consumer
var OnOrderCreated = helper.QueueGroupService[OrderCreatedNotification]("notification", "on-order-created")

func (m *NotificationModule) RegisterServices(container mono.ServiceContainer) error {
    return contract.OnOrderCreated.Register(container, types.TypedQGHP[contract.OrderCreatedNotification]{
        QueueGroup: "notification-workers",
        Handler:    m.sendNotification,
    })
}

func (m *NotificationModule) RegisterEventConsumers(registry mono.EventRegistry) error {
    return helper.RegisterTypedEventConsumer(registry, ordercontract.OrderCreatedV1, m.handleOrderCreatedEvent, m)
}
```

### Order Module (Ports and Adapters)

The order module places an order in four steps:

1. Checking inventory (request-reply, synchronous)
2. Processing payment (request-reply, synchronous)
3. Sending a notification (queue group, fire-and-forget: only the acceptance ACK is awaited)
4. Publishing the `OrderCreatedV1` event (broadcast)

Its domain talks to ports in its own terms; adapters translate them to the providers' contracts:

```go
// order/ports.go — what the domain needs, in the order module's own terms
type InventoryPort interface {
    Available(ctx context.Context, productID string, quantity int) (bool, error)
}

// order/adapter.go — the port implemented over the provider's contract,
// translating between the order module's terms and the provider's DTOs
type inventoryAdapter struct{ services mono.ServiceLookup }

func (a inventoryAdapter) Available(ctx context.Context, productID string, quantity int) (bool, error) {
    resp, err := inventorycontract.CheckStock.Call(ctx, a.services,
        inventorycontract.CheckStockRequest{ProductID: productID, Quantity: quantity})
    if err != nil {
        return false, fmt.Errorf("inventory check failed: %w", err)
    }
    return resp.Available, nil
}
```

The domain (`order/service.go`, plain Go, no framework, no other module) handles each outcome; a failed notification does not fail the order (graceful degradation):

```go
// Step 1: request-reply through the inventory port
available, err := s.inventory.Available(ctx, order.ProductID, order.Quantity)
// ...
if !available {
    return Result{Status: StatusOutOfStock}, nil // stop order placement
}

// Step 2: request-reply through the payment port
if !approved {
    return Result{Status: StatusPaymentFailed}, nil
}

// Step 3: queue group send (adapter: notificationcontract.OnOrderCreated.Send(...)), best effort
if err := s.notification.OrderPlaced(ctx, placed); err != nil {
    fmt.Printf("  → Warning: failed to send notification for order %s: %v\n", orderID, err)
}
```

Events are declared in the emitter's contract and published through an events port:

```go
// order/contract/events.go: the emitter declares the event
var OrderCreatedV1 = helper.EventDefinition[OrderCreatedEvent]("order", "OrderCreated", "v1")

// Order publishes (typed, best-effort): the domain calls its events port,
// the adapter publishes on the bus
contract.OrderCreatedV1.Publish(a.bus, event, nil)
```

The module itself wires it together:

```go
// order/module.go
func (m *OrderModule) Dependencies() []string {
    return []string{"inventory", "payment", "notification"}
}

// Validated by the framework at startup: each contract must be provided by a
// declared dependency with the matching service type. The list is complete:
// a call to a dependency service that is not in it is a boundary violation.
func (m *OrderModule) ConsumeServices() []types.BaseServiceDefinition {
    return []types.BaseServiceDefinition{
        inventorycontract.CheckStock.ToBase(),
        paymentcontract.Process.ToBase(),
        notificationcontract.OnOrderCreated.ToBase(),
    }
}

func (m *OrderModule) SetDependencyServiceContainer(dependency string, container mono.ServiceContainer) {
    switch dependency {
    case "inventory":
        m.inventory = inventoryAdapter{services: container}
    // ...
    }
}

// The containers and the event bus are set before Start: build the domain.
func (m *OrderModule) Start(_ context.Context) error {
    service, err := NewService(m.inventory, m.payment, m.notification, m.events)
    m.service = service
    return err
}

// The driving adapter: contract DTO in, domain call, contract DTO out.
func (m *OrderModule) placeOrder(ctx context.Context, request contract.CreateOrderRequest, _ *mono.Msg) (contract.CreateOrderResponse, error) {
    result, err := m.service.PlaceOrder(ctx, NewOrder{ProductID: request.ProductID, /* ... */})
    // ...
}
```

### Request ID and Access Log Middleware

```go
accessFile, _ := os.OpenFile("access.log",
    os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
accessModule, _ := accesslog.New(
    accesslog.WithOutput(accessFile),
    accesslog.WithFormat(accesslog.FormatJSON),
)
requestIDModule, _ := requestid.New()

// Register middleware in order (see "Middleware order" below):
app.Register(accessModule)    // 1. accesslog
app.Register(requestIDModule) // 2. requestid (outermost)
app.Register(inventoryModule) // 3. business modules, logged with request IDs
app.Register(paymentModule)
// ...
```

**Request ID middleware**: if an incoming message has an `X-Request-ID` header it is used, otherwise a new UUID is generated. The request ID is put into the handler's context, and outgoing service calls made by the handler carry it in the `X-Request-ID` header, so logs can be correlated across services.

**Access log middleware**: wraps all service handlers, with no instrumentation in the business modules, and logs timing, request and response sizes, success/error status, module and service names, and the request ID. Output is text or JSON, and the logged fields can be selected.

#### Middleware order

Middleware registered later wraps handlers outermost (see [Middleware: Ordering](../../docs/official/middleware/README.md#ordering)), so register accesslog **before** requestid. When a handler runs:

1. Request ID middleware extracts/generates the request ID and puts it into the handler context (outgoing calls carry it in the X-Request-ID header)
2. Access log middleware logs the `X-Request-ID` header value, or falls back to `requestid.GetRequestID(ctx)` when the incoming message had no header

## Expected Output

Framework log lines are interleaved and omitted here; the order of the module list printed from `app.Modules()` varies from run to run, and order and transaction IDs are timestamps.

```
=== Mono-Framework Multi-Module Example ===
Demonstrates: Module dependencies, RequestReply, QueueGroup, EventEmitterModule, EventRegistryAwareModule

✓ App created successfully
✓ Access log middleware registered (output: access.log)
✓ Request ID middleware registered
✓ Business modules registered: [payment notification order accesslog requestid inventory]
  → Inventory module started with 4 products in stock
  → Payment module started
  → Notification: registered consumer for order.OrderCreated (events.order.v1.order-created)
  → Notification module started
  → Order module started (depends on: inventory, payment, notification)
✓ App started (request ID tracking and access logging active)

App Health: healthy=true, nats_healthy=true

Running example scenarios...
(All service calls are being logged to access.log)

[Scenario 1] Successful Order
  → Placing order for product 'laptop' (qty: 1, $999.99 USD)
  → Inventory: check_stock(laptop, qty=1) → available=true, stock=10
  → Payment: process(order=order_xxx, amount=999.99 USD) → SUCCESS, txn=txn_xxx
  → Notification [service]: Order order_xxx created for product laptop (amount: $999.99)
  ✓ Order created successfully: order_xxx

  → Notification [event]: Order order_xxx created for product laptop (amount: $999.99)
[Scenario 2] Out of Stock
  → Placing order for product 'rare-item' (qty: 1, $1999.99 USD)
  → Inventory: check_stock(rare-item, qty=1) → available=false, stock=0
  ✗ Order failed: out of stock

[Scenario 3] Another Order
  → Placing order for product 'mouse' (qty: 2, $29.99 USD)
  ...
[Scenario 4] Third Order
  → Placing order for product 'keyboard' (qty: 1, $79.99 USD)
  ...

Press Ctrl+C to shutdown...
Tip: Check 'access.log' to see all service calls logged in JSON format with X-Request-ID
```

### Access Log Output

`access.log` holds one JSON object per service call. Example entries:

```json
{"duration_ms":0,"module":"inventory","request_id":"6e770331-fb0b-42c3-b5d1-fc717bdf3321","request_size":36,"response_size":29,"service":"check-stock","service_type":"request_reply","status":"success","ts":"2026-11-17T12:57:34Z"}
{"duration_ms":50,"module":"payment","request_id":"6e770331-fb0b-42c3-b5d1-fc717bdf3321","request_size":64,"response_size":93,"service":"process","service_type":"request_reply","status":"success","ts":"2026-11-17T12:57:34Z"}
{"duration_ms":0,"module":"notification","request_id":"6e770331-fb0b-42c3-b5d1-fc717bdf3321","request_size":119,"response_size":0,"service":"on-order-created","service_type":"queue_group","status":"success","ts":"2026-11-17T12:57:34Z"}
```

Keys appear in alphabetical order. Each log entry includes:
- **ts**: Timestamp in RFC3339 format
- **request_id**: Unique request ID (UUID) for distributed tracing
- **module**: Module that registered the service
- **service**: Service name
- **service_type**: Service type (request_reply, queue_group, stream_consumer, event_consumer, event_stream_consumer, channel)
- **status**: success or error
- **duration_ms**: Handler execution time in milliseconds
- **request_size**: Request data size in bytes
- **response_size**: Response data size in bytes (0 for queue_group)

Analyze the log with `jq`:

```bash
# Find slow requests (>10ms)
cat access.log | jq 'select(.duration_ms > 10)'

# Count requests by module
cat access.log | jq -r '.module' | sort | uniq -c

# Find failed requests
cat access.log | jq 'select(.status == "error")'

# Trace all service calls by request ID (distributed tracing)
cat access.log | jq 'select(.request_id == "a1b2c3d4-e5f6-7890-abcd-ef1234567890")'
```

## Next Steps

Ideas for experimenting with the example:

1. **Add Health Checks**: Implement `HealthCheckableModule` for each module
2. **Use JetStream**: Replace event subscription with durable consumers for reliable event processing
3. **Add More Modules**: Implement shipping, refund, or customer modules
4. **Add Retry Logic**: Implement payment retry with exponential backoff
5. **Customize Access Logs**: Try text format, custom fields, or request ID headers

See also:

- [Basic Example](../basic/README.md) - Simple single-module example
- [Analytics Example](../analytics/README.md) - Channel-based communication patterns
- [Design Document](https://github.com/go-monolith/mono/blob/main/docs/spec/foundation.md) - Complete framework design
- [Hexagonal Architecture Guide](https://github.com/go-monolith/mono/blob/main/docs/architect/hexagonal-architecture.md) - Contracts, ports and adapters
- [Module Interface Documentation](https://github.com/go-monolith/mono/blob/main/pkg/types/module.go) - All module interfaces
