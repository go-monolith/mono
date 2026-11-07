# Multi-Module Example

This example demonstrates advanced usage of the Monolith-Framework with multiple interdependent modules communicating through various messaging patterns.

## Overview

This example implements an e-commerce order processing system with four modules:

- **Inventory Module**: Manages product stock availability
- **Payment Module**: Processes payments for orders
- **Notification Module**: Sends notifications about orders (via service calls and events)
- **Order Module**: Orchestrates order placement by coordinating with other modules

## What You'll Learn

1. **Module Dependencies**: How to declare and manage dependencies between modules
2. **RequestReply Services**: Synchronous request-response communication patterns
3. **QueueGroup Services**: Asynchronous fire-and-forget messaging with load balancing
4. **Event Publishing**: Broadcasting events to multiple subscribers
5. **Event Subscription**: Listening to events from other modules
6. **Typed Service Contracts**: Declaring a service once in a `contract` package and using it on both sides
7. **Ports and Adapters**: Consumer-owned ports, adapters over contracts, and `ConsumeServices()` validated at startup
8. **Error Handling**: Managing failures gracefully across module boundaries
9. **Request ID Middleware**: Automatic request ID generation and propagation for distributed tracing
10. **Access Logging Middleware**: Automatic logging of all service calls with timing, status, and request IDs

## Architecture

### Module Dependency Graph

```
Inventory (no deps) ─┐
Payment (no deps)   ─┼─→ Order (depends on all three)
Notification (no deps)─┘
```

### Communication Patterns

1. **Order → Inventory** (RequestReply)
   - Service: `check-stock`
   - Subject: `services.inventory.check-stock`
   - Synchronous: Order waits for inventory response

2. **Order → Payment** (RequestReply)
   - Service: `process`
   - Subject: `services.payment.process`
   - Synchronous: Order waits for payment confirmation

3. **Order → Notification** (QueueGroup)
   - Service: `on-order-created`
   - Subject: `services.notification.on-order-created`
   - Queue group: `notification-workers`
   - Asynchronous: Fire-and-forget

4. **Order → EventBus** (Publish)
   - Event: `order/contract.OrderCreatedV1`, subject `events.order.v1.order-created`
   - Broadcast: All subscribers receive the event

5. **Notification ← EventBus** (Subscribe)
   - Imports `order/contract.OrderCreatedV1` and registers a typed consumer
   - Receives all order creation events

## Code Structure

Every module follows the hexagonal layout from [`docs/architect/hexagonal-architecture.md`](../../docs/architect/hexagonal-architecture.md): a `contract` sub-package is the only thing other modules import.

```
examples/multi-module/
├── main.go                    # Application orchestration (calls order through its contract)
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
    ├── ports.go              # Driven ports the order domain needs (InventoryPort, ...)
    ├── adapter.go            # Adapters implementing the ports over the providers' contracts
    └── module.go             # Orchestration, Dependencies(), ConsumeServices()
```

## Module Implementations

### Inventory Module

```go
// inventory/contract/contract.go — declared once, used by both sides
var CheckStock = helper.RequestReplyService[CheckStockRequest, CheckStockResponse]("inventory", "check-stock")

// inventory/module.go — driving adapter binds the typed domain handler
func (m *InventoryModule) RegisterServices(container mono.ServiceContainer) error {
    return contract.CheckStock.Register(container, m.checkStock)
}
```

Maintains in-memory inventory and responds to stock availability queries.

### Payment Module

```go
var Process = helper.RequestReplyService[ProcessPaymentRequest, ProcessPaymentResponse]("payment", "process")

func (m *PaymentModule) RegisterServices(container mono.ServiceContainer) error {
    return contract.Process.Register(container, m.process)
}
```

Simulates payment processing with 90% success rate and transaction ID generation.

### Notification Module

```go
// notification/contract/contract.go
var OnOrderCreated = helper.QueueGroupService[OrderCreatedNotification]("notification", "on-order-created")

// notification/module.go — queue group service plus a typed event consumer
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

Receives notifications through both service calls (QueueGroup) and events (typed consumer).

### Order Module

```go
// order/ports.go — what the domain needs, in the providers' contract DTOs
type InventoryPort interface {
    CheckStock(ctx context.Context, req inventorycontract.CheckStockRequest) (inventorycontract.CheckStockResponse, error)
}

// order/adapter.go — the port implemented over the provider's contract
func (a inventoryAdapter) CheckStock(ctx context.Context, req inventorycontract.CheckStockRequest) (inventorycontract.CheckStockResponse, error) {
    return inventorycontract.CheckStock.Call(ctx, a.container, req)
}

// order/module.go
func (m *OrderModule) Dependencies() []string {
    return []string{"inventory", "payment", "notification"}
}

// Validated by the framework at startup: each contract must be provided by a
// declared dependency with the matching service type.
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
        m.inventory = inventoryAdapter{container: container}
    // ...
    }
}
```

Orchestrates order placement by:
1. Checking inventory (RequestReply, synchronous)
2. Processing payment (RequestReply, synchronous)
3. Sending notification (QueueGroup, fire-and-forget)
4. Publishing event (EventBus, broadcast)

## Tests

Each module has a conformance test built on `monotest.CheckModule` (`inventory/module_test.go`, `payment/module_test.go`, `notification/module_test.go`, `order/module_test.go`). The order test stands in for its three dependencies with `monotest.NewContainer` and the providers' contracts, then exercises `PlaceOrder`; a module that registered on a dependency's container, bypassed a port with a raw request, or never used a declared dependency would fail it.

```bash
go test ./examples/multi-module/...
```

## Running the Example

```bash
# Navigate to the example directory
cd examples/multi-module

# Run the example
go run .

# Expected output shows 4 scenarios:
# 1. Successful order (all steps complete)
# 2. Out of stock (inventory check fails)
# 3. Another order (may succeed or fail payment due to 90% success rate)
# 4. Third order attempt
```

### Expected Output

```
=== Mono-Framework Multi-Module Example ===
Demonstrates: Module dependencies, RequestReply, QueueGroup, Events, Request ID Tracking, Access Logging

✓ App created successfully
✓ Request ID middleware registered
✓ Access log middleware registered (output: access.log)
✓ Business modules registered: [requestid accesslog inventory payment notification order]
✓ App started (request ID tracking and access logging active)

App Health: healthy=true, nats_healthy=true

Running example scenarios...
(All service calls are being logged to access.log)

[Scenario 1] Successful Order
  → Placing order for product 'laptop' (qty: 1, $999.99 USD)
  → Inventory: check_stock(laptop, qty=1) → available=true, stock=10
  → Payment: process(order=order_xxx, amount=999.99 USD) → SUCCESS, txn=txn_xxx
  ✓ Order created successfully: order_xxx
  → Notification [service]: Order order_xxx created for product laptop (amount: $999.99)
  → Notification [event]: Order order_xxx created for product laptop (amount: $999.99)

[Scenario 2] Out of Stock
  → Placing order for product 'rare-item' (qty: 1, $1999.99 USD)
  → Inventory: check_stock(rare-item, qty=1) → available=false, stock=0
  ✗ Order failed: out of stock

Press Ctrl+C to shutdown...
Tip: Check 'access.log' to see all service calls logged in JSON format with X-Request-ID
```

### Access Log Output

All service calls are automatically logged to `access.log` in JSON format. The request ID middleware generates unique IDs (UUIDs) for each request and propagates them across service calls. Example entries:

```json
{"ts":"2024-01-15T10:30:00Z","request_id":"a1b2c3d4-e5f6-7890-abcd-ef1234567890","module":"inventory","service":"check-stock","method":"request_reply","status":"success","duration_ms":2,"request_size":45,"response_size":62}
{"ts":"2024-01-15T10:30:00Z","request_id":"a1b2c3d4-e5f6-7890-abcd-ef1234567890","module":"payment","service":"process","method":"request_reply","status":"success","duration_ms":5,"request_size":98,"response_size":85}
{"ts":"2024-01-15T10:30:00Z","request_id":"a1b2c3d4-e5f6-7890-abcd-ef1234567890","module":"notification","service":"on-order-created","method":"queue_group","status":"success","duration_ms":1,"request_size":150,"response_size":0}
```

Each log entry includes:
- **ts**: Timestamp in RFC3339 format
- **request_id**: Unique request ID (UUID) for distributed tracing
- **module**: Module that registered the service
- **service**: Service name
- **method**: Service type (request_reply, queue_group, stream_consumer)
- **status**: success or error
- **duration_ms**: Handler execution time in milliseconds
- **request_size**: Request data size in bytes
- **response_size**: Response data size in bytes (0 for queue_group)

You can analyze the access logs with tools like `jq`:

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

## Key Concepts Demonstrated

### 1. Request ID Middleware

```go
// Create request ID middleware (must be registered BEFORE accesslog)
requestIDModule, _ := requestid.New()

// Register request ID middleware first
app.Register(requestIDModule)
```

The request ID middleware provides automatic request tracking across distributed service calls:

- **Extracts X-Request-ID**: If an incoming message has an `X-Request-ID` header, it's extracted and used
- **Generates UUID**: If no request ID is present, a new UUID is generated
- **Context Injection**: The request ID is injected into the handler's context for use in logging
- **Automatic Propagation**: When a handler makes outgoing service calls, the request ID is automatically added to outgoing message headers

**Why register before accesslog:**
The request ID middleware wraps handlers *before* accesslog, so when handlers execute:
1. Request ID middleware extracts/generates the request ID and injects it into message headers
2. Access log middleware extracts the request ID from headers for logging

### 2. Access Logging Middleware

```go
// Create access log middleware (after requestid)
accessFile, _ := os.OpenFile("access.log",
    os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
accessModule, _ := accesslog.New(
    accesslog.WithOutput(accessFile),
    accesslog.WithFormat(accesslog.FormatJSON),
)

// Register middleware in order:
// 1. requestid (extracts/generates request IDs)
// 2. accesslog (logs requests with request IDs)
// 3. business modules
app.Register(requestIDModule)
app.Register(accessModule)
app.Register(inventoryModule)  // Will be logged with request IDs
app.Register(paymentModule)    // Will be logged with request IDs
// ...
```

The access log middleware automatically wraps all service handlers to capture:
- Request timing (start time, duration)
- Request and response sizes
- Success/error status
- Module and service names
- Request ID (from X-Request-ID header)

**Key features:**
- Zero-code logging: No instrumentation needed in business modules
- Configurable output: Text or JSON format
- Field selection: Choose which fields to log
- Thread-safe: Safe for concurrent requests
- Minimal overhead: ~1-2μs per request
- Distributed tracing: Request IDs allow correlating logs across services

### 3. Module Dependencies

```go
// Order module declares its dependencies
func (m *Module) Dependencies() []string {
    return []string{"inventory", "payment", "notification"}
}
```

The framework automatically starts modules in dependency order:
- Independent modules (Inventory, Payment, Notification) start first
- Dependent module (Order) starts after all dependencies are running

### 4. Typed Service Contracts

```go
// Declared once in the provider's contract package
var CheckStock = helper.RequestReplyService[CheckStockRequest, CheckStockResponse]("inventory", "check-stock")

// Provider side
contract.CheckStock.Register(container, m.checkStock)

// Consumer side (the container received in SetDependencyServiceContainer)
resp, err := inventorycontract.CheckStock.Call(ctx, container, req)
```

Service name, subject, kind and serialization are written once; a consumer that imports a renamed contract fails to compile, and a consumer whose declared contract is not provided fails at startup.

### 5. RequestReply Pattern (Synchronous)

```go
// Domain logic talks to the port; the adapter calls the contract and waits for the reply
stockResult, err := m.inventory.CheckStock(ctx, inventorycontract.CheckStockRequest{ProductID: id, Quantity: qty})
if !stockResult.Available {
    return failureResponse  // Stop order placement
}
```

### 6. QueueGroup Pattern (Asynchronous)

```go
// Send notification and continue (acceptance ACK only)
_ = m.notification.OrderCreated(ctx, notif) // adapter: notificationcontract.OnOrderCreated.Send(...)
```

### 7. Event Publishing and Subscription

```go
// order/contract/events.go: the emitter declares the event
var OrderCreatedV1 = helper.EventDefinition[OrderCreatedEvent]("order", "OrderCreated", "v1")

// Order publishes (typed, best-effort)
contract.OrderCreatedV1.Publish(m.eventBus, event, nil)

// Notification consumes (typed)
helper.RegisterTypedEventConsumer(registry, ordercontract.OrderCreatedV1, m.handleOrderCreatedEvent, m)
```

### 8. Graceful Error Handling

```go
// Check inventory first
if !available {
    return CreateOrderResponse{
        Status: "failed_out_of_stock",
    }
}

// Process payment
if !paymentSuccess {
    return CreateOrderResponse{
        Status: "payment_failed",
    }
}

// Notification failure doesn't fail the order (graceful degradation)
if err := m.notification.OrderCreated(ctx, notif); err != nil {
    fmt.Printf("warning: %v\n", err) // Best effort
}
```

## Extending the Example

Ideas for learning and experimentation:

1. **Add Health Checks**: Implement `HealthCheckableModule` for each module
2. **Add Metrics**: Track order success/failure rates
3. **Implement Inventory Reservation**: Prevent overselling with stock reservations
4. **Add Retry Logic**: Implement payment retry with exponential backoff
5. **Use JetStream**: Replace event subscription with durable consumers for reliable event processing
6. **Add More Modules**: Implement shipping, refund, or customer modules
7. **Load Testing**: Run multiple order workers to test concurrent order processing
8. **Add Timeouts**: Implement context timeouts for each service call
9. **Customize Access Logs**: Try text format, custom fields, or request ID headers
10. **Analyze Performance**: Use access logs to identify slow services and bottlenecks

## See Also

- [Basic Example](../basic/) - Simple single-module example
- [Channel Services Example](../channel-services/) - Channel-based communication patterns (if implemented)
- [Design Document](../../docs/spec/foundation.md) - Complete framework design
- [Hexagonal Architecture Guide](../../docs/architect/hexagonal-architecture.md) - Contracts, ports and adapters
- [Module Interface Documentation](../../pkg/types/module.go) - All module interfaces
