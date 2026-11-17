# Services

Services are the **public APIs** of modules. A module that exposes functionality to other modules registers services; a module that uses them declares the provider as a dependency, so the framework can start the provider first. Read this page to understand how services connect modules; the five kinds of service are compared in [Inter-Module Communication](inter-module-communication.md), and every method is in the [ServiceContainer API](../api/container.md).

The recommended way to declare and call a service is a [typed service contract](#typed-service-contracts-recommended).

## What is a Service?

A service is a named endpoint registered by a module that other modules can invoke. Think of services as the contract between modules:

```
┌───────────────────────────────────────────────────────────────┐
│                  Module A (Service Provider)                  │
│                                                               │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │            ServiceContainer (Module A)                  │  │
│  │                                                         │  │
│  │  ┌───────────────────────────────────────────────────┐  │  │
│  │  │  Service: "process-payment"                       │  │  │
│  │  │  Type: Request-Reply                              │  │  │
│  │  │  Handler: handleProcessPayment()                  │  │  │
│  │  └───────────────────────────────────────────────────┘  │  │
│  │                                                         │  │
│  │  ┌───────────────────────────────────────────────────┐  │  │
│  │  │  Service: "validate-card"                         │  │  │
│  │  │  Type: Request-Reply                              │  │  │
│  │  │  Handler: handleValidateCard()                    │  │  │
│  │  └───────────────────────────────────────────────────┘  │  │
│  │                                                         │  │
│  └─────────────────────────────────────────────────────────┘  │
│                                                               │
└───────────────────────────────────────────────────────────────┘
                             │
                             │ Framework injects ServiceContainer
                             ▼
┌───────────────────────────────────────────────────────────────┐
│                  Module B (Service Consumer)                  │
│                                                               │
│  Dependencies() → ["payment"]                                 │
│                                                               │
│  SetDependencyServiceContainer("payment", containerA)         │
│                                                               │
│  m.paymentContainer.GetRequestReplyService("process-payment") │
│                                                               │
└───────────────────────────────────────────────────────────────┘
```

## Services Establish Module Dependencies

When Module B uses a service from Module A, this creates an **explicit dependency**:

| Relationship | Effect |
|-------------|--------|
| B uses A's service | B must declare A as a dependency |
| B declares dependency on A | Framework starts A before B |
| B receives A's ServiceContainer | B can call A's services |

This is different from **events**, which do not create dependencies (see [Services vs Events](#services-vs-events)). Services are for direct, point-to-point communication where the caller knows which module provides the functionality.

## Typed Service Contracts (Recommended)

Since v0.1.0, declare a service once in the provider's `contract` sub-package and use the same value on both sides. The service name, the types and the encoding are then written once:

```go
// payment/contract/contract.go
var Process = helper.RequestReplyService[PaymentRequest, PaymentResponse]("payment", "process-payment")

// Provider
func (m *PaymentModule) RegisterServices(container mono.ServiceContainer) error {
    return contract.Process.Register(container, m.process)
}

// Consumer
resp, err := paymentcontract.Process.Call(ctx, m.paymentContainer, paymentcontract.PaymentRequest{Amount: order.Total})
```

`helper.QueueGroupService[T]` and `helper.StreamConsumerService[T]` follow the same shape. See the [Module Boundaries Guide](../guides/module-boundaries.md).

## The ServiceContainer

Each module receives its own `ServiceContainer` for two purposes:

1. **Registering services** the module provides
2. **Accessing services** from dependency modules

```go
// Abridged; see the ServiceContainer API for every method.
type ServiceContainer interface {
    // Register services (provider side)
    RegisterChannelService(name string, in chan *Msg, out chan *Msg) error
    RegisterRequestReplyService(name string, handler RequestReplyHandler, opts ...ServiceOption) error
    RegisterQueueGroupService(name string, pairs ...QGHP) error
    RegisterStreamConsumerService(name string, config StreamConsumerConfig, handler StreamConsumerHandler) error
    RegisterCronService(name string, config CronServiceConfig, handler CronHandler) error

    // Get services (consumer side)
    GetChannelService(serviceName string, consumerModule string) (in chan *Msg, out chan *Msg, err error)
    MustGetChannelService(serviceName string, consumerModule string) (in chan *Msg, out chan *Msg)
    GetRequestReplyService(name string) (RequestReplyServiceClient, error)
    GetQueueGroupService(name string) (QueueGroupServiceClient, error)
    GetStreamConsumerService(name string) (StreamConsumerServiceClient, error)
    // (Cron services are server-driven and have no consumer-side client.)

    // Query
    Has(name string) bool
    Entries() []*ServiceEntry
    Unregister(name string) error
}
```

## Without Contracts: Raw Registration and Calls

The contract above wraps these lower-level calls.

### Registering Services (Provider Side)

A module provides services by implementing `ServiceProviderModule`:

```go
// PaymentModule provides payment services
type PaymentModule struct {
    logger mono.Logger
}

func (m *PaymentModule) Name() string { return "payment" }

// RegisterServices is called during module initialization
func (m *PaymentModule) RegisterServices(container mono.ServiceContainer) error {
    // Register a Request-Reply service
    err := container.RegisterRequestReplyService(
        "process-payment",  // Service name
        m.handleProcessPayment,
    )
    if err != nil {
        return err
    }

    // Register a Queue Group service
    return container.RegisterQueueGroupService(
        "send-receipt",
        mono.QGHP{
            QueueGroup: "receipt-workers",
            Handler:    m.handleSendReceipt,
        },
    )
}

func (m *PaymentModule) handleProcessPayment(ctx context.Context, req *mono.Msg) ([]byte, error) {
    // Process payment and return response
    var request PaymentRequest
    if err := json.Unmarshal(req.Data, &request); err != nil {
        return nil, err
    }

    response := PaymentResponse{
        TransactionID: generateID(),
        Status:        "approved",
    }
    return json.Marshal(response)
}

func (m *PaymentModule) handleSendReceipt(ctx context.Context, msg *mono.Msg) error {
    // Fire-and-forget: send receipt email
    return nil
}
```

### Consuming Services (Consumer Side)

A module consumes services in three steps:

1. **Declare dependencies** via `DependentModule`
2. **Receive ServiceContainers** via `SetDependencyServiceContainer`
3. **Call services** via the container's Get methods

```go
// OrderModule uses PaymentModule's services
type OrderModule struct {
    logger           mono.Logger
    paymentContainer mono.ServiceContainer
}

func (m *OrderModule) Name() string { return "order" }

// Step 1: Declare dependency on payment module
func (m *OrderModule) Dependencies() []string {
    return []string{"payment"}
}

// Step 2: Receive payment module's ServiceContainer
func (m *OrderModule) SetDependencyServiceContainer(dep string, container mono.ServiceContainer) {
    if dep == "payment" {
        m.paymentContainer = container
    }
}

// Step 3: Use the service
func (m *OrderModule) createOrder(ctx context.Context, order *Order) error {
    // Get the payment service client
    client, err := m.paymentContainer.GetRequestReplyService("process-payment")
    if err != nil {
        return fmt.Errorf("payment service not available: %w", err)
    }

    // Prepare request
    reqData, _ := json.Marshal(PaymentRequest{Amount: order.Total})

    // Call the service
    resp, err := client.Call(ctx, reqData)
    if err != nil {
        return fmt.Errorf("payment failed: %w", err)
    }

    // Parse response
    var paymentResp PaymentResponse
    if err := json.Unmarshal(resp.Data, &paymentResp); err != nil {
        return err
    }

    m.logger.Info("Payment processed", "txn", paymentResp.TransactionID)
    return nil
}
```

### Using Helper Functions

The string-based helpers do the marshaling for you, without a contract:

```go
import "github.com/go-monolith/mono/pkg/helper"

func (m *OrderModule) createOrder(ctx context.Context, order *Order) error {
    var response PaymentResponse

    err := helper.CallRequestReplyService(
        ctx,
        m.paymentContainer,
        "process-payment",
        json.Marshal,
        json.Unmarshal,
        &PaymentRequest{Amount: order.Total},
        &response,
    )
    if err != nil {
        return err
    }

    m.logger.Info("Payment processed", "txn", response.TransactionID)
    return nil
}
```

## Service Types

The framework supports five service types for different use cases:

| Service Type | Registration Method | Use Case |
|-------------|-------------------|----------|
| **Channel** | `RegisterChannelService` | In-process, high-throughput communication |
| **Request-Reply** | `RegisterRequestReplyService` | Synchronous calls with response |
| **Queue Group** | `RegisterQueueGroupService` | Async, load-balanced processing |
| **Stream Consumer** | `RegisterStreamConsumerService` | Durable, at-least-once delivery |
| **Cron** | `RegisterCronService` | Server-scheduled periodic work (single fire per occurrence) |

For detailed information about each service type, see [Inter-Module Communication](inter-module-communication.md).

## Dependency Resolution and Startup Order

The framework automatically handles startup order based on declared dependencies:

```
┌───────────────────────────────────────────────────────────────┐
│                    Dependency Resolution                      │
├───────────────────────────────────────────────────────────────┤
│                                                               │
│  Modules Registered:                                          │
│    - OrderModule     → Dependencies: ["payment", "inventory"] │
│    - PaymentModule   → Dependencies: []                       │
│    - InventoryModule → Dependencies: []                       │
│    - ShippingModule  → Dependencies: ["order"]                │
│                                                               │
│  Computed Startup Order:                                      │
│    1. PaymentModule    (no dependencies)                      │
│    2. InventoryModule  (no dependencies)                      │
│    3. OrderModule      (after payment, inventory)             │
│    4. ShippingModule   (after order)                          │
│                                                               │
│  Shutdown Order:                                              │
│    4 → 3 → 2 → 1      (reverse of startup)                    │
│                                                               │
└───────────────────────────────────────────────────────────────┘
```

### Circular Dependency Detection

The framework detects and rejects circular dependencies at startup:

```go
// This will fail at startup:
// OrderModule depends on PaymentModule
// PaymentModule depends on OrderModule (circular!)

// Error: failed to resolve dependencies: ... circular dependency detected (chain: order -> payment -> order)
// errors.Is(err, monoerrors.ErrCircularDependency) reports true
// (monoerrors = "github.com/go-monolith/mono/pkg/errors")
```

## Subject Naming Convention

Services are automatically assigned NATS subjects following this pattern:

```
services.<module>.<service>
```

Examples:
- `services.payment.process-payment`
- `services.payment.send-receipt`
- `services.inventory.check-stock`

This naming is handled automatically by the framework - you only specify the service name when registering.

## Best Practices

### Do

- **Keep services focused**: Each service should do one thing well
- **Use meaningful names**: `process-payment` is clearer than `handle`
- **Declare all dependencies**: If you use a service, declare the dependency
- **Handle errors explicitly**: Return meaningful errors from handlers
- **Use typed contracts**: Register and call through a contract in `<module>/contract`
- **Use appropriate service types**: Choose based on your needs (sync/async, durability)

### Don't

- **Don't call modules directly**: Always use services for inter-module calls
- **Don't create circular dependencies**: Refactor to break the cycle
- **Don't share mutable state**: Use services for communication, not shared objects
- **Don't mutate a dependency's container**: Only look services up on it
- **Don't ignore the ServiceContainer**: It's your gateway to other modules

## Services vs Events

| Aspect | Services | Events |
|--------|----------|--------|
| **Coupling** | Tight (declared dependency) | Loose (no dependency) |
| **Direction** | Point-to-point | Broadcast |
| **Response** | Can have response (Request-Reply) | No response |
| **Discovery** | Via ServiceContainer | Via EventRegistry |
| **Startup Order** | Enforced by framework | Independent |

Use **services** when:
- You need a response from the called module
- You know exactly which module provides the functionality
- There's a clear caller-callee relationship

Use **events** when:
- Multiple modules might be interested
- The emitter doesn't care who consumes
- You want loose coupling

Event consumers, including type-safe batch handlers (`TypedEventStreamConsumerHandler[T]`) and their Ack/Nak semantics, are covered in [Event Consumers](consumers.md).

## Complete Example

See the [multi-module example](../../../examples/multi-module/README.md) for a complete implementation showing service registration, dependency declaration, and service consumption.

## Summary

- **Services** are the public APIs modules expose to each other
- **ServiceContainer** manages service registration and discovery
- **Dependencies** must be declared when using another module's services
- The **framework handles startup order** based on dependencies
- Choose the right **service type** for your communication needs

## Next Steps

- Learn about [Inter-Module Communication](inter-module-communication.md) patterns in detail
- Explore the [API Reference for ServiceContainer](../api/container.md)
- Review [Module interfaces](modules.md) including ServiceProviderModule and DependentModule

---

Now you understand how services connect modules! Continue to [Inter-Module Communication](inter-module-communication.md) for detailed patterns.
