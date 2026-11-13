# Testing Guide

This guide covers testing strategies for the Monolith Framework, including unit testing modules, integration testing with embedded NATS, and mocking dependencies.

**Time to complete:** 20 minutes

**What you'll learn:**
- Writing unit tests for modules and handlers
- Integration testing with embedded NATS
- Creating and using mock dependencies
- Testing a module without NATS with `monotest`, including a conformance test
- Table-driven test patterns

**Prerequisites:**
- Familiarity with Go's `testing` package
- Understanding of [Module interfaces](../core-concepts/modules.md)

---

## Overview

The Monolith Framework is designed to be testable with clear separation of concerns. Modules can be tested in isolation using unit tests, and tested together with embedded NATS using integration tests.

{% hint style="info" %}
**Testing Philosophy:** Test behavior, not implementation. Focus on what your module does (public API), not how it does it (internal details).
{% endhint %}

## Unit Testing Modules

### Basic Module Test Structure

Create a test file alongside your module:

```go
// mymodule.go
type MyModule struct {
    logger types.Logger
    // ... other fields
}

// mymodule_test.go
package mymodule

import (
    "context"
    "testing"
)

func TestMyModuleStart(t *testing.T) {
    module := &MyModule{
        logger: newTestLogger(),
    }

    ctx := context.Background()
    if err := module.Start(ctx); err != nil {
        t.Fatalf("Start failed: %v", err)
    }

    if err := module.Stop(ctx); err != nil {
        t.Fatalf("Stop failed: %v", err)
    }
}
```

### Testing Service Handlers

Test service handlers directly without running the full framework:

```go
func TestHandleGetOrder(t *testing.T) {
    module := &OrderModule{
        logger: newTestLogger(),
        orders: map[string]*Order{
            "order-1": {ID: "order-1", Amount: 100},
        },
    }

    ctx := context.Background()
    req := &GetOrderRequest{OrderID: "order-1"}

    order, err := module.handleGetOrder(ctx, req)
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }

    if order.Amount != 100 {
        t.Errorf("expected amount 100, got %d", order.Amount)
    }
}
```

### Table-Driven Tests

Use table-driven tests for multiple test cases:

```go
func TestValidateOrder(t *testing.T) {
    testCases := []struct {
        name    string
        order   *Order
        wantErr bool
        errMsg  string
    }{
        {
            name: "valid order",
            order: &Order{
                ID:         "ord-1",
                CustomerID: "cust-1",
                Amount:     100,
            },
            wantErr: false,
        },
        {
            name: "missing order ID",
            order: &Order{
                CustomerID: "cust-1",
                Amount:     100,
            },
            wantErr: true,
            errMsg:  "order ID required",
        },
        {
            name: "negative amount",
            order: &Order{
                ID:         "ord-1",
                CustomerID: "cust-1",
                Amount:     -100,
            },
            wantErr: true,
            errMsg:  "amount must be positive",
        },
    }

    module := &OrderModule{}

    for _, tt := range testCases {
        t.Run(tt.name, func(t *testing.T) {
            err := module.validateOrder(tt.order)

            if (err != nil) != tt.wantErr {
                t.Errorf("got error %v, want error %v", err != nil, tt.wantErr)
            }

            if tt.wantErr && !strings.Contains(err.Error(), tt.errMsg) {
                t.Errorf("error %q does not contain %q", err.Error(), tt.errMsg)
            }
        })
    }
}
```

### Testing Concurrent Code

{% hint style="warning" %}
**Avoid `time.Sleep` for synchronization.** Use channels, sync.WaitGroup, or context cancellation instead. Sleep-based tests are slow and flaky.
{% endhint %}

For concurrent handlers, prefer channels over sleep:

```go
func TestHandleOrderUpdateConcurrently(t *testing.T) {
    module := &OrderModule{
        logger: newTestLogger(),
    }

    ctx := context.Background()
    updateChan := make(chan *OrderUpdate, 10)

    // Start handler processing updates
    go module.listenForOrderUpdates(ctx, updateChan)

    // Send multiple updates
    for i := 0; i < 10; i++ {
        updateChan <- &OrderUpdate{
            OrderID: fmt.Sprintf("order-%d", i),
            Status:  "completed",
        }
    }

    close(updateChan)

    // Allow goroutine to finish processing
    time.Sleep(100 * time.Millisecond)

    // Verify results...
}
```

## Integration Testing with Embedded NATS

{% hint style="info" %}
**Real NATS, no mocking:** Integration tests use the actual embedded NATS server. This catches issues that mocks might miss, like serialization problems or subject mismatches.
{% endhint %}

### Running with Embedded NATS

Integration tests run with a real embedded NATS server:

```go
// test/integration/order_service_test.go
package integration

import (
    "context"
    "testing"

    mono "github.com/go-monolith/mono"
)

func TestOrderServiceWithNATS(t *testing.T) {
    ctx := context.Background()

    // Create application with embedded NATS
    app, err := mono.NewMonoApplication()
    if err != nil {
        t.Fatalf("failed to create app: %v", err)
    }

    // Register modules
    app.Register(&OrderModule{})
    app.Register(&PaymentModule{})

    // Start application (starts embedded NATS)
    if err := app.Start(ctx); err != nil {
        t.Fatalf("failed to start app: %v", err)
    }
    defer app.Stop(ctx)

    // Test inter-module communication through NATS
    // ...
}
```

### Testing Service Communication

Test Request-Reply services:

```go
func TestOrderServiceRequestReply(t *testing.T) {
    ctx := context.Background()

    app, _ := mono.NewMonoApplication()
    app.Register(&OrderModule{})
    app.Start(ctx)
    defer app.Stop(ctx)

    // Call a Request-Reply service registered by OrderModule
    req := &GetOrderRequest{OrderID: "order-1"}
    order := &Order{}

    // Framework would provide service discovery here
    // This is a simplified example
    service, err := getOrderService(app)
    if err != nil {
        t.Fatalf("service not found: %v", err)
    }

    result, err := service.GetOrder(ctx, req)
    if err != nil {
        t.Fatalf("service call failed: %v", err)
    }

    if result.ID != "order-1" {
        t.Errorf("unexpected order ID: %v", result.ID)
    }
}
```

### Testing Event Publishing

Test event pub/sub:

```go
func TestOrderEventPublishing(t *testing.T) {
    ctx := context.Background()

    app, _ := mono.NewMonoApplication()

    // Register emitter module
    app.Register(&OrderModule{})

    // Register consumer module with event handler
    consumer := &NotificationModule{}
    app.Register(consumer)

    app.Start(ctx)
    defer app.Stop(ctx)

    // Publish an event (from OrderModule)
    app.EventBus().Publish(ctx, &OrderCreatedEvent{
        OrderID: "order-1",
        Amount:  100,
    })

    // Allow event to propagate
    time.Sleep(100 * time.Millisecond)

    // Verify consumer received event
    if !consumer.ReceivedOrderCreated {
        t.Error("consumer did not receive order created event")
    }
}
```

### Testing with JetStream

Test with persistent streams:

```go
func TestOrderEventStreamWithJetStream(t *testing.T) {
    ctx := context.Background()

    // Enable JetStream for persistence
    app, err := mono.NewMonoApplication(
        mono.WithJetStreamEnabled(true),
    )
    if err != nil {
        t.Fatalf("failed to create app: %v", err)
    }

    app.Register(&OrderModule{})
    app.Register(&AuditModule{})

    app.Start(ctx)
    defer app.Stop(ctx)

    // Publish event
    app.EventBus().Publish(ctx, &OrderCreatedEvent{
        OrderID: "order-1",
    })

    // Restart modules - JetStream should replay events
    // (This would be a separate integration test scenario)
}
```

## Testing Without NATS (monotest)

The `monotest` package (`github.com/go-monolith/mono/monotest`, v0.1.0+) starts one module through the framework's lifecycle order with no NATS server. It uses an in-memory event bus, the framework's real service container, and stand-in containers for dependencies.

### Conformance Test

Add one per module. `CheckModule` starts the module, runs your exercise, stops it, and reports every boundary rule the module breaks:

```go
func TestOrderModuleConformance(t *testing.T) {
    bus := monotest.NewEventBus()
    inventory := monotest.NewContainer("inventory", bus)
    err := monotest.StubRequestReply(inventory, "check-stock",
        func(_ context.Context, req CheckStockRequest) (CheckStockResponse, error) {
            return CheckStockResponse{Available: true, Stock: 5}, nil
        })
    if err != nil {
        t.Fatal(err)
    }

    monotest.CheckModule(t, order.NewModule(),
        monotest.WithDependency("inventory", inventory),
        monotest.WithExercise(func(ctx context.Context, h *monotest.Harness) error {
            _, err := contract.PlaceOrder.Call(ctx, h.Container(), contract.PlaceOrderRequest{ProductID: "laptop", Quantity: 1})
            return err
        }))
}
```

It checks the module name, the dependency list, the registered services, the consumed contracts (provided, and each one used), boundary violations, unused dependencies, emitted events, and `Start`/`Stop`. The full rule list and options are in the [Module Boundaries Guide](module-boundaries.md#testing-with-monotest).

### Import Rule Test

Since v0.2.0. Add one for the application, in its root directory, so that `./...` covers every module. `CheckImports` runs `go list` and reports every contract package that imports more than it may, and every package that imports another module's package instead of its `contract`:

```go
func TestImportRules(t *testing.T) {
    monotest.CheckImports(t)
}
```

A test placed elsewhere, such as beside `cmd/app/main.go`, points the check at the root with `monotest.WithImportDir("../..")`. The check fails when it matched no package to check, when the application has no module at all, or when `./...` matched no package of a module, so a wrong directory cannot pass by accident. A package named `contract` that is not a module's contract is excluded with `monotest.WithNonContractPackages`. Options are in the [Module Boundaries Guide](module-boundaries.md#checking-imports).

### Domain Tests with Fake Ports

A module that keeps its rules in a plain Go service over its own ports (`service.go`, `ports.go`) is tested with fakes and nothing else: no framework, no NATS, no other module.

```go
type fakeInventory struct{ available bool }

func (f fakeInventory) Available(context.Context, string, int) (bool, error) { return f.available, nil }

func TestPlaceOrderOutOfStock(t *testing.T) {
    service, err := order.NewService(fakeInventory{available: false}, fakePayment{}, fakeEvents{})
    if err != nil {
        t.Fatal(err)
    }
    result, err := service.PlaceOrder(context.Background(), order.NewOrder{ProductID: "laptop", Quantity: 1, Amount: 10})
    if err != nil || result.Status != order.StatusOutOfStock {
        t.Fatalf("result = %+v, err = %v", result, err)
    }
}
```

### Driving a Module

```go
h := monotest.New(t, order.NewModule(), monotest.WithDependency("inventory", inventory))
if err := h.Start(ctx); err != nil {
    t.Fatal(err)
}
defer h.Stop(ctx)

resp := monotest.Call[PlaceOrderRequest, PlaceOrderResponse](t, h, "place-order", PlaceOrderRequest{ProductID: "laptop", Quantity: 1})

if inventory.Calls("check-stock") != 1 {
    t.Error("inventory was not consulted")
}
```

{% hint style="warning" %}
**Limits:** `monotest` does not emulate JetStream (stream consumers, event stream consumers, cron services), the middleware chain, or worker pools. Use an integration test with embedded NATS for those.
{% endhint %}

## Mocking Dependencies

{% hint style="info" %}
**Use `monotest.NewContainer` instead of a hand-written `ServiceContainer` fake.** It is the real container, so it never falls behind the interface.
{% endhint %}

{% hint style="info" %}
**Keep mocks simple.** Don't over-engineer mocks with complex behavior. If your mock is getting complicated, consider using integration tests instead.
{% endhint %}

### Creating Mock Interfaces

Implement lightweight mocks:

```go
// mockpaymentservice.go
type MockPaymentService struct {
    ChargeFunc func(ctx context.Context, amount int64) (*PaymentResult, error)
    lastCall   *PaymentCall
}

type PaymentCall struct {
    Amount int64
}

func (m *MockPaymentService) Charge(ctx context.Context, amount int64) (*PaymentResult, error) {
    m.lastCall = &PaymentCall{Amount: amount}

    if m.ChargeFunc != nil {
        return m.ChargeFunc(ctx, amount)
    }

    return &PaymentResult{Success: true}, nil
}

// Verify what was called
func (m *MockPaymentService) WasCalled(t *testing.T, expectedAmount int64) {
    if m.lastCall == nil {
        t.Error("Charge was not called")
        return
    }

    if m.lastCall.Amount != expectedAmount {
        t.Errorf("expected charge amount %d, got %d", expectedAmount, m.lastCall.Amount)
    }
}
```

### Using Mocks in Tests

```go
func TestCreateOrderWithPayment(t *testing.T) {
    mockPayment := &MockPaymentService{
        ChargeFunc: func(ctx context.Context, amount int64) (*PaymentResult, error) {
            return &PaymentResult{TransactionID: "tx-123"}, nil
        },
    }

    module := &OrderModule{
        paymentService: mockPayment,
        logger:        newTestLogger(),
    }

    ctx := context.Background()
    order, err := module.handleCreateOrder(ctx, &CreateOrderRequest{
        CustomerID: "cust-1",
        Amount:     100,
    })

    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }

    // Verify payment was called with correct amount
    mockPayment.WasCalled(t, 100)
}
```

### Injecting Dependencies

Use dependency injection for testability:

```go
// Module with injected dependencies
type OrderModule struct {
    paymentService PaymentService
    storage        OrderStorage
    logger         types.Logger
}

// New creates a module with dependencies
func New(payment PaymentService, storage OrderStorage, logger types.Logger) *OrderModule {
    return &OrderModule{
        paymentService: payment,
        storage:        storage,
        logger:         logger,
    }
}

// Test with mocks
func TestOrderModuleWithMocks(t *testing.T) {
    mockPayment := &MockPaymentService{}
    mockStorage := &MockOrderStorage{}
    mockLogger := newTestLogger()

    module := New(mockPayment, mockStorage, mockLogger)

    // Test module behavior...
}
```

## Test Utilities

### Creating Test Logger

```go
func newTestLogger() types.Logger {
    // Return a no-op logger for testing
    return &testLogger{}
}

type testLogger struct{}

func (l *testLogger) Debug(msg string, args ...any) {}
func (l *testLogger) Info(msg string, args ...any) {}
func (l *testLogger) Warn(msg string, args ...any) {}
func (l *testLogger) Error(msg string, args ...any) {}
func (l *testLogger) With(args ...any) types.Logger { return l }
func (l *testLogger) WithModule(name string) types.Logger { return l }
func (l *testLogger) WithError(err error) types.Logger { return l }
```

### Creating Test Context

```go
func newTestContext() context.Context {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    // Note: Don't cancel in test setup - let timeout do cleanup
    _ = cancel
    return ctx
}
```

### Test Helpers

```go
func requireNoError(t *testing.T, err error) {
    t.Helper()
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
}

func requireEqual(t *testing.T, got, want any) {
    t.Helper()
    if got != want {
        t.Errorf("got %v, want %v", got, want)
    }
}
```

## Test Organization

### Test File Structure

```go
// mymodule_test.go

package mymodule

import "testing"

// Test public API
func TestPublicFunction(t *testing.T) { ... }

// Test error cases
func TestPublicFunctionErrors(t *testing.T) { ... }

// Test concurrent behavior
func TestPublicFunctionConcurrently(t *testing.T) { ... }

// Helper functions
func newTestModule() *MyModule { ... }
```

### Running Tests

```bash
# Run all tests
go test ./...

# Run tests with verbose output
go test -v ./...

# Run specific test
go test -run TestHandleCreateOrder ./...

# Run with coverage
go test -cover ./...

# Run integration tests only
go test -tags integration ./test/integration/...
```

## Checklist for Test Coverage

When writing tests for a module, ensure you cover:

- [ ] Happy path (normal operation)
- [ ] Error cases (dependency failures, validation errors)
- [ ] Edge cases (empty input, maximum values, nil pointers)
- [ ] Concurrent access (if applicable)
- [ ] Timeout scenarios (if applicable)
- [ ] Integration with other modules (integration tests)
- [ ] Module boundaries (`monotest.CheckModule`)
- [ ] Import rule (`monotest.CheckImports`)

## Best Practices Summary

✓ **Do**
- Write unit tests for all public functions
- Use table-driven tests for multiple cases
- Name test files with `_test.go` suffix
- Test error scenarios
- Mock external dependencies
- Add a `monotest.CheckModule` test per module
- Use helper functions for test setup
- Write descriptive test names
- Test concurrent behavior explicitly

✗ **Don't**
- Skip error test cases
- Make tests dependent on execution order
- Use `time.Sleep()` for synchronization (use channels instead)
- Create test data files in the repository
- Test implementation details (test behavior)
- Leave debug code in tests
- Share state between tests

## Related Documentation

- [Error Handling Guide](error-handling.md)
- [Logging Guide](logging.md)
- [Module Boundaries Guide](module-boundaries.md)
- [Core Concepts - Modules](../core-concepts/modules.md)

---

For more information on testing in Go, see the [Go testing package documentation](https://golang.org/pkg/testing/).
