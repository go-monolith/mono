# Testing Guide

This guide shows how to test a Monolith Framework application, from plain Go tests of a module's domain logic to integration tests with the embedded NATS server. Read it when you add tests to a module or decide which kind of test a behaviour needs.

**Time to complete:** 20 minutes

**What you'll learn:**
- Testing domain logic and handlers with fakes, without the framework
- Testing a module without NATS with `monotest`, including a conformance test
- Integration testing with embedded NATS
- Table-driven test patterns and test utilities

**Prerequisites:**
- Familiarity with Go's `testing` package
- Understanding of [Module interfaces](../core-concepts/modules.md)

---

## The Recommended Path

Use the cheapest test that covers a behaviour:

1. **Domain tests with fake ports.** The module's rules, in plain Go, with no framework and no NATS.
2. **`monotest.CheckModule`**, one per module, and **`monotest.CheckImports`**, one for the application. They check the module's boundaries without NATS.
3. **The `monotest` harness** to drive a module through its services without NATS.
4. **Integration tests with embedded NATS** for what `monotest` does not emulate: JetStream, the middleware chain and worker pools.

{% hint style="info" %}
**Testing Philosophy:** Test behavior, not implementation. Focus on what your module does (public API), not how it does it (internal details).
{% endhint %}

## Unit Tests Without the Framework

### Domain Tests with Fake Ports

A module that keeps its rules in a plain Go service over its own ports (`service.go`, `ports.go`) is tested with fakes and nothing else: no framework, no NATS, no other module.

```go
type fakeInventory struct{ available bool }

func (f fakeInventory) Available(context.Context, string, int) (bool, error) { return f.available, nil }

func TestPlaceOrderOutOfStock(t *testing.T) {
    service, err := order.NewService(fakeInventory{available: false}, fakePayment{}, fakeNotification{}, fakeEvents{})
    if err != nil {
        t.Fatal(err)
    }
    result, err := service.PlaceOrder(context.Background(), order.NewOrder{ProductID: "laptop", Quantity: 1, Amount: 10})
    if err != nil || result.Status != order.StatusOutOfStock {
        t.Fatalf("result = %+v, err = %v", result, err)
    }
}
```

### Fakes That Record Calls

{% hint style="info" %}
**Keep mocks simple.** Don't over-engineer mocks with complex behavior. If your mock is getting complicated, consider using integration tests instead.
{% endhint %}

A fake can record what it was called with, so the test can check it:

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

Use it in a test:

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

Pass dependencies in, so a test can hand the module fakes:

```go
// Module with injected dependencies
type OrderModule struct {
    paymentService PaymentService
    storage        OrderStorage
    logger         mono.Logger
}

// New creates a module with dependencies
func New(payment PaymentService, storage OrderStorage, logger mono.Logger) *OrderModule {
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

### Module Start and Stop

Create a test file alongside your module:

```go
// mymodule.go
type MyModule struct {
    logger mono.Logger
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

### Service Handlers

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

### Concurrent Code

{% hint style="warning" %}
**Avoid `time.Sleep` for synchronization.** Use channels, sync.WaitGroup, or context cancellation instead. Sleep-based tests are slow and flaky.
{% endhint %}

Wait for the goroutine to finish instead of sleeping:

```go
func TestHandleOrderUpdateConcurrently(t *testing.T) {
    module := &OrderModule{
        logger: newTestLogger(),
    }

    ctx := context.Background()
    updateChan := make(chan *OrderUpdate, 10)

    // Start handler processing updates; done is closed when it returns
    done := make(chan struct{})
    go func() {
        defer close(done)
        module.listenForOrderUpdates(ctx, updateChan)
    }()

    // Send multiple updates
    for i := 0; i < 10; i++ {
        updateChan <- &OrderUpdate{
            OrderID: fmt.Sprintf("order-%d", i),
            Status:  "completed",
        }
    }

    // Closing the channel ends the loop in listenForOrderUpdates
    close(updateChan)
    <-done

    // Verify results...
}
```

## Testing Without NATS (monotest)

The `monotest` package (`github.com/go-monolith/mono/monotest`, v0.1.0+) starts one module through the framework's lifecycle order with no NATS server. It uses an in-memory event bus, the framework's real service container, and stand-in containers for dependencies.

{% hint style="info" %}
**Use `monotest.NewContainer` instead of a hand-written `ServiceContainer` fake.** It is the real container, so it never falls behind the interface.
{% endhint %}

Add these tests:

- **A conformance test per module.** `monotest.CheckModule` starts the module, runs your exercise, stops it, and reports every boundary rule the module breaks: the module name, the dependency list, the registered services, the consumed contracts (provided, and each one used), boundary violations, unused dependencies, emitted events, and `Start`/`Stop`. The test, the full rule list and the options are in the [Module Boundaries Guide](module-boundaries.md#testing-with-monotest).
- **One import rule test for the application** (since v0.1.2), below.
- **Harness tests** where you want to call the module's services and check what it did: `monotest.New` starts the module, `monotest.Call` calls a service, and the stand-in containers count the calls they handled. See [Driving a module directly](module-boundaries.md#testing-with-monotest).

To test how a module behaves without JetStream, pass `monotest.WithJetStreamEnabled(false)`. `Start` then fails with `ErrJetStreamNotEnabled` exactly as the framework does: for a module or plugin implementing `JetStreamRequiredModule`, and for a stream consumer service, cron service or event stream consumer registration. Without the option, nothing is checked.

```go
func TestCacheModuleNeedsJetStream(t *testing.T) {
    h := monotest.New(t, &CacheModule{}, monotest.WithJetStreamEnabled(false))
    if err := h.Start(context.Background()); !errors.Is(err, monoerrors.ErrJetStreamNotEnabled) {
        t.Fatalf("Start err = %v, want ErrJetStreamNotEnabled", err)
    }
}
```

### Import Rule Test

Add one for the application, in its root directory, so that `./...` covers every module. `CheckImports` runs `go list` and reports every contract package that imports more than it may, and every package that imports another module's package instead of its `contract`:

```go
func TestImportRules(t *testing.T) {
    monotest.CheckImports(t)
}
```

- A test placed elsewhere, such as beside `cmd/app/main.go`, points the check at the root with `monotest.WithImportDir("../..")`.
- The check fails when it matched no package to check, when the application has no module at all, or when `./...` matched no package of a module, so a wrong directory cannot pass by accident.
- A package named `contract` that is not a module's contract is excluded with `monotest.WithNonContractPackages`.

All options are in the [Module Boundaries Guide](module-boundaries.md#checking-imports).

{% hint style="warning" %}
**Limits:** `monotest` does not emulate JetStream (stream consumers, event stream consumers, cron services), the middleware chain, or worker pools. Use an integration test with embedded NATS for those.
{% endhint %}

## Integration Testing with Embedded NATS

{% hint style="info" %}
**Real NATS, no mocking:** Integration tests use the actual embedded NATS server. This catches issues that mocks might miss, like serialization problems or subject mismatches.
{% endhint %}

The snippets below use `contract`, the order module's contract package, as in the [multi-module example](../../../examples/multi-module/README.md).

### Running with Embedded NATS

Integration tests run with a real embedded NATS server:

```go
//go:build integration

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

### Service Communication

Call a request-reply service through its contract:

```go
func TestOrderServiceRequestReply(t *testing.T) {
    ctx := context.Background()

    app, _ := mono.NewMonoApplication()
    app.Register(&OrderModule{})
    app.Start(ctx)
    defer app.Stop(ctx)

    // Call a Request-Reply service registered by OrderModule through its contract
    resp, err := contract.PlaceOrder.Call(ctx, app.Services("order"),
        contract.CreateOrderRequest{ProductID: "laptop", Quantity: 1, Amount: 10})
    if err != nil {
        t.Fatalf("service call failed: %v", err)
    }

    if resp.OrderID == "" {
        t.Error("expected an order ID")
    }
}
```

### Event Publishing

Publish an event and wait for the consumer with a deadline, not a sleep. Here the consumer module sends each event it handles on its `Received` channel:

```go
func TestOrderEventPublishing(t *testing.T) {
    ctx := context.Background()

    app, _ := mono.NewMonoApplication()

    // Register emitter module
    app.Register(&OrderModule{})

    // Register consumer module; its event handler sends to Received
    consumer := &NotificationModule{Received: make(chan contract.OrderCreatedEvent, 1)}
    app.Register(consumer)

    app.Start(ctx)
    defer app.Stop(ctx)

    // Publish an event (from OrderModule)
    if err := contract.OrderCreatedV1.Publish(app.EventBus("order"), contract.OrderCreatedEvent{
        OrderID: "order-1",
        Amount:  100,
    }, nil); err != nil {
        t.Fatalf("publish failed: %v", err)
    }

    // Verify consumer received event
    select {
    case event := <-consumer.Received:
        if event.OrderID != "order-1" {
            t.Errorf("unexpected order ID: %q", event.OrderID)
        }
    case <-time.After(2 * time.Second):
        t.Error("consumer did not receive order created event")
    }
}
```

### JetStream

Stream consumer services, cron services, event stream consumers and the kv-jetstream and fs-jetstream plugins need JetStream. An application without it fails `Start` at once with `ErrJetStreamNotEnabled`, so enable it in tests that use them.

Test with persistent streams:

```go
func TestOrderEventStreamWithJetStream(t *testing.T) {
    ctx := context.Background()

    // Enable JetStream (stored in a temporary directory)
    app, err := mono.NewMonoApplication(
        mono.WithJetStreamStorageDir(t.TempDir()),
    )
    if err != nil {
        t.Fatalf("failed to create app: %v", err)
    }

    app.Register(&OrderModule{})
    app.Register(&AuditModule{})

    app.Start(ctx)
    defer app.Stop(ctx)

    // Publish event to its JetStream stream
    if _, err := contract.OrderCreatedV1.EventStreamPublish(ctx, app.EventBus("order"), contract.OrderCreatedEvent{
        OrderID: "order-1",
    }, nil); err != nil {
        t.Fatalf("publish failed: %v", err)
    }

    // Restart modules - JetStream should replay events
    // (This would be a separate integration test scenario)
}
```

## Test Utilities

### Test Logger

```go
func newTestLogger() mono.Logger {
    // Return a no-op logger for testing
    return &testLogger{}
}

type testLogger struct{}

func (l *testLogger) Debug(msg string, args ...any) {}
func (l *testLogger) Info(msg string, args ...any) {}
func (l *testLogger) Warn(msg string, args ...any) {}
func (l *testLogger) Error(msg string, args ...any) {}
func (l *testLogger) With(args ...any) mono.Logger { return l }
func (l *testLogger) WithModule(name string) mono.Logger { return l }
func (l *testLogger) WithError(err error) mono.Logger { return l }
```

### Test Context

```go
func newTestContext(t *testing.T) context.Context {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    t.Cleanup(cancel) // cancelled when the test ends
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
