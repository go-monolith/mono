// Package monotest tests mono modules without NATS.
//
// It provides an in-memory EventBus with core NATS semantics (subjects with
// wildcards, queue groups, request-reply), a Container that is the framework's
// real ServiceContainer bound to a module and dispatching its handlers on that
// bus, a Harness that starts one module through the documented lifecycle order
// (plugins, dependency containers, event bus, RegisterServices,
// RegisterEventConsumers, Start) with the same read-only dependency views and
// module-scoped event bus the framework uses, and CheckModule, which runs the
// harness and asserts that the module follows the Ports and Adapters rules the
// framework can observe.
//
// A typical module test:
//
//	bus := monotest.NewEventBus()
//	inventory := monotest.NewContainer("inventory", bus)
//	monotest.StubRequestReply(inventory, "check-stock", func(_ context.Context, req contract.CheckStockRequest) (contract.CheckStockResponse, error) {
//	    return contract.CheckStockResponse{Available: true, Stock: 3}, nil
//	})
//
//	h := monotest.New(t, order.NewModule(), monotest.WithDependency("inventory", inventory))
//	h.Start(context.Background())
//	defer h.Stop(context.Background())
//
//	resp := monotest.Call[contract.CreateOrderRequest, contract.CreateOrderResponse](t, h, "place-order", contract.CreateOrderRequest{ProductID: "a", Quantity: 1, Amount: 10})
//
// And the conformance check, usually one per module package:
//
//	func TestOrderModuleConformance(t *testing.T) {
//	    monotest.CheckModule(t, order.NewModule(),
//	        monotest.WithDependency("inventory", inventory),
//	        monotest.WithExercise(func(ctx context.Context, h *monotest.Harness) error {
//	            _, err := contract.PlaceOrder.Call(ctx, h.Container(), contract.CreateOrderRequest{ProductID: "a", Quantity: 1, Amount: 10})
//	            return err
//	        }))
//	}
//
// JetStream is not emulated: stream consumer, event stream consumer and cron
// services are registered but not dispatched, and EventStream returns an
// error. Use an integration test with the embedded server for those.
package monotest
