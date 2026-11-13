// Package order implements an order management module that orchestrates order placement
// by coordinating with inventory, payment, and notification modules.
// It demonstrates the EventEmitterModule pattern for publishing events and the
// ServiceConsumerModule pattern for declaring the service contracts it calls.
//
// The package follows the Ports and Adapters layout:
//
//   - contract/   what other modules may import: the place-order service, its
//     DTOs and the events the module emits;
//   - ports.go    the driven ports the domain needs, in the module's own terms;
//   - service.go  the domain logic, plain Go over the ports;
//   - adapter.go  the driven adapters, translating the ports to the providers'
//     contracts;
//   - module.go   this file: the mono.Module that wires the adapters behind the
//     ports and the driving adapter that turns a place-order request into a
//     call of the domain.
//
// The module imports only the contract sub-packages of its dependencies, never
// their module packages.
package order

import (
	"context"
	"fmt"

	"github.com/go-monolith/mono"
	inventorycontract "github.com/go-monolith/mono/examples/multi-module/inventory/contract"
	notificationcontract "github.com/go-monolith/mono/examples/multi-module/notification/contract"
	"github.com/go-monolith/mono/examples/multi-module/order/contract"
	paymentcontract "github.com/go-monolith/mono/examples/multi-module/payment/contract"
	"github.com/go-monolith/mono/pkg/types"
)

// NAME is the unique identifier for the order module.
const NAME = "order"

// OrderModule implements the mono.Module of the order module. It holds no
// order rules: it receives the framework's containers and event bus, puts the
// adapters behind the ports, builds the domain Service and exposes it through
// the place-order contract.
type OrderModule struct {
	inventory    InventoryPort
	payment      PaymentPort
	notification NotificationPort
	events       EventsPort

	service *Service
}

// Compile-time interface checks
var (
	_ mono.DependentModule        = (*OrderModule)(nil)
	_ mono.ServiceProviderModule  = (*OrderModule)(nil)
	_ mono.EventEmitterModule     = (*OrderModule)(nil)
	_ types.ServiceConsumerModule = (*OrderModule)(nil)
)

// NewModule creates a new order module
func NewModule() *OrderModule {
	return &OrderModule{}
}

// EmitEvents returns all event definitions this module can emit.
// This implements the EventEmitterModule interface for event discovery.
func (m *OrderModule) EmitEvents() []mono.BaseEventDefinition {
	return []mono.BaseEventDefinition{
		contract.OrderCreatedV1.ToBase(),
	}
}

// Name returns the module identifier
func (m *OrderModule) Name() string {
	return NAME
}

// Dependencies returns the list of module dependencies
func (m *OrderModule) Dependencies() []string {
	return []string{"inventory", "payment", "notification"}
}

// ConsumeServices declares the service contracts this module calls. The
// framework checks at startup that each one is provided by a declared
// dependency with the right service type, so a renamed or missing service
// fails fast instead of on the first order. The list is complete: a call to a
// dependency service that is not in it is reported as a boundary violation.
func (m *OrderModule) ConsumeServices() []types.BaseServiceDefinition {
	return []types.BaseServiceDefinition{
		inventorycontract.CheckStock.ToBase(),
		paymentcontract.Process.ToBase(),
		notificationcontract.OnOrderCreated.ToBase(),
	}
}

// SetDependencyServiceContainer receives service containers from dependencies
// and wires the driven adapters behind the module's ports.
func (m *OrderModule) SetDependencyServiceContainer(dependency string, container mono.ServiceContainer) {
	switch dependency {
	case "inventory":
		m.inventory = inventoryAdapter{services: container}
	case "payment":
		m.payment = paymentAdapter{services: container}
	case "notification":
		m.notification = notificationAdapter{services: container}
	}
}

// SetEventBus is called by the framework to inject the event bus. It goes
// behind the events port like any other outside dependency.
func (m *OrderModule) SetEventBus(bus mono.EventBus) {
	m.events = eventsAdapter{bus: bus}
}

// RegisterServices binds the driving adapter to the place-order contract.
func (m *OrderModule) RegisterServices(container mono.ServiceContainer) error {
	return contract.PlaceOrder.Register(container, m.placeOrder)
}

// Start builds the domain service over the ports wired above. The framework
// has set every dependency container and the event bus by now, so a missing
// port is a wiring error and fails the start.
func (m *OrderModule) Start(_ context.Context) error {
	service, err := NewService(m.inventory, m.payment, m.notification, m.events)
	if err != nil {
		return fmt.Errorf("order module: %w", err)
	}
	m.service = service
	fmt.Println("  → Order module started (depends on: inventory, payment, notification)")
	return nil
}

// Stop gracefully shuts down the module
func (m *OrderModule) Stop(_ context.Context) error {
	fmt.Println("  → Order module stopped")
	return nil
}

// placeOrder is the driving adapter of the place-order contract: it translates
// the contract's request into the domain's terms, calls the domain, and
// translates the result back. Requests are served only after Start.
func (m *OrderModule) placeOrder(ctx context.Context, request contract.CreateOrderRequest, _ *mono.Msg) (contract.CreateOrderResponse, error) {
	result, err := m.service.PlaceOrder(ctx, NewOrder{
		ProductID: request.ProductID,
		Quantity:  request.Quantity,
		Amount:    request.Amount,
		Currency:  request.Currency,
	})
	if err != nil {
		return contract.CreateOrderResponse{}, err
	}
	return contract.CreateOrderResponse{
		OrderID: result.OrderID,
		Status:  string(result.Status),
		Message: result.Message,
	}, nil
}
