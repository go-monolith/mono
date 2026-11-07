// Package order implements an order management module that orchestrates order placement
// by coordinating with inventory, payment, and notification modules.
// It demonstrates the EventEmitterModule pattern for publishing events and the
// ServiceConsumerModule pattern for declaring the service contracts it calls.
//
// The module imports only the contract sub-packages of its dependencies, never
// their module packages: the ports it needs are its own (ports.go) and the
// adapters (adapter.go) implement them over the providers' contracts.
package order

import (
	"context"
	"fmt"
	"time"

	"github.com/go-monolith/mono"
	inventorycontract "github.com/go-monolith/mono/examples/multi-module/inventory/contract"
	notificationcontract "github.com/go-monolith/mono/examples/multi-module/notification/contract"
	"github.com/go-monolith/mono/examples/multi-module/order/contract"
	paymentcontract "github.com/go-monolith/mono/examples/multi-module/payment/contract"
	"github.com/go-monolith/mono/pkg/types"
)

// NAME is the unique identifier for the order module.
const NAME = "order"

// OrderModule implements the mono.Module that orchestrates order placement
type OrderModule struct {
	inventory    InventoryPort
	payment      PaymentPort
	notification NotificationPort
	eventBus     mono.EventBus
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
// fails fast instead of on the first order.
func (m *OrderModule) ConsumeServices() []types.BaseServiceDefinition {
	return []types.BaseServiceDefinition{
		inventorycontract.CheckStock.ToBase(),
		paymentcontract.Process.ToBase(),
		notificationcontract.OnOrderCreated.ToBase(),
	}
}

// Start initializes the module
func (m *OrderModule) Start(_ context.Context) error {
	fmt.Println("  → Order module started (depends on: inventory, payment, notification)")
	return nil
}

// Stop gracefully shuts down the module
func (m *OrderModule) Stop(_ context.Context) error {
	fmt.Println("  → Order module stopped")
	return nil
}

// SetDependencyServiceContainer receives service containers from dependencies
// and wires the driven adapters behind the module's ports.
func (m *OrderModule) SetDependencyServiceContainer(dependency string, container mono.ServiceContainer) {
	switch dependency {
	case "inventory":
		m.inventory = inventoryAdapter{container: container}
	case "payment":
		m.payment = paymentAdapter{container: container}
	case "notification":
		m.notification = notificationAdapter{container: container}
	}
}

// SetEventBus is called by the framework to inject the event bus
func (m *OrderModule) SetEventBus(bus mono.EventBus) {
	m.eventBus = bus
}

// RegisterServices binds the domain handler to the place-order contract.
func (m *OrderModule) RegisterServices(container mono.ServiceContainer) error {
	return contract.PlaceOrder.Register(container, m.placeOrder)
}

// placeOrder handles order placement requests with typed request/response
func (m *OrderModule) placeOrder(ctx context.Context, request contract.CreateOrderRequest, _ *mono.Msg) (contract.CreateOrderResponse, error) {
	if request.ProductID == "" {
		return contract.CreateOrderResponse{}, fmt.Errorf("product_id is required")
	}
	if request.Quantity <= 0 {
		return contract.CreateOrderResponse{}, fmt.Errorf("quantity must be positive")
	}
	if request.Amount <= 0 {
		return contract.CreateOrderResponse{}, fmt.Errorf("amount must be positive")
	}
	if request.Currency == "" {
		request.Currency = "USD"
	}

	orderID := fmt.Sprintf("order_%d", time.Now().Unix())

	// Step 1: Check inventory
	stockResult, err := m.inventory.CheckStock(ctx, inventorycontract.CheckStockRequest{
		ProductID: request.ProductID,
		Quantity:  request.Quantity,
	})
	if err != nil {
		return contract.CreateOrderResponse{}, fmt.Errorf("inventory check failed: %w", err)
	}
	if !stockResult.Available {
		return contract.CreateOrderResponse{
			OrderID: orderID,
			Status:  "failed_out_of_stock",
			Message: "Product is out of stock",
		}, nil
	}

	// Step 2: Process payment
	paymentResult, err := m.payment.Process(ctx, paymentcontract.ProcessPaymentRequest{
		OrderID:  orderID,
		Amount:   request.Amount,
		Currency: request.Currency,
	})
	if err != nil {
		return contract.CreateOrderResponse{}, fmt.Errorf("payment processing failed: %w", err)
	}
	if !paymentResult.Success {
		return contract.CreateOrderResponse{
			OrderID: orderID,
			Status:  "payment_failed",
			Message: "Payment was declined",
		}, nil
	}

	// Step 3: Send notification via queue group (fire-and-forget).
	// Graceful degradation: notification failure doesn't fail the order.
	notif := notificationcontract.OrderCreatedNotification{
		OrderID:   orderID,
		ProductID: request.ProductID,
		Amount:    request.Amount,
		Timestamp: time.Now(),
	}
	if err := m.notification.OrderCreated(ctx, notif); err != nil {
		fmt.Printf("  → Warning: failed to send notification for order %s: %v\n", orderID, err)
	}

	// Step 4: Publish event to event bus using the typed definition (best-effort)
	orderEvent := contract.OrderCreatedEvent{
		OrderID:   orderID,
		ProductID: request.ProductID,
		Amount:    request.Amount,
		Timestamp: time.Now(),
	}
	if err := contract.OrderCreatedV1.Publish(m.eventBus, orderEvent, nil); err != nil {
		fmt.Printf("  → Warning: failed to publish OrderCreated event for order %s: %v\n", orderID, err)
	}

	return contract.CreateOrderResponse{
		OrderID: orderID,
		Status:  "success",
		Message: "Order created successfully",
	}, nil
}
