// Package order implements an order management module that orchestrates order placement
// by coordinating with inventory, payment, and notification modules.
// It demonstrates the EventEmitterModule pattern for publishing events.
package order

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-monolith/mono/v1"
	"github.com/go-monolith/mono/v1/examples/multi-module/events"
	"github.com/go-monolith/mono/v1/examples/multi-module/inventory"
	"github.com/go-monolith/mono/v1/examples/multi-module/notification"
	"github.com/go-monolith/mono/v1/examples/multi-module/payment"
	"github.com/go-monolith/mono/v1/pkg/helper"
)

// NAME is the unique identifier for the order module.
const NAME = "order"

// OrderModule implements the mono.Module that orchestrates order placement
type OrderModule struct {
	inventory    inventory.InventoryAdapterPort
	payment      payment.PaymentAdapterPort
	notification notification.NotificationAdapterPort
	eventBus     mono.EventBus
}

// Compile-time interface checks
var (
	_ mono.DependentModule       = (*OrderModule)(nil)
	_ mono.ServiceProviderModule = (*OrderModule)(nil)
	_ mono.EventEmitterModule    = (*OrderModule)(nil)
)

// NewModule creates a new order module
func NewModule() *OrderModule {
	return &OrderModule{}
}

// EmitEvents returns all event definitions this module can emit.
// This implements the EventEmitterModule interface for event discovery.
func (m *OrderModule) EmitEvents() []mono.BaseEventDefinition {
	return []mono.BaseEventDefinition{
		events.OrderCreatedV1.ToBase(),
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
func (m *OrderModule) SetDependencyServiceContainer(dependency string, container mono.ServiceContainer) {
	switch dependency {
	case "inventory":
		m.inventory = inventory.NewInventoryAdapter(container)
	case "payment":
		m.payment = payment.NewPaymentAdapter(container)
	case "notification":
		m.notification = notification.NewNotificationAdapter(container)
	}
}

// SetEventBus is called by the framework to inject the event bus
func (m *OrderModule) SetEventBus(bus mono.EventBus) {
	m.eventBus = bus
}

// RegisterServices registers the place_order service using type-safe helper
func (m *OrderModule) RegisterServices(container mono.ServiceContainer) error {
	// Register the place_order request-reply service using typed helper
	// This automatically handles JSON unmarshaling of requests and marshaling of responses
	return helper.RegisterTypedRequestReplyService(
		container,
		"place-order",
		json.Unmarshal,
		json.Marshal,
		m.placeOrder,
	)
}

// placeOrder handles order placement requests with typed request/response
func (m *OrderModule) placeOrder(ctx context.Context, request CreateOrderRequest, _ *mono.Msg) (CreateOrderResponse, error) {
	// Validate request
	if request.ProductID == "" {
		return CreateOrderResponse{}, fmt.Errorf("product_id is required")
	}
	if request.Quantity <= 0 {
		return CreateOrderResponse{}, fmt.Errorf("quantity must be positive")
	}
	if request.Amount <= 0 {
		return CreateOrderResponse{}, fmt.Errorf("amount must be positive")
	}
	if request.Currency == "" {
		request.Currency = "USD"
	}

	// Generate order ID
	orderID := fmt.Sprintf("order_%d", time.Now().Unix())

	// Step 1: Check inventory
	checkStockReq := &inventory.CheckStockRequest{
		ProductID: request.ProductID,
		Quantity:  request.Quantity,
	}
	stockResult, err := m.inventory.CheckStock(ctx, checkStockReq)
	if err != nil {
		return CreateOrderResponse{}, fmt.Errorf("inventory check failed: %w", err)
	}

	if !stockResult.Available {
		return CreateOrderResponse{
			OrderID: orderID,
			Status:  "failed_out_of_stock",
			Message: "Product is out of stock",
		}, nil
	}

	// Step 2: Process payment
	processPaymentReq := &payment.ProcessPaymentRequest{
		OrderID:  orderID,
		Amount:   request.Amount,
		Currency: request.Currency,
	}
	paymentResult, err := m.payment.Process(ctx, processPaymentReq)
	if err != nil {
		return CreateOrderResponse{}, fmt.Errorf("payment processing failed: %w", err)
	}
	if !paymentResult.Success {
		return CreateOrderResponse{
			OrderID: orderID,
			Status:  "payment_failed",
			Message: "Payment was declined",
		}, nil
	}

	// Step 3: Send notification via queue group (fire-and-forget)
	notif := &notification.OrderCreatedNotification{
		OrderID:   orderID,
		ProductID: request.ProductID,
		Amount:    request.Amount,
		Timestamp: time.Now(),
	}
	// Graceful degradation: notification failure doesn't fail the order
	if err := m.notification.SendOnOrderCreatedNotification(ctx, notif); err != nil {
		fmt.Printf("  → Warning: failed to send notification for order %s: %v\n", orderID, err)
	}

	// Step 4: Publish event to event bus using type-safe Publish method
	orderEvent := events.OrderCreatedEvent{
		OrderID:   orderID,
		ProductID: request.ProductID,
		Amount:    request.Amount,
		Timestamp: time.Now(),
	}
	// Use type-safe Publish method (auto-marshals to JSON)
	if err := events.OrderCreatedV1.Publish(m.eventBus, orderEvent, nil); err != nil {
		// Event publishing is best-effort; log but don't fail the order
		fmt.Printf("  → Warning: failed to publish OrderCreated event for order %s: %v\n", orderID, err)
	}

	// Step 5: Return success response
	return CreateOrderResponse{
		OrderID: orderID,
		Status:  "success",
		Message: "Order created successfully",
	}, nil
}
