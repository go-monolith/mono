// Package tracking provides an order tracking module that demonstrates EventEmitterModule.
// This module tracks orders and emits events when orders are created or shipped.
package tracking

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/go-monolith/mono/v1"
)

// TrackingModule implements the tracking module that emits order-related events.
// It implements mono.TrackingModule, mono.EventBusAwareModule, and mono.EventEmitterModule.
type TrackingModule struct {
	eventBus   mono.EventBus
	orderCount atomic.Int64
}

var _ mono.EventEmitterModule = (*TrackingModule)(nil)

// NewModule creates a new tracking module instance.
func NewModule() *TrackingModule {
	m := &TrackingModule{}

	return m
}

// Name returns the module name.
func (m *TrackingModule) Name() string {
	return "tracking"
}

// Start initializes the module.
func (m *TrackingModule) Start(ctx context.Context) error {
	return nil
}

// Stop gracefully shuts down the module.
func (m *TrackingModule) Stop(ctx context.Context) error {
	return nil
}

// SetEventBus receives the event bus for publishing events.
func (m *TrackingModule) SetEventBus(bus mono.EventBus) {
	m.eventBus = bus
}

// EmitEvents returns all event definitions this module can emit.
// This implements the EventEmitterModule interface.
func (m *TrackingModule) EmitEvents() []mono.BaseEventDefinition {
	return []mono.BaseEventDefinition{
		OrderCreatedV1.ToBase(),
		OrderShippedV1.ToBase(),
	}
}

// CreateOrder creates a new order and emits an OrderCreated event.
func (m *TrackingModule) CreateOrder(customerID, productID string, quantity int, amount float64, currency string) (string, error) {
	// Generate order ID
	orderNum := m.orderCount.Add(1)
	orderID := fmt.Sprintf("ORD-%06d", orderNum)

	// Create the event payload
	event := OrderCreatedEvent{
		OrderID:    orderID,
		CustomerID: customerID,
		ProductID:  productID,
		Quantity:   quantity,
		Amount:     amount,
		Currency:   currency,
	}

	// Use type-safe Publish method (auto-marshals to JSON)
	if err := OrderCreatedV1.Publish(m.eventBus, event, nil); err != nil {
		return "", fmt.Errorf("failed to publish order created event: %w", err)
	}

	return orderID, nil
}

// ShipOrder marks an order as shipped and emits an OrderShipped event.
func (m *TrackingModule) ShipOrder(orderID, trackingNum, carrier string) error {
	// Create the event payload
	event := OrderShippedEvent{
		OrderID:     orderID,
		TrackingNum: trackingNum,
		Carrier:     carrier,
	}

	// Use type-safe Publish method (auto-marshals to JSON)
	if err := OrderShippedV1.Publish(m.eventBus, event, nil); err != nil {
		return fmt.Errorf("failed to publish order shipped event: %w", err)
	}

	return nil
}
