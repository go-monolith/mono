// Package events provides shared event definitions for the multi-module example.
// This package breaks circular dependencies by centralizing event definitions
// that are published and consumed across different modules.
package events

import (
	"time"

	"github.com/go-monolith/mono/pkg/helper"
)

// OrderCreatedEvent represents an event published when an order is created.
type OrderCreatedEvent struct {
	OrderID   string    `json:"order_id"`
	ProductID string    `json:"product_id"`
	Amount    float64   `json:"amount"`
	Timestamp time.Time `json:"timestamp"`
}

// OrderCreatedV1 is the event definition for order created events.
// This follows the EventEmitterModule pattern for event discovery.
var OrderCreatedV1 = helper.EventDefinition[OrderCreatedEvent](
	"order", "OrderCreated", "v1",
)
