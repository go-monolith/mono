// Package contract is the port of the notification module: the payload of its
// queue group service and the typed contract other modules send through.
package contract

import (
	"time"

	"github.com/go-monolith/mono/pkg/helper"
)

// OrderCreatedNotification asks the notification module to notify about a new order.
type OrderCreatedNotification struct {
	OrderID   string    `json:"order_id"`
	ProductID string    `json:"product_id"`
	Amount    float64   `json:"amount"`
	Timestamp time.Time `json:"timestamp"`
}

// OnOrderCreated is the queue group service that sends order notifications
// (fire-and-forget with acceptance ACK).
var OnOrderCreated = helper.QueueGroupService[OrderCreatedNotification]("notification", "on-order-created")
