package contract

import (
	"time"

	"github.com/go-monolith/mono/pkg/helper"
)

// OrderCreatedEvent is published when an order has been created.
type OrderCreatedEvent struct {
	OrderID   string    `json:"order_id"`
	ProductID string    `json:"product_id"`
	Amount    float64   `json:"amount"`
	Timestamp time.Time `json:"timestamp"`
}

// OrderCreatedV1 is the event definition for order created events. Emitted by
// the order module (EventEmitterModule) and consumed by the notification
// module; both import it from here, so there is no shared "events" package
// and no import cycle.
var OrderCreatedV1 = helper.EventDefinition[OrderCreatedEvent](
	"order", "OrderCreated", "v1",
)
