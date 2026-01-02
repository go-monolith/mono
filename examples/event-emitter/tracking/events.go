package tracking

import (
	"github.com/go-monolith/mono/pkg/helper"
)

// OrderCreatedV1 is the event definition for when an order is created.
var OrderCreatedV1 = helper.EventDefinition[OrderCreatedEvent](
	"tracking", "OrderCreated", "v1",
)

// OrderShippedV1 is the event definition for when an order is shipped.
var OrderShippedV1 = helper.EventDefinition[OrderShippedEvent](
	"tracking", "OrderShipped", "v1",
)
