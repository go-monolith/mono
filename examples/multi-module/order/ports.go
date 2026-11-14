package order

import "context"

// The driven ports of the order module: what its domain logic (service.go)
// needs from the outside world. They are owned by the order module and written
// in its own terms, so nothing here mentions another module, a DTO of another
// module, NATS or the framework. The adapters in adapter.go implement them and
// translate to the providers' contracts; tests substitute fakes.

// InventoryPort tells whether the stock covers an order.
type InventoryPort interface {
	// Available reports whether quantity units of productID are in stock.
	Available(ctx context.Context, productID string, quantity int) (bool, error)
}

// PaymentPort charges for an order.
type PaymentPort interface {
	// Charge charges amount in currency for orderID and reports whether the
	// payment was approved. A declined payment is not an error.
	Charge(ctx context.Context, orderID string, amount float64, currency string) (approved bool, err error)
}

// NotificationPort asks for the customer to be notified about a placed order.
type NotificationPort interface {
	// OrderPlaced requests a notification for order.
	OrderPlaced(ctx context.Context, order PlacedOrder) error
}

// EventsPort announces what happened in the order module to whoever listens.
type EventsPort interface {
	// OrderPlaced announces that order was placed.
	OrderPlaced(ctx context.Context, order PlacedOrder) error
}
