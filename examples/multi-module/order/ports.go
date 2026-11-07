package order

import (
	"context"

	inventorycontract "github.com/go-monolith/mono/examples/multi-module/inventory/contract"
	notificationcontract "github.com/go-monolith/mono/examples/multi-module/notification/contract"
	paymentcontract "github.com/go-monolith/mono/examples/multi-module/payment/contract"
)

// The driven ports of the order module. They are owned by the order module
// and describe what its domain logic needs from the outside world, in the
// DTOs of the providers' contract packages. Tests substitute fakes for them;
// the adapters in adapter.go implement them over the providers' contracts.

// InventoryPort checks stock for an order.
type InventoryPort interface {
	CheckStock(ctx context.Context, req inventorycontract.CheckStockRequest) (inventorycontract.CheckStockResponse, error)
}

// PaymentPort charges for an order.
type PaymentPort interface {
	Process(ctx context.Context, req paymentcontract.ProcessPaymentRequest) (paymentcontract.ProcessPaymentResponse, error)
}

// NotificationPort notifies about a created order.
type NotificationPort interface {
	OrderCreated(ctx context.Context, notification notificationcontract.OrderCreatedNotification) error
}
