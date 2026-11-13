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

// The driven adapters: each implements one port of ports.go over a provider's
// contract and translates between the order module's own terms and the
// provider's DTOs. This file is the only place in the module that knows which
// module answers a port, what its DTOs look like, and that the call goes over
// NATS: when a provider changes its contract, the change stops here.
//
// An adapter holds its dependency as a types.ServiceLookup, the read side of
// the container received in SetDependencyServiceContainer: it can look
// services up and nothing else.

// Compile-time checks that the adapters implement the ports.
var (
	_ InventoryPort    = inventoryAdapter{}
	_ PaymentPort      = paymentAdapter{}
	_ NotificationPort = notificationAdapter{}
	_ EventsPort       = eventsAdapter{}
)

// inventoryAdapter implements InventoryPort over the inventory module's
// check-stock contract.
type inventoryAdapter struct{ services types.ServiceLookup }

func (a inventoryAdapter) Available(ctx context.Context, productID string, quantity int) (bool, error) {
	resp, err := inventorycontract.CheckStock.Call(ctx, a.services, inventorycontract.CheckStockRequest{
		ProductID: productID,
		Quantity:  quantity,
	})
	if err != nil {
		return false, fmt.Errorf("inventory check failed: %w", err)
	}
	return resp.Available, nil
}

// paymentAdapter implements PaymentPort over the payment module's process
// contract.
type paymentAdapter struct{ services types.ServiceLookup }

func (a paymentAdapter) Charge(ctx context.Context, orderID string, amount float64, currency string) (bool, error) {
	resp, err := paymentcontract.Process.Call(ctx, a.services, paymentcontract.ProcessPaymentRequest{
		OrderID:  orderID,
		Amount:   amount,
		Currency: currency,
	})
	if err != nil {
		return false, fmt.Errorf("payment processing failed: %w", err)
	}
	return resp.Success, nil
}

// notificationAdapter implements NotificationPort over the notification
// module's on-order-created queue group contract (fire-and-forget with an
// acceptance ACK).
type notificationAdapter struct{ services types.ServiceLookup }

func (a notificationAdapter) OrderPlaced(ctx context.Context, order PlacedOrder) error {
	err := notificationcontract.OnOrderCreated.Send(ctx, a.services, notificationcontract.OrderCreatedNotification{
		OrderID:   order.OrderID,
		ProductID: order.ProductID,
		Amount:    order.Amount,
		Timestamp: order.PlacedAt,
	})
	if err != nil {
		return fmt.Errorf("notification send failed: %w", err)
	}
	return nil
}

// eventsAdapter implements EventsPort by publishing the events the order
// module declares in its own contract package on the event bus.
type eventsAdapter struct{ bus mono.EventBus }

func (a eventsAdapter) OrderPlaced(_ context.Context, order PlacedOrder) error {
	err := contract.OrderCreatedV1.Publish(a.bus, contract.OrderCreatedEvent{
		OrderID:   order.OrderID,
		ProductID: order.ProductID,
		Amount:    order.Amount,
		Timestamp: order.PlacedAt,
	}, nil)
	if err != nil {
		return fmt.Errorf("publish %s failed: %w", contract.OrderCreatedV1.Name, err)
	}
	return nil
}
