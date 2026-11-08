package order

import (
	"context"
	"fmt"

	"github.com/go-monolith/mono"
	inventorycontract "github.com/go-monolith/mono/examples/multi-module/inventory/contract"
	notificationcontract "github.com/go-monolith/mono/examples/multi-module/notification/contract"
	paymentcontract "github.com/go-monolith/mono/examples/multi-module/payment/contract"
)

// The driven adapters: each implements one port of ports.go over the
// provider's contract, using the provider's ServiceContainer received in
// SetDependencyServiceContainer. This is the only place in the module that
// knows the calls go over NATS.

type inventoryAdapter struct{ container mono.ServiceContainer }

func (a inventoryAdapter) CheckStock(ctx context.Context, req inventorycontract.CheckStockRequest) (inventorycontract.CheckStockResponse, error) {
	resp, err := inventorycontract.CheckStock.Call(ctx, a.container, req)
	if err != nil {
		return inventorycontract.CheckStockResponse{}, fmt.Errorf("inventory check failed: %w", err)
	}
	return resp, nil
}

type paymentAdapter struct{ container mono.ServiceContainer }

func (a paymentAdapter) Process(ctx context.Context, req paymentcontract.ProcessPaymentRequest) (paymentcontract.ProcessPaymentResponse, error) {
	resp, err := paymentcontract.Process.Call(ctx, a.container, req)
	if err != nil {
		return paymentcontract.ProcessPaymentResponse{}, fmt.Errorf("payment processing failed: %w", err)
	}
	return resp, nil
}

type notificationAdapter struct{ container mono.ServiceContainer }

func (a notificationAdapter) OrderCreated(ctx context.Context, notification notificationcontract.OrderCreatedNotification) error {
	if err := notificationcontract.OnOrderCreated.Send(ctx, a.container, notification); err != nil {
		return fmt.Errorf("notification send failed: %w", err)
	}
	return nil
}
