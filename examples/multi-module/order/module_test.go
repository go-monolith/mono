package order_test

import (
	"context"
	"testing"

	inventorycontract "github.com/go-monolith/mono/examples/multi-module/inventory/contract"
	notificationcontract "github.com/go-monolith/mono/examples/multi-module/notification/contract"
	"github.com/go-monolith/mono/examples/multi-module/order"
	"github.com/go-monolith/mono/examples/multi-module/order/contract"
	paymentcontract "github.com/go-monolith/mono/examples/multi-module/payment/contract"
	"github.com/go-monolith/mono/monotest"
	"github.com/go-monolith/mono/pkg/types"
)

// stubs builds stand-in containers for the three dependencies of the order
// module, registering the services through the providers' own contracts.
func stubs(t *testing.T, available bool, paymentOK bool) (*monotest.Container, *monotest.Container, *monotest.Container) {
	t.Helper()
	bus := monotest.NewEventBus()
	inventory := monotest.NewContainer("inventory", bus)
	payment := monotest.NewContainer("payment", bus)
	notification := monotest.NewContainer("notification", bus)

	if err := inventorycontract.CheckStock.Register(inventory, func(_ context.Context, req inventorycontract.CheckStockRequest, _ *types.Msg) (inventorycontract.CheckStockResponse, error) {
		return inventorycontract.CheckStockResponse{Available: available, Stock: 5}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := paymentcontract.Process.Register(payment, func(_ context.Context, req paymentcontract.ProcessPaymentRequest, _ *types.Msg) (paymentcontract.ProcessPaymentResponse, error) {
		return paymentcontract.ProcessPaymentResponse{Success: paymentOK, TransactionID: "txn"}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := notificationcontract.OnOrderCreated.Register(notification, types.TypedQGHP[notificationcontract.OrderCreatedNotification]{
		QueueGroup: "notification-workers",
		Handler:    func(context.Context, notificationcontract.OrderCreatedNotification, *types.Msg) error { return nil },
	}); err != nil {
		t.Fatal(err)
	}
	return inventory, payment, notification
}

func TestOrderModuleConformance(t *testing.T) {
	inventory, payment, notification := stubs(t, true, true)
	monotest.CheckModule(t, order.NewModule(),
		monotest.WithDependency("inventory", inventory),
		monotest.WithDependency("payment", payment),
		monotest.WithDependency("notification", notification),
		monotest.WithExercise(func(ctx context.Context, h *monotest.Harness) error {
			resp, err := contract.PlaceOrder.Call(ctx, h.Container(), contract.CreateOrderRequest{ProductID: "laptop", Quantity: 1, Amount: 10})
			if err != nil {
				return err
			}
			if resp.Status != "success" {
				t.Errorf("status = %q", resp.Status)
			}
			return nil
		}))
}

func TestOrderOutOfStockAndPaymentDeclined(t *testing.T) {
	cases := []struct {
		name               string
		available, payment bool
		wantStatus         string
	}{
		{"out of stock", false, true, "failed_out_of_stock"},
		{"payment declined", true, false, "payment_failed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inventory, payment, notification := stubs(t, tc.available, tc.payment)
			h := monotest.New(t, order.NewModule(),
				monotest.WithDependency("inventory", inventory),
				monotest.WithDependency("payment", payment),
				monotest.WithDependency("notification", notification))
			if err := h.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = h.Stop(context.Background()) }()

			resp := monotest.Call[contract.CreateOrderRequest, contract.CreateOrderResponse](t, h, "place-order",
				contract.CreateOrderRequest{ProductID: "laptop", Quantity: 1, Amount: 10})
			if resp.Status != tc.wantStatus {
				t.Errorf("status = %q, want %q", resp.Status, tc.wantStatus)
			}
			// Publishes are recorded synchronously, so this is a reliable
			// "nothing was sent" check even though handlers run asynchronously.
			for _, subject := range notification.EventBus().Published() {
				if subject == "services.notification.on-order-created" {
					t.Error("a failed order must not notify")
				}
			}
		})
	}
}
