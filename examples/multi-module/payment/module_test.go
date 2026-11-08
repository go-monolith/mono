package payment_test

import (
	"context"
	"testing"

	"github.com/go-monolith/mono/examples/multi-module/payment"
	"github.com/go-monolith/mono/examples/multi-module/payment/contract"
	"github.com/go-monolith/mono/monotest"
)

func TestPaymentModuleConformance(t *testing.T) {
	monotest.CheckModule(t, payment.NewModule(),
		monotest.WithExercise(func(ctx context.Context, h *monotest.Harness) error {
			_, err := contract.Process.Call(ctx, h.Container(), contract.ProcessPaymentRequest{OrderID: "o1", Amount: 10, Currency: "USD"})
			return err
		}))
}

func TestPaymentRejectsUnsupportedCurrency(t *testing.T) {
	h := monotest.New(t, payment.NewModule())
	if err := h.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = h.Stop(context.Background()) }()
	if _, err := contract.Process.Call(context.Background(), h.Container(), contract.ProcessPaymentRequest{OrderID: "o1", Amount: 10, Currency: "XXX"}); err == nil {
		t.Fatal("expected an error for an unsupported currency")
	}
}
