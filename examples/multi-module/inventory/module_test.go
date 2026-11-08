package inventory_test

import (
	"context"
	"testing"

	"github.com/go-monolith/mono/examples/multi-module/inventory"
	"github.com/go-monolith/mono/examples/multi-module/inventory/contract"
	"github.com/go-monolith/mono/monotest"
)

// TestInventoryModuleConformance checks the Ports and Adapters rules the
// framework can observe: valid name, services registered through the contract,
// no boundary crossings.
func TestInventoryModuleConformance(t *testing.T) {
	monotest.CheckModule(t, inventory.NewModule(),
		monotest.WithExercise(func(ctx context.Context, h *monotest.Harness) error {
			resp, err := contract.CheckStock.Call(ctx, h.Container(), contract.CheckStockRequest{ProductID: "laptop", Quantity: 1})
			if err != nil {
				return err
			}
			if !resp.Available || resp.Stock != 10 {
				t.Errorf("unexpected response: %+v", resp)
			}
			return nil
		}))
}

func TestInventoryCheckStockValidation(t *testing.T) {
	h := monotest.New(t, inventory.NewModule())
	if err := h.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = h.Stop(context.Background()) }()

	_, err := contract.CheckStock.Call(context.Background(), h.Container(), contract.CheckStockRequest{ProductID: "", Quantity: 1})
	if err == nil {
		t.Fatal("expected a validation error for an empty product id")
	}
	resp, err := contract.CheckStock.Call(context.Background(), h.Container(), contract.CheckStockRequest{ProductID: "unknown", Quantity: 1})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Available || resp.Stock != 0 {
		t.Errorf("unknown product must be unavailable: %+v", resp)
	}
}
