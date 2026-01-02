package inventory

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-monolith/mono/v1"
	"github.com/go-monolith/mono/v1/pkg/helper"
)

type inventoryAdapter struct {
	container mono.ServiceContainer
}

func NewInventoryAdapter(container mono.ServiceContainer) InventoryAdapterPort {
	if container == nil {
		panic("inventory adapter requires non-nil ServiceContainer")
	}
	return &inventoryAdapter{
		container: container,
	}
}

func (i *inventoryAdapter) CheckStock(ctx context.Context, req *CheckStockRequest) (*CheckStockResponse, error) {
	// Call request-reply service using helper
	var resp CheckStockResponse
	if err := helper.CallRequestReplyService(ctx, i.container, "check-stock", json.Marshal, json.Unmarshal, req, &resp); err != nil {
		return nil, fmt.Errorf("inventory check failed: %w", err)
	}

	return &resp, nil
}
