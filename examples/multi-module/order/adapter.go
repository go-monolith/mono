package order

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-monolith/mono/v1"
	"github.com/go-monolith/mono/v1/pkg/helper"
)

type orderAdapter struct {
	container mono.ServiceContainer
}

func NewOrderAdapter(container mono.ServiceContainer) OrderAdapterPort {
	if container == nil {
		panic("order adapter requires non-nil ServiceContainer")
	}
	return &orderAdapter{
		container: container,
	}
}

func (o *orderAdapter) PlaceOrder(ctx context.Context, req *CreateOrderRequest) (*CreateOrderResponse, error) {
	var resp CreateOrderResponse
	if err := helper.CallRequestReplyService(ctx, o.container, "place-order", json.Marshal, json.Unmarshal, req, &resp); err != nil {
		return nil, fmt.Errorf("failed to place order: %w", err)
	}
	return &resp, nil
}
