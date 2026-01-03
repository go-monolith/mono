package payment

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-monolith/mono"
	"github.com/go-monolith/mono/pkg/helper"
)

type paymentAdapter struct {
	container mono.ServiceContainer
}

func NewPaymentAdapter(container mono.ServiceContainer) PaymentAdapterPort {
	if container == nil {
		panic("payment adapter requires non-nil ServiceContainer")
	}
	return &paymentAdapter{
		container: container,
	}
}

func (p *paymentAdapter) Process(ctx context.Context, req *ProcessPaymentRequest) (*ProcessPaymentResponse, error) {
	// Call request-reply service using helper
	var resp ProcessPaymentResponse
	if err := helper.CallRequestReplyService(ctx, p.container, "process", json.Marshal, json.Unmarshal, req, &resp); err != nil {
		return nil, fmt.Errorf("payment processing failed: %w", err)
	}

	return &resp, nil
}
