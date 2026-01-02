package analytics

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-monolith/mono/v1"
	"github.com/go-monolith/mono/v1/pkg/helper"
)

type analyticsAdapter struct {
	container mono.ServiceContainer
}

func NewAnalyticsAdapter(container mono.ServiceContainer) AnalyticsAdapterPort {
	if container == nil {
		panic("analytics adapter requires non-nil ServiceContainer")
	}
	return &analyticsAdapter{
		container: container,
	}
}

func (a *analyticsAdapter) GetEvent(ctx context.Context, req *GetEventRequest) (*GetEventResponse, error) {
	// Call request-reply service using helper
	var resp GetEventResponse
	if err := helper.CallRequestReplyService(ctx, a.container, "get-event", json.Marshal, json.Unmarshal, req, &resp); err != nil {
		return nil, fmt.Errorf("get event failed: %w", err)
	}

	return &resp, nil
}
