package notification

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-monolith/mono"
	"github.com/go-monolith/mono/pkg/helper"
)

type notificationAdapter struct {
	container mono.ServiceContainer
}

func NewNotificationAdapter(container mono.ServiceContainer) NotificationAdapterPort {
	if container == nil {
		panic("notification adapter requires non-nil ServiceContainer")
	}
	return &notificationAdapter{
		container: container,
	}
}

func (n *notificationAdapter) SendOnOrderCreatedNotification(ctx context.Context, notification *OrderCreatedNotification) error {
	// Send via queue group using helper (fire-and-forget)
	if err := helper.SendQueueGroupService(ctx, n.container, "on-order-created", json.Marshal, notification); err != nil {
		return fmt.Errorf("notification send failed: %w", err)
	}

	return nil
}
