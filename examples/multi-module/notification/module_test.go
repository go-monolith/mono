package notification_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-monolith/mono/examples/multi-module/notification"
	"github.com/go-monolith/mono/examples/multi-module/notification/contract"
	ordercontract "github.com/go-monolith/mono/examples/multi-module/order/contract"
	"github.com/go-monolith/mono/monotest"
)

func TestNotificationModuleConformance(t *testing.T) {
	monotest.CheckModule(t, notification.NewModule(),
		monotest.WithExercise(func(ctx context.Context, h *monotest.Harness) error {
			return contract.OnOrderCreated.Send(ctx, h.Container(), contract.OrderCreatedNotification{OrderID: "o1", ProductID: "laptop", Amount: 1})
		}))
}

func TestNotificationConsumesOrderCreatedEvent(t *testing.T) {
	m := notification.NewModule()
	h := monotest.New(t, m)
	if err := h.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = h.Stop(context.Background()) }()

	// The event is delivered on the in-memory bus to the typed consumer the
	// module registered in RegisterEventConsumers.
	if err := ordercontract.OrderCreatedV1.Publish(h.EventBus(), ordercontract.OrderCreatedEvent{OrderID: "o1", ProductID: "mouse", Amount: 2}, nil); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if logs := m.GetLogs(); len(logs) == 1 && logs[0].Channel == "event" && logs[0].NotificationID == "o1" {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("event not consumed, logs: %+v", m.GetLogs())
}
