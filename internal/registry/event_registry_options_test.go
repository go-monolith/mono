package registry

import (
	"context"
	"testing"

	"github.com/go-monolith/mono/pkg/types"
)

func TestRegisterEventConsumerWithOptions(t *testing.T) {
	eventDef := types.BaseEventDefinition{
		ModuleName: "order",
		Name:       "OrderCreated",
		Subject:    "events.orders.v1.created",
		Version:    "v1",
	}
	module := &mockModule{name: "notification"}
	handler := func(_ context.Context, _ *types.Msg) error { return nil }

	t.Run("options and queue group are stored", func(t *testing.T) {
		registry := NewEventRegistry(&mockLogger{})
		err := registry.RegisterEventConsumerWithOptions(eventDef, handler, module, types.EventConsumerOptions{
			QueueGroup:     "projector",
			ServiceOptions: types.ServiceOptions{MaxConcurrency: 1, MaxPending: 50},
		})
		if err != nil {
			t.Fatalf("RegisterEventConsumerWithOptions failed: %v", err)
		}
		entries := registry.Entries()
		if len(entries) != 1 {
			t.Fatalf("expected 1 entry, got %d", len(entries))
		}
		if entries[0].QueueGroup != "projector" {
			t.Errorf("QueueGroup = %q, want projector", entries[0].QueueGroup)
		}
		if entries[0].Options.MaxConcurrency != 1 || entries[0].Options.MaxPending != 50 {
			t.Errorf("Options = %+v, want {1 50}", entries[0].Options)
		}
	})

	t.Run("empty queue group defaults to the module name", func(t *testing.T) {
		registry := NewEventRegistry(&mockLogger{})
		err := registry.RegisterEventConsumerWithOptions(eventDef, handler, module, types.EventConsumerOptions{})
		if err != nil {
			t.Fatalf("RegisterEventConsumerWithOptions failed: %v", err)
		}
		if got := registry.Entries()[0].QueueGroup; got != "notification" {
			t.Errorf("QueueGroup = %q, want notification", got)
		}
	})

	t.Run("negative options are rejected", func(t *testing.T) {
		registry := NewEventRegistry(&mockLogger{})
		err := registry.RegisterEventConsumerWithOptions(eventDef, handler, module, types.EventConsumerOptions{
			ServiceOptions: types.ServiceOptions{MaxConcurrency: -1},
		})
		if err == nil {
			t.Fatal("expected an error for MaxConcurrency -1")
		}
		if len(registry.Entries()) != 0 {
			t.Fatal("invalid registration must not be stored")
		}
	})

	t.Run("validation matches RegisterEventConsumer", func(t *testing.T) {
		registry := NewEventRegistry(&mockLogger{})
		if err := registry.RegisterEventConsumerWithOptions(eventDef, nil, module, types.EventConsumerOptions{}); err == nil {
			t.Error("expected error for nil handler")
		}
		if err := registry.RegisterEventConsumerWithOptions(eventDef, handler, nil, types.EventConsumerOptions{}); err == nil {
			t.Error("expected error for nil module")
		}
		noSubject := eventDef
		noSubject.Subject = ""
		if err := registry.RegisterEventConsumerWithOptions(noSubject, handler, module, types.EventConsumerOptions{}); err == nil {
			t.Error("expected error for empty subject")
		}
	})

	t.Run("RegisterEventConsumer leaves options at zero", func(t *testing.T) {
		registry := NewEventRegistry(&mockLogger{})
		if err := registry.RegisterEventConsumer(eventDef, handler, module, "custom", "ignored"); err != nil {
			t.Fatalf("RegisterEventConsumer failed: %v", err)
		}
		entry := registry.Entries()[0]
		if entry.QueueGroup != "custom" {
			t.Errorf("QueueGroup = %q, want custom", entry.QueueGroup)
		}
		if entry.Options != (types.ServiceOptions{}) {
			t.Errorf("Options = %+v, want zero value", entry.Options)
		}
	})
}
