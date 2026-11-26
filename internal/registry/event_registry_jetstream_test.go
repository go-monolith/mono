package registry

import (
	"context"
	"errors"
	"testing"

	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
)

func TestRegisterEventStreamConsumer_JetStreamAvailability(t *testing.T) {
	eventDef := types.BaseEventDefinition{
		ModuleName: "order",
		Name:       "OrderCreated",
		Subject:    "events.orders.v1.created",
		Version:    "v1",
	}
	config := types.StreamConsumerConfig{Stream: types.StreamConfig{Name: "ORDERS"}}
	handler := func(context.Context, []*types.Msg) error { return nil }

	tests := []struct {
		name    string
		set     func(r *eventRegistry)
		wantErr bool
	}{
		{name: "disabled", set: func(r *eventRegistry) { r.SetJetStreamEnabled(false) }, wantErr: true},
		{name: "enabled", set: func(r *eventRegistry) { r.SetJetStreamEnabled(true) }},
		{name: "unknown", set: func(*eventRegistry) {}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewEventRegistry(&mockLogger{}).(*eventRegistry)
			tt.set(r)
			err := r.RegisterEventStreamConsumer(eventDef, config, handler, &mockModule{name: "billing"})
			if tt.wantErr {
				if !errors.Is(err, monoerrors.ErrJetStreamNotEnabled) {
					t.Fatalf("err = %v, want ErrJetStreamNotEnabled", err)
				}
				if n := len(r.StreamConsumerEntries()); n != 0 {
					t.Errorf("a rejected consumer was stored (%d entries)", n)
				}
				return
			}
			if err != nil {
				t.Fatalf("RegisterEventStreamConsumer failed: %v", err)
			}
		})
	}
}

// TestRegisterEventStreamConsumer_JetStreamSurvivesRestore checks that a
// checkpoint restore keeps the JetStream setting.
func TestRegisterEventStreamConsumer_JetStreamSurvivesRestore(t *testing.T) {
	r := NewEventRegistry(&mockLogger{}).(*eventRegistry)
	restore := r.Checkpoint()
	r.SetJetStreamEnabled(false)
	restore()
	err := r.RegisterEventStreamConsumer(types.BaseEventDefinition{
		ModuleName: "order", Name: "OrderCreated", Subject: "events.orders.v1.created", Version: "v1",
	}, types.StreamConsumerConfig{Stream: types.StreamConfig{Name: "ORDERS"}},
		func(context.Context, []*types.Msg) error { return nil }, &mockModule{name: "billing"})
	if !errors.Is(err, monoerrors.ErrJetStreamNotEnabled) {
		t.Fatalf("err = %v, want ErrJetStreamNotEnabled", err)
	}
}
