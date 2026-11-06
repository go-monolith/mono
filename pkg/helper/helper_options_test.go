package helper_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/go-monolith/mono/pkg/helper"
	"github.com/go-monolith/mono/pkg/types"
)

func TestRegisterTypedEventConsumerWithOptions(t *testing.T) {
	t.Run("passes options through and unmarshals events", func(t *testing.T) {
		registry := newMockEventRegistry()
		def := helper.EventDefinition[testRequest]("order", "OrderCreated", "v1")
		module := &mockModule{}

		var received testRequest
		handler := func(_ context.Context, event testRequest, _ *types.Msg) error {
			received = event
			return nil
		}
		opts := types.EventConsumerOptions{
			QueueGroup:     "projector",
			ServiceOptions: types.ServiceOptions{MaxConcurrency: 1, MaxPending: 10},
		}
		if err := helper.RegisterTypedEventConsumerWithOptions(registry, def, handler, module, opts); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		subject := def.ToBase().Subject
		if got := registry.registeredConsumerOptions[subject]; got != opts {
			t.Fatalf("options = %+v, want %+v", got, opts)
		}
		wrapped, ok := registry.registeredConsumers[subject]
		if !ok {
			t.Fatal("handler not registered")
		}
		data, _ := json.Marshal(testRequest{Name: "n", Value: 7})
		if err := wrapped(context.Background(), &types.Msg{Data: data}); err != nil {
			t.Fatalf("wrapped handler: %v", err)
		}
		if received.Name != "n" || received.Value != 7 {
			t.Fatalf("received = %+v", received)
		}
	})

	t.Run("unmarshal error is returned", func(t *testing.T) {
		registry := newMockEventRegistry()
		def := helper.EventDefinition[testRequest]("order", "OrderCreated", "v1")
		handler := func(_ context.Context, _ testRequest, _ *types.Msg) error { return nil }
		if err := helper.RegisterTypedEventConsumerWithOptions(registry, def, handler, &mockModule{}, types.EventConsumerOptions{}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		wrapped := registry.registeredConsumers[def.ToBase().Subject]
		if err := wrapped(context.Background(), &types.Msg{Data: []byte("not json")}); err == nil {
			t.Fatal("expected unmarshal error")
		}
	})

	t.Run("registry error is returned", func(t *testing.T) {
		registry := newMockEventRegistry()
		registry.returnError = errors.New("registry failed")
		def := helper.EventDefinition[testRequest]("order", "OrderCreated", "v1")
		handler := func(_ context.Context, _ testRequest, _ *types.Msg) error { return nil }
		err := helper.RegisterTypedEventConsumerWithOptions(registry, def, handler, &mockModule{}, types.EventConsumerOptions{})
		if err == nil || err.Error() != "registry failed" {
			t.Fatalf("got %v, want registry failed", err)
		}
	})
}

func TestRegisterTypedQueueGroupService_CarriesOptions(t *testing.T) {
	mock := &mockServiceContainer{}
	handler := func(_ context.Context, _ testRequest, _ *types.Msg) error { return nil }
	err := helper.RegisterTypedQueueGroupService(mock, "notify", json.Unmarshal,
		types.TypedQGHP[testRequest]{QueueGroup: "ordered", Handler: handler, Options: types.ServiceOptions{MaxConcurrency: 1}},
		types.TypedQGHP[testRequest]{QueueGroup: "parallel", Handler: handler},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(mock.queueGroupPairs) != 2 {
		t.Fatalf("expected 2 pairs, got %d", len(mock.queueGroupPairs))
	}
	if mock.queueGroupPairs[0].Options.MaxConcurrency != 1 {
		t.Errorf("pair 0 MaxConcurrency = %d, want 1", mock.queueGroupPairs[0].Options.MaxConcurrency)
	}
	if mock.queueGroupPairs[1].Options != (types.ServiceOptions{}) {
		t.Errorf("pair 1 Options = %+v, want zero value", mock.queueGroupPairs[1].Options)
	}
}
