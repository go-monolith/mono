package middleware

import (
	"context"
	"testing"

	"github.com/go-monolith/mono/v1/pkg/types"
)

func TestNewChain(t *testing.T) {
	t.Run("creates empty chain", func(t *testing.T) {
		chain := NewChain([]types.MiddlewareModule{})
		if chain == nil {
			t.Fatal("expected non-nil chain")
		}

		// Verify it implements the interface
		var _ types.MiddlewareChainRunner = chain

		if chain.Len() != 0 {
			t.Errorf("expected empty chain, got %d middleware", chain.Len())
		}
	})

	t.Run("creates chain with middleware", func(t *testing.T) {
		mw := &testMiddleware{name: "test"}
		chain := NewChain([]types.MiddlewareModule{mw})

		if chain.Len() != 1 {
			t.Errorf("expected 1 middleware, got %d", chain.Len())
		}

		middlewares := chain.Middlewares()
		if middlewares[0] != mw {
			t.Error("middleware not added correctly")
		}
	})

	t.Run("creates chain with multiple middleware in order", func(t *testing.T) {
		mw1 := &testMiddleware{name: "mw1"}
		mw2 := &testMiddleware{name: "mw2"}
		mw3 := &testMiddleware{name: "mw3"}

		chain := NewChain([]types.MiddlewareModule{mw1, mw2, mw3})

		if chain.Len() != 3 {
			t.Errorf("expected 3 middleware, got %d", chain.Len())
		}

		middlewares := chain.Middlewares()
		if middlewares[0].Name() != "mw1" {
			t.Errorf("expected mw1 at position 0, got %s", middlewares[0].Name())
		}
		if middlewares[1].Name() != "mw2" {
			t.Errorf("expected mw2 at position 1, got %s", middlewares[1].Name())
		}
		if middlewares[2].Name() != "mw3" {
			t.Errorf("expected mw3 at position 2, got %s", middlewares[2].Name())
		}
	})
}

func TestChainRunModuleLifecycle(t *testing.T) {
	t.Run("runs all middleware in order", func(t *testing.T) {
		var executionOrder []string

		chain := NewChain([]types.MiddlewareModule{
			&testMiddleware{
				name: "mw1",
				onModuleLifecycle: func(ctx context.Context, event types.ModuleLifecycleEvent) types.ModuleLifecycleEvent {
					executionOrder = append(executionOrder, "mw1")
					return event
				},
			},
			&testMiddleware{
				name: "mw2",
				onModuleLifecycle: func(ctx context.Context, event types.ModuleLifecycleEvent) types.ModuleLifecycleEvent {
					executionOrder = append(executionOrder, "mw2")
					return event
				},
			},
		})

		event := types.ModuleLifecycleEvent{
			Type:       types.ModuleStartedEvent,
			ModuleName: "test",
		}

		chain.RunModuleLifecycle(context.Background(), event)

		if len(executionOrder) != 2 {
			t.Errorf("expected 2 executions, got %d", len(executionOrder))
		}
		if executionOrder[0] != "mw1" || executionOrder[1] != "mw2" {
			t.Errorf("wrong execution order: %v", executionOrder)
		}
	})

	t.Run("passes modified event through chain", func(t *testing.T) {
		chain := NewChain([]types.MiddlewareModule{
			&testMiddleware{
				name: "mw1",
				onModuleLifecycle: func(ctx context.Context, event types.ModuleLifecycleEvent) types.ModuleLifecycleEvent {
					event.ModuleName = event.ModuleName + "-modified1"
					return event
				},
			},
			&testMiddleware{
				name: "mw2",
				onModuleLifecycle: func(ctx context.Context, event types.ModuleLifecycleEvent) types.ModuleLifecycleEvent {
					event.ModuleName = event.ModuleName + "-modified2"
					return event
				},
			},
		})

		event := types.ModuleLifecycleEvent{
			Type:       types.ModuleStartedEvent,
			ModuleName: "test",
		}

		result := chain.RunModuleLifecycle(context.Background(), event)

		if result.ModuleName != "test-modified1-modified2" {
			t.Errorf("expected 'test-modified1-modified2', got %q", result.ModuleName)
		}
	})
}

func TestChainRunServiceRegistration(t *testing.T) {
	t.Run("runs all middleware in order", func(t *testing.T) {
		var executionOrder []string

		chain := NewChain([]types.MiddlewareModule{
			&testMiddleware{
				name: "mw1",
				onServiceRegistration: func(ctx context.Context, reg types.ServiceRegistration) types.ServiceRegistration {
					executionOrder = append(executionOrder, "mw1")
					return reg
				},
			},
			&testMiddleware{
				name: "mw2",
				onServiceRegistration: func(ctx context.Context, reg types.ServiceRegistration) types.ServiceRegistration {
					executionOrder = append(executionOrder, "mw2")
					return reg
				},
			},
		})

		reg := types.ServiceRegistration{
			Type: types.ServiceTypeRequestReply,
		}

		chain.RunServiceRegistration(context.Background(), reg)

		if len(executionOrder) != 2 {
			t.Errorf("expected 2 executions, got %d", len(executionOrder))
		}
		if executionOrder[0] != "mw1" || executionOrder[1] != "mw2" {
			t.Errorf("wrong execution order: %v", executionOrder)
		}
	})
}

func TestChainRunConfigurationChange(t *testing.T) {
	t.Run("runs all middleware in order", func(t *testing.T) {
		var executionOrder []string

		chain := NewChain([]types.MiddlewareModule{
			&testMiddleware{
				name: "mw1",
				onConfigurationChange: func(ctx context.Context, event types.ConfigurationEvent) types.ConfigurationEvent {
					executionOrder = append(executionOrder, "mw1")
					return event
				},
			},
			&testMiddleware{
				name: "mw2",
				onConfigurationChange: func(ctx context.Context, event types.ConfigurationEvent) types.ConfigurationEvent {
					executionOrder = append(executionOrder, "mw2")
					return event
				},
			},
		})

		event := types.ConfigurationEvent{
			Type:       types.ConfigurationUpdatedEvent,
			OptionName: "test-key",
			NewValue:   "test-value",
		}

		chain.RunConfigurationChange(context.Background(), event)

		if len(executionOrder) != 2 {
			t.Errorf("expected 2 executions, got %d", len(executionOrder))
		}
		if executionOrder[0] != "mw1" || executionOrder[1] != "mw2" {
			t.Errorf("wrong execution order: %v", executionOrder)
		}
	})
}

func TestChainRunOutgoingMessage(t *testing.T) {
	t.Run("runs all middleware in order", func(t *testing.T) {
		var executionOrder []string

		chain := NewChain([]types.MiddlewareModule{
			&testMiddleware{
				name: "mw1",
				onOutgoingMessage: func(octx types.OutgoingMessageContext) types.OutgoingMessageContext {
					executionOrder = append(executionOrder, "mw1")
					return octx
				},
			},
			&testMiddleware{
				name: "mw2",
				onOutgoingMessage: func(octx types.OutgoingMessageContext) types.OutgoingMessageContext {
					executionOrder = append(executionOrder, "mw2")
					return octx
				},
			},
		})

		octx := types.OutgoingMessageContext{
			ServiceType: types.ServiceTypeRequestReply,
			ServiceName: "test",
			ModuleName:  "test-module",
			Subject:     "test.subject",
			Msg:         &types.Msg{Data: []byte("test")},
			Ctx:         context.Background(),
			Metadata:    make(map[string]any),
		}

		chain.RunOutgoingMessage(octx)

		if len(executionOrder) != 2 {
			t.Errorf("expected 2 executions, got %d", len(executionOrder))
		}
		if executionOrder[0] != "mw1" || executionOrder[1] != "mw2" {
			t.Errorf("wrong execution order: %v", executionOrder)
		}
	})

	t.Run("passes modified context through chain", func(t *testing.T) {
		chain := NewChain([]types.MiddlewareModule{
			&testMiddleware{
				name: "mw1",
				onOutgoingMessage: func(octx types.OutgoingMessageContext) types.OutgoingMessageContext {
					if octx.Msg.Header == nil {
						octx.Msg.Header = make(types.Header)
					}
					octx.Msg.Header["X-MW1"] = []string{"value1"}
					return octx
				},
			},
			&testMiddleware{
				name: "mw2",
				onOutgoingMessage: func(octx types.OutgoingMessageContext) types.OutgoingMessageContext {
					if octx.Msg.Header == nil {
						octx.Msg.Header = make(types.Header)
					}
					octx.Msg.Header["X-MW2"] = []string{"value2"}
					return octx
				},
			},
		})

		msg := &types.Msg{
			Data:   []byte("test"),
			Header: make(types.Header),
		}

		octx := types.OutgoingMessageContext{
			ServiceType: types.ServiceTypeRequestReply,
			ServiceName: "test",
			ModuleName:  "test-module",
			Subject:     "test.subject",
			Msg:         msg,
			Ctx:         context.Background(),
			Metadata:    make(map[string]any),
		}

		result := chain.RunOutgoingMessage(octx)

		if len(result.Msg.Header["X-MW1"]) == 0 {
			t.Error("expected X-MW1 header to be added")
		}
		if result.Msg.Header["X-MW1"][0] != "value1" {
			t.Errorf("expected X-MW1='value1', got %q", result.Msg.Header["X-MW1"][0])
		}

		if len(result.Msg.Header["X-MW2"]) == 0 {
			t.Error("expected X-MW2 header to be added")
		}
		if result.Msg.Header["X-MW2"][0] != "value2" {
			t.Errorf("expected X-MW2='value2', got %q", result.Msg.Header["X-MW2"][0])
		}
	})

	t.Run("empty chain returns unchanged context", func(t *testing.T) {
		chain := NewChain([]types.MiddlewareModule{})

		msg := &types.Msg{
			Data:   []byte("test"),
			Header: types.Header{"X-Original": []string{"value"}},
		}

		octx := types.OutgoingMessageContext{
			ServiceType: types.ServiceTypeRequestReply,
			ServiceName: "test",
			ModuleName:  "test-module",
			Subject:     "test.subject",
			Msg:         msg,
			Ctx:         context.Background(),
			Metadata:    make(map[string]any),
		}

		result := chain.RunOutgoingMessage(octx)

		if result.Msg != msg {
			t.Error("expected message to be unchanged")
		}

		if len(result.Msg.Header) != 1 {
			t.Errorf("expected 1 header, got %d", len(result.Msg.Header))
		}

		if result.Msg.Header["X-Original"][0] != "value" {
			t.Errorf("expected original header value, got %q", result.Msg.Header["X-Original"][0])
		}
	})
}

func TestChainRunEventConsumerRegistration(t *testing.T) {
	t.Run("runs all middleware in order", func(t *testing.T) {
		var executionOrder []string

		chain := NewChain([]types.MiddlewareModule{
			&testMiddleware{
				name: "mw1",
				onEventConsumerRegistration: func(ctx context.Context, entry types.EventConsumerEntry) types.EventConsumerEntry {
					executionOrder = append(executionOrder, "mw1")
					return entry
				},
			},
			&testMiddleware{
				name: "mw2",
				onEventConsumerRegistration: func(ctx context.Context, entry types.EventConsumerEntry) types.EventConsumerEntry {
					executionOrder = append(executionOrder, "mw2")
					return entry
				},
			},
		})

		entry := types.EventConsumerEntry{
			EventDef: types.BaseEventDefinition{
				ModuleName: "order",
				Name:       "OrderCreated",
				Subject:    "events.orders.v1.created",
				Version:    "v1",
			},
			Module:     &testModule{name: "notification"},
			QueueGroup: "notification",
		}

		chain.RunEventConsumerRegistration(context.Background(), entry)

		if len(executionOrder) != 2 {
			t.Errorf("expected 2 executions, got %d", len(executionOrder))
		}
		if executionOrder[0] != "mw1" || executionOrder[1] != "mw2" {
			t.Errorf("wrong execution order: %v", executionOrder)
		}
	})

	t.Run("passes modified entry through chain", func(t *testing.T) {
		chain := NewChain([]types.MiddlewareModule{
			&testMiddleware{
				name: "mw1",
				onEventConsumerRegistration: func(ctx context.Context, entry types.EventConsumerEntry) types.EventConsumerEntry {
					entry.QueueGroup = entry.QueueGroup + "-modified1"
					return entry
				},
			},
			&testMiddleware{
				name: "mw2",
				onEventConsumerRegistration: func(ctx context.Context, entry types.EventConsumerEntry) types.EventConsumerEntry {
					entry.QueueGroup = entry.QueueGroup + "-modified2"
					return entry
				},
			},
		})

		entry := types.EventConsumerEntry{
			EventDef: types.BaseEventDefinition{
				ModuleName: "order",
				Name:       "OrderCreated",
				Subject:    "events.orders.v1.created",
				Version:    "v1",
			},
			Module:     &testModule{name: "notification"},
			QueueGroup: "notification",
		}

		result := chain.RunEventConsumerRegistration(context.Background(), entry)

		if result.QueueGroup != "notification-modified1-modified2" {
			t.Errorf("expected 'notification-modified1-modified2', got %q", result.QueueGroup)
		}
	})

	t.Run("wraps handler through chain", func(t *testing.T) {
		var handlerCalled bool
		var wrappersCalled []string

		originalHandler := func(ctx context.Context, msg *types.Msg) error {
			handlerCalled = true
			return nil
		}

		chain := NewChain([]types.MiddlewareModule{
			&testMiddleware{
				name: "mw1",
				onEventConsumerRegistration: func(ctx context.Context, entry types.EventConsumerEntry) types.EventConsumerEntry {
					original := entry.Handler
					entry.Handler = func(ctx context.Context, msg *types.Msg) error {
						wrappersCalled = append(wrappersCalled, "mw1-before")
						err := original(ctx, msg)
						wrappersCalled = append(wrappersCalled, "mw1-after")
						return err
					}
					return entry
				},
			},
			&testMiddleware{
				name: "mw2",
				onEventConsumerRegistration: func(ctx context.Context, entry types.EventConsumerEntry) types.EventConsumerEntry {
					original := entry.Handler
					entry.Handler = func(ctx context.Context, msg *types.Msg) error {
						wrappersCalled = append(wrappersCalled, "mw2-before")
						err := original(ctx, msg)
						wrappersCalled = append(wrappersCalled, "mw2-after")
						return err
					}
					return entry
				},
			},
		})

		entry := types.EventConsumerEntry{
			EventDef: types.BaseEventDefinition{
				ModuleName: "order",
				Name:       "OrderCreated",
				Subject:    "events.orders.v1.created",
				Version:    "v1",
			},
			Handler:    originalHandler,
			Module:     &testModule{name: "notification"},
			QueueGroup: "notification",
		}

		result := chain.RunEventConsumerRegistration(context.Background(), entry)

		// Call the wrapped handler
		err := result.Handler(context.Background(), &types.Msg{Data: []byte("test")})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !handlerCalled {
			t.Error("expected original handler to be called")
		}

		// mw2 wraps mw1's wrapper, so mw2 runs first (outer), then mw1 (inner)
		expected := []string{"mw2-before", "mw1-before", "mw1-after", "mw2-after"}
		if len(wrappersCalled) != len(expected) {
			t.Errorf("expected %d wrapper calls, got %d: %v", len(expected), len(wrappersCalled), wrappersCalled)
		} else {
			for i, exp := range expected {
				if wrappersCalled[i] != exp {
					t.Errorf("position %d: expected %q, got %q", i, exp, wrappersCalled[i])
				}
			}
		}
	})

	t.Run("empty chain returns unchanged entry", func(t *testing.T) {
		chain := NewChain([]types.MiddlewareModule{})

		var handlerCalled bool
		originalHandler := func(ctx context.Context, msg *types.Msg) error {
			handlerCalled = true
			return nil
		}

		entry := types.EventConsumerEntry{
			EventDef: types.BaseEventDefinition{
				ModuleName: "order",
				Name:       "OrderCreated",
				Subject:    "events.orders.v1.created",
				Version:    "v1",
			},
			Handler:    originalHandler,
			Module:     &testModule{name: "notification"},
			QueueGroup: "notification",
		}

		result := chain.RunEventConsumerRegistration(context.Background(), entry)

		if result.QueueGroup != "notification" {
			t.Errorf("expected unchanged queue group")
		}

		// Handler should still work
		err := result.Handler(context.Background(), &types.Msg{Data: []byte("test")})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !handlerCalled {
			t.Error("expected handler to be called")
		}
	})
}

// Test helper type
type testMiddleware struct {
	name                              string
	onModuleLifecycle                 func(context.Context, types.ModuleLifecycleEvent) types.ModuleLifecycleEvent
	onServiceRegistration             func(context.Context, types.ServiceRegistration) types.ServiceRegistration
	onConfigurationChange             func(context.Context, types.ConfigurationEvent) types.ConfigurationEvent
	onOutgoingMessage                 func(types.OutgoingMessageContext) types.OutgoingMessageContext
	onEventConsumerRegistration       func(context.Context, types.EventConsumerEntry) types.EventConsumerEntry
	onEventStreamConsumerRegistration func(context.Context, types.EventStreamConsumerEntry) types.EventStreamConsumerEntry
}

// testModule is a minimal module implementation for testing
type testModule struct {
	name string
}

func (m *testModule) Name() string                    { return m.name }
func (m *testModule) Start(ctx context.Context) error { return nil }
func (m *testModule) Stop(ctx context.Context) error  { return nil }

func (m *testMiddleware) Name() string {
	return m.name
}

func (m *testMiddleware) Start(ctx context.Context) error {
	return nil
}

func (m *testMiddleware) Stop(ctx context.Context) error {
	return nil
}

func (m *testMiddleware) OnModuleLifecycle(ctx context.Context, event types.ModuleLifecycleEvent) types.ModuleLifecycleEvent {
	if m.onModuleLifecycle != nil {
		return m.onModuleLifecycle(ctx, event)
	}
	return event
}

func (m *testMiddleware) OnServiceRegistration(ctx context.Context, reg types.ServiceRegistration) types.ServiceRegistration {
	if m.onServiceRegistration != nil {
		return m.onServiceRegistration(ctx, reg)
	}
	return reg
}

func (m *testMiddleware) OnConfigurationChange(ctx context.Context, event types.ConfigurationEvent) types.ConfigurationEvent {
	if m.onConfigurationChange != nil {
		return m.onConfigurationChange(ctx, event)
	}
	return event
}

func (m *testMiddleware) OnOutgoingMessage(octx types.OutgoingMessageContext) types.OutgoingMessageContext {
	if m.onOutgoingMessage != nil {
		return m.onOutgoingMessage(octx)
	}
	return octx
}

func (m *testMiddleware) OnEventConsumerRegistration(ctx context.Context, entry types.EventConsumerEntry) types.EventConsumerEntry {
	if m.onEventConsumerRegistration != nil {
		return m.onEventConsumerRegistration(ctx, entry)
	}
	return entry
}

func (m *testMiddleware) OnEventStreamConsumerRegistration(ctx context.Context, entry types.EventStreamConsumerEntry) types.EventStreamConsumerEntry {
	if m.onEventStreamConsumerRegistration != nil {
		return m.onEventStreamConsumerRegistration(ctx, entry)
	}
	return entry
}

func TestChainRunEventStreamConsumerRegistration(t *testing.T) {
	t.Run("runs all middleware in order", func(t *testing.T) {
		var executionOrder []string

		chain := NewChain([]types.MiddlewareModule{
			&testMiddleware{
				name: "mw1",
				onEventStreamConsumerRegistration: func(ctx context.Context, entry types.EventStreamConsumerEntry) types.EventStreamConsumerEntry {
					executionOrder = append(executionOrder, "mw1")
					return entry
				},
			},
			&testMiddleware{
				name: "mw2",
				onEventStreamConsumerRegistration: func(ctx context.Context, entry types.EventStreamConsumerEntry) types.EventStreamConsumerEntry {
					executionOrder = append(executionOrder, "mw2")
					return entry
				},
			},
		})

		entry := types.EventStreamConsumerEntry{
			EventDef: types.BaseEventDefinition{
				ModuleName: "order",
				Name:       "OrderShipped",
				Subject:    "events.orders.v1.shipped",
				Version:    "v1",
			},
			Module: &testModule{name: "analytics"},
			Config: types.StreamConsumerConfig{
				Stream: types.StreamConfig{
					Name: "EVENTS",
				},
			},
			SequenceID: 123,
		}

		chain.RunEventStreamConsumerRegistration(context.Background(), entry)

		if len(executionOrder) != 2 {
			t.Errorf("expected 2 executions, got %d", len(executionOrder))
		}
		if executionOrder[0] != "mw1" || executionOrder[1] != "mw2" {
			t.Errorf("wrong execution order: %v", executionOrder)
		}
	})

	t.Run("passes modified entry through chain", func(t *testing.T) {
		chain := NewChain([]types.MiddlewareModule{
			&testMiddleware{
				name: "mw1",
				onEventStreamConsumerRegistration: func(ctx context.Context, entry types.EventStreamConsumerEntry) types.EventStreamConsumerEntry {
					entry.SequenceID = entry.SequenceID + 100
					return entry
				},
			},
			&testMiddleware{
				name: "mw2",
				onEventStreamConsumerRegistration: func(ctx context.Context, entry types.EventStreamConsumerEntry) types.EventStreamConsumerEntry {
					entry.SequenceID = entry.SequenceID + 200
					return entry
				},
			},
		})

		entry := types.EventStreamConsumerEntry{
			EventDef: types.BaseEventDefinition{
				ModuleName: "order",
				Name:       "OrderShipped",
				Subject:    "events.orders.v1.shipped",
				Version:    "v1",
			},
			Module: &testModule{name: "analytics"},
			Config: types.StreamConsumerConfig{
				Stream: types.StreamConfig{
					Name: "EVENTS",
				},
			},
			SequenceID: 1,
		}

		result := chain.RunEventStreamConsumerRegistration(context.Background(), entry)

		if result.SequenceID != 301 {
			t.Errorf("expected SequenceID=301 (1+100+200), got %d", result.SequenceID)
		}
	})

	t.Run("wraps handler through chain", func(t *testing.T) {
		var handlerCalled bool
		var wrappersCalled []string

		originalHandler := func(ctx context.Context, msgs []*types.Msg) error {
			handlerCalled = true
			return nil
		}

		chain := NewChain([]types.MiddlewareModule{
			&testMiddleware{
				name: "mw1",
				onEventStreamConsumerRegistration: func(ctx context.Context, entry types.EventStreamConsumerEntry) types.EventStreamConsumerEntry {
					original := entry.Handler
					entry.Handler = func(ctx context.Context, msgs []*types.Msg) error {
						wrappersCalled = append(wrappersCalled, "mw1-before")
						err := original(ctx, msgs)
						wrappersCalled = append(wrappersCalled, "mw1-after")
						return err
					}
					return entry
				},
			},
			&testMiddleware{
				name: "mw2",
				onEventStreamConsumerRegistration: func(ctx context.Context, entry types.EventStreamConsumerEntry) types.EventStreamConsumerEntry {
					original := entry.Handler
					entry.Handler = func(ctx context.Context, msgs []*types.Msg) error {
						wrappersCalled = append(wrappersCalled, "mw2-before")
						err := original(ctx, msgs)
						wrappersCalled = append(wrappersCalled, "mw2-after")
						return err
					}
					return entry
				},
			},
		})

		entry := types.EventStreamConsumerEntry{
			EventDef: types.BaseEventDefinition{
				ModuleName: "order",
				Name:       "OrderShipped",
				Subject:    "events.orders.v1.shipped",
				Version:    "v1",
			},
			Handler: originalHandler,
			Module:  &testModule{name: "analytics"},
			Config: types.StreamConsumerConfig{
				Stream: types.StreamConfig{
					Name: "EVENTS",
				},
			},
			SequenceID: 1,
		}

		result := chain.RunEventStreamConsumerRegistration(context.Background(), entry)

		// Call the wrapped handler
		err := result.Handler(context.Background(), []*types.Msg{{Data: []byte("test")}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !handlerCalled {
			t.Error("expected original handler to be called")
		}

		// mw2 wraps mw1's wrapper, so mw2 runs first (outer), then mw1 (inner)
		expected := []string{"mw2-before", "mw1-before", "mw1-after", "mw2-after"}
		if len(wrappersCalled) != len(expected) {
			t.Errorf("expected %d wrapper calls, got %d: %v", len(expected), len(wrappersCalled), wrappersCalled)
		} else {
			for i, exp := range expected {
				if wrappersCalled[i] != exp {
					t.Errorf("position %d: expected %q, got %q", i, exp, wrappersCalled[i])
				}
			}
		}
	})

	t.Run("empty chain returns unchanged entry", func(t *testing.T) {
		chain := NewChain([]types.MiddlewareModule{})

		var handlerCalled bool
		originalHandler := func(ctx context.Context, msgs []*types.Msg) error {
			handlerCalled = true
			return nil
		}

		entry := types.EventStreamConsumerEntry{
			EventDef: types.BaseEventDefinition{
				ModuleName: "order",
				Name:       "OrderShipped",
				Subject:    "events.orders.v1.shipped",
				Version:    "v1",
			},
			Handler: originalHandler,
			Module:  &testModule{name: "analytics"},
			Config: types.StreamConsumerConfig{
				Stream: types.StreamConfig{
					Name: "EVENTS",
				},
			},
			SequenceID: 42,
		}

		result := chain.RunEventStreamConsumerRegistration(context.Background(), entry)

		if result.SequenceID != 42 {
			t.Errorf("expected unchanged SequenceID, got %d", result.SequenceID)
		}

		// Handler should still work
		err := result.Handler(context.Background(), []*types.Msg{{Data: []byte("test")}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !handlerCalled {
			t.Error("expected handler to be called")
		}
	})
}
