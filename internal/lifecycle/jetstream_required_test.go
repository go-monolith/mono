package lifecycle

import (
	"context"
	"errors"
	"testing"

	"github.com/go-monolith/mono/internal/registry"
	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
)

// jsRequiredModule is a module implementing types.JetStreamRequiredModule.
type jsRequiredModule struct {
	mockModule
	requires bool
}

func (m *jsRequiredModule) RequiresJetStream() bool { return m.requires }

// jsRequiredPlugin is a plugin implementing types.JetStreamRequiredModule.
type jsRequiredPlugin struct {
	mockPluginModule
}

func (p *jsRequiredPlugin) RequiresJetStream() bool { return true }

// jsRequiredMiddleware is a middleware implementing
// types.JetStreamRequiredModule.
type jsRequiredMiddleware struct {
	mockMiddlewareModule
}

func (m *jsRequiredMiddleware) RequiresJetStream() bool { return true }

// newJetStreamTestLifecycle returns a lifecycle manager whose event bus has
// JetStream when withJetStream is set, and none otherwise (EventStream fails,
// so waitForJetStream reports it disabled).
func newJetStreamTestLifecycle(withJetStream bool) (*lifecycleManager, registry.ModuleRegistry, registry.PluginRegistry) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()
	if withJetStream {
		eventBus.eventStream = &mockEventStream{}
	}
	lm := NewLifecycleManager(reg, pluginReg, eventBus, registry.NewEventRegistry(logger), logger, 0).(*lifecycleManager)
	return lm, reg, pluginReg
}

func assertJetStreamNotEnabled(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, monoerrors.ErrJetStreamNotEnabled) {
		t.Fatalf("Start err = %v, want ErrJetStreamNotEnabled", err)
	}
	if !errors.Is(err, monoerrors.ErrModuleStartFailed) {
		t.Errorf("Start err = %v, want it to wrap ErrModuleStartFailed", err)
	}
}

func TestStart_JetStreamRequiredModule(t *testing.T) {
	tests := []struct {
		name          string
		requires      bool
		withJetStream bool
		wantErr       bool
	}{
		{name: "required without JetStream fails", requires: true, wantErr: true},
		{name: "required with JetStream starts", requires: true, withJetStream: true},
		{name: "not required without JetStream starts", requires: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lm, reg, _ := newJetStreamTestLifecycle(tt.withJetStream)
			m := &jsRequiredModule{mockModule: mockModule{name: "cache"}, requires: tt.requires}
			if err := reg.Register(m); err != nil {
				t.Fatalf("Register failed: %v", err)
			}
			err := lm.Start(context.Background())
			if tt.wantErr {
				assertJetStreamNotEnabled(t, err)
				if m.startCalled {
					t.Error("module Start was called")
				}
				return
			}
			if err != nil {
				t.Fatalf("Start failed: %v", err)
			}
			defer func() { _ = lm.Stop(context.Background()) }()
			if !m.startCalled {
				t.Error("module Start was not called")
			}
		})
	}
}

func TestStart_JetStreamRequiredPlugin(t *testing.T) {
	lm, _, pluginReg := newJetStreamTestLifecycle(false)
	var started []string
	p := &jsRequiredPlugin{mockPluginModule: mockPluginModule{name: "kv", startOrder: &started}}
	if err := pluginReg.Register(p, "kv"); err != nil {
		t.Fatalf("Register plugin failed: %v", err)
	}
	assertJetStreamNotEnabled(t, lm.Start(context.Background()))
	if len(started) != 0 {
		t.Errorf("plugin Start was called: %v", started)
	}
}

func TestStart_JetStreamRequiredMiddleware(t *testing.T) {
	lm, reg, _ := newJetStreamTestLifecycle(false)
	mw := &jsRequiredMiddleware{mockMiddlewareModule: mockMiddlewareModule{mockModule: mockModule{name: "mw"}}}
	if err := reg.Register(mw); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	assertJetStreamNotEnabled(t, lm.Start(context.Background()))
	if mw.startCalled {
		t.Error("middleware Start was called")
	}
}

// TestStart_StreamConsumerRegistrationWithoutJetStream checks that the
// lifecycle tells module containers that JetStream is disabled, so that a
// stream consumer registration fails the module start.
func TestStart_StreamConsumerRegistrationWithoutJetStream(t *testing.T) {
	lm, reg, _ := newJetStreamTestLifecycle(false)
	m := &registeringModule{mockModule: mockModule{name: "orders"}}
	if err := reg.Register(m); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	assertJetStreamNotEnabled(t, lm.Start(context.Background()))
}

// registeringModule registers a stream consumer service.
type registeringModule struct {
	mockModule
}

func (m *registeringModule) RegisterServices(c types.ServiceContainer) error {
	return c.RegisterStreamConsumerService("orders",
		types.StreamConsumerConfig{Stream: types.StreamConfig{Name: "ORDERS"}},
		func(context.Context, []*types.Msg) error { return nil })
}

// eventStreamConsumerModule registers an event stream consumer.
type eventStreamConsumerModule struct {
	mockModule
}

func (m *eventStreamConsumerModule) RegisterEventConsumers(r types.EventRegistry) error {
	return r.RegisterEventStreamConsumer(types.BaseEventDefinition{
		ModuleName: "order", Name: "OrderCreated", Subject: "events.orders.v1.created", Version: "v1",
	}, types.StreamConsumerConfig{Stream: types.StreamConfig{Name: "ORDERS"}},
		func(context.Context, []*types.Msg) error { return nil }, m)
}

// TestStart_EventStreamConsumerRegistrationWithoutJetStream checks that the
// lifecycle tells the event registry that JetStream is disabled, so that an
// event stream consumer registration fails the module start.
func TestStart_EventStreamConsumerRegistrationWithoutJetStream(t *testing.T) {
	lm, reg, _ := newJetStreamTestLifecycle(false)
	m := &eventStreamConsumerModule{mockModule: mockModule{name: "billing"}}
	if err := reg.Register(m); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	assertJetStreamNotEnabled(t, lm.Start(context.Background()))
	if m.startCalled {
		t.Error("module Start was called")
	}
}
