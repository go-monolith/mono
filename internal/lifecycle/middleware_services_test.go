package lifecycle

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/go-monolith/mono/internal/registry"
	"github.com/go-monolith/mono/pkg/types"
)

// serviceMiddleware is a middleware that registers services.
type serviceMiddleware struct {
	mockMiddlewareModule
	registerErr error
	registered  bool
}

func (m *serviceMiddleware) RegisterServices(c types.ServiceContainer) error {
	m.registered = true
	if m.registerErr != nil {
		return m.registerErr
	}
	if err := c.RegisterChannelService("trail", make(chan *types.Msg, 1), make(chan *types.Msg, 1)); err != nil {
		return err
	}
	return c.RegisterRequestReplyService("ping", func(context.Context, *types.Msg) ([]byte, error) { return []byte("pong"), nil })
}

// TestMiddlewareServices: a middleware's services are registered on a bound
// container before its Start, a module depending on it gets a view of that
// container, and its NATS services are subscribed (issue #55).
func TestMiddlewareServices(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	mw := &serviceMiddleware{mockMiddlewareModule: mockMiddlewareModule{mockModule: mockModule{name: "trail-mw"}}}
	consumer := &mockModule{name: "consumer", dependencies: []string{"trail-mw"}}
	_ = reg.Register(mw)
	_ = reg.Register(consumer)
	bus := newMockEventBus()
	lm := NewLifecycleManager(reg, registry.NewPluginRegistry(logger), bus, nil, logger, 0)

	if err := lm.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = lm.Stop(context.Background()) })

	if !mw.registered {
		t.Fatal("RegisterServices was not called on the middleware")
	}
	if lm.GetServiceContainer("trail-mw") == nil {
		t.Fatal("no container for the middleware")
	}
	consumer.mu.Lock()
	view := consumer.depContainers["trail-mw"]
	consumer.mu.Unlock()
	if view == nil {
		t.Fatal("the dependent module got no container of the middleware")
	}
	if _, _, err := view.GetChannelService("trail", "consumer"); err != nil {
		t.Fatalf("GetChannelService through the dependency view: %v", err)
	}
	bus.mu.Lock()
	_, subscribed := bus.queueSubscriptions["services.trail-mw.ping"]
	bus.mu.Unlock()
	if !subscribed {
		t.Fatal("the middleware's request-reply service is not subscribed")
	}
}

func TestMiddlewareRegisterServicesFailure(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	var stopOrder []string
	first := &mockMiddlewareModule{mockModule: mockModule{name: "first"}, stopOrder: &stopOrder}
	bad := &serviceMiddleware{mockMiddlewareModule: mockMiddlewareModule{mockModule: mockModule{name: "bad"}, stopOrder: &stopOrder}, registerErr: errors.New("no")}
	_ = reg.Register(first)
	_ = reg.Register(bad)
	lm := NewLifecycleManager(reg, registry.NewPluginRegistry(logger), newMockEventBus(), nil, logger, 0)

	err := lm.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "failed to register services of middleware module bad") {
		t.Fatalf("err = %v", err)
	}
	bad.mu.Lock()
	badStarted := bad.startCalled
	bad.mu.Unlock()
	if badStarted {
		t.Fatal("a middleware whose RegisterServices failed was started")
	}
	if len(stopOrder) != 1 || stopOrder[0] != "first" {
		t.Fatalf("stopped = %v, want [first]", stopOrder)
	}
}
