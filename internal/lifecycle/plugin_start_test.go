package lifecycle

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-monolith/mono/internal/registry"
	"github.com/go-monolith/mono/pkg/types"
)

// servicePlugin is a plugin that registers a request-reply service and a
// channel service in its own container.
type servicePlugin struct {
	mockPluginModule
	in, out chan *types.Msg
}

func (p *servicePlugin) RegisterServices(c types.ServiceContainer) error {
	if err := c.RegisterRequestReplyService("echo", func(_ context.Context, req *types.Msg) ([]byte, error) {
		return append([]byte("echo: "), req.Data...), nil
	}); err != nil {
		return err
	}
	p.in, p.out = make(chan *types.Msg, 1), make(chan *types.Msg, 1)
	return c.RegisterChannelService("feed", p.in, p.out)
}

// countingPlugin counts its Start and Stop calls.
type countingPlugin struct {
	mockPluginModule
	starts, stops int
}

func (p *countingPlugin) Start(ctx context.Context) error {
	p.starts++
	return p.mockPluginModule.Start(ctx)
}

func (p *countingPlugin) Stop(ctx context.Context) error {
	p.stops++
	return p.mockPluginModule.Stop(ctx)
}

// emitterModule emits one event.
type emitterModule struct {
	mockModule
}

func (m *emitterModule) SetEventBus(types.EventBus) {}

func (m *emitterModule) EmitEvents() []types.BaseEventDefinition {
	return []types.BaseEventDefinition{{ModuleName: m.name, Name: "created", Version: "v1", Subject: "events." + m.name + ".created"}}
}

// failingEventRegistry rejects every event definition.
type failingEventRegistry struct {
	*mockEventRegistry
}

func (failingEventRegistry) RegisterEvent(types.BaseEventDefinition) error {
	return errors.New("event rejected")
}

func TestPluginServicesAreServed(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	plugin := &servicePlugin{mockPluginModule: mockPluginModule{name: "echo-plugin"}}
	if err := pluginReg.Register(plugin, "echo"); err != nil {
		t.Fatal(err)
	}
	bus := newMockEventBus()
	lm := NewLifecycleManager(reg, pluginReg, bus, nil, logger, 0)

	if err := lm.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = lm.Stop(context.Background()) })

	bus.mu.Lock()
	queues := bus.queueSubscriptions["services.echo-plugin.echo"]
	bus.mu.Unlock()
	if len(queues) == 0 {
		t.Fatalf("no subscription on services.echo-plugin.echo; subscriptions: %v", keys(bus))
	}

	// The subscriptions are tracked under the plugin's name, so they are
	// drained at Stop (after the modules').
	impl := lm.(*lifecycleManager)
	impl.mu.RLock()
	subs := len(impl.subscriptions["echo-plugin"])
	impl.mu.RUnlock()
	if subs != 1 {
		t.Fatalf("subscriptions tracked for echo-plugin = %d, want 1", subs)
	}

	// The channel router of the plugin's channel service runs: a message the
	// plugin sends reaches the consumer's own out channel.
	_, out, err := lm.GetPluginContainer("echo").GetChannelService("feed", "client")
	if err != nil {
		t.Fatalf("GetChannelService: %v", err)
	}
	plugin.out <- &types.Msg{Data: []byte("tick")}
	select {
	case msg := <-out:
		if string(msg.Data) != "tick" {
			t.Fatalf("routed %q", msg.Data)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the plugin's channel router does not run")
	}
}

func keys(bus *mockEventBus) []string {
	bus.mu.Lock()
	defer bus.mu.Unlock()
	out := make([]string, 0, len(bus.queueSubscriptions))
	for k := range bus.queueSubscriptions {
		out = append(out, k)
	}
	return out
}

func TestPluginServiceNameCollisions(t *testing.T) {
	t.Run("two plugins with the same name", func(t *testing.T) {
		logger := &mockLogger{}
		reg := registry.NewModuleRegistry(logger)
		pluginReg := registry.NewPluginRegistry(logger)
		var stopOrder []string
		p1 := &servicePlugin{mockPluginModule: mockPluginModule{name: "echo-plugin", stopOrder: &stopOrder}}
		p2 := &servicePlugin{mockPluginModule: mockPluginModule{name: "echo-plugin", stopOrder: &stopOrder}}
		_ = pluginReg.Register(p1, "a")
		_ = pluginReg.Register(p2, "b")
		lm := NewLifecycleManager(reg, pluginReg, newMockEventBus(), nil, logger, 0)

		err := lm.Start(context.Background())
		if err == nil || !strings.Contains(err.Error(), `plugins "a" and "b" both register services as "echo-plugin"`) {
			t.Fatalf("err = %v", err)
		}
		if len(stopOrder) != 2 {
			t.Fatalf("plugins stopped = %v, want both", stopOrder)
		}
	})
	t.Run("a plugin named like a module", func(t *testing.T) {
		logger := &mockLogger{}
		reg := registry.NewModuleRegistry(logger)
		pluginReg := registry.NewPluginRegistry(logger)
		_ = reg.Register(&mockModule{name: "echo-plugin"})
		_ = pluginReg.Register(&servicePlugin{mockPluginModule: mockPluginModule{name: "echo-plugin"}}, "echo")
		lm := NewLifecycleManager(reg, pluginReg, newMockEventBus(), nil, logger, 0)

		err := lm.Start(context.Background())
		if err == nil || !strings.Contains(err.Error(), `plugin "echo" registers services as "echo-plugin", the name of a module`) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("plugins without services may share a name", func(t *testing.T) {
		logger := &mockLogger{}
		reg := registry.NewModuleRegistry(logger)
		pluginReg := registry.NewPluginRegistry(logger)
		_ = reg.Register(&mockModule{name: "storage"})
		_ = pluginReg.Register(&mockPluginModule{name: "storage"}, "a")
		_ = pluginReg.Register(&mockPluginModule{name: "storage"}, "b")
		lm := NewLifecycleManager(reg, pluginReg, newMockEventBus(), nil, logger, 0)
		if err := lm.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		_ = lm.Stop(context.Background())
	})
}

// TestFailedStartStopsPluginsAndMiddleware checks that every failure after
// the plugins started stops what was started: modules, then the middleware
// whose Start succeeded, then the plugins.
func TestFailedStartStopsPluginsAndMiddleware(t *testing.T) {
	type setup struct {
		reg       registry.ModuleRegistry
		eventReg  types.EventRegistry
		bus       *mockEventBus
		stopOrder *[]string
	}
	tests := []struct {
		name      string
		configure func(s *setup)
		wantErr   string
		wantStops []string
	}{
		{
			name: "module start fails",
			configure: func(s *setup) {
				_ = s.reg.Register(&mockMiddlewareModule{mockModule: mockModule{name: "mw"}, stopOrder: s.stopOrder})
				_ = s.reg.Register(&mockModuleWithStopOrder{mockModule: mockModule{name: "ok"}, stopOrder: s.stopOrder})
				_ = s.reg.Register(&mockModule{name: "failing", startErr: errors.New("boom"), dependencies: []string{"ok"}})
			},
			wantErr:   "rolled back 1 modules",
			wantStops: []string{"ok", "mw", "plugin"},
		},
		{
			name: "middleware start fails",
			configure: func(s *setup) {
				_ = s.reg.Register(&mockMiddlewareModule{mockModule: mockModule{name: "mw1"}, stopOrder: s.stopOrder})
				_ = s.reg.Register(&mockMiddlewareModule{mockModule: mockModule{name: "mw2", startErr: errors.New("boom")}, stopOrder: s.stopOrder})
			},
			wantErr:   "failed to start middleware module mw2",
			wantStops: []string{"mw1", "plugin"},
		},
		{
			name: "dependencies do not resolve",
			configure: func(s *setup) {
				_ = s.reg.Register(&mockModule{name: "orphan", dependencies: []string{"missing"}})
			},
			wantErr:   "failed to resolve dependencies",
			wantStops: []string{"plugin"},
		},
		{
			name: "an event is rejected",
			configure: func(s *setup) {
				s.eventReg = failingEventRegistry{newMockEventRegistry()}
				_ = s.reg.Register(&emitterModule{mockModule: mockModule{name: "emitter"}})
			},
			wantErr:   "failed to register event from emitter",
			wantStops: []string{"plugin"},
		},
		{
			name: "subscription setup fails",
			configure: func(s *setup) {
				s.bus.queueSubscribeErr = errors.New("no subscribe")
				_ = s.reg.Register(&mockModuleWithStopOrder{mockModule: mockModule{name: "provider"}, stopOrder: s.stopOrder})
				_ = s.reg.Register(&mockModuleWithServices{
					mockModule:     mockModule{name: "svc"},
					serviceEntries: []*types.ServiceEntry{{Name: "ping", Type: types.ServiceTypeRequestReply, RequestHandler: func(context.Context, *types.Msg) ([]byte, error) { return nil, nil }}},
				})
			},
			wantErr:   "rolled back 2 modules",
			wantStops: []string{"provider", "plugin"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := &mockLogger{}
			var stopOrder []string
			s := &setup{reg: registry.NewModuleRegistry(logger), eventReg: newMockEventRegistry(), bus: newMockEventBus(), stopOrder: &stopOrder}
			pluginReg := registry.NewPluginRegistry(logger)
			plugin := &countingPlugin{mockPluginModule: mockPluginModule{name: "plugin", stopOrder: &stopOrder}}
			_ = pluginReg.Register(plugin, "p")
			tt.configure(s)
			lm := NewLifecycleManager(s.reg, pluginReg, s.bus, s.eventReg, logger, 0)

			err := lm.Start(context.Background())
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("err = %v, want %q", err, tt.wantErr)
			}
			if got := filterStops(stopOrder, tt.wantStops); !reflect.DeepEqual(got, tt.wantStops) {
				t.Fatalf("stop order = %v, want %v", stopOrder, tt.wantStops)
			}
			if plugin.starts != 1 || plugin.stops != 1 {
				t.Fatalf("plugin starts=%d stops=%d, want 1 and 1", plugin.starts, plugin.stops)
			}
			if c := lm.GetPluginContainer("p"); c != nil {
				t.Fatal("plugin container kept after a failed Start")
			}
		})
	}
}

// filterStops keeps the entries of got that are named in want, so a module
// mock that records its stop in another way does not disturb the order.
func filterStops(got, want []string) []string {
	named := make(map[string]bool, len(want))
	for _, w := range want {
		named[w] = true
	}
	out := []string{}
	for _, g := range got {
		if named[g] {
			out = append(out, g)
		}
	}
	return out
}

// TestStartRetryAfterFailure checks that a Start retried after a failure
// starts the plugin once more (it was stopped by the failed Start) and serves
// its services.
func TestStartRetryAfterFailure(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	plugin := &countingPlugin{mockPluginModule: mockPluginModule{name: "plugin"}}
	_ = pluginReg.Register(plugin, "p")
	flaky := &mockModule{name: "flaky", startErr: errors.New("not yet")}
	_ = reg.Register(flaky)
	bus := newMockEventBus()
	lm := NewLifecycleManager(reg, pluginReg, bus, newMockEventRegistry(), logger, 0)

	if err := lm.Start(context.Background()); err == nil {
		t.Fatal("first Start succeeded")
	}
	flaky.mu.Lock()
	flaky.startErr = nil
	flaky.mu.Unlock()
	if err := lm.Start(context.Background()); err != nil {
		t.Fatalf("second Start: %v", err)
	}
	if plugin.starts != 2 || plugin.stops != 1 {
		t.Fatalf("plugin starts=%d stops=%d, want 2 and 1", plugin.starts, plugin.stops)
	}
	if lm.GetPluginContainer("p") == nil {
		t.Fatal("no plugin container after the retried Start")
	}
	if err := lm.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if plugin.stops != 2 {
		t.Fatalf("plugin stops=%d after Stop, want 2", plugin.stops)
	}
}
