package monotest_test

import (
	"context"
	"errors"
	"testing"

	"github.com/go-monolith/mono/monotest"
	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
)

// jsModule registers, depending on its fields, services or an event stream
// consumer that need JetStream, and may declare that it needs JetStream.
type jsModule struct {
	requires       bool
	streamConsumer bool
	cron           bool
	eventStream    bool
	started        bool
}

func (m *jsModule) Name() string                         { return "reports" }
func (m *jsModule) Start(context.Context) error          { m.started = true; return nil }
func (m *jsModule) Stop(context.Context) error           { return nil }
func (m *jsModule) RequiresJetStream() bool              { return m.requires }
func (m *jsModule) SetPlugin(string, types.PluginModule) {}

func (m *jsModule) RegisterServices(c types.ServiceContainer) error {
	if m.streamConsumer {
		if err := c.RegisterStreamConsumerService("orders",
			types.StreamConsumerConfig{Stream: types.StreamConfig{Name: "ORDERS"}},
			func(context.Context, []*types.Msg) error { return nil }); err != nil {
			return err
		}
	}
	if m.cron {
		return c.RegisterCronService("rollup", types.CronServiceConfig{Schedule: "@every 1m"},
			func(context.Context, *types.Msg) error { return nil })
	}
	return nil
}

func (m *jsModule) RegisterEventConsumers(r types.EventRegistry) error {
	if !m.eventStream {
		return nil
	}
	return r.RegisterEventStreamConsumer(types.BaseEventDefinition{
		ModuleName: "order", Name: "Placed", Subject: "events.order.v1.placed", Version: "v1",
	}, types.StreamConsumerConfig{Stream: types.StreamConfig{Name: "ORDER_EVENTS"}},
		func(context.Context, []*types.Msg) error { return nil }, m)
}

// jsPlugin is a plugin that needs JetStream.
type jsPlugin struct {
	container types.ServiceContainer
}

func (p *jsPlugin) Name() string                          { return "kv" }
func (p *jsPlugin) Start(context.Context) error           { return nil }
func (p *jsPlugin) Stop(context.Context) error            { return nil }
func (p *jsPlugin) SetContainer(c types.ServiceContainer) { p.container = c }
func (p *jsPlugin) Container() types.ServiceContainer     { return p.container }
func (p *jsPlugin) RequiresJetStream() bool               { return true }

func TestWithJetStreamEnabled(t *testing.T) {
	tests := []struct {
		name    string
		module  *jsModule
		opts    []monotest.Option
		wantErr bool
	}{
		{name: "required module without JetStream", module: &jsModule{requires: true},
			opts: []monotest.Option{monotest.WithJetStreamEnabled(false)}, wantErr: true},
		{name: "stream consumer without JetStream", module: &jsModule{streamConsumer: true},
			opts: []monotest.Option{monotest.WithJetStreamEnabled(false)}, wantErr: true},
		{name: "cron without JetStream", module: &jsModule{cron: true},
			opts: []monotest.Option{monotest.WithJetStreamEnabled(false)}, wantErr: true},
		{name: "event stream consumer without JetStream", module: &jsModule{eventStream: true},
			opts: []monotest.Option{monotest.WithJetStreamEnabled(false)}, wantErr: true},
		{name: "required plugin without JetStream", module: &jsModule{},
			opts: []monotest.Option{monotest.WithJetStreamEnabled(false), monotest.WithPlugin("kv", &jsPlugin{})}, wantErr: true},
		{name: "everything with JetStream", module: &jsModule{requires: true, streamConsumer: true, cron: true, eventStream: true},
			opts: []monotest.Option{monotest.WithJetStreamEnabled(true), monotest.WithPlugin("kv", &jsPlugin{})}},
		{name: "everything unchecked by default", module: &jsModule{requires: true, streamConsumer: true, cron: true, eventStream: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := monotest.New(t, tt.module, tt.opts...)
			err := h.Start(context.Background())
			if tt.wantErr {
				if !errors.Is(err, monoerrors.ErrJetStreamNotEnabled) {
					t.Fatalf("Start err = %v, want ErrJetStreamNotEnabled", err)
				}
				if !errors.Is(err, monoerrors.ErrModuleStartFailed) {
					t.Errorf("Start err = %v, want it to wrap ErrModuleStartFailed", err)
				}
				if tt.module.started {
					t.Error("module Start was called")
				}
				return
			}
			if err != nil {
				t.Fatalf("Start failed: %v", err)
			}
			if err := h.Stop(context.Background()); err != nil {
				t.Fatalf("Stop failed: %v", err)
			}
		})
	}
}
