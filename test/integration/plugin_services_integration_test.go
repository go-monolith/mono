//go:build integration
// +build integration

package integration_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/go-monolith/mono"
	"github.com/go-monolith/mono/pkg/helper"
	"github.com/go-monolith/mono/pkg/types"
)

type pluginEchoReq struct{ Msg string }
type pluginEchoResp struct{ Msg string }

var (
	pluginEcho = helper.RequestReplyService[pluginEchoReq, pluginEchoResp]("echo-plugin", "echo")
	pluginJobs = helper.QueueGroupService[pluginEchoReq]("echo-plugin", "jobs")
)

// echoServicePlugin registers a request-reply and a queue group service in
// its own container.
type echoServicePlugin struct {
	c    mono.ServiceContainer
	jobs chan string
}

func (p *echoServicePlugin) Name() string                         { return "echo-plugin" }
func (p *echoServicePlugin) Start(context.Context) error          { return nil }
func (p *echoServicePlugin) Stop(context.Context) error           { return nil }
func (p *echoServicePlugin) SetContainer(c mono.ServiceContainer) { p.c = c }
func (p *echoServicePlugin) Container() mono.ServiceContainer     { return p.c }
func (p *echoServicePlugin) RegisterServices(c mono.ServiceContainer) error {
	if err := pluginEcho.Register(c, func(_ context.Context, r pluginEchoReq, _ *mono.Msg) (pluginEchoResp, error) {
		return pluginEchoResp{Msg: "echo: " + r.Msg}, nil
	}); err != nil {
		return err
	}
	return pluginJobs.Register(c, types.TypedQGHP[pluginEchoReq]{QueueGroup: "workers", Handler: func(_ context.Context, r pluginEchoReq, _ *mono.Msg) error {
		p.jobs <- r.Msg
		return nil
	}})
}

// pluginCaller declares the plugin's contract and calls it through the
// plugin's container.
type pluginCaller struct{ plugin mono.PluginModule }

func (m *pluginCaller) Name() string                            { return "caller" }
func (m *pluginCaller) Start(context.Context) error             { return nil }
func (m *pluginCaller) Stop(context.Context) error              { return nil }
func (m *pluginCaller) SetPlugin(_ string, p mono.PluginModule) { m.plugin = p }
func (m *pluginCaller) ConsumeServices() []types.BaseServiceDefinition {
	return []types.BaseServiceDefinition{pluginEcho.ToBase(), pluginJobs.ToBase()}
}

// TestPluginServicesServedOverNATS: a request-reply service registered in a
// plugin's container answers at services.<plugin>.<service>, and a queue
// group service of the plugin handles what is sent to it (issue #52).
func TestPluginServicesServedOverNATS(t *testing.T) {
	app, err := mono.NewMonoApplication(mono.WithCustomLogger(&noOpsLogger{}), mono.WithNATSPort(freeTCPPort(t)))
	if err != nil {
		t.Fatal(err)
	}
	m := &pluginCaller{}
	plugin := &echoServicePlugin{jobs: make(chan string, 1)}
	if err := app.RegisterPlugin(plugin, "echo"); err != nil {
		t.Fatal(err)
	}
	if err := app.Register(m); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := app.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = app.Stop(ctx) })

	cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	resp, err := pluginEcho.Call(cctx, m.plugin.Container(), pluginEchoReq{Msg: "hi"})
	if err != nil || resp.Msg != "echo: hi" {
		t.Fatalf("Call = %+v, %v", resp, err)
	}
	reply, err := app.EventBus("").Request("services.echo-plugin.echo", []byte(`{"Msg":"bus"}`), 2*time.Second)
	if err != nil || string(reply.Data) != `{"Msg":"echo: bus"}` {
		t.Fatalf("Request = %v, %v", reply, err)
	}

	if err := pluginJobs.Send(cctx, m.plugin.Container(), pluginEchoReq{Msg: "job"}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	select {
	case got := <-plugin.jobs:
		if got != "job" {
			t.Fatalf("queue group handler got %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the plugin's queue group handler did not run")
	}
}

// listenerPlugin holds a TCP listener between Start and Stop.
type listenerPlugin struct {
	c             mono.ServiceContainer
	ln            net.Listener
	starts, stops int
}

func (p *listenerPlugin) Name() string                         { return "listener-plugin" }
func (p *listenerPlugin) SetContainer(c mono.ServiceContainer) { p.c = c }
func (p *listenerPlugin) Container() mono.ServiceContainer     { return p.c }
func (p *listenerPlugin) Start(context.Context) error {
	p.starts++
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	p.ln = ln
	return nil
}
func (p *listenerPlugin) Stop(context.Context) error {
	p.stops++
	return p.ln.Close()
}

type failOnceModule struct{ fail bool }

func (m *failOnceModule) Name() string { return "fail-once" }
func (m *failOnceModule) Start(context.Context) error {
	if m.fail {
		m.fail = false
		return errors.New("boom")
	}
	return nil
}
func (m *failOnceModule) Stop(context.Context) error { return nil }

// TestFailedStartStopsPlugins: a module failing to start stops the plugins
// that started before it, and a retried Start starts them again (issue #53).
func TestFailedStartStopsPlugins(t *testing.T) {
	app, err := mono.NewMonoApplication(mono.WithCustomLogger(&noOpsLogger{}), mono.WithNATSPort(freeTCPPort(t)))
	if err != nil {
		t.Fatal(err)
	}
	p := &listenerPlugin{}
	if err := app.RegisterPlugin(p, "listener"); err != nil {
		t.Fatal(err)
	}
	if err := app.Register(&failOnceModule{fail: true}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := app.Start(ctx); err == nil {
		t.Fatal("Start succeeded")
	}
	if p.starts != 1 || p.stops != 1 {
		t.Fatalf("after a failed Start: plugin starts=%d stops=%d, want 1 and 1", p.starts, p.stops)
	}
	if c, err := net.Dial("tcp", p.ln.Addr().String()); err == nil {
		_ = c.Close()
		t.Fatal("the plugin's listener still accepts connections after a failed Start")
	}

	if err := app.Start(ctx); err != nil {
		t.Fatalf("retried Start: %v", err)
	}
	if err := app.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if p.starts != 2 || p.stops != 2 {
		t.Fatalf("after Stop: plugin starts=%d stops=%d, want 2 and 2", p.starts, p.stops)
	}
}
