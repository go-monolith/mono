//go:build integration
// +build integration

package integration_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-monolith/mono"
)

// guardModule counts its Start and Stop calls and runs the given functions
// in them.
type guardModule struct {
	name      string
	deps      []string
	starts    atomic.Int32
	stops     atomic.Int32
	startFunc func(ctx context.Context) error
	stopFunc  func(ctx context.Context) error
	register  func(c mono.ServiceContainer) error
}

func (m *guardModule) Name() string           { return m.name }
func (m *guardModule) Dependencies() []string { return m.deps }

func (m *guardModule) Start(ctx context.Context) error {
	m.starts.Add(1)
	if m.startFunc != nil {
		return m.startFunc(ctx)
	}
	return nil
}

func (m *guardModule) Stop(ctx context.Context) error {
	m.stops.Add(1)
	if m.stopFunc != nil {
		return m.stopFunc(ctx)
	}
	return nil
}

func (m *guardModule) RegisterServices(c mono.ServiceContainer) error {
	if m.register != nil {
		return m.register(c)
	}
	return nil
}

// waitUntil fails the test unless cond holds within five seconds.
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A Stop during Start (a SIGTERM during a slow start) used to stop the NATS
// server under the start, which then returned nil and left the application
// "running" without a server and without any module stopped. Stop must abort
// the start, and what was started must be stopped before the server is.
func TestIntegration_StopDuringStart(t *testing.T) {
	tests := []struct {
		name string
		// slowStart is the Start of the module the Stop arrives in.
		slowStart func(ctx context.Context, release <-chan struct{}) error
		// wantSlowStops is how often that module must be stopped: a module
		// whose Start failed is not stopped, one that started is.
		wantSlowStops int32
	}{
		{
			name: "the module honours the cancelled context",
			slowStart: func(ctx context.Context, _ <-chan struct{}) error {
				<-ctx.Done()
				return ctx.Err()
			},
			wantSlowStops: 0,
		},
		{
			name: "the module ignores the context and starts",
			slowStart: func(_ context.Context, release <-chan struct{}) error {
				<-release
				return nil
			},
			wantSlowStops: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fw := newWorkerPoolApp(t)
			first := &guardModule{name: "first"}
			release := make(chan struct{})
			inStart := make(chan struct{})
			slow := &guardModule{name: "slow", deps: []string{"first"}}
			slow.startFunc = func(ctx context.Context) error {
				close(inStart)
				return tt.slowStart(ctx, release)
			}
			// The server must still be there while the modules are stopped.
			var serverGoneAtModuleStop atomic.Bool
			first.stopFunc = func(context.Context) error {
				if err := fw.EventBus("").Publish("start-stop-guard.probe", nil); err != nil {
					serverGoneAtModuleStop.Store(true)
				}
				return nil
			}
			for _, m := range []*guardModule{first, slow} {
				if err := fw.Register(m); err != nil {
					t.Fatalf("Register: %v", err)
				}
			}

			startErr := make(chan error, 1)
			go func() { startErr <- fw.Start(context.Background()) }()
			<-inStart

			stopErr := make(chan error, 1)
			go func() { stopErr <- fw.Stop(context.Background()) }()
			// Stop has taken over once the state says so; only then let a
			// module that ignores its context finish starting.
			waitUntil(t, "Stop to take over", func() bool {
				return fw.Health(context.Background()).State == mono.StateStopping
			})
			close(release)

			if err := <-startErr; !errors.Is(err, context.Canceled) {
				t.Errorf("Start = %v, want an error wrapping context.Canceled", err)
			}
			if err := <-stopErr; err != nil {
				t.Errorf("Stop = %v, want nil", err)
			}
			if state := fw.Health(context.Background()).State; state != mono.StateStopped {
				t.Errorf("state %v after Stop, want stopped", state)
			}
			if got := first.stops.Load(); got != 1 {
				t.Errorf("module started before the Stop was stopped %d times, want 1", got)
			}
			if got := slow.stops.Load(); got != tt.wantSlowStops {
				t.Errorf("module the Stop arrived in was stopped %d times, want %d", got, tt.wantSlowStops)
			}
			if serverGoneAtModuleStop.Load() {
				t.Error("the NATS server was stopped before the modules were")
			}
			if err := fw.Stop(context.Background()); err == nil || !strings.Contains(err.Error(), "already stopped") {
				t.Errorf("Stop after Stop = %v, want already stopped", err)
			}
		})
	}
}

// Two Start calls at once both ran: modules were started twice, with data
// races in the lifecycle. Only one may run.
func TestIntegration_ConcurrentStart(t *testing.T) {
	fw := newWorkerPoolApp(t)
	inStart := make(chan struct{})
	release := make(chan struct{})
	m := &guardModule{name: "svc"}
	m.startFunc = func(context.Context) error {
		close(inStart)
		<-release
		return nil
	}
	if err := fw.Register(m); err != nil {
		t.Fatalf("Register: %v", err)
	}

	first := make(chan error, 1)
	go func() { first <- fw.Start(context.Background()) }()
	<-inStart

	if err := fw.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "already starting") {
		t.Errorf("Start during Start = %v, want already starting", err)
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatalf("Start: %v", err)
	}
	if got := m.starts.Load(); got != 1 {
		t.Errorf("module started %d times, want 1", got)
	}
	if err := fw.Stop(context.Background()); err != nil {
		t.Errorf("Stop: %v", err)
	}
}

// A second Stop during a graceful one used to skip the lifecycle and stop the
// NATS server at once, under the handlers the first one was waiting for. It
// must wait for the first and return its result.
func TestIntegration_ConcurrentStop(t *testing.T) {
	fw := newWorkerPoolApp(t)
	inHandler := make(chan struct{})
	m := &guardModule{name: "svc"}
	m.register = func(c mono.ServiceContainer) error {
		return c.RegisterRequestReplyService("slow", func(_ context.Context, _ *mono.Msg) ([]byte, error) {
			close(inHandler)
			time.Sleep(700 * time.Millisecond)
			return []byte("ok"), nil
		})
	}
	if err := fw.Register(m); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := fw.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	client, err := fw.Services("svc").GetRequestReplyService("slow")
	if err != nil {
		t.Fatalf("GetRequestReplyService: %v", err)
	}

	type reply struct {
		data string
		err  error
	}
	call := make(chan reply, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resp, err := client.Call(ctx, []byte("x"))
		if err != nil {
			call <- reply{err: err}
			return
		}
		call <- reply{data: string(resp.Data)}
	}()
	<-inHandler

	firstStop := make(chan error, 1)
	go func() { firstStop <- fw.Stop(context.Background()) }()
	waitUntil(t, "the first Stop to begin", func() bool {
		return fw.Health(context.Background()).State == mono.StateStopping
	})

	begin := time.Now()
	if err := fw.Stop(context.Background()); err != nil {
		t.Errorf("second Stop = %v, want the result of the first (nil)", err)
	}
	if state := fw.Health(context.Background()).State; state != mono.StateStopped {
		t.Errorf("state %v when the second Stop returned after %s, want stopped", state, time.Since(begin))
	}
	if got := <-call; got.err != nil || got.data != "ok" {
		t.Errorf("request in flight at Stop = %q, %v; want its reply", got.data, got.err)
	}
	if err := <-firstStop; err != nil {
		t.Errorf("first Stop = %v, want nil", err)
	}
	if got := m.stops.Load(); got != 1 {
		t.Errorf("module stopped %d times, want 1", got)
	}

	// A second Stop that cannot wait says so instead of stopping again.
	if err := fw.Stop(context.Background()); err == nil || !strings.Contains(err.Error(), "already stopped") {
		t.Errorf("Stop after Stop = %v, want already stopped", err)
	}
}
