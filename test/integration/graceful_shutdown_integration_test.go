//go:build integration
// +build integration

package integration_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-monolith/mono"
)

// These tests cover issue #21: Stop drains subscriptions, waits for in-flight
// handlers with a live context, then cancels the runtime context and stops
// modules; the drain wait is bounded by a configurable timeout.

// TestGracefulShutdown_InFlightRequestCompletes is the acceptance test of #21:
// a handler that takes 1s and starts just before Stop finishes successfully
// (the caller gets its reply, the handler's context was never cancelled) and
// its module's Stop runs only after the handler returned.
func TestGracefulShutdown_InFlightRequestCompletes(t *testing.T) {
	fw := newWorkerPoolApp(t)

	started := make(chan struct{}, 1)
	var ctxLiveAtEnd atomic.Bool
	module := &stopOrderModule{testModule: testModule{name: "slow-responder"}}
	module.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterRequestReplyService("slow", func(ctx context.Context, req *mono.Msg) ([]byte, error) {
			started <- struct{}{}
			select {
			case <-time.After(time.Second):
				ctxLiveAtEnd.Store(true)
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			module.handlerDone.Store(true)
			return req.Data, nil
		})
	}
	if err := fw.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start application: %v", err)
	}

	client, err := module.container.GetRequestReplyService("slow")
	if err != nil {
		t.Fatalf("Failed to get client: %v", err)
	}
	type result struct {
		data []byte
		err  error
	}
	results := make(chan result, 1)
	go func() {
		callCtx, callCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer callCancel()
		resp, err := client.Call(callCtx, []byte("work"))
		if err != nil {
			results <- result{err: err}
			return
		}
		results <- result{data: resp.Data}
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not start")
	}

	// Stop while the handler is running. Handler takes ~1s.
	stopStart := time.Now()
	if err := fw.Stop(ctx); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
	stopElapsed := time.Since(stopStart)

	select {
	case r := <-results:
		if r.err != nil {
			t.Fatalf("in-flight call failed: %v (Stop took %s)", r.err, stopElapsed)
		}
		if string(r.data) != "work" {
			t.Fatalf("got %q, want work", r.data)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight call never returned")
	}
	if !ctxLiveAtEnd.Load() {
		t.Fatal("handler context was cancelled before the handler finished")
	}
	if !module.wasStopped() {
		t.Fatal("module Stop was not called")
	}
	if !module.handlerDoneAtSt.Load() {
		t.Fatal("module Stop was called while its handler was still running")
	}
	if stopElapsed < 900*time.Millisecond {
		t.Fatalf("Stop returned after %s, before the 1s handler could finish", stopElapsed)
	}
}

// TestGracefulShutdown_DrainTimeoutIsConfigurable checks that a handler which
// never finishes is cut off by WithShutdownDrainTimeout and that modules are
// still stopped.
func TestGracefulShutdown_DrainTimeoutIsConfigurable(t *testing.T) {
	fw := newWorkerPoolApp(t,
		mono.WithShutdownDrainTimeout(time.Second),
		mono.WithShutdownTimeout(10*time.Second),
	)

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	module := &stopOrderModule{testModule: testModule{name: "stuck-responder"}}
	module.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterRequestReplyService("stuck", func(_ context.Context, req *mono.Msg) ([]byte, error) {
			started <- struct{}{}
			<-release // ignores ctx on purpose: this handler never gives up
			return req.Data, nil
		})
	}
	if err := fw.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start application: %v", err)
	}
	defer close(release)

	client, err := module.container.GetRequestReplyService("stuck")
	if err != nil {
		t.Fatalf("Failed to get client: %v", err)
	}
	go func() {
		callCtx, callCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer callCancel()
		_, _ = client.Call(callCtx, []byte("x"))
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not start")
	}

	start := time.Now()
	if err := fw.Stop(context.Background()); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed < time.Second {
		t.Fatalf("Stop returned after %s, before the 1s drain timeout", elapsed)
	}
	if elapsed > 10*time.Second {
		t.Fatalf("Stop took %s; the 1s drain timeout was not respected", elapsed)
	}
	if !module.wasStopped() {
		t.Fatal("module Stop was not called after the drain timeout")
	}
	if module.handlerDoneAtSt.Load() {
		t.Fatal("test setup error: the handler should still have been running")
	}
}

// slowStreamModule owns a stream consumer whose handler takes a while and
// records whether its context was still live when it finished.
type slowStreamModule struct {
	container    mono.ServiceContainer
	started      chan struct{}
	ctxLiveAtEnd atomic.Bool
	acked        atomic.Int32
	stoppedAfter atomic.Bool // module Stop saw the handler finished
	handlerDone  atomic.Bool
}

func (m *slowStreamModule) Name() string                  { return "slow-stream" }
func (m *slowStreamModule) Start(_ context.Context) error { return nil }
func (m *slowStreamModule) Stop(_ context.Context) error {
	m.stoppedAfter.Store(m.handlerDone.Load())
	return nil
}
func (m *slowStreamModule) RegisterServices(container mono.ServiceContainer) error {
	m.container = container
	return container.RegisterStreamConsumerService("ingest", mono.StreamConsumerConfig{
		Stream:   mono.StreamConfig{Name: "SHUTDOWN_TEST_STREAM", Subjects: []string{"shutdown-test.>"}},
		Consumer: mono.ConsumerConfig{AckWait: 30 * time.Second, MaxDeliver: 3},
		Fetch:    mono.FetchConfig{BatchSize: 1, Timeout: 300 * time.Millisecond},
	}, func(ctx context.Context, msgs []*mono.Msg) error {
		m.started <- struct{}{}
		select {
		case <-time.After(time.Second):
			m.ctxLiveAtEnd.Store(true)
		case <-ctx.Done():
			return ctx.Err()
		}
		for _, msg := range msgs {
			if err := msg.Ack(); err != nil {
				return err
			}
			m.acked.Add(1)
		}
		m.handlerDone.Store(true)
		return nil
	})
}

// TestGracefulShutdown_StreamConsumerBatchFinishes checks that a stream
// consumer handler in flight when Stop begins keeps a live context, acks its
// batch, and that the module stops only afterwards.
func TestGracefulShutdown_StreamConsumerBatchFinishes(t *testing.T) {
	fw := newWorkerPoolApp(t, mono.WithJetStreamStorageDir(t.TempDir()))

	module := &slowStreamModule{started: make(chan struct{}, 4)}
	if err := fw.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start application: %v", err)
	}

	client, err := module.container.GetStreamConsumerService("ingest")
	if err != nil {
		t.Fatalf("Failed to get client: %v", err)
	}
	if _, err := client.Publish(ctx, []byte("batch")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	select {
	case <-module.started:
	case <-time.After(10 * time.Second):
		t.Fatal("stream handler did not start")
	}

	if err := fw.Stop(ctx); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
	if !module.ctxLiveAtEnd.Load() {
		t.Fatal("stream handler context was cancelled before the batch finished")
	}
	if module.acked.Load() != 1 {
		t.Fatalf("acked = %d, want 1", module.acked.Load())
	}
	if !module.stoppedAfter.Load() {
		t.Fatal("module Stop ran while the batch handler was still running")
	}
}

// =============================================================================
// Fetch-loop handler calling a dependency during the drain
// =============================================================================

// ledgerModule provides the request-reply service a billing stream handler
// calls while finishing.
type ledgerModule struct {
	container mono.ServiceContainer
	calls     atomic.Int32
}

func (m *ledgerModule) Name() string                  { return "ledger" }
func (m *ledgerModule) Start(_ context.Context) error { return nil }
func (m *ledgerModule) Stop(_ context.Context) error  { return nil }
func (m *ledgerModule) RegisterServices(container mono.ServiceContainer) error {
	m.container = container
	return container.RegisterRequestReplyService("record", func(_ context.Context, req *mono.Msg) ([]byte, error) {
		m.calls.Add(1)
		return append([]byte("recorded:"), req.Data...), nil
	})
}

// billingModule depends on ledger and owns a stream consumer whose handler
// waits until Stop has begun, then calls ledger.record.
type billingModule struct {
	container     mono.ServiceContainer
	ledger        mono.ServiceContainer
	started       chan struct{}
	stopBegan     chan struct{}
	callErr       atomic.Value // error from the nested call, or "" when it succeeded
	handlerDone   atomic.Bool
	stoppedAfter  atomic.Bool
	ackedMessages atomic.Int32
}

func (m *billingModule) Name() string                  { return "billing" }
func (m *billingModule) Dependencies() []string        { return []string{"ledger"} }
func (m *billingModule) Start(_ context.Context) error { return nil }
func (m *billingModule) Stop(_ context.Context) error {
	m.stoppedAfter.Store(m.handlerDone.Load())
	return nil
}
func (m *billingModule) SetDependencyServiceContainer(name string, container mono.ServiceContainer) {
	if name == "ledger" {
		m.ledger = container
	}
}
func (m *billingModule) RegisterServices(container mono.ServiceContainer) error {
	m.container = container
	return container.RegisterStreamConsumerService("invoices", mono.StreamConsumerConfig{
		Stream:   mono.StreamConfig{Name: "BILLING_SHUTDOWN_STREAM", Subjects: []string{"billing-shutdown.>"}},
		Consumer: mono.ConsumerConfig{AckWait: 30 * time.Second, MaxDeliver: 3},
		Fetch:    mono.FetchConfig{BatchSize: 1, Timeout: 300 * time.Millisecond},
	}, func(ctx context.Context, msgs []*mono.Msg) error {
		m.started <- struct{}{}
		<-m.stopBegan
		time.Sleep(200 * time.Millisecond) // let Stop get past "stop taking new work"

		client, err := m.ledger.GetRequestReplyService("record")
		if err != nil {
			m.callErr.Store(err.Error())
			return err
		}
		callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if _, err := client.Call(callCtx, []byte("inv-1")); err != nil {
			m.callErr.Store(err.Error())
			return err
		}
		m.callErr.Store("")
		for _, msg := range msgs {
			if err := msg.Ack(); err != nil {
				return err
			}
			m.ackedMessages.Add(1)
		}
		m.handlerDone.Store(true)
		return nil
	})
}

// TestGracefulShutdown_StreamHandlerCanCallDependencyDuringDrain checks that a
// stream consumer handler in flight at Stop can still call a dependency's
// request-reply service: in-flight fetch-loop handlers are awaited before any
// subscription is drained, and dependents are drained before their
// dependencies.
func TestGracefulShutdown_StreamHandlerCanCallDependencyDuringDrain(t *testing.T) {
	fw := newWorkerPoolApp(t, mono.WithJetStreamStorageDir(t.TempDir()))

	ledger := &ledgerModule{}
	billing := &billingModule{started: make(chan struct{}, 4), stopBegan: make(chan struct{})}
	for _, m := range []mono.Module{ledger, billing} {
		if err := fw.Register(m); err != nil {
			t.Fatalf("Failed to register %s: %v", m.Name(), err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start application: %v", err)
	}

	client, err := billing.container.GetStreamConsumerService("invoices")
	if err != nil {
		t.Fatalf("Failed to get client: %v", err)
	}
	if _, err := client.Publish(ctx, []byte("inv-1")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	select {
	case <-billing.started:
	case <-time.After(10 * time.Second):
		t.Fatal("stream handler did not start")
	}

	close(billing.stopBegan)
	if err := fw.Stop(ctx); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	if got, _ := billing.callErr.Load().(string); got != "" {
		t.Fatalf("nested request-reply call during the drain failed: %s", got)
	}
	if ledger.calls.Load() != 1 {
		t.Fatalf("ledger.record calls = %d, want 1", ledger.calls.Load())
	}
	if billing.ackedMessages.Load() != 1 {
		t.Fatalf("acked = %d, want 1", billing.ackedMessages.Load())
	}
	if !billing.stoppedAfter.Load() {
		t.Fatal("billing Stop ran while its stream handler was still running")
	}
}
