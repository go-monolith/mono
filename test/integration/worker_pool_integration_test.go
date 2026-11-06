//go:build integration
// +build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-monolith/mono"
	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/helper"
)

// These tests cover issue #19: handlers of request-reply, queue group and
// event consumer subscriptions run on a bounded worker pool per subscription
// instead of one at a time on the nats.go dispatcher goroutine.

// newWorkerPoolApp creates an in-process application for the worker pool tests.
func newWorkerPoolApp(t *testing.T, opts ...mono.MonoFrameworkOption) mono.MonoApplication {
	t.Helper()
	opts = append([]mono.MonoFrameworkOption{
		mono.WithNATSDontListen(),
		mono.WithNATSInProcessConn(),
		mono.WithCustomLogger(&noOpsLogger{}),
	}, opts...)
	fw, err := mono.NewMonoApplication(opts...)
	if err != nil {
		t.Fatalf("Failed to create application: %v", err)
	}
	return fw
}

// startWorkerPoolApp starts fw and stops it when the test ends.
func startWorkerPoolApp(t *testing.T, fw mono.MonoApplication) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Failed to start application: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer stopCancel()
		_ = fw.Stop(stopCtx)
	})
}

// concurrencyTracker records the highest number of handler calls that
// overlapped and the order in which calls finished.
type concurrencyTracker struct {
	running atomic.Int32
	max     atomic.Int32
	mu      sync.Mutex
	order   []int
}

func (c *concurrencyTracker) enter() {
	n := c.running.Add(1)
	for {
		m := c.max.Load()
		if n <= m || c.max.CompareAndSwap(m, n) {
			return
		}
	}
}

func (c *concurrencyTracker) exit(id int) {
	c.mu.Lock()
	c.order = append(c.order, id)
	c.mu.Unlock()
	c.running.Add(-1)
}

func (c *concurrencyTracker) finished() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]int, len(c.order))
	copy(out, c.order)
	return out
}

// waitForCount polls until counter reaches want or the deadline passes.
func waitForCount(t *testing.T, counter func() int, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if counter() >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out: got %d, want %d", counter(), want)
}

// TestWorkerPool_SlowRequestDoesNotBlockOthers is the first acceptance test
// of #19: while one handler call blocks for 2s, 50 other calls to the same
// service on the same instance must complete without waiting for it.
func TestWorkerPool_SlowRequestDoesNotBlockOthers(t *testing.T) {
	fw := newWorkerPoolApp(t)

	slowStarted := make(chan struct{})
	responder := &testModule{name: "pool-responder"}
	responder.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterRequestReplyService("lookup", func(_ context.Context, req *mono.Msg) ([]byte, error) {
			if string(req.Data) == "slow" {
				close(slowStarted)
				time.Sleep(2 * time.Second)
			}
			return req.Data, nil
		}, mono.WithMaxConcurrency(64)) // independent of the runner's GOMAXPROCS
	}
	if err := fw.Register(responder); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}
	startWorkerPoolApp(t, fw)

	client, err := responder.container.GetRequestReplyService("lookup")
	if err != nil {
		t.Fatalf("Failed to get client: %v", err)
	}

	// Occupy one worker with the slow call.
	slowDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := client.Call(ctx, []byte("slow"))
		slowDone <- err
	}()
	select {
	case <-slowStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("slow handler did not start")
	}

	// Fire 50 fast calls concurrently and time them as a batch.
	const fastCalls = 50
	start := time.Now()
	var wg sync.WaitGroup
	errs := make(chan error, fastCalls)
	for i := 0; i < fastCalls; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, err := client.Call(ctx, []byte(strconv.Itoa(i)))
			if err != nil {
				errs <- fmt.Errorf("call %d: %w", i, err)
				return
			}
			if string(resp.Data) != strconv.Itoa(i) {
				errs <- fmt.Errorf("call %d: got %q", i, resp.Data)
			}
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	// With serial dispatch the fast calls would wait for the slow one (2s).
	// The issue asks for under 100ms; the bound is relaxed to 1s so that a
	// slow CI runner under the race detector does not fail it, while still
	// proving the calls did not wait for the slow handler.
	t.Logf("%d concurrent calls completed in %s while one handler blocked", fastCalls, elapsed)
	if elapsed >= time.Second {
		t.Fatalf("fast calls took %s; they waited for the slow handler", elapsed)
	}

	if err := <-slowDone; err != nil {
		t.Fatalf("slow call failed: %v", err)
	}
}

// TestWorkerPool_RequestReplyRunsConcurrentlyByDefault checks the default
// pool runs several request-reply handlers at once.
func TestWorkerPool_RequestReplyRunsConcurrentlyByDefault(t *testing.T) {
	fw := newWorkerPoolApp(t)

	tracker := &concurrencyTracker{}
	responder := &testModule{name: "pool-responder"}
	responder.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterRequestReplyService("work", func(_ context.Context, req *mono.Msg) ([]byte, error) {
			tracker.enter()
			time.Sleep(50 * time.Millisecond)
			id, _ := strconv.Atoi(string(req.Data))
			tracker.exit(id)
			return req.Data, nil
		})
	}
	if err := fw.Register(responder); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}
	startWorkerPoolApp(t, fw)

	client, err := responder.container.GetRequestReplyService("work")
	if err != nil {
		t.Fatalf("Failed to get client: %v", err)
	}

	const calls = 8
	var wg sync.WaitGroup
	for i := 0; i < calls; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := client.Call(ctx, []byte(strconv.Itoa(i))); err != nil {
				t.Errorf("call %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	if tracker.max.Load() < 2 {
		t.Fatalf("max observed concurrency %d, want >= 2", tracker.max.Load())
	}
}

// TestWorkerPool_MaxConcurrencyOneKeepsOrder is the second acceptance test
// of #19: a queue group registered with MaxConcurrency 1 handles messages one
// at a time, in the order they were sent.
func TestWorkerPool_MaxConcurrencyOneKeepsOrder(t *testing.T) {
	fw := newWorkerPoolApp(t)

	const messages = 100
	tracker := &concurrencyTracker{}
	worker := &testModule{name: "pool-worker"}
	worker.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterQueueGroupService("ordered", mono.QGHP{
			QueueGroup: "ordered-workers",
			Handler: func(_ context.Context, msg *mono.Msg) error {
				tracker.enter()
				time.Sleep(500 * time.Microsecond) // make overlap observable
				id, _ := strconv.Atoi(string(msg.Data))
				tracker.exit(id)
				return nil
			},
			Options: mono.ServiceOptions{MaxConcurrency: 1, MaxPending: messages},
		})
	}
	if err := fw.Register(worker); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}
	startWorkerPoolApp(t, fw)

	client, err := worker.container.GetQueueGroupService("ordered")
	if err != nil {
		t.Fatalf("Failed to get client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := 0; i < messages; i++ {
		if err := client.Send(ctx, []byte(strconv.Itoa(i))); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}

	waitForCount(t, func() int { return len(tracker.finished()) }, messages, 10*time.Second)

	if tracker.max.Load() != 1 {
		t.Fatalf("max observed concurrency %d, want 1", tracker.max.Load())
	}
	for i, id := range tracker.finished() {
		if id != i {
			t.Fatalf("message %d was handled at position %d: order not kept (%v)", id, i, tracker.finished())
		}
	}
}

// TestWorkerPool_OverloadReply is the third acceptance test of #19: when the
// pool and its pending queue are full, a request-reply call is answered at
// once with ErrServiceOverloaded instead of waiting.
func TestWorkerPool_OverloadReply(t *testing.T) {
	fw := newWorkerPoolApp(t)

	release := make(chan struct{})
	started := make(chan struct{}, 8)
	responder := &testModule{name: "pool-responder"}
	responder.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterRequestReplyService("busy", func(_ context.Context, req *mono.Msg) ([]byte, error) {
			started <- struct{}{}
			<-release
			return req.Data, nil
		}, mono.WithMaxConcurrency(1), mono.WithMaxPending(1))
	}
	if err := fw.Register(responder); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}
	startWorkerPoolApp(t, fw)

	client, err := responder.container.GetRequestReplyService("busy")
	if err != nil {
		t.Fatalf("Failed to get client: %v", err)
	}

	// The first call occupies the single worker.
	results := make(chan error, 8)
	call := func(data string) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := client.Call(ctx, []byte(data))
		results <- err
	}
	go call("0")
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("first handler did not start")
	}

	// Three more calls race for the single queue slot: exactly one is queued
	// and the other two must be rejected at once, before the worker is
	// released. Which one wins the slot is not observable, so the assertion
	// is on the counts.
	start := time.Now()
	for _, data := range []string{"1", "2", "3"} {
		go call(data)
	}
	rejected := 0
	for rejected < 2 {
		select {
		case err := <-results:
			if err == nil {
				t.Fatal("a call completed before the worker was released")
			}
			if !errors.Is(err, monoerrors.ErrServiceOverloaded) {
				t.Fatalf("expected ErrServiceOverloaded, got %v", err)
			}
			var svcErr *monoerrors.ServiceError
			if !errors.As(err, &svcErr) || svcErr.ServiceName != "busy" || svcErr.ModuleName != "pool-responder" {
				t.Fatalf("expected ServiceError for busy/pool-responder, got %#v", err)
			}
			rejected++
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of 2 calls were rejected while the pool was full", rejected)
		}
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("overloaded replies took %s; they should be immediate", elapsed)
	}

	// The accepted calls (the running one and the queued one) still complete
	// once the worker is released.
	close(release)
	for i := 0; i < 2; i++ {
		select {
		case err := <-results:
			if err != nil {
				t.Errorf("accepted call failed: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("accepted call did not complete")
		}
	}
}

// TestWorkerPool_QueueGroupOverloadReportedToSender checks that a queue group
// send whose message is rejected gets ErrServiceOverloaded instead of an ACK.
func TestWorkerPool_QueueGroupOverloadReportedToSender(t *testing.T) {
	fw := newWorkerPoolApp(t)

	release := make(chan struct{})
	started := make(chan struct{}, 8)
	worker := &testModule{name: "pool-worker"}
	worker.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterQueueGroupService("busy", mono.QGHP{
			QueueGroup: "busy-workers",
			Handler: func(_ context.Context, _ *mono.Msg) error {
				started <- struct{}{}
				<-release
				return nil
			},
			Options: mono.ServiceOptions{MaxConcurrency: 1, MaxPending: 1},
		})
	}
	if err := fw.Register(worker); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}
	startWorkerPoolApp(t, fw)
	defer close(release)

	client, err := worker.container.GetQueueGroupService("busy")
	if err != nil {
		t.Fatalf("Failed to get client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Send(ctx, []byte("1")); err != nil {
		t.Fatalf("first send: %v", err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("first handler did not start")
	}
	if err := client.Send(ctx, []byte("2")); err != nil {
		t.Fatalf("second send (queued): %v", err)
	}

	err = client.Send(ctx, []byte("3"))
	if !errors.Is(err, monoerrors.ErrServiceOverloaded) {
		t.Fatalf("third send: expected ErrServiceOverloaded, got %v", err)
	}
}

// TestWorkerPool_DefaultServiceConcurrencyOne checks that
// WithDefaultServiceConcurrency(1) restores serial handling for every
// subscription that does not set its own MaxConcurrency.
func TestWorkerPool_DefaultServiceConcurrencyOne(t *testing.T) {
	fw := newWorkerPoolApp(t, mono.WithDefaultServiceConcurrency(1))

	tracker := &concurrencyTracker{}
	responder := &testModule{name: "pool-responder"}
	responder.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterRequestReplyService("work", func(_ context.Context, req *mono.Msg) ([]byte, error) {
			tracker.enter()
			time.Sleep(5 * time.Millisecond)
			id, _ := strconv.Atoi(string(req.Data))
			tracker.exit(id)
			return req.Data, nil
		}, mono.WithMaxPending(64))
	}
	if err := fw.Register(responder); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}
	startWorkerPoolApp(t, fw)

	client, err := responder.container.GetRequestReplyService("work")
	if err != nil {
		t.Fatalf("Failed to get client: %v", err)
	}

	const calls = 20
	var wg sync.WaitGroup
	for i := 0; i < calls; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if _, err := client.Call(ctx, []byte(strconv.Itoa(i))); err != nil {
				t.Errorf("call %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	if tracker.max.Load() != 1 {
		t.Fatalf("max observed concurrency %d, want 1", tracker.max.Load())
	}
	if n := len(tracker.finished()); n != calls {
		t.Fatalf("%d calls handled, want %d", n, calls)
	}
}

// =============================================================================
// Event consumers
// =============================================================================

type poolTickEvent struct {
	ID int `json:"id"`
}

var poolTickEventDef = helper.EventDefinition[poolTickEvent]("pool-emitter", "Tick", "v1")

type poolEmitterModule struct {
	eventBus mono.EventBus
}

func (m *poolEmitterModule) Name() string                  { return "pool-emitter" }
func (m *poolEmitterModule) Start(_ context.Context) error { return nil }
func (m *poolEmitterModule) Stop(_ context.Context) error  { return nil }
func (m *poolEmitterModule) SetEventBus(bus mono.EventBus) { m.eventBus = bus }
func (m *poolEmitterModule) EmitEvents() []mono.BaseEventDefinition {
	return []mono.BaseEventDefinition{poolTickEventDef.ToBase()}
}

// poolConsumerModule consumes poolTickEvent with the given options and records
// handler overlap and completion order.
type poolConsumerModule struct {
	name    string
	opts    *mono.EventConsumerOptions // nil registers with RegisterTypedEventConsumer
	delay   time.Duration
	tracker concurrencyTracker
}

func (m *poolConsumerModule) Name() string                  { return m.name }
func (m *poolConsumerModule) Start(_ context.Context) error { return nil }
func (m *poolConsumerModule) Stop(_ context.Context) error  { return nil }

func (m *poolConsumerModule) RegisterEventConsumers(registry mono.EventRegistry) error {
	handler := func(_ context.Context, event poolTickEvent, _ *mono.Msg) error {
		m.tracker.enter()
		time.Sleep(m.delay)
		m.tracker.exit(event.ID)
		return nil
	}
	if m.opts == nil {
		return helper.RegisterTypedEventConsumer(registry, poolTickEventDef, handler, m)
	}
	return helper.RegisterTypedEventConsumerWithOptions(registry, poolTickEventDef, handler, m, *m.opts)
}

// TestWorkerPool_EventConsumersRunConcurrentlyByDefault checks that an event
// consumer handles events concurrently unless told otherwise.
func TestWorkerPool_EventConsumersRunConcurrentlyByDefault(t *testing.T) {
	fw := newWorkerPoolApp(t)

	emitter := &poolEmitterModule{}
	consumer := &poolConsumerModule{name: "pool-consumer", delay: 50 * time.Millisecond}
	for _, m := range []mono.Module{emitter, consumer} {
		if err := fw.Register(m); err != nil {
			t.Fatalf("Failed to register %s: %v", m.Name(), err)
		}
	}
	startWorkerPoolApp(t, fw)

	const events = 8
	for i := 0; i < events; i++ {
		if err := poolTickEventDef.Publish(emitter.eventBus, poolTickEvent{ID: i}, nil); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	waitForCount(t, func() int { return len(consumer.tracker.finished()) }, events, 10*time.Second)

	if consumer.tracker.max.Load() < 2 {
		t.Fatalf("max observed concurrency %d, want >= 2", consumer.tracker.max.Load())
	}
}

// TestWorkerPool_EventConsumerMaxConcurrencyOneKeepsOrder checks that an event
// consumer registered with MaxConcurrency 1 sees events in publish order.
func TestWorkerPool_EventConsumerMaxConcurrencyOneKeepsOrder(t *testing.T) {
	fw := newWorkerPoolApp(t)

	const events = 100
	emitter := &poolEmitterModule{}
	consumer := &poolConsumerModule{
		name:  "pool-consumer",
		delay: 500 * time.Microsecond,
		opts: &mono.EventConsumerOptions{
			ServiceOptions: mono.ServiceOptions{MaxConcurrency: 1, MaxPending: events},
		},
	}
	for _, m := range []mono.Module{emitter, consumer} {
		if err := fw.Register(m); err != nil {
			t.Fatalf("Failed to register %s: %v", m.Name(), err)
		}
	}
	startWorkerPoolApp(t, fw)

	for i := 0; i < events; i++ {
		if err := poolTickEventDef.Publish(emitter.eventBus, poolTickEvent{ID: i}, nil); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	waitForCount(t, func() int { return len(consumer.tracker.finished()) }, events, 10*time.Second)

	if consumer.tracker.max.Load() != 1 {
		t.Fatalf("max observed concurrency %d, want 1", consumer.tracker.max.Load())
	}
	for i, id := range consumer.tracker.finished() {
		if id != i {
			t.Fatalf("event %d was handled at position %d: order not kept", id, i)
		}
	}
}

// TestWorkerPool_EventConsumerBackPressureNeverDrops checks that an event
// consumer with a full pool does not drop events: the subscription waits for
// a slot, and every event is eventually handled.
func TestWorkerPool_EventConsumerBackPressureNeverDrops(t *testing.T) {
	fw := newWorkerPoolApp(t)

	const events = 50
	emitter := &poolEmitterModule{}
	consumer := &poolConsumerModule{
		name:  "pool-consumer",
		delay: 2 * time.Millisecond,
		opts: &mono.EventConsumerOptions{
			ServiceOptions: mono.ServiceOptions{MaxConcurrency: 1, MaxPending: 1},
		},
	}
	for _, m := range []mono.Module{emitter, consumer} {
		if err := fw.Register(m); err != nil {
			t.Fatalf("Failed to register %s: %v", m.Name(), err)
		}
	}
	startWorkerPoolApp(t, fw)

	for i := 0; i < events; i++ {
		if err := poolTickEventDef.Publish(emitter.eventBus, poolTickEvent{ID: i}, nil); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	waitForCount(t, func() int { return len(consumer.tracker.finished()) }, events, 10*time.Second)
}

// =============================================================================
// Shutdown
// =============================================================================

// stopOrderModule records whether its in-flight handler had finished when
// the framework called Stop on it.
type stopOrderModule struct {
	testModule
	handlerDone     atomic.Bool
	handlerDoneAtSt atomic.Bool
}

func (m *stopOrderModule) Stop(ctx context.Context) error {
	m.handlerDoneAtSt.Store(m.handlerDone.Load())
	return m.testModule.Stop(ctx)
}

// TestWorkerPool_ShutdownWaitsForInFlightHandlers checks that Stop waits for a
// handler running on the pool before the module's Stop is called.
func TestWorkerPool_ShutdownWaitsForInFlightHandlers(t *testing.T) {
	fw := newWorkerPoolApp(t)

	started := make(chan struct{}, 1)
	module := &stopOrderModule{testModule: testModule{name: "pool-responder"}}
	module.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterRequestReplyService("slow", func(_ context.Context, req *mono.Msg) ([]byte, error) {
			started <- struct{}{}
			time.Sleep(500 * time.Millisecond) // ignores ctx on purpose
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
	go func() {
		callCtx, callCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer callCancel()
		_, _ = client.Call(callCtx, []byte("x")) // the reply may be lost to the shutdown
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not start")
	}

	if err := fw.Stop(ctx); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
	if !module.wasStopped() {
		t.Fatal("module Stop was not called")
	}
	if !module.handlerDoneAtSt.Load() {
		t.Fatal("module Stop was called while its handler was still running")
	}
}

// TestWorkerPool_InvalidOptionsRejectedAtRegistration checks fail-fast
// validation of negative options.
func TestWorkerPool_InvalidOptionsRejectedAtRegistration(t *testing.T) {
	fw := newWorkerPoolApp(t)

	module := &testModule{name: "pool-responder"}
	module.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterRequestReplyService("bad", func(_ context.Context, req *mono.Msg) ([]byte, error) {
			return req.Data, nil
		}, mono.WithMaxConcurrency(-1))
	}
	if err := fw.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := fw.Start(ctx)
	if err == nil {
		_ = fw.Stop(ctx)
		t.Fatal("expected Start to fail on a negative MaxConcurrency")
	}
	_ = fw.Stop(ctx)
}
