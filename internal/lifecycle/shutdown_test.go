package lifecycle

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-monolith/mono/internal/registry"
	"github.com/go-monolith/mono/pkg/types"
	"github.com/nats-io/nats.go/jetstream"
)

// These tests cover issue #21: Stop drains first, waits for in-flight
// handlers with a live runtime context, and only then cancels the runtime
// context and stops modules.

// TestStop_RuntimeContextStaysLiveDuringDrain checks that a handler running
// when Stop begins keeps an uncancelled context until it returns, and that the
// runtime context is cancelled once Stop has finished.
func TestStop_RuntimeContextStaysLiveDuringDrain(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()
	lm := NewLifecycleManager(reg, pluginReg, eventBus, registry.NewEventRegistry(logger), logger, 0)

	release := make(chan struct{})
	started := make(chan struct{}, 1)
	var ctxErrAtEnd atomic.Value
	module := &mockModuleWithServices{
		mockModule: mockModule{name: "service-module"},
		serviceEntries: []*types.ServiceEntry{{
			Name: "svc", Type: types.ServiceTypeRequestReply,
			Subject: "services.service-module.svc", QueueGroup: "service-module",
			RequestHandler: func(ctx context.Context, msg *types.Msg) ([]byte, error) {
				started <- struct{}{}
				<-release
				ctxErrAtEnd.Store(ctx.Err() == nil)
				return msg.Data, nil
			},
		}},
	}
	if err := reg.Register(module); err != nil {
		t.Fatal(err)
	}
	if err := lm.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	handler := eventBus.getHandlers("services.service-module.svc")[0]
	handler(context.Background(), &types.Msg{Subject: "services.service-module.svc", Reply: "reply", Data: []byte("x")})
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start")
	}

	stopDone := make(chan error, 1)
	go func() { stopDone <- lm.Stop(context.Background()) }()

	// While the handler runs, Stop waits and the runtime context is still live.
	time.Sleep(100 * time.Millisecond)
	select {
	case <-stopDone:
		t.Fatal("Stop returned while the handler was running")
	default:
	}
	if lm.GetRuntimeContext().Err() != nil {
		t.Fatal("runtime context was cancelled before the drain finished")
	}
	if module.stopCalled {
		t.Fatal("module Stop was called while the handler was running")
	}

	close(release)
	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("Stop failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return after the handler finished")
	}

	if live, _ := ctxErrAtEnd.Load().(bool); !live {
		t.Fatal("handler saw a cancelled context before it finished")
	}
	if lm.GetRuntimeContext().Err() == nil {
		t.Fatal("runtime context should be cancelled after Stop")
	}
	if len(eventBus.getPublishedMessages("reply")) != 1 {
		t.Fatal("the in-flight request should have been answered")
	}
	if !module.stopCalled {
		t.Fatal("module Stop should have been called")
	}
}

// TestStop_FetchLoopStopsWhileHandlerKeepsLiveContext checks the loop/handler
// context split: cancelling the fetch context stops the loop from fetching,
// but an in-flight batch handler keeps a live context and the loop goroutine
// only returns when that handler does.
func TestStop_FetchLoopStopsWhileHandlerKeepsLiveContext(t *testing.T) {
	logger := &mockLogger{}
	lm := &lifecycleManager{
		logger:          logger,
		streamConsumers: make(map[string]context.CancelFunc),
		runtimeCtx:      context.Background(),
	}

	release := make(chan struct{})
	started := make(chan struct{}, 1)
	var ctxLiveAtEnd atomic.Bool
	var calls atomic.Int32
	entry := &types.ServiceEntry{
		Name: "s", ModuleName: "m",
		StreamConsumerConfig: &types.StreamConsumerConfig{Fetch: types.FetchConfig{BatchSize: 1, Timeout: 50 * time.Millisecond}},
		StreamConsumerHandler: func(ctx context.Context, _ []*types.Msg) error {
			if calls.Add(1) == 1 {
				started <- struct{}{}
				<-release
				ctxLiveAtEnd.Store(ctx.Err() == nil)
			}
			return nil
		},
	}
	consumer := &mockJetStreamConsumer{fetchMessages: []jetstream.Msg{&mockJetStreamMsg{data: []byte("m")}}}

	loopCtx, cancel := lm.newLoopContext()
	defer cancel()
	loopReturned := make(chan struct{})
	go func() {
		defer close(loopReturned)
		lm.runStreamConsumerLoop(loopCtx, consumer, entry)
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start")
	}

	// Stop fetching: the handler in flight is counted until it returns.
	lm.mu.Lock()
	lm.cancelFetch()
	lm.mu.Unlock()
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	if n := lm.waitInFlightHandlers(waitCtx); n != 1 {
		t.Fatalf("handlers in flight = %d, want 1 while the handler is running", n)
	}
	waitCancel()

	close(release)
	waitCtx2, waitCancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer waitCancel2()
	if n := lm.waitInFlightHandlers(waitCtx2); n != 0 {
		t.Fatalf("handlers in flight = %d after the handler returned, want 0", n)
	}
	if !ctxLiveAtEnd.Load() {
		t.Fatal("handler context was cancelled by the fetch context cancellation")
	}
	select {
	case <-loopReturned:
	case <-time.After(5 * time.Second):
		t.Fatal("loop did not return after the fetch context was cancelled")
	}
	if calls.Load() != 1 {
		t.Fatalf("handler called %d times, want 1: a batch fetched after shutdown began must not be handled", calls.Load())
	}
	if !logger.hasEntryContaining("left for redelivery") && calls.Load() == 1 {
		// The loop may have exited on ctx.Done before fetching again; both are fine.
		t.Log("loop exited without fetching another batch")
	}
}

// TestStop_IncompleteDrainIsCountedAndLogged checks the configurable drain
// timeout and the summary written when handlers are still running at the end
// of the budget.
func TestStop_IncompleteDrainIsCountedAndLogged(t *testing.T) {
	f := newBlockingRequestReplyFixture(t, types.ServiceOptions{}, 0)
	f.lm.SetDrainTimeout(150 * time.Millisecond)
	defer close(f.release)

	f.send("reply")
	f.waitStarted(t)

	start := time.Now()
	if err := f.lm.Stop(context.Background()); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed < 150*time.Millisecond || elapsed > 3*time.Second {
		t.Fatalf("Stop took %s with a 150ms drain timeout", elapsed)
	}
	if !f.logger.hasWarnContaining("Shutdown drain incomplete") || !f.logger.hasWarnContaining("pools_still_busy 1") {
		t.Error("expected a warning counting the pool still busy")
	}
	if !f.logger.hasWarnContaining("drain_timeout 150ms") {
		t.Error("expected the drain timeout in the warning")
	}
}

func TestDrainBudget(t *testing.T) {
	lm := &lifecycleManager{}
	if got := lm.drainBudget(); got != types.DefaultShutdownDrainTimeout {
		t.Fatalf("default drain budget = %s, want %s", got, types.DefaultShutdownDrainTimeout)
	}
	lm.SetDrainTimeout(7 * time.Second)
	if got := lm.drainBudget(); got != 7*time.Second {
		t.Fatalf("drain budget = %s, want 7s", got)
	}
	lm.SetDrainTimeout(-1)
	if got := lm.drainBudget(); got != types.DefaultShutdownDrainTimeout {
		t.Fatalf("negative drain timeout should select the default, got %s", got)
	}
}
