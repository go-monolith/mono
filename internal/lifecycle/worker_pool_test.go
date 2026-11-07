package lifecycle

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-monolith/mono/internal/registry"
	"github.com/go-monolith/mono/internal/workerpool"
	"github.com/go-monolith/mono/pkg/types"
)

// hasEntryContaining reports whether any log line, at any level, contains substr.
func (m *mockLogger) hasEntryContaining(substr string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, entry := range m.entries {
		if strings.Contains(entry, substr) {
			return true
		}
	}
	return false
}

// countWarnsContaining returns the number of WARN lines containing substr.
func (m *mockLogger) countWarnsContaining(substr string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, entry := range m.entries {
		if strings.HasPrefix(entry, "WARN:") && strings.Contains(entry, substr) {
			n++
		}
	}
	return n
}

// blockingRequestReplyFixture starts a lifecycle manager with one request-reply
// service whose handler blocks until release is closed, on a pool bounded by opts.
type blockingRequestReplyFixture struct {
	lm       LifecycleManager
	eventBus *mockEventBus
	logger   *mockLogger
	handler  types.MsgHandler
	release  chan struct{}
	started  chan struct{}
}

func newBlockingRequestReplyFixture(t *testing.T, opts types.ServiceOptions, defaultConcurrency int) *blockingRequestReplyFixture {
	t.Helper()
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()
	eventRegistry := registry.NewEventRegistry(logger)

	lm := NewLifecycleManager(reg, pluginReg, eventBus, eventRegistry, logger, 0)
	lm.SetDefaultServiceConcurrency(defaultConcurrency)

	f := &blockingRequestReplyFixture{
		lm:       lm,
		eventBus: eventBus,
		logger:   logger,
		release:  make(chan struct{}),
		started:  make(chan struct{}, 64),
	}

	serviceModule := &mockModuleWithServices{
		mockModule: mockModule{name: "service-module"},
		serviceEntries: []*types.ServiceEntry{
			{
				Name:    "test-service",
				Type:    types.ServiceTypeRequestReply,
				Subject: "services.service-module.test-service",
				RequestHandler: func(ctx context.Context, msg *types.Msg) ([]byte, error) {
					f.started <- struct{}{}
					<-f.release
					return msg.Data, nil
				},
				QueueGroup: "service-module",
				Options:    opts,
			},
		},
	}
	if err := reg.Register(serviceModule); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}
	if err := lm.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	handlers := eventBus.getHandlers("services.service-module.test-service")
	if len(handlers) != 1 {
		t.Fatalf("expected 1 handler, got %d", len(handlers))
	}
	f.handler = handlers[0]
	return f
}

func (f *blockingRequestReplyFixture) send(reply string) {
	f.handler(context.Background(), &types.Msg{
		Subject: "services.service-module.test-service",
		Reply:   reply,
		Data:    []byte("request"),
	})
}

func (f *blockingRequestReplyFixture) waitStarted(t *testing.T) {
	t.Helper()
	select {
	case <-f.started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start")
	}
}

// requireErrorReply asserts that exactly one framework error response of the
// given type was published to reply.
func requireErrorReply(t *testing.T, eventBus *mockEventBus, reply, errorType string) {
	t.Helper()
	msgs := eventBus.getPublishedMsgs(reply)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 error response on %q, got %d", reply, len(msgs))
	}
	h := msgs[0].Header
	if got := h[types.HeaderError]; len(got) != 1 || got[0] != "true" {
		t.Fatalf("%s header = %v, want [true]", types.HeaderError, got)
	}
	if got := h[types.HeaderErrorType]; len(got) != 1 || got[0] != errorType {
		t.Fatalf("%s header = %v, want [%s]", types.HeaderErrorType, got, errorType)
	}
	if got := h[types.HeaderErrorMessage]; len(got) != 1 || got[0] == "" {
		t.Fatalf("%s header = %v, want a message", types.HeaderErrorMessage, got)
	}
}

// TestWorkerPool_RequestReplyOverloadReply checks that a request arriving
// while the pool and its queue are full is answered with an overloaded error
// instead of waiting, and that accepted requests still complete.
func TestWorkerPool_RequestReplyOverloadReply(t *testing.T) {
	f := newBlockingRequestReplyFixture(t, types.ServiceOptions{MaxConcurrency: 1, MaxPending: 1}, 0)
	defer func() { _ = f.lm.Stop(context.Background()) }()

	f.send("reply-1") // runs
	f.waitStarted(t)
	f.send("reply-2") // queued
	f.send("reply-3") // rejected

	requireErrorReply(t, f.eventBus, "reply-3", types.ErrorTypeOverloaded)
	if !f.logger.hasWarnContaining("rejected") {
		t.Error("expected a warning about rejected messages")
	}
	if len(f.eventBus.getPublishedMessages("reply-1")) != 0 {
		t.Error("blocked request must not have been answered yet")
	}

	close(f.release)
	eventually(t, func() bool {
		return len(f.eventBus.getPublishedMessages("reply-1")) == 1 &&
			len(f.eventBus.getPublishedMessages("reply-2")) == 1
	}, "accepted requests should be answered once the worker is released")
	if len(f.eventBus.getPublishedMsgs("reply-1")) != 0 || len(f.eventBus.getPublishedMsgs("reply-2")) != 0 {
		t.Error("accepted requests must not get an error response")
	}
}

// TestWorkerPool_ErrorReplyPublishFailuresAreLogged covers the paths where the
// framework's own error response (overloaded, shutting-down) cannot be
// published: the failure is logged and nothing else breaks.
func TestWorkerPool_ErrorReplyPublishFailuresAreLogged(t *testing.T) {
	t.Run("overloaded reply", func(t *testing.T) {
		f := newBlockingRequestReplyFixture(t, types.ServiceOptions{MaxConcurrency: 1, MaxPending: 1}, 0)
		defer func() { _ = f.lm.Stop(context.Background()) }()
		defer close(f.release)

		f.send("reply-1")
		f.waitStarted(t)
		f.send("reply-2")
		f.eventBus.setPublishError(errors.New("connection closed"))
		f.send("reply-3")

		if !f.logger.hasErrorContaining("Failed to publish overloaded response") {
			t.Error("expected the failed overloaded reply to be logged")
		}
	})

	t.Run("shutting-down reply", func(t *testing.T) {
		f := newBlockingRequestReplyFixture(t, types.ServiceOptions{}, 0)
		close(f.release)
		if err := f.lm.Stop(context.Background()); err != nil {
			t.Fatalf("Stop failed: %v", err)
		}
		f.eventBus.setPublishError(errors.New("connection closed"))
		f.send("late-reply")

		if !f.logger.hasErrorContaining("Failed to publish shutting-down response") {
			t.Error("expected the failed shutting-down reply to be logged")
		}
	})
}

// TestWorkerPool_RequestReplyAfterShutdownGetsShuttingDownReply checks that a
// request delivered after the pools are closed is answered with a
// shutting-down error rather than being silently dropped.
func TestWorkerPool_RequestReplyAfterShutdownGetsShuttingDownReply(t *testing.T) {
	f := newBlockingRequestReplyFixture(t, types.ServiceOptions{}, 0)
	close(f.release)

	if err := f.lm.Stop(context.Background()); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	f.send("late-reply")
	requireErrorReply(t, f.eventBus, "late-reply", types.ErrorTypeShuttingDown)
}

// TestWorkerPool_DefaultServiceConcurrencyApplied checks that the framework
// default is used when a registration leaves MaxConcurrency at 0, and that an
// explicit value wins over it.
func TestWorkerPool_DefaultServiceConcurrencyApplied(t *testing.T) {
	t.Run("framework default", func(t *testing.T) {
		f := newBlockingRequestReplyFixture(t, types.ServiceOptions{}, 1)
		defer func() { _ = f.lm.Stop(context.Background()) }()
		defer close(f.release)

		if !f.logger.hasEntryContaining("max_concurrency 1 ") {
			t.Fatal("expected the pool to be created with max_concurrency 1")
		}
		// With one worker and the default queue (the floor), the request after
		// the queue fills is rejected.
		f.send("r-0")
		f.waitStarted(t)
		for i := 1; i <= types.DefaultMaxPendingFloor; i++ {
			f.send("queued")
		}
		f.send("rejected")
		requireErrorReply(t, f.eventBus, "rejected", types.ErrorTypeOverloaded)
	})

	t.Run("explicit option wins", func(t *testing.T) {
		f := newBlockingRequestReplyFixture(t, types.ServiceOptions{MaxConcurrency: 2, MaxPending: 1}, 1)
		defer func() { _ = f.lm.Stop(context.Background()) }()
		defer close(f.release)

		f.send("r-0")
		f.waitStarted(t)
		f.send("r-1")
		f.waitStarted(t) // second worker started: concurrency is 2, not the default 1
	})
}

// TestWorkerPool_StopWaitsForInFlightHandlers checks that Stop does not stop
// modules while a handler is still running, and that it gives up after the
// drain timeout or when its context is done.
func TestWorkerPool_StopWaitsForInFlightHandlers(t *testing.T) {
	t.Run("waits for the handler", func(t *testing.T) {
		f := newBlockingRequestReplyFixture(t, types.ServiceOptions{}, 0)
		f.send("reply")
		f.waitStarted(t)

		stopDone := make(chan error, 1)
		go func() { stopDone <- f.lm.Stop(context.Background()) }()

		select {
		case <-stopDone:
			t.Fatal("Stop returned while the handler was still running")
		case <-time.After(100 * time.Millisecond):
		}

		close(f.release)
		select {
		case err := <-stopDone:
			if err != nil {
				t.Fatalf("Stop failed: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Stop did not return after the handler finished")
		}
		if !f.logger.hasEntryContaining("NATS subscriptions teardown complete") {
			t.Error("expected Stop to report that in-flight handlers finished")
		}
	})

	t.Run("gives up after the drain timeout with a background context", func(t *testing.T) {
		f := newBlockingRequestReplyFixture(t, types.ServiceOptions{}, 0)
		f.lm.SetDrainTimeout(200 * time.Millisecond)
		defer close(f.release)
		f.send("reply")
		f.waitStarted(t)

		start := time.Now()
		if err := f.lm.Stop(context.Background()); err != nil {
			t.Fatalf("Stop failed: %v", err)
		}
		if elapsed := time.Since(start); elapsed > 3*time.Second {
			t.Fatalf("Stop took %s with a 200ms drain timeout and a stuck handler", elapsed)
		}
		if !f.logger.hasWarnContaining("Shutdown drain incomplete") {
			t.Error("expected a warning that handlers were still running")
		}
	})

	t.Run("gives up when the context is done", func(t *testing.T) {
		f := newBlockingRequestReplyFixture(t, types.ServiceOptions{}, 0)
		defer close(f.release)
		f.send("reply")
		f.waitStarted(t)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		start := time.Now()
		if err := f.lm.Stop(ctx); err != nil {
			t.Fatalf("Stop failed: %v", err)
		}
		if elapsed := time.Since(start); elapsed > 3*time.Second {
			t.Fatalf("Stop took %s with a 100ms context", elapsed)
		}
		if !f.logger.hasWarnContaining("Shutdown drain incomplete") {
			t.Error("expected a warning that handlers were still running")
		}
	})
}

// TestWorkerPool_SerialByDefaultForQueueGroupsAndEvents checks that a queue
// group pair and an event consumer registered without options get a pool of
// one worker (serial, in order) even when the framework default is higher,
// while a request-reply service gets the framework default.
func TestWorkerPool_SerialByDefaultForQueueGroupsAndEvents(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()
	eventReg := newMockEventRegistry()
	eventReg.consumerEntries = []types.EventConsumerEntry{{
		EventDef: types.BaseEventDefinition{
			Name: "TestEvent", Version: "v1", ModuleName: "test-producer",
			Subject: "events.test-producer.v1.test-event",
		},
		Handler:    func(context.Context, *types.Msg) error { return nil },
		Module:     &mockModule{name: "consumer-module"},
		QueueGroup: "consumers",
	}}

	serviceModule := &mockModuleWithServices{
		mockModule: mockModule{name: "service-module"},
		serviceEntries: []*types.ServiceEntry{
			{
				Name: "rr", Type: types.ServiceTypeRequestReply,
				Subject:        "services.service-module.rr",
				RequestHandler: func(context.Context, *types.Msg) ([]byte, error) { return nil, nil },
				QueueGroup:     "service-module",
			},
			{
				Name: "qg", Type: types.ServiceTypeQueueGroup,
				Subject: "services.service-module.qg",
				QueueHandlers: []types.QGHP{{
					QueueGroup: "workers",
					Handler:    func(context.Context, *types.Msg) error { return nil },
				}},
			},
		},
	}
	if err := reg.Register(serviceModule); err != nil {
		t.Fatal(err)
	}

	lm := NewLifecycleManager(reg, pluginReg, eventBus, eventReg, logger, 0)
	lm.SetDefaultServiceConcurrency(7)
	if err := lm.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lm.Stop(context.Background()) }()

	if !logger.hasEntryContaining("kind request_reply name rr queue_group rr max_concurrency 7 ") {
		t.Error("request-reply pool should use the framework default of 7")
	}
	if !logger.hasEntryContaining("kind queue_group name qg queue_group workers max_concurrency 1 ") {
		t.Error("queue group pool should be serial by default")
	}
	if !logger.hasEntryContaining("kind event_consumer name TestEvent queue_group consumers max_concurrency 1 ") {
		t.Error("event consumer pool should be serial by default")
	}
}

// TestWorkerPool_QueueGroupOverloadReply checks that a queue group message
// rejected by a full pool gets an error response instead of an ACK, so the
// sender can tell it was not accepted.
func TestWorkerPool_QueueGroupOverloadReply(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()
	eventRegistry := registry.NewEventRegistry(logger)
	lm := NewLifecycleManager(reg, pluginReg, eventBus, eventRegistry, logger, 0)

	release := make(chan struct{})
	started := make(chan struct{}, 8)
	serviceModule := &mockModuleWithQueueGroupServices{
		mockModule: mockModule{name: "qg-module"},
		serviceEntries: []*types.ServiceEntry{
			{
				Name:    "qg-service",
				Type:    types.ServiceTypeQueueGroup,
				Subject: "services.qg-module.qg-service",
				QueueHandlers: []types.QGHP{
					{
						QueueGroup: "qg-module",
						Handler: func(ctx context.Context, msg *types.Msg) error {
							started <- struct{}{}
							<-release
							return nil
						},
						Options: types.ServiceOptions{MaxConcurrency: 1, MaxPending: 1},
					},
				},
			},
		},
	}
	if err := reg.Register(serviceModule); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}
	if err := lm.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = lm.Stop(context.Background()) }()
	defer close(release)

	handlers := eventBus.getHandlers("services.qg-module.qg-service")
	if len(handlers) != 1 {
		t.Fatalf("expected 1 handler, got %d", len(handlers))
	}
	send := func(reply string) {
		handlers[0](context.Background(), &types.Msg{Subject: "services.qg-module.qg-service", Reply: reply, Data: []byte("m")})
	}

	send("ack-1")
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start")
	}
	send("ack-2")
	send("ack-3")

	// Accepted messages were ACKed with an empty publish.
	if len(eventBus.getPublishedMessages("ack-1")) != 1 || len(eventBus.getPublishedMessages("ack-2")) != 1 {
		t.Fatal("accepted messages should have been ACKed")
	}
	// The rejected one got an error response and no ACK.
	if len(eventBus.getPublishedMessages("ack-3")) != 0 {
		t.Fatal("rejected message must not be ACKed")
	}
	requireErrorReply(t, eventBus, "ack-3", types.ErrorTypeOverloaded)
}

// TestWorkerPool_EventConsumerBackPressure checks that an event (no reply
// subject) is never dropped when the pool is full: the subscription callback
// waits for a slot instead.
func TestWorkerPool_EventConsumerBackPressure(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()
	eventReg := newMockEventRegistry()

	release := make(chan struct{})
	var mu sync.Mutex
	var handled []string
	eventReg.consumerEntries = []types.EventConsumerEntry{
		{
			EventDef: types.BaseEventDefinition{
				Name: "TestEvent", Version: "v1", ModuleName: "test-producer",
				Subject: "events.test-producer.v1.test-event",
			},
			Handler: func(ctx context.Context, msg *types.Msg) error {
				<-release
				mu.Lock()
				handled = append(handled, string(msg.Data))
				mu.Unlock()
				return nil
			},
			Module:     &mockModule{name: "consumer-module"},
			QueueGroup: "consumers",
			Options:    types.ServiceOptions{MaxConcurrency: 1, MaxPending: 1},
		},
	}
	if err := reg.Register(&mockModule{name: "test"}); err != nil {
		t.Fatal(err)
	}
	lm := NewLifecycleManager(reg, pluginReg, eventBus, eventReg, logger, 0)
	if err := lm.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lm.Stop(context.Background()) }()

	handlers := eventBus.getHandlers("events.test-producer.v1.test-event")
	if len(handlers) != 1 {
		t.Fatalf("expected 1 handler, got %d", len(handlers))
	}

	// First runs, second queues, third must block in the callback.
	for _, id := range []string{"1", "2"} {
		handlers[0](context.Background(), &types.Msg{Subject: "events.test-producer.v1.test-event", Data: []byte(id)})
	}
	thirdReturned := make(chan struct{})
	go func() {
		handlers[0](context.Background(), &types.Msg{Subject: "events.test-producer.v1.test-event", Data: []byte("3")})
		close(thirdReturned)
	}()
	select {
	case <-thirdReturned:
		t.Fatal("third event was not held back while the pool was full")
	case <-time.After(100 * time.Millisecond):
	}
	if logger.hasWarnContaining("rejected") || logger.hasWarnContaining("dropped") {
		t.Fatal("events must not be rejected or dropped")
	}

	close(release)
	select {
	case <-thirdReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("third event was not accepted after a slot freed")
	}
	eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(handled) == 3
	}, "all three events should be handled")
	mu.Lock()
	defer mu.Unlock()
	for i, want := range []string{"1", "2", "3"} {
		if handled[i] != want {
			t.Fatalf("handled = %v, want in order 1 2 3", handled)
		}
	}
}

// TestOverloadReporter_RateLimits checks that sustained rejections produce
// one warning per interval, carrying the count.
func TestOverloadReporter_RateLimits(t *testing.T) {
	logger := &mockLogger{}
	reporter := newOverloadReporter(logger, "service", "svc")
	pool := workerpool.New(1, 1)

	for i := 0; i < 50; i++ {
		reporter.reject(pool)
	}
	if n := logger.countWarnsContaining("rejected"); n != 1 {
		t.Fatalf("got %d warnings for 50 rejections within one interval, want 1", n)
	}
	if !logger.hasWarnContaining("rejected 1 ") {
		t.Error("first warning should report the first rejection")
	}

	// Pretend the interval has passed: the next rejection reports the 49 others.
	reporter.lastLog.Store(time.Now().Add(-2 * overloadLogInterval).UnixNano())
	reporter.reject(pool)
	if n := logger.countWarnsContaining("rejected"); n != 2 {
		t.Fatalf("got %d warnings after the interval passed, want 2", n)
	}
	if !logger.hasWarnContaining("rejected 50 ") {
		t.Error("second warning should carry the rejections accumulated since the first")
	}
}

// TestSubmitToPool_UnexpectedError covers the fallback branch: an error that
// is neither overloaded nor closed is logged and the message is not accepted.
func TestFrameworkErrorReply(t *testing.T) {
	msg := frameworkErrorReply("reply.subject", types.ErrorTypeOverloaded, "busy")
	if msg.Subject != "reply.subject" {
		t.Fatalf("Subject = %q", msg.Subject)
	}
	if msg.Header[types.HeaderError][0] != "true" ||
		msg.Header[types.HeaderErrorType][0] != types.ErrorTypeOverloaded ||
		msg.Header[types.HeaderErrorMessage][0] != "busy" {
		t.Fatalf("unexpected headers: %v", msg.Header)
	}
	if msg.Data != nil {
		t.Fatal("error replies carry no payload")
	}
}

// TestSubmitToPool_ClosedWithoutReplyDropsWithWarning checks that a message
// without a reply subject arriving after shutdown is dropped with a warning
// (it cannot be answered and nothing will run it).
func TestSubmitToPool_ClosedWithoutReplyDropsWithWarning(t *testing.T) {
	logger := &mockLogger{}
	eventBus := newMockEventBus()
	pool := workerpool.New(1, 1)
	if err := pool.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	reporter := newOverloadReporter(logger, "event", "e")

	accepted := submitToPool(pool, eventBus, logger, reporter, &types.Msg{Subject: "events.x"}, func() {
		t.Error("job must not run on a closed pool")
	})
	if accepted {
		t.Fatal("closed pool must not accept")
	}
	if !logger.hasWarnContaining("dropped") || !logger.hasWarnContaining("dropped 1 ") {
		t.Error("expected a rate-limited warning counting the dropped message")
	}
	// A second drop within the interval is counted, not logged again.
	submitToPool(pool, eventBus, logger, reporter, &types.Msg{Subject: "events.x"}, func() {})
	if n := logger.countWarnsContaining("dropped"); n != 1 {
		t.Errorf("got %d drop warnings within one interval, want 1", n)
	}
}

// TestWaitSubscriptionDrained checks that teardown waits for a subscription's
// drain to complete (IsValid turns false) and gives up when ctx is done.
func TestWaitSubscriptionDrained(t *testing.T) {
	t.Run("returns once the subscription is invalid", func(t *testing.T) {
		sub := &mockSubscription{subject: "s"}
		go func() {
			time.Sleep(30 * time.Millisecond)
			_ = sub.Drain()
		}()
		start := time.Now()
		waitSubscriptionDrained(context.Background(), sub)
		if sub.IsValid() {
			t.Fatal("returned while the subscription was still valid")
		}
		if time.Since(start) > 2*time.Second {
			t.Fatal("took too long to notice the drain completed")
		}
	})

	t.Run("gives up when ctx is done", func(t *testing.T) {
		sub := &slowDrainingSubscription{} // IsValid is always true
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		start := time.Now()
		waitSubscriptionDrained(ctx, sub)
		if time.Since(start) > 2*time.Second {
			t.Fatal("did not stop waiting when ctx was done")
		}
	})
}

// TestWorkerPool_BufferedMessagesHandledBeforePoolsClose checks that a message
// a subscription delivers during its drain still reaches the pool: the pools
// close only after every subscription finished draining.
func TestWorkerPool_BufferedMessagesHandledBeforePoolsClose(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()
	eventReg := newMockEventRegistry()

	var handled atomic.Int32
	eventReg.consumerEntries = []types.EventConsumerEntry{
		{
			EventDef: types.BaseEventDefinition{
				Name: "TestEvent", Version: "v1", ModuleName: "test-producer",
				Subject: "events.test-producer.v1.test-event",
			},
			Handler: func(ctx context.Context, msg *types.Msg) error {
				handled.Add(1)
				return nil
			},
			Module:     &mockModule{name: "consumer-module"},
			QueueGroup: "consumers",
		},
	}
	if err := reg.Register(&mockModule{name: "test"}); err != nil {
		t.Fatal(err)
	}
	lm := NewLifecycleManager(reg, pluginReg, eventBus, eventReg, logger, 0)
	if err := lm.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	handlers := eventBus.getHandlers("events.test-producer.v1.test-event")
	if len(handlers) != 1 {
		t.Fatalf("expected 1 handler, got %d", len(handlers))
	}

	// Model nats.go delivering a buffered message during the drain: the
	// subscription stays valid until that delivery has gone through.
	impl := lm.(*lifecycleManager)
	impl.mu.Lock()
	sub := &drainDeliveringSubscription{deliver: func() {
		handlers[0](context.Background(), &types.Msg{Subject: "events.test-producer.v1.test-event", Data: []byte("buffered")})
	}}
	impl.subscriptions["consumer-module"] = []types.Subscription{sub}
	impl.mu.Unlock()

	if err := lm.Stop(context.Background()); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
	if handled.Load() != 1 {
		t.Fatalf("buffered message handled %d times, want 1", handled.Load())
	}
	if logger.hasWarnContaining("dropped") {
		t.Fatal("the buffered message must not be dropped")
	}
}

// drainDeliveringSubscription models a nats.go subscription whose Drain
// returns at once while a buffered message is still delivered to the callback
// shortly after; IsValid turns false only once that delivery has happened.
type drainDeliveringSubscription struct {
	deliver   func()
	delivered atomic.Bool
}

func (s *drainDeliveringSubscription) Drain() error {
	go func() {
		time.Sleep(30 * time.Millisecond)
		s.deliver()
		s.delivered.Store(true)
	}()
	return nil
}

func (s *drainDeliveringSubscription) Unsubscribe() error { return nil }
func (s *drainDeliveringSubscription) IsValid() bool      { return !s.delivered.Load() }
func (s *drainDeliveringSubscription) Subject() string    { return "events.test-producer.v1.test-event" }
func (s *drainDeliveringSubscription) Queue() string      { return "consumers" }
func (s *drainDeliveringSubscription) NextMsg(time.Duration) (*types.Msg, error) {
	return nil, errors.New("not implemented")
}
func (s *drainDeliveringSubscription) NextMsgWithContext(context.Context) (*types.Msg, error) {
	return nil, errors.New("not implemented")
}

// TestNewPool_PanicInJobIsLoggedAndPoolSurvives checks the last-resort panic
// handler of a subscription pool: a panic in the job itself (not the handler)
// is logged with the subscription identity and the pool keeps running jobs.
func TestNewPool_PanicInJobIsLoggedAndPoolSurvives(t *testing.T) {
	lm, logger, _ := newBareManager(t, newMockEventBus())
	lm.mu.Lock()
	lm.pools = nil // the first pool creates the map
	lm.mu.Unlock()

	pool := lm.newPool(types.ServiceOptions{MaxConcurrency: 1}, true, logger, "orders", "request-reply", "check", "check")
	if err := pool.Submit(func() { panic("job bug") }, true); err != nil {
		t.Fatalf("submit: %v", err)
	}
	done := make(chan struct{})
	if err := pool.Submit(func() { close(done) }, true); err != nil {
		t.Fatalf("submit second job: %v", err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the pool stopped running jobs after a panic")
	}
	if !logger.hasErrorContaining("Panic in worker pool job") || !logger.hasErrorContaining("check") || !logger.hasErrorContaining("job bug") {
		t.Fatalf("expected the panic to be logged with the subscription identity, got %v", logger.entries)
	}
	lm.mu.Lock()
	n := len(lm.pools["orders"])
	lm.mu.Unlock()
	if n != 1 {
		t.Fatalf("pools recorded for orders = %d, want 1", n)
	}
	_ = pool.Close(context.Background())
}
