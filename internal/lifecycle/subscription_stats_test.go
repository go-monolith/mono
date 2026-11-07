package lifecycle

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-monolith/mono/internal/registry"
	"github.com/go-monolith/mono/internal/workerpool"
	"github.com/go-monolith/mono/pkg/helper"
	"github.com/go-monolith/mono/pkg/types"
	"github.com/nats-io/nats.go"
)

// limitedSubscription is a mock subscription that records pending limits and
// serves client buffer counters, like the real nats.go-backed one.
type limitedSubscription struct {
	mockSubscription
	limitMsgs, limitBytes int
	limitCalls            int
	limitErr              error
	pendingMsgs           int
	pendingBytes          int
	dropped               int
	delivered             int64
}

func (s *limitedSubscription) SetPendingLimits(msgs, bytes int) error {
	s.limitCalls++
	s.limitMsgs, s.limitBytes = msgs, bytes
	return s.limitErr
}
func (s *limitedSubscription) Pending() (int, int, error) { return s.pendingMsgs, s.pendingBytes, nil }
func (s *limitedSubscription) Dropped() (int, error)      { return s.dropped, nil }
func (s *limitedSubscription) Delivered() (int64, error)  { return s.delivered, nil }

func TestApplyPendingLimits(t *testing.T) {
	t.Run("request-reply gets the lower defaults", func(t *testing.T) {
		sub := &limitedSubscription{}
		if err := applyPendingLimits(sub, types.ServiceOptions{}, true); err != nil {
			t.Fatal(err)
		}
		if sub.limitCalls != 1 || sub.limitMsgs != types.DefaultRequestReplyPendingMsgs || sub.limitBytes != types.DefaultRequestReplyPendingBytes {
			t.Fatalf("limits = %d/%d (calls %d), want request-reply defaults", sub.limitMsgs, sub.limitBytes, sub.limitCalls)
		}
	})
	t.Run("queue group and events keep the nats.go defaults unless set", func(t *testing.T) {
		sub := &limitedSubscription{}
		if err := applyPendingLimits(sub, types.ServiceOptions{}, false); err != nil {
			t.Fatal(err)
		}
		if sub.limitCalls != 0 {
			t.Fatal("no limits should be applied for a zero configuration")
		}
		if err := applyPendingLimits(sub, types.ServiceOptions{PendingMsgs: 500, PendingBytes: -1}, false); err != nil {
			t.Fatal(err)
		}
		if sub.limitCalls != 1 || sub.limitMsgs != 500 || sub.limitBytes != -1 {
			t.Fatalf("limits = %d/%d, want 500/-1", sub.limitMsgs, sub.limitBytes)
		}
		// A single dimension: the other one takes nats.go's default, never 0
		// (nats.go rejects a 0 limit).
		if err := applyPendingLimits(sub, types.ServiceOptions{PendingMsgs: 1000}, false); err != nil {
			t.Fatal(err)
		}
		if sub.limitMsgs != 1000 || sub.limitBytes != nats.DefaultSubPendingBytesLimit {
			t.Fatalf("limits = %d/%d, want 1000/%d", sub.limitMsgs, sub.limitBytes, nats.DefaultSubPendingBytesLimit)
		}
	})
	t.Run("explicit request-reply limits win over the defaults", func(t *testing.T) {
		sub := &limitedSubscription{}
		if err := applyPendingLimits(sub, types.ServiceOptions{PendingMsgs: 7}, true); err != nil {
			t.Fatal(err)
		}
		if sub.limitMsgs != 7 || sub.limitBytes != types.DefaultRequestReplyPendingBytes {
			t.Fatalf("limits = %d/%d", sub.limitMsgs, sub.limitBytes)
		}
	})
	t.Run("subscriptions without the capability are left alone", func(t *testing.T) {
		if err := applyPendingLimits(&mockSubscription{}, types.ServiceOptions{PendingMsgs: 1}, true); err != nil {
			t.Fatalf("plain subscription: %v", err)
		}
	})
	t.Run("errors are returned", func(t *testing.T) {
		sub := &limitedSubscription{limitErr: errors.New("closed")}
		if err := applyPendingLimits(sub, types.ServiceOptions{}, true); err == nil {
			t.Fatal("expected the SetPendingLimits error")
		}
	})
}

// TestSubscriptionStats_Snapshot checks the aggregation of client buffer
// counters, pool state and reporter totals into one snapshot per subscription.
func TestSubscriptionStats_Snapshot(t *testing.T) {
	logger := &mockLogger{}
	lm := &lifecycleManager{logger: logger}

	pool := workerpool.New(2, 8)
	reporter := newOverloadReporter(logger, "service", "svc")
	reporter.reject(pool)
	reporter.reject(pool)
	reporter.drop(pool)
	reporter.expire(pool)
	reporter.expire(pool)
	reporter.expire(pool)
	sub := &limitedSubscription{pendingMsgs: 3, pendingBytes: 300, dropped: 1, delivered: 42}
	lm.subRecords = []*subscriptionRecord{
		{kind: "request_reply", module: "orders", name: "svc", subject: "services.orders.svc", queueGroup: "svc", sub: sub, pool: pool, reporter: reporter},
		{kind: "event_consumer", module: "billing", name: "orders.Created", subject: "events.orders.created.v1", queueGroup: "billing", sub: &mockSubscription{}},
	}

	stats := lm.SubscriptionStats()
	if len(stats) != 2 {
		t.Fatalf("got %d stats, want 2", len(stats))
	}
	s := stats[0]
	want := types.SubscriptionStats{
		Kind: "request_reply", Module: "orders", Name: "svc", Subject: "services.orders.svc", QueueGroup: "svc",
		PendingMsgs: 3, PendingBytes: 300, Dropped: 1, Delivered: 42,
		MaxConcurrency: 2, MaxPending: 8,
		Rejected: 2, Expired: 3, DroppedAtShutdown: 1,
	}
	if s != want {
		t.Fatalf("stats[0] = %+v, want %+v", s, want)
	}
	// A subscription without counters or pool reports only its identity.
	if stats[1].Kind != "event_consumer" || stats[1].Delivered != 0 || stats[1].MaxConcurrency != 0 {
		t.Fatalf("stats[1] = %+v", stats[1])
	}

	// The totals are monotonic even though the rate-limited log resets its
	// own counters.
	reporter.lastLog.Store(0)
	reporter.reject(pool)
	if got := lm.SubscriptionStats()[0].Rejected; got != 3 {
		t.Fatalf("Rejected = %d, want 3", got)
	}
}

// TestSubscriptionStats_FromRunningManager checks that every pooled
// subscription set up by Start is reported, with its pool bounds.
func TestSubscriptionStats_FromRunningManager(t *testing.T) {
	f := newBlockingRequestReplyFixture(t, types.ServiceOptions{MaxConcurrency: 3, MaxPending: 9}, 0)
	defer func() { _ = f.lm.Stop(context.Background()) }()
	defer close(f.release)

	stats := f.lm.SubscriptionStats()
	if len(stats) != 1 {
		t.Fatalf("got %d stats, want 1", len(stats))
	}
	s := stats[0]
	if s.Kind != "request_reply" || s.Module != "service-module" || s.Name != "test-service" ||
		s.Subject != "services.service-module.test-service" || s.QueueGroup != "test-service" {
		t.Fatalf("identity = %+v", s)
	}
	if s.MaxConcurrency != 3 || s.MaxPending != 9 {
		t.Fatalf("pool bounds = %d/%d, want 3/9", s.MaxConcurrency, s.MaxPending)
	}

	f.send("r")
	f.waitStarted(t)
	if got := f.lm.SubscriptionStats()[0].PoolActive; got != 1 {
		t.Fatalf("PoolActive = %d, want 1 while a handler runs", got)
	}
}

// TestStatsLog_PeriodicDebugLine checks that the optional stats log emits a
// Debug line per subscription at the configured interval and stops at Stop.
func TestStatsLog_PeriodicDebugLine(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()
	lm := NewLifecycleManager(reg, pluginReg, eventBus, registry.NewEventRegistry(logger), logger, 0)
	lm.SetStatsLogInterval(20 * time.Millisecond)

	module := &mockModuleWithServices{
		mockModule: mockModule{name: "service-module"},
		serviceEntries: []*types.ServiceEntry{{
			Name: "svc", Type: types.ServiceTypeRequestReply,
			Subject: "services.service-module.svc", QueueGroup: "service-module",
			RequestHandler: func(_ context.Context, msg *types.Msg) ([]byte, error) { return msg.Data, nil },
		}},
	}
	if err := reg.Register(module); err != nil {
		t.Fatal(err)
	}
	if err := lm.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		return logger.hasEntryContaining("Subscription stats") && logger.hasEntryContaining("name svc")
	},
		"expected a periodic stats line for the service")
	if err := lm.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Stopped: no further lines after a settle period.
	time.Sleep(50 * time.Millisecond)
	logger.mu.Lock()
	before := len(logger.entries)
	logger.mu.Unlock()
	time.Sleep(80 * time.Millisecond)
	logger.mu.Lock()
	after := len(logger.entries)
	logger.mu.Unlock()
	if after != before {
		t.Fatalf("stats log kept running after Stop (%d new lines)", after-before)
	}
}

// TestStart_RetryAfterFailedSubscriptionSetup checks that a Start that fails
// while setting up subscriptions tears everything down and that a retried
// Start works: live fetch context, exactly one subscription and one stats
// record per service, and handlers that run.
func TestStart_RetryAfterFailedSubscriptionSetup(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()
	lm := NewLifecycleManager(reg, pluginReg, eventBus, registry.NewEventRegistry(logger), logger, 0)
	impl := lm.(*lifecycleManager)

	var handled atomic.Int32
	module := &mockModuleWithServices{
		mockModule: mockModule{name: "service-module"},
		serviceEntries: []*types.ServiceEntry{
			{
				Name: "first", Type: types.ServiceTypeRequestReply,
				Subject: "services.service-module.first", QueueGroup: "first",
				RequestHandler: func(_ context.Context, msg *types.Msg) ([]byte, error) { handled.Add(1); return msg.Data, nil },
			},
			{
				Name: "second", Type: types.ServiceTypeRequestReply,
				Subject: "services.service-module.second", QueueGroup: "second",
				RequestHandler: func(_ context.Context, msg *types.Msg) ([]byte, error) { return msg.Data, nil },
			},
		},
	}
	if err := reg.Register(module); err != nil {
		t.Fatal(err)
	}

	// First attempt: the second subscription fails, after the first was created.
	eventBus.queueSubscribeErr = errors.New("subscribe failed")
	eventBus.queueSubscribeErrFor = "services.service-module.second"
	if err := lm.Start(context.Background()); err == nil {
		t.Fatal("first Start should fail")
	}
	impl.mu.RLock()
	fetchErr := impl.fetchCtx.Err()
	leftover := len(impl.subscriptions) + len(impl.pools) + len(impl.subRecords)
	impl.mu.RUnlock()
	if fetchErr != nil {
		t.Fatal("fetch context must be fresh after a failed Start, or every retried fetch loop is dead")
	}
	if leftover != 0 {
		t.Fatalf("subscription state not reset after failed Start: %d leftovers", leftover)
	}
	if n := len(lm.SubscriptionStats()); n != 0 {
		t.Fatalf("no stats expected after a failed Start, got %d", n)
	}

	// Retry succeeds.
	eventBus.queueSubscribeErr = nil
	eventBus.queueSubscribeErrFor = ""
	if err := lm.Start(context.Background()); err != nil {
		t.Fatalf("retried Start failed: %v", err)
	}
	defer func() { _ = lm.Stop(context.Background()) }()

	stats := lm.SubscriptionStats()
	if len(stats) != 2 {
		t.Fatalf("got %d stats after retry, want exactly 2 (no stale duplicates): %+v", len(stats), stats)
	}
	if _, ok := impl.beginHandler(); !ok {
		t.Fatal("fetch loops must be able to admit handlers after a retried Start")
	}
	impl.endHandler()

	// The mock bus may also keep the callback of the failed attempt (entries
	// are set up in map order, so "first" may or may not have been subscribed
	// before "second" failed); the live one is the last registered.
	handlers := eventBus.getHandlers("services.service-module.first")
	if len(handlers) < 1 {
		t.Fatal("expected a live handler for first after the retried Start")
	}
	handlers[len(handlers)-1](context.Background(), &types.Msg{Subject: "services.service-module.first", Reply: "r", Data: []byte("x")})
	eventually(t, func() bool { return handled.Load() == 1 }, "handler should run after the retried Start")
}

func TestStatsLog_DisabledByDefault(t *testing.T) {
	lm := &lifecycleManager{logger: &mockLogger{}}
	lm.startStatsLog() // interval 0 and nil fetch context: must be a no-op
	lm.SetStatsLogInterval(time.Hour)
	lm.startStatsLog() // nil fetch context: still a no-op
}

// failingLimitSubscription is a subscription whose SetPendingLimits and
// Unsubscribe both fail, so the cleanup path after a pending limit error is
// exercised end to end.
type failingLimitSubscription struct {
	mockSubscription
}

func (s *failingLimitSubscription) SetPendingLimits(int, int) error {
	return errors.New("limits refused")
}

func (s *failingLimitSubscription) Unsubscribe() error { return errors.New("already gone") }

// failingLimitBus returns failingLimitSubscription for every subscription.
type failingLimitBus struct {
	*mockEventBus
}

func (b *failingLimitBus) Subscribe(subject string, handler types.MsgHandler) (types.Subscription, error) {
	if _, err := b.mockEventBus.Subscribe(subject, handler); err != nil {
		return nil, err
	}
	return &failingLimitSubscription{mockSubscription{subject: subject}}, nil
}

func (b *failingLimitBus) QueueSubscribe(subject, queue string, handler types.MsgHandler) (types.Subscription, error) {
	if _, err := b.mockEventBus.QueueSubscribe(subject, queue, handler); err != nil {
		return nil, err
	}
	return &failingLimitSubscription{mockSubscription{subject: subject}}, nil
}

// limitedEventConsumerModule registers one event consumer with pending limits.
type limitedEventConsumerModule struct {
	mockModule
}

var limitedEventDef = helper.EventDefinition[struct{}]("billing", "InvoiceIssued", "v1")

func (m *limitedEventConsumerModule) RegisterEventConsumers(reg types.EventRegistry) error {
	return reg.RegisterEventConsumerWithOptions(limitedEventDef.ToBase(), func(context.Context, *types.Msg) error { return nil }, m,
		types.EventConsumerOptions{ServiceOptions: types.ServiceOptions{PendingMsgs: 5}})
}

// TestStart_PendingLimitErrorUnsubscribesAndWarnsWhenThatFailsToo checks,
// for each subscription kind, that a failed SetPendingLimits fails Start, that
// the framework tries to unsubscribe the live subscription, and that a failed
// unsubscribe is logged rather than ignored.
func TestStart_PendingLimitErrorUnsubscribesAndWarnsWhenThatFailsToo(t *testing.T) {
	cases := []struct {
		name   string
		module types.Module
	}{
		{"request-reply", &mockModuleWithServices{
			mockModule: mockModule{name: "orders"},
			serviceEntries: []*types.ServiceEntry{{
				Name: "check", Type: types.ServiceTypeRequestReply, Subject: "services.orders.check", QueueGroup: "check",
				RequestHandler: func(_ context.Context, msg *types.Msg) ([]byte, error) { return msg.Data, nil },
			}},
		}},
		{"queue-group", &mockModuleWithServices{
			mockModule: mockModule{name: "orders"},
			serviceEntries: []*types.ServiceEntry{{
				Name: "jobs", Type: types.ServiceTypeQueueGroup, Subject: "services.orders.jobs",
				QueueHandlers: []types.QGHP{{QueueGroup: "workers", Handler: func(context.Context, *types.Msg) error { return nil },
					Options: types.ServiceOptions{PendingMsgs: 5}}},
			}},
		}},
		{"event-consumer", &limitedEventConsumerModule{mockModule: mockModule{name: "billing"}}},
	}
	for _, tc := range cases {
		module := tc.module
		t.Run(tc.name, func(t *testing.T) {
			bus := &failingLimitBus{mockEventBus: newMockEventBus()}
			lm, logger, reg := newBareManager(t, bus)
			if err := reg.Register(module); err != nil {
				t.Fatal(err)
			}
			err := lm.Start(context.Background())
			if err == nil || !strings.Contains(err.Error(), "limits refused") {
				t.Fatalf("Start error = %v, want the pending limit error", err)
			}
			if !logger.hasWarnContaining("Failed to unsubscribe after pending limit error") || !logger.hasWarnContaining("already gone") {
				t.Fatalf("expected the failed unsubscribe to be logged, got %v", logger.entries)
			}
			lm.mu.RLock()
			leftover := len(lm.subscriptions)
			lm.mu.RUnlock()
			if leftover != 0 {
				t.Fatalf("subscriptions left after failed Start = %d, want 0", leftover)
			}
		})
	}
}
