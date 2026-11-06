package lifecycle

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-monolith/mono/internal/registry"
	"github.com/go-monolith/mono/internal/workerpool"
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

func TestStatsLog_DisabledByDefault(t *testing.T) {
	lm := &lifecycleManager{logger: &mockLogger{}}
	lm.startStatsLog() // interval 0 and nil fetch context: must be a no-op
	lm.SetStatsLogInterval(time.Hour)
	lm.startStatsLog() // nil fetch context: still a no-op
}
