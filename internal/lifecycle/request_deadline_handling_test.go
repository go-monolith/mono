package lifecycle

import (
	"context"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-monolith/mono/internal/registry"
	"github.com/go-monolith/mono/internal/workerpool"
	"github.com/go-monolith/mono/pkg/types"
)

// deadlineFixture runs a request-reply service whose handler records the
// deadline of the context it was given, and blocks while release is open.
type deadlineFixture struct {
	lm        LifecycleManager
	eventBus  *mockEventBus
	logger    *mockLogger
	handler   types.MsgHandler
	release   chan struct{}
	started   chan struct{}
	calls     atomic.Int32
	deadlines chan time.Time // deadline seen by the handler (zero when none)
}

func newDeadlineFixture(t *testing.T, opts types.ServiceOptions, skew time.Duration) *deadlineFixture {
	t.Helper()
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()
	lm := NewLifecycleManager(reg, pluginReg, eventBus, registry.NewEventRegistry(logger), logger, 0)
	if skew > 0 {
		lm.SetDeadlineSkewAllowance(skew)
	}

	f := &deadlineFixture{
		lm: lm, eventBus: eventBus, logger: logger,
		release: make(chan struct{}), started: make(chan struct{}, 16), deadlines: make(chan time.Time, 16),
	}
	module := &mockModuleWithServices{
		mockModule: mockModule{name: "service-module"},
		serviceEntries: []*types.ServiceEntry{{
			Name: "svc", Type: types.ServiceTypeRequestReply,
			Subject: "services.service-module.svc", QueueGroup: "service-module",
			RequestHandler: func(ctx context.Context, msg *types.Msg) ([]byte, error) {
				f.calls.Add(1)
				d, _ := ctx.Deadline()
				f.deadlines <- d
				f.started <- struct{}{}
				<-f.release
				return msg.Data, nil
			},
			Options: opts,
		}},
	}
	if err := reg.Register(module); err != nil {
		t.Fatal(err)
	}
	if err := lm.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lm.Stop(context.Background()) })
	f.handler = eventBus.getHandlers("services.service-module.svc")[0]
	return f
}

func (f *deadlineFixture) send(reply string, header types.Header) {
	f.handler(context.Background(), &types.Msg{
		Subject: "services.service-module.svc", Reply: reply, Data: []byte("x"), Header: header,
	})
}

func (f *deadlineFixture) waitStarted(t *testing.T) {
	t.Helper()
	select {
	case <-f.started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start")
	}
}

// TestRequestDeadline_HandlerContextCarriesCallerDeadline checks that the
// handler's context deadline is the caller's (plus the skew allowance), and
// that a request without the headers gets no deadline, as before.
func TestRequestDeadline_HandlerContextCarriesCallerDeadline(t *testing.T) {
	f := newDeadlineFixture(t, types.ServiceOptions{}, 250*time.Millisecond)
	defer close(f.release)

	want := time.Now().Add(3 * time.Second)
	f.send("r1", types.Header{types.HeaderDeadline: {strconv.FormatInt(want.UnixMilli(), 10)}})
	f.waitStarted(t)
	got := <-f.deadlines
	if got.IsZero() {
		t.Fatal("handler context had no deadline")
	}
	// The header carries millisecond precision; the handler gets the caller's
	// deadline itself (the skew allowance only widens the drop decision).
	if diff := got.Sub(want); diff.Abs() > 5*time.Millisecond {
		t.Fatalf("handler deadline off by %s", diff)
	}

	f.send("r2", nil)
	f.waitStarted(t)
	if got := <-f.deadlines; !got.IsZero() {
		t.Fatalf("a request without deadline headers must give the handler no deadline, got %s", got)
	}

	// Mono-Timeout alone is applied relative to arrival.
	before := time.Now()
	f.send("r3", types.Header{types.HeaderTimeout: {"2000"}})
	f.waitStarted(t)
	got = <-f.deadlines
	lo, hi := before.Add(2*time.Second), time.Now().Add(2*time.Second)
	if got.Before(lo) || got.After(hi) {
		t.Fatalf("Mono-Timeout deadline %s not within [%s, %s]", got, lo, hi)
	}
}

// TestRequestDeadline_ClockSkewFallsBackToTimeout checks that when the two
// headers disagree by more than the allowance, the clock-independent
// Mono-Timeout is used (the request is not dropped) and a warning is logged.
func TestRequestDeadline_ClockSkewFallsBackToTimeout(t *testing.T) {
	f := newDeadlineFixture(t, types.ServiceOptions{}, 250*time.Millisecond)
	defer close(f.release)

	// The caller's clock is 5s behind this instance's: its absolute deadline
	// already looks 3s in the past here, but it had 2s left when it sent.
	f.send("r", types.Header{
		types.HeaderDeadline: {strconv.FormatInt(time.Now().Add(-3*time.Second).UnixMilli(), 10)},
		types.HeaderTimeout:  {"2000"},
	})
	f.waitStarted(t)
	got := <-f.deadlines
	if time.Until(got) < time.Second || time.Until(got) > 2*time.Second {
		t.Fatalf("handler deadline %s should be about 2s from arrival", got)
	}
	if !f.logger.hasWarnContaining("clock skew") {
		t.Error("expected a clock skew warning")
	}
}

// TestRequestDeadline_ExpiredWhileQueuedIsDropped checks that a request whose
// deadline passed while it waited for a worker is not handled and gets no
// reply, that it is counted in the debug log, and that later requests run.
func TestRequestDeadline_ExpiredWhileQueuedIsDropped(t *testing.T) {
	f := newDeadlineFixture(t, types.ServiceOptions{MaxConcurrency: 1, MaxPending: 10}, 50*time.Millisecond)

	// Occupy the single worker.
	f.send("blocker", nil)
	f.waitStarted(t)

	// Queue a request that expires in 100ms (plus 50ms skew), then wait past it.
	expiring := time.Now().Add(100 * time.Millisecond)
	f.send("expired-reply", types.Header{types.HeaderDeadline: {strconv.FormatInt(expiring.UnixMilli(), 10)}})
	// And one with plenty of time.
	f.send("ok-reply", types.Header{types.HeaderDeadline: {strconv.FormatInt(time.Now().Add(time.Minute).UnixMilli(), 10)}})
	time.Sleep(300 * time.Millisecond)

	close(f.release)
	eventually(t, func() bool { return len(f.eventBus.getPublishedMessages("ok-reply")) == 1 },
		"the request with time left should be answered")
	eventually(t, func() bool { return len(f.eventBus.getPublishedMessages("blocker")) == 1 },
		"the first request should be answered")

	if n := len(f.eventBus.getPublishedMessages("expired-reply")) + len(f.eventBus.getPublishedMsgs("expired-reply")); n != 0 {
		t.Fatalf("expired request got %d replies, want none (the caller is gone)", n)
	}
	if f.calls.Load() != 2 {
		t.Fatalf("handler ran %d times, want 2 (the expired request must not run)", f.calls.Load())
	}
	if !f.logger.hasEntryContaining("Expired requests dropped") || !f.logger.hasEntryContaining("expired 1 ") {
		t.Error("expected a debug line counting the expired request")
	}
}

// TestRequestDeadline_SkewAllowanceIsConfigurable checks that a deadline
// slightly in the past still runs when the allowance covers it.
func TestRequestDeadline_SkewAllowanceIsConfigurable(t *testing.T) {
	f := newDeadlineFixture(t, types.ServiceOptions{}, 2*time.Second)
	defer close(f.release)

	past := time.Now().Add(-500 * time.Millisecond)
	f.send("r", types.Header{types.HeaderDeadline: {strconv.FormatInt(past.UnixMilli(), 10)}})
	f.waitStarted(t)
	if f.calls.Load() != 1 {
		t.Fatal("a deadline within the skew allowance must still be handled")
	}
}

func TestOverloadReporter_ExpireRateLimits(t *testing.T) {
	logger := &mockLogger{}
	reporter := newOverloadReporter(logger, "service", "svc")
	pool := newTestPool()
	for i := 0; i < 20; i++ {
		reporter.expire(pool)
	}
	n := 0
	logger.mu.Lock()
	for _, e := range logger.entries {
		if len(e) > 6 && e[:6] == "DEBUG:" && containsStr(e, "Expired requests dropped") {
			n++
		}
	}
	logger.mu.Unlock()
	if n != 1 {
		t.Fatalf("got %d debug lines for 20 expirations within one interval, want 1", n)
	}
	reporter.lastExpiredLog.Store(time.Now().Add(-2 * overloadLogInterval).UnixNano())
	reporter.expire(pool)
	if !logger.hasEntryContaining("expired 20 ") {
		t.Error("second line should carry the 20 expirations accumulated since the first")
	}
}

func newTestPool() *workerpool.Pool { return workerpool.New(1, 1) }

func containsStr(s, sub string) bool { return strings.Contains(s, sub) }
