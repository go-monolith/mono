//go:build integration
// +build integration

package integration_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-monolith/mono"
	"github.com/go-monolith/mono/pkg/types"
	"github.com/nats-io/nats.go"
)

// These tests cover issue #23: framework NATS client options and
// per-subscription stats.

// TestNATSClientOptions_NameAndUserOptionsReachTheConnection checks that the
// configured connection name and extra nats.go options are applied to the
// framework's client connection.
func TestNATSClientOptions_NameAndUserOptionsReachTheConnection(t *testing.T) {
	fw := newWorkerPoolApp(t,
		mono.WithNATSClientName("stats-app"),
		mono.WithNATSClientOptions(nats.ReconnectWait(3*time.Second)),
	)
	module := &testModule{name: "probe"}
	if err := fw.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}
	startWorkerPoolApp(t, fw)

	withConn, ok := fw.EventBus("probe").(types.EventBusWithConn[*nats.Conn])
	if !ok {
		t.Fatal("event bus does not expose its connection")
	}
	conn := withConn.Conn()
	if conn.Opts.Name != "stats-app" {
		t.Fatalf("connection name = %q, want stats-app", conn.Opts.Name)
	}
	if conn.Opts.ReconnectWait != 3*time.Second {
		t.Fatalf("reconnect wait = %s, want 3s (user option)", conn.Opts.ReconnectWait)
	}
	if conn.Opts.AsyncErrorCB == nil {
		t.Fatal("framework error handler should be installed")
	}
}

// TestSubscriptionStats_ReportsServicesAndPools checks that
// MonoFramework.SubscriptionStats lists every pooled subscription with its
// NATS counters and pool bounds, that delivered counts grow with traffic, and
// that overload rejections are counted.
func TestSubscriptionStats_ReportsServicesAndPools(t *testing.T) {
	fw := newWorkerPoolApp(t)

	release := make(chan struct{})
	started := make(chan struct{}, 8)
	module := &testModule{name: "orders"}
	module.registerServicesFunc = func(container mono.ServiceContainer) error {
		if err := container.RegisterRequestReplyService("check", func(_ context.Context, req *mono.Msg) ([]byte, error) {
			return req.Data, nil
		}, mono.WithMaxConcurrency(4), mono.WithPendingLimits(2000, 1<<20)); err != nil {
			return err
		}
		return container.RegisterRequestReplyService("busy", func(_ context.Context, req *mono.Msg) ([]byte, error) {
			started <- struct{}{}
			<-release
			return req.Data, nil
		}, mono.WithMaxConcurrency(1), mono.WithMaxPending(1))
	}
	emitter := &poolEmitterModule{}
	consumer := &poolConsumerModule{name: "pool-consumer", delay: time.Millisecond}
	for _, m := range []mono.Module{module, emitter, consumer} {
		if err := fw.Register(m); err != nil {
			t.Fatalf("Failed to register %s: %v", m.Name(), err)
		}
	}
	if stats := fw.SubscriptionStats(); stats != nil {
		t.Fatalf("stats before Start = %v, want nil", stats)
	}
	startWorkerPoolApp(t, fw)
	defer close(release)

	byName := func() map[string]mono.SubscriptionStats {
		out := map[string]mono.SubscriptionStats{}
		for _, s := range fw.SubscriptionStats() {
			out[s.Kind+"/"+s.Name] = s
		}
		return out
	}
	stats := byName()
	check, ok := stats["request_reply/check"]
	if !ok {
		t.Fatalf("missing request_reply/check in %v", stats)
	}
	if check.Module != "orders" || check.Subject != "services.orders.check" || check.QueueGroup != "check" || check.MaxConcurrency != 4 {
		t.Fatalf("check stats = %+v", check)
	}
	if _, ok := stats["event_consumer/pool-emitter.Tick"]; !ok {
		t.Fatalf("missing event consumer stats in %v", stats)
	}

	// Traffic shows up in Delivered.
	client, err := module.container.GetRequestReplyService("check")
	if err != nil {
		t.Fatalf("Failed to get client: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i := 0; i < 5; i++ {
		if _, err := client.Call(ctx, []byte("x")); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	if got := byName()["request_reply/check"].Delivered; got != 5 {
		t.Fatalf("Delivered = %d, want 5", got)
	}

	// Overload: one worker busy, one queued, the third is rejected and counted.
	busy, err := module.container.GetRequestReplyService("busy")
	if err != nil {
		t.Fatalf("Failed to get client: %v", err)
	}
	results := make(chan error, 3)
	for i := 0; i < 3; i++ {
		go func() {
			callCtx, callCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer callCancel()
			_, err := busy.Call(callCtx, []byte("b"))
			results <- err
		}()
		if i == 0 {
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("busy handler did not start")
			}
		} else {
			time.Sleep(100 * time.Millisecond)
		}
	}
	waitForCount(t, func() int { return int(byName()["request_reply/busy"].Rejected) }, 1, 5*time.Second)
	b := byName()["request_reply/busy"]
	if b.PoolActive != 1 || b.PoolQueued != 1 {
		t.Fatalf("busy pool active/queued = %d/%d, want 1/1", b.PoolActive, b.PoolQueued)
	}
}

// TestSubscriptionStats_NilAfterStop checks the accessor is safe once the
// application has stopped.
func TestSubscriptionStats_NilAfterStop(t *testing.T) {
	fw := newWorkerPoolApp(t, mono.WithSubscriptionStatsLogInterval(50*time.Millisecond))
	module := &testModule{name: "orders"}
	module.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterRequestReplyService("check", func(_ context.Context, req *mono.Msg) ([]byte, error) {
			return req.Data, nil
		})
	}
	if err := fw.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := fw.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if len(fw.SubscriptionStats()) != 1 {
		t.Fatal("expected one subscription while running")
	}
	time.Sleep(120 * time.Millisecond) // let the periodic log tick at least once
	if err := fw.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if stats := fw.SubscriptionStats(); stats != nil {
		t.Fatalf("stats after Stop = %v, want nil", stats)
	}
}

// waitForLog waits until the log output contains every substring.
func waitForLog(t *testing.T, logs *syncBuffer, timeout time.Duration, substrings ...string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		out := logs.String()
		missing := ""
		for _, s := range substrings {
			if !strings.Contains(out, s) {
				missing = s
				break
			}
		}
		if missing == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("log output never contained %q; output:\n%s", missing, out)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// newLoggingApp creates an in-process application whose JSON debug log is
// captured in logs.
func newLoggingApp(t *testing.T, logs *syncBuffer, opts ...mono.MonoFrameworkOption) mono.MonoApplication {
	t.Helper()
	opts = append([]mono.MonoFrameworkOption{
		mono.WithNATSDontListen(),
		mono.WithNATSInProcessConn(),
		mono.WithLogOutput(logs),
		mono.WithLogFormat(mono.LogFormatJSON),
		mono.WithLogLevel(mono.LogLevelDebug),
	}, opts...)
	fw, err := mono.NewMonoApplication(opts...)
	if err != nil {
		t.Fatalf("Failed to create application: %v", err)
	}
	return fw
}

// TestNATSClientOptions_SlowConsumerIsLoggedWithServiceAndCounted forces a
// slow consumer on a real service (issue #23): a serial queue group with a
// one-message client buffer and a blocked handler. The framework logger must
// report it with the module and service derived from the subject, and
// SubscriptionStats must count the dropped messages.
func TestNATSClientOptions_SlowConsumerIsLoggedWithServiceAndCounted(t *testing.T) {
	logs := &syncBuffer{}
	fw := newLoggingApp(t, logs)

	gate := make(chan struct{})
	defer close(gate)
	module := &testModule{name: "slow"}
	module.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterQueueGroupService("jobs", mono.QGHP{
			QueueGroup: "workers",
			Handler:    func(context.Context, *mono.Msg) error { <-gate; return nil },
			Options:    mono.ServiceOptions{MaxConcurrency: 1, MaxPending: 1, PendingMsgs: 1, PendingBytes: -1},
		})
	}
	if err := fw.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}
	startWorkerPoolApp(t, fw)

	// Optimistic sends (no reply subject) are never rejected: the callback
	// blocks once the pool is full, the client buffer fills, and nats.go
	// drops what does not fit in the one-message buffer.
	for i := 0; i < 50; i++ {
		if err := module.eventBus.Publish("services.slow.jobs", []byte("x")); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	dropped := func() int {
		for _, s := range fw.SubscriptionStats() {
			if s.Module == "slow" && s.Name == "jobs" {
				return s.Dropped
			}
		}
		return 0
	}
	waitForCount(t, dropped, 1, 10*time.Second)
	waitForLog(t, logs, 10*time.Second, "NATS slow consumer", `"module":"slow"`, `"service":"jobs"`, `"subject":"services.slow.jobs"`, `"dropped":`)
}

// TestSubscriptionStats_PeriodicLogEmitsOneLinePerSubscription checks the
// optional Debug line of WithSubscriptionStatsLogInterval.
func TestSubscriptionStats_PeriodicLogEmitsOneLinePerSubscription(t *testing.T) {
	logs := &syncBuffer{}
	fw := newLoggingApp(t, logs, mono.WithSubscriptionStatsLogInterval(20*time.Millisecond))
	module := &testModule{name: "stats"}
	module.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterRequestReplyService("check", func(_ context.Context, req *mono.Msg) ([]byte, error) { return req.Data, nil })
	}
	if err := fw.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}
	startWorkerPoolApp(t, fw)

	client, err := module.container.GetRequestReplyService("check")
	if err != nil {
		t.Fatalf("Failed to get client: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.Call(ctx, []byte("x")); err != nil {
		t.Fatalf("Call: %v", err)
	}
	waitForLog(t, logs, 5*time.Second, `"msg":"Subscription stats"`, `"kind":"request_reply"`, `"module":"stats"`, `"name":"check"`, `"delivered":1`)
}
