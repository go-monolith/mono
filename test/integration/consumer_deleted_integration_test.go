//go:build integration
// +build integration

package integration_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/go-monolith/mono"
)

// levelLog records every line with its level.
type levelLog struct {
	mu    sync.Mutex
	lines []string
}

func (l *levelLog) add(level, msg string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, level+": "+msg+" "+fmt.Sprint(args...))
}
func (l *levelLog) Debug(string, ...any)          {}
func (l *levelLog) Info(msg string, args ...any)  { l.add("INFO", msg, args...) }
func (l *levelLog) Warn(msg string, args ...any)  { l.add("WARN", msg, args...) }
func (l *levelLog) Error(msg string, args ...any) { l.add("ERROR", msg, args...) }
func (l *levelLog) With(...any) mono.Logger       { return l }
func (l *levelLog) WithModule(string) mono.Logger { return l }
func (l *levelLog) WithError(error) mono.Logger   { return l }
func (l *levelLog) count(level, substr string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, line := range l.lines {
		if strings.HasPrefix(line, level+": ") && strings.Contains(line, substr) {
			n++
		}
	}
	return n
}

// waitWithin fails the test when cond is not true within timeout.
func waitWithin(t *testing.T, timeout time.Duration, cond func() bool, failure string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal(failure)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestIntegration_StreamConsumer_DeletedConsumerIsReported: a consumer deleted
// under a running application (nats consumer rm) leaves its fetch loop
// without anything to pull. That is said once, at Error, with what brings it
// back; the loop is not given a new consumer, and it resumes by itself once
// the consumer exists again.
func TestIntegration_StreamConsumer_DeletedConsumerIsReported(t *testing.T) {
	ctx := context.Background()
	log := &levelLog{}
	handled := &atomic.Int64{}
	app, err := mono.NewMonoApplication(
		mono.WithCustomLogger(log),
		mono.WithJetStreamStorageDir(t.TempDir()),
		mono.WithNATSDontListen(),
		mono.WithNATSInProcessConn(),
	)
	if err != nil {
		t.Fatalf("NewMonoApplication: %v", err)
	}
	if err := app.Register(&keptSettingModule{ack: true, handled: handled}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := app.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = app.Stop(context.Background()) })

	es, err := app.EventBus("orders").EventStream()
	if err != nil {
		t.Fatalf("EventStream: %v", err)
	}
	publish := func() {
		t.Helper()
		if _, err := es.PublishMsg(ctx, &mono.Msg{Subject: "services.orders.ingest", Data: []byte("x")}); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	publish()
	waitWithin(t, 10*time.Second, func() bool { return handled.Load() == 1 }, "the first message was not handled")

	stream, err := es.Stream(ctx, "ORDERS")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	existing, err := stream.Consumer(ctx, "orders-ingest")
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	cfg := existing.CachedInfo().Config
	if err := stream.DeleteConsumer(ctx, "orders-ingest"); err != nil {
		t.Fatalf("delete the consumer: %v", err)
	}

	const gone = "Consumer no longer exists"
	waitWithin(t, 20*time.Second, func() bool { return log.count("ERROR", gone) == 1 }, "the deleted consumer was not reported")
	for _, want := range []string{"ORDERS", "orders-ingest", "restart this instance"} {
		if log.count("ERROR", want) == 0 {
			t.Errorf("the report lacks %q", want)
		}
	}

	// Not recreated by the running instance, and nothing is consumed.
	publish()
	time.Sleep(3 * time.Second)
	if _, err := stream.Consumer(ctx, "orders-ingest"); err == nil {
		t.Fatal("the running instance created the consumer again")
	}
	if got := handled.Load(); got != 1 {
		t.Fatalf("%d messages handled without a consumer", got)
	}
	if n := log.count("ERROR", gone); n != 1 {
		t.Errorf("the deleted consumer was reported %d times within seconds", n)
	}

	// What the start of another instance does: the consumer exists again,
	// and the loop of this one resumes without a restart.
	cfg.DeliverPolicy = jetstream.DeliverAllPolicy
	if _, err := stream.CreateOrUpdateConsumer(ctx, cfg); err != nil {
		t.Fatalf("create the consumer again: %v", err)
	}
	waitWithin(t, 20*time.Second, func() bool { return handled.Load() >= 2 }, "the loop did not resume once the consumer existed again")
	waitWithin(t, 5*time.Second, func() bool { return log.count("INFO", "Consumer exists again") == 1 }, "the recovery was not logged")
}
