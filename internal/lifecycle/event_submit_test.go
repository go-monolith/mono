package lifecycle

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-monolith/mono/internal/workerpool"
)

// An event that carries a reply subject (one published through JetStream
// carries the inbox of its publish acknowledgement) was treated as a request:
// a full pool rejected it and the framework answered into that inbox.

func TestSubmitEventToPool_FullPoolHoldsTheEventBack(t *testing.T) {
	logger := &mockLogger{}
	pool := workerpool.New(1, 1)
	t.Cleanup(func() { _ = pool.Close(context.Background()) })
	reporter := newOverloadReporter(logger, "event", "e")

	release := make(chan struct{})
	// Registered after the pool's cleanup, so it runs first: a test that
	// fails before it released the handler must not leave Close waiting.
	releaseOnce := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseOnce)
	var handled atomic.Int32
	job := func() {
		<-release
		handled.Add(1)
	}
	const events = 5
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range events {
			if !SubmitEventToPool(pool, logger, reporter, job) {
				t.Error("an event was not accepted")
			}
		}
	}()
	// One job runs and one waits; the third submit is held back.
	select {
	case <-done:
		t.Fatal("the submits returned although the pool was full")
	case <-time.After(100 * time.Millisecond):
	}
	if n := reporter.rejectedTotal.Load(); n != 0 {
		t.Fatalf("%d events rejected, want none", n)
	}
	releaseOnce()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the submits did not finish after the handler was released")
	}
	deadline := time.Now().Add(5 * time.Second)
	for handled.Load() != events && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if n := handled.Load(); n != events {
		t.Fatalf("%d events handled, want %d", n, events)
	}
}

func TestSubmitEventToPool_ClosedPoolDropsAndCounts(t *testing.T) {
	logger := &mockLogger{}
	pool := workerpool.New(1, 1)
	if err := pool.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	reporter := newOverloadReporter(logger, "event", "e")
	if SubmitEventToPool(pool, logger, reporter, func() { t.Error("job must not run on a closed pool") }) {
		t.Fatal("closed pool must not accept")
	}
	if n := reporter.droppedTotal.Load(); n != 1 {
		t.Fatalf("dropped = %d, want 1", n)
	}
	if !logger.hasWarnContaining("dropped 1 ") {
		t.Error("expected the warning that counts the dropped event")
	}
}
