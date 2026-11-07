package eventbus

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-monolith/mono/internal/logger"
	"github.com/go-monolith/mono/pkg/types"
)

// TestSubscription_PendingLimitsAndClientStats checks the optional
// capabilities the lifecycle manager relies on since #23: a subscription
// returned by the event bus can bound its client buffer (PendingLimiter) and
// report its pending, dropped and delivered counters (SubscriptionStatsReader),
// and a bound of one message turns a blocked handler into dropped messages.
func TestSubscription_PendingLimitsAndClientStats(t *testing.T) {
	conn := setupTestNATS(t)
	eb, err := NewEventBus(conn, logger.NewDefaultLogger())
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}

	gate := make(chan struct{})
	defer close(gate)
	var delivered atomic.Int32
	sub, err := eb.Subscribe("stats.subject", func(_ context.Context, _ *types.Msg) {
		delivered.Add(1)
		<-gate
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	limiter, ok := sub.(types.PendingLimiter)
	if !ok {
		t.Fatal("subscription must implement types.PendingLimiter")
	}
	reader, ok := sub.(types.SubscriptionStatsReader)
	if !ok {
		t.Fatal("subscription must implement types.SubscriptionStatsReader")
	}
	if err := limiter.SetPendingLimits(1, -1); err != nil {
		t.Fatalf("SetPendingLimits: %v", err)
	}
	if err := limiter.SetPendingLimits(0, 0); err == nil {
		t.Fatal("nats.go rejects a 0 limit; the error must be returned, not hidden")
	}

	for i := 0; i < 20; i++ {
		if err := eb.Publish("stats.subject", []byte("x")); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	if err := conn.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		dropped, err := reader.Dropped()
		if err != nil {
			t.Fatalf("Dropped: %v", err)
		}
		if dropped > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no message was dropped with a pending limit of 1 and a blocked handler (delivered=%d)", delivered.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
	if n, err := reader.Delivered(); err != nil || n < 1 {
		t.Fatalf("Delivered = %d (%v), want at least 1", n, err)
	}
	if msgs, bytes, err := reader.Pending(); err != nil || msgs < 0 || bytes < 0 {
		t.Fatalf("Pending = %d/%d (%v)", msgs, bytes, err)
	}
}
