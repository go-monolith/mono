package lifecycle

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-monolith/mono/pkg/types"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestFetchErrorReporter(t *testing.T) {
	logger := &mockLogger{}
	clock := time.Unix(0, 0)
	r := newFetchErrorReporter(logger, "Fetch error", "service", "jobs")
	r.now = func() time.Time { return clock }

	r.success() // nothing to report yet
	r.failure(nats.ErrNoResponders)
	r.failure(nats.ErrNoResponders) // within the interval: suppressed
	r.failure(errors.New("boom"))   // suppressed as well
	clock = clock.Add(fetchErrorLogInterval)
	r.failure(errors.New("boom"))
	r.success() // within the interval of that line: the recovery waits
	clock = clock.Add(fetchErrorLogInterval)
	r.success()
	r.success() // already recovered: no second line

	entries := logger.entries
	if len(entries) != 3 {
		t.Fatalf("entries = %v, want first failure, one summary line and the recovery", entries)
	}
	if !strings.HasPrefix(entries[0], "WARN: Fetch error") || !strings.Contains(entries[0], "failures 1 suppressed 0") {
		t.Errorf("first failure (no responders) should be a warning: %q", entries[0])
	}
	if !strings.HasPrefix(entries[1], "ERROR: Fetch error") || !strings.Contains(entries[1], "failures 4 suppressed 2") {
		t.Errorf("summary line = %q", entries[1])
	}
	if !strings.HasPrefix(entries[2], "INFO: Fetch error cleared") || !strings.Contains(entries[2], "failures 4") {
		t.Errorf("recovery line = %q", entries[2])
	}
}

// TestFetchErrorReporter_Flapping checks that fetches alternating between
// failing and working log at most one line per interval.
func TestFetchErrorReporter_Flapping(t *testing.T) {
	logger := &mockLogger{}
	clock := time.Unix(0, 0)
	r := newFetchErrorReporter(logger, "Fetch error", "service", "jobs")
	r.now = func() time.Time { return clock }
	for i := 0; i < 20; i++ {
		r.failure(nats.ErrNoResponders)
		r.success()
		clock = clock.Add(fetchErrorLogInterval / 10)
	}
	// 20 cycles over two intervals: the first failure, then one line per
	// interval.
	if n := len(logger.entries); n > 3 {
		t.Errorf("%d lines for 20 flapping cycles in two intervals, want at most 3: %v", n, logger.entries)
	}
}

func TestSleepCtx(t *testing.T) {
	if !sleepCtx(context.Background(), time.Millisecond) {
		t.Error("full sleep should report true")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if sleepCtx(ctx, time.Hour) {
		t.Error("cancelled sleep should report false")
	}
}

// countingConsumer counts fetches; every fetch returns an empty batch whose
// Error is no responders, as a consumer without a leader does.
type countingConsumer struct {
	*mockJetStreamConsumer
	fetches atomic.Int64
}

func (c *countingConsumer) Fetch(int, ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	c.fetches.Add(1)
	return &mockMessageBatch{err: nats.ErrNoResponders}, nil
}

// TestFetchLoops_BackOffOnEmptyFailedBatch checks that a fetch loop whose
// fetches fail at once with an empty batch waits between attempts instead of
// spinning, and logs the failure once.
func TestFetchLoops_BackOffOnEmptyFailedBatch(t *testing.T) {
	loops := map[string]func(lm *lifecycleManager, ctx context.Context, c jetstream.Consumer){
		"stream consumer": func(lm *lifecycleManager, ctx context.Context, c jetstream.Consumer) {
			lm.runStreamConsumerLoop(ctx, c, &types.ServiceEntry{Name: "jobs",
				StreamConsumerConfig: &types.StreamConsumerConfig{Fetch: types.FetchConfig{BatchSize: 1, Timeout: time.Second}}})
		},
		"cron": func(lm *lifecycleManager, ctx context.Context, c jetstream.Consumer) {
			lm.runCronConsumerLoop(ctx, c, &types.ServiceEntry{Name: "tick"})
		},
		"event stream consumer": func(lm *lifecycleManager, ctx context.Context, c jetstream.Consumer) {
			lm.runEventStreamConsumerLoop(ctx, c, types.EventStreamConsumerEntry{
				EventDef: types.BaseEventDefinition{ModuleName: "billing", Name: "Paid"},
				Config:   types.StreamConsumerConfig{Fetch: types.FetchConfig{BatchSize: 1, Timeout: time.Second}},
			})
		},
	}
	for name, run := range loops {
		t.Run(name, func(t *testing.T) {
			logger := &mockLogger{}
			lm := &lifecycleManager{logger: logger}
			consumer := &countingConsumer{mockJetStreamConsumer: &mockJetStreamConsumer{}}
			ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
			defer cancel()
			run(lm, ctx, consumer)
			if n := consumer.fetches.Load(); n > 3 {
				t.Errorf("%d fetches in 1.5s, want about 2: the loop must back off", n)
			}
			warnings := 0
			for _, e := range logger.entries {
				if strings.HasPrefix(e, "WARN:") && strings.Contains(e, "no responders") {
					warnings++
				}
			}
			if warnings != 1 {
				t.Errorf("want one warning for repeated failures, got %d: %v", warnings, logger.entries)
			}
		})
	}
}
