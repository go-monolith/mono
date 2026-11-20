package lifecycle

import (
	"context"
	"errors"
	"fmt"
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

func TestIsTransientFetchError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"leadership change", jetstream.ErrConsumerLeadershipChanged, true},
		{"server shutdown", jetstream.ErrServerShutdown, true},
		{"wrapped leadership change", fmt.Errorf("fetch: %w", jetstream.ErrConsumerLeadershipChanged), true},
		{"wrapped server shutdown", fmt.Errorf("fetch: %w", jetstream.ErrServerShutdown), true},
		{"no responders", nats.ErrNoResponders, true},
		{"timeout", nats.ErrTimeout, true},
		{"consumer deleted", jetstream.ErrConsumerDeleted, false},
		{"other", errors.New("boom"), false},
	}
	for _, tt := range tests {
		if got := isTransientFetchError(tt.err); got != tt.want {
			t.Errorf("%s: isTransientFetchError = %v, want %v", tt.name, got, tt.want)
		}
	}
	// The setup retries keep their own, narrower set.
	if isTransientJetStreamError(jetstream.ErrConsumerLeadershipChanged) || isTransientJetStreamError(jetstream.ErrServerShutdown) {
		t.Error("isTransientJetStreamError must not retry the fetch-only statuses")
	}
}

// TestFetchErrorReporter_LeaderMoveIsWarning covers the log lines of a
// rolling restart: the fetch loop of a surviving instance sees the statuses
// the server sends while the consumer's leader moves.
func TestFetchErrorReporter_LeaderMoveIsWarning(t *testing.T) {
	for _, err := range []error{jetstream.ErrConsumerLeadershipChanged, jetstream.ErrServerShutdown} {
		logger := &mockLogger{}
		r := newFetchErrorReporter(logger, "Fetch error", "service", "jobs")
		r.failure(err)
		if len(logger.entries) != 1 || !strings.HasPrefix(logger.entries[0], "WARN: Fetch error") {
			t.Errorf("%v: entries = %v, want one warning", err, logger.entries)
		}
	}
}

// TestFetchErrorReporter_ConsumerGone: a pull request for a consumer that no
// longer exists is answered with "no responders", like one for a consumer
// without a leader. The reporter looks the consumer up and says once, at
// Error, that it is gone and what brings the loop back, instead of a warning
// every ten seconds that reads as if it would clear by itself.
func TestFetchErrorReporter_ConsumerGone(t *testing.T) {
	newReporter := func(lookupErr *error) (*fetchErrorReporter, *mockLogger, *time.Time, *int) {
		logger := &mockLogger{}
		clock := time.Unix(0, 0)
		lookups := 0
		r := newFetchErrorReporter(logger, "Fetch error", "service", "jobs")
		r.now = func() time.Time { return clock }
		r.WatchConsumer("JOBS", "orders-jobs", func(context.Context) error { lookups++; return *lookupErr })
		return r, logger, &clock, &lookups
	}

	t.Run("deleted: said at once, then a reminder, then the recovery", func(t *testing.T) {
		lookupErr := error(jetstream.ErrConsumerNotFound)
		r, logger, clock, lookups := newReporter(&lookupErr)
		r.failure(jetstream.ErrConsumerDeleted)
		if len(logger.entries) != 1 || !strings.HasPrefix(logger.entries[0], "ERROR: "+ConsumerGoneMessage) {
			t.Fatalf("entries = %v, want the consumer reported as gone at once", logger.entries)
		}
		for _, want := range []string{"stream JOBS", "consumer orders-jobs", "restart this instance", "service jobs"} {
			if !strings.Contains(logger.entries[0], want) {
				t.Errorf("line %q lacks %q", logger.entries[0], want)
			}
		}
		// The fetches that follow get no responders: nothing more is logged
		// until the reminder is due, and the lookup is not made every time.
		for i := 0; i < 50; i++ {
			*clock = clock.Add(time.Second)
			r.failure(nats.ErrNoResponders)
		}
		if len(logger.entries) != 1 {
			t.Fatalf("entries = %v, want no further line within the reminder interval", logger.entries)
		}
		if *lookups > 12 {
			t.Errorf("%d lookups for 51 failures", *lookups)
		}
		*clock = clock.Add(goneLogInterval)
		r.failure(nats.ErrNoResponders)
		if len(logger.entries) != 2 || !strings.HasPrefix(logger.entries[1], "ERROR: "+ConsumerGoneMessage) {
			t.Fatalf("entries = %v, want one reminder", logger.entries)
		}
		// Another instance started and created the consumer: fetching works.
		r.success()
		if len(logger.entries) != 3 || !strings.HasPrefix(logger.entries[2], "INFO: Consumer exists again") {
			t.Fatalf("entries = %v, want the recovery", logger.entries)
		}
		r.success()
		if len(logger.entries) != 3 {
			t.Fatalf("recovery logged twice: %v", logger.entries)
		}
	})

	t.Run("removed by the server: found after a few failed fetches", func(t *testing.T) {
		lookupErr := error(jetstream.ErrConsumerNotFound)
		r, logger, _, lookups := newReporter(&lookupErr)
		for i := 1; i < goneCheckEvery; i++ {
			r.failure(nats.ErrNoResponders)
		}
		if *lookups != 0 || logger.hasErrorContaining(ConsumerGoneMessage) {
			t.Fatalf("looked up %d times before %d failures: %v", *lookups, goneCheckEvery, logger.entries)
		}
		r.failure(nats.ErrNoResponders)
		if *lookups != 1 || !logger.hasErrorContaining(ConsumerGoneMessage) {
			t.Fatalf("lookups = %d, entries = %v, want the consumer reported as gone", *lookups, logger.entries)
		}
	})

	t.Run("the stream is gone too", func(t *testing.T) {
		lookupErr := error(jetstream.ErrStreamNotFound)
		r, logger, _, _ := newReporter(&lookupErr)
		r.failure(jetstream.ErrConsumerDeleted)
		if !logger.hasErrorContaining(ConsumerGoneMessage) {
			t.Fatalf("entries = %v", logger.entries)
		}
	})

	t.Run("a consumer without a leader is not reported as gone", func(t *testing.T) {
		var lookupErr error // the consumer exists
		r, logger, clock, _ := newReporter(&lookupErr)
		for i := 0; i < 3*goneCheckEvery; i++ {
			*clock = clock.Add(time.Second)
			r.failure(nats.ErrNoResponders)
		}
		if logger.hasErrorContaining(ConsumerGoneMessage) {
			t.Fatalf("entries = %v", logger.entries)
		}
		if !logger.hasWarnContaining("Fetch error") {
			t.Fatalf("the failure is still reported as before: %v", logger.entries)
		}
	})

	t.Run("a lookup that fails says nothing", func(t *testing.T) {
		lookupErr := errors.New("timeout")
		r, logger, _, _ := newReporter(&lookupErr)
		r.failure(jetstream.ErrConsumerDeleted)
		if logger.hasErrorContaining(ConsumerGoneMessage) {
			t.Fatalf("entries = %v", logger.entries)
		}
		// Once the lookup answers, it is reported.
		lookupErr = jetstream.ErrConsumerNotFound
		for i := 0; i < goneCheckEvery; i++ {
			r.failure(nats.ErrNoResponders)
		}
		if !logger.hasErrorContaining(ConsumerGoneMessage) {
			t.Fatalf("entries = %v", logger.entries)
		}
	})

	t.Run("without a consumer to watch nothing changes", func(t *testing.T) {
		logger := &mockLogger{}
		r := newFetchErrorReporter(logger, "Fetch error", "service", "jobs")
		r.WatchJetStreamConsumer(nil)
		r.failure(jetstream.ErrConsumerDeleted)
		if len(logger.entries) != 1 || !strings.HasPrefix(logger.entries[0], "ERROR: Fetch error") {
			t.Fatalf("entries = %v", logger.entries)
		}
	})
}
