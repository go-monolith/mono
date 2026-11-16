package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-monolith/mono/pkg/types"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func newReadyTestManager(es types.EventStream) (*lifecycleManager, *mockLogger) {
	logger := &mockLogger{}
	lm := &lifecycleManager{logger: logger}
	if es != nil {
		lm.eventBus = &mockEventBus{eventStream: es}
	}
	return lm, logger
}

func apiErr(code jetstream.ErrorCode) error {
	return fmt.Errorf("wrapped: %w", &jetstream.APIError{ErrorCode: code, Description: "test"})
}

func TestIsTransientJetStreamError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nats timeout", nats.ErrTimeout, true},
		{"deadline", fmt.Errorf("x: %w", context.DeadlineExceeded), true},
		{"no responders", fmt.Errorf("x: %w", nats.ErrNoResponders), true},
		{"no stream response", jetstream.ErrNoStreamResponse, true},
		{"temporarily unavailable", apiErr(10008), true},
		{"no peers for placement", apiErr(10005), true},
		{"incomplete results", apiErr(10004), true},
		{"stream offline", apiErr(10118), true},
		{"consumer offline", apiErr(10119), true},
		{"stream offline with a reason", apiErr(10194), false},
		{"consumer offline with a reason", apiErr(10195), false},
		{"stream not found", apiErr(jetstream.JSErrCodeStreamNotFound), false},
		{"insufficient resources", apiErr(10023), false},
		{"canceled", context.Canceled, false},
		{"plain", errors.New("boom"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isTransientJetStreamError(tt.err); got != tt.want {
				t.Errorf("isTransientJetStreamError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestIsJetStreamNotEnabled(t *testing.T) {
	for _, err := range []error{
		jetstream.ErrJetStreamNotEnabled,
		jetstream.ErrJetStreamNotEnabledForAccount,
		apiErr(jetstream.JSErrCodeJetStreamNotEnabled),
		apiErr(jetstream.JSErrCodeJetStreamNotEnabledForAccount),
	} {
		if !isJetStreamNotEnabled(err) {
			t.Errorf("isJetStreamNotEnabled(%v) = false", err)
		}
	}
	if isJetStreamNotEnabled(apiErr(10008)) || isJetStreamNotEnabled(errors.New("x")) {
		t.Error("other errors must not count as JetStream not enabled")
	}
}

func TestRetryJetStream(t *testing.T) {
	t.Run("retries transient errors until success", func(t *testing.T) {
		lm, logger := newReadyTestManager(nil)
		calls := 0
		err := lm.retryJetStream(context.Background(), "step", func(ctx context.Context) error {
			calls++
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) > jetStreamAttemptTimeout {
				t.Errorf("each attempt must be bounded by the attempt timeout, deadline in %s", time.Until(deadline))
			}
			if calls < 3 {
				return apiErr(10008)
			}
			return nil
		})
		if err != nil || calls != 3 {
			t.Fatalf("err=%v calls=%d, want nil and 3", err, calls)
		}
		if !logger.hasEntryContaining("INFO: Waiting for JetStream") || !logger.hasEntryContaining("INFO: JetStream step succeeded after waiting") {
			t.Errorf("wait and recovery not logged: %v", logger.entries)
		}
	})

	t.Run("hints at the replica count while there are no peers for placement", func(t *testing.T) {
		lm, logger := newReadyTestManager(nil)
		calls := 0
		if err := lm.retryJetStream(context.Background(), "step", func(context.Context) error {
			calls++
			if calls == 1 {
				return apiErr(10005)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if !logger.hasEntryContaining("does not exceed the number of servers") {
			t.Errorf("hint not logged: %v", logger.entries)
		}
	})

	t.Run("returns other errors at once", func(t *testing.T) {
		lm, _ := newReadyTestManager(nil)
		calls := 0
		want := apiErr(jetstream.JSErrCodeStreamNotFound)
		err := lm.retryJetStream(context.Background(), "step", func(context.Context) error {
			calls++
			return want
		})
		if !errors.Is(err, want) || calls != 1 {
			t.Fatalf("err=%v calls=%d, want the error after one call", err, calls)
		}
	})

	t.Run("gives up after the ready timeout", func(t *testing.T) {
		lm, _ := newReadyTestManager(nil)
		lm.SetJetStreamReadyTimeout(300 * time.Millisecond)
		start := time.Now()
		err := lm.retryJetStream(context.Background(), "set up x", func(context.Context) error {
			return nats.ErrNoResponders
		})
		if err == nil || !strings.Contains(err.Error(), "set up x: still failing after 300ms (WithJetStreamReadyTimeout)") ||
			!errors.Is(err, nats.ErrNoResponders) {
			t.Fatalf("err = %v", err)
		}
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Errorf("took %s, want about the ready timeout", elapsed)
		}
	})

	t.Run("stops when the caller's context ends", func(t *testing.T) {
		lm, _ := newReadyTestManager(nil)
		ctx, cancel := context.WithCancel(context.Background())
		calls := 0
		err := lm.retryJetStream(ctx, "step", func(context.Context) error {
			calls++
			cancel()
			return nats.ErrTimeout
		})
		if !errors.Is(err, nats.ErrTimeout) || calls != 1 {
			t.Fatalf("err=%v calls=%d", err, calls)
		}
		if strings.Contains(err.Error(), "WithJetStreamReadyTimeout") {
			t.Errorf("a cancelled start is not a ready timeout: %v", err)
		}
	})
}

func TestJetStreamReadyTimeoutAndReplicasSettings(t *testing.T) {
	lm, _ := newReadyTestManager(nil)
	if got := lm.jetStreamReadyTimeout(); got != types.DefaultJetStreamReadyTimeout {
		t.Errorf("default ready timeout = %s", got)
	}
	lm.SetJetStreamReadyTimeout(time.Second)
	if got := lm.jetStreamReadyTimeout(); got != time.Second {
		t.Errorf("ready timeout = %s", got)
	}
	if got := lm.streamReplicas(0); got != 0 {
		t.Errorf("replicas without a default = %d, want 0 (server default)", got)
	}
	lm.SetJetStreamDefaultReplicas(3)
	if got := lm.streamReplicas(0); got != 3 {
		t.Errorf("default replicas = %d, want 3", got)
	}
	if got := lm.streamReplicas(1); got != 1 {
		t.Errorf("configured replicas = %d, want 1", got)
	}
}

// readyEventStream is an EventStream whose Ready returns the queued errors.
type readyEventStream struct {
	*mockEventStream
	mu    sync.Mutex
	errs  []error
	calls int
}

func (r *readyEventStream) Ready(context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if len(r.errs) == 0 {
		return nil
	}
	err := r.errs[0]
	r.errs = r.errs[1:]
	return err
}

func TestWaitForJetStream(t *testing.T) {
	t.Run("no event bus", func(t *testing.T) {
		lm, _ := newReadyTestManager(nil)
		if enabled, err := lm.waitForJetStream(context.Background()); enabled || err != nil {
			t.Fatalf("enabled=%v err=%v", enabled, err)
		}
	})
	t.Run("no readiness probe", func(t *testing.T) {
		lm, _ := newReadyTestManager(&mockEventStream{})
		if enabled, err := lm.waitForJetStream(context.Background()); !enabled || err != nil {
			t.Fatalf("enabled=%v err=%v", enabled, err)
		}
	})
	t.Run("waits until ready", func(t *testing.T) {
		es := &readyEventStream{mockEventStream: &mockEventStream{}, errs: []error{apiErr(10008), nats.ErrTimeout}}
		lm, _ := newReadyTestManager(es)
		if enabled, err := lm.waitForJetStream(context.Background()); !enabled || err != nil || es.calls != 3 {
			t.Fatalf("enabled=%v err=%v calls=%d", enabled, err, es.calls)
		}
	})
	t.Run("JetStream not enabled", func(t *testing.T) {
		es := &readyEventStream{mockEventStream: &mockEventStream{}, errs: []error{apiErr(jetstream.JSErrCodeJetStreamNotEnabled)}}
		lm, _ := newReadyTestManager(es)
		if enabled, err := lm.waitForJetStream(context.Background()); enabled || err != nil {
			t.Fatalf("enabled=%v err=%v", enabled, err)
		}
	})
	t.Run("other error fails", func(t *testing.T) {
		es := &readyEventStream{mockEventStream: &mockEventStream{}, errs: []error{errors.New("boom")}}
		lm, _ := newReadyTestManager(es)
		if _, err := lm.waitForJetStream(context.Background()); err == nil || !strings.Contains(err.Error(), "JetStream is not available") {
			t.Fatalf("err = %v", err)
		}
	})
}

// fakeProtector lists streams and consumers and records threshold extensions.
type fakeProtector struct {
	*mockEventStream
	mu          sync.Mutex
	streams     []string
	streamsErrs []error
	consumers   map[string][]string
	listErr     map[string]error
	extendErr   map[string]error
	notExtended map[string]bool // reported as already durable or protected
	extended    []string
}

func (f *fakeProtector) StreamNames(context.Context) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.streamsErrs) > 0 {
		err := f.streamsErrs[0]
		f.streamsErrs = f.streamsErrs[1:]
		return nil, err
	}
	return f.streams, nil
}

func (f *fakeProtector) ConsumerNames(_ context.Context, stream string) ([]string, error) {
	return f.consumers[stream], f.listErr[stream]
}

func (f *fakeProtector) ExtendInactiveThreshold(_ context.Context, stream, consumer string, d time.Duration) (bool, error) {
	if d != nonDurableProtectThreshold {
		return false, fmt.Errorf("unexpected threshold %s", d)
	}
	if err := f.extendErr[consumer]; err != nil {
		return false, err
	}
	if f.notExtended[consumer] {
		return false, nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.extended = append(f.extended, stream+"/"+consumer)
	return true, nil
}

func TestProtectNonDurableConsumers(t *testing.T) {
	es := &fakeProtector{
		mockEventStream: &mockEventStream{},
		streams:         []string{"ORDERS", "MONO_CRON_billing_nightly", "BROKEN", "APP"},
		streamsErrs:     []error{nats.ErrTimeout}, // retried
		consumers: map[string][]string{
			"ORDERS":                    {"billing-orders", "billing-PaymentProcessed-v1-3", "dashboard"},
			"MONO_CRON_billing_nightly": {"billing-nightly-cron"},
			"APP":                       {"my-own-consumer", "billing-already-durable", "billing-fails"},
		},
		listErr:     map[string]error{"BROKEN": errors.New("list failed")},
		extendErr:   map[string]error{"billing-fails": errors.New("update failed")},
		notExtended: map[string]bool{"billing-already-durable": true},
	}
	lm, logger := newReadyTestManager(es)
	lm.protectNonDurableConsumers(context.Background(), []string{"billing", "my.plugin"})

	want := []string{
		"ORDERS/billing-orders",
		"ORDERS/billing-PaymentProcessed-v1-3",
		"MONO_CRON_billing_nightly/billing-nightly-cron",
	}
	slices.Sort(want)
	got := slices.Sorted(slices.Values(es.extended))
	if !slices.Equal(got, want) {
		t.Errorf("extended = %v, want %v", got, want)
	}
	if !logger.hasWarnContaining("Could not list consumers") || !logger.hasWarnContaining("Could not protect a non-durable consumer") {
		t.Errorf("failures not logged: %v", logger.entries)
	}
	if !logger.hasEntryContaining("INFO: Protected non-durable consumers until they are set up") {
		t.Errorf("summary not logged: %v", logger.entries)
	}
}

func TestProtectNonDurableConsumers_NoProtector(t *testing.T) {
	lm, logger := newReadyTestManager(&mockEventStream{})
	lm.protectNonDurableConsumers(context.Background(), []string{"billing"})
	if len(logger.entries) != 0 {
		t.Errorf("nothing should happen without a protector: %v", logger.entries)
	}
}

// streamConsumerStore is a fakeCronStore whose CreateOrUpdateConsumer is
// recorded instead of rejected, for stream consumer services.
type streamConsumerStore struct {
	*fakeCronStore
}

func (s *streamConsumerStore) CreateOrUpdateConsumer(_ context.Context, _ string, cfg types.ConsumerConfig) (jetstream.Consumer, error) {
	s.recordConfig("create-or-update", cfg)
	return &mockJetStreamConsumer{}, nil
}

func TestEnsureStreamConsumer(t *testing.T) {
	entry := &types.ServiceEntry{Name: "orders", ModuleName: "billing"}
	cfg := types.ConsumerConfig{Name: "billing-orders", Durable: "billing-orders", AckPolicy: types.AckExplicitPolicy}

	t.Run("converts a non-durable consumer with two updates", func(t *testing.T) {
		store := &streamConsumerStore{newFakeCronStore()}
		store.consumer = &jetstream.ConsumerInfo{Config: jetstream.ConsumerConfig{Name: "billing-orders"}}
		lm, logger := newReadyTestManager(store)
		if _, err := lm.ensureStreamConsumer(context.Background(), store, "ORDERS", cfg, entry); err != nil {
			t.Fatal(err)
		}
		if got := store.recorded(); !slices.Equal(got, []string{"update:billing-orders:0", "update:billing-orders:0"}) {
			t.Errorf("ops = %v", got)
		}
		for _, c := range store.configs {
			if c.Durable != "billing-orders" {
				t.Errorf("update sent Durable %q", c.Durable)
			}
		}
		if !logger.hasEntryContaining("INFO: Stream consumer converted to durable") {
			t.Errorf("conversion not logged: %v", logger.entries)
		}
	})

	t.Run("updates a durable consumer in place", func(t *testing.T) {
		store := &streamConsumerStore{newFakeCronStore()}
		store.consumer = &jetstream.ConsumerInfo{Config: jetstream.ConsumerConfig{Name: "billing-orders", Durable: "billing-orders"}}
		lm, _ := newReadyTestManager(store)
		if _, err := lm.ensureStreamConsumer(context.Background(), store, "ORDERS", cfg, entry); err != nil {
			t.Fatal(err)
		}
		if got := store.recorded(); !slices.Equal(got, []string{"create-or-update:billing-orders:0"}) {
			t.Errorf("ops = %v", got)
		}
	})

	t.Run("creates a missing consumer", func(t *testing.T) {
		store := &streamConsumerStore{newFakeCronStore()}
		lm, _ := newReadyTestManager(store)
		if _, err := lm.ensureStreamConsumer(context.Background(), store, "ORDERS", cfg, entry); err != nil {
			t.Fatal(err)
		}
		if got := store.recorded(); !slices.Equal(got, []string{"create-or-update:billing-orders:0"}) {
			t.Errorf("ops = %v", got)
		}
	})

	t.Run("creates one deleted before the update", func(t *testing.T) {
		store := &streamConsumerStore{newFakeCronStore()}
		store.consumer = &jetstream.ConsumerInfo{Config: jetstream.ConsumerConfig{Name: "billing-orders"}}
		store.updateErrs = []error{fmt.Errorf("update: %w", jetstream.ErrConsumerDoesNotExist)}
		lm, _ := newReadyTestManager(store)
		if _, err := lm.ensureStreamConsumer(context.Background(), store, "ORDERS", cfg, entry); err != nil {
			t.Fatal(err)
		}
		if got := store.recorded(); !slices.Equal(got, []string{"update:billing-orders:0", "create-or-update:billing-orders:0"}) {
			t.Errorf("ops = %v", got)
		}
	})

	t.Run("lookup and update failures are returned", func(t *testing.T) {
		store := &streamConsumerStore{newFakeCronStore()}
		store.infoErrs = []error{nats.ErrTimeout}
		lm, _ := newReadyTestManager(store)
		if _, err := lm.ensureStreamConsumer(context.Background(), store, "ORDERS", cfg, entry); !errors.Is(err, nats.ErrTimeout) {
			t.Errorf("lookup err = %v", err)
		}

		store = &streamConsumerStore{newFakeCronStore()}
		store.consumer = &jetstream.ConsumerInfo{Config: jetstream.ConsumerConfig{Name: "billing-orders"}}
		store.updateErrs = []error{nil, errors.New("second update failed")}
		lm, _ = newReadyTestManager(store)
		if _, err := lm.ensureStreamConsumer(context.Background(), store, "ORDERS", cfg, entry); err == nil ||
			!strings.Contains(err.Error(), "inactivity threshold") {
			t.Errorf("second update err = %v", err)
		}

		store = &streamConsumerStore{newFakeCronStore()}
		store.consumer = &jetstream.ConsumerInfo{Config: jetstream.ConsumerConfig{Name: "billing-orders"}}
		store.updateErrs = []error{errors.New("deliver policy can not be updated")}
		lm, _ = newReadyTestManager(store)
		if _, err := lm.ensureStreamConsumer(context.Background(), store, "ORDERS", cfg, entry); err == nil ||
			!strings.Contains(err.Error(), "convert consumer billing-orders to durable") {
			t.Errorf("first update err = %v", err)
		}
	})

	t.Run("an EventStream without consumer management creates or updates", func(t *testing.T) {
		es := &mockEventStream{}
		lm, _ := newReadyTestManager(es)
		if _, err := lm.ensureStreamConsumer(context.Background(), es, "ORDERS", cfg, entry); err != nil {
			t.Fatal(err)
		}
		if len(es.createdConsumers) != 1 {
			t.Errorf("created = %v", es.createdConsumers)
		}
	})
}
