package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/go-monolith/mono/pkg/types"
)

// leftoverTestStore is a fakeConsumerStore that also lists stream names, so
// that it implements LeftoverStore.
type leftoverTestStore struct {
	*fakeConsumerStore
	streams    []string
	streamsErr error
}

// StreamNames fails on a context that is done, as the real store does.
func (s *leftoverTestStore) StreamNames(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.streams, s.streamsErr
}

// ConsumerNames fails on a context that is done, as the real store does.
func (s *leftoverTestStore) ConsumerNames(ctx context.Context, stream string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.fakeConsumerStore.ConsumerNames(ctx, stream)
}

// markedConsumer returns the info of a consumer that carries the registration
// marker with owner; an empty owner leaves it unmarked.
func markedConsumer(name, owner string) *jetstream.ConsumerInfo {
	info := &jetstream.ConsumerInfo{Name: name, Config: jetstream.ConsumerConfig{Durable: name}}
	if owner != "" {
		info.Config.Metadata = map[string]string{RegistrationMetadataKey: owner}
	}
	return info
}

// leftoverTestManager returns a manager whose plan holds one event stream
// consumer of module billing on stream PAYMENTS with the retention, and the
// store its JetStream answers from.
func leftoverTestManager(t *testing.T, retention types.RetentionPolicy) (*lifecycleManager, *mockLogger, *leftoverTestStore) {
	t.Helper()
	store := &leftoverTestStore{fakeConsumerStore: newFakeConsumerStore()}
	lm, logger := newMigrationTestManager(store)
	entry := streamEntry("billing", "PAYMENTS", paymentProcessedV1)
	entry.Config.Stream.Retention = retention
	plan, err := NewStreamPlan(StreamRegistrations(nil, []types.EventStreamConsumerEntry{entry}))
	if err != nil {
		t.Fatalf("NewStreamPlan: %v", err)
	}
	lm.streamPlan = plan
	return lm, logger, store
}

func TestReportLeftovers(t *testing.T) {
	t.Run("reports marked consumers and streams without a registration", func(t *testing.T) {
		lm, logger, store := leftoverTestManager(t, types.InterestPolicy)
		store.consumers["PAYMENTS"] = []*jetstream.ConsumerInfo{
			markedConsumer("billing-billing-PaymentProcessed-v1", "event stream consumer of module billing for event billing.PaymentProcessed"),
			markedConsumer("invoicing-billing-PaymentProcessed-v1", "event stream consumer of module invoicing for event billing.PaymentProcessed"),
			markedConsumer("app-own", ""),
			{Name: "push", Config: jetstream.ConsumerConfig{DeliverSubject: "deliver.here"}},
		}
		store.streams = []string{"PAYMENTS", "ORDERS", "APP_OWN", "KV_cache", "OBJ_files", types.CronStreamNamePrefix + "billing_tick"}
		store.metadata = map[string]map[string]string{
			"ORDERS":   {RegistrationMetadataKey: "stream consumer service orders.ingest"},
			"APP_OWN":  {"team": "payments"},
			"KV_cache": {RegistrationMetadataKey: "must not be read"},
		}

		lm.ReportLeftovers(context.Background())

		if got := logger.countEntries("WARN: " + LeftoverConsumerWarning); got != 1 {
			t.Errorf("consumer warnings = %d, want 1: %v", got, logger.entries)
		}
		for _, part := range []string{
			"consumer invoicing-billing-PaymentProcessed-v1",
			"created_for event stream consumer of module invoicing for event billing.PaymentProcessed",
			"retains_messages true",
			"remove_with nats consumer rm PAYMENTS invoicing-billing-PaymentProcessed-v1",
		} {
			if !logger.hasWarnContaining(part) {
				t.Errorf("no warning contains %q: %v", part, logger.entries)
			}
		}
		if got := logger.countEntries("WARN: " + LeftoverStreamWarning); got != 1 {
			t.Errorf("stream warnings = %d, want 1: %v", got, logger.entries)
		}
		for _, part := range []string{"stream ORDERS", "created_for stream consumer service orders.ingest", "remove_with nats stream rm ORDERS"} {
			if !logger.hasWarnContaining(part) {
				t.Errorf("no warning contains %q: %v", part, logger.entries)
			}
		}

		// The registered consumer, the registered stream, buckets and cron
		// streams are never looked up.
		for _, op := range store.recorded() {
			for _, skipped := range []string{
				"info:PAYMENTS/billing-billing-PaymentProcessed-v1", "metadata:PAYMENTS",
				"metadata:KV_cache", "metadata:OBJ_files", "metadata:" + types.CronStreamNamePrefix + "billing_tick",
			} {
				if op == skipped {
					t.Errorf("%s must not be looked up", skipped)
				}
			}
		}
	})

	t.Run("a consumer on a limits stream does not retain messages", func(t *testing.T) {
		lm, logger, store := leftoverTestManager(t, types.LimitsPolicy)
		store.consumers["PAYMENTS"] = []*jetstream.ConsumerInfo{markedConsumer("old", "stream consumer service billing.old")}
		lm.ReportLeftovers(context.Background())
		if !logger.hasWarnContaining("retains_messages false") {
			t.Errorf("want retains_messages false: %v", logger.entries)
		}
	})

	t.Run("nothing to report", func(t *testing.T) {
		lm, logger, store := leftoverTestManager(t, types.LimitsPolicy)
		store.consumers["PAYMENTS"] = []*jetstream.ConsumerInfo{markedConsumer("billing-billing-PaymentProcessed-v1", "x")}
		store.streams = []string{"PAYMENTS"}
		lm.ReportLeftovers(context.Background())
		if logger.countEntries("WARN:") != 0 || logger.countEntries("INFO:") != 0 {
			t.Errorf("want no warning: %v", logger.entries)
		}
	})

	t.Run("failures are logged at debug and never reported as leftovers", func(t *testing.T) {
		boom := errors.New("no responders")

		lm, logger, store := leftoverTestManager(t, types.LimitsPolicy)
		store.listErr = boom
		store.streamsErr = boom
		lm.ReportLeftovers(context.Background())
		if logger.countEntries("WARN:") != 0 || logger.countEntries("DEBUG: Could not list consumers") != 1 ||
			logger.countEntries("DEBUG: Could not list streams") != 1 {
			t.Errorf("listing failures: %v", logger.entries)
		}

		lm, logger, store = leftoverTestManager(t, types.LimitsPolicy)
		store.consumers["PAYMENTS"] = []*jetstream.ConsumerInfo{markedConsumer("old", "stream consumer service billing.old")}
		store.infoErr = boom
		store.streams = []string{"ORDERS"}
		store.metadataErr = boom
		lm.ReportLeftovers(context.Background())
		if logger.countEntries("WARN:") != 0 || logger.countEntries("DEBUG: Could not inspect consumer") != 1 ||
			logger.countEntries("DEBUG: Could not inspect stream") != 1 {
			t.Errorf("lookup failures: %v", logger.entries)
		}
	})

	t.Run("inspects a bounded number of consumers and of streams", func(t *testing.T) {
		lm, logger, store := leftoverTestManager(t, types.LimitsPolicy)
		for i := 0; i < LeftoverLookupLimit+6; i++ {
			name := fmt.Sprintf("old-%03d", i)
			store.consumers["PAYMENTS"] = append(store.consumers["PAYMENTS"], markedConsumer(name, "stream consumer service billing."+name))
		}
		store.metadata = make(map[string]map[string]string)
		for i := 0; i < LeftoverLookupLimit+2; i++ {
			name := fmt.Sprintf("OLD_%03d", i)
			store.streams = append(store.streams, name)
			store.metadata[name] = map[string]string{RegistrationMetadataKey: "stream consumer service billing." + name}
		}
		lm.ReportLeftovers(context.Background())

		if got := logger.countEntries("WARN: " + LeftoverConsumerWarning); got != LeftoverLookupLimit {
			t.Errorf("consumer warnings = %d, want %d", got, LeftoverLookupLimit)
		}
		// The stream pass has a budget of its own.
		if got := logger.countEntries("WARN: " + LeftoverStreamWarning); got != LeftoverLookupLimit {
			t.Errorf("stream warnings = %d, want %d", got, LeftoverLookupLimit)
		}
		want := fmt.Sprintf("inspected %d not_inspected 8 timed_out false", 2*LeftoverLookupLimit)
		if !hasInfoContaining(logger, "Leftover report incomplete", want) {
			t.Errorf("no incomplete-report line with %q", want)
		}
	})

	t.Run("many consumers of the application's own do not hide a leftover stream", func(t *testing.T) {
		lm, logger, store := leftoverTestManager(t, types.LimitsPolicy)
		for i := 0; i < LeftoverLookupLimit+10; i++ {
			store.consumers["PAYMENTS"] = append(store.consumers["PAYMENTS"], markedConsumer(fmt.Sprintf("app-own-%03d", i), ""))
		}
		store.streams = []string{"ORDERS"}
		store.metadata = map[string]map[string]string{"ORDERS": {RegistrationMetadataKey: "stream consumer service orders.ingest"}}
		lm.ReportLeftovers(context.Background())
		if got := logger.countEntries("WARN: " + LeftoverStreamWarning); got != 1 {
			t.Errorf("stream warnings = %d, want 1", got)
		}
	})

	t.Run("a report that ran out of time says so and looks nothing up", func(t *testing.T) {
		lm, logger, store := leftoverTestManager(t, types.LimitsPolicy)
		store.consumers["PAYMENTS"] = []*jetstream.ConsumerInfo{markedConsumer("old", "stream consumer service billing.old")}
		store.streams = []string{"ORDERS"}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		lm.ReportLeftovers(ctx)

		if logger.countEntries("WARN:") != 0 {
			t.Errorf("want no warning: %v", logger.entries)
		}
		// The listings fail on the expired context, so nothing is counted;
		// the report is still said to be incomplete.
		if !hasInfoContaining(logger, "Leftover report incomplete", "inspected 0 not_inspected 0 timed_out true") {
			t.Errorf("no timed-out line: %v", logger.entries)
		}
		for _, op := range store.recorded() {
			if strings.HasPrefix(op, "info:") || strings.HasPrefix(op, "metadata:") {
				t.Errorf("lookup %s after the time limit", op)
			}
		}
	})

	t.Run("retention is that of the registration set up last", func(t *testing.T) {
		store := &leftoverTestStore{fakeConsumerStore: newFakeConsumerStore()}
		lm, logger := newMigrationTestManager(store)
		limits := streamEntry("billing", "PAYMENTS", paymentProcessedV1)
		interest := streamEntry("shipping", "PAYMENTS", paymentProcessedV1)
		interest.Config.Stream.Retention = types.InterestPolicy
		plan, err := NewStreamPlan(StreamRegistrations(nil, []types.EventStreamConsumerEntry{limits, interest}))
		if err != nil {
			t.Fatalf("NewStreamPlan: %v", err)
		}
		lm.streamPlan = plan
		store.consumers["PAYMENTS"] = []*jetstream.ConsumerInfo{markedConsumer("old", "stream consumer service billing.old")}
		lm.ReportLeftovers(context.Background())
		if !logger.hasWarnContaining("retains_messages true") {
			t.Errorf("want retains_messages true: %v", logger.entries)
		}
	})

	t.Run("does nothing without JetStream or consumer management", func(t *testing.T) {
		logger := &mockLogger{}
		lm := &lifecycleManager{logger: logger}
		lm.ReportLeftovers(context.Background())

		lm.eventBus = &mockEventBus{eventStreamErr: errors.New("JetStream not enabled")}
		lm.ReportLeftovers(context.Background())

		lm.eventBus = &mockEventBus{eventStream: &mockEventStream{}}
		lm.ReportLeftovers(context.Background())

		if len(logger.entries) != 0 {
			t.Errorf("want nothing logged: %v", logger.entries)
		}
	})

	t.Run("without a plan only streams are inspected", func(t *testing.T) {
		store := &leftoverTestStore{fakeConsumerStore: newFakeConsumerStore()}
		lm, logger := newMigrationTestManager(store)
		store.streams = []string{"ORDERS"}
		store.metadata = map[string]map[string]string{"ORDERS": {RegistrationMetadataKey: "stream consumer service orders.ingest"}}
		lm.ReportLeftovers(context.Background())
		if got := logger.countEntries("WARN: " + LeftoverStreamWarning); got != 1 {
			t.Errorf("stream warnings = %d, want 1: %v", got, logger.entries)
		}
	})
}

// hasInfoContaining reports whether an Info line starting with prefix
// contains part.
func hasInfoContaining(logger *mockLogger, prefix, part string) bool {
	logger.mu.Lock()
	defer logger.mu.Unlock()
	for _, entry := range logger.entries {
		if strings.HasPrefix(entry, "INFO: "+prefix) && strings.Contains(entry, part) {
			return true
		}
	}
	return false
}
