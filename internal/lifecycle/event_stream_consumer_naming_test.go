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

// fakeConsumerStore is an EventStream that also implements eventConsumerStore.
// It records every consumer operation in order.
type fakeConsumerStore struct {
	*mockEventStream

	mu        sync.Mutex
	consumers map[string][]*jetstream.ConsumerInfo // stream -> existing consumers
	listErr   error
	info      *jetstream.ConsumerInfo // ConsumerInfo of a consumer not in consumers
	infoErr   error
	cached    *jetstream.ConsumerInfo // CachedInfo of consumers returned by CreateOrUpdateConsumer
	deleteErr error
	resetErrs []error // one per ResetConsumerToSequence call, nil once exhausted
	ops       []string

	metadata       map[string]map[string]string // stream -> metadata
	metadataErr    error
	streamWrites   []types.StreamConfig // every CreateOrUpdateStream, in order
	streamWriteErr error
}

func newFakeConsumerStore() *fakeConsumerStore {
	return &fakeConsumerStore{
		mockEventStream: &mockEventStream{},
		consumers:       make(map[string][]*jetstream.ConsumerInfo),
	}
}

func (f *fakeConsumerStore) record(op string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ops = append(f.ops, op)
}

func (f *fakeConsumerStore) recorded() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.ops)
}

func (f *fakeConsumerStore) CreateOrUpdateStream(_ context.Context, cfg types.StreamConfig) (jetstream.Stream, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.streamWriteErr != nil {
		return nil, f.streamWriteErr
	}
	f.streamWrites = append(f.streamWrites, cfg)
	return nil, nil
}

func (f *fakeConsumerStore) writtenStreams() []types.StreamConfig {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.streamWrites)
}

func (f *fakeConsumerStore) StreamMetadata(_ context.Context, stream string) (map[string]string, error) {
	f.record("metadata:" + stream)
	if f.metadataErr != nil {
		return nil, f.metadataErr
	}
	return f.metadata[stream], nil
}

func (f *fakeConsumerStore) CreateOrUpdateConsumer(_ context.Context, stream string, cfg types.ConsumerConfig) (jetstream.Consumer, error) {
	f.record("create:" + stream + "/" + cfg.Name)
	return &cachedInfoConsumer{mockJetStreamConsumer: &mockJetStreamConsumer{}, info: f.cached}, nil
}

func (f *fakeConsumerStore) ConsumerNames(_ context.Context, stream string) ([]string, error) {
	f.record("list:" + stream)
	if f.listErr != nil {
		return nil, f.listErr
	}
	var names []string
	for _, info := range f.consumers[stream] {
		names = append(names, info.Name)
	}
	return names, nil
}

func (f *fakeConsumerStore) ConsumerInfo(_ context.Context, stream, consumer string) (*jetstream.ConsumerInfo, error) {
	f.record("info:" + stream + "/" + consumer)
	if f.infoErr != nil {
		return nil, f.infoErr
	}
	for _, info := range f.consumers[stream] {
		if info.Name == consumer {
			if info.Config.DeliverSubject != "" {
				// Like the real store: pull-consumer lookups reject push consumers.
				return nil, fmt.Errorf("lookup: %w", jetstream.ErrNotPullConsumer)
			}
			return info, nil
		}
	}
	if f.info != nil {
		return f.info, nil
	}
	return &jetstream.ConsumerInfo{Name: consumer}, nil
}

func (f *fakeConsumerStore) DeleteConsumer(_ context.Context, stream, consumer string) error {
	f.record("delete:" + stream + "/" + consumer)
	return f.deleteErr
}

func (f *fakeConsumerStore) ResetConsumerToSequence(_ context.Context, stream, consumer string, seq uint64) error {
	f.record(fmt.Sprintf("reset:%s/%s@%d", stream, consumer, seq))
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.resetErrs) == 0 {
		return nil
	}
	err := f.resetErrs[0]
	f.resetErrs = f.resetErrs[1:]
	return err
}

// cachedInfoConsumer is a jetstream.Consumer with configurable cached info.
type cachedInfoConsumer struct {
	*mockJetStreamConsumer
	info *jetstream.ConsumerInfo
}

func (c *cachedInfoConsumer) CachedInfo() *jetstream.ConsumerInfo { return c.info }

// legacyInfo builds the info of a legacy pull consumer that delivered and
// acknowledged everything up to stream sequence ackFloor.
func legacyInfo(name string, delivered, ackFloor, numPending uint64) *jetstream.ConsumerInfo {
	return &jetstream.ConsumerInfo{
		Name:       name,
		Delivered:  jetstream.SequenceInfo{Consumer: delivered, Stream: ackFloor},
		AckFloor:   jetstream.SequenceInfo{Consumer: delivered, Stream: ackFloor},
		NumPending: numPending,
	}
}

func streamEntry(consumerModule, stream string, def types.BaseEventDefinition) types.EventStreamConsumerEntry {
	return types.EventStreamConsumerEntry{
		EventDef: def,
		Config: types.StreamConsumerConfig{
			Stream: types.StreamConfig{Name: stream, Subjects: []string{def.Subject}},
			Fetch:  types.FetchConfig{BatchSize: 10, Timeout: 100 * time.Millisecond},
		},
		Handler: func(context.Context, []*types.Msg) error { return nil },
		Module:  &mockModule{name: consumerModule},
	}
}

func newMigrationTestManager(es types.EventStream) (*lifecycleManager, *mockLogger) {
	logger := &mockLogger{}
	return &lifecycleManager{
		logger:          logger,
		eventBus:        &mockEventBus{eventStream: es},
		streamConsumers: make(map[string]context.CancelFunc),
		runtimeCtx:      context.Background(),
	}, logger
}

var (
	paymentProcessedV1 = types.BaseEventDefinition{
		ModuleName: "billing", Name: "PaymentProcessed", Version: "v1",
		Subject: "events.billing.v1.payment-processed",
	}
	paymentNotificationV1 = types.BaseEventDefinition{
		ModuleName: "billing", Name: "PaymentProcessedNotification", Version: "v1",
		Subject: "events.billing.v1.payment-processed",
	}
	subscriptionChangedV1 = types.BaseEventDefinition{
		ModuleName: "billing", Name: "SubscriptionChanged", Version: "v1",
		Subject: "events.billing.v1.subscription-changed",
	}
)

func TestEventStreamConsumerNames(t *testing.T) {
	t.Run("names do not depend on registration order or module set", func(t *testing.T) {
		analytics := streamEntry("analytics", "analytics-payment-processed", paymentProcessedV1)
		usage := streamEntry("usage", "usage-quota-sync", subscriptionChangedV1)
		notification := streamEntry("notification", "analytics-payment-processed", paymentNotificationV1)

		want := map[string]string{
			"analytics":    "analytics-billing-PaymentProcessed-v1",
			"usage":        "usage-billing-SubscriptionChanged-v1",
			"notification": "notification-billing-PaymentProcessedNotification-v1",
		}
		orders := [][]types.EventStreamConsumerEntry{
			{analytics, usage, notification},
			{usage, analytics, notification},
			{notification, usage, analytics},
			{usage}, // modules added or removed do not change the others
			{analytics},
		}
		for _, entries := range orders {
			names := eventStreamConsumerNames(entries)
			for i, entry := range entries {
				if got := names[i]; got != want[entry.Module.Name()] {
					t.Errorf("name of %s = %q, want %q", entry.Module.Name(), got, want[entry.Module.Name()])
				}
			}
		}
	})

	t.Run("repeat registrations by the same module on the same stream get suffixes", func(t *testing.T) {
		entries := []types.EventStreamConsumerEntry{
			streamEntry("analytics", "payments", paymentProcessedV1),
			streamEntry("audit", "payments", paymentProcessedV1),
			streamEntry("analytics", "payments", paymentProcessedV1),
			streamEntry("analytics", "payments", paymentProcessedV1),
		}
		got := eventStreamConsumerNames(entries)
		want := []string{
			"analytics-billing-PaymentProcessed-v1",
			"audit-billing-PaymentProcessed-v1",
			"analytics-billing-PaymentProcessed-v1-2",
			"analytics-billing-PaymentProcessed-v1-3",
		}
		if !slices.Equal(got, want) {
			t.Errorf("names = %v, want %v", got, want)
		}
	})

	t.Run("same name on different streams needs no suffix", func(t *testing.T) {
		entries := []types.EventStreamConsumerEntry{
			streamEntry("analytics", "stream-a", paymentProcessedV1),
			streamEntry("analytics", "stream-b", paymentProcessedV1),
		}
		got := eventStreamConsumerNames(entries)
		if got[0] != got[1] || got[0] != "analytics-billing-PaymentProcessed-v1" {
			t.Errorf("names = %v, want both analytics-billing-PaymentProcessed-v1", got)
		}
	})

	t.Run("entry without module falls back to the event identity", func(t *testing.T) {
		entry := streamEntry("x", "s", paymentProcessedV1)
		entry.Module = nil
		if got := eventStreamConsumerNames([]types.EventStreamConsumerEntry{entry})[0]; got != "billing-PaymentProcessed-v1" {
			t.Errorf("name = %q, want billing-PaymentProcessed-v1", got)
		}
	})
}

func TestLegacyConsumerMatching(t *testing.T) {
	prefix, ok := legacyConsumerPrefix(paymentProcessedV1)
	if !ok || prefix != "billing-PaymentProcessed-v1" {
		t.Fatalf("legacyConsumerPrefix = %q, %v; want billing-PaymentProcessed-v1, true", prefix, ok)
	}
	if _, ok := legacyConsumerPrefix(types.BaseEventDefinition{ModuleName: "billing", Name: "X", Version: "v1.2"}); ok {
		t.Error("a prefix containing '.' can never have been created and must not match")
	}

	stable := map[string]struct{}{"billing-PaymentProcessed-v1-9": {}}
	tests := []struct {
		name string
		want bool
	}{
		{"billing-PaymentProcessed-v1-2", true},
		{"billing-PaymentProcessed-v1-12", true},
		{"billing-PaymentProcessedNotification-v1-7", false}, // other event sharing the subject
		{"analytics-billing-PaymentProcessed-v1", false},     // stable name
		{"billing-PaymentProcessed-v1-9", false},             // this app's stable name that looks legacy
		{"billing-PaymentProcessed-v1-", false},
		{"billing-PaymentProcessed-v1-2a", false},
		{"billing-PaymentProcessed-v1", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isLegacyConsumerName(tt.name, prefix, stable); got != tt.want {
				t.Errorf("isLegacyConsumerName(%q) = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}

func TestPlanEventStreamDurables(t *testing.T) {
	t.Run("assigns legacy durables per event and counts sharing registrations", func(t *testing.T) {
		store := newFakeConsumerStore()
		store.consumers["analytics-payment-processed"] = []*jetstream.ConsumerInfo{
			legacyInfo("billing-PaymentProcessed-v1-1", 10, 10, 5),
			legacyInfo("billing-PaymentProcessed-v1-2", 5, 5, 10),
			legacyInfo("billing-PaymentProcessedNotification-v1-7", 15, 15, 0),
			legacyInfo("analytics-billing-PaymentProcessed-v1", 3, 3, 0), // already migrated
			legacyInfo("ops-tool", 1, 1, 0),
			{ // matches the legacy name but is a push consumer, which mono never created
				Name:   "billing-PaymentProcessed-v1-3",
				Config: jetstream.ConsumerConfig{DeliverSubject: "deliver.here"},
			},
		}
		lm, _ := newMigrationTestManager(store)

		entries := []types.EventStreamConsumerEntry{
			streamEntry("analytics", "analytics-payment-processed", paymentProcessedV1),
			streamEntry("usage", "usage-quota-sync", subscriptionChangedV1),
			streamEntry("notification", "analytics-payment-processed", paymentNotificationV1),
		}
		durables, err := lm.planEventStreamDurables(context.Background(), entries)
		if err != nil {
			t.Fatalf("planEventStreamDurables: %v", err)
		}

		legacyNames := func(d eventStreamDurable) []string {
			var names []string
			for _, info := range d.legacy {
				names = append(names, info.Name)
			}
			return names
		}
		if got := legacyNames(durables[0]); !slices.Equal(got, []string{"billing-PaymentProcessed-v1-1", "billing-PaymentProcessed-v1-2"}) {
			t.Errorf("analytics legacy = %v", got)
		}
		if got := legacyNames(durables[1]); len(got) != 0 {
			t.Errorf("usage legacy = %v, want none (stream does not exist yet)", got)
		}
		if got := legacyNames(durables[2]); !slices.Equal(got, []string{"billing-PaymentProcessedNotification-v1-7"}) {
			t.Errorf("notification legacy = %v", got)
		}
		for i, d := range durables {
			if d.sharedLegacy != 1 {
				t.Errorf("durable %d sharedLegacy = %d, want 1", i, d.sharedLegacy)
			}
		}
		if durables[0].name != "analytics-billing-PaymentProcessed-v1" {
			t.Errorf("analytics name = %q", durables[0].name)
		}

		lists := 0
		for _, op := range store.recorded() {
			if strings.HasPrefix(op, "list:") {
				lists++
			}
		}
		if lists != 2 {
			t.Errorf("listed streams %d times, want once per stream (2)", lists)
		}
		for _, op := range store.recorded() {
			if op == "info:analytics-payment-processed/ops-tool" || op == "info:analytics-payment-processed/analytics-billing-PaymentProcessed-v1" {
				t.Errorf("fetched info of a consumer that is not legacy: %s", op)
			}
		}
	})

	t.Run("legacy info is fetched once when registrations share it", func(t *testing.T) {
		store := newFakeConsumerStore()
		store.consumers["payments"] = []*jetstream.ConsumerInfo{legacyInfo("billing-PaymentProcessed-v1-1", 10, 10, 0)}
		lm, _ := newMigrationTestManager(store)
		durables, err := lm.planEventStreamDurables(context.Background(), []types.EventStreamConsumerEntry{
			streamEntry("analytics", "payments", paymentProcessedV1),
			streamEntry("audit", "payments", paymentProcessedV1),
		})
		if err != nil {
			t.Fatalf("planEventStreamDurables: %v", err)
		}
		if len(durables[0].legacy) != 1 || len(durables[1].legacy) != 1 {
			t.Fatalf("legacy = %v, %v; want the shared durable for both", durables[0].legacy, durables[1].legacy)
		}
		want := []string{"metadata:payments", "list:payments", "info:payments/billing-PaymentProcessed-v1-1"}
		if got := store.recorded(); !slices.Equal(got, want) {
			t.Errorf("ops = %v, want %v", got, want)
		}
	})

	t.Run("legacy durable deleted after the listing is skipped", func(t *testing.T) {
		store := newFakeConsumerStore()
		store.consumers["payments"] = []*jetstream.ConsumerInfo{legacyInfo("billing-PaymentProcessed-v1-1", 10, 10, 0)}
		store.infoErr = fmt.Errorf("lookup: %w", jetstream.ErrConsumerNotFound)
		lm, _ := newMigrationTestManager(store)
		durables, err := lm.planEventStreamDurables(context.Background(), []types.EventStreamConsumerEntry{
			streamEntry("analytics", "payments", paymentProcessedV1),
		})
		if err != nil {
			t.Fatalf("planEventStreamDurables: %v", err)
		}
		if len(durables[0].legacy) != 0 {
			t.Errorf("legacy = %v, want none", durables[0].legacy)
		}
	})

	// Findings 3 and 4: once a stream carries the migration marker, legacy
	// consumers are only reported by name. Nothing is inspected, so a leftover
	// legacy consumer that cannot be inspected no longer blocks startup, and a
	// consumer added later for the same event gets no legacy position.
	t.Run("migrated stream only reports leftover names", func(t *testing.T) {
		store := newFakeConsumerStore()
		store.consumers["payments"] = []*jetstream.ConsumerInfo{
			legacyInfo("billing-PaymentProcessed-v1-1", 10, 10, 0),
			legacyInfo("analytics-billing-PaymentProcessed-v1", 20, 20, 0),
		}
		store.metadata = map[string]map[string]string{"payments": {legacyMigrationMarkerKey: legacyMigrationMarkerValue}}
		store.infoErr = errors.New("consumer offline") // must not be reached
		lm, _ := newMigrationTestManager(store)
		durables, err := lm.planEventStreamDurables(context.Background(), []types.EventStreamConsumerEntry{
			streamEntry("analytics", "payments", paymentProcessedV1),
			streamEntry("ledger", "payments", paymentProcessedV1), // added after the migration
		})
		if err != nil {
			t.Fatalf("planEventStreamDurables: %v", err)
		}
		for i, d := range durables {
			if !d.migrated || d.legacy != nil || !slices.Equal(d.leftover, []string{"billing-PaymentProcessed-v1-1"}) {
				t.Errorf("durable %d = %+v, want migrated with only the leftover name", i, d)
			}
		}
		for _, op := range store.recorded() {
			if strings.HasPrefix(op, "info:") {
				t.Errorf("inspected a consumer on a migrated stream: %s", op)
			}
		}
	})

	t.Run("metadata error fails", func(t *testing.T) {
		store := newFakeConsumerStore()
		store.metadataErr = errors.New("jetstream unavailable")
		lm, _ := newMigrationTestManager(store)
		_, err := lm.planEventStreamDurables(context.Background(), []types.EventStreamConsumerEntry{
			streamEntry("analytics", "payments", paymentProcessedV1),
		})
		if err == nil || !strings.Contains(err.Error(), "jetstream unavailable") {
			t.Errorf("err = %v, want metadata error", err)
		}
	})

	t.Run("legacy info error fails", func(t *testing.T) {
		store := newFakeConsumerStore()
		store.consumers["payments"] = []*jetstream.ConsumerInfo{legacyInfo("billing-PaymentProcessed-v1-1", 10, 10, 0)}
		store.infoErr = errors.New("consumer offline")
		lm, _ := newMigrationTestManager(store)
		_, err := lm.planEventStreamDurables(context.Background(), []types.EventStreamConsumerEntry{
			streamEntry("analytics", "payments", paymentProcessedV1),
		})
		if err == nil || !strings.Contains(err.Error(), "consumer offline") {
			t.Errorf("err = %v, want the info error", err)
		}
	})

	t.Run("counts registrations sharing a legacy prefix on one stream", func(t *testing.T) {
		store := newFakeConsumerStore()
		lm, _ := newMigrationTestManager(store)
		entries := []types.EventStreamConsumerEntry{
			streamEntry("analytics", "payments", paymentProcessedV1),
			streamEntry("audit", "payments", paymentProcessedV1),
		}
		durables, err := lm.planEventStreamDurables(context.Background(), entries)
		if err != nil {
			t.Fatalf("planEventStreamDurables: %v", err)
		}
		if durables[0].sharedLegacy != 2 || durables[1].sharedLegacy != 2 {
			t.Errorf("sharedLegacy = %d, %d; want 2, 2", durables[0].sharedLegacy, durables[1].sharedLegacy)
		}
	})

	t.Run("listing error fails", func(t *testing.T) {
		store := newFakeConsumerStore()
		store.listErr = errors.New("jetstream unavailable")
		lm, _ := newMigrationTestManager(store)
		_, err := lm.planEventStreamDurables(context.Background(), []types.EventStreamConsumerEntry{
			streamEntry("analytics", "payments", paymentProcessedV1),
		})
		if err == nil || !strings.Contains(err.Error(), "jetstream unavailable") {
			t.Errorf("err = %v, want listing error", err)
		}
	})

	t.Run("event stream without consumer management only gets names", func(t *testing.T) {
		lm, logger := newMigrationTestManager(&mockEventStream{})
		durables, err := lm.planEventStreamDurables(context.Background(), []types.EventStreamConsumerEntry{
			streamEntry("analytics", "payments", paymentProcessedV1),
		})
		if err != nil {
			t.Fatalf("planEventStreamDurables: %v", err)
		}
		if durables[0].name != "analytics-billing-PaymentProcessed-v1" || durables[0].legacy != nil {
			t.Errorf("durable = %+v", durables[0])
		}
		if len(logger.entries) == 0 || !strings.Contains(strings.Join(logger.entries, "\n"), "legacy durable migration skipped") {
			t.Error("expected a debug log about the skipped migration")
		}
	})
}

func TestSetupEventStreamConsumerLegacyMigration(t *testing.T) {
	setup := func(t *testing.T, store *fakeConsumerStore, entry types.EventStreamConsumerEntry, durable eventStreamDurable) (*mockLogger, error) {
		t.Helper()
		lm, logger := newMigrationTestManager(store)
		err := lm.setupEventStreamConsumer(context.Background(), entry, durable)
		lm.mu.RLock()
		for _, cancel := range lm.streamConsumers {
			cancel()
		}
		lm.mu.RUnlock()
		return logger, err
	}
	interestEntry := func(policy types.DeliverPolicy) types.EventStreamConsumerEntry {
		entry := streamEntry("analytics", "payments", paymentProcessedV1)
		entry.Config.Stream.Retention = types.InterestPolicy
		entry.Config.Consumer.DeliverPolicy = policy
		return entry
	}
	const stable = "analytics-billing-PaymentProcessed-v1"

	t.Run("single legacy position is carried over and the legacy durable kept", func(t *testing.T) {
		store := newFakeConsumerStore()
		store.cached = &jetstream.ConsumerInfo{Name: stable}
		durable := eventStreamDurable{name: stable, sharedLegacy: 1, legacy: []*jetstream.ConsumerInfo{
			legacyInfo("billing-PaymentProcessed-v1-1", 10, 10, 5),
		}}
		logger, err := setup(t, store, interestEntry(types.DeliverAllPolicy), durable)
		if err != nil {
			t.Fatalf("setup: %v", err)
		}
		want := []string{"create:payments/" + stable, "reset:payments/" + stable + "@11"}
		if got := store.recorded(); !slices.Equal(got, want) {
			t.Errorf("ops = %v, want %v", got, want)
		}
		if !logger.hasWarnContaining("nats consumer rm payments billing-PaymentProcessed-v1-1") {
			t.Error("missing warning for the kept legacy durable")
		}
	})

	t.Run("legacy durables stopped at the same floor are carried over", func(t *testing.T) {
		store := newFakeConsumerStore()
		store.cached = &jetstream.ConsumerInfo{Name: stable}
		durable := eventStreamDurable{name: stable, sharedLegacy: 1, legacy: []*jetstream.ConsumerInfo{
			legacyInfo("billing-PaymentProcessed-v1-1", 10, 10, 5),
			legacyInfo("billing-PaymentProcessed-v1-2", 12, 10, 5),
		}}
		if _, err := setup(t, store, interestEntry(types.DeliverAllPolicy), durable); err != nil {
			t.Fatalf("setup: %v", err)
		}
		if got := store.recorded(); !slices.Contains(got, "reset:payments/"+stable+"@11") {
			t.Errorf("ops = %v, want reset to 11", got)
		}
	})

	// Finding 1: a single registration now, but the legacy durables stopped at
	// different positions. One may have belonged to a module removed in the
	// same deploy, so resetting to the highest floor could skip messages this
	// consumer never processed. The configured start is kept (replay over loss).
	t.Run("legacy durables at different positions are not carried over", func(t *testing.T) {
		store := newFakeConsumerStore()
		store.cached = &jetstream.ConsumerInfo{Name: stable}
		durable := eventStreamDurable{name: stable, sharedLegacy: 1, legacy: []*jetstream.ConsumerInfo{
			legacyInfo("billing-PaymentProcessed-v1-1", 300, 300, 0), // this module
			legacyInfo("billing-PaymentProcessed-v1-2", 500, 500, 0), // a removed module, further ahead
		}}
		logger, err := setup(t, store, interestEntry(types.DeliverAllPolicy), durable)
		if err != nil {
			t.Fatalf("setup: %v", err)
		}
		if got := store.recorded(); !slices.Equal(got, []string{"create:payments/" + stable}) {
			t.Errorf("ops = %v, want only create (no reset)", got)
		}
		if !logger.hasWarnContaining("stopped at different positions") {
			t.Error("expected a warning that the position was not carried over")
		}
		for _, legacy := range []string{"billing-PaymentProcessed-v1-1", "billing-PaymentProcessed-v1-2"} {
			if !logger.hasWarnContaining("nats consumer rm payments " + legacy) {
				t.Errorf("missing warning for kept legacy durable %s", legacy)
			}
		}
	})

	// Finding 2: several registrations share the legacy durables. Legacy names
	// do not say which one each module used, and a module may have none of its
	// own (never delivered, or added in this deploy), so no registration is
	// reset to another one's position.
	t.Run("shared legacy durables are not carried over", func(t *testing.T) {
		for name, legacy := range map[string][]*jetstream.ConsumerInfo{
			"one position for two modules": {
				legacyInfo("billing-PaymentProcessed-v1-1", 100, 100, 0),
				legacyInfo("billing-PaymentProcessed-v1-2", 0, 0, 100), // the other module's never delivered
			},
			"a position each": {
				legacyInfo("billing-PaymentProcessed-v1-1", 10, 10, 5),
				legacyInfo("billing-PaymentProcessed-v1-2", 5, 5, 10),
			},
		} {
			t.Run(name, func(t *testing.T) {
				store := newFakeConsumerStore()
				store.cached = &jetstream.ConsumerInfo{Name: stable}
				durable := eventStreamDurable{name: stable, sharedLegacy: 2, legacy: legacy}
				logger, err := setup(t, store, interestEntry(types.DeliverAllPolicy), durable)
				if err != nil {
					t.Fatalf("setup: %v", err)
				}
				if got := store.recorded(); !slices.Equal(got, []string{"create:payments/" + stable}) {
					t.Errorf("ops = %v, want only create (no reset)", got)
				}
				if !logger.hasWarnContaining("do not identify their owner") {
					t.Error("expected a warning about shared legacy durables")
				}
			})
		}
	})

	// Finding 3: a consumer added after the migration starts from its
	// configured deliver policy instead of being fast-forwarded to the legacy
	// position, and the stream keeps its marker.
	t.Run("migrated stream keeps its marker and carries nothing over", func(t *testing.T) {
		store := newFakeConsumerStore()
		store.cached = &jetstream.ConsumerInfo{Name: "ledger-billing-PaymentProcessed-v1"}
		entry := streamEntry("ledger", "payments", paymentProcessedV1)
		entry.Config.Stream.Retention = types.InterestPolicy
		entry.Config.Stream.Metadata = map[string]string{"owner": "billing"}
		durable := eventStreamDurable{
			name:     "ledger-billing-PaymentProcessed-v1",
			migrated: true,
			leftover: []string{"billing-PaymentProcessed-v1-1"},
		}
		logger, err := setup(t, store, entry, durable)
		if err != nil {
			t.Fatalf("setup: %v", err)
		}
		if got := store.recorded(); !slices.Equal(got, []string{"create:payments/ledger-billing-PaymentProcessed-v1"}) {
			t.Errorf("ops = %v, want only create", got)
		}
		writes := store.writtenStreams()
		if len(writes) != 1 || writes[0].Metadata[legacyMigrationMarkerKey] != legacyMigrationMarkerValue || writes[0].Metadata["owner"] != "billing" {
			t.Errorf("stream writes = %+v, want the user's metadata plus the marker", writes)
		}
		if !logger.hasWarnContaining("nats consumer rm payments billing-PaymentProcessed-v1-1") {
			t.Error("expected a warning for the leftover legacy consumer")
		}
	})

	t.Run("legacy durables that never delivered carry no position", func(t *testing.T) {
		store := newFakeConsumerStore()
		store.cached = &jetstream.ConsumerInfo{Name: stable}
		durable := eventStreamDurable{name: stable, sharedLegacy: 1, legacy: []*jetstream.ConsumerInfo{
			legacyInfo("billing-PaymentProcessed-v1-1", 10, 10, 5),
			legacyInfo("billing-PaymentProcessed-v1-2", 0, 199, 10), // fresh: floor is FirstSeq-1
		}}
		if _, err := setup(t, store, interestEntry(types.DeliverAllPolicy), durable); err != nil {
			t.Fatalf("setup: %v", err)
		}
		if got := store.recorded(); !slices.Contains(got, "reset:payments/"+stable+"@11") {
			t.Errorf("ops = %v, want reset to 11", got)
		}
	})

	t.Run("no reset when nothing was ever delivered by legacy durables", func(t *testing.T) {
		store := newFakeConsumerStore()
		durable := eventStreamDurable{name: stable, sharedLegacy: 1, legacy: []*jetstream.ConsumerInfo{
			legacyInfo("billing-PaymentProcessed-v1-1", 0, 0, 5),
		}}
		logger, err := setup(t, store, interestEntry(types.DeliverAllPolicy), durable)
		if err != nil {
			t.Fatalf("setup: %v", err)
		}
		if got := store.recorded(); !slices.Equal(got, []string{"create:payments/" + stable}) {
			t.Errorf("ops = %v, want only create", got)
		}
		if !logger.hasWarnContaining("billing-PaymentProcessed-v1-1") {
			t.Error("kept legacy durable must still be reported")
		}
	})

	t.Run("stable durable already consuming is not rewound", func(t *testing.T) {
		store := newFakeConsumerStore()
		store.cached = &jetstream.ConsumerInfo{Name: stable, Delivered: jetstream.SequenceInfo{Consumer: 4, Stream: 14}}
		durable := eventStreamDurable{name: stable, sharedLegacy: 1, legacy: []*jetstream.ConsumerInfo{
			legacyInfo("billing-PaymentProcessed-v1-1", 10, 10, 5),
		}}
		if _, err := setup(t, store, interestEntry(types.DeliverAllPolicy), durable); err != nil {
			t.Fatalf("setup: %v", err)
		}
		for _, op := range store.recorded() {
			if strings.HasPrefix(op, "reset:") {
				t.Errorf("unexpected reset: %v", store.recorded())
			}
		}
	})

	t.Run("falls back to consumer info when the reply carried none", func(t *testing.T) {
		store := newFakeConsumerStore()
		durable := eventStreamDurable{name: stable, sharedLegacy: 1, legacy: []*jetstream.ConsumerInfo{
			legacyInfo("billing-PaymentProcessed-v1-1", 10, 10, 5),
		}}
		if _, err := setup(t, store, interestEntry(types.DeliverAllPolicy), durable); err != nil {
			t.Fatalf("setup: %v", err)
		}
		want := []string{"create:payments/" + stable, "info:payments/" + stable, "reset:payments/" + stable + "@11"}
		if got := store.recorded(); !slices.Equal(got, want) {
			t.Errorf("ops = %v, want %v", got, want)
		}
	})

	t.Run("consumer info error fails instead of replaying", func(t *testing.T) {
		store := newFakeConsumerStore()
		store.infoErr = errors.New("no responders")
		durable := eventStreamDurable{name: stable, sharedLegacy: 1, legacy: []*jetstream.ConsumerInfo{
			legacyInfo("billing-PaymentProcessed-v1-1", 10, 10, 5),
		}}
		if _, err := setup(t, store, interestEntry(types.DeliverAllPolicy), durable); err == nil {
			t.Fatal("expected an error")
		}
	})

	t.Run("deliver-new consumer is not repositioned and reports the backlog", func(t *testing.T) {
		store := newFakeConsumerStore()
		store.cached = &jetstream.ConsumerInfo{Name: stable, Delivered: jetstream.SequenceInfo{Stream: 18}}
		durable := eventStreamDurable{name: stable, sharedLegacy: 1, legacy: []*jetstream.ConsumerInfo{
			legacyInfo("billing-PaymentProcessed-v1-7", 15, 15, 3),
		}}
		logger, err := setup(t, store, interestEntry(types.DeliverNewPolicy), durable)
		if err != nil {
			t.Fatalf("setup: %v", err)
		}
		if got := store.recorded(); !slices.Equal(got, []string{"create:payments/" + stable}) {
			t.Errorf("ops = %v, want only create", got)
		}
		if !logger.hasWarnContaining("does not allow repositioning") {
			t.Error("expected a warning that the backlog was not carried over")
		}
	})

	t.Run("deliver-new consumer reports a backlog the legacy durable never acknowledged", func(t *testing.T) {
		store := newFakeConsumerStore()
		store.cached = &jetstream.ConsumerInfo{Name: stable, Delivered: jetstream.SequenceInfo{Stream: 18}}
		legacy := legacyInfo("billing-PaymentProcessed-v1-7", 3, 0, 15) // delivered 3, acknowledged none
		legacy.NumAckPending = 3
		durable := eventStreamDurable{name: stable, sharedLegacy: 1, legacy: []*jetstream.ConsumerInfo{legacy}}
		logger, err := setup(t, store, interestEntry(types.DeliverNewPolicy), durable)
		if err != nil {
			t.Fatalf("setup: %v", err)
		}
		if got := store.recorded(); !slices.Equal(got, []string{"create:payments/" + stable}) {
			t.Errorf("ops = %v, want only create", got)
		}
		if !logger.hasWarnContaining("does not allow repositioning") {
			t.Error("expected a warning that the unacknowledged backlog was not carried over")
		}
	})

	t.Run("caught-up deliver-new consumer needs no warning", func(t *testing.T) {
		store := newFakeConsumerStore()
		store.cached = &jetstream.ConsumerInfo{Name: stable, Delivered: jetstream.SequenceInfo{Stream: 15}}
		durable := eventStreamDurable{name: stable, sharedLegacy: 1, legacy: []*jetstream.ConsumerInfo{
			legacyInfo("billing-PaymentProcessed-v1-7", 15, 15, 0),
		}}
		logger, err := setup(t, store, interestEntry(types.DeliverNewPolicy), durable)
		if err != nil {
			t.Fatalf("setup: %v", err)
		}
		if logger.hasWarnContaining("does not allow repositioning") {
			t.Error("nothing was left behind, so no warning is expected")
		}
	})

	t.Run("durable already at the legacy floor is not reset again", func(t *testing.T) {
		store := newFakeConsumerStore()
		// Another instance reset it to 11 and has not delivered yet.
		store.cached = &jetstream.ConsumerInfo{Name: stable, Delivered: jetstream.SequenceInfo{Stream: 10}}
		durable := eventStreamDurable{name: stable, sharedLegacy: 1, legacy: []*jetstream.ConsumerInfo{
			legacyInfo("billing-PaymentProcessed-v1-1", 10, 10, 5),
		}}
		if _, err := setup(t, store, interestEntry(types.DeliverAllPolicy), durable); err != nil {
			t.Fatalf("setup: %v", err)
		}
		if got := store.recorded(); !slices.Equal(got, []string{"create:payments/" + stable}) {
			t.Errorf("ops = %v, want only create", got)
		}
	})

	t.Run("rejected reset keeps the configured start", func(t *testing.T) {
		store := newFakeConsumerStore()
		store.cached = &jetstream.ConsumerInfo{Name: stable}
		store.resetErrs = []error{fmt.Errorf("wrapped: %w", jetstream.ErrConsumerInvalidReset)}
		durable := eventStreamDurable{name: stable, sharedLegacy: 1, legacy: []*jetstream.ConsumerInfo{
			legacyInfo("billing-PaymentProcessed-v1-1", 10, 10, 5),
		}}
		if _, err := setup(t, store, interestEntry(types.DeliverByStartSequencePolicy), durable); err != nil {
			t.Fatalf("setup: %v", err)
		}
	})

	t.Run("transient reset errors are retried", func(t *testing.T) {
		defer func(prev time.Duration) { legacyResetBackoff = prev }(legacyResetBackoff)
		legacyResetBackoff = time.Millisecond

		store := newFakeConsumerStore()
		store.cached = &jetstream.ConsumerInfo{Name: stable}
		store.resetErrs = []error{nats.ErrTimeout, fmt.Errorf("request: %w", nats.ErrNoResponders)}
		durable := eventStreamDurable{name: stable, sharedLegacy: 1, legacy: []*jetstream.ConsumerInfo{
			legacyInfo("billing-PaymentProcessed-v1-1", 10, 10, 5),
		}}
		if _, err := setup(t, store, interestEntry(types.DeliverAllPolicy), durable); err != nil {
			t.Fatalf("setup: %v", err)
		}
		resets := 0
		for _, op := range store.recorded() {
			if strings.HasPrefix(op, "reset:") {
				resets++
			}
		}
		if resets != 3 {
			t.Errorf("resets = %d, want 3", resets)
		}
	})

	t.Run("persistent reset failure fails startup and starts no loop", func(t *testing.T) {
		defer func(prev time.Duration) { legacyResetBackoff = prev }(legacyResetBackoff)
		legacyResetBackoff = time.Millisecond

		store := newFakeConsumerStore()
		store.cached = &jetstream.ConsumerInfo{Name: stable}
		for range legacyResetAttempts {
			store.resetErrs = append(store.resetErrs, nats.ErrTimeout)
		}
		durable := eventStreamDurable{name: stable, sharedLegacy: 1, legacy: []*jetstream.ConsumerInfo{
			legacyInfo("billing-PaymentProcessed-v1-1", 10, 10, 5),
		}}
		lm, _ := newMigrationTestManager(store)
		err := lm.setupEventStreamConsumer(context.Background(), interestEntry(types.DeliverAllPolicy), durable)
		if !errors.Is(err, nats.ErrTimeout) {
			t.Fatalf("err = %v, want reset failure", err)
		}
		if len(lm.streamConsumers) != 0 {
			t.Error("fetch loop must not start before the position is carried over")
		}
	})

	t.Run("permanent reset error is not retried", func(t *testing.T) {
		store := newFakeConsumerStore()
		store.cached = &jetstream.ConsumerInfo{Name: stable}
		store.resetErrs = []error{errors.New("permission denied")}
		durable := eventStreamDurable{name: stable, sharedLegacy: 1, legacy: []*jetstream.ConsumerInfo{
			legacyInfo("billing-PaymentProcessed-v1-1", 10, 10, 5),
		}}
		if _, err := setup(t, store, interestEntry(types.DeliverAllPolicy), durable); err == nil {
			t.Fatal("expected an error")
		}
		want := []string{"create:payments/" + stable, "reset:payments/" + stable + "@11"}
		if got := store.recorded(); !slices.Equal(got, want) {
			t.Errorf("ops = %v, want %v", got, want)
		}
	})

	t.Run("work-queue stream removes legacy durable before creating the stable one", func(t *testing.T) {
		store := newFakeConsumerStore()
		entry := streamEntry("orders", "orders", paymentProcessedV1)
		entry.Config.Stream.Retention = types.WorkQueuePolicy
		durable := eventStreamDurable{name: "orders-billing-PaymentProcessed-v1", sharedLegacy: 1, legacy: []*jetstream.ConsumerInfo{
			legacyInfo("billing-PaymentProcessed-v1-2", 10, 10, 3),
		}}
		logger, err := setup(t, store, entry, durable)
		if err != nil {
			t.Fatalf("setup: %v", err)
		}
		want := []string{"delete:orders/billing-PaymentProcessed-v1-2", "create:orders/orders-billing-PaymentProcessed-v1"}
		if got := store.recorded(); !slices.Equal(got, want) {
			t.Errorf("ops = %v, want %v", got, want)
		}
		if logger.hasWarnContaining("nats consumer rm") {
			t.Error("a removed durable must not be reported as kept")
		}
	})

	t.Run("work-queue removal tolerates an already deleted durable", func(t *testing.T) {
		store := newFakeConsumerStore()
		store.deleteErr = fmt.Errorf("wrapped: %w", jetstream.ErrConsumerNotFound)
		entry := streamEntry("orders", "orders", paymentProcessedV1)
		entry.Config.Stream.Retention = types.WorkQueuePolicy
		durable := eventStreamDurable{name: "orders-billing-PaymentProcessed-v1", legacy: []*jetstream.ConsumerInfo{
			legacyInfo("billing-PaymentProcessed-v1-2", 10, 10, 3),
		}}
		logger, err := setup(t, store, entry, durable)
		if err != nil {
			t.Fatalf("setup: %v", err)
		}
		want := []string{"delete:orders/billing-PaymentProcessed-v1-2", "create:orders/orders-billing-PaymentProcessed-v1"}
		if got := store.recorded(); !slices.Equal(got, want) {
			t.Errorf("ops = %v, want %v", got, want)
		}
		logger.mu.Lock()
		defer logger.mu.Unlock()
		for _, e := range logger.entries {
			if strings.Contains(e, "Removed legacy event stream consumer durable") {
				t.Errorf("a durable that was already gone must not be reported as removed: %s", e)
			}
		}
	})

	t.Run("work-queue removal error fails", func(t *testing.T) {
		store := newFakeConsumerStore()
		store.deleteErr = errors.New("permission denied")
		entry := streamEntry("orders", "orders", paymentProcessedV1)
		entry.Config.Stream.Retention = types.WorkQueuePolicy
		durable := eventStreamDurable{name: "orders-billing-PaymentProcessed-v1", legacy: []*jetstream.ConsumerInfo{
			legacyInfo("billing-PaymentProcessed-v1-2", 10, 10, 3),
		}}
		if _, err := setup(t, store, entry, durable); err == nil {
			t.Fatal("expected an error")
		}
		if got := store.recorded(); slices.Contains(got, "create:orders/orders-billing-PaymentProcessed-v1") {
			t.Errorf("must not create after a failed removal: %v", got)
		}
	})

	t.Run("work-queue configuration the server rejects keeps the legacy durable", func(t *testing.T) {
		for name, mutate := range map[string]func(*types.ConsumerConfig){
			"deliver new": func(c *types.ConsumerConfig) { c.DeliverPolicy = types.DeliverNewPolicy },
			"ack none":    func(c *types.ConsumerConfig) { c.AckPolicy = types.AckNonePolicy },
		} {
			t.Run(name, func(t *testing.T) {
				store := newFakeConsumerStore()
				entry := streamEntry("orders", "orders", paymentProcessedV1)
				entry.Config.Stream.Retention = types.WorkQueuePolicy
				mutate(&entry.Config.Consumer)
				durable := eventStreamDurable{name: "orders-billing-PaymentProcessed-v1", legacy: []*jetstream.ConsumerInfo{
					legacyInfo("billing-PaymentProcessed-v1-2", 10, 10, 3),
				}}
				if _, err := setup(t, store, entry, durable); err == nil {
					t.Fatal("expected an error")
				}
				if got := store.recorded(); len(got) != 0 {
					t.Errorf("ops = %v, want none", got)
				}
			})
		}
	})
}

func TestMarkLegacyMigrationDone(t *testing.T) {
	t.Run("marks every unmarked stream with its last applied configuration", func(t *testing.T) {
		store := newFakeConsumerStore()
		lm, _ := newMigrationTestManager(store)
		first := streamEntry("analytics", "payments", paymentProcessedV1)
		second := streamEntry("notification", "payments", paymentNotificationV1)
		second.Config.Stream.Retention = types.InterestPolicy
		marked := streamEntry("usage", "quota", subscriptionChangedV1)
		entries := []types.EventStreamConsumerEntry{first, second, marked}
		durables := []eventStreamDurable{{}, {}, {migrated: true}}

		lm.markLegacyMigrationDone(context.Background(), entries, durables)

		writes := store.writtenStreams()
		if len(writes) != 1 {
			t.Fatalf("stream writes = %+v, want one for the unmarked stream", writes)
		}
		got := writes[0]
		if got.Name != "payments" || got.Retention != types.InterestPolicy || got.Storage != types.FileStorage ||
			got.Metadata[legacyMigrationMarkerKey] != legacyMigrationMarkerValue {
			t.Errorf("stream write = %+v, want the last configuration with defaults and the marker", got)
		}
	})

	t.Run("failure is only logged", func(t *testing.T) {
		store := newFakeConsumerStore()
		store.streamWriteErr = errors.New("jetstream unavailable")
		lm, logger := newMigrationTestManager(store)
		lm.markLegacyMigrationDone(context.Background(),
			[]types.EventStreamConsumerEntry{streamEntry("analytics", "payments", paymentProcessedV1)},
			[]eventStreamDurable{{}})
		if !logger.hasWarnContaining("Could not mark stream as migrated") {
			t.Error("expected a warning")
		}
	})

	t.Run("event stream without consumer management is left alone", func(t *testing.T) {
		es := &mockEventStream{}
		lm, _ := newMigrationTestManager(es)
		lm.markLegacyMigrationDone(context.Background(),
			[]types.EventStreamConsumerEntry{streamEntry("analytics", "payments", paymentProcessedV1)},
			[]eventStreamDurable{{}})
		if len(es.createdStreams) != 0 {
			t.Errorf("created streams = %v, want none", es.createdStreams)
		}
	})
}

func TestEventStreamConfig(t *testing.T) {
	user := types.StreamConfig{Name: "payments", Metadata: map[string]string{"owner": "billing"}}

	plain := eventStreamConfig(user, false)
	if plain.Retention != types.LimitsPolicy || plain.Storage != types.FileStorage {
		t.Errorf("defaults not applied: %+v", plain)
	}
	if _, ok := plain.Metadata[legacyMigrationMarkerKey]; ok {
		t.Error("unmarked configuration must not carry the marker")
	}

	marked := eventStreamConfig(user, true)
	if marked.Metadata[legacyMigrationMarkerKey] != legacyMigrationMarkerValue || marked.Metadata["owner"] != "billing" {
		t.Errorf("marked metadata = %v", marked.Metadata)
	}
	if _, ok := user.Metadata[legacyMigrationMarkerKey]; ok {
		t.Error("the caller's metadata map must not be modified")
	}
}

func TestResetConsumerWithRetryHonoursContext(t *testing.T) {
	defer func(prev time.Duration) { legacyResetBackoff = prev }(legacyResetBackoff)
	legacyResetBackoff = time.Hour

	store := newFakeConsumerStore()
	store.resetErrs = []error{nats.ErrTimeout}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := resetConsumerWithRetry(ctx, store, "s", "c", 5)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}
