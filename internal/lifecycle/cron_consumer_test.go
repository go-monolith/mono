package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-monolith/mono/pkg/types"
	"github.com/nats-io/nats.go/jetstream"
)

// fakeCronStore is an EventStream that also implements cronConsumerStore. It
// models the one consumer of a cron stream as the server holds it, and records
// every consumer operation in order: creates and updates with the deliver
// policy sent, resets with the sequence.
type fakeCronStore struct {
	*mockEventStream

	mu sync.Mutex
	// consumer is the consumer as the server holds it; nil means missing.
	// A create sets it, a reset moves it.
	consumer *jetstream.ConsumerInfo
	// infoErrs are returned by successive ConsumerInfo calls; nil entries,
	// and calls after the list is exhausted, read consumer.
	infoErrs []error
	// updateErrs are returned by successive UpdateConsumer calls.
	updateErrs []error
	// afterUpdate, when set, runs after each successful update, to model
	// another instance acting in between.
	afterUpdate func(f *fakeCronStore)
	createErr   error
	// peer, when set, is a consumer another instance created just before
	// this one's create: a create finds it and fails with ErrConsumerExists
	// if its metadata differs.
	peer      *jetstream.ConsumerInfo
	resetErr  error
	stream    jetstream.Stream // returned by Stream
	streamErr error
	ops       []string
	configs   []types.ConsumerConfig // config of every create and update, in order
}

func newFakeCronStore() *fakeCronStore {
	return &fakeCronStore{mockEventStream: &mockEventStream{}}
}

func (f *fakeCronStore) record(op string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ops = append(f.ops, op)
}

func (f *fakeCronStore) recordConfig(op string, cfg types.ConsumerConfig) {
	f.record(fmt.Sprintf("%s:%s:%d", op, cfg.Name, cfg.DeliverPolicy))
	f.mu.Lock()
	defer f.mu.Unlock()
	f.configs = append(f.configs, cfg)
}

func (f *fakeCronStore) recorded() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.ops)
}

func (f *fakeCronStore) ConsumerInfo(_ context.Context, _, consumer string) (*jetstream.ConsumerInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.infoErrs) > 0 {
		err := f.infoErrs[0]
		f.infoErrs = f.infoErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	if f.consumer == nil {
		return nil, fmt.Errorf("lookup %s: %w", consumer, jetstream.ErrConsumerNotFound)
	}
	info := *f.consumer
	return &info, nil
}

func (f *fakeCronStore) CreateConsumer(_ context.Context, _ string, cfg types.ConsumerConfig) (jetstream.Consumer, error) {
	f.recordConfig("create", cfg)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.createErr != nil {
		return nil, f.createErr
	}
	if f.peer != nil {
		f.consumer = f.peer
		if !maps.Equal(f.peer.Config.Metadata, cfg.Metadata) {
			return nil, fmt.Errorf("create: %w", jetstream.ErrConsumerExists)
		}
	} else {
		f.consumer = &jetstream.ConsumerInfo{Config: jetstream.ConsumerConfig{
			Name: cfg.Name, Durable: cfg.Durable, Metadata: cfg.Metadata,
		}}
	}
	return &mockJetStreamConsumer{}, nil
}

func (f *fakeCronStore) UpdateConsumer(_ context.Context, _ string, cfg types.ConsumerConfig) (jetstream.Consumer, error) {
	f.recordConfig("update", cfg)
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.updateErrs) > 0 {
		err := f.updateErrs[0]
		f.updateErrs = f.updateErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	if f.consumer != nil {
		updated := *f.consumer
		updated.Config.Durable = cfg.Durable
		updated.Config.Metadata = cfg.Metadata
		f.consumer = &updated
	}
	if f.afterUpdate != nil {
		f.afterUpdate(f)
	}
	return &mockJetStreamConsumer{}, nil
}

func (f *fakeCronStore) CreateOrUpdateConsumer(context.Context, string, types.ConsumerConfig) (jetstream.Consumer, error) {
	return nil, errors.New("cron consumers must not be created with CreateOrUpdateConsumer")
}

func (f *fakeCronStore) ResetConsumerToSequence(_ context.Context, _, consumer string, seq uint64) error {
	f.record(fmt.Sprintf("reset:%s:%d", consumer, seq))
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.resetErr != nil {
		return f.resetErr
	}
	if f.consumer != nil {
		reset := *f.consumer
		reset.Delivered.Stream = seq - 1
		f.consumer = &reset
	}
	return nil
}

func (f *fakeCronStore) Stream(_ context.Context, _ string) (jetstream.Stream, error) {
	return f.stream, f.streamErr
}

// cronPositionStream is a jetstream.Stream whose Info reports a message count
// for one subject and a last sequence. Its other methods are not implemented.
type cronPositionStream struct {
	jetstream.Stream
	subject string
	count   uint64
	lastSeq uint64
	err     error
}

func (s *cronPositionStream) Info(_ context.Context, _ ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
	if s.err != nil {
		return nil, s.err
	}
	return &jetstream.StreamInfo{State: jetstream.StreamState{
		LastSeq:  s.lastSeq,
		Subjects: map[string]uint64{s.subject: s.count},
	}}, nil
}

// configRecordingEventStream is an EventStream without consumer management
// that records the consumer configs it is asked to create.
type configRecordingEventStream struct {
	*mockEventStream
	configs []types.ConsumerConfig
}

func (e *configRecordingEventStream) CreateOrUpdateConsumer(_ context.Context, _ string, cfg types.ConsumerConfig) (jetstream.Consumer, error) {
	e.configs = append(e.configs, cfg)
	return &mockJetStreamConsumer{}, nil
}

const (
	cronTestStream   = "MONO_CRON_reports_rollup"
	cronTestConsumer = "reports-rollup-cron"
	cronTestSubject  = "services.reports.rollup"
)

func cronTestConsumerConfig() types.ConsumerConfig {
	return types.ConsumerConfig{
		Name:          cronTestConsumer,
		Durable:       cronTestConsumer,
		FilterSubject: cronTestSubject,
		AckPolicy:     types.AckExplicitPolicy,
		AckWait:       defaultCronAckWait,
		MaxDeliver:    defaultCronMaxDeliver,
	}
}

func cronTestEntry() *types.ServiceEntry {
	return &types.ServiceEntry{Name: "rollup", ModuleName: "reports", Subject: cronTestSubject}
}

func (m *mockLogger) logged() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.entries)
}

func loggedContaining(log *mockLogger, substr string) []string {
	var matched []string
	for _, entry := range log.logged() {
		if strings.Contains(entry, substr) {
			matched = append(matched, entry)
		}
	}
	return matched
}

func TestEnsureCronConsumer(t *testing.T) {
	ctx := context.Background()
	update := func(policy types.DeliverPolicy) string {
		return fmt.Sprintf("update:%s:%d", cronTestConsumer, policy)
	}
	updateAll := update(types.DeliverAllPolicy)
	create := fmt.Sprintf("create:%s:%d", cronTestConsumer, types.DeliverAllPolicy)
	reset := func(seq uint64) string {
		return fmt.Sprintf("reset:%s:%d", cronTestConsumer, seq)
	}
	ensure := func(lm *lifecycleManager, es types.EventStream) (jetstream.Consumer, error) {
		return lm.ensureCronConsumer(ctx, es, cronTestStream, cronTestConsumerConfig(), cronTestEntry())
	}
	logs := func(lm *lifecycleManager, substr string) []string {
		return loggedContaining(lm.logger.(*mockLogger), substr)
	}
	positioned := func(start uint64, delivered uint64) *jetstream.ConsumerInfo {
		return &jetstream.ConsumerInfo{
			Config:    jetstream.ConsumerConfig{Durable: cronTestConsumer, Metadata: cronStartMetadata(start)},
			Delivered: jetstream.SequenceInfo{Stream: delivered},
		}
	}

	t.Run("existing durable is updated once with its own deliver policy", func(t *testing.T) {
		lm := newCronTestManager()
		store := newFakeCronStore()
		store.consumer = &jetstream.ConsumerInfo{Config: jetstream.ConsumerConfig{
			Durable: cronTestConsumer, DeliverPolicy: jetstream.DeliverNewPolicy,
		}}

		consumer, err := ensure(lm, store)
		if err != nil || consumer == nil {
			t.Fatalf("ensureCronConsumer: consumer=%v err=%v", consumer, err)
		}
		if got, want := store.recorded(), []string{update(types.DeliverNewPolicy)}; !slices.Equal(got, want) {
			t.Errorf("ops = %v, want %v", got, want)
		}
		if store.configs[0].Metadata != nil {
			t.Errorf("metadata = %v, want none", store.configs[0].Metadata)
		}
		if got := logs(lm, "Cron consumer"); len(got) != 0 {
			t.Errorf("expected no cron consumer log, got %v", got)
		}
	})

	t.Run("non-durable consumer of an earlier version is converted with two updates", func(t *testing.T) {
		lm := newCronTestManager()
		store := newFakeCronStore()
		start := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
		store.consumer = &jetstream.ConsumerInfo{Config: jetstream.ConsumerConfig{
			Name:              cronTestConsumer,
			DeliverPolicy:     jetstream.DeliverByStartTimePolicy,
			OptStartTime:      &start,
			InactiveThreshold: 5 * time.Second,
		}}

		if _, err := ensure(lm, store); err != nil {
			t.Fatalf("ensureCronConsumer: %v", err)
		}
		want := []string{update(types.DeliverByStartTimePolicy), update(types.DeliverByStartTimePolicy)}
		if got := store.recorded(); !slices.Equal(got, want) {
			t.Errorf("ops = %v, want %v", got, want)
		}
		for _, cfg := range store.configs {
			if cfg.Durable != cronTestConsumer || cfg.InactiveThreshold != 0 || cfg.OptStartTime == nil || !cfg.OptStartTime.Equal(start) {
				t.Errorf("update config = %+v, want durable, no threshold, start time kept", cfg)
			}
		}
		if got := logs(lm, "Cron consumer converted to durable"); len(got) != 1 {
			t.Errorf("expected one conversion log line, got %v", lm.logger.(*mockLogger).logged())
		}
	})

	t.Run("start sequence is kept", func(t *testing.T) {
		lm := newCronTestManager()
		store := newFakeCronStore()
		store.consumer = &jetstream.ConsumerInfo{Config: jetstream.ConsumerConfig{
			Durable: cronTestConsumer, DeliverPolicy: jetstream.DeliverByStartSequencePolicy, OptStartSeq: 42,
		}}

		if _, err := ensure(lm, store); err != nil {
			t.Fatalf("ensureCronConsumer: %v", err)
		}
		if len(store.configs) != 1 || store.configs[0].OptStartSeq != 42 {
			t.Errorf("configs = %+v, want one update with start sequence 42", store.configs)
		}
	})

	t.Run("missing consumer is created after the stored occurrences", func(t *testing.T) {
		lm := newCronTestManager()
		store := newFakeCronStore()
		store.stream = &cronPositionStream{subject: cronTestSubject, count: 256, lastSeq: 300}

		if _, err := ensure(lm, store); err != nil {
			t.Fatalf("ensureCronConsumer: %v", err)
		}
		if got, want := store.recorded(), []string{create, reset(301), updateAll}; !slices.Equal(got, want) {
			t.Errorf("ops = %v, want %v", got, want)
		}
		if cfg := store.configs[0]; cfg.Durable != cronTestConsumer || cfg.Metadata[cronStartSequenceKey] != "301" {
			t.Errorf("created config = %+v, want durable with start sequence 301 recorded", cfg)
		}
		if cfg := store.configs[1]; cfg.Durable != cronTestConsumer || cfg.Metadata != nil {
			t.Errorf("update after the reset = %+v, want durable with the start sequence removed", cfg)
		}
		warnings := logs(lm, "skipped occurrences stored before it")
		if len(warnings) != 1 || !strings.Contains(warnings[0], "skipped 256") {
			t.Errorf("expected one warning with the skipped count, got %v", lm.logger.(*mockLogger).logged())
		}
	})

	t.Run("new consumer on an empty stream is not reset", func(t *testing.T) {
		lm := newCronTestManager()
		store := newFakeCronStore()
		store.stream = &cronPositionStream{subject: cronTestSubject}

		if _, err := ensure(lm, store); err != nil {
			t.Fatalf("ensureCronConsumer: %v", err)
		}
		if got, want := store.recorded(), []string{create}; !slices.Equal(got, want) {
			t.Errorf("ops = %v, want %v", got, want)
		}
		if got := store.configs[0].Metadata; got != nil {
			t.Errorf("metadata = %v, want none", got)
		}
		if got := logs(lm, "WARN"); len(got) != 0 {
			t.Errorf("expected no warning, got %v", got)
		}
		if got := logs(lm, "Cron consumer created"); len(got) != 1 {
			t.Errorf("expected one creation log line, got %v", lm.logger.(*mockLogger).logged())
		}
	})

	// For example a deprecated service re-armed after its consumer was
	// removed: the stream holds only schedule and control messages.
	t.Run("new consumer on a stream without occurrences is positioned without a warning", func(t *testing.T) {
		lm := newCronTestManager()
		store := newFakeCronStore()
		store.stream = &cronPositionStream{subject: cronTestSubject, lastSeq: 5}

		if _, err := ensure(lm, store); err != nil {
			t.Fatalf("ensureCronConsumer: %v", err)
		}
		if got, want := store.recorded(), []string{create, reset(6), updateAll}; !slices.Equal(got, want) {
			t.Errorf("ops = %v, want %v", got, want)
		}
		if got := logs(lm, "WARN"); len(got) != 0 {
			t.Errorf("expected no warning, got %v", got)
		}
		if got := logs(lm, "Cron consumer created"); len(got) != 1 {
			t.Errorf("expected one creation log line, got %v", lm.logger.(*mockLogger).logged())
		}
	})

	// Two instances start at once and both find the consumer missing.
	t.Run("another instance created it with the same start and positioned it", func(t *testing.T) {
		lm := newCronTestManager()
		store := newFakeCronStore()
		store.stream = &cronPositionStream{subject: cronTestSubject, count: 4, lastSeq: 300}
		store.peer = positioned(301, 305)

		if _, err := ensure(lm, store); err != nil {
			t.Fatalf("ensureCronConsumer: %v", err)
		}
		// Not reset again; the start sequence the other instance has not
		// removed yet is removed.
		if got, want := store.recorded(), []string{create, updateAll}; !slices.Equal(got, want) {
			t.Errorf("ops = %v, want %v", got, want)
		}
		if got := store.consumer.Config.Metadata; got != nil {
			t.Errorf("metadata = %v, want none", got)
		}
		if got := logs(lm, "WARN"); len(got) != 0 {
			t.Errorf("expected no warning, got %v", got)
		}
	})

	t.Run("another instance created it with a different start, not yet positioned", func(t *testing.T) {
		lm := newCronTestManager()
		store := newFakeCronStore()
		store.stream = &cronPositionStream{subject: cronTestSubject, count: 4, lastSeq: 301}
		store.peer = positioned(301, 0)

		if _, err := ensure(lm, store); err != nil {
			t.Fatalf("ensureCronConsumer: %v", err)
		}
		// The create fails, and the peer's consumer is updated and moved to
		// the peer's start, never replaced.
		if got, want := store.recorded(), []string{create, updateAll, reset(301), updateAll}; !slices.Equal(got, want) {
			t.Errorf("ops = %v, want %v", got, want)
		}
		if got := store.configs[1].Metadata[cronStartSequenceKey]; got != "301" {
			t.Errorf("update kept start sequence %q, want the peer's 301", got)
		}
	})

	t.Run("another instance created it with a different start and positioned it", func(t *testing.T) {
		lm := newCronTestManager()
		store := newFakeCronStore()
		store.stream = &cronPositionStream{subject: cronTestSubject, count: 4, lastSeq: 301}
		store.peer = positioned(301, 300)

		if _, err := ensure(lm, store); err != nil {
			t.Fatalf("ensureCronConsumer: %v", err)
		}
		if got, want := store.recorded(), []string{create, updateAll}; !slices.Equal(got, want) {
			t.Errorf("ops = %v, want %v", got, want)
		}
	})

	// The other instance positions the consumer and removes the start
	// sequence between this instance's lookup and its update, which writes
	// the start sequence back.
	t.Run("start sequence written back after another instance positioned the consumer is removed", func(t *testing.T) {
		lm := newCronTestManager()
		store := newFakeCronStore()
		store.consumer = positioned(301, 0)
		store.afterUpdate = func(f *fakeCronStore) {
			f.consumer.Delivered.Stream = 300
			f.afterUpdate = nil
		}

		if _, err := ensure(lm, store); err != nil {
			t.Fatalf("ensureCronConsumer: %v", err)
		}
		if got, want := store.recorded(), []string{updateAll, updateAll}; !slices.Equal(got, want) {
			t.Errorf("ops = %v, want %v", got, want)
		}
		if got := store.consumer.Config.Metadata; got != nil {
			t.Errorf("metadata = %v, want none", got)
		}
		if got := logs(lm, "interrupted creation"); len(got) != 0 {
			t.Errorf("expected no positioning log line, got %v", got)
		}
	})

	t.Run("interrupted creation is positioned on the next start", func(t *testing.T) {
		lm := newCronTestManager()
		store := newFakeCronStore()
		store.consumer = positioned(301, 0)
		store.consumer.Config.Metadata["_nats.level"] = "1"

		if _, err := ensure(lm, store); err != nil {
			t.Fatalf("ensureCronConsumer: %v", err)
		}
		if got, want := store.recorded(), []string{updateAll, reset(301), updateAll}; !slices.Equal(got, want) {
			t.Errorf("ops = %v, want %v", got, want)
		}
		// The start sequence is kept until the reset is done, without the
		// server's own entries, and removed afterwards.
		if got := store.configs[0].Metadata; len(got) != 1 || got[cronStartSequenceKey] != "301" {
			t.Errorf("update metadata before the reset = %v, want only the start sequence 301", got)
		}
		if got := store.configs[1].Metadata; got != nil {
			t.Errorf("update metadata after the reset = %v, want none", got)
		}
		if got := logs(lm, "interrupted creation"); len(got) != 1 {
			t.Errorf("expected one log line, got %v", lm.logger.(*mockLogger).logged())
		}
	})

	// The start sequence is left over when a start stopped after the reset.
	// It is dropped, so a later reset by an operator is not undone.
	t.Run("positioned consumer is not reset again and loses its start sequence", func(t *testing.T) {
		for _, delivered := range []uint64{300, 450} {
			lm := newCronTestManager()
			store := newFakeCronStore()
			store.consumer = positioned(301, delivered)

			if _, err := ensure(lm, store); err != nil {
				t.Fatalf("ensureCronConsumer: %v", err)
			}
			if got, want := store.recorded(), []string{updateAll}; !slices.Equal(got, want) {
				t.Errorf("delivered %d: ops = %v, want %v", delivered, got, want)
			}
			if got := store.configs[0].Metadata; got != nil {
				t.Errorf("delivered %d: metadata = %v, want none", delivered, got)
			}
		}
	})

	t.Run("invalid start sequence metadata is ignored", func(t *testing.T) {
		for _, value := range []string{"", "0", "-1", "abc"} {
			lm := newCronTestManager()
			store := newFakeCronStore()
			store.consumer = &jetstream.ConsumerInfo{Config: jetstream.ConsumerConfig{
				Durable: cronTestConsumer, Metadata: map[string]string{cronStartSequenceKey: value},
			}}
			if _, err := ensure(lm, store); err != nil {
				t.Fatalf("ensureCronConsumer: %v", err)
			}
			if got, want := store.recorded(), []string{updateAll}; !slices.Equal(got, want) {
				t.Errorf("%q: ops = %v, want %v", value, got, want)
			}
			if store.configs[0].Metadata != nil {
				t.Errorf("%q: metadata = %v, want none", value, store.configs[0].Metadata)
			}
		}
	})

	// A non-durable consumer of an earlier version can time out between the
	// lookup and an update. It must be created after the stored occurrences,
	// never recreated from the start of the stream.
	t.Run("consumer deleted before an update is created after the stored occurrences", func(t *testing.T) {
		tests := map[string]struct {
			updateErrs []error
			updates    int
		}{
			"first update, does not exist": {updateErrs: []error{jetstream.ErrConsumerDoesNotExist}, updates: 1},
			"first update, not found":      {updateErrs: []error{jetstream.ErrConsumerNotFound}, updates: 1},
			"second update":                {updateErrs: []error{nil, jetstream.ErrConsumerDoesNotExist}, updates: 2},
		}
		for name, tt := range tests {
			t.Run(name, func(t *testing.T) {
				lm := newCronTestManager()
				store := newFakeCronStore()
				store.consumer = &jetstream.ConsumerInfo{Config: jetstream.ConsumerConfig{
					Name: cronTestConsumer, DeliverPolicy: jetstream.DeliverAllPolicy,
				}}
				for _, err := range tt.updateErrs {
					if err != nil {
						err = fmt.Errorf("update: %w", err)
					}
					store.updateErrs = append(store.updateErrs, err)
				}
				store.stream = &cronPositionStream{subject: cronTestSubject, count: 3, lastSeq: 7}
				deleted := &fakeDeletingStore{fakeCronStore: store}

				if _, err := ensure(lm, deleted); err != nil {
					t.Fatalf("ensureCronConsumer: %v", err)
				}
				var want []string
				for range tt.updates {
					want = append(want, updateAll)
				}
				want = append(want, create, reset(8), updateAll)
				if got := store.recorded(); !slices.Equal(got, want) {
					t.Errorf("ops = %v, want %v", got, want)
				}
			})
		}
	})

	t.Run("errors are returned", func(t *testing.T) {
		boom := errors.New("boom")
		position := func(f *fakeCronStore) {
			f.stream = &cronPositionStream{subject: cronTestSubject, count: 2, lastSeq: 9}
		}
		tests := map[string]struct {
			setup func(*fakeCronStore)
			ops   []string
		}{
			"lookup fails": {
				setup: func(f *fakeCronStore) { f.infoErrs = []error{boom} },
			},
			"unknown deliver policy": {
				setup: func(f *fakeCronStore) {
					f.consumer = &jetstream.ConsumerInfo{Config: jetstream.ConsumerConfig{DeliverPolicy: jetstream.DeliverPolicy(99)}}
				},
			},
			"update fails": {
				setup: func(f *fakeCronStore) {
					f.consumer = &jetstream.ConsumerInfo{Config: jetstream.ConsumerConfig{Durable: cronTestConsumer}}
					f.updateErrs = []error{boom}
				},
				ops: []string{updateAll},
			},
			"second conversion update fails": {
				setup: func(f *fakeCronStore) {
					f.consumer = &jetstream.ConsumerInfo{Config: jetstream.ConsumerConfig{Name: cronTestConsumer}}
					f.updateErrs = []error{nil, boom}
				},
				ops: []string{updateAll, updateAll},
			},
			"reset of an interrupted creation fails": {
				setup: func(f *fakeCronStore) {
					f.consumer = positioned(5, 0)
					f.resetErr = boom
				},
				ops: []string{updateAll, reset(5)},
			},
			"stream lookup fails": {
				setup: func(f *fakeCronStore) { f.streamErr = boom },
			},
			"stream is unavailable": {
				setup: func(*fakeCronStore) {},
			},
			"stream info fails": {
				setup: func(f *fakeCronStore) { f.stream = &cronPositionStream{err: boom} },
			},
			"create fails": {
				setup: func(f *fakeCronStore) {
					position(f)
					f.createErr = boom
				},
				ops: []string{create},
			},
			"lookup before the reset fails": {
				setup: func(f *fakeCronStore) {
					position(f)
					f.infoErrs = []error{nil, boom}
				},
				ops: []string{create},
			},
			"reset after create fails": {
				setup: func(f *fakeCronStore) {
					position(f)
					f.resetErr = boom
				},
				ops: []string{create, reset(10)},
			},
			"removing the start sequence fails": {
				setup: func(f *fakeCronStore) {
					position(f)
					f.updateErrs = []error{boom}
				},
				ops: []string{create, reset(10), updateAll},
			},
			"lookup of the consumer another instance created fails": {
				setup: func(f *fakeCronStore) {
					position(f)
					f.peer = positioned(5, 0)
					f.infoErrs = []error{nil, boom}
				},
				ops: []string{create},
			},
		}
		for name, tt := range tests {
			t.Run(name, func(t *testing.T) {
				lm := newCronTestManager()
				store := newFakeCronStore()
				tt.setup(store)
				if _, err := ensure(lm, store); err == nil {
					t.Fatal("expected an error")
				}
				if got := store.recorded(); !slices.Equal(got, tt.ops) {
					t.Errorf("ops = %v, want %v", got, tt.ops)
				}
			})
		}
	})

	t.Run("event stream without consumer management creates a durable for new occurrences", func(t *testing.T) {
		lm := newCronTestManager()
		es := &configRecordingEventStream{mockEventStream: &mockEventStream{}}
		if _, err := ensure(lm, es); err != nil {
			t.Fatalf("ensureCronConsumer: %v", err)
		}
		if len(es.configs) != 1 {
			t.Fatalf("created %d consumers, want 1", len(es.configs))
		}
		if cfg := es.configs[0]; cfg.Durable != cronTestConsumer || cfg.DeliverPolicy != types.DeliverNewPolicy {
			t.Errorf("created config = %+v, want durable with deliver new", cfg)
		}
	})
}

// fakeDeletingStore deletes the consumer when an update reports it missing,
// as the server does for a non-durable consumer that timed out.
type fakeDeletingStore struct {
	*fakeCronStore
}

func (f *fakeDeletingStore) UpdateConsumer(ctx context.Context, stream string, cfg types.ConsumerConfig) (jetstream.Consumer, error) {
	consumer, err := f.fakeCronStore.UpdateConsumer(ctx, stream, cfg)
	if isCronConsumerMissing(err) {
		f.mu.Lock()
		f.consumer = nil
		f.mu.Unlock()
	}
	return consumer, err
}

func TestCronStartSequence(t *testing.T) {
	tests := []struct {
		metadata map[string]string
		want     uint64
		ok       bool
	}{
		{metadata: nil},
		{metadata: map[string]string{"other": "5"}},
		{metadata: map[string]string{cronStartSequenceKey: "abc"}},
		{metadata: map[string]string{cronStartSequenceKey: "0"}},
		{metadata: map[string]string{cronStartSequenceKey: "1"}, want: 1, ok: true},
		{metadata: map[string]string{cronStartSequenceKey: "18446744073709551615"}, want: 18446744073709551615, ok: true},
	}
	for _, tt := range tests {
		got, ok := cronStartSequence(tt.metadata)
		if got != tt.want || ok != tt.ok {
			t.Errorf("cronStartSequence(%v) = %d, %v; want %d, %v", tt.metadata, got, ok, tt.want, tt.ok)
		}
	}
}
