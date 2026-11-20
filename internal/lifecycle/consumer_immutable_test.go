package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/go-monolith/mono/pkg/types"
)

func TestIsConsumerInvalidConfig(t *testing.T) {
	rejected := fmt.Errorf("update: %w", &jetstream.APIError{Code: 500, ErrorCode: errCodeConsumerInvalidConfig, Description: "deliver policy can not be updated"})
	if !IsConsumerInvalidConfig(rejected) {
		t.Error("a wrapped 10012 must be recognised")
	}
	for _, err := range []error{nil, errors.New("deliver policy can not be updated"), &jetstream.APIError{ErrorCode: 10116}} {
		if IsConsumerInvalidConfig(err) {
			t.Errorf("%v must not be recognised", err)
		}
	}
}

func TestKeepImmutableConsumerSettings(t *testing.T) {
	first := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	second := first.Add(time.Hour)
	defaults := jetstream.ConsumerConfig{MaxWaiting: DefaultConsumerMaxWaiting}

	tests := []struct {
		name     string
		cfg      types.ConsumerConfig
		existing jetstream.ConsumerConfig
		want     []string // "Name: configured -> kept"
		check    func(t *testing.T, got types.ConsumerConfig)
	}{
		{name: "nothing differs, MaxWaiting left at zero", existing: defaults},
		{name: "deliver policy",
			cfg: types.ConsumerConfig{DeliverPolicy: types.DeliverNewPolicy}, existing: defaults,
			want: []string{"DeliverPolicy: new -> all"},
			check: func(t *testing.T, got types.ConsumerConfig) {
				if got.DeliverPolicy != types.DeliverAllPolicy {
					t.Errorf("DeliverPolicy = %v, want all", got.DeliverPolicy)
				}
			}},
		{name: "deliver policy to by start sequence keeps the existing start position",
			cfg:      types.ConsumerConfig{DeliverPolicy: types.DeliverByStartSequencePolicy, OptStartSeq: 7},
			existing: defaults,
			want:     []string{"DeliverPolicy: by start sequence -> all"},
			check: func(t *testing.T, got types.ConsumerConfig) {
				if got.DeliverPolicy != types.DeliverAllPolicy || got.OptStartSeq != 0 {
					t.Errorf("deliver %v, start sequence %d; want all and 0", got.DeliverPolicy, got.OptStartSeq)
				}
			}},
		{name: "start sequence",
			cfg: types.ConsumerConfig{DeliverPolicy: types.DeliverByStartSequencePolicy, OptStartSeq: 2},
			existing: jetstream.ConsumerConfig{DeliverPolicy: jetstream.DeliverByStartSequencePolicy, OptStartSeq: 1,
				MaxWaiting: DefaultConsumerMaxWaiting},
			want: []string{"OptStartSeq: 2 -> 1"},
			check: func(t *testing.T, got types.ConsumerConfig) {
				if got.OptStartSeq != 1 {
					t.Errorf("OptStartSeq = %d, want 1", got.OptStartSeq)
				}
			}},
		{name: "start time",
			cfg: types.ConsumerConfig{DeliverPolicy: types.DeliverByStartTimePolicy, OptStartTime: &second},
			existing: jetstream.ConsumerConfig{DeliverPolicy: jetstream.DeliverByStartTimePolicy, OptStartTime: &first,
				MaxWaiting: DefaultConsumerMaxWaiting},
			want: []string{"OptStartTime: 2026-01-01T01:00:00Z -> 2026-01-01T00:00:00Z"},
			check: func(t *testing.T, got types.ConsumerConfig) {
				if got.OptStartTime == nil || !got.OptStartTime.Equal(first) {
					t.Errorf("OptStartTime = %v, want %v", got.OptStartTime, first)
				}
			}},
		{name: "the same start time in another zone is not a difference",
			cfg: types.ConsumerConfig{DeliverPolicy: types.DeliverByStartTimePolicy,
				OptStartTime: func() *time.Time { local := first.In(time.FixedZone("x", 3600)); return &local }()},
			existing: jetstream.ConsumerConfig{DeliverPolicy: jetstream.DeliverByStartTimePolicy, OptStartTime: &first,
				MaxWaiting: DefaultConsumerMaxWaiting}},
		{name: "a start time missing in the configuration",
			cfg: types.ConsumerConfig{DeliverPolicy: types.DeliverByStartTimePolicy},
			existing: jetstream.ConsumerConfig{DeliverPolicy: jetstream.DeliverByStartTimePolicy, OptStartTime: &first,
				MaxWaiting: DefaultConsumerMaxWaiting},
			want: []string{"OptStartTime: none -> 2026-01-01T00:00:00Z"}},
		{name: "ack policy",
			cfg: types.ConsumerConfig{AckPolicy: types.AckNonePolicy}, existing: defaults,
			want: []string{"AckPolicy: none -> explicit"},
			check: func(t *testing.T, got types.ConsumerConfig) {
				if got.AckPolicy != types.AckExplicitPolicy {
					t.Errorf("AckPolicy = %v, want explicit", got.AckPolicy)
				}
			}},
		{name: "replay policy",
			cfg: types.ConsumerConfig{ReplayPolicy: types.ReplayOriginalPolicy}, existing: defaults,
			want: []string{"ReplayPolicy: original -> instant"}},
		{name: "max waiting set",
			cfg: types.ConsumerConfig{MaxWaiting: 100}, existing: defaults,
			want: []string{"MaxWaiting: 100 -> 512"},
			check: func(t *testing.T, got types.ConsumerConfig) {
				if got.MaxWaiting != DefaultConsumerMaxWaiting {
					t.Errorf("MaxWaiting = %d, want %d", got.MaxWaiting, DefaultConsumerMaxWaiting)
				}
			}},
		{name: "max waiting back to the default",
			existing: jetstream.ConsumerConfig{MaxWaiting: 100},
			want:     []string{"MaxWaiting: 512 -> 100"},
			check: func(t *testing.T, got types.ConsumerConfig) {
				if got.MaxWaiting != 100 {
					t.Errorf("MaxWaiting = %d, want 100", got.MaxWaiting)
				}
			}},
		{name: "a negative max waiting asks for the default too",
			cfg: types.ConsumerConfig{MaxWaiting: -1}, existing: defaults},
		{name: "memory storage",
			cfg: types.ConsumerConfig{MemoryStorage: true}, existing: defaults,
			want: []string{"MemoryStorage: true -> false"}},
		{name: "several at once, the rest of the configuration untouched",
			cfg: types.ConsumerConfig{Name: "c", AckPolicy: types.AckAllPolicy, ReplayPolicy: types.ReplayOriginalPolicy, MaxDeliver: 9,
				FilterSubject: "orders.eu"},
			existing: defaults,
			want:     []string{"AckPolicy: all -> explicit", "ReplayPolicy: original -> instant"},
			check: func(t *testing.T, got types.ConsumerConfig) {
				if got.Name != "c" || got.MaxDeliver != 9 || got.FilterSubject != "orders.eu" {
					t.Errorf("other settings changed: %+v", got)
				}
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, kept, err := KeepImmutableConsumerSettings(tt.cfg, tt.existing)
			if err != nil {
				t.Fatalf("error = %v", err)
			}
			var names []string
			for _, k := range kept {
				names = append(names, fmt.Sprintf("%s: %s -> %s", k.Name, k.Configured, k.Kept))
			}
			if strings.Join(names, "; ") != strings.Join(tt.want, "; ") {
				t.Errorf("kept = %v, want %v", names, tt.want)
			}
			if tt.check != nil {
				tt.check(t, got)
			}
		})
	}

	t.Run("notes for the settings that change what the handler must do", func(t *testing.T) {
		_, kept, err := KeepImmutableConsumerSettings(
			types.ConsumerConfig{AckPolicy: types.AckNonePolicy, DeliverPolicy: types.DeliverByStartTimePolicy, OptStartTime: &second},
			jetstream.ConsumerConfig{DeliverPolicy: jetstream.DeliverByStartTimePolicy, OptStartTime: &first, MaxWaiting: DefaultConsumerMaxWaiting})
		if err != nil || len(kept) != 2 {
			t.Fatalf("kept = %+v, error %v", kept, err)
		}
		if !strings.Contains(kept[0].Note, "use a fixed time") || !strings.Contains(kept[1].Note, "keep acknowledging") {
			t.Errorf("notes = %q, %q", kept[0].Note, kept[1].Note)
		}
	})

	t.Run("a policy the framework does not know", func(t *testing.T) {
		for name, existing := range map[string]jetstream.ConsumerConfig{
			"deliver": {DeliverPolicy: jetstream.DeliverPolicy(99)},
			"ack":     {AckPolicy: jetstream.AckPolicy(99)},
			"replay":  {ReplayPolicy: jetstream.ReplayPolicy(99)},
		} {
			cfg := types.ConsumerConfig{MaxDeliver: 9}
			got, kept, err := KeepImmutableConsumerSettings(cfg, existing)
			if err == nil || kept != nil || got.MaxDeliver != 9 {
				t.Errorf("%s: got %+v, kept %v, error %v; want an error and the configuration unchanged", name, got, kept, err)
			}
		}
	})

	t.Run("names of unknown configured policies", func(t *testing.T) {
		for _, name := range []string{
			deliverPolicyName(types.DeliverPolicy(99)), ackPolicyName(types.AckPolicy(99)), replayPolicyName(types.ReplayPolicy(99)),
		} {
			if name != "unknown (99)" {
				t.Errorf("name = %q", name)
			}
		}
		for policy, want := range map[types.DeliverPolicy]string{
			types.DeliverLastPolicy: "last", types.DeliverLastPerSubjectPolicy: "last per subject",
		} {
			if got := deliverPolicyName(policy); got != want {
				t.Errorf("deliverPolicyName = %q, want %q", got, want)
			}
		}
	})
}

// keepTestStream is a jetstream.Stream whose Consumer returns a consumer with
// the configuration, or lookupErr.
type keepTestStream struct {
	jetstream.Stream
	existing  *jetstream.ConsumerConfig
	noInfo    bool
	lookupErr error
}

func (s *keepTestStream) Consumer(context.Context, string) (jetstream.Consumer, error) {
	if s.lookupErr != nil {
		return nil, s.lookupErr
	}
	if s.noInfo {
		return &cachedInfoConsumer{mockJetStreamConsumer: &mockJetStreamConsumer{}}, nil
	}
	return &cachedInfoConsumer{mockJetStreamConsumer: &mockJetStreamConsumer{}, info: &jetstream.ConsumerInfo{Config: *s.existing}}, nil
}

// keepTestEventStream is an EventStream whose stream lookup returns stream
// and lookupErr.
type keepTestEventStream struct {
	mockEventStream
	stream    jetstream.Stream
	lookupErr error
	lookups   int
}

func (e *keepTestEventStream) Stream(context.Context, string) (jetstream.Stream, error) {
	e.lookups++
	return e.stream, e.lookupErr
}

func TestUpdateKeepingImmutable(t *testing.T) {
	rejected := fmt.Errorf("update: %w", &jetstream.APIError{Code: 500, ErrorCode: errCodeConsumerInvalidConfig, Description: "deliver policy can not be updated"})
	existing := jetstream.ConsumerConfig{Durable: "orders-ingest", MaxWaiting: DefaultConsumerMaxWaiting}
	cfg := types.ConsumerConfig{Name: "orders-ingest", Durable: "orders-ingest", DeliverPolicy: types.DeliverNewPolicy, MaxDeliver: 5}
	const owner = "stream consumer service orders.ingest"
	created := &mockJetStreamConsumer{}

	// run calls UpdateKeepingImmutable with an update that fails with the
	// errors queued in updateErrs and records the configurations it is given.
	type result struct {
		consumer jetstream.Consumer
		applied  types.ConsumerConfig
		err      error
		sent     []types.ConsumerConfig
		logger   *mockLogger
	}
	run := func(es *keepTestEventStream, updateErrs ...error) result {
		lm, logger := newMigrationTestManager(es)
		r := result{logger: logger}
		r.consumer, r.applied, r.err = lm.UpdateKeepingImmutable(context.Background(), es, "ORDERS", cfg, owner,
			func(c types.ConsumerConfig) (jetstream.Consumer, error) {
				r.sent = append(r.sent, c)
				if len(updateErrs) > 0 {
					next := updateErrs[0]
					updateErrs = updateErrs[1:]
					if next != nil {
						return nil, next
					}
				}
				return created, nil
			})
		return r
	}

	t.Run("accepted at once makes no lookup", func(t *testing.T) {
		es := &keepTestEventStream{stream: &keepTestStream{existing: &existing}}
		r := run(es)
		if r.err != nil || r.consumer != created || r.applied.DeliverPolicy != types.DeliverNewPolicy || len(r.sent) != 1 || es.lookups != 0 {
			t.Fatalf("consumer %v, applied %+v, error %v, sent %d, lookups %d", r.consumer, r.applied, r.err, len(r.sent), es.lookups)
		}
		if r.logger.countEntries("WARN:") != 0 {
			t.Errorf("want no warning: %v", r.logger.entries)
		}
	})

	t.Run("a rejected immutable change is repeated with the existing value and warned about", func(t *testing.T) {
		es := &keepTestEventStream{stream: &keepTestStream{existing: &existing}}
		r := run(es, rejected)
		if r.err != nil || r.consumer != created {
			t.Fatalf("consumer %v, error %v", r.consumer, r.err)
		}
		if len(r.sent) != 2 || r.sent[0].DeliverPolicy != types.DeliverNewPolicy || r.sent[1].DeliverPolicy != types.DeliverAllPolicy || r.sent[1].MaxDeliver != 5 {
			t.Errorf("sent = %+v, want the configured policy first and the kept one second", r.sent)
		}
		if r.applied.DeliverPolicy != types.DeliverAllPolicy {
			t.Errorf("applied deliver policy = %v, want the kept one", r.applied.DeliverPolicy)
		}
		if got := r.logger.countEntries("WARN: " + KeptConsumerSettingWarning); got != 1 {
			t.Fatalf("warnings = %d, want 1: %v", got, r.logger.entries)
		}
		for _, part := range []string{
			"registration " + owner, "stream ORDERS", "consumer orders-ingest", "setting DeliverPolicy",
			"configured new", "kept all", "apply_with nats consumer rm ORDERS orders-ingest",
		} {
			if !r.logger.hasWarnContaining(part) {
				t.Errorf("no warning contains %q: %v", part, r.logger.entries)
			}
		}
	})

	t.Run("the repeated update fails", func(t *testing.T) {
		other := errors.New("max deliver does not fit the kept policy")
		es := &keepTestEventStream{stream: &keepTestStream{existing: &existing}}
		r := run(es, rejected, other)
		if !errors.Is(r.err, other) || len(r.sent) != 2 || r.applied.DeliverPolicy != types.DeliverNewPolicy {
			t.Fatalf("error %v, sent %d, applied %+v; want the error of the repeated update", r.err, len(r.sent), r.applied)
		}
		for _, part := range []string{"keeps the existing DeliverPolicy of consumer orders-ingest", "nats consumer rm ORDERS orders-ingest"} {
			if !strings.Contains(r.err.Error(), part) {
				t.Errorf("error %q does not contain %q", r.err.Error(), part)
			}
		}
		if r.logger.countEntries("WARN:") != 0 {
			t.Errorf("want no warning for an update that was not applied: %v", r.logger.entries)
		}
	})

	t.Run("the original error is returned when nothing can be kept", func(t *testing.T) {
		same := jetstream.ConsumerConfig{Durable: "orders-ingest", DeliverPolicy: jetstream.DeliverNewPolicy, MaxWaiting: DefaultConsumerMaxWaiting}
		unknown := jetstream.ConsumerConfig{DeliverPolicy: jetstream.DeliverPolicy(99)}
		for name, es := range map[string]*keepTestEventStream{
			"the consumer agrees: rejected for another reason": {stream: &keepTestStream{existing: &same}},
			"stream lookup fails":                              {lookupErr: errors.New("stream gone")},
			"no stream handle":                                 {},
			"consumer lookup fails":                            {stream: &keepTestStream{lookupErr: errors.New("not a pull consumer")}},
			"consumer without info":                            {stream: &keepTestStream{noInfo: true}},
			"a policy the framework does not know":             {stream: &keepTestStream{existing: &unknown}},
		} {
			r := run(es, rejected)
			if !errors.Is(r.err, rejected) || len(r.sent) != 1 {
				t.Errorf("%s: error %v, sent %d; want the original error and no repeat", name, r.err, len(r.sent))
			}
			if r.logger.countEntries("WARN:") != 0 {
				t.Errorf("%s: want no warning", name)
			}
		}
	})

	t.Run("a consumer deleted since the rejected update gets the configured update again", func(t *testing.T) {
		es := &keepTestEventStream{stream: &keepTestStream{lookupErr: jetstream.ErrConsumerNotFound}}
		r := run(es, rejected)
		if r.err != nil || r.consumer != created || len(r.sent) != 2 || r.sent[1].DeliverPolicy != types.DeliverNewPolicy {
			t.Fatalf("consumer %v, error %v, sent %+v; want the configured update sent again", r.consumer, r.err, r.sent)
		}
		if r.logger.countEntries("WARN:") != 0 {
			t.Errorf("want no warning: nothing was kept")
		}

		// Its error is returned as it is, for the caller to act on.
		missing := jetstream.ErrConsumerDoesNotExist
		r = run(&keepTestEventStream{stream: &keepTestStream{lookupErr: jetstream.ErrConsumerNotFound}}, rejected, missing)
		if !errors.Is(r.err, missing) {
			t.Errorf("error = %v, want the error of the second configured update", r.err)
		}
	})

	t.Run("a read-back that failed for a reason that clears by itself is returned for a retry", func(t *testing.T) {
		for name, es := range map[string]*keepTestEventStream{
			"stream lookup":   {lookupErr: context.DeadlineExceeded},
			"consumer lookup": {stream: &keepTestStream{lookupErr: nats.ErrNoResponders}},
		} {
			r := run(es, rejected)
			if !isTransientJetStreamError(r.err) || len(r.sent) != 1 {
				t.Errorf("%s: error %v, sent %d; want the transient lookup error and no repeat", name, r.err, len(r.sent))
			}
			if !strings.Contains(r.err.Error(), "deliver policy can not be updated") {
				t.Errorf("%s: error %q does not mention the rejection", name, r.err.Error())
			}
		}
	})

	t.Run("another error makes no lookup", func(t *testing.T) {
		other := errors.New("no responders")
		es := &keepTestEventStream{stream: &keepTestStream{existing: &existing}}
		r := run(es, other)
		if !errors.Is(r.err, other) || len(r.sent) != 1 || es.lookups != 0 {
			t.Errorf("error %v, sent %d, lookups %d", r.err, len(r.sent), es.lookups)
		}
	})
}
