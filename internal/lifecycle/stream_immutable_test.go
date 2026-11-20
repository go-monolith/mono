package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
)

// immutableTestStream is a jetstream.Stream whose CachedInfo reports info.
// Its other methods are not implemented.
type immutableTestStream struct {
	jetstream.Stream
	info *jetstream.StreamInfo
}

func (s *immutableTestStream) CachedInfo() *jetstream.StreamInfo { return s.info }

// immutableTestEventStream is a mockEventStream whose stream lookup returns
// stream and lookupErr, and counts the lookups.
type immutableTestEventStream struct {
	mockEventStream
	stream    jetstream.Stream
	lookupErr error
	lookups   int
}

func (e *immutableTestEventStream) Stream(context.Context, string) (jetstream.Stream, error) {
	e.lookups++
	return e.stream, e.lookupErr
}

// existingStream returns an event stream on which the looked-up stream has
// the storage type and retention policy.
func existingStream(storage jetstream.StorageType, retention jetstream.RetentionPolicy) *immutableTestEventStream {
	return &immutableTestEventStream{stream: &immutableTestStream{info: &jetstream.StreamInfo{
		Config: jetstream.StreamConfig{Name: "ORDERS", Storage: storage, Retention: retention},
	}}}
}

// invalidConfigErr is the server's rejection of a stream update, wrapped as
// the framework's JetStream wrapper returns it.
func invalidConfigErr(description string) error {
	return fmt.Errorf("failed to create/update stream ORDERS: %w",
		&jetstream.APIError{Code: 500, ErrorCode: errCodeStreamInvalidConfig, Description: description})
}

func TestExplainImmutableStreamChange(t *testing.T) {
	const owner = "stream consumer service orders.ingest"
	storageErr := invalidConfigErr("stream configuration update can not change storage type")

	tests := []struct {
		name      string
		es        *immutableTestEventStream
		cfg       types.StreamConfig
		createErr error
		want      []string // parts of the message; nil when createErr is returned unchanged
		lookups   int
	}{
		{
			name:      "file to memory",
			es:        existingStream(jetstream.FileStorage, jetstream.LimitsPolicy),
			cfg:       types.StreamConfig{Name: "ORDERS", Storage: types.MemoryStorage},
			createErr: storageErr,
			want:      []string{"stream ORDERS of " + owner, "exists with file storage and is configured with memory storage", "keep the existing value"},
			lookups:   1,
		},
		{
			name:      "memory to file",
			es:        existingStream(jetstream.MemoryStorage, jetstream.InterestPolicy),
			cfg:       types.StreamConfig{Name: "ORDERS", Storage: types.FileStorage, Retention: types.InterestPolicy},
			createErr: storageErr,
			want:      []string{"exists with memory storage and is configured with file storage"},
			lookups:   1,
		},
		{
			name:      "retention to work-queue",
			es:        existingStream(jetstream.FileStorage, jetstream.InterestPolicy),
			cfg:       types.StreamConfig{Name: "ORDERS", Retention: types.WorkQueuePolicy},
			createErr: invalidConfigErr("stream configuration update can not change retention policy to/from workqueue"),
			want:      []string{"exists with interest retention and is configured with work-queue retention"},
			lookups:   1,
		},
		{
			name:      "retention from work-queue",
			es:        existingStream(jetstream.FileStorage, jetstream.WorkQueuePolicy),
			cfg:       types.StreamConfig{Name: "ORDERS", Retention: types.LimitsPolicy},
			createErr: invalidConfigErr("stream configuration update can not change retention policy to/from workqueue"),
			want:      []string{"exists with work-queue retention and is configured with limits retention"},
			lookups:   1,
		},
		{
			name:      "storage and retention",
			es:        existingStream(jetstream.FileStorage, jetstream.WorkQueuePolicy),
			cfg:       types.StreamConfig{Name: "ORDERS", Storage: types.MemoryStorage},
			createErr: storageErr,
			want: []string{
				"exists with file storage and is configured with memory storage, and " +
					"exists with work-queue retention and is configured with limits retention",
			},
			lookups: 1,
		},
		{
			name:      "invalid configuration for another reason",
			es:        existingStream(jetstream.FileStorage, jetstream.LimitsPolicy),
			cfg:       types.StreamConfig{Name: "ORDERS", Retention: types.InterestPolicy},
			createErr: invalidConfigErr("stream configuration update can not change MaxConsumers"),
			lookups:   1,
		},
		{
			name:      "another API error makes no lookup",
			es:        existingStream(jetstream.FileStorage, jetstream.LimitsPolicy),
			cfg:       types.StreamConfig{Name: "ORDERS", Storage: types.MemoryStorage},
			createErr: &jetstream.APIError{Code: 400, ErrorCode: 10065, Description: "subjects overlap with an existing stream"},
		},
		{
			name:      "an error that is not an API error makes no lookup",
			es:        existingStream(jetstream.FileStorage, jetstream.LimitsPolicy),
			cfg:       types.StreamConfig{Name: "ORDERS", Storage: types.MemoryStorage},
			createErr: context.DeadlineExceeded,
		},
		{
			name:      "failed lookup",
			es:        &immutableTestEventStream{lookupErr: errors.New("no responders")},
			cfg:       types.StreamConfig{Name: "ORDERS", Storage: types.MemoryStorage},
			createErr: storageErr,
			lookups:   1,
		},
		{
			name:      "lookup without a stream",
			es:        &immutableTestEventStream{},
			cfg:       types.StreamConfig{Name: "ORDERS", Storage: types.MemoryStorage},
			createErr: storageErr,
			lookups:   1,
		},
		{
			name:      "stream without info",
			es:        &immutableTestEventStream{stream: &immutableTestStream{}},
			cfg:       types.StreamConfig{Name: "ORDERS", Storage: types.MemoryStorage},
			createErr: storageErr,
			lookups:   1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := explainImmutableStreamChange(context.Background(), tt.es, tt.cfg, owner, tt.createErr)
			if tt.es.lookups != tt.lookups {
				t.Errorf("stream lookups = %d, want %d", tt.es.lookups, tt.lookups)
			}
			if tt.want == nil {
				if err != tt.createErr { //nolint:errorlint // the same error value must come back
					t.Fatalf("error = %v, want the create error unchanged", err)
				}
				if errors.Is(err, monoerrors.ErrStreamConfigImmutable) {
					t.Fatalf("error %v must not be ErrStreamConfigImmutable", err)
				}
				return
			}
			if !errors.Is(err, monoerrors.ErrStreamConfigImmutable) {
				t.Fatalf("error = %v, want ErrStreamConfigImmutable", err)
			}
			var apiErr *jetstream.APIError
			if !errors.As(err, &apiErr) || apiErr.ErrorCode != errCodeStreamInvalidConfig {
				t.Errorf("error %v does not wrap the server error", err)
			}
			for _, part := range tt.want {
				if !strings.Contains(err.Error(), part) {
					t.Errorf("error %q does not contain %q", err.Error(), part)
				}
			}
		})
	}
}

func TestCheckStreamRegistrations_Events(t *testing.T) {
	entry := func(module, stream string, storage types.StorageType, retention types.RetentionPolicy) types.EventStreamConsumerEntry {
		e := streamEntry(module, stream, paymentProcessedV1)
		e.Config.Stream.Storage = storage
		e.Config.Stream.Retention = retention
		return e
	}
	noModule := entry("", "PAYMENTS", types.MemoryStorage, types.LimitsPolicy)
	noModule.Module = nil

	tests := []struct {
		name    string
		entries []types.EventStreamConsumerEntry
		want    []string // nil when the registrations are accepted
	}{
		{name: "no registrations"},
		{
			name: "same settings",
			entries: []types.EventStreamConsumerEntry{
				entry("billing", "PAYMENTS", types.FileStorage, types.WorkQueuePolicy),
				entry("shipping", "PAYMENTS", types.FileStorage, types.WorkQueuePolicy),
			},
		},
		{
			name: "limits and interest can be changed into each other",
			entries: []types.EventStreamConsumerEntry{
				entry("billing", "PAYMENTS", types.FileStorage, types.LimitsPolicy),
				entry("shipping", "PAYMENTS", types.FileStorage, types.InterestPolicy),
			},
		},
		{
			name: "different streams",
			entries: []types.EventStreamConsumerEntry{
				entry("billing", "PAYMENTS", types.FileStorage, types.LimitsPolicy),
				entry("shipping", "SHIPMENTS", types.MemoryStorage, types.WorkQueuePolicy),
			},
		},
		{
			name: "storage",
			entries: []types.EventStreamConsumerEntry{
				entry("billing", "PAYMENTS", types.FileStorage, types.LimitsPolicy),
				entry("audit", "AUDIT", types.FileStorage, types.LimitsPolicy),
				entry("shipping", "PAYMENTS", types.MemoryStorage, types.LimitsPolicy),
			},
			want: []string{
				"stream PAYMENTS is registered by the event stream consumer of module billing for event billing.PaymentProcessed",
				"and by the event stream consumer of module shipping for event billing.PaymentProcessed",
				"the first with file storage and the second with memory storage",
				"every registration of a stream must agree on them",
			},
		},
		{
			name: "work-queue retention",
			entries: []types.EventStreamConsumerEntry{
				entry("billing", "PAYMENTS", types.FileStorage, types.WorkQueuePolicy),
				entry("shipping", "PAYMENTS", types.FileStorage, types.InterestPolicy),
			},
			want: []string{"the first with work-queue retention and the second with interest retention"},
		},
		{
			name: "registration without a module",
			entries: []types.EventStreamConsumerEntry{
				entry("billing", "PAYMENTS", types.FileStorage, types.LimitsPolicy),
				noModule,
			},
			want: []string{"module <unknown>"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckStreamRegistrations(StreamRegistrations(nil, tt.entries))
			if tt.want == nil {
				if err != nil {
					t.Fatalf("error = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, monoerrors.ErrStreamConfigImmutable) {
				t.Fatalf("error = %v, want ErrStreamConfigImmutable", err)
			}
			for _, part := range tt.want {
				if !strings.Contains(err.Error(), part) {
					t.Errorf("error %q does not contain %q", err.Error(), part)
				}
			}
		})
	}
}

// A stream consumer service and an event stream consumer whose stream update
// the server rejects for a storage change both fail with
// ErrStreamConfigImmutable, naming what registered the stream.
func TestSetupStreamConsumers_ImmutableStreamChange(t *testing.T) {
	newEventStream := func() *immutableTestEventStream {
		es := existingStream(jetstream.FileStorage, jetstream.LimitsPolicy)
		es.createStreamErr = invalidConfigErr("stream configuration update can not change storage type")
		return es
	}

	t.Run("stream consumer service", func(t *testing.T) {
		lm, _ := newMigrationTestManager(newEventStream())
		err := lm.setupStreamConsumer(context.Background(), &types.ServiceEntry{
			Name:       "ingest",
			ModuleName: "orders",
			StreamConsumerConfig: &types.StreamConsumerConfig{
				Stream: types.StreamConfig{Name: "ORDERS", Subjects: []string{"services.orders.ingest"}, Storage: types.MemoryStorage},
			},
			StreamConsumerHandler: func(context.Context, []*types.Msg) error { return nil },
		})
		if !errors.Is(err, monoerrors.ErrStreamConfigImmutable) {
			t.Fatalf("error = %v, want ErrStreamConfigImmutable", err)
		}
		if want := "stream ORDERS of stream consumer service orders.ingest exists with file storage"; !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err.Error(), want)
		}
	})

	t.Run("event stream consumer", func(t *testing.T) {
		lm, _ := newMigrationTestManager(newEventStream())
		entry := streamEntry("shipping", "ORDERS", paymentProcessedV1)
		entry.Config.Stream.Storage = types.MemoryStorage
		err := lm.setupEventStreamConsumer(context.Background(), entry, eventStreamDurable{name: "shipping-billing-PaymentProcessed-v1"})
		if !errors.Is(err, monoerrors.ErrStreamConfigImmutable) {
			t.Fatalf("error = %v, want ErrStreamConfigImmutable", err)
		}
		if want := "stream ORDERS of event stream consumer of module shipping for event billing.PaymentProcessed exists with file storage"; !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err.Error(), want)
		}
	})
}

func TestSealedStreamConfig(t *testing.T) {
	sealed := SealedStreamConfig(types.StreamConfig{Name: "S", Sealed: true, AllowRollup: true})
	if !sealed.Sealed || !sealed.DenyDelete || !sealed.DenyPurge || sealed.AllowRollup {
		t.Errorf("sealed = %+v, want DenyDelete and DenyPurge on and AllowRollup off", sealed)
	}
	open := SealedStreamConfig(types.StreamConfig{Name: "S", DenyDelete: true, AllowRollup: true})
	if open.Sealed || !open.DenyDelete || open.DenyPurge || !open.AllowRollup {
		t.Errorf("unsealed = %+v, want it unchanged", open)
	}
}

func TestNormalisedStreamConfig(t *testing.T) {
	tests := []struct {
		name          string
		cfg           types.StreamConfig
		wantTTL       bool
		wantDenyPurge bool
	}{
		{name: "plain"},
		{name: "configured values are kept", cfg: types.StreamConfig{AllowMsgTTL: true, DenyPurge: true}, wantTTL: true, wantDenyPurge: true},
		{name: "a subject delete marker TTL enables per-message TTLs and clears DenyPurge",
			cfg: types.StreamConfig{SubjectDeleteMarkerTTL: time.Minute, DenyPurge: true}, wantTTL: true},
		{name: "message schedules clear DenyPurge", cfg: types.StreamConfig{AllowMsgSchedules: true, DenyPurge: true}},
		{name: "with roll-ups set the server rejects instead of clearing DenyPurge",
			cfg: types.StreamConfig{AllowMsgSchedules: true, AllowRollup: true, DenyPurge: true}, wantDenyPurge: true},
		{name: "sealing enables DenyPurge", cfg: types.StreamConfig{Sealed: true}, wantDenyPurge: true},
		{name: "sealing wins over the marker TTL", cfg: types.StreamConfig{Sealed: true, SubjectDeleteMarkerTTL: time.Minute},
			wantTTL: true, wantDenyPurge: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NormalisedStreamConfig(tt.cfg)
			if got.AllowMsgTTL != tt.wantTTL || got.DenyPurge != tt.wantDenyPurge {
				t.Errorf("AllowMsgTTL=%v DenyPurge=%v, want %v and %v", got.AllowMsgTTL, got.DenyPurge, tt.wantTTL, tt.wantDenyPurge)
			}
		})
	}
}

func TestCheckSealedStream(t *testing.T) {
	const owner = "stream consumer service orders.ingest"
	for _, cfg := range []types.StreamConfig{
		{Name: "S"}, {Name: "S", Sealed: true}, {Name: "S", AllowMsgSchedules: true},
		{Name: "S", Sealed: true, AllowRollup: true, SubjectDeleteMarkerTTL: time.Minute},
	} {
		if err := CheckSealedStream(cfg, owner); err != nil {
			t.Errorf("%+v: error = %v, want nil", cfg, err)
		}
	}
	err := CheckSealedStream(types.StreamConfig{Name: "S", Sealed: true, AllowMsgSchedules: true}, owner)
	if !errors.Is(err, monoerrors.ErrInvalidConfiguration) {
		t.Fatalf("error = %v, want ErrInvalidConfiguration", err)
	}
	for _, part := range []string{"stream S of " + owner, "Sealed and AllowMsgSchedules", "the stream is not sealed"} {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("error %q does not contain %q", err.Error(), part)
		}
	}
}

func TestOneWayStreamDiff(t *testing.T) {
	tests := []struct {
		name     string
		existing jetstream.StreamConfig
		cfg      types.StreamConfig
		want     []string
	}{
		{name: "nothing on"},
		{name: "switched on is not a difference", cfg: types.StreamConfig{DenyDelete: true, DenyPurge: true, AllowMsgTTL: true, AllowMsgSchedules: true}},
		{name: "kept on", existing: jetstream.StreamConfig{DenyDelete: true, AllowMsgTTL: true},
			cfg: types.StreamConfig{DenyDelete: true, AllowMsgTTL: true}},
		{name: "each setting switched off",
			existing: jetstream.StreamConfig{DenyDelete: true, DenyPurge: true, AllowMsgTTL: true, AllowMsgSchedules: true},
			want: []string{
				"has DenyDelete enabled and is configured with it disabled",
				"has DenyPurge enabled and is configured with it disabled",
				"has AllowMsgTTL enabled and is configured with it disabled",
				"has AllowMsgSchedules enabled and is configured with it disabled",
			}},
		{name: "unsealing names Sealed only",
			existing: jetstream.StreamConfig{Sealed: true, DenyDelete: true, DenyPurge: true},
			want:     []string{"has Sealed enabled and is configured with it disabled"}},
		{name: "per-message TTLs the server enabled for a subject delete marker TTL are not a difference",
			existing: jetstream.StreamConfig{AllowMsgTTL: true},
			cfg:      types.StreamConfig{SubjectDeleteMarkerTTL: time.Minute}},
		{name: "DenyPurge kept on but switched off by message schedules names the cause",
			existing: jetstream.StreamConfig{DenyPurge: true},
			cfg:      types.StreamConfig{DenyPurge: true, AllowMsgSchedules: true},
			want:     []string{"has DenyPurge enabled and is configured with AllowMsgSchedules or SubjectDeleteMarkerTTL, which switches it off"}},
		{name: "a sealed stream kept sealed needs neither denial spelled out",
			existing: jetstream.StreamConfig{Sealed: true, DenyDelete: true, DenyPurge: true},
			cfg:      types.StreamConfig{Sealed: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := OneWayStreamDiff(tt.existing, tt.cfg); !slices.Equal(got, tt.want) {
				t.Errorf("diff = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsStreamInvalidConfig(t *testing.T) {
	if !IsStreamInvalidConfig(invalidConfigErr("message TTL status can not be disabled")) {
		t.Error("a wrapped 10052 must be recognised")
	}
	for _, err := range []error{nil, context.DeadlineExceeded, &jetstream.APIError{ErrorCode: 10065}} {
		if IsStreamInvalidConfig(err) {
			t.Errorf("%v must not be recognised", err)
		}
	}
}

func TestExplainImmutableStreamChange_OneWaySettings(t *testing.T) {
	const owner = "stream consumer service orders.ingest"
	existing := func(cfg jetstream.StreamConfig) *immutableTestEventStream {
		cfg.Name = "ORDERS"
		return &immutableTestEventStream{stream: &immutableTestStream{info: &jetstream.StreamInfo{Config: cfg}}}
	}

	t.Run("a setting switched off", func(t *testing.T) {
		createErr := invalidConfigErr("stream configuration update can not cancel deny purge")
		err := explainImmutableStreamChange(context.Background(), existing(jetstream.StreamConfig{DenyPurge: true}),
			types.StreamConfig{Name: "ORDERS"}, owner, createErr)
		if !errors.Is(err, monoerrors.ErrStreamConfigImmutable) || !errors.Is(err, createErr) {
			t.Fatalf("error = %v, want ErrStreamConfigImmutable wrapping the server error", err)
		}
		for _, part := range []string{"stream ORDERS of " + owner + " has DenyPurge enabled and is configured with it disabled", "keep the setting enabled"} {
			if !strings.Contains(err.Error(), part) {
				t.Errorf("error %q does not contain %q", err.Error(), part)
			}
		}
		if strings.Contains(err.Error(), "keep the existing value") {
			t.Errorf("error %q carries the storage and retention help, which does not apply", err.Error())
		}
	})

	t.Run("storage changed and a setting switched off", func(t *testing.T) {
		err := explainImmutableStreamChange(context.Background(), existing(jetstream.StreamConfig{AllowMsgTTL: true}),
			types.StreamConfig{Name: "ORDERS", Storage: types.MemoryStorage}, owner,
			invalidConfigErr("stream configuration update can not change storage type"))
		for _, part := range []string{
			"exists with file storage and is configured with memory storage, and has AllowMsgTTL enabled and is configured with it disabled",
			"keep the existing value", "keep the setting enabled",
		} {
			if !strings.Contains(err.Error(), part) {
				t.Errorf("error %q does not contain %q", err.Error(), part)
			}
		}
	})

	t.Run("sealing a stream that does not exist yet", func(t *testing.T) {
		createErr := invalidConfigErr("stream configuration for create can not be sealed")
		notFound := &immutableTestEventStream{lookupErr: fmt.Errorf("failed to get stream ORDERS: %w", jetstream.ErrStreamNotFound)}
		err := explainImmutableStreamChange(context.Background(), notFound,
			types.StreamConfig{Name: "ORDERS", Sealed: true}, owner, createErr)
		if !errors.Is(err, monoerrors.ErrInvalidConfiguration) || !errors.Is(err, createErr) {
			t.Fatalf("error = %v, want ErrInvalidConfiguration wrapping the server error", err)
		}
		if errors.Is(err, monoerrors.ErrStreamConfigImmutable) {
			t.Errorf("error %v must not be ErrStreamConfigImmutable: nothing existing was changed", err)
		}
		for _, part := range []string{"stream ORDERS of " + owner, "does not exist yet", "start once without Sealed"} {
			if !strings.Contains(err.Error(), part) {
				t.Errorf("error %q does not contain %q", err.Error(), part)
			}
		}

		// Without Sealed a missing stream explains nothing.
		got := explainImmutableStreamChange(context.Background(), notFound, types.StreamConfig{Name: "ORDERS"}, owner, createErr)
		if got != createErr { //nolint:errorlint // the same error value must come back
			t.Errorf("error = %v, want the create error unchanged", got)
		}
	})
}

func TestCheckStreamRegistrations_SealedWithSchedules(t *testing.T) {
	entry := planService("orders", "ingest", "ORDERS", "services.orders.ingest")
	entry.StreamConsumerConfig.Stream.Sealed = true
	entry.StreamConsumerConfig.Stream.AllowMsgSchedules = true
	_, err := NewStreamPlan(StreamRegistrations([]*types.ServiceEntry{entry}, nil))
	if !errors.Is(err, monoerrors.ErrInvalidConfiguration) || !strings.Contains(err.Error(), "stream consumer service orders.ingest") {
		t.Fatalf("error = %v, want ErrInvalidConfiguration naming the registration", err)
	}
}

func TestCheckStreamRegistrations_OneWaySettings(t *testing.T) {
	service := func(name string, set func(*types.StreamConfig)) *types.ServiceEntry {
		entry := planService("orders", name, "ORDERS", "services.orders."+name)
		if set != nil {
			set(&entry.StreamConsumerConfig.Stream)
		}
		return entry
	}
	tests := []struct {
		name          string
		first, second func(*types.StreamConfig)
		want          string // the setting named; empty when accepted
	}{
		{name: "both off"},
		{name: "both on", first: func(s *types.StreamConfig) { s.DenyDelete = true }, second: func(s *types.StreamConfig) { s.DenyDelete = true }},
		{name: "only the first", first: func(s *types.StreamConfig) { s.AllowMsgTTL = true }, want: "AllowMsgTTL"},
		{name: "only the second", second: func(s *types.StreamConfig) { s.DenyPurge = true }, want: "DenyPurge"},
		{name: "schedules", first: func(s *types.StreamConfig) { s.AllowMsgSchedules = true }, want: "AllowMsgSchedules"},
		{name: "sealed against unsealed names Sealed", first: func(s *types.StreamConfig) { s.Sealed = true }, want: "Sealed"},
		{name: "a subject delete marker TTL with and without AllowMsgTTL spelled out",
			first:  func(s *types.StreamConfig) { s.SubjectDeleteMarkerTTL = time.Minute },
			second: func(s *types.StreamConfig) { s.SubjectDeleteMarkerTTL = time.Minute; s.AllowMsgTTL = true }},
		{name: "message schedules with and without DenyPurge",
			first:  func(s *types.StreamConfig) { s.AllowMsgSchedules = true; s.DenyPurge = true },
			second: func(s *types.StreamConfig) { s.AllowMsgSchedules = true }},
		{name: "both set DenyPurge but a marker TTL switches it off for one",
			first:  func(s *types.StreamConfig) { s.DenyPurge = true },
			second: func(s *types.StreamConfig) { s.DenyPurge = true; s.SubjectDeleteMarkerTTL = time.Minute },
			want:   "keeps DenyPurge enabled"},
		{name: "sealed agrees with sealed and both denials",
			first:  func(s *types.StreamConfig) { s.Sealed = true },
			second: func(s *types.StreamConfig) { s.Sealed = true; s.DenyDelete = true; s.DenyPurge = true }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckStreamRegistrations(StreamRegistrations(
				[]*types.ServiceEntry{service("first", tt.first), service("second", tt.second)}, nil))
			if tt.want == "" {
				if err != nil {
					t.Fatalf("error = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, monoerrors.ErrStreamConfigImmutable) {
				t.Fatalf("error = %v, want ErrStreamConfigImmutable", err)
			}
			for _, part := range []string{
				"stream ORDERS is registered by the stream consumer service orders.first and by the stream consumer service orders.second",
				"only one ", tt.want,
			} {
				if !strings.Contains(err.Error(), part) {
					t.Errorf("error %q does not contain %q", err.Error(), part)
				}
			}
		})
	}
}

// cronStreamWriter is an EventStream whose stream writes fail with the errors
// queued in writeErrs, in order, and are recorded.
type cronStreamWriter struct {
	mockEventStream
	writeErrs []error
	writes    []types.StreamConfig
	stream    jetstream.Stream
	lookupErr error
	lookups   int
}

func (w *cronStreamWriter) CreateOrUpdateStream(_ context.Context, cfg types.StreamConfig) (jetstream.Stream, error) {
	w.writes = append(w.writes, cfg)
	if len(w.writeErrs) == 0 {
		return nil, nil
	}
	err := w.writeErrs[0]
	w.writeErrs = w.writeErrs[1:]
	return nil, err
}

func (w *cronStreamWriter) Stream(context.Context, string) (jetstream.Stream, error) {
	w.lookups++
	return w.stream, w.lookupErr
}

func TestWriteCronStream(t *testing.T) {
	ttlErr := invalidConfigErr("message TTL status can not be disabled")
	withTTL := func(on bool) jetstream.Stream {
		return &immutableTestStream{info: &jetstream.StreamInfo{Config: jetstream.StreamConfig{AllowMsgTTL: on}}}
	}
	cfg := types.StreamConfig{Name: "MONO_CRON_reports_rollup", AllowMsgSchedules: true}
	other := errors.New("another rejection")

	tests := []struct {
		name       string
		es         *cronStreamWriter
		cfg        types.StreamConfig
		wantErr    error
		wantWrites []bool // AllowMsgTTL of every write
		wantLookup int
	}{
		{name: "accepted at once", es: &cronStreamWriter{}, cfg: cfg, wantWrites: []bool{false}},
		{name: "the stream has per-message TTLs: repeated with them kept",
			es: &cronStreamWriter{writeErrs: []error{ttlErr}, stream: withTTL(true)}, cfg: cfg,
			wantWrites: []bool{false, true}, wantLookup: 1},
		{name: "the repeated write fails",
			es: &cronStreamWriter{writeErrs: []error{ttlErr, other}, stream: withTTL(true)}, cfg: cfg,
			wantErr: other, wantWrites: []bool{false, true}, wantLookup: 1},
		{name: "the stream has no per-message TTLs: rejected for another reason",
			es: &cronStreamWriter{writeErrs: []error{ttlErr}, stream: withTTL(false)}, cfg: cfg,
			wantErr: ttlErr, wantWrites: []bool{false}, wantLookup: 1},
		{name: "another error makes no lookup",
			es: &cronStreamWriter{writeErrs: []error{other}, stream: withTTL(true)}, cfg: cfg,
			wantErr: other, wantWrites: []bool{false}},
		{name: "a configuration with a TTL is not repeated",
			es:      &cronStreamWriter{writeErrs: []error{ttlErr}, stream: withTTL(true)},
			cfg:     types.StreamConfig{Name: cfg.Name, AllowMsgSchedules: true, AllowMsgTTL: true},
			wantErr: ttlErr, wantWrites: []bool{true}},
		{name: "failed lookup", es: &cronStreamWriter{writeErrs: []error{ttlErr}, lookupErr: errors.New("no responders")}, cfg: cfg,
			wantErr: ttlErr, wantWrites: []bool{false}, wantLookup: 1},
		{name: "lookup without a stream", es: &cronStreamWriter{writeErrs: []error{ttlErr}}, cfg: cfg,
			wantErr: ttlErr, wantWrites: []bool{false}, wantLookup: 1},
		{name: "stream without info", es: &cronStreamWriter{writeErrs: []error{ttlErr}, stream: &immutableTestStream{}}, cfg: cfg,
			wantErr: ttlErr, wantWrites: []bool{false}, wantLookup: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lm, _ := newMigrationTestManager(tt.es)
			err := lm.WriteCronStream(context.Background(), tt.es, tt.cfg)
			if !errors.Is(err, tt.wantErr) || (tt.wantErr == nil && err != nil) {
				t.Fatalf("error = %v, want %v", err, tt.wantErr)
			}
			var writes []bool
			for _, w := range tt.es.writes {
				writes = append(writes, w.AllowMsgTTL)
			}
			if !slices.Equal(writes, tt.wantWrites) {
				t.Errorf("AllowMsgTTL of the writes = %v, want %v", writes, tt.wantWrites)
			}
			if tt.es.lookups != tt.wantLookup {
				t.Errorf("lookups = %d, want %d", tt.es.lookups, tt.wantLookup)
			}
		})
	}
}
