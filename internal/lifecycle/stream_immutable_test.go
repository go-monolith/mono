package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

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

func TestCheckEventStreamRegistrations(t *testing.T) {
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
			err := checkEventStreamRegistrations(tt.entries)
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
