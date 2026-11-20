package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
)

// writeTestEventStream is an EventStream whose stream writes fail with
// writeErrs, one per write, and succeed after that. It knows which streams
// hold a subject, and records every request it is sent.
type writeTestEventStream struct {
	mockEventStream
	writeErrs []error
	writes    []types.StreamConfig

	streams       map[string]jetstream.Stream
	streamErr     error
	streamLookups []string

	holders       map[string][]string
	holderErr     error
	holderLookups []string
}

func (e *writeTestEventStream) CreateOrUpdateStream(_ context.Context, cfg types.StreamConfig) (jetstream.Stream, error) {
	e.writes = append(e.writes, cfg)
	if len(e.writeErrs) >= len(e.writes) && e.writeErrs[len(e.writes)-1] != nil {
		return nil, e.writeErrs[len(e.writes)-1]
	}
	return e.streams[cfg.Name], nil
}

func (e *writeTestEventStream) Stream(_ context.Context, name string) (jetstream.Stream, error) {
	e.streamLookups = append(e.streamLookups, name)
	if e.streamErr != nil {
		return nil, e.streamErr
	}
	stream, ok := e.streams[name]
	if !ok {
		return nil, jetstream.ErrStreamNotFound
	}
	return stream, nil
}

func (e *writeTestEventStream) StreamNamesBySubject(_ context.Context, subject string) ([]string, error) {
	e.holderLookups = append(e.holderLookups, subject)
	if e.holderErr != nil {
		return nil, e.holderErr
	}
	return e.holders[subject], nil
}

// requests returns how many requests besides stream writes were sent.
func (e *writeTestEventStream) requests() int {
	return len(e.streamLookups) + len(e.holderLookups)
}

// existingStreamInfo is a stream as the server holds it.
func existingStreamInfo(name string, metadata map[string]string, subjects ...string) *conflictTestStream {
	return &conflictTestStream{info: &jetstream.StreamInfo{
		Config: jetstream.StreamConfig{Name: name, Subjects: subjects, Metadata: metadata},
	}}
}

var (
	overlapErr = rejection(errCodeStreamSubjectsOverlap, "subjects overlap with an existing stream")
	limitsErr  = rejection(errCodeStreamUpdate, "change to limits violates consumers: orders-ingest")
)

func TestConsumerLimitViolators(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want []string
	}{
		{"one consumer", limitsErr, []string{"orders-ingest"}},
		{"several consumers", rejection(errCodeStreamUpdate, "change to limits violates consumers: b, a,c"), []string{"b", "a", "c"}},
		{"the generic code for another failure", rejection(errCodeStreamUpdate, "insufficient storage resources available"), nil},
		{"no consumer named", rejection(errCodeStreamUpdate, "change to limits violates consumers: "), nil},
		{"the text under another code", rejection(errCodeStreamInvalidConfig, "change to limits violates consumers: a"), nil},
		{"not an API error", errors.New("change to limits violates consumers: a"), nil},
		{"nil", nil, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ConsumerLimitViolators(tt.err); !slices.Equal(got, tt.want) {
				t.Errorf("ConsumerLimitViolators = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStreamErrorCodes(t *testing.T) {
	if !IsStreamSubjectsOverlap(overlapErr) || IsStreamSubjectsOverlap(limitsErr) || IsStreamSubjectsOverlap(errors.New("x")) {
		t.Error("IsStreamSubjectsOverlap must match 10065 only")
	}
	if !IsStreamUpdateRejected(limitsErr) || IsStreamUpdateRejected(overlapErr) || IsStreamUpdateRejected(nil) {
		t.Error("IsStreamUpdateRejected must match 10069 only")
	}
	for _, tt := range []struct {
		limits types.StreamConsumerLimits
		want   bool
	}{
		{types.StreamConsumerLimits{}, false},
		{types.StreamConsumerLimits{MaxAckPending: 1}, true},
		{types.StreamConsumerLimits{InactiveThreshold: time.Second}, true},
	} {
		if got := HasConsumerLimits(types.StreamConfig{ConsumerLimits: tt.limits}); got != tt.want {
			t.Errorf("HasConsumerLimits(%+v) = %v, want %v", tt.limits, got, tt.want)
		}
	}
}

// The plan rejects two streams that would listen on overlapping subjects.
func TestNewStreamPlan_StreamsDisjoint(t *testing.T) {
	tests := []struct {
		name     string
		services []*types.ServiceEntry
		events   []types.EventStreamConsumerEntry
		mentions []string // nil: the plan is accepted
	}{
		{name: "one event on two streams",
			events: []types.EventStreamConsumerEntry{
				streamEntry("billing", "BILLING_EVENTS", paymentProcessedV1), streamEntry("shipping", "SHIPPING_EVENTS", paymentProcessedV1)},
			mentions: []string{"stream BILLING_EVENTS", "stream SHIPPING_EVENTS", "module billing", "module shipping",
				paymentProcessedV1.Subject, "same stream name"}},
		{name: "a wildcard covering another stream's subject",
			services: []*types.ServiceEntry{planService("orders", "all", "ALL", "jobs.>"), planService("orders", "urgent", "URGENT", "jobs.urgent")},
			mentions: []string{`"jobs.>"`, `"jobs.urgent"`, "stream consumer service orders.all", "stream consumer service orders.urgent"}},
		{name: "a service and an event",
			services: []*types.ServiceEntry{planService("orders", "tap", "TAP", "events.billing.>")},
			events:   []types.EventStreamConsumerEntry{streamEntry("billing", "EVENTS", paymentProcessedV1)},
			mentions: []string{"stream EVENTS", "stream TAP"}},
		{name: "streams on different subjects",
			services: []*types.ServiceEntry{planService("orders", "a", "A", "jobs.a"), planService("orders", "b", "B", "jobs.b")},
			events: []types.EventStreamConsumerEntry{
				streamEntry("billing", "EVENTS", paymentProcessedV1), streamEntry("billing", "OTHER", subscriptionChangedV1)}},
		{name: "one stream shared by both",
			events: []types.EventStreamConsumerEntry{
				streamEntry("billing", "EVENTS", paymentProcessedV1), streamEntry("shipping", "EVENTS", paymentProcessedV1)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewStreamPlan(StreamRegistrations(tt.services, tt.events))
			if tt.mentions == nil {
				if err != nil {
					t.Fatalf("NewStreamPlan: %v", err)
				}
				return
			}
			if !errors.Is(err, monoerrors.ErrStreamSubjectsOverlap) {
				t.Fatalf("error = %v, want ErrStreamSubjectsOverlap", err)
			}
			for _, part := range tt.mentions {
				if !strings.Contains(err.Error(), part) {
					t.Errorf("error does not mention %q: %v", part, err)
				}
			}
		})
	}
}

func TestStreamPlan_ExplainSubjectsOverlap(t *testing.T) {
	ctx := context.Background()
	const owner = "stream consumer service orders.ingest"
	cfg := types.StreamConfig{Name: "ORDERS_B", Subjects: []string{"services.orders.ingest", "services.orders.ingest.>", "jobs.>"}}
	plan, err := NewStreamPlan(StreamRegistrations([]*types.ServiceEntry{
		planService("orders", "ingest", "ORDERS_B", cfg.Subjects...),
		planService("orders", "audit", "AUDIT", "audit.>"),
	}, nil))
	if err != nil {
		t.Fatalf("NewStreamPlan: %v", err)
	}
	marked := map[string]string{RegistrationMetadataKey: "stream consumer service orders.ingest"}

	t.Run("names the streams that hold the subjects", func(t *testing.T) {
		es := &writeTestEventStream{
			streams: map[string]jetstream.Stream{
				"ORDERS_A":             existingStreamInfo("ORDERS_A", marked, "services.orders.ingest", "services.orders.ingest.>"),
				"MONO_CRON_orders_job": existingStreamInfo("MONO_CRON_orders_job", nil, "jobs.x"),
				"AUDIT":                existingStreamInfo("AUDIT", nil, "audit.>", "jobs.audit"),
				// FOREIGN cannot be looked up.
			},
			holders: map[string][]string{
				"services.orders.ingest":   {"ORDERS_A", "ORDERS_B"},
				"services.orders.ingest.>": {"ORDERS_A"},
				"jobs.>":                   {"AUDIT", "FOREIGN", "MONO_CRON_orders_job", "ORDERS_B"},
			},
		}
		err := plan.ExplainSubjectsOverlap(ctx, es, cfg, owner, overlapErr)
		if !errors.Is(err, monoerrors.ErrStreamSubjectsOverlap) || !IsStreamSubjectsOverlap(err) {
			t.Fatalf("error = %v, want ErrStreamSubjectsOverlap wrapping the server error", err)
		}
		for _, part := range []string{
			"stream ORDERS_B of the " + owner,
			"stream ORDERS_A (listening on services.orders.ingest, services.orders.ingest.>; created by the framework for the stream consumer service orders.ingest; " +
				"no registration of this application uses this stream) holds services.orders.ingest, services.orders.ingest.>",
			"stream MONO_CRON_orders_job (listening on jobs.x; the stream of a cron service) holds jobs.>",
			"stream AUDIT (listening on audit.>, jobs.audit; the stream of this application's stream consumer service orders.audit) holds jobs.>",
			"stream FOREIGN (not marked as created by the framework for a registration) holds jobs.>",
			"Keep the stream name", "nats stream rm", "use another service name",
		} {
			if !strings.Contains(err.Error(), part) {
				t.Errorf("error does not mention %q:\n%v", part, err)
			}
		}
		if strings.Contains(err.Error(), "stream ORDERS_B (") {
			t.Errorf("the stream is never in its own way: %v", err)
		}
		if !slices.Equal(es.holderLookups, cfg.Subjects) {
			t.Errorf("subjects looked up = %v, want %v", es.holderLookups, cfg.Subjects)
		}
	})

	t.Run("describes a bounded number of streams", func(t *testing.T) {
		es := &writeTestEventStream{holders: map[string][]string{}}
		for i := range SubjectsOverlapLookupLimit + 3 {
			es.holders["jobs.>"] = append(es.holders["jobs.>"], fmt.Sprintf("S%02d", i))
		}
		err := plan.ExplainSubjectsOverlap(ctx, es, types.StreamConfig{Name: "ORDERS_B", Subjects: []string{"jobs.>"}}, owner, overlapErr)
		if !strings.Contains(err.Error(), "and 3 more") || len(es.streamLookups) != SubjectsOverlapLookupLimit {
			t.Errorf("stream lookups = %d, error: %v", len(es.streamLookups), err)
		}
	})

	unchanged := []struct {
		name    string
		es      types.EventStream
		err     error
		lookups int
	}{
		{"another error", &writeTestEventStream{holders: map[string][]string{"jobs.>": {"X"}}}, limitsErr, 0},
		{"not an API error", &writeTestEventStream{holders: map[string][]string{"jobs.>": {"X"}}}, nats.ErrTimeout, 0},
		{"the lookup fails", &writeTestEventStream{holderErr: errors.New("no responders")}, overlapErr, 1},
		{"only the stream itself", &writeTestEventStream{holders: map[string][]string{"jobs.>": {"ORDERS_B"}}}, overlapErr, 3},
		{"no stream holds them any more", &writeTestEventStream{}, overlapErr, 3},
	}
	for _, tt := range unchanged {
		t.Run("unchanged: "+tt.name, func(t *testing.T) {
			if got := plan.ExplainSubjectsOverlap(ctx, tt.es, cfg, owner, tt.err); got != tt.err {
				t.Errorf("error = %v, want %v unchanged", got, tt.err)
			}
			if es := tt.es.(*writeTestEventStream); len(es.holderLookups) != tt.lookups {
				t.Errorf("subject lookups = %d, want %d", len(es.holderLookups), tt.lookups)
			}
		})
	}

	t.Run("unchanged: an event stream that cannot tell", func(t *testing.T) {
		if got := plan.ExplainSubjectsOverlap(ctx, &mockEventStream{}, cfg, owner, overlapErr); got != overlapErr {
			t.Errorf("error = %v, want the server error unchanged", got)
		}
	})
}

func TestExplainConsumerLimits(t *testing.T) {
	cfg := types.StreamConfig{Name: "ORDERS", ConsumerLimits: types.StreamConsumerLimits{MaxAckPending: 10, InactiveThreshold: time.Minute}}
	err := ExplainConsumerLimits(cfg, "stream consumer service orders.ingest", limitsErr)
	if !IsStreamUpdateRejected(err) {
		t.Fatalf("the server error is not wrapped: %v", err)
	}
	for _, part := range []string{
		"stream ORDERS of the stream consumer service orders.ingest", "ConsumerLimits (MaxAckPending 10, InactiveThreshold 1m0s)",
		"nats consumer edit ORDERS", "never lowered automatically", "violates consumers: orders-ingest",
	} {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("error does not mention %q:\n%v", part, err)
		}
	}
	only := ExplainConsumerLimits(types.StreamConfig{Name: "ORDERS", ConsumerLimits: types.StreamConsumerLimits{InactiveThreshold: time.Minute}}, "x", limitsErr)
	if !strings.Contains(only.Error(), "ConsumerLimits (InactiveThreshold 1m0s)") {
		t.Errorf("limits are not listed as configured: %v", only)
	}
}

// writeTestManager returns a manager whose plan holds two services on stream
// ORDERS and one on ALONE.
func writeTestManager(t *testing.T, es types.EventStream) (*lifecycleManager, *mockLogger) {
	t.Helper()
	plan, err := NewStreamPlan(StreamRegistrations([]*types.ServiceEntry{
		planService("orders", "ingest", "ORDERS", "services.orders.ingest"),
		planService("orders", "audit", "ORDERS", "services.orders.audit"),
		planService("orders", "export", "ORDERS", "services.orders.export"),
		planService("orders", "bulk", "ORDERS", "services.orders.bulk"),
		planService("orders", "alone", "ALONE", "services.orders.alone"),
	}, nil))
	if err != nil {
		t.Fatalf("NewStreamPlan: %v", err)
	}
	lm, logger := newMigrationTestManager(es)
	lm.streamPlan = plan
	return lm, logger
}

// limitedConsumers are the consumers of stream ORDERS before its limit of 10
// is applied.
func limitedConsumers() map[string]*jetstream.ConsumerInfo {
	return map[string]*jetstream.ConsumerInfo{
		"orders-ingest": {Config: jetstream.ConsumerConfig{Durable: "orders-ingest", MaxAckPending: 1000, MaxDeliver: 7, FilterSubject: "services.orders.ingest"}},
		"orders-audit":  {Config: jetstream.ConsumerConfig{Durable: "orders-audit", MaxAckPending: 10}},
		"orders-export": {Config: jetstream.ConsumerConfig{Durable: "orders-export", MaxAckPending: -1}},
		// orders-bulk does not exist yet.
		"reporting": {Config: jetstream.ConsumerConfig{Durable: "reporting", MaxAckPending: 1000}},
	}
}

func TestWriteStream(t *testing.T) {
	ctx := context.Background()
	const owner = "stream consumer service orders.ingest"
	limited := types.StreamConfig{Name: "ORDERS", Subjects: []string{"services.orders.ingest"},
		ConsumerLimits: types.StreamConsumerLimits{MaxAckPending: 10}}

	t.Run("a write that succeeds sends nothing else", func(t *testing.T) {
		stream := &conflictTestStream{planTestStream: planTestStream{consumers: limitedConsumers()}}
		es := &writeTestEventStream{streams: map[string]jetstream.Stream{"ORDERS": stream}}
		lm, _ := writeTestManager(t, es)
		got, err := lm.WriteStream(ctx, es, limited, owner)
		if err != nil || got != stream {
			t.Fatalf("WriteStream = %v, %v; want the stream", got, err)
		}
		if len(es.writes) != 1 || es.requests() != 0 || len(stream.lookups) != 0 || stream.listings != 0 {
			t.Errorf("writes %d, other requests %d, consumer lookups %v, listings %d; want one write and nothing else",
				len(es.writes), es.requests(), stream.lookups, stream.listings)
		}
	})

	t.Run("lowers the consumers over a new limit and writes again", func(t *testing.T) {
		stream := &conflictTestStream{planTestStream: planTestStream{consumers: limitedConsumers()}}
		es := &writeTestEventStream{writeErrs: []error{limitsErr}, streams: map[string]jetstream.Stream{"ORDERS": stream}}
		lm, logger := writeTestManager(t, es)
		got, err := lm.WriteStream(ctx, es, limited, owner)
		if err != nil || got != stream {
			t.Fatalf("WriteStream = %v, %v; want the stream", got, err)
		}
		if len(es.writes) != 2 {
			t.Errorf("stream writes = %d, want the rejected one and one repeat", len(es.writes))
		}
		// Only the registration's consumer over the limit is changed, and
		// only its MaxAckPending.
		want := jetstream.ConsumerConfig{Durable: "orders-ingest", MaxAckPending: 10, MaxDeliver: 7, FilterSubject: "services.orders.ingest"}
		if len(stream.updates) != 1 || fmt.Sprint(stream.updates[0]) != fmt.Sprint(want) {
			t.Errorf("consumer updates = %+v, want only %+v", stream.updates, want)
		}
		if slices.Contains(stream.lookups, "reporting") {
			t.Error("a consumer no registration uses must not be looked at")
		}
		logged := strings.Join(logger.entries, "\n")
		for _, part := range []string{"INFO: Consumer MaxAckPending lowered", "orders-ingest", "from 1000", "to 10"} {
			if !strings.Contains(logged, part) {
				t.Errorf("log does not mention %q:\n%s", part, logged)
			}
		}
	})

	t.Run("a repeat that is rejected too is explained", func(t *testing.T) {
		stream := &conflictTestStream{planTestStream: planTestStream{consumers: limitedConsumers()}}
		foreign := rejection(errCodeStreamUpdate, "change to limits violates consumers: reporting")
		es := &writeTestEventStream{writeErrs: []error{limitsErr, foreign}, streams: map[string]jetstream.Stream{"ORDERS": stream}}
		lm, _ := writeTestManager(t, es)
		_, err := lm.WriteStream(ctx, es, limited, owner)
		if !errors.Is(err, foreign) || !strings.Contains(err.Error(), "ConsumerLimits (MaxAckPending 10)") {
			t.Fatalf("error = %v, want the explained error of the repeat", err)
		}
		if len(es.writes) != 2 {
			t.Errorf("stream writes = %d, want two", len(es.writes))
		}
	})

	t.Run("nothing to lower: no repeat", func(t *testing.T) {
		consumers := limitedConsumers()
		consumers["orders-ingest"].Config.MaxAckPending = 10
		stream := &conflictTestStream{planTestStream: planTestStream{consumers: consumers}}
		es := &writeTestEventStream{writeErrs: []error{limitsErr}, streams: map[string]jetstream.Stream{"ORDERS": stream}}
		lm, _ := writeTestManager(t, es)
		_, err := lm.WriteStream(ctx, es, limited, owner)
		if !errors.Is(err, limitsErr) || !strings.Contains(err.Error(), "ConsumerLimits (MaxAckPending 10)") {
			t.Fatalf("error = %v, want the explained rejection", err)
		}
		if len(es.writes) != 1 || len(stream.updates) != 0 {
			t.Errorf("writes %d, consumer updates %d; want one write and no update", len(es.writes), len(stream.updates))
		}
	})

	t.Run("an InactiveThreshold limit lowers nothing", func(t *testing.T) {
		consumers := limitedConsumers()
		consumers["orders-ingest"].Config.InactiveThreshold = time.Hour
		stream := &conflictTestStream{planTestStream: planTestStream{consumers: consumers}}
		es := &writeTestEventStream{writeErrs: []error{limitsErr}, streams: map[string]jetstream.Stream{"ORDERS": stream}}
		lm, _ := writeTestManager(t, es)
		cfg := types.StreamConfig{Name: "ORDERS", ConsumerLimits: types.StreamConsumerLimits{InactiveThreshold: time.Minute}}
		_, err := lm.WriteStream(ctx, es, cfg, owner)
		if !errors.Is(err, limitsErr) || !strings.Contains(err.Error(), "never lowered automatically") {
			t.Fatalf("error = %v, want the explained rejection", err)
		}
		if len(es.writes) != 1 || len(stream.updates) != 0 || es.requests() != 0 {
			t.Errorf("writes %d, consumer updates %d, other requests %d; want one write only", len(es.writes), len(stream.updates), es.requests())
		}
	})

	// A Start that fails anyway must leave every consumer as it was.
	t.Run("a consumer over the InactiveThreshold limit: nothing is lowered", func(t *testing.T) {
		consumers := limitedConsumers()
		consumers["orders-ingest"].Config.InactiveThreshold = time.Hour
		consumers["orders-audit"].Config.MaxAckPending = 1000
		stream := &conflictTestStream{planTestStream: planTestStream{consumers: consumers}}
		both := rejection(errCodeStreamUpdate, "change to limits violates consumers: orders-audit, orders-ingest")
		es := &writeTestEventStream{writeErrs: []error{both}, streams: map[string]jetstream.Stream{"ORDERS": stream}}
		lm, _ := writeTestManager(t, es)
		cfg := limited
		cfg.ConsumerLimits.InactiveThreshold = time.Minute
		_, err := lm.WriteStream(ctx, es, cfg, owner)
		if !errors.Is(err, both) || !strings.Contains(err.Error(), "ConsumerLimits (MaxAckPending 10, InactiveThreshold 1m0s)") {
			t.Fatalf("error = %v, want the explained rejection", err)
		}
		if len(es.writes) != 1 || len(stream.updates) != 0 {
			t.Errorf("writes %d, consumer updates %+v; want one write and no update", len(es.writes), stream.updates)
		}
	})

	t.Run("a consumer no registration uses is named: nothing is lowered or looked up", func(t *testing.T) {
		stream := &conflictTestStream{planTestStream: planTestStream{consumers: limitedConsumers()}}
		mixed := rejection(errCodeStreamUpdate, "change to limits violates consumers: reporting, orders-ingest")
		es := &writeTestEventStream{writeErrs: []error{mixed}, streams: map[string]jetstream.Stream{"ORDERS": stream}}
		lm, _ := writeTestManager(t, es)
		_, err := lm.WriteStream(ctx, es, limited, owner)
		if !errors.Is(err, mixed) || !strings.Contains(err.Error(), "ConsumerLimits (MaxAckPending 10)") {
			t.Fatalf("error = %v, want the explained rejection", err)
		}
		if len(es.writes) != 1 || len(stream.updates) != 0 || es.requests() != 0 || len(stream.lookups) != 0 {
			t.Errorf("writes %d, consumer updates %d, other requests %d, consumer lookups %v; want one write only",
				len(es.writes), len(stream.updates), es.requests(), stream.lookups)
		}
	})

	t.Run("a violator that is gone is skipped", func(t *testing.T) {
		stream := &conflictTestStream{planTestStream: planTestStream{consumers: limitedConsumers()}}
		gone := rejection(errCodeStreamUpdate, "change to limits violates consumers: orders-bulk, orders-ingest")
		es := &writeTestEventStream{writeErrs: []error{gone}, streams: map[string]jetstream.Stream{"ORDERS": stream}}
		lm, _ := writeTestManager(t, es)
		if _, err := lm.WriteStream(ctx, es, limited, owner); err != nil {
			t.Fatalf("WriteStream: %v", err)
		}
		if len(es.writes) != 2 || len(stream.updates) != 1 || stream.updates[0].Durable != "orders-ingest" {
			t.Errorf("writes %d, consumer updates %+v; want two writes and orders-ingest lowered", len(es.writes), stream.updates)
		}
	})

	t.Run("the generic code for another failure is not looked into", func(t *testing.T) {
		other := rejection(errCodeStreamUpdate, "insufficient storage resources available")
		es := &writeTestEventStream{writeErrs: []error{other}}
		lm, _ := writeTestManager(t, es)
		if _, err := lm.WriteStream(ctx, es, limited, owner); err != other || len(es.writes) != 1 || es.requests() != 0 {
			t.Errorf("error = %v, writes %d, other requests %d; want the server error unchanged and one write", err, len(es.writes), es.requests())
		}
	})

	t.Run("the generic code without limits is not looked into", func(t *testing.T) {
		es := &writeTestEventStream{writeErrs: []error{limitsErr}}
		lm, _ := writeTestManager(t, es)
		_, err := lm.WriteStream(ctx, es, types.StreamConfig{Name: "ORDERS"}, owner)
		if err != limitsErr || len(es.writes) != 1 || es.requests() != 0 {
			t.Errorf("error = %v, writes %d, other requests %d; want the server error and one write", err, len(es.writes), es.requests())
		}
	})

	t.Run("a stream the plan does not know lowers nothing", func(t *testing.T) {
		es := &writeTestEventStream{writeErrs: []error{limitsErr}}
		lm, _ := writeTestManager(t, es)
		cfg := limited
		cfg.Name = "UNPLANNED"
		if _, err := lm.WriteStream(ctx, es, cfg, owner); !errors.Is(err, limitsErr) || len(es.writes) != 1 || es.requests() != 0 {
			t.Errorf("error = %v, writes %d, other requests %d", err, len(es.writes), es.requests())
		}
	})

	t.Run("a failed consumer update is returned with both errors", func(t *testing.T) {
		stream := &conflictTestStream{planTestStream: planTestStream{consumers: limitedConsumers(), updateErr: nats.ErrTimeout}}
		es := &writeTestEventStream{writeErrs: []error{limitsErr}, streams: map[string]jetstream.Stream{"ORDERS": stream}}
		lm, _ := writeTestManager(t, es)
		_, err := lm.WriteStream(ctx, es, limited, owner)
		// The timeout is what the setup retry looks at.
		if !errors.Is(err, nats.ErrTimeout) || !isTransientJetStreamError(err) {
			t.Fatalf("error = %v, want the transient update error", err)
		}
		for _, part := range []string{"violates consumers: orders-ingest", "lower MaxAckPending of consumer orders-ingest", "from 1000 to 10"} {
			if !strings.Contains(err.Error(), part) {
				t.Errorf("error does not mention %q: %v", part, err)
			}
		}
		if len(es.writes) != 1 {
			t.Errorf("stream writes = %d, want one", len(es.writes))
		}
	})

	t.Run("a failed lookup is returned", func(t *testing.T) {
		for name, es := range map[string]*writeTestEventStream{
			"stream": {writeErrs: []error{limitsErr}, streamErr: nats.ErrNoResponders},
			"consumer": {writeErrs: []error{limitsErr}, streams: map[string]jetstream.Stream{
				"ORDERS": &conflictTestStream{planTestStream: planTestStream{lookupErr: nats.ErrNoResponders}}}},
		} {
			lm, _ := writeTestManager(t, es)
			if _, err := lm.WriteStream(ctx, es, limited, owner); !errors.Is(err, nats.ErrNoResponders) {
				t.Errorf("%s lookup: error = %v, want it to wrap the lookup error", name, err)
			}
		}
	})

	t.Run("subjects another stream holds", func(t *testing.T) {
		es := &writeTestEventStream{writeErrs: []error{overlapErr}, holders: map[string][]string{"services.orders.ingest": {"OLD"}}}
		lm, _ := writeTestManager(t, es)
		_, err := lm.WriteStream(ctx, es, limited, owner)
		if !errors.Is(err, monoerrors.ErrStreamSubjectsOverlap) || !strings.Contains(err.Error(), "stream OLD") {
			t.Fatalf("error = %v, want ErrStreamSubjectsOverlap naming the stream", err)
		}
		if len(es.writes) != 1 {
			t.Errorf("stream writes = %d, want one", len(es.writes))
		}
	})

	t.Run("a setting NATS cannot change", func(t *testing.T) {
		es := &writeTestEventStream{
			writeErrs: []error{invalidConfigErr("stream configuration update can not change storage type")},
			streams: map[string]jetstream.Stream{"ORDERS": &immutableTestStream{info: &jetstream.StreamInfo{
				Config: jetstream.StreamConfig{Name: "ORDERS", Storage: jetstream.MemoryStorage},
			}}},
		}
		lm, _ := writeTestManager(t, es)
		cfg := limited
		cfg.Storage = types.FileStorage
		if _, err := lm.WriteStream(ctx, es, cfg, owner); !errors.Is(err, monoerrors.ErrStreamConfigImmutable) {
			t.Fatalf("error = %v, want ErrStreamConfigImmutable", err)
		}
	})

	t.Run("any other error is returned as it is", func(t *testing.T) {
		es := &writeTestEventStream{writeErrs: []error{nats.ErrTimeout}}
		lm, _ := writeTestManager(t, es)
		if _, err := lm.WriteStream(ctx, es, limited, owner); err != nats.ErrTimeout || es.requests() != 0 {
			t.Errorf("error = %v, other requests %d; want the error unchanged and no request", err, es.requests())
		}
	})
}

func TestInheritsInactiveThreshold(t *testing.T) {
	tests := []struct {
		name     string
		limit    time.Duration
		consumer time.Duration
		want     bool
	}{
		{"limit on the stream, none on the consumer", time.Minute, 0, true},
		{"a negative threshold is none", time.Minute, -time.Second, true},
		{"the consumer sets its own", time.Minute, 30 * time.Second, false},
		{"no limit on the stream", 0, 0, false},
		{"no limit on the stream, the consumer sets its own", 0, time.Hour, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := InheritsInactiveThreshold(tt.limit, types.ConsumerConfig{InactiveThreshold: tt.consumer}); got != tt.want {
				t.Errorf("InheritsInactiveThreshold = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStreamPlan_InactiveThresholdLimit(t *testing.T) {
	limited := func(name string, limit time.Duration) *types.ServiceEntry {
		entry := planService("orders", name, "SHARED", "services.orders."+name)
		entry.StreamConsumerConfig.Stream.ConsumerLimits.InactiveThreshold = limit
		return entry
	}
	plan, err := NewStreamPlan(StreamRegistrations([]*types.ServiceEntry{
		limited("none", 0), limited("hour", time.Hour), limited("minute", time.Minute),
		planService("orders", "alone", "ALONE", "services.orders.alone"),
	}, nil))
	if err != nil {
		t.Fatalf("NewStreamPlan: %v", err)
	}
	var none *StreamPlan
	tests := []struct {
		name   string
		plan   *StreamPlan
		stream string
		own    time.Duration
		want   time.Duration
	}{
		{"the registration's own limit", plan, "SHARED", time.Hour, time.Hour},
		{"the shortest limit of another registration", plan, "SHARED", 0, time.Minute},
		{"a stream without any limit", plan, "ALONE", 0, 0},
		{"a stream the plan does not know", plan, "OTHER", 0, 0},
		{"a stream the plan does not know, own limit", plan, "OTHER", time.Second, time.Second},
		{"no plan", none, "SHARED", 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.plan.InactiveThresholdLimit(tt.stream, tt.own); got != tt.want {
				t.Errorf("InactiveThresholdLimit = %s, want %s", got, tt.want)
			}
		})
	}
}

// Both setup paths warn about a durable that inherits an inactivity
// threshold from its stream, and only about that.
func TestSetupStreamConsumers_InheritedInactiveThreshold(t *testing.T) {
	warnings := func(logger *mockLogger) []string {
		var found []string
		for _, entry := range logger.entries {
			if strings.HasPrefix(entry, "WARN: "+InheritedInactiveThresholdWarning) {
				found = append(found, entry)
			}
		}
		return found
	}
	tests := []struct {
		name     string
		limit    time.Duration
		consumer time.Duration
		warned   bool
	}{
		{"inherited", time.Minute, 0, true},
		{"the consumer sets its own", time.Minute, 30 * time.Second, false},
		{"no limit", 0, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := planService("orders", "ingest", "ORDERS", "services.orders.ingest")
			service.StreamConsumerConfig.Stream.ConsumerLimits.InactiveThreshold = tt.limit
			service.StreamConsumerConfig.Consumer.InactiveThreshold = tt.consumer
			service.StreamConsumerConfig.Fetch = types.FetchConfig{BatchSize: 1, Timeout: 10 * time.Millisecond}
			event := streamEntry("billing", "EVENTS", paymentProcessedV1)
			event.Config.Stream.ConsumerLimits.InactiveThreshold = tt.limit
			event.Config.Consumer.InactiveThreshold = tt.consumer

			plan, err := NewStreamPlan(StreamRegistrations([]*types.ServiceEntry{service}, []types.EventStreamConsumerEntry{event}))
			if err != nil {
				t.Fatalf("NewStreamPlan: %v", err)
			}
			es := &recordingEventStream{}
			lm, logger := newMigrationTestManager(es)
			lm.streamPlan = plan
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			lm.runtimeCtx = ctx
			if err := lm.setupStreamConsumer(ctx, service); err != nil {
				t.Fatalf("setupStreamConsumer: %v", err)
			}
			if err := lm.setupEventStreamConsumer(ctx, event, eventStreamDurable{name: "billing-billing-PaymentProcessed-v1"}); err != nil {
				t.Fatalf("setupEventStreamConsumer: %v", err)
			}
			for _, stop := range lm.streamConsumers {
				stop()
			}

			got := warnings(logger)
			if !tt.warned {
				if len(got) != 0 {
					t.Fatalf("warnings = %v, want none", got)
				}
				return
			}
			if len(got) != 2 {
				t.Fatalf("warnings = %v, want one per registration", got)
			}
			for i, parts := range [][]string{
				{"stream consumer service orders.ingest", "ORDERS", "orders-ingest", "1m0s"},
				{"event stream consumer of module billing for event billing.PaymentProcessed", "EVENTS", "billing-billing-PaymentProcessed-v1", "1m0s"},
			} {
				for _, part := range parts {
					if !strings.Contains(got[i], part) {
						t.Errorf("warning %d does not mention %q: %s", i, part, got[i])
					}
				}
			}
		})
	}
}

// On a shared stream a registration without the limit is warned about too:
// the stream carries the limit of the other registration.
func TestWarnInheritedInactiveThreshold_SharedStream(t *testing.T) {
	limited := planService("orders", "limited", "SHARED", "services.orders.limited")
	limited.StreamConsumerConfig.Stream.ConsumerLimits.InactiveThreshold = time.Minute
	plain := planService("orders", "plain", "SHARED", "services.orders.plain")
	plan, err := NewStreamPlan(StreamRegistrations([]*types.ServiceEntry{limited, plain}, nil))
	if err != nil {
		t.Fatalf("NewStreamPlan: %v", err)
	}
	lm, logger := newMigrationTestManager(&mockEventStream{})
	lm.streamPlan = plan

	lm.WarnInheritedInactiveThreshold("stream consumer service orders.plain", plain.StreamConsumerConfig.Stream,
		types.ConsumerConfig{Name: "orders-plain"})
	if len(logger.entries) != 1 || !strings.Contains(logger.entries[0], "orders-plain") || !strings.Contains(logger.entries[0], "1m0s") {
		t.Fatalf("log = %v, want one warning naming the consumer and the other registration's limit", logger.entries)
	}
	lm.WarnInheritedInactiveThreshold("stream consumer service orders.plain", plain.StreamConsumerConfig.Stream,
		types.ConsumerConfig{Name: "orders-plain", InactiveThreshold: time.Second})
	if len(logger.entries) != 1 {
		t.Errorf("a consumer with its own threshold was warned about: %v", logger.entries)
	}
}
