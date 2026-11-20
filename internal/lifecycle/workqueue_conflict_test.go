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

// conflictTestLister is a jetstream.ConsumerInfoLister over a fixed list.
type conflictTestLister struct {
	infos []*jetstream.ConsumerInfo
	err   error
}

func (l *conflictTestLister) Info() <-chan *jetstream.ConsumerInfo {
	ch := make(chan *jetstream.ConsumerInfo, len(l.infos))
	for _, info := range l.infos {
		ch <- info
	}
	close(ch)
	return ch
}

func (l *conflictTestLister) Err() error { return l.err }

// conflictTestStream is a jetstream.Stream with consumers that can be looked
// up, updated and listed, and with cached info. It counts the listings.
type conflictTestStream struct {
	planTestStream
	info     *jetstream.StreamInfo
	listed   []*jetstream.ConsumerInfo
	listErr  error
	listings int
}

func (s *conflictTestStream) CachedInfo() *jetstream.StreamInfo { return s.info }

func (s *conflictTestStream) ListConsumers(context.Context) jetstream.ConsumerInfoLister {
	s.listings++
	return &conflictTestLister{infos: s.listed, err: s.listErr}
}

// rejection is a server rejection with the code, wrapped as the framework's
// JetStream wrapper returns it.
func rejection(code jetstream.ErrorCode, description string) error {
	return fmt.Errorf("request failed: %w", &jetstream.APIError{Code: 400, ErrorCode: code, Description: description})
}

// workQueueService builds a stream consumer service on a work-queue stream.
func workQueueService(module, name, stream string, subjects ...string) *types.ServiceEntry {
	entry := planService(module, name, stream, subjects...)
	entry.StreamConsumerConfig.Stream.Retention = types.WorkQueuePolicy
	return entry
}

// workQueueEvent builds an event stream consumer on a work-queue stream.
func workQueueEvent(module, stream string, def types.BaseEventDefinition) types.EventStreamConsumerEntry {
	entry := streamEntry(module, stream, def)
	entry.Config.Stream.Retention = types.WorkQueuePolicy
	return entry
}

func TestFiltersCollide(t *testing.T) {
	tests := []struct {
		name string
		a, b []string
		want bool
	}{
		{"both unfiltered", nil, nil, true},
		{"one unfiltered", nil, []string{"a.b"}, true},
		{"the other unfiltered", []string{"a.b"}, nil, true},
		{"same subject", []string{"a.b"}, []string{"a.b"}, true},
		{"one covers the other", []string{"a.>"}, []string{"a.b"}, true},
		{"wildcards overlap", []string{"a.*.c"}, []string{"a.b.*"}, true},
		{"one of several overlaps", []string{"x.y", "a.b"}, []string{"q.r", "a.*"}, true},
		{"disjoint", []string{"a.b"}, []string{"a.c"}, false},
		{"disjoint lists", []string{"a.b", "a.c"}, []string{"b.>", "a.d"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FiltersCollide(tt.a, tt.b); got != tt.want {
				t.Errorf("FiltersCollide(%v, %v) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestIsWorkQueueConsumerConflict(t *testing.T) {
	for code, want := range map[jetstream.ErrorCode]bool{10099: true, 10100: true, 10012: false, 10065: false} {
		if got := IsWorkQueueConsumerConflict(rejection(code, "x")); got != want {
			t.Errorf("code %d: %v, want %v", code, got, want)
		}
	}
	if IsWorkQueueConsumerConflict(errors.New("plain")) || IsWorkQueueConsumerConflict(nil) {
		t.Error("an error that is not an API error is not a conflict")
	}
}

// The plan rejects registrations that would put two consumers on the same
// subjects of a work-queue stream, and only those.
func TestNewStreamPlan_WorkQueueConsumers(t *testing.T) {
	filtered := func(name, filter string) *types.ServiceEntry {
		entry := workQueueService("orders", name, "JOBS", "jobs.>")
		entry.StreamConsumerConfig.Consumer.FilterSubject = filter
		return entry
	}
	tests := []struct {
		name     string
		services []*types.ServiceEntry
		events   []types.EventStreamConsumerEntry
		mentions []string // nil: the plan is accepted
	}{
		{name: "two modules on one event",
			events: []types.EventStreamConsumerEntry{
				workQueueEvent("billing", "TASKS", paymentProcessedV1), workQueueEvent("shipping", "TASKS", paymentProcessedV1)},
			mentions: []string{"work-queue stream TASKS", "module billing", "module shipping",
				"billing-billing-PaymentProcessed-v1", "shipping-billing-PaymentProcessed-v1", "not filtered", "interest retention"}},
		{name: "one module registers the event twice",
			events: []types.EventStreamConsumerEntry{
				workQueueEvent("billing", "TASKS", paymentProcessedV1), workQueueEvent("billing", "TASKS", paymentProcessedV1)},
			mentions: []string{"billing-billing-PaymentProcessed-v1-2"}},
		{name: "services on the same subjects",
			services: []*types.ServiceEntry{workQueueService("orders", "a", "JOBS", "jobs.>"), workQueueService("orders", "b", "JOBS", "jobs.>")},
			mentions: []string{"stream consumer service orders.a", "stream consumer service orders.b"}},
		{name: "service subjects of which one covers the other",
			services: []*types.ServiceEntry{workQueueService("orders", "all", "JOBS", "jobs.>"), workQueueService("orders", "urgent", "JOBS", "jobs.urgent")},
			mentions: []string{"filtered on jobs.>", "filtered on jobs.urgent"}},
		{name: "a caller's filter beside an unfiltered consumer",
			services: []*types.ServiceEntry{filtered("eu", "jobs.eu.>"), workQueueService("orders", "rest", "JOBS", "jobs.>")},
			mentions: []string{"filtered on jobs.eu.>", "not filtered"}},
		{name: "callers' filters that overlap",
			services: []*types.ServiceEntry{filtered("eu", "jobs.eu.>"), filtered("urgent", "jobs.*.urgent")},
			mentions: []string{"filtered on jobs.eu.>", "filtered on jobs.*.urgent"}},
		{name: "callers' filters on different subjects",
			services: []*types.ServiceEntry{filtered("eu", "jobs.eu.>"), filtered("us", "jobs.us.>")}},
		{name: "services on different subjects",
			services: []*types.ServiceEntry{workQueueService("orders", "a", "JOBS", "jobs.a"), workQueueService("orders", "b", "JOBS", "jobs.b")}},
		{name: "two events on one stream",
			events: []types.EventStreamConsumerEntry{
				workQueueEvent("billing", "TASKS", paymentProcessedV1), workQueueEvent("billing", "TASKS", subscriptionChangedV1)}},
		{name: "one registration",
			services: []*types.ServiceEntry{workQueueService("orders", "a", "JOBS", "jobs.>")}},
		{name: "the same subjects on a stream that is not a work queue",
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
			if !errors.Is(err, monoerrors.ErrWorkQueueConsumerConflict) {
				t.Fatalf("error = %v, want ErrWorkQueueConsumerConflict", err)
			}
			for _, part := range tt.mentions {
				if !strings.Contains(err.Error(), part) {
					t.Errorf("error does not mention %q: %v", part, err)
				}
			}
		})
	}
}

func TestStreamPlan_EffectiveFilters(t *testing.T) {
	callerFiltered := planService("orders", "audit", "SHARED", "services.orders.audit")
	callerFiltered.StreamConsumerConfig.Consumer.FilterSubjects = []string{"x.a", "x.b"}
	regs := StreamRegistrations([]*types.ServiceEntry{
		planService("orders", "first", "SHARED", "services.orders.first", "services.orders.first.>"),
		callerFiltered,
		planService("orders", "alone", "ALONE", "services.orders.alone"),
	}, nil)
	plan, err := NewStreamPlan(regs)
	if err != nil {
		t.Fatalf("NewStreamPlan: %v", err)
	}
	want := [][]string{
		{"services.orders.first", "services.orders.first.>"}, // the registration's subjects on a shared stream
		{"x.a", "x.b"}, // the caller's own
		nil,            // not shared: not filtered
	}
	for i, reg := range regs {
		got := plan.EffectiveFilters(reg)
		if strings.Join(got, ",") != strings.Join(want[i], ",") {
			t.Errorf("EffectiveFilters(%s) = %v, want %v", reg.Owner, got, want[i])
		}
	}
}

func TestStreamPlan_ExplainWorkQueueConflict(t *testing.T) {
	ctx := context.Background()
	const owner = "stream consumer service orders.intake"
	plan, err := NewStreamPlan(StreamRegistrations([]*types.ServiceEntry{
		workQueueService("orders", "intake", "JOBS", "jobs.eu.>"),
		workQueueService("orders", "other", "JOBS", "jobs.us.>"),
	}, nil))
	if err != nil {
		t.Fatalf("NewStreamPlan: %v", err)
	}
	consumers := func() []*jetstream.ConsumerInfo {
		return []*jetstream.ConsumerInfo{
			nil,
			{Name: "orders-intake", Config: jetstream.ConsumerConfig{FilterSubject: "jobs.eu.>"}},
			{Name: "orders-ingest", Config: jetstream.ConsumerConfig{
				Metadata: map[string]string{RegistrationMetadataKey: "stream consumer service orders.ingest"}}},
			{Name: "orders-other", Config: jetstream.ConsumerConfig{FilterSubjects: []string{"jobs.eu.x", "jobs.us.>"}}},
			{Name: "reporting", Config: jetstream.ConsumerConfig{FilterSubject: "jobs.*.urgent"}},
			{Name: "elsewhere", Config: jetstream.ConsumerConfig{FilterSubject: "jobs.us.slow"}},
		}
	}
	conflict := rejection(errCodeWorkQueueNotUnique, "filtered consumer not unique on workqueue stream")

	t.Run("names the consumers in the way", func(t *testing.T) {
		stream := &conflictTestStream{listed: consumers()}
		err := plan.ExplainWorkQueueConflict(ctx, stream, "JOBS", "orders-intake", []string{"jobs.eu.>"}, owner, conflict)
		if !errors.Is(err, monoerrors.ErrWorkQueueConsumerConflict) || !IsWorkQueueConsumerConflict(err) {
			t.Fatalf("error = %v, want ErrWorkQueueConsumerConflict wrapping the server error", err)
		}
		for _, part := range []string{
			"consumer orders-intake (filtered on jobs.eu.>) of the " + owner, "work-queue stream JOBS",
			"orders-ingest (not filtered, created by the framework for the stream consumer service orders.ingest, which this application does not register)",
			"orders-other (filtered on jobs.eu.x, jobs.us.>, the consumer of this application's stream consumer service orders.other)",
			"reporting (filtered on jobs.*.urgent, not marked as created by the framework for a registration)",
			"nats consumer rm JOBS <consumer>", "not deleted automatically",
		} {
			if !strings.Contains(err.Error(), part) {
				t.Errorf("error does not mention %q:\n%v", part, err)
			}
		}
		if strings.Contains(err.Error(), "elsewhere") {
			t.Errorf("a consumer on other subjects is not in the way: %v", err)
		}
	})

	t.Run("an unfiltered consumer is in the way of every other", func(t *testing.T) {
		stream := &conflictTestStream{listed: consumers()}
		err := plan.ExplainWorkQueueConflict(ctx, stream, "JOBS", "orders-intake", nil, owner,
			rejection(errCodeWorkQueueMultipleUnfiltered, "multiple non-filtered consumers not allowed on workqueue stream"))
		for _, name := range []string{"orders-ingest", "orders-other", "reporting", "elsewhere"} {
			if !strings.Contains(err.Error(), name+" (") {
				t.Errorf("error does not name %s: %v", name, err)
			}
		}
	})

	t.Run("a nil plan still names them", func(t *testing.T) {
		var none *StreamPlan
		err := none.ExplainWorkQueueConflict(ctx, &conflictTestStream{listed: consumers()}, "JOBS", "orders-intake", nil, owner, conflict)
		if !errors.Is(err, monoerrors.ErrWorkQueueConsumerConflict) {
			t.Fatalf("error = %v, want ErrWorkQueueConsumerConflict", err)
		}
	})

	t.Run("stops at the lookup limit", func(t *testing.T) {
		var many []*jetstream.ConsumerInfo
		for i := 0; i <= WorkQueueConflictLookupLimit; i++ {
			many = append(many, &jetstream.ConsumerInfo{Name: fmt.Sprintf("c%d", i), Config: jetstream.ConsumerConfig{FilterSubject: "other.subject"}})
		}
		many = append(many, &jetstream.ConsumerInfo{Name: "beyond"})
		err := plan.ExplainWorkQueueConflict(ctx, &conflictTestStream{listed: many}, "JOBS", "orders-intake", []string{"jobs.eu.>"}, owner, conflict)
		if err != conflict {
			t.Errorf("error = %v, want the server error: the consumer in the way is beyond the limit", err)
		}
	})

	unchanged := []struct {
		name   string
		stream *conflictTestStream
		err    error
		listed int
	}{
		{"another error", &conflictTestStream{listed: consumers()}, rejection(errCodeConsumerInvalidConfig, "deliver policy can not be updated"), 0},
		{"not an API error", &conflictTestStream{listed: consumers()}, errors.New("timeout"), 0},
		{"nothing listed", &conflictTestStream{listErr: errors.New("no responders")}, conflict, 1},
		{"only the consumer itself", &conflictTestStream{listed: consumers()[:2]}, conflict, 1},
	}
	for _, tt := range unchanged {
		t.Run("unchanged: "+tt.name, func(t *testing.T) {
			got := plan.ExplainWorkQueueConflict(ctx, tt.stream, "JOBS", "orders-intake", []string{"jobs.eu.>"}, owner, tt.err)
			if got != tt.err {
				t.Errorf("error = %v, want %v unchanged", got, tt.err)
			}
			if tt.stream.listings != tt.listed {
				t.Errorf("consumer listings = %d, want %d", tt.stream.listings, tt.listed)
			}
		})
	}

	t.Run("unchanged: no stream handle", func(t *testing.T) {
		if got := plan.ExplainWorkQueueConflict(ctx, nil, "JOBS", "orders-intake", nil, owner, conflict); got != conflict {
			t.Errorf("error = %v, want the server error unchanged", got)
		}
	})
}

// A consumer of a shared work-queue stream whose filter update is rejected
// because of another consumer gets the explanation too.
func TestStreamPlan_FilterExistingConsumers_WorkQueueConflict(t *testing.T) {
	plan, err := NewStreamPlan(StreamRegistrations([]*types.ServiceEntry{
		workQueueService("orders", "a", "JOBS", "jobs.a"),
		workQueueService("orders", "b", "JOBS", "jobs.b"),
	}, nil))
	if err != nil {
		t.Fatalf("NewStreamPlan: %v", err)
	}
	stream := &conflictTestStream{
		planTestStream: planTestStream{
			consumers: map[string]*jetstream.ConsumerInfo{"orders-a": {Config: jetstream.ConsumerConfig{Durable: "orders-a"}}},
			updateErr: rejection(errCodeWorkQueueNotUnique, "filtered consumer not unique on workqueue stream"),
		},
		listed: []*jetstream.ConsumerInfo{
			{Name: "orders-a"},
			{Name: "orders-old", Config: jetstream.ConsumerConfig{FilterSubject: "jobs.a"}},
		},
	}
	err = plan.FilterExistingConsumers(context.Background(), "JOBS", stream)
	if !errors.Is(err, monoerrors.ErrWorkQueueConsumerConflict) {
		t.Fatalf("error = %v, want ErrWorkQueueConsumerConflict", err)
	}
	for _, part := range []string{"consumer orders-a (filtered on jobs.a)", "stream consumer service orders.a", "orders-old (filtered on jobs.a"} {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("error does not mention %q: %v", part, err)
		}
	}
}

// The stream consumer service path explains a rejected consumer too, and
// looks the stream up only then.
func TestEnsureStreamConsumer_WorkQueueConflict(t *testing.T) {
	ctx := context.Background()
	entry := workQueueService("orders", "intake", "JOBS", "jobs.>")
	cfg := types.ConsumerConfig{Name: "orders-intake", Durable: "orders-intake"}
	stream := &conflictTestStream{listed: []*jetstream.ConsumerInfo{
		{Name: "orders-ingest", Config: jetstream.ConsumerConfig{
			Metadata: map[string]string{RegistrationMetadataKey: "stream consumer service orders.ingest"}}},
	}}

	t.Run("rejected", func(t *testing.T) {
		es := &writeTestEventStream{streams: map[string]jetstream.Stream{"JOBS": stream}}
		es.createConsumerErr = rejection(errCodeWorkQueueMultipleUnfiltered, "multiple non-filtered consumers not allowed on workqueue stream")
		lm, _ := newMigrationTestManager(es)
		_, err := lm.ensureStreamConsumer(ctx, es, "JOBS", cfg, entry)
		if !errors.Is(err, monoerrors.ErrWorkQueueConsumerConflict) || !IsWorkQueueConsumerConflict(err) {
			t.Fatalf("error = %v, want ErrWorkQueueConsumerConflict wrapping the server error", err)
		}
		for _, part := range []string{"consumer orders-intake (not filtered) of the stream consumer service orders.intake", "orders-ingest (not filtered"} {
			if !strings.Contains(err.Error(), part) {
				t.Errorf("error does not mention %q: %v", part, err)
			}
		}
	})

	t.Run("another error is not looked into", func(t *testing.T) {
		es := &writeTestEventStream{streams: map[string]jetstream.Stream{"JOBS": stream}}
		es.createConsumerErr = rejection(errCodeConsumerInvalidConfig, "max deliver can not be updated")
		lm, _ := newMigrationTestManager(es)
		_, err := lm.ensureStreamConsumer(ctx, es, "JOBS", cfg, entry)
		if err == nil || errors.Is(err, monoerrors.ErrWorkQueueConsumerConflict) {
			t.Fatalf("error = %v, want the server error", err)
		}
	})

	t.Run("accepted: no stream lookup", func(t *testing.T) {
		es := &writeTestEventStream{streams: map[string]jetstream.Stream{"JOBS": stream}}
		lm, _ := newMigrationTestManager(es)
		if _, err := lm.ensureStreamConsumer(ctx, es, "JOBS", cfg, entry); err != nil {
			t.Fatalf("ensureStreamConsumer: %v", err)
		}
		if es.requests() != 0 {
			t.Errorf("requests besides the consumer write = %d, want none", es.requests())
		}
	})
}
