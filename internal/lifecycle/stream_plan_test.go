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

	"github.com/go-monolith/mono/internal/registry"
	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
)

func TestSubjectCovers(t *testing.T) {
	tests := []struct {
		pattern, sub string
		want         bool
	}{
		{"tasks.new", "tasks.new", true},
		{"tasks.new", "tasks.old", false},
		{"tasks.new", "tasks", false},
		{"tasks", "tasks.new", false},
		{"tasks.*", "tasks.new", true},
		{"tasks.*", "tasks.*", true},
		{"tasks.*", "tasks.new.urgent", false},
		{"tasks.*", "tasks", false},
		{"tasks.*", "tasks.>", false},
		{"tasks.>", "tasks.new", true},
		{"tasks.>", "tasks.new.urgent", true},
		{"tasks.>", "tasks.*", true},
		{"tasks.>", "tasks.>", true},
		{"tasks.>", "tasks", false},
		{"tasks.>", "orders.new", false},
		{">", "tasks", true},
		{">", "tasks.>", true},
		{"*.new", "tasks.new", true},
		{"*.new", "tasks.old", false},
		{"tasks.*.done", "tasks.a.done", true},
		{"tasks.*.done", "tasks.a.*", false},
		{"tasks.a.*", "tasks.*.done", false},
		{"a.*.>", "a.>", false},
		{"a.>", "a.*.>", true},
		{"a.*.>", "a.b.>", true},
		{"*", ">", false},
		{">", "*", true},
		{"a.*", "a.*.>", false},
	}
	for _, tt := range tests {
		if got := SubjectCovers(tt.pattern, tt.sub); got != tt.want {
			t.Errorf("SubjectCovers(%q, %q) = %v, want %v", tt.pattern, tt.sub, got, tt.want)
		}
	}
}

func TestSubjectsCollide(t *testing.T) {
	tests := []struct {
		a, b string
		want bool
	}{
		{"tasks.new", "tasks.new", true},
		{"tasks.new", "tasks.old", false},
		{"tasks.new", "tasks", false},
		{"tasks.*", "tasks.new", true},
		{"tasks.*", "tasks.new.urgent", false},
		{"tasks.>", "tasks.new.urgent", true},
		{"tasks.>", "tasks", false},
		{"tasks.>", "orders.>", false},
		{">", "tasks", true},
		{"a.*.c", "a.b.*", true},
		{"a.*.c", "a.b.d", false},
		{"a.*.>", "a.>", true},
		{"*.b", "a.*", true},
		{"*.b", "a.c", false},
		{"a.*", "a.*.>", false},
		{"a.>", "*.b.c", true},
	}
	for _, tt := range tests {
		if got := SubjectsCollide(tt.a, tt.b); got != tt.want {
			t.Errorf("SubjectsCollide(%q, %q) = %v, want %v", tt.a, tt.b, got, tt.want)
		}
		if got := SubjectsCollide(tt.b, tt.a); got != tt.want {
			t.Errorf("SubjectsCollide(%q, %q) = %v, want %v", tt.b, tt.a, got, tt.want)
		}
	}
}

func TestCheckStreamSubjects(t *testing.T) {
	// Covering, equal and disjoint subjects are accepted, and so are
	// overlapping subjects on different streams.
	accepted := StreamRegistrations([]*types.ServiceEntry{
		planService("tasks", "all", "TASKS", "tasks.>"),
		planService("tasks", "urgent", "TASKS", "tasks.urgent", "tasks.urgent"),
		planService("tasks", "other", "TASKS", "orders.new"),
		planService("tasks", "elsewhere", "ELSEWHERE", "a.*.c"),
		planService("tasks", "third", "THIRD", "a.b.*"),
	}, nil)
	if err := CheckStreamSubjects(accepted); err != nil {
		t.Fatalf("error = %v, want nil", err)
	}

	// Two subjects that overlap are accepted when a third one covers both:
	// the stream is then written with the covering subject only.
	covered := StreamRegistrations([]*types.ServiceEntry{
		planService("events", "orders", "EVENTS", "events.orders.*"),
		planService("events", "created", "EVENTS", "events.*.created"),
		planService("events", "all", "EVENTS", "events.>"),
	}, nil)
	plan, err := NewStreamPlan(covered)
	if err != nil {
		t.Fatalf("covered by a third subject: error = %v, want nil", err)
	}
	if got := plan.Stream("EVENTS").Subjects; !slices.Equal(got, []string{"events.>"}) {
		t.Errorf("subjects = %v, want the covering subject only", got)
	}

	_, err = NewStreamPlan(StreamRegistrations([]*types.ServiceEntry{
		planService("tasks", "first", "TASKS", "a.*.c"),
		planService("tasks", "second", "TASKS", "x.y", "a.b.*"),
	}, nil))
	if !errors.Is(err, monoerrors.ErrInvalidConfiguration) {
		t.Fatalf("error = %v, want ErrInvalidConfiguration", err)
	}
	for _, part := range []string{
		`stream TASKS cannot listen on subject "a.*.c" of the stream consumer service tasks.first`,
		`and subject "a.b.*" of the stream consumer service tasks.second`,
		"overlap without one covering the other",
	} {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("error %q does not contain %q", err.Error(), part)
		}
	}
}

func TestMinimiseSubjects(t *testing.T) {
	tests := []struct {
		name string
		in   []string
		want []string
	}{
		{"nothing", nil, []string{}},
		{"kept in order", []string{"b", "a"}, []string{"b", "a"}},
		{"duplicates", []string{"a", "b", "a", "b"}, []string{"a", "b"}},
		{"covered by a later one", []string{"tasks.urgent", "tasks.>"}, []string{"tasks.>"}},
		{"a duplicate that is also covered", []string{"a.b", "a.b", "a.>"}, []string{"a.>"}},
		{"covered by an earlier one", []string{"tasks.>", "tasks.urgent", "tasks.*"}, []string{"tasks.>"}},
		{"service subjects do not cover each other", []string{"services.m.s", "services.m.s.>"}, []string{"services.m.s", "services.m.s.>"}},
		{"overlap without cover is kept", []string{"a.*.c", "a.b.*"}, []string{"a.*.c", "a.b.*"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MinimiseSubjects(tt.in); !slices.Equal(got, tt.want) {
				t.Errorf("MinimiseSubjects(%v) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

// planService builds a stream consumer service entry.
func planService(module, name, stream string, subjects ...string) *types.ServiceEntry {
	return &types.ServiceEntry{
		Name:       name,
		ModuleName: module,
		Type:       types.ServiceTypeStreamConsumer,
		StreamConsumerConfig: &types.StreamConsumerConfig{
			Stream: types.StreamConfig{Name: stream, Subjects: subjects},
		},
		StreamConsumerHandler: func(context.Context, []*types.Msg) error { return nil },
	}
}

func TestStreamRegistrations(t *testing.T) {
	filtered := planService("orders", "audit", "ORDERS", "orders.>")
	filtered.StreamConsumerConfig.Consumer.FilterSubjects = []string{"orders.eu.>"}
	event := streamEntry("shipping", "PAYMENTS", paymentProcessedV1)
	event.Config.Consumer.FilterSubject = paymentProcessedV1.Subject

	regs := StreamRegistrations(
		[]*types.ServiceEntry{
			planService("orders", "ingest", "ORDERS", "services.orders.ingest"),
			nil,
			{Name: "lookup", ModuleName: "orders", Type: types.ServiceTypeRequestReply},
			{Name: "broken", ModuleName: "orders", Type: types.ServiceTypeStreamConsumer},
			filtered,
		},
		[]types.EventStreamConsumerEntry{event, streamEntry("shipping", "PAYMENTS", paymentProcessedV1)},
	)

	want := []StreamRegistration{
		{Owner: "stream consumer service orders.ingest", Consumer: "orders-ingest"},
		{Owner: "stream consumer service orders.audit", Consumer: "orders-audit", CallerFilter: true},
		{Owner: "event stream consumer of module shipping for event billing.PaymentProcessed", Consumer: "shipping-billing-PaymentProcessed-v1", CallerFilter: true},
		{Owner: "event stream consumer of module shipping for event billing.PaymentProcessed", Consumer: "shipping-billing-PaymentProcessed-v1-2"},
	}
	if len(regs) != len(want) {
		t.Fatalf("registrations = %+v, want %d", regs, len(want))
	}
	for i, w := range want {
		if regs[i].Owner != w.Owner || regs[i].Consumer != w.Consumer || regs[i].CallerFilter != w.CallerFilter {
			t.Errorf("registration %d = {%s, %s, %v}, want {%s, %s, %v}", i,
				regs[i].Owner, regs[i].Consumer, regs[i].CallerFilter, w.Owner, w.Consumer, w.CallerFilter)
		}
	}
	if regs[0].Stream.Name != "ORDERS" || regs[2].Stream.Name != "PAYMENTS" {
		t.Errorf("stream names = %s, %s", regs[0].Stream.Name, regs[2].Stream.Name)
	}
}

func TestNewStreamPlan(t *testing.T) {
	regs := StreamRegistrations(
		[]*types.ServiceEntry{
			planService("orders", "first", "SHARED", "services.orders.first", "services.orders.first.>"),
			planService("orders", "second", "SHARED", "services.orders.second", "services.orders.second.>"),
			planService("orders", "alone", "ALONE", "services.orders.alone"),
			planService("tasks", "all", "TASKS", "tasks.>"),
			planService("tasks", "urgent", "TASKS", "tasks.urgent"),
		},
		[]types.EventStreamConsumerEntry{
			streamEntry("billing", "SAME", paymentProcessedV1),
			streamEntry("shipping", "SAME", paymentProcessedV1),
			streamEntry("billing", "SHARED", subscriptionChangedV1),
		},
	)
	plan, err := NewStreamPlan(regs)
	if err != nil {
		t.Fatalf("NewStreamPlan: %v", err)
	}

	if got, want := plan.StreamNames(), []string{"ALONE", "SAME", "SHARED", "TASKS"}; !slices.Equal(got, want) {
		t.Errorf("StreamNames = %v, want %v", got, want)
	}

	shared := plan.Stream("SHARED")
	wantSubjects := []string{
		"services.orders.first", "services.orders.first.>",
		"services.orders.second", "services.orders.second.>",
		subscriptionChangedV1.Subject,
	}
	if !shared.Shared || !slices.Equal(shared.Subjects, wantSubjects) {
		t.Errorf("SHARED = shared %v, subjects %v; want shared with %v", shared.Shared, shared.Subjects, wantSubjects)
	}
	for _, consumer := range []string{"orders-first", "orders-second", "billing-billing-SubscriptionChanged-v1"} {
		if _, ok := shared.Consumers[consumer]; !ok {
			t.Errorf("SHARED consumers %v do not contain %s", shared.Consumers, consumer)
		}
	}
	if got := plan.Subjects("SHARED", []string{"services.orders.first"}); !slices.Equal(got, wantSubjects) {
		t.Errorf("Subjects(SHARED) = %v, want %v", got, wantSubjects)
	}
	wantMarker := "event stream consumer of module billing for event billing.SubscriptionChanged; " +
		"stream consumer service orders.first; stream consumer service orders.second"
	if got := plan.Marker("SHARED", "ignored"); got != wantMarker {
		t.Errorf("Marker(SHARED) = %q, want %q", got, wantMarker)
	}

	if !shared.HasEvents || plan.Stream("ALONE").HasEvents || plan.Stream("TASKS").HasEvents {
		t.Error("HasEvents must be set for SHARED only among the service streams")
	}
	if same := plan.Stream("SAME"); same.Shared || !slices.Equal(same.Subjects, []string{paymentProcessedV1.Subject}) {
		t.Errorf("SAME = shared %v, subjects %v; want not shared, one subject", same.Shared, same.Subjects)
	}
	if alone := plan.Stream("ALONE"); alone.Shared {
		t.Error("ALONE must not be shared")
	}
	if tasks := plan.Stream("TASKS"); !tasks.Shared || !slices.Equal(tasks.Subjects, []string{"tasks.>"}) {
		t.Errorf("TASKS = shared %v, subjects %v; want shared with the covering subject only", tasks.Shared, tasks.Subjects)
	}

	// A stream the plan does not know, and a nil plan, plan nothing.
	own := []string{"own.subject"}
	for name, p := range map[string]*StreamPlan{"unknown stream": plan, "nil plan": nil} {
		if got := p.Subjects("OTHER", own); !slices.Equal(got, own) {
			t.Errorf("%s: Subjects = %v, want %v", name, got, own)
		}
		if got := p.Marker("OTHER", "its owner"); got != "its owner" {
			t.Errorf("%s: Marker = %q, want the registration's own", name, got)
		}
		if p.Stream("OTHER") != nil {
			t.Errorf("%s: Stream must be nil", name)
		}
	}
	var none *StreamPlan
	if names := none.StreamNames(); names != nil {
		t.Errorf("nil plan: StreamNames = %v, want nil", names)
	}
}

func TestNewStreamPlan_ImmutableConflict(t *testing.T) {
	memory := planService("orders", "ingest", "ORDERS", "services.orders.ingest")
	memory.StreamConsumerConfig.Stream.Storage = types.MemoryStorage
	event := streamEntry("billing", "ORDERS", paymentProcessedV1)
	event.Config.Stream.Retention = types.WorkQueuePolicy

	_, err := NewStreamPlan(StreamRegistrations([]*types.ServiceEntry{memory}, []types.EventStreamConsumerEntry{event}))
	if !errors.Is(err, monoerrors.ErrStreamConfigImmutable) {
		t.Fatalf("error = %v, want ErrStreamConfigImmutable", err)
	}
	for _, part := range []string{
		"stream ORDERS is registered by the stream consumer service orders.ingest",
		"and by the event stream consumer of module billing for event billing.PaymentProcessed",
		"the first with memory storage and the second with file storage",
		"the first with limits retention and the second with work-queue retention",
	} {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("error %q does not contain %q", err.Error(), part)
		}
	}
}

func TestNewStreamPlan_DuplicateConsumerDurableName(t *testing.T) {
	// Different streams may reuse the same consumer durable name.
	diffStreams := StreamRegistrations([]*types.ServiceEntry{
		planService("order", "sync", "ORDERS", "orders.>"),
		planService("order", "sync", "ARCHIVE", "archive.>"),
	}, nil)
	if err := CheckStreamConsumers(diffStreams); err != nil {
		t.Fatalf("different streams: error = %v, want nil", err)
	}

	// Two stream consumer services whose module and service names join to
	// the same durable consumer name on the same stream are rejected.
	s1 := planService("order", "events-sync", "ORDERS", "orders.events.>")
	s2 := planService("order-events", "sync", "ORDERS", "orders.sync.>")
	regs := StreamRegistrations([]*types.ServiceEntry{s1, s2}, nil)

	if err := CheckStreamConsumers(regs); !errors.Is(err, monoerrors.ErrInvalidConfiguration) {
		t.Fatalf("CheckStreamConsumers: error = %v, want ErrInvalidConfiguration", err)
	}

	_, err := NewStreamPlan(regs)
	if !errors.Is(err, monoerrors.ErrInvalidConfiguration) {
		t.Fatalf("NewStreamPlan: error = %v, want ErrInvalidConfiguration", err)
	}
	for _, part := range []string{
		`stream ORDERS has duplicate consumer durable name "order-events-sync"`,
		"registered by the stream consumer service order.events-sync",
		"and by the stream consumer service order-events.sync",
		"consumer durable names must be distinct on the same stream",
	} {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("error %q does not contain %q", err.Error(), part)
		}
	}

	// An event stream consumer and a stream consumer service producing
	// the same consumer durable name on the same stream are also rejected.
	svc := planService("shipping", "billing-PaymentProcessed-v1", "PAYMENTS", "payments.shipping.>")
	ev := streamEntry("shipping", "PAYMENTS", paymentProcessedV1)
	regsEv := StreamRegistrations([]*types.ServiceEntry{svc}, []types.EventStreamConsumerEntry{ev})

	if err := CheckStreamConsumers(regsEv); !errors.Is(err, monoerrors.ErrInvalidConfiguration) {
		t.Fatalf("CheckStreamConsumers (service + event): error = %v, want ErrInvalidConfiguration", err)
	}

	_, err = NewStreamPlan(regsEv)
	if !errors.Is(err, monoerrors.ErrInvalidConfiguration) {
		t.Fatalf("NewStreamPlan (service + event): error = %v, want ErrInvalidConfiguration", err)
	}
	for _, part := range []string{
		`stream PAYMENTS has duplicate consumer durable name "shipping-billing-PaymentProcessed-v1"`,
		"registered by the stream consumer service shipping.billing-PaymentProcessed-v1",
		"and by the event stream consumer of module shipping for event billing.PaymentProcessed",
	} {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("error %q does not contain %q", err.Error(), part)
		}
	}
}

func TestStreamPlan_FilterConsumer(t *testing.T) {
	plan, err := NewStreamPlan(StreamRegistrations(
		[]*types.ServiceEntry{
			planService("orders", "first", "SHARED", "services.orders.first", "services.orders.first.>"),
			planService("orders", "second", "SHARED", "services.orders.second"),
			planService("orders", "alone", "ALONE", "services.orders.alone"),
		},
		[]types.EventStreamConsumerEntry{
			streamEntry("billing", "SAME", paymentProcessedV1),
			streamEntry("shipping", "SAME", paymentProcessedV1),
		},
	))
	if err != nil {
		t.Fatalf("NewStreamPlan: %v", err)
	}

	tests := []struct {
		name         string
		plan         *StreamPlan
		stream       string
		own          []string
		cfg          types.ConsumerConfig
		wantSubject  string
		wantSubjects []string
	}{
		{name: "one subject on a shared stream", plan: plan, stream: "SHARED", own: []string{"services.orders.second"},
			wantSubject: "services.orders.second"},
		{name: "several subjects on a shared stream", plan: plan, stream: "SHARED",
			own:          []string{"services.orders.first", "services.orders.first.>"},
			wantSubjects: []string{"services.orders.first", "services.orders.first.>"}},
		{name: "the caller's FilterSubject wins", plan: plan, stream: "SHARED", own: []string{"services.orders.second"},
			cfg: types.ConsumerConfig{FilterSubject: "caller"}, wantSubject: "caller"},
		{name: "the caller's FilterSubjects win", plan: plan, stream: "SHARED", own: []string{"services.orders.second"},
			cfg: types.ConsumerConfig{FilterSubjects: []string{"a", "b"}}, wantSubjects: []string{"a", "b"}},
		{name: "same subjects on one stream", plan: plan, stream: "SAME", own: []string{paymentProcessedV1.Subject}},
		{name: "a stream of its own", plan: plan, stream: "ALONE", own: []string{"services.orders.alone"}},
		{name: "a stream the plan does not know", plan: plan, stream: "OTHER", own: []string{"x"}},
		{name: "nil plan", stream: "SHARED", own: []string{"services.orders.second"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.cfg.MaxDeliver = 7
			got := tt.plan.FilterConsumer(tt.stream, tt.own, tt.cfg)
			if got.FilterSubject != tt.wantSubject || !slices.Equal(got.FilterSubjects, tt.wantSubjects) {
				t.Errorf("filters = %q / %v, want %q / %v", got.FilterSubject, got.FilterSubjects, tt.wantSubject, tt.wantSubjects)
			}
			if got.FilterSubject != "" && len(got.FilterSubjects) > 0 {
				t.Error("FilterSubject and FilterSubjects must not both be set")
			}
			if got.MaxDeliver != 7 {
				t.Errorf("MaxDeliver = %d, the rest of the configuration must be kept", got.MaxDeliver)
			}
			// Only a filter the framework set is marked as such.
			frameworkSet := hasSubjectFilter(got) && !hasSubjectFilter(tt.cfg)
			if marked := got.Metadata[FilterMetadataKey] == FilterMetadataValue; marked != frameworkSet {
				t.Errorf("filter marker = %v, want %v (metadata %v)", marked, frameworkSet, got.Metadata)
			}
		})
	}

	t.Run("the caller's metadata map is not modified", func(t *testing.T) {
		caller := map[string]string{"team": "orders"}
		got := plan.FilterConsumer("SHARED", []string{"services.orders.second"}, types.ConsumerConfig{Metadata: caller})
		if got.Metadata["team"] != "orders" || got.Metadata[FilterMetadataKey] != FilterMetadataValue {
			t.Errorf("metadata = %v, want the caller's entry and the marker", got.Metadata)
		}
		if len(caller) != 1 {
			t.Errorf("the caller's map was modified: %v", caller)
		}
	})
}

func TestWithRegistrationMarker(t *testing.T) {
	caller := map[string]string{"team": "payments"}
	marked := WithRegistrationMarker(caller, "stream consumer service orders.ingest")
	if marked[RegistrationMetadataKey] != "stream consumer service orders.ingest" || marked["team"] != "payments" {
		t.Errorf("marked = %v", marked)
	}
	if _, changed := caller[RegistrationMetadataKey]; changed || len(caller) != 1 {
		t.Errorf("the caller's map was modified: %v", caller)
	}
	if got := WithRegistrationMarker(nil, "x"); got[RegistrationMetadataKey] != "x" {
		t.Errorf("nil metadata: %v", got)
	}
}

// planTestStream is a jetstream.Stream with the consumers of a shared stream.
// Its other methods are not implemented.
type planTestStream struct {
	jetstream.Stream
	consumers map[string]*jetstream.ConsumerInfo
	lookupErr error
	updateErr error
	// appliedDespiteErr makes a failing update take effect all the same.
	appliedDespiteErr bool
	// readBackErr fails the lookups that follow the first update attempt.
	readBackErr    error
	updateAttempts int
	lookups        []string
	updates        []jetstream.ConsumerConfig
}

func (s *planTestStream) Consumer(_ context.Context, name string) (jetstream.Consumer, error) {
	s.lookups = append(s.lookups, name)
	if s.lookupErr != nil {
		return nil, s.lookupErr
	}
	if s.readBackErr != nil && s.updateAttempts > 0 {
		return nil, s.readBackErr
	}
	info, ok := s.consumers[name]
	if !ok {
		return nil, jetstream.ErrConsumerNotFound
	}
	return &cachedInfoConsumer{mockJetStreamConsumer: &mockJetStreamConsumer{}, info: info}, nil
}

func (s *planTestStream) UpdateConsumer(_ context.Context, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	s.updateAttempts++
	if s.updateErr != nil {
		if s.appliedDespiteErr {
			s.consumers[cfg.Durable] = &jetstream.ConsumerInfo{Config: cfg}
		}
		return nil, s.updateErr
	}
	s.updates = append(s.updates, cfg)
	return nil, nil
}

func TestStreamPlan_FilterExistingConsumers(t *testing.T) {
	newPlan := func(t *testing.T) *StreamPlan {
		t.Helper()
		callerFiltered := planService("orders", "audit", "SHARED", "services.orders.audit")
		callerFiltered.StreamConsumerConfig.Consumer.FilterSubject = "services.orders.audit"
		plan, err := NewStreamPlan(StreamRegistrations([]*types.ServiceEntry{
			planService("orders", "first", "SHARED", "services.orders.first", "services.orders.first.>"),
			planService("orders", "second", "SHARED", "services.orders.second"),
			planService("orders", "third", "SHARED", "services.orders.third"),
			planService("orders", "fourth", "SHARED", "services.orders.fourth"),
			planService("orders", "fifth", "SHARED", "services.orders.fifth"),
			callerFiltered,
			planService("orders", "alone", "ALONE", "services.orders.alone"),
		}, nil))
		if err != nil {
			t.Fatalf("NewStreamPlan: %v", err)
		}
		return plan
	}
	existing := func() map[string]*jetstream.ConsumerInfo {
		return map[string]*jetstream.ConsumerInfo{
			// Unfiltered, with settings that must survive.
			"orders-first":  {Config: jetstream.ConsumerConfig{Durable: "orders-first", MaxDeliver: 9}},
			"orders-second": {Config: jetstream.ConsumerConfig{Durable: "orders-second", FilterSubjects: []string{"stale.a", "stale.b"}}},
			// Already filtered as planned.
			"orders-third": {Config: jetstream.ConsumerConfig{Durable: "orders-third", FilterSubject: "services.orders.third"}},
			// Without cached info.
			"orders-fourth": nil,
			// orders-fifth does not exist yet.
			"orders-audit": {Config: jetstream.ConsumerConfig{Durable: "orders-audit"}},
		}
	}

	t.Run("filters the existing consumers once", func(t *testing.T) {
		plan := newPlan(t)
		stream := &planTestStream{consumers: existing()}
		if err := plan.FilterExistingConsumers(context.Background(), "SHARED", stream); err != nil {
			t.Fatalf("FilterExistingConsumers: %v", err)
		}
		if len(stream.updates) != 2 {
			t.Fatalf("updates = %+v, want two", stream.updates)
		}
		first, second := stream.updates[0], stream.updates[1]
		if first.Durable != "orders-first" || first.MaxDeliver != 9 || first.FilterSubject != "" ||
			!slices.Equal(first.FilterSubjects, []string{"services.orders.first", "services.orders.first.>"}) {
			t.Errorf("update of orders-first = %+v", first)
		}
		if second.Durable != "orders-second" || second.FilterSubject != "services.orders.second" || len(second.FilterSubjects) != 0 {
			t.Errorf("update of orders-second = %+v", second)
		}
		for _, update := range stream.updates {
			if update.Metadata[FilterMetadataKey] != FilterMetadataValue {
				t.Errorf("update of %s is not marked as filtered by the framework: %v", update.Durable, update.Metadata)
			}
		}
		if slices.Contains(stream.lookups, "orders-audit") {
			t.Error("a consumer whose caller set a filter must not be looked up")
		}

		// A second call in the same start does nothing.
		lookups := len(stream.lookups)
		if err := plan.FilterExistingConsumers(context.Background(), "SHARED", stream); err != nil {
			t.Fatalf("second call: %v", err)
		}
		if len(stream.lookups) != lookups || len(stream.updates) != 2 {
			t.Error("the second call must not look up or update anything")
		}
	})

	t.Run("skips what is not shared", func(t *testing.T) {
		plan := newPlan(t)
		stream := &planTestStream{consumers: existing()}
		for _, name := range []string{"ALONE", "UNKNOWN"} {
			if err := plan.FilterExistingConsumers(context.Background(), name, stream); err != nil {
				t.Fatalf("%s: %v", name, err)
			}
		}
		var none *StreamPlan
		if err := none.FilterExistingConsumers(context.Background(), "SHARED", stream); err != nil {
			t.Fatalf("nil plan: %v", err)
		}
		if err := plan.FilterExistingConsumers(context.Background(), "SHARED", nil); err != nil {
			t.Fatalf("nil stream: %v", err)
		}
		if len(stream.lookups) != 0 {
			t.Errorf("lookups = %v, want none", stream.lookups)
		}
	})

	t.Run("an update the server applied but answered with the earlier configuration succeeds", func(t *testing.T) {
		plan := newPlan(t)
		stream := &planTestStream{
			consumers:         existing(),
			updateErr:         fmt.Errorf("update: %w", jetstream.ErrConsumerMultipleFilterSubjectsNotSupported),
			appliedDespiteErr: true,
		}
		if err := plan.FilterExistingConsumers(context.Background(), "SHARED", stream); err != nil {
			t.Fatalf("FilterExistingConsumers: %v", err)
		}
		if got := stream.consumers["orders-first"].Config.FilterSubjects; !slices.Equal(got, []string{"services.orders.first", "services.orders.first.>"}) {
			t.Errorf("filters of orders-first = %v", got)
		}
	})

	t.Run("the same answer for an update the server did not apply fails", func(t *testing.T) {
		plan := newPlan(t)
		stream := &planTestStream{consumers: existing(), updateErr: jetstream.ErrConsumerMultipleFilterSubjectsNotSupported}
		err := plan.FilterExistingConsumers(context.Background(), "SHARED", stream)
		if !errors.Is(err, jetstream.ErrConsumerMultipleFilterSubjectsNotSupported) {
			t.Fatalf("error = %v, want the update error", err)
		}
	})

	t.Run("the same answer when the consumer cannot be read back fails with both errors", func(t *testing.T) {
		plan := newPlan(t)
		readBack := errors.New("no responders")
		stream := &planTestStream{
			consumers:         existing(),
			updateErr:         jetstream.ErrConsumerMultipleFilterSubjectsNotSupported,
			appliedDespiteErr: true,
			readBackErr:       readBack,
		}
		err := plan.FilterExistingConsumers(context.Background(), "SHARED", stream)
		if !errors.Is(err, jetstream.ErrConsumerMultipleFilterSubjectsNotSupported) || !errors.Is(err, readBack) {
			t.Fatalf("error = %v, want the update error and the read-back error", err)
		}
	})

	t.Run("a failed lookup or update is returned and retried", func(t *testing.T) {
		plan := newPlan(t)
		boom := errors.New("no responders")
		stream := &planTestStream{consumers: existing(), lookupErr: boom}
		if err := plan.FilterExistingConsumers(context.Background(), "SHARED", stream); !errors.Is(err, boom) {
			t.Fatalf("lookup error = %v, want %v", err, boom)
		}
		stream.lookupErr, stream.updateErr = nil, boom
		if err := plan.FilterExistingConsumers(context.Background(), "SHARED", stream); !errors.Is(err, boom) {
			t.Fatalf("update error = %v, want %v", err, boom)
		}
		stream.updateErr = nil
		if err := plan.FilterExistingConsumers(context.Background(), "SHARED", stream); err != nil {
			t.Fatalf("retry: %v", err)
		}
		if len(stream.updates) != 2 {
			t.Errorf("updates after the retry = %+v, want two", stream.updates)
		}
	})
}

// recordingEventStream is an EventStream that records the stream and consumer
// configurations it is given, and returns stream from CreateOrUpdateStream.
type recordingEventStream struct {
	mockEventStream
	stream    jetstream.Stream
	streams   []types.StreamConfig
	consumers []types.ConsumerConfig
}

func (r *recordingEventStream) CreateOrUpdateStream(_ context.Context, cfg types.StreamConfig) (jetstream.Stream, error) {
	r.streams = append(r.streams, cfg)
	return r.stream, nil
}

func (r *recordingEventStream) CreateOrUpdateConsumer(_ context.Context, _ string, cfg types.ConsumerConfig) (jetstream.Consumer, error) {
	r.consumers = append(r.consumers, cfg)
	return &mockJetStreamConsumer{}, nil
}

// On a shared stream both setup paths write the subjects of every
// registration, filter their own consumer and mark what they create; a
// stream of its own is written with its own subjects and no filter.
func TestSetupStreamConsumers_SharedStream(t *testing.T) {
	service := planService("orders", "ingest", "SHARED", "services.orders.ingest", "services.orders.ingest.>")
	service.StreamConsumerConfig.Stream.Metadata = map[string]string{"team": "orders"}
	service.StreamConsumerConfig.Fetch = types.FetchConfig{BatchSize: 1, Timeout: 10_000_000}
	alone := planService("orders", "alone", "ALONE", "services.orders.alone")
	alone.StreamConsumerConfig.Fetch = service.StreamConsumerConfig.Fetch
	event := streamEntry("billing", "SHARED", paymentProcessedV1)

	plan, err := NewStreamPlan(StreamRegistrations([]*types.ServiceEntry{service, alone}, []types.EventStreamConsumerEntry{event}))
	if err != nil {
		t.Fatalf("NewStreamPlan: %v", err)
	}
	existing := &planTestStream{consumers: map[string]*jetstream.ConsumerInfo{
		"billing-billing-PaymentProcessed-v1": {Config: jetstream.ConsumerConfig{Durable: "billing-billing-PaymentProcessed-v1"}},
	}}
	es := &recordingEventStream{stream: existing}
	lm, _ := newMigrationTestManager(es)
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
	if err := lm.setupStreamConsumer(ctx, alone); err != nil {
		t.Fatalf("setupStreamConsumer alone: %v", err)
	}
	for _, stop := range lm.streamConsumers {
		stop()
	}

	union := []string{"services.orders.ingest", "services.orders.ingest.>", paymentProcessedV1.Subject}
	marker := "event stream consumer of module billing for event billing.PaymentProcessed; stream consumer service orders.ingest"
	for i := 0; i < 2; i++ {
		got := es.streams[i]
		if !slices.Equal(got.Subjects, union) || got.Metadata[RegistrationMetadataKey] != marker {
			t.Errorf("write %d of SHARED: subjects %v, metadata %v; want %v and marker %q", i, got.Subjects, got.Metadata, union, marker)
		}
	}
	if es.streams[0].Metadata["team"] != "orders" {
		t.Errorf("the caller's stream metadata was dropped: %v", es.streams[0].Metadata)
	}
	if _, changed := service.StreamConsumerConfig.Stream.Metadata[RegistrationMetadataKey]; changed {
		t.Error("the caller's stream metadata map was modified")
	}
	if got := es.streams[2]; !slices.Equal(got.Subjects, []string{"services.orders.alone"}) ||
		got.Metadata[RegistrationMetadataKey] != "stream consumer service orders.alone" {
		t.Errorf("write of ALONE: subjects %v, metadata %v", got.Subjects, got.Metadata)
	}

	// The durable that already existed was filtered before the first
	// consumer of the shared stream was created.
	if len(existing.updates) != 1 || existing.updates[0].FilterSubject != paymentProcessedV1.Subject {
		t.Errorf("updates of existing consumers = %+v, want the event's durable filtered", existing.updates)
	}

	ingest, payment, own := es.consumers[0], es.consumers[1], es.consumers[2]
	if !slices.Equal(ingest.FilterSubjects, []string{"services.orders.ingest", "services.orders.ingest.>"}) || ingest.FilterSubject != "" ||
		ingest.Metadata[RegistrationMetadataKey] != "stream consumer service orders.ingest" {
		t.Errorf("consumer of the service = %+v", ingest)
	}
	if payment.FilterSubject != paymentProcessedV1.Subject || len(payment.FilterSubjects) != 0 ||
		payment.Metadata[RegistrationMetadataKey] != "event stream consumer of module billing for event billing.PaymentProcessed" {
		t.Errorf("consumer of the event = %+v", payment)
	}
	if own.FilterSubject != "" || len(own.FilterSubjects) != 0 || own.Metadata[RegistrationMetadataKey] != "stream consumer service orders.alone" {
		t.Errorf("consumer of the service with a stream of its own = %+v", own)
	}
}

// The migration marker write uses the subjects of every registration of the
// stream, like the setup writes before it.
func TestMarkLegacyMigrationDone_SharedStream(t *testing.T) {
	store := newFakeConsumerStore()
	lm, _ := newMigrationTestManager(store)
	first := streamEntry("billing", "EVENTS", paymentProcessedV1)
	second := streamEntry("billing", "EVENTS", subscriptionChangedV1)
	entries := []types.EventStreamConsumerEntry{first, second}
	plan, err := NewStreamPlan(StreamRegistrations(nil, entries))
	if err != nil {
		t.Fatalf("NewStreamPlan: %v", err)
	}
	lm.streamPlan = plan

	lm.markLegacyMigrationDone(context.Background(), entries, []eventStreamDurable{{}, {}})

	writes := store.writtenStreams()
	if len(writes) != 1 {
		t.Fatalf("stream writes = %+v, want one", writes)
	}
	want := []string{paymentProcessedV1.Subject, subscriptionChangedV1.Subject}
	if got := writes[0]; !slices.Equal(got.Subjects, want) ||
		got.Metadata[legacyMigrationMarkerKey] != legacyMigrationMarkerValue || got.Metadata[RegistrationMetadataKey] == "" {
		t.Errorf("write = subjects %v, metadata %v; want %v with both markers", got.Subjects, got.Metadata, want)
	}
}

// A stream consumer service that shares its stream with an event stream
// consumer keeps the legacy migration marker the stream carries: its write
// replaces the metadata, and without the marker the migration would run
// again on every start.
func TestSetupStreamConsumer_KeepsLegacyMigrationMarker(t *testing.T) {
	setup := func(t *testing.T, store *fakeConsumerStore, stream string) (types.StreamConfig, error) {
		t.Helper()
		service := planService("orders", "ingest", stream, "services.orders.ingest")
		service.StreamConsumerConfig.Fetch = types.FetchConfig{BatchSize: 1, Timeout: 10_000_000}
		alone := planService("orders", "alone", "ALONE", "services.orders.alone")
		plan, err := NewStreamPlan(StreamRegistrations(
			[]*types.ServiceEntry{service, alone},
			[]types.EventStreamConsumerEntry{streamEntry("billing", "MIXED", paymentProcessedV1)}))
		if err != nil {
			t.Fatalf("NewStreamPlan: %v", err)
		}
		lm, _ := newMigrationTestManager(store)
		lm.streamPlan = plan
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		lm.runtimeCtx = ctx
		err = lm.setupStreamConsumer(ctx, service)
		for _, stop := range lm.streamConsumers {
			stop()
		}
		if writes := store.writtenStreams(); len(writes) > 0 {
			return writes[0], err
		}
		return types.StreamConfig{}, err
	}

	t.Run("marked stream", func(t *testing.T) {
		store := newFakeConsumerStore()
		store.metadata = map[string]map[string]string{"MIXED": {legacyMigrationMarkerKey: legacyMigrationMarkerValue, "stale": "dropped"}}
		written, err := setup(t, store, "MIXED")
		if err != nil {
			t.Fatalf("setupStreamConsumer: %v", err)
		}
		if written.Metadata[legacyMigrationMarkerKey] != legacyMigrationMarkerValue || written.Metadata[RegistrationMetadataKey] == "" {
			t.Errorf("metadata = %v, want both markers", written.Metadata)
		}
		if _, kept := written.Metadata["stale"]; kept {
			t.Errorf("metadata = %v: only the marker is carried over", written.Metadata)
		}
	})

	t.Run("unmarked stream", func(t *testing.T) {
		written, err := setup(t, newFakeConsumerStore(), "MIXED")
		if err != nil {
			t.Fatalf("setupStreamConsumer: %v", err)
		}
		if _, marked := written.Metadata[legacyMigrationMarkerKey]; marked {
			t.Errorf("metadata = %v, want no migration marker", written.Metadata)
		}
	})

	t.Run("a stream without an event stream consumer is not read", func(t *testing.T) {
		store := newFakeConsumerStore()
		store.metadata = map[string]map[string]string{"ALONE_TOO": {legacyMigrationMarkerKey: legacyMigrationMarkerValue}}
		if _, err := setup(t, store, "ALONE_TOO"); err != nil {
			t.Fatalf("setupStreamConsumer: %v", err)
		}
		for _, op := range store.recorded() {
			if strings.HasPrefix(op, "metadata:") {
				t.Errorf("unexpected lookup %s", op)
			}
		}
	})

	t.Run("a failed read fails the step", func(t *testing.T) {
		store := newFakeConsumerStore()
		store.metadataErr = errors.New("no responders")
		if _, err := setup(t, store, "MIXED"); err == nil || !strings.Contains(err.Error(), "failed to read metadata of stream MIXED") {
			t.Fatalf("error = %v, want the metadata read failure", err)
		}
	})
}

// Start plans the streams before the first one is written, and sets the
// services up in name order: with two modules sharing one stream, every write
// of it already carries the subjects of both, and the writes come in the same
// order on every start.
func TestLifecycleManager_SharedStreamPlannedBeforeFirstWrite(t *testing.T) {
	for attempt := 0; attempt < 5; attempt++ {
		logger := &mockLogger{}
		reg := registry.NewModuleRegistry(logger)
		eventBus := newMockEventBus()
		es := &recordingEventStream{}
		eventBus.eventStream = es
		lm := NewLifecycleManager(reg, registry.NewPluginRegistry(logger), eventBus, registry.NewEventRegistry(logger), logger, 0)

		service := func(name string) *types.ServiceEntry {
			return &types.ServiceEntry{
				Name: name,
				Type: types.ServiceTypeStreamConsumer,
				StreamConsumerConfig: &types.StreamConsumerConfig{
					Stream: types.StreamConfig{Name: "SHARED"},
					Fetch:  types.FetchConfig{BatchSize: 1, Timeout: 10 * time.Millisecond},
				},
				StreamConsumerHandler: func(context.Context, []*types.Msg) error { return nil },
			}
		}
		// Registered against the name order, on purpose.
		for _, module := range []*mockModuleWithStreamConsumer{
			{mockModule: mockModule{name: "zulu"}, serviceEntries: []*types.ServiceEntry{service("only")}},
			{mockModule: mockModule{name: "alpha"}, serviceEntries: []*types.ServiceEntry{service("second"), service("first")}},
		} {
			if err := reg.Register(module); err != nil {
				t.Fatalf("Register: %v", err)
			}
		}
		if err := lm.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		union := []string{
			"services.alpha.first", "services.alpha.first.>",
			"services.alpha.second", "services.alpha.second.>",
			"services.zulu.only", "services.zulu.only.>",
		}
		if len(es.streams) != 3 {
			t.Fatalf("stream writes = %d, want 3", len(es.streams))
		}
		for i, write := range es.streams {
			if !slices.Equal(write.Subjects, union) {
				t.Errorf("attempt %d: write %d has subjects %v, want %v", attempt, i, write.Subjects, union)
			}
		}
		var order []string
		for _, consumer := range es.consumers {
			order = append(order, consumer.Name)
		}
		if want := []string{"alpha-first", "alpha-second", "zulu-only"}; !slices.Equal(order, want) {
			t.Errorf("attempt %d: consumers set up in order %v, want %v", attempt, order, want)
		}
		if err := lm.Stop(context.Background()); err != nil {
			t.Fatalf("Stop: %v", err)
		}
	}
}

func TestKeepRegistrationFilter(t *testing.T) {
	own := []string{"services.orders.ingest", "services.orders.ingest.>"}
	tests := []struct {
		name         string
		cfg          types.ConsumerConfig
		existing     jetstream.ConsumerConfig
		own          []string
		wantSubject  string
		wantSubjects []string
	}{
		{name: "the registration's own filters are kept", own: own,
			existing:     jetstream.ConsumerConfig{FilterSubjects: []string{"services.orders.ingest.>", "services.orders.ingest"}},
			wantSubjects: own},
		{name: "a single own filter is kept", own: []string{"events.a"},
			existing: jetstream.ConsumerConfig{FilterSubject: "events.a"}, wantSubject: "events.a"},
		{name: "an unfiltered consumer stays unfiltered", own: own},
		{name: "another filter is not kept", own: own, existing: jetstream.ConsumerConfig{FilterSubject: "services.orders.ingest"}},
		{name: "an unmarked filter on other subjects was the caller's and is not kept", own: []string{"events.b"},
			existing: jetstream.ConsumerConfig{FilterSubject: "events.a"}},
		{name: "a marked filter follows the registration's new subjects", own: []string{"events.a", "events.c"},
			existing: jetstream.ConsumerConfig{FilterSubject: "events.a",
				Metadata: map[string]string{FilterMetadataKey: FilterMetadataValue}},
			wantSubjects: []string{"events.a", "events.c"}},
		{name: "a marker without a filter keeps nothing", own: []string{"events.a"},
			existing: jetstream.ConsumerConfig{Metadata: map[string]string{FilterMetadataKey: FilterMetadataValue}}},
		{name: "the configured filter wins", own: []string{"events.a"}, cfg: types.ConsumerConfig{FilterSubject: "caller"},
			existing: jetstream.ConsumerConfig{FilterSubject: "events.a"}, wantSubject: "caller"},
		{name: "a registration without subjects", existing: jetstream.ConsumerConfig{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.cfg.MaxDeliver = 7
			got := KeepRegistrationFilter(tt.cfg, tt.existing, tt.own)
			if got.FilterSubject != tt.wantSubject || !slices.Equal(got.FilterSubjects, tt.wantSubjects) {
				t.Errorf("filters = %q / %v, want %q / %v", got.FilterSubject, got.FilterSubjects, tt.wantSubject, tt.wantSubjects)
			}
			if got.MaxDeliver != 7 {
				t.Error("the rest of the configuration must be kept")
			}
			kept := hasSubjectFilter(got) && !hasSubjectFilter(tt.cfg)
			if marked := got.Metadata[FilterMetadataKey] == FilterMetadataValue; marked != kept {
				t.Errorf("filter marker = %v, want %v", marked, kept)
			}
		})
	}
}

func TestKeepExistingFilter(t *testing.T) {
	own := []string{"events.a"}
	cfg := types.ConsumerConfig{Name: "billing-a", Durable: "billing-a"}
	filtered := func() *planTestStream {
		return &planTestStream{consumers: map[string]*jetstream.ConsumerInfo{
			"billing-a": {Config: jetstream.ConsumerConfig{Durable: "billing-a", FilterSubject: "events.a"}},
		}}
	}

	t.Run("keeps the filter of the existing consumer", func(t *testing.T) {
		got, err := KeepExistingFilter(context.Background(), filtered(), own, cfg)
		if err != nil || got.FilterSubject != "events.a" {
			t.Fatalf("got filter %q, error %v", got.FilterSubject, err)
		}
	})

	t.Run("makes no request without a stream handle or with a configured filter", func(t *testing.T) {
		if got, err := KeepExistingFilter(context.Background(), nil, own, cfg); err != nil || hasSubjectFilter(got) {
			t.Fatalf("nil stream: filter %q, error %v", got.FilterSubject, err)
		}
		stream := filtered()
		configured := cfg
		configured.FilterSubject = "caller"
		got, err := KeepExistingFilter(context.Background(), stream, own, configured)
		if err != nil || got.FilterSubject != "caller" || len(stream.lookups) != 0 {
			t.Fatalf("configured filter: filter %q, error %v, lookups %v", got.FilterSubject, err, stream.lookups)
		}
	})

	t.Run("a consumer that does not exist or has no info leaves the configuration unchanged", func(t *testing.T) {
		for name, stream := range map[string]*planTestStream{
			"missing": {consumers: map[string]*jetstream.ConsumerInfo{}},
			"no info": {consumers: map[string]*jetstream.ConsumerInfo{"billing-a": nil}},
		} {
			if got, err := KeepExistingFilter(context.Background(), stream, own, cfg); err != nil || hasSubjectFilter(got) {
				t.Errorf("%s: filter %q, error %v", name, got.FilterSubject, err)
			}
		}
	})

	t.Run("a failed lookup is returned", func(t *testing.T) {
		boom := errors.New("no responders")
		_, err := KeepExistingFilter(context.Background(), &planTestStream{lookupErr: boom}, own, cfg)
		if !errors.Is(err, boom) {
			t.Fatalf("error = %v, want %v", err, boom)
		}
	})
}

// Both setup paths keep the registration's own filter on a consumer that has
// it, once its stream is no longer shared.
func TestSetupStreamConsumers_KeepFilterAfterSharing(t *testing.T) {
	t.Run("stream consumer service", func(t *testing.T) {
		store := &streamConsumerStore{newFakeCronStore()}
		store.consumer = &jetstream.ConsumerInfo{Config: jetstream.ConsumerConfig{
			Name: "orders-ingest", Durable: "orders-ingest",
			FilterSubjects: []string{"services.orders.ingest", "services.orders.ingest.>"},
		}}
		lm, _ := newReadyTestManager(store)
		entry := planService("orders", "ingest", "ORDERS", "services.orders.ingest", "services.orders.ingest.>")
		cfg := types.ConsumerConfig{Name: "orders-ingest", Durable: "orders-ingest", AckPolicy: types.AckExplicitPolicy}
		if _, err := lm.ensureStreamConsumer(context.Background(), store, "ORDERS", cfg, entry); err != nil {
			t.Fatalf("ensureStreamConsumer: %v", err)
		}
		if len(store.configs) != 1 || !slices.Equal(store.configs[0].FilterSubjects, []string{"services.orders.ingest", "services.orders.ingest.>"}) {
			t.Errorf("consumer configurations sent = %+v, want the filters kept", store.configs)
		}
	})

	t.Run("stream consumer service converted from a non-durable consumer", func(t *testing.T) {
		store := &streamConsumerStore{newFakeCronStore()}
		store.consumer = &jetstream.ConsumerInfo{Config: jetstream.ConsumerConfig{
			Name:           "orders-ingest",
			FilterSubjects: []string{"services.orders.ingest", "services.orders.ingest.>"},
		}}
		lm, _ := newReadyTestManager(store)
		entry := planService("orders", "ingest", "ORDERS", "services.orders.ingest", "services.orders.ingest.>")
		cfg := types.ConsumerConfig{Name: "orders-ingest", Durable: "orders-ingest", AckPolicy: types.AckExplicitPolicy}
		if _, err := lm.ensureStreamConsumer(context.Background(), store, "ORDERS", cfg, entry); err != nil {
			t.Fatalf("ensureStreamConsumer: %v", err)
		}
		if len(store.configs) != 2 {
			t.Fatalf("consumer configurations sent = %+v, want the two updates of a conversion", store.configs)
		}
		for i, sent := range store.configs {
			if !slices.Equal(sent.FilterSubjects, []string{"services.orders.ingest", "services.orders.ingest.>"}) ||
				sent.Metadata[FilterMetadataKey] != FilterMetadataValue {
				t.Errorf("update %d = filters %v, metadata %v; want the filters kept and marked", i, sent.FilterSubjects, sent.Metadata)
			}
		}
	})

	t.Run("event stream consumer", func(t *testing.T) {
		entry := streamEntry("billing", "EVENTS", paymentProcessedV1)
		durable := "billing-billing-PaymentProcessed-v1"
		es := &recordingEventStream{stream: &planTestStream{consumers: map[string]*jetstream.ConsumerInfo{
			durable: {Config: jetstream.ConsumerConfig{Durable: durable, FilterSubject: paymentProcessedV1.Subject}},
		}}}
		lm, _ := newMigrationTestManager(es)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		lm.runtimeCtx = ctx
		if err := lm.setupEventStreamConsumer(ctx, entry, eventStreamDurable{name: durable}); err != nil {
			t.Fatalf("setupEventStreamConsumer: %v", err)
		}
		for _, stop := range lm.streamConsumers {
			stop()
		}
		if len(es.consumers) != 1 || es.consumers[0].FilterSubject != paymentProcessedV1.Subject {
			t.Errorf("consumer configurations sent = %+v, want the filter kept", es.consumers)
		}
	})

	t.Run("event stream consumer, failed lookup", func(t *testing.T) {
		es := &recordingEventStream{stream: &planTestStream{lookupErr: errors.New("no responders")}}
		lm, _ := newMigrationTestManager(es)
		err := lm.setupEventStreamConsumer(context.Background(), streamEntry("billing", "EVENTS", paymentProcessedV1),
			eventStreamDurable{name: "billing-billing-PaymentProcessed-v1"})
		if err == nil || !strings.Contains(err.Error(), "failed to look up consumer") {
			t.Fatalf("error = %v, want the lookup failure", err)
		}
		if len(es.consumers) != 0 {
			t.Error("the consumer must not be written without knowing its filter")
		}
	})
}
