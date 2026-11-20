//go:build integration
// +build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/go-monolith/mono"
	"github.com/go-monolith/mono/pkg/helper"
	"github.com/go-monolith/mono/pkg/types"
)

// sharedAlpha and sharedBeta are two events of one module, with different
// subjects, that the tests register on one stream.
var (
	sharedAlpha = helper.EventDefinition[OrderEventPayload]("shared-emitter", "AlphaHappened", "v1")
	sharedBeta  = helper.EventDefinition[OrderEventPayload]("shared-emitter", "BetaHappened", "v1")
)

// sharedLog records the warnings an application logs.
type sharedLog struct {
	mu    sync.Mutex
	warns []string
}

func (l *sharedLog) Debug(string, ...any) {}
func (l *sharedLog) Info(string, ...any)  {}
func (l *sharedLog) Error(string, ...any) {}
func (l *sharedLog) Warn(msg string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warns = append(l.warns, msg+" "+fmt.Sprint(args...))
}
func (l *sharedLog) With(...any) mono.Logger       { return l }
func (l *sharedLog) WithModule(string) mono.Logger { return l }
func (l *sharedLog) WithError(error) mono.Logger   { return l }

// leftBehind returns the recorded warnings about leftover streams and
// consumers.
func (l *sharedLog) leftBehind() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var found []string
	for _, w := range l.warns {
		if strings.Contains(w, "left behind by a stream registration") {
			found = append(found, w)
		}
	}
	return found
}

// sharedReg is one registration of a sharedModule: a stream consumer service,
// or an event stream consumer when event is set. It records the subjects of
// the messages its handler receives.
type sharedReg struct {
	service string
	event   *mono.BaseEventDefinition
	stream  mono.StreamConfig

	mu       sync.Mutex
	subjects []string
}

func (r *sharedReg) handle(_ context.Context, msgs []*mono.Msg) error {
	for _, msg := range msgs {
		r.mu.Lock()
		r.subjects = append(r.subjects, msg.Subject)
		r.mu.Unlock()
		_ = msg.Ack()
	}
	return nil
}

// received returns the subjects of the messages handled so far.
func (r *sharedReg) received() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.subjects)
}

func (r *sharedReg) config() mono.StreamConsumerConfig {
	return mono.StreamConsumerConfig{Stream: r.stream, Fetch: mono.FetchConfig{BatchSize: 1, Timeout: time.Second}}
}

// sharedModule registers stream consumer services and event stream consumers.
type sharedModule struct {
	name string
	regs []*sharedReg
}

func (m *sharedModule) Name() string                { return m.name }
func (m *sharedModule) Start(context.Context) error { return nil }
func (m *sharedModule) Stop(context.Context) error  { return nil }

func (m *sharedModule) RegisterServices(c mono.ServiceContainer) error {
	for _, reg := range m.regs {
		if reg.event != nil {
			continue
		}
		if err := c.RegisterStreamConsumerService(reg.service, reg.config(), reg.handle); err != nil {
			return err
		}
	}
	return nil
}

func (m *sharedModule) RegisterEventConsumers(registry mono.EventRegistry) error {
	for _, reg := range m.regs {
		if reg.event == nil {
			continue
		}
		if err := registry.RegisterEventStreamConsumer(*reg.event, reg.config(), reg.handle, m); err != nil {
			return err
		}
	}
	return nil
}

func sharedService(name, stream string, retention mono.RetentionPolicy) *sharedReg {
	return &sharedReg{service: name, stream: mono.StreamConfig{Name: stream, Retention: retention}}
}

func sharedEvent(def mono.BaseEventDefinition, stream string, retention mono.RetentionPolicy) *sharedReg {
	return &sharedReg{event: &def, stream: mono.StreamConfig{Name: stream, Retention: retention}}
}

// startShared starts an application on the JetStream store in dir with the
// modules, and stops it when the test ends or the returned function is
// called.
func startShared(t *testing.T, dir string, log mono.Logger, modules ...*sharedModule) (mono.MonoApplication, func()) {
	t.Helper()
	app, err := mono.NewMonoApplication(
		mono.WithCustomLogger(log),
		mono.WithJetStreamStorageDir(dir),
		mono.WithNATSDontListen(),
		mono.WithNATSInProcessConn(),
	)
	if err != nil {
		t.Fatalf("NewMonoApplication: %v", err)
	}
	for _, m := range modules {
		if err := app.Register(m); err != nil {
			t.Fatalf("Register %s: %v", m.name, err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := app.Start(ctx); err != nil {
		_ = app.Stop(context.Background())
		t.Fatalf("Start: %v", err)
	}
	var once sync.Once
	stop := func() { once.Do(func() { _ = app.Stop(context.Background()) }) }
	t.Cleanup(stop)
	return app, stop
}

// sharedStream returns the named stream of the application.
func sharedStream(t *testing.T, app mono.MonoApplication, module, name string) jetstream.Stream {
	t.Helper()
	es, err := app.EventBus(module).EventStream()
	if err != nil {
		t.Fatalf("EventStream: %v", err)
	}
	stream, err := es.Stream(context.Background(), name)
	if err != nil {
		t.Fatalf("Stream %s: %v", name, err)
	}
	return stream
}

// requireReceived waits until the registration handled want messages, gives
// stray deliveries a moment to arrive, and checks that every message handled
// had one of the registration's own subjects.
func requireReceived(t *testing.T, what string, reg *sharedReg, want int, ownSubjects ...string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for len(reg.received()) < want && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond)
	got := reg.received()
	if len(got) != want {
		t.Errorf("%s handled %d messages %v, want %d", what, len(got), got, want)
	}
	for _, subject := range got {
		if !slices.Contains(ownSubjects, subject) {
			t.Errorf("%s handled a message published to %q, which is not one of its subjects %v", what, subject, ownSubjects)
		}
	}
}

// consumerFilters returns the subject filters of a consumer.
func consumerFilters(t *testing.T, stream jetstream.Stream, consumer string) []string {
	t.Helper()
	c, err := stream.Consumer(context.Background(), consumer)
	if err != nil {
		t.Fatalf("Consumer %s: %v", consumer, err)
	}
	cfg := c.CachedInfo().Config
	filters := slices.Clone(cfg.FilterSubjects)
	if cfg.FilterSubject != "" {
		filters = append(filters, cfg.FilterSubject)
	}
	return filters
}

// TestIntegration_SharedStream_TwoServices: two stream consumer services that
// name the same stream are both stored and each handler receives only what
// was published to its own service, on every start.
func TestIntegration_SharedStream_TwoServices(t *testing.T) {
	ctx := context.Background()
	// Four new deployments, then a restart of the last one on its store.
	dir := ""
	for attempt := 1; attempt <= 5; attempt++ {
		if attempt < 5 {
			dir = t.TempDir()
		}
		first := sharedService("first", "SHARED", mono.LimitsPolicy)
		second := sharedService("second", "SHARED", mono.LimitsPolicy)
		app, stop := startShared(t, dir, &noOpsLogger{}, &sharedModule{name: "orders", regs: []*sharedReg{first, second}})

		for i := 0; i < 2; i++ {
			if _, err := helper.PublishStreamConsumerService(ctx, app.Services("orders"), "first", json.Marshal, i); err != nil {
				t.Fatalf("attempt %d: publish to first: %v", attempt, err)
			}
		}
		for i := 0; i < 3; i++ {
			if _, err := helper.PublishStreamConsumerService(ctx, app.Services("orders"), "second", json.Marshal, i); err != nil {
				t.Fatalf("attempt %d: publish to second: %v", attempt, err)
			}
		}
		requireReceived(t, "first", first, 2, "services.orders.first")
		requireReceived(t, "second", second, 3, "services.orders.second")

		subjects := sharedStream(t, app, "orders", "SHARED").CachedInfo().Config.Subjects
		for _, want := range []string{"services.orders.first", "services.orders.first.>", "services.orders.second", "services.orders.second.>"} {
			if !slices.Contains(subjects, want) {
				t.Errorf("attempt %d: stream subjects %v do not contain %s", attempt, subjects, want)
			}
		}
		stop()
	}
}

// TestIntegration_SharedStream_TwoEvents: two event stream consumers for
// different events on one stream. Both events are stored, whether published
// with or without acknowledgement, and each handler receives only its own.
func TestIntegration_SharedStream_TwoEvents(t *testing.T) {
	ctx := context.Background()
	for _, tt := range []struct {
		name      string
		retention mono.RetentionPolicy
	}{{"limits", mono.LimitsPolicy}, {"interest", mono.InterestPolicy}, {"work-queue", mono.WorkQueuePolicy}} {
		t.Run(tt.name, func(t *testing.T) {
			alpha := sharedEvent(sharedAlpha.ToBase(), "EVENTS", tt.retention)
			beta := sharedEvent(sharedBeta.ToBase(), "EVENTS", tt.retention)
			app, _ := startShared(t, t.TempDir(), &noOpsLogger{}, &sharedModule{name: "billing", regs: []*sharedReg{alpha, beta}})
			bus := app.EventBus("billing")

			if err := sharedAlpha.Publish(bus, OrderEventPayload{OrderID: "a"}, nil); err != nil {
				t.Fatalf("Publish alpha: %v", err)
			}
			for i := 0; i < 2; i++ {
				if _, err := sharedAlpha.EventStreamPublish(ctx, bus, OrderEventPayload{OrderID: "a"}, nil); err != nil {
					t.Fatalf("EventStreamPublish alpha: %v", err)
				}
			}
			if err := sharedBeta.Publish(bus, OrderEventPayload{OrderID: "b"}, nil); err != nil {
				t.Fatalf("Publish beta: %v", err)
			}
			if _, err := sharedBeta.EventStreamPublish(ctx, bus, OrderEventPayload{OrderID: "b"}, nil); err != nil {
				t.Fatalf("EventStreamPublish beta: %v", err)
			}
			requireReceived(t, "alpha", alpha, 3, sharedAlpha.Subject)
			requireReceived(t, "beta", beta, 2, sharedBeta.Subject)
		})
	}
}

// TestIntegration_SharedStream_ServiceAndEvent: a stream consumer service and
// an event stream consumer on one stream.
func TestIntegration_SharedStream_ServiceAndEvent(t *testing.T) {
	ctx := context.Background()
	service := sharedService("ingest", "MIXED", mono.LimitsPolicy)
	event := sharedEvent(sharedAlpha.ToBase(), "MIXED", mono.LimitsPolicy)
	app, _ := startShared(t, t.TempDir(), &noOpsLogger{}, &sharedModule{name: "orders", regs: []*sharedReg{service, event}})

	for i := 0; i < 2; i++ {
		if _, err := helper.PublishStreamConsumerService(ctx, app.Services("orders"), "ingest", json.Marshal, i); err != nil {
			t.Fatalf("publish to the service: %v", err)
		}
	}
	if _, err := sharedAlpha.EventStreamPublish(ctx, app.EventBus("orders"), OrderEventPayload{}, nil); err != nil {
		t.Fatalf("publish the event: %v", err)
	}
	requireReceived(t, "service", service, 2, "services.orders.ingest")
	requireReceived(t, "event", event, 1, sharedAlpha.Subject)
}

// TestIntegration_SharedStream_CoveringSubjects: two stream consumer services
// with custom subjects of which one covers the other. The stream is written
// with the covering subject, and a message on the narrower subject reaches
// both handlers, each through its own filter.
func TestIntegration_SharedStream_CoveringSubjects(t *testing.T) {
	ctx := context.Background()
	all := &sharedReg{service: "all", stream: mono.StreamConfig{Name: "TASKS", Subjects: []string{"tasks.>"}}}
	urgent := &sharedReg{service: "urgent", stream: mono.StreamConfig{Name: "TASKS", Subjects: []string{"tasks.urgent"}}}
	app, _ := startShared(t, t.TempDir(), &noOpsLogger{}, &sharedModule{name: "work", regs: []*sharedReg{all, urgent}})

	es, err := app.EventBus("work").EventStream()
	if err != nil {
		t.Fatalf("EventStream: %v", err)
	}
	for _, subject := range []string{"tasks.urgent", "tasks.normal", "tasks.normal"} {
		if _, err := es.Publish(ctx, subject, []byte("x")); err != nil {
			t.Fatalf("publish to %s: %v", subject, err)
		}
	}
	requireReceived(t, "all", all, 3, "tasks.urgent", "tasks.normal")
	requireReceived(t, "urgent", urgent, 1, "tasks.urgent")
	if subjects := sharedStream(t, app, "work", "TASKS").CachedInfo().Config.Subjects; !slices.Equal(subjects, []string{"tasks.>"}) {
		t.Errorf("stream subjects = %v, want the covering subject only", subjects)
	}
}

// TestIntegration_SharedStream_SameSubjectsUnchanged: registrations with the
// same subjects on one stream, and a registration with a stream of its own,
// get no filter: their consumers are sent as before.
func TestIntegration_SharedStream_SameSubjectsUnchanged(t *testing.T) {
	ctx := context.Background()
	billing := sharedEvent(sharedAlpha.ToBase(), "EVENTS", mono.InterestPolicy)
	shipping := sharedEvent(sharedAlpha.ToBase(), "EVENTS", mono.InterestPolicy)
	alone := sharedService("ingest", "ORDERS", mono.LimitsPolicy)
	app, _ := startShared(t, t.TempDir(), &noOpsLogger{},
		&sharedModule{name: "billing", regs: []*sharedReg{billing, alone}},
		&sharedModule{name: "shipping", regs: []*sharedReg{shipping}})

	for i := 0; i < 3; i++ {
		if _, err := sharedAlpha.EventStreamPublish(ctx, app.EventBus("billing"), OrderEventPayload{}, nil); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	requireReceived(t, "billing", billing, 3, sharedAlpha.Subject)
	requireReceived(t, "shipping", shipping, 3, sharedAlpha.Subject)

	events := sharedStream(t, app, "billing", "EVENTS")
	for _, module := range []string{"billing", "shipping"} {
		consumer := types.EventStreamConsumerName(module, sharedAlpha.ToBase())
		if filters := consumerFilters(t, events, consumer); len(filters) != 0 {
			t.Errorf("consumer %s has filters %v, want none", consumer, filters)
		}
	}
	if filters := consumerFilters(t, sharedStream(t, app, "billing", "ORDERS"), "billing-ingest"); len(filters) != 0 {
		t.Errorf("consumer billing-ingest has filters %v, want none", filters)
	}
}

// TestIntegration_SharedStream_UpgradeFromOverwrittenSubjects: the state an
// earlier version leaves for two events on one stream (the stream listens on
// the last event only, and both durables are unfiltered and have a position)
// is repaired in place. The durables keep their position: nothing they
// acknowledged is delivered again.
func TestIntegration_SharedStream_UpgradeFromOverwrittenSubjects(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	alphaDurable := types.EventStreamConsumerName("billing", sharedAlpha.ToBase())
	betaDurable := types.EventStreamConsumerName("billing", sharedBeta.ToBase())

	// The earlier state, built by hand: three Beta events stored, of which
	// each durable acknowledged the first two.
	app, stop := startShared(t, dir, &noOpsLogger{}, &sharedModule{name: "billing"})
	es, err := app.EventBus("billing").EventStream()
	if err != nil {
		t.Fatalf("EventStream: %v", err)
	}
	if _, err := es.CreateOrUpdateStream(ctx, mono.StreamConfig{Name: "EVENTS", Subjects: []string{sharedBeta.Subject}}); err != nil {
		t.Fatalf("create the stream: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := es.Publish(ctx, sharedBeta.Subject, []byte(`{}`)); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	for _, durable := range []string{alphaDurable, betaDurable} {
		consumer, err := es.CreateOrUpdateConsumer(ctx, "EVENTS", mono.ConsumerConfig{Name: durable, Durable: durable, AckPolicy: mono.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("create %s: %v", durable, err)
		}
		batch, err := consumer.Fetch(2, jetstream.FetchMaxWait(2*time.Second))
		if err != nil {
			t.Fatalf("fetch from %s: %v", durable, err)
		}
		for msg := range batch.Messages() {
			if err := msg.DoubleAck(ctx); err != nil {
				t.Fatalf("ack on %s: %v", durable, err)
			}
		}
	}
	stop()

	alpha := sharedEvent(sharedAlpha.ToBase(), "EVENTS", mono.LimitsPolicy)
	beta := sharedEvent(sharedBeta.ToBase(), "EVENTS", mono.LimitsPolicy)
	app, _ = startShared(t, dir, &noOpsLogger{}, &sharedModule{name: "billing", regs: []*sharedReg{alpha, beta}})
	bus := app.EventBus("billing")
	for i := 0; i < 2; i++ {
		if _, err := sharedAlpha.EventStreamPublish(ctx, bus, OrderEventPayload{}, nil); err != nil {
			t.Fatalf("publish alpha: %v", err)
		}
	}
	if _, err := sharedBeta.EventStreamPublish(ctx, bus, OrderEventPayload{}, nil); err != nil {
		t.Fatalf("publish beta: %v", err)
	}
	// Alpha: the two new Alpha events, and not the Beta event it had not
	// acknowledged. Beta: that third Beta event and the new one, and not the
	// two it had acknowledged.
	requireReceived(t, "alpha", alpha, 2, sharedAlpha.Subject)
	requireReceived(t, "beta", beta, 2, sharedBeta.Subject)

	stream := sharedStream(t, app, "billing", "EVENTS")
	if got := consumerFilters(t, stream, alphaDurable); !slices.Equal(got, []string{sharedAlpha.Subject}) {
		t.Errorf("filters of %s = %v, want %v", alphaDurable, got, []string{sharedAlpha.Subject})
	}
	if got := consumerFilters(t, stream, betaDurable); !slices.Equal(got, []string{sharedBeta.Subject}) {
		t.Errorf("filters of %s = %v, want %v", betaDurable, got, []string{sharedBeta.Subject})
	}
}

// TestIntegration_SharedStream_UpgradeFromNonDurableServiceConsumers: the
// state versions up to v0.1.x leave for two stream consumer services on one
// stream: the stream listens on one service only, and both consumers are
// non-durable (a Name only), unfiltered and have a position. They are
// converted to durables and filtered in place, on a limits and on an interest
// stream, and nothing they acknowledged is delivered again.
func TestIntegration_SharedStream_UpgradeFromNonDurableServiceConsumers(t *testing.T) {
	ctx := context.Background()
	for _, tt := range []struct {
		name      string
		retention mono.RetentionPolicy
	}{{"limits", mono.LimitsPolicy}, {"interest", mono.InterestPolicy}} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			app, stop := startShared(t, dir, &noOpsLogger{}, &sharedModule{name: "orders"})
			es, err := app.EventBus("orders").EventStream()
			if err != nil {
				t.Fatalf("EventStream: %v", err)
			}
			if _, err := es.CreateOrUpdateStream(ctx, mono.StreamConfig{
				Name: "SHARED", Retention: tt.retention,
				Subjects: []string{"services.orders.second", "services.orders.second.>"},
			}); err != nil {
				t.Fatalf("create the stream: %v", err)
			}
			consumers := make(map[string]jetstream.Consumer)
			for _, name := range []string{"orders-first", "orders-second"} {
				consumer, err := es.CreateOrUpdateConsumer(ctx, "SHARED", mono.ConsumerConfig{
					Name: name, AckPolicy: mono.AckExplicitPolicy, InactiveThreshold: time.Hour,
				})
				if err != nil {
					t.Fatalf("create %s: %v", name, err)
				}
				if consumer.CachedInfo().Config.Durable != "" {
					t.Fatalf("%s must start as a non-durable consumer", name)
				}
				consumers[name] = consumer
			}
			for i := 0; i < 3; i++ {
				if _, err := es.Publish(ctx, "services.orders.second", []byte("1")); err != nil {
					t.Fatalf("publish: %v", err)
				}
			}
			for name, consumer := range consumers {
				batch, err := consumer.Fetch(2, jetstream.FetchMaxWait(2*time.Second))
				if err != nil {
					t.Fatalf("fetch from %s: %v", name, err)
				}
				for msg := range batch.Messages() {
					if err := msg.DoubleAck(ctx); err != nil {
						t.Fatalf("ack on %s: %v", name, err)
					}
				}
			}
			stop()

			first := sharedService("first", "SHARED", tt.retention)
			second := sharedService("second", "SHARED", tt.retention)
			app, _ = startShared(t, dir, &noOpsLogger{}, &sharedModule{name: "orders", regs: []*sharedReg{first, second}})
			for i := 0; i < 2; i++ {
				if _, err := helper.PublishStreamConsumerService(ctx, app.Services("orders"), "first", json.Marshal, i); err != nil {
					t.Fatalf("publish to first: %v", err)
				}
			}
			if _, err := helper.PublishStreamConsumerService(ctx, app.Services("orders"), "second", json.Marshal, 1); err != nil {
				t.Fatalf("publish to second: %v", err)
			}
			// first: its two new messages, not the message of second it had
			// not acknowledged. second: that third message and the new one.
			requireReceived(t, "first", first, 2, "services.orders.first")
			requireReceived(t, "second", second, 2, "services.orders.second")

			stream := sharedStream(t, app, "orders", "SHARED")
			for _, service := range []string{"first", "second"} {
				name := "orders-" + service
				consumer, err := stream.Consumer(ctx, name)
				if err != nil {
					t.Fatalf("Consumer %s: %v", name, err)
				}
				if got := consumer.CachedInfo().Config.Durable; got != name {
					t.Errorf("%s: Durable = %q, want it converted to a durable", name, got)
				}
				want := []string{"services.orders." + service, "services.orders." + service + ".>"}
				if got := consumerFilters(t, stream, name); !slices.Equal(got, want) {
					t.Errorf("filters of %s = %v, want %v", name, got, want)
				}
			}
		})
	}
}

// TestIntegration_SharedStream_SecondEventAddedToWorkQueue: a second event is
// added to a work-queue stream whose first event already has an unfiltered
// durable, in either registration order.
func TestIntegration_SharedStream_SecondEventAddedToWorkQueue(t *testing.T) {
	ctx := context.Background()
	for _, tt := range []struct {
		name       string
		alphaFirst bool
	}{{"existing event registered first", true}, {"new event registered first", false}} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			alpha := sharedEvent(sharedAlpha.ToBase(), "TASKS", mono.WorkQueuePolicy)
			_, stop := startShared(t, dir, &noOpsLogger{}, &sharedModule{name: "billing", regs: []*sharedReg{alpha}})
			stop()

			alpha = sharedEvent(sharedAlpha.ToBase(), "TASKS", mono.WorkQueuePolicy)
			beta := sharedEvent(sharedBeta.ToBase(), "TASKS", mono.WorkQueuePolicy)
			regs := []*sharedReg{alpha, beta}
			if !tt.alphaFirst {
				regs = []*sharedReg{beta, alpha}
			}
			app, _ := startShared(t, dir, &noOpsLogger{}, &sharedModule{name: "billing", regs: regs})
			bus := app.EventBus("billing")
			if _, err := sharedAlpha.EventStreamPublish(ctx, bus, OrderEventPayload{}, nil); err != nil {
				t.Fatalf("publish alpha: %v", err)
			}
			if _, err := sharedBeta.EventStreamPublish(ctx, bus, OrderEventPayload{}, nil); err != nil {
				t.Fatalf("publish beta: %v", err)
			}
			requireReceived(t, "alpha", alpha, 1, sharedAlpha.Subject)
			requireReceived(t, "beta", beta, 1, sharedBeta.Subject)
		})
	}
}

// TestIntegration_SharedStream_WorkQueueServices: two stream consumer
// services, each with two subjects, on one work-queue stream: on a new
// deployment, after a restart, and when the second service is added to a
// stream whose first service already has an unfiltered durable.
func TestIntegration_SharedStream_WorkQueueServices(t *testing.T) {
	ctx := context.Background()
	publishAndCheck := func(t *testing.T, app mono.MonoApplication, first, second *sharedReg) {
		t.Helper()
		if _, err := helper.PublishStreamConsumerService(ctx, app.Services("orders"), "first", json.Marshal, 1); err != nil {
			t.Fatalf("publish to first: %v", err)
		}
		for i := 0; i < 2; i++ {
			if _, err := helper.PublishStreamConsumerService(ctx, app.Services("orders"), "second", json.Marshal, i); err != nil {
				t.Fatalf("publish to second: %v", err)
			}
		}
		requireReceived(t, "first", first, 1, "services.orders.first")
		requireReceived(t, "second", second, 2, "services.orders.second")
	}

	t.Run("new deployment, then a restart", func(t *testing.T) {
		dir := t.TempDir()
		for start := 1; start <= 2; start++ {
			first := sharedService("first", "TASKS", mono.WorkQueuePolicy)
			second := sharedService("second", "TASKS", mono.WorkQueuePolicy)
			app, stop := startShared(t, dir, &noOpsLogger{}, &sharedModule{name: "orders", regs: []*sharedReg{first, second}})
			publishAndCheck(t, app, first, second)
			stop()
		}
	})

	for _, existing := range []string{"first", "second"} {
		t.Run("a service added beside "+existing, func(t *testing.T) {
			dir := t.TempDir()
			_, stop := startShared(t, dir, &noOpsLogger{}, &sharedModule{name: "orders", regs: []*sharedReg{sharedService(existing, "TASKS", mono.WorkQueuePolicy)}})
			stop()

			first := sharedService("first", "TASKS", mono.WorkQueuePolicy)
			second := sharedService("second", "TASKS", mono.WorkQueuePolicy)
			app, _ := startShared(t, dir, &noOpsLogger{}, &sharedModule{name: "orders", regs: []*sharedReg{first, second}})
			publishAndCheck(t, app, first, second)
		})
	}
}

// TestIntegration_SharedStream_WorkQueueSharingEnds: one of two events on a
// work-queue stream is no longer registered. Its filtered durable is still
// there; the remaining consumer starts and keeps consuming.
func TestIntegration_SharedStream_WorkQueueSharingEnds(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	_, stop := startShared(t, dir, &noOpsLogger{}, &sharedModule{name: "billing", regs: []*sharedReg{
		sharedEvent(sharedAlpha.ToBase(), "TASKS", mono.WorkQueuePolicy),
		sharedEvent(sharedBeta.ToBase(), "TASKS", mono.WorkQueuePolicy),
	}})
	stop()

	for start := 1; start <= 2; start++ {
		alpha := sharedEvent(sharedAlpha.ToBase(), "TASKS", mono.WorkQueuePolicy)
		app, stop := startShared(t, dir, &noOpsLogger{}, &sharedModule{name: "billing", regs: []*sharedReg{alpha}})
		if _, err := sharedAlpha.EventStreamPublish(ctx, app.EventBus("billing"), OrderEventPayload{}, nil); err != nil {
			t.Fatalf("start %d: publish alpha: %v", start, err)
		}
		requireReceived(t, "alpha", alpha, 1, sharedAlpha.Subject)
		stop()
	}
}

// TestIntegration_Leftovers_RenamedConsumerModule: after the consuming module
// of an event stream consumer is renamed, the durable of the old name is
// reported once, with the command that removes it, and is not deleted.
func TestIntegration_Leftovers_RenamedConsumerModule(t *testing.T) {
	dir := t.TempDir()
	oldDurable := types.EventStreamConsumerName("billing", sharedAlpha.ToBase())

	first := &sharedLog{}
	_, stop := startShared(t, dir, first, &sharedModule{name: "billing", regs: []*sharedReg{sharedEvent(sharedAlpha.ToBase(), "EVENTS", mono.InterestPolicy)}})
	stop()
	if got := first.leftBehind(); len(got) != 0 {
		t.Fatalf("first start reported leftovers: %v", got)
	}

	second := &sharedLog{}
	app, _ := startShared(t, dir, second, &sharedModule{name: "invoicing", regs: []*sharedReg{sharedEvent(sharedAlpha.ToBase(), "EVENTS", mono.InterestPolicy)}})
	got := second.leftBehind()
	if len(got) != 1 {
		t.Fatalf("leftover warnings = %v, want one", got)
	}
	for _, part := range []string{"Consumer left behind", oldDurable, "nats consumer rm EVENTS " + oldDurable, "retains_messagestrue", "module billing"} {
		if !strings.Contains(got[0], part) {
			t.Errorf("warning %q does not contain %q", got[0], part)
		}
	}
	if _, err := sharedStream(t, app, "invoicing", "EVENTS").Consumer(context.Background(), oldDurable); err != nil {
		t.Errorf("the old durable %s must not be deleted: %v", oldDurable, err)
	}
}

// TestIntegration_Leftovers_RemovedRegistrations: a removed stream consumer
// service leaves its stream, and an event no longer registered on a shared
// stream leaves its durable. Both are reported and kept; the remaining
// consumer loses its filter again. What the application created itself is
// not reported.
func TestIntegration_Leftovers_RemovedRegistrations(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	alphaDurable := types.EventStreamConsumerName("billing", sharedAlpha.ToBase())
	betaDurable := types.EventStreamConsumerName("billing", sharedBeta.ToBase())

	app, stop := startShared(t, dir, &noOpsLogger{}, &sharedModule{name: "billing", regs: []*sharedReg{
		sharedEvent(sharedAlpha.ToBase(), "EVENTS", mono.LimitsPolicy),
		sharedEvent(sharedBeta.ToBase(), "EVENTS", mono.LimitsPolicy),
		sharedService("ingest", "ORDERS", mono.LimitsPolicy),
	}})
	// A stream and a consumer of the application's own.
	es, err := app.EventBus("billing").EventStream()
	if err != nil {
		t.Fatalf("EventStream: %v", err)
	}
	if _, err := es.CreateOrUpdateStream(ctx, mono.StreamConfig{Name: "APP_OWN", Subjects: []string{"app.own.>"}}); err != nil {
		t.Fatalf("create APP_OWN: %v", err)
	}
	if _, err := es.CreateOrUpdateConsumer(ctx, "EVENTS", mono.ConsumerConfig{Name: "app-own", Durable: "app-own", AckPolicy: mono.AckExplicitPolicy}); err != nil {
		t.Fatalf("create app-own: %v", err)
	}
	stop()

	log := &sharedLog{}
	app, stop = startShared(t, dir, log, &sharedModule{name: "billing", regs: []*sharedReg{sharedEvent(sharedAlpha.ToBase(), "EVENTS", mono.LimitsPolicy)}})
	got := log.leftBehind()
	if len(got) != 2 {
		t.Fatalf("leftover warnings = %v, want two", got)
	}
	joined := strings.Join(got, "\n")
	for _, part := range []string{
		"Consumer left behind", "nats consumer rm EVENTS " + betaDurable,
		"Stream left behind", "nats stream rm ORDERS", "stream consumer service billing.ingest",
	} {
		if !strings.Contains(joined, part) {
			t.Errorf("warnings %q do not contain %q", joined, part)
		}
	}
	for _, own := range []string{"APP_OWN", "app-own"} {
		if strings.Contains(joined, own) {
			t.Errorf("warnings %q report %s, which the application created itself", joined, own)
		}
	}
	events := sharedStream(t, app, "billing", "EVENTS")
	if _, err := events.Consumer(ctx, betaDurable); err != nil {
		t.Errorf("the durable %s must not be deleted: %v", betaDurable, err)
	}
	if info, err := sharedStream(t, app, "billing", "ORDERS").Info(ctx); err != nil || info == nil {
		t.Errorf("the stream ORDERS must not be deleted: %v", err)
	}
	if filters := consumerFilters(t, events, alphaDurable); len(filters) != 0 {
		t.Errorf("consumer %s still has filters %v after the stream is no longer shared", alphaDurable, filters)
	}
	stop()

	// The same registrations again: reported again, nothing new.
	again := &sharedLog{}
	startShared(t, dir, again, &sharedModule{name: "billing", regs: []*sharedReg{sharedEvent(sharedAlpha.ToBase(), "EVENTS", mono.LimitsPolicy)}})
	if got := again.leftBehind(); len(got) != 2 {
		t.Errorf("leftover warnings on the next start = %v, want the same two", got)
	}
}
