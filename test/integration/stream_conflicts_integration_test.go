//go:build integration
// +build integration

package integration_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/go-monolith/mono"
	monoerrors "github.com/go-monolith/mono/pkg/errors"
)

// conflictReg is one registration of a conflictModule: a stream consumer
// service, an event stream consumer when event is set, or a cron service
// when cron is set. It records the subjects of the messages it handles.
type conflictReg struct {
	service string
	event   *mono.BaseEventDefinition
	cron    bool
	config  mono.StreamConsumerConfig
	noAck   bool // leave handled messages unacknowledged

	mu       sync.Mutex
	subjects []string
}

func (r *conflictReg) handle(_ context.Context, msgs []*mono.Msg) error {
	for _, msg := range msgs {
		r.mu.Lock()
		r.subjects = append(r.subjects, msg.Subject)
		r.mu.Unlock()
		if !r.noAck {
			_ = msg.Ack()
		}
	}
	return nil
}

// waitHandled waits until the registration handled want messages and returns
// how many it handled.
func (r *conflictReg) waitHandled(want int) int {
	deadline := time.Now().Add(10 * time.Second)
	for {
		r.mu.Lock()
		n := len(r.subjects)
		r.mu.Unlock()
		if n >= want || time.Now().After(deadline) {
			return n
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// conflictModule registers the registrations it is given.
type conflictModule struct {
	name string
	regs []*conflictReg
}

func (m *conflictModule) Name() string                { return m.name }
func (m *conflictModule) Start(context.Context) error { return nil }
func (m *conflictModule) Stop(context.Context) error  { return nil }

func (m *conflictModule) RegisterServices(c mono.ServiceContainer) error {
	for _, reg := range m.regs {
		var err error
		switch {
		case reg.event != nil:
		case reg.cron:
			err = c.RegisterCronService(reg.service, mono.CronServiceConfig{Schedule: "@every 1h", Payload: []byte("tick")},
				func(context.Context, *mono.Msg) error { return nil })
		default:
			err = c.RegisterStreamConsumerService(reg.service, reg.config, reg.handle)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *conflictModule) RegisterEventConsumers(registry mono.EventRegistry) error {
	for _, reg := range m.regs {
		if reg.event == nil {
			continue
		}
		if err := registry.RegisterEventStreamConsumer(*reg.event, reg.config, reg.handle, m); err != nil {
			return err
		}
	}
	return nil
}

// conflictConfig returns a stream consumer configuration on the named stream.
func conflictConfig(stream string, retention mono.RetentionPolicy, subjects ...string) mono.StreamConsumerConfig {
	return mono.StreamConsumerConfig{
		Stream: mono.StreamConfig{Name: stream, Retention: retention, Subjects: subjects},
		Fetch:  mono.FetchConfig{BatchSize: 10, Timeout: time.Second},
	}
}

func conflictService(name string, config mono.StreamConsumerConfig) *conflictReg {
	return &conflictReg{service: name, config: config}
}

func conflictEvent(def mono.BaseEventDefinition, config mono.StreamConsumerConfig) *conflictReg {
	return &conflictReg{event: &def, config: config}
}

// startConflict starts an application with the modules on the JetStream store
// in dir. It returns the application, a function that stops it (also called
// when the test ends) and the error of Start; an application that failed to
// start is stopped already.
func startConflict(t *testing.T, dir string, modules ...*conflictModule) (mono.MonoApplication, func(), error) {
	t.Helper()
	return startConflictLogged(t, dir, &noOpsLogger{}, modules...)
}

// startConflictLogged is startConflict with the application's logger.
func startConflictLogged(t *testing.T, dir string, log mono.Logger, modules ...*conflictModule) (mono.MonoApplication, func(), error) {
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
	startErr := app.Start(ctx)
	var once sync.Once
	stop := func() { once.Do(func() { _ = app.Stop(context.Background()) }) }
	if startErr != nil {
		stop()
	}
	t.Cleanup(stop)
	return app, stop, startErr
}

// mustStartConflict is startConflict for a start that has to succeed.
func mustStartConflict(t *testing.T, dir string, modules ...*conflictModule) (mono.MonoApplication, func()) {
	t.Helper()
	app, stop, err := startConflict(t, dir, modules...)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	return app, stop
}

// conflictEventStream returns the JetStream of a started application.
func conflictEventStream(t *testing.T, app mono.MonoApplication) mono.EventStream {
	t.Helper()
	es, err := app.EventBus("operator").EventStream()
	if err != nil {
		t.Fatalf("EventStream: %v", err)
	}
	return es
}

// conflictStream returns the named stream of a started application.
func conflictStream(t *testing.T, app mono.MonoApplication, name string) jetstream.Stream {
	t.Helper()
	stream, err := conflictEventStream(t, app).Stream(context.Background(), name)
	if err != nil {
		t.Fatalf("Stream %s: %v", name, err)
	}
	return stream
}

// conflictConsumer returns a consumer as the server holds it.
func conflictConsumer(t *testing.T, stream jetstream.Stream, name string) *jetstream.ConsumerInfo {
	t.Helper()
	c, err := stream.Consumer(context.Background(), name)
	if err != nil {
		t.Fatalf("Consumer %s: %v", name, err)
	}
	info, err := c.Info(context.Background())
	if err != nil {
		t.Fatalf("Info of consumer %s: %v", name, err)
	}
	return info
}

// streamNames returns the names of the streams on the JetStream store in dir,
// read through an application without modules.
func streamNames(t *testing.T, dir string) []string {
	t.Helper()
	app, stop := mustStartConflict(t, dir)
	defer stop()
	lister, ok := conflictEventStream(t, app).(interface {
		StreamNames(ctx context.Context) ([]string, error)
	})
	if !ok {
		t.Fatal("the event stream cannot list stream names")
	}
	names, err := lister.StreamNames(context.Background())
	if err != nil {
		t.Fatalf("StreamNames: %v", err)
	}
	slices.Sort(names)
	return names
}

// requireMentions fails unless the error text contains every part.
func requireMentions(t *testing.T, err error, parts ...string) {
	t.Helper()
	for _, part := range parts {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("error does not mention %q:\n%v", part, err)
		}
	}
}

// requireAPIError fails unless err wraps the server's error with the code.
func requireAPIError(t *testing.T, err error, code jetstream.ErrorCode) {
	t.Helper()
	var apiErr *jetstream.APIError
	if !errors.As(err, &apiErr) || apiErr.ErrorCode != code {
		t.Errorf("error does not wrap the server error %d: %v", code, err)
	}
}

// TestIntegration_WorkQueue_TwoModulesOneEvent: two modules that consume the
// same event on one work-queue stream cannot both get it. Start fails before
// anything is created, naming both registrations, instead of with the
// server's error for the second consumer.
func TestIntegration_WorkQueue_TwoModulesOneEvent(t *testing.T) {
	dir := t.TempDir()
	module := func(name string) *conflictModule {
		return &conflictModule{name: name, regs: []*conflictReg{
			conflictEvent(sharedAlpha.ToBase(), conflictConfig("TASKS", mono.WorkQueuePolicy)),
		}}
	}
	_, _, err := startConflict(t, dir, module("billing"), module("shipping"))
	if !errors.Is(err, monoerrors.ErrWorkQueueConsumerConflict) {
		t.Fatalf("Start error = %v, want ErrWorkQueueConsumerConflict", err)
	}
	requireMentions(t, err, "TASKS", "module billing", "module shipping", "interest retention")
	var apiErr *jetstream.APIError
	if errors.As(err, &apiErr) {
		t.Errorf("the registrations must be rejected before the server is asked, got %v", apiErr)
	}
	if names := streamNames(t, dir); len(names) != 0 {
		t.Errorf("streams after the rejected start = %v, want none", names)
	}

	// With interest retention every module gets every message.
	interest := func(name string) (*conflictModule, *conflictReg) {
		reg := conflictEvent(sharedAlpha.ToBase(), conflictConfig("TASKS", mono.InterestPolicy))
		return &conflictModule{name: name, regs: []*conflictReg{reg}}, reg
	}
	billing, billingReg := interest("billing")
	shipping, shippingReg := interest("shipping")
	app, _ := mustStartConflict(t, dir, billing, shipping)
	if _, err := sharedAlpha.EventStreamPublish(context.Background(), app.EventBus("billing"), OrderEventPayload{}, nil); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if billingReg.waitHandled(1) != 1 || shippingReg.waitHandled(1) != 1 {
		t.Error("with interest retention both modules must handle the event")
	}
}

// TestIntegration_WorkQueue_RenamedEventModule: renaming the module of an
// event stream consumer on a work-queue stream asks for a new durable while
// the old one exists. Start fails with an error that names the old consumer
// and how to remove it, on every start, and nothing is deleted. Once the old
// consumer is removed, the messages it had not acknowledged are delivered to
// the new one.
func TestIntegration_WorkQueue_RenamedEventModule(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	const oldConsumer = "billing-shared-emitter-AlphaHappened-v1"
	module := func(name string, noAck bool) (*conflictModule, *conflictReg) {
		reg := conflictEvent(sharedAlpha.ToBase(), conflictConfig("TASKS", mono.WorkQueuePolicy))
		reg.noAck = noAck
		return &conflictModule{name: name, regs: []*conflictReg{reg}}, reg
	}

	billing, billingReg := module("billing", true)
	app, stop := mustStartConflict(t, dir, billing)
	for range 3 {
		if _, err := sharedAlpha.EventStreamPublish(ctx, app.EventBus("billing"), OrderEventPayload{}, nil); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	if got := billingReg.waitHandled(3); got != 3 {
		t.Fatalf("billing handled %d messages, want 3", got)
	}
	stop()

	for start := 1; start <= 2; start++ {
		invoicing, _ := module("invoicing", false)
		_, _, err := startConflict(t, dir, invoicing)
		if !errors.Is(err, monoerrors.ErrWorkQueueConsumerConflict) {
			t.Fatalf("start %d after the rename: error = %v, want ErrWorkQueueConsumerConflict", start, err)
		}
		requireAPIError(t, err, 10099)
		requireMentions(t, err, "TASKS", "module invoicing", oldConsumer,
			"event stream consumer of module billing", "nats consumer rm", "not deleted automatically")
	}

	// Nothing was deleted: the old consumer and the three messages are there.
	operator, stop := mustStartConflict(t, dir)
	stream := conflictStream(t, operator, "TASKS")
	if info := conflictConsumer(t, stream, oldConsumer); info.NumAckPending+int(info.NumPending) != 3 {
		t.Errorf("old consumer has %d unacknowledged and %d pending messages, want 3 in all", info.NumAckPending, info.NumPending)
	}
	if err := stream.DeleteConsumer(ctx, oldConsumer); err != nil {
		t.Fatalf("delete the old consumer: %v", err)
	}
	stop()

	invoicing, invoicingReg := module("invoicing", false)
	mustStartConflict(t, dir, invoicing)
	if got := invoicingReg.waitHandled(3); got != 3 {
		t.Errorf("the renamed module handled %d of the 3 messages the old consumer had not acknowledged", got)
	}
}

// TestIntegration_WorkQueue_RenamedService: the same for a stream consumer
// service that is renamed while its stream and custom subjects stay.
func TestIntegration_WorkQueue_RenamedService(t *testing.T) {
	dir := t.TempDir()
	module := func(service string) *conflictModule {
		return &conflictModule{name: "orders", regs: []*conflictReg{
			conflictService(service, conflictConfig("JOBS", mono.WorkQueuePolicy, "jobs.>")),
		}}
	}
	_, stop := mustStartConflict(t, dir, module("ingest"))
	stop()

	_, _, err := startConflict(t, dir, module("intake"))
	if !errors.Is(err, monoerrors.ErrWorkQueueConsumerConflict) {
		t.Fatalf("Start error = %v, want ErrWorkQueueConsumerConflict", err)
	}
	requireAPIError(t, err, 10099)
	requireMentions(t, err, "JOBS", "stream consumer service orders.intake", "orders-ingest",
		"stream consumer service orders.ingest", "nats consumer rm")

	// The previous name still starts.
	mustStartConflict(t, dir, module("ingest"))
}

// TestIntegration_WorkQueue_OverlappingServices: two services of one
// work-queue stream whose subjects overlap are rejected before anything is
// created; services on subjects that do not overlap are not.
func TestIntegration_WorkQueue_OverlappingServices(t *testing.T) {
	dir := t.TempDir()
	module := func(second string) *conflictModule {
		return &conflictModule{name: "orders", regs: []*conflictReg{
			conflictService("all", conflictConfig("JOBS", mono.WorkQueuePolicy, "jobs.eu.>")),
			conflictService("urgent", conflictConfig("JOBS", mono.WorkQueuePolicy, second)),
		}}
	}
	_, _, err := startConflict(t, dir, module("jobs.eu.urgent"))
	if !errors.Is(err, monoerrors.ErrWorkQueueConsumerConflict) {
		t.Fatalf("Start error = %v, want ErrWorkQueueConsumerConflict", err)
	}
	requireMentions(t, err, "orders.all", "orders.urgent", "jobs.eu.>", "jobs.eu.urgent")
	if names := streamNames(t, dir); len(names) != 0 {
		t.Errorf("streams after the rejected start = %v, want none", names)
	}
	mustStartConflict(t, dir, module("jobs.us.>"))
}

// TestIntegration_StreamSubjectsOverlap: a registration that asks for
// subjects another stream holds fails Start with an error that names that
// stream and the way out, on every start, and going back to the previous
// configuration starts again. The cases are those of renaming a stream and
// of switching a service between stream consumer and cron.
func TestIntegration_StreamSubjectsOverlap(t *testing.T) {
	serviceOn := func(stream string) *conflictModule {
		return &conflictModule{name: "orders", regs: []*conflictReg{conflictService("ingest", conflictConfig(stream, mono.LimitsPolicy))}}
	}
	eventOn := func(stream string) *conflictModule {
		return &conflictModule{name: "billing", regs: []*conflictReg{conflictEvent(sharedAlpha.ToBase(), conflictConfig(stream, mono.LimitsPolicy))}}
	}
	rollup := func(cron bool) *conflictModule {
		reg := conflictService("rollup", conflictConfig("ROLLUPS", mono.LimitsPolicy))
		reg.cron = cron
		return &conflictModule{name: "reports", regs: []*conflictReg{reg}}
	}

	tests := []struct {
		name          string
		before, after *conflictModule
		again         func() *conflictModule // the configuration of before, for the start that goes back
		mentions      []string
	}{
		{"stream of a stream consumer service renamed", serviceOn("ORDERS_A"), serviceOn("ORDERS_B"),
			func() *conflictModule { return serviceOn("ORDERS_A") },
			[]string{"stream ORDERS_B", "stream consumer service orders.ingest", "stream ORDERS_A", "services.orders.ingest", "Keep the stream name", "nats stream rm"}},
		{"stream of an event stream consumer renamed", eventOn("EVENTS_A"), eventOn("EVENTS_B"),
			func() *conflictModule { return eventOn("EVENTS_A") },
			[]string{"stream EVENTS_B", "event stream consumer of module billing", "stream EVENTS_A", sharedAlpha.Subject, "nats stream rm"}},
		{"stream consumer service becomes a cron service", rollup(false), rollup(true),
			func() *conflictModule { return rollup(false) },
			[]string{"MONO_CRON_reports_rollup", "cron service reports.rollup", "stream ROLLUPS", "stream consumer service reports.rollup", "use another service name"}},
		{"cron service becomes a stream consumer service", rollup(true), rollup(false),
			func() *conflictModule { return rollup(true) },
			[]string{"stream ROLLUPS", "stream consumer service reports.rollup", "stream MONO_CRON_reports_rollup", "the stream of a cron service", "use another service name"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			_, stop := mustStartConflict(t, dir, tt.before)
			stop()
			before := streamNames(t, dir)

			for start := 1; start <= 2; start++ {
				_, _, err := startConflict(t, dir, tt.after)
				if !errors.Is(err, monoerrors.ErrStreamSubjectsOverlap) {
					t.Fatalf("start %d: error = %v, want ErrStreamSubjectsOverlap", start, err)
				}
				requireAPIError(t, err, 10065)
				requireMentions(t, err, tt.mentions...)
				if strings.Contains(err.Error(), "server may not support message schedules") {
					t.Errorf("the overlap must not be blamed on the server version: %v", err)
				}
			}
			if after := streamNames(t, dir); !slices.Equal(after, before) {
				t.Errorf("streams after the rejected starts = %v, want %v as before", after, before)
			}
			mustStartConflict(t, dir, tt.again())
		})
	}
}

// TestIntegration_StreamSubjectsOverlap_TwoRegistrations: two modules that
// consume one event through streams with different names are rejected before
// anything is created, naming both; with one stream name they share it.
func TestIntegration_StreamSubjectsOverlap_TwoRegistrations(t *testing.T) {
	dir := t.TempDir()
	module := func(name, stream string) *conflictModule {
		return &conflictModule{name: name, regs: []*conflictReg{conflictEvent(sharedAlpha.ToBase(), conflictConfig(stream, mono.LimitsPolicy))}}
	}
	_, _, err := startConflict(t, dir, module("billing", "BILLING_EVENTS"), module("shipping", "SHIPPING_EVENTS"))
	if !errors.Is(err, monoerrors.ErrStreamSubjectsOverlap) {
		t.Fatalf("Start error = %v, want ErrStreamSubjectsOverlap", err)
	}
	requireMentions(t, err, "BILLING_EVENTS", "SHIPPING_EVENTS", "module billing", "module shipping", sharedAlpha.Subject, "same stream name")
	var apiErr *jetstream.APIError
	if errors.As(err, &apiErr) {
		t.Errorf("the registrations must be rejected before the server is asked, got %v", apiErr)
	}
	if names := streamNames(t, dir); len(names) != 0 {
		t.Errorf("streams after the rejected start = %v, want none", names)
	}
	mustStartConflict(t, dir, module("billing", "EVENTS"), module("shipping", "EVENTS"))
}

// limitsService returns a module with stream consumer service "ingest" on
// stream ORDERS, with the stream's consumer limits and the consumer's own
// settings.
func limitsService(limits mono.StreamConsumerLimits, consumer mono.ConsumerConfig) (*conflictModule, *conflictReg) {
	config := conflictConfig("ORDERS", mono.LimitsPolicy)
	config.Stream.ConsumerLimits = limits
	config.Consumer = consumer
	reg := conflictService("ingest", config)
	return &conflictModule{name: "orders", regs: []*conflictReg{reg}}, reg
}

// TestIntegration_ConsumerLimits_Tightened: a MaxAckPending limit added to
// an existing stream starts, although the server checks the limit against
// the consumer as it exists, which the framework updates only after the
// stream. The consumer keeps its position.
func TestIntegration_ConsumerLimits_Tightened(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name   string
		before int // Consumer.MaxAckPending of the first deployment
		after  int // and of the one that adds the limit of 10
		want   int // what the consumer ends up with
	}{
		{"consumer configured with the limit", 0, 10, 10},
		{"consumer left at the default", 0, 0, 10},
		{"consumer configured below the limit", 500, 5, 5},
		{"consumer without a limit of its own", -1, -1, -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			module, reg := limitsService(mono.StreamConsumerLimits{}, mono.ConsumerConfig{MaxAckPending: tt.before})
			app, stop := mustStartConflict(t, dir, module)
			for range 2 {
				if _, err := conflictEventStream(t, app).Publish(ctx, "services.orders.ingest", []byte("x")); err != nil {
					t.Fatalf("publish: %v", err)
				}
			}
			if got := reg.waitHandled(2); got != 2 {
				t.Fatalf("handled %d messages, want 2", got)
			}
			stop()

			for start := 1; start <= 2; start++ {
				module, reg := limitsService(mono.StreamConsumerLimits{MaxAckPending: 10}, mono.ConsumerConfig{MaxAckPending: tt.after})
				app, stop := mustStartConflict(t, dir, module)
				stream := conflictStream(t, app, "ORDERS")
				if got := stream.CachedInfo().Config.ConsumerLimits.MaxAckPending; got != 10 {
					t.Errorf("start %d: stream limit = %d, want 10", start, got)
				}
				info := conflictConsumer(t, stream, "orders-ingest")
				if info.Config.MaxAckPending != tt.want {
					t.Errorf("start %d: consumer MaxAckPending = %d, want %d", start, info.Config.MaxAckPending, tt.want)
				}
				if info.AckFloor.Stream != 2 {
					t.Errorf("start %d: consumer acknowledged up to %d, want 2 (its position kept)", start, info.AckFloor.Stream)
				}
				time.Sleep(300 * time.Millisecond)
				if got := reg.waitHandled(0); got != 0 {
					t.Errorf("start %d: %d handled messages were delivered again", start, got)
				}
				stop()
			}
		})
	}
}

// TestIntegration_ConsumerLimits_SharedStream: the limit is added to a
// stream two services share; the existing consumers of both are brought
// under it.
func TestIntegration_ConsumerLimits_SharedStream(t *testing.T) {
	dir := t.TempDir()
	module := func(limits mono.StreamConsumerLimits) *conflictModule {
		config := func() mono.StreamConsumerConfig {
			c := conflictConfig("ORDERS", mono.LimitsPolicy)
			c.Stream.ConsumerLimits = limits
			return c
		}
		return &conflictModule{name: "orders", regs: []*conflictReg{
			conflictService("ingest", config()), conflictService("audit", config()),
		}}
	}
	_, stop := mustStartConflict(t, dir, module(mono.StreamConsumerLimits{}))
	stop()

	app, _ := mustStartConflict(t, dir, module(mono.StreamConsumerLimits{MaxAckPending: 10}))
	stream := conflictStream(t, app, "ORDERS")
	for _, consumer := range []string{"orders-ingest", "orders-audit"} {
		if got := conflictConsumer(t, stream, consumer).Config.MaxAckPending; got != 10 {
			t.Errorf("consumer %s MaxAckPending = %d, want 10", consumer, got)
		}
	}
}

// TestIntegration_ConsumerLimits_ForeignConsumer: a consumer the application
// did not register exceeds the new limit. Nothing is changed; Start fails
// with an error that says so and wraps the server's, which names it. Once
// the operator has changed it, the application starts.
func TestIntegration_ConsumerLimits_ForeignConsumer(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	module, _ := limitsService(mono.StreamConsumerLimits{}, mono.ConsumerConfig{})
	app, stop := mustStartConflict(t, dir, module)
	foreign := jetstream.ConsumerConfig{Name: "reporting", Durable: "reporting", AckPolicy: jetstream.AckExplicitPolicy}
	if _, err := conflictStream(t, app, "ORDERS").CreateOrUpdateConsumer(ctx, foreign); err != nil {
		t.Fatalf("create the foreign consumer: %v", err)
	}
	stop()

	module, _ = limitsService(mono.StreamConsumerLimits{MaxAckPending: 10}, mono.ConsumerConfig{})
	_, _, err := startConflict(t, dir, module)
	if err == nil {
		t.Fatal("Start must fail while a foreign consumer exceeds the limit")
	}
	requireAPIError(t, err, 10069)
	requireMentions(t, err, "stream ORDERS", "stream consumer service orders.ingest", "ConsumerLimits (MaxAckPending 10)",
		"change to limits violates consumers: ", "reporting", "nats consumer edit ORDERS")

	// A start that failed changed nothing, the application's own consumer
	// included: an instance of the previous version may still pull from it.
	operator, stop := mustStartConflict(t, dir)
	stream := conflictStream(t, operator, "ORDERS")
	for _, consumer := range []string{"reporting", "orders-ingest"} {
		if got := conflictConsumer(t, stream, consumer).Config.MaxAckPending; got != 1000 {
			t.Errorf("consumer %s MaxAckPending = %d, want 1000 (unchanged)", consumer, got)
		}
	}
	foreign.MaxAckPending = 10
	if _, err := stream.UpdateConsumer(ctx, foreign); err != nil {
		t.Fatalf("update the foreign consumer: %v", err)
	}
	stop()

	module, _ = limitsService(mono.StreamConsumerLimits{MaxAckPending: 10}, mono.ConsumerConfig{})
	mustStartConflict(t, dir, module)
}

// TestIntegration_ConsumerLimits_InactiveThresholdNotLowered: an
// InactiveThreshold limit that the existing consumer exceeds is never applied
// by lowering the consumer's threshold, because the server deletes a
// consumer that is not pulled for that long. Start fails with an error that
// says so, and the consumer is unchanged.
func TestIntegration_ConsumerLimits_InactiveThresholdNotLowered(t *testing.T) {
	dir := t.TempDir()
	module, _ := limitsService(mono.StreamConsumerLimits{}, mono.ConsumerConfig{InactiveThreshold: time.Hour})
	_, stop := mustStartConflict(t, dir, module)
	stop()

	module, _ = limitsService(mono.StreamConsumerLimits{InactiveThreshold: time.Minute, MaxAckPending: 10},
		mono.ConsumerConfig{InactiveThreshold: 30 * time.Second})
	_, _, err := startConflict(t, dir, module)
	if err == nil {
		t.Fatal("Start must fail: the existing consumer exceeds the InactiveThreshold limit")
	}
	requireAPIError(t, err, 10069)
	requireMentions(t, err, "InactiveThreshold 1m0s", "never lowered automatically", "violates consumers: orders-ingest")

	operator, stop := mustStartConflict(t, dir)
	info := conflictConsumer(t, conflictStream(t, operator, "ORDERS"), "orders-ingest")
	if info.Config.InactiveThreshold != time.Hour || info.Config.MaxAckPending != 1000 {
		t.Errorf("consumer InactiveThreshold = %s, MaxAckPending = %d; want 1h and 1000 (unchanged)",
			info.Config.InactiveThreshold, info.Config.MaxAckPending)
	}
	stop()

	// The way out is two deployments: the consumer first, then the limit.
	module, _ = limitsService(mono.StreamConsumerLimits{}, mono.ConsumerConfig{InactiveThreshold: 30 * time.Second})
	_, stop = mustStartConflict(t, dir, module)
	stop()
	module, _ = limitsService(mono.StreamConsumerLimits{InactiveThreshold: time.Minute, MaxAckPending: 10},
		mono.ConsumerConfig{InactiveThreshold: 30 * time.Second})
	app, _ := mustStartConflict(t, dir, module)
	stream := conflictStream(t, app, "ORDERS")
	if got := stream.CachedInfo().Config.ConsumerLimits.InactiveThreshold; got != time.Minute {
		t.Errorf("stream InactiveThreshold limit = %s, want 1m", got)
	}
	if got := conflictConsumer(t, stream, "orders-ingest").Config.InactiveThreshold; got != 30*time.Second {
		t.Errorf("consumer InactiveThreshold = %s, want 30s", got)
	}
}

// inheritedThresholdWarnings returns the recorded warnings about a durable
// that inherits its stream's inactivity threshold.
func inheritedThresholdWarnings(log *sharedLog) []string {
	log.mu.Lock()
	defer log.mu.Unlock()
	var found []string
	for _, w := range log.warns {
		if strings.Contains(w, "inherits the InactiveThreshold of its stream's ConsumerLimits") {
			found = append(found, w)
		}
	}
	return found
}

// TestIntegration_ConsumerLimits_InheritedInactiveThresholdWarned: a stream
// with an InactiveThreshold consumer limit hands it to the durable consumer
// of its registration, which the server then deletes, with its position,
// once nothing pulls it for that long while a server is running. The
// framework warns about it at every start. The test also shows what the
// warning says:
//
//   - the consumer survives while the application pulls it;
//   - it survives a downtime of the whole application, however long, because
//     the embedded server is down as well and nothing counts;
//   - it is deleted when a server runs for longer than the threshold without
//     the registration pulling it, and the next start delivers the stream
//     again.
func TestIntegration_ConsumerLimits_InheritedInactiveThresholdWarned(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	const threshold = 2 * time.Second
	start := func(limit time.Duration, wantWarning bool) (mono.MonoApplication, *conflictReg, func()) {
		t.Helper()
		log := &sharedLog{}
		module, reg := limitsService(mono.StreamConsumerLimits{InactiveThreshold: limit}, mono.ConsumerConfig{})
		app, stop, err := startConflictLogged(t, dir, log, module)
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		warnings := inheritedThresholdWarnings(log)
		if !wantWarning {
			if len(warnings) != 0 {
				t.Fatalf("warnings without a limit = %v, want none", warnings)
			}
			return app, reg, stop
		}
		if len(warnings) != 1 {
			t.Fatalf("warnings about the inherited threshold = %v, want one", warnings)
		}
		for _, part := range []string{"stream consumer service orders.ingest", "ORDERS", "orders-ingest", "2s"} {
			if !strings.Contains(warnings[0], part) {
				t.Errorf("warning does not mention %q: %s", part, warnings[0])
			}
		}
		return app, reg, stop
	}

	// A deployment without the limit creates the consumer and handles two
	// messages.
	app, reg, stop := start(0, false)
	for range 2 {
		if _, err := conflictEventStream(t, app).Publish(ctx, "services.orders.ingest", []byte("x")); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	if got := reg.waitHandled(2); got != 2 {
		t.Fatalf("handled %d messages, want 2", got)
	}
	stop()

	// The limit is added: the existing consumer inherits it.
	app, _, stop = start(threshold, true)
	stream := conflictStream(t, app, "ORDERS")
	created := conflictConsumer(t, stream, "orders-ingest")
	if created.Config.InactiveThreshold != threshold {
		t.Fatalf("consumer InactiveThreshold = %s, want the stream's limit %s", created.Config.InactiveThreshold, threshold)
	}
	// While the application runs, its fetch loop pulls the consumer.
	time.Sleep(2 * threshold)
	conflictConsumer(t, stream, "orders-ingest")
	stop()

	// A downtime of the whole application does not count: its server is
	// down too.
	time.Sleep(2 * threshold)
	app, reg, stop = start(threshold, true)
	survived := conflictConsumer(t, conflictStream(t, app, "ORDERS"), "orders-ingest")
	if !survived.Created.Equal(created.Created) || survived.AckFloor.Stream != 2 {
		t.Errorf("after a downtime the consumer was created at %s with ack floor %d, want %s and 2 (the same consumer)",
			survived.Created, survived.AckFloor.Stream, created.Created)
	}
	time.Sleep(300 * time.Millisecond)
	if got := reg.waitHandled(0); got != 0 {
		t.Errorf("%d handled messages were delivered again after a downtime", got)
	}
	stop()

	// A server that runs without the registration for longer than the
	// threshold: nothing pulls the consumer, and it is deleted.
	operator, stop := mustStartConflict(t, dir)
	stream = conflictStream(t, operator, "ORDERS")
	deadline := time.Now().Add(15 * time.Second)
	for {
		_, err := stream.Consumer(ctx, "orders-ingest")
		if errors.Is(err, jetstream.ErrConsumerNotFound) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the consumer still exists after %s without a pull (last error %v)", 15*time.Second, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	stop()

	// The next start creates it again from its deliver policy: the two
	// handled messages are delivered again.
	app, reg, _ = start(threshold, true)
	recreated := conflictConsumer(t, conflictStream(t, app, "ORDERS"), "orders-ingest")
	if !recreated.Created.After(created.Created) {
		t.Errorf("the consumer was created at %s, want a new one after %s", recreated.Created, created.Created)
	}
	if got := reg.waitHandled(2); got != 2 {
		t.Errorf("the new consumer handled %d of the 2 messages again, want 2", got)
	}
}
