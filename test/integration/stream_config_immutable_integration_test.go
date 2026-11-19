//go:build integration
// +build integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-monolith/mono"
	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/helper"
)

// immutableStreamEventDef is the event the event stream consumers of these
// tests register for, and immutableStreamEventDefV2 its next version, which
// has another subject.
var (
	immutableStreamEventDef   = helper.EventDefinition[OrderEventPayload]("immutable-orders", "OrderPlaced", "v1")
	immutableStreamEventDefV2 = helper.EventDefinition[OrderEventPayload]("immutable-orders", "OrderPlaced", "v2")
)

// immutableStreamReg is one registration of an immutableStreamModule: a
// stream consumer service, or an event stream consumer when event is set.
type immutableStreamReg struct {
	service string                    // stream consumer service name
	event   *mono.BaseEventDefinition // event of an event stream consumer
	stream  mono.StreamConfig
	handled *atomic.Int64 // counts handled messages when set
}

// immutableStreamModule registers stream consumer services and event stream
// consumers with the stream configurations it is given.
type immutableStreamModule struct {
	name string
	regs []immutableStreamReg
}

// serviceStreamModule returns a module with one stream consumer service.
func serviceStreamModule(name, service string, stream mono.StreamConfig) *immutableStreamModule {
	return &immutableStreamModule{name: name, regs: []immutableStreamReg{{service: service, stream: stream}}}
}

// eventStreamModule returns a module with one event stream consumer of
// immutableStreamEventDef.
func eventStreamModule(name string, stream mono.StreamConfig) *immutableStreamModule {
	def := immutableStreamEventDef.ToBase()
	return &immutableStreamModule{name: name, regs: []immutableStreamReg{{event: &def, stream: stream}}}
}

func (m *immutableStreamModule) Name() string                { return m.name }
func (m *immutableStreamModule) Start(context.Context) error { return nil }
func (m *immutableStreamModule) Stop(context.Context) error  { return nil }

func (r immutableStreamReg) config() mono.StreamConsumerConfig {
	return mono.StreamConsumerConfig{
		Stream: r.stream,
		Fetch:  mono.FetchConfig{BatchSize: 1, Timeout: time.Second},
	}
}

// handle acknowledges every message and counts it.
func (r immutableStreamReg) handle(_ context.Context, msgs []*mono.Msg) error {
	for _, msg := range msgs {
		if r.handled != nil {
			r.handled.Add(1)
		}
		_ = msg.Ack()
	}
	return nil
}

func (m *immutableStreamModule) RegisterServices(c mono.ServiceContainer) error {
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

func (m *immutableStreamModule) RegisterEventConsumers(registry mono.EventRegistry) error {
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

// startImmutableStreamApp starts an application on the JetStream store in dir
// with the modules and returns it, a function that stops it and the error of
// Start.
func startImmutableStreamApp(t *testing.T, dir string, modules ...*immutableStreamModule) (mono.MonoApplication, func(), error) {
	t.Helper()
	app, err := mono.NewMonoApplication(
		mono.WithCustomLogger(&noOpsLogger{}),
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
	return app, func() { _ = app.Stop(context.Background()) }, startErr
}

// requireImmutableStreamError checks that err is ErrStreamConfigImmutable and
// that its message carries every one of the parts.
func requireImmutableStreamError(t *testing.T, err error, parts ...string) {
	t.Helper()
	if !errors.Is(err, monoerrors.ErrStreamConfigImmutable) {
		t.Fatalf("Start error = %v, want ErrStreamConfigImmutable", err)
	}
	for _, part := range parts {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("Start error %q does not contain %q", err.Error(), part)
		}
	}
}

// TestIntegration_StreamConsumerService_StorageChange: changing the storage
// type of a stream consumer service's stream fails Start with
// ErrStreamConfigImmutable, leaves the stream and its messages as they were,
// and the previous configuration starts again.
func TestIntegration_StreamConsumerService_StorageChange(t *testing.T) {
	dir := t.TempDir()
	module := func(storage mono.StorageType) *immutableStreamModule {
		return serviceStreamModule("orders", "ingest", mono.StreamConfig{Name: "ORDERS_INGEST", Storage: storage})
	}

	app, stop, err := startImmutableStreamApp(t, dir, module(mono.FileStorage))
	if err != nil {
		t.Fatalf("first Start: %v", err)
	}
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := helper.PublishStreamConsumerService(ctx, app.Services("orders"), "ingest", json.Marshal, i); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	stop()

	// The same service with memory storage: every Start fails the same way.
	for attempt := 1; attempt <= 2; attempt++ {
		_, stop, err = startImmutableStreamApp(t, dir, module(mono.MemoryStorage))
		stop()
		requireImmutableStreamError(t, err,
			"stream ORDERS_INGEST", "stream consumer service orders.ingest",
			"exists with file storage", "is configured with memory storage", "10052")
	}

	// Nothing was deleted: the previous configuration starts and the stream
	// still holds its messages.
	app, stop, err = startImmutableStreamApp(t, dir, module(mono.FileStorage))
	defer stop()
	if err != nil {
		t.Fatalf("Start with the previous storage type: %v", err)
	}
	es, err := app.EventBus("orders").EventStream()
	if err != nil {
		t.Fatalf("EventStream: %v", err)
	}
	stream, err := es.Stream(ctx, "ORDERS_INGEST")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.State.Msgs != 3 {
		t.Errorf("stream holds %d messages after the failed starts, want 3", info.State.Msgs)
	}
}

// TestIntegration_StreamConsumerService_WorkQueueRetentionChange: changing a
// stream's retention policy to or from work-queue fails Start with
// ErrStreamConfigImmutable; limits to interest is applied in place.
func TestIntegration_StreamConsumerService_WorkQueueRetentionChange(t *testing.T) {
	module := func(retention mono.RetentionPolicy) *immutableStreamModule {
		return serviceStreamModule("orders", "tasks", mono.StreamConfig{Name: "ORDERS_TASKS", Retention: retention})
	}
	tests := []struct {
		name       string
		from, to   mono.RetentionPolicy
		have, want string
	}{
		{"to work-queue", mono.LimitsPolicy, mono.WorkQueuePolicy, "exists with limits retention", "is configured with work-queue retention"},
		{"from work-queue", mono.WorkQueuePolicy, mono.InterestPolicy, "exists with work-queue retention", "is configured with interest retention"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			_, stop, err := startImmutableStreamApp(t, dir, module(tt.from))
			stop()
			if err != nil {
				t.Fatalf("first Start: %v", err)
			}
			_, stop, err = startImmutableStreamApp(t, dir, module(tt.to))
			stop()
			requireImmutableStreamError(t, err, "stream ORDERS_TASKS", "stream consumer service orders.tasks", tt.have, tt.want)
		})
	}

	t.Run("limits to interest is updated in place", func(t *testing.T) {
		dir := t.TempDir()
		_, stop, err := startImmutableStreamApp(t, dir, module(mono.LimitsPolicy))
		stop()
		if err != nil {
			t.Fatalf("first Start: %v", err)
		}
		_, stop, err = startImmutableStreamApp(t, dir, module(mono.InterestPolicy))
		stop()
		if err != nil {
			t.Fatalf("Start with interest retention: %v", err)
		}
	})
}

// TestIntegration_EventStreamConsumer_StorageChange: changing the storage type
// of an event stream consumer's stream fails Start with
// ErrStreamConfigImmutable.
func TestIntegration_EventStreamConsumer_StorageChange(t *testing.T) {
	dir := t.TempDir()
	module := func(storage mono.StorageType) *immutableStreamModule {
		return eventStreamModule("billing", mono.StreamConfig{Name: "ORDER_EVENTS", Storage: storage})
	}
	_, stop, err := startImmutableStreamApp(t, dir, module(mono.FileStorage))
	stop()
	if err != nil {
		t.Fatalf("first Start: %v", err)
	}
	_, stop, err = startImmutableStreamApp(t, dir, module(mono.MemoryStorage))
	stop()
	requireImmutableStreamError(t, err,
		"stream ORDER_EVENTS", "event stream consumer of module billing for event immutable-orders.OrderPlaced",
		"exists with file storage", "is configured with memory storage")
}

// TestIntegration_EventStreamConsumer_ConflictingRegistrations: two modules
// that register the same stream with different storage types fail Start with
// ErrStreamConfigImmutable before the stream is created.
func TestIntegration_EventStreamConsumer_ConflictingRegistrations(t *testing.T) {
	dir := t.TempDir()
	module := func(name string, storage mono.StorageType) *immutableStreamModule {
		return eventStreamModule(name, mono.StreamConfig{Name: "ORDER_EVENTS", Storage: storage})
	}
	_, stop, err := startImmutableStreamApp(t, dir, module("billing", mono.FileStorage), module("shipping", mono.MemoryStorage))
	stop()
	requireImmutableStreamError(t, err,
		"stream ORDER_EVENTS", "module billing", "module shipping",
		"the first with file storage", "the second with memory storage")

	// The failed start created nothing: had it created the stream with file
	// storage, this start with memory storage would fail.
	_, stop, err = startImmutableStreamApp(t, dir, module("billing", mono.MemoryStorage), module("shipping", mono.MemoryStorage))
	stop()
	if err != nil {
		t.Fatalf("Start with agreeing registrations: %v", err)
	}
}

// waitHandled waits until counter reaches want.
func waitHandled(t *testing.T, what string, counter *atomic.Int64, want int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for counter.Load() < want {
		if time.Now().After(deadline) {
			t.Fatalf("%s handled %d messages, want %d", what, counter.Load(), want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestIntegration_StreamConsumerService_SideBySideMigration: the documented
// way to another storage type. A second service name on a new stream runs
// beside the old one, both are served, and the old registration can then be
// removed.
func TestIntegration_StreamConsumerService_SideBySideMigration(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	var oldHandled, newHandled atomic.Int64
	oldReg := immutableStreamReg{service: "ingest", stream: mono.StreamConfig{Name: "ORDERS_INGEST", Storage: mono.FileStorage}, handled: &oldHandled}
	newReg := immutableStreamReg{service: "ingest-v2", stream: mono.StreamConfig{Name: "ORDERS_INGEST_V2", Storage: mono.MemoryStorage}, handled: &newHandled}
	publish := func(app mono.MonoApplication, service string, n int) {
		t.Helper()
		for i := 0; i < n; i++ {
			if _, err := helper.PublishStreamConsumerService(ctx, app.Services("orders"), service, json.Marshal, i); err != nil {
				t.Fatalf("publish to %s: %v", service, err)
			}
		}
	}

	// The old service alone.
	app, stop, err := startImmutableStreamApp(t, dir, &immutableStreamModule{name: "orders", regs: []immutableStreamReg{oldReg}})
	if err != nil {
		t.Fatalf("Start with the old service: %v", err)
	}
	publish(app, "ingest", 2)
	waitHandled(t, "old service", &oldHandled, 2)
	stop()

	// Old and new side by side.
	app, stop, err = startImmutableStreamApp(t, dir, &immutableStreamModule{name: "orders", regs: []immutableStreamReg{oldReg, newReg}})
	if err != nil {
		t.Fatalf("Start with both services: %v", err)
	}
	publish(app, "ingest", 3)
	publish(app, "ingest-v2", 4)
	waitHandled(t, "old service", &oldHandled, 5)
	waitHandled(t, "new service", &newHandled, 4)
	stop()

	// The new service alone; the old stream is left in place.
	app, stop, err = startImmutableStreamApp(t, dir, &immutableStreamModule{name: "orders", regs: []immutableStreamReg{newReg}})
	defer stop()
	if err != nil {
		t.Fatalf("Start with the new service: %v", err)
	}
	publish(app, "ingest-v2", 2)
	waitHandled(t, "new service", &newHandled, 6)
	if got := oldHandled.Load(); got != 5 {
		t.Errorf("old service handled %d messages, want 5", got)
	}
}

// TestIntegration_EventStreamConsumer_SideBySideMigration: the same for an
// event stream consumer. A new event version has another subject, so its
// stream, with another storage type, runs beside the old one.
func TestIntegration_EventStreamConsumer_SideBySideMigration(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	var oldHandled, newHandled atomic.Int64
	v1, v2 := immutableStreamEventDef.ToBase(), immutableStreamEventDefV2.ToBase()
	oldReg := immutableStreamReg{event: &v1, stream: mono.StreamConfig{Name: "ORDER_EVENTS", Storage: mono.FileStorage}, handled: &oldHandled}
	newReg := immutableStreamReg{event: &v2, stream: mono.StreamConfig{Name: "ORDER_EVENTS_V2", Storage: mono.MemoryStorage}, handled: &newHandled}

	_, stop, err := startImmutableStreamApp(t, dir, &immutableStreamModule{name: "billing", regs: []immutableStreamReg{oldReg}})
	stop()
	if err != nil {
		t.Fatalf("Start with the old event version: %v", err)
	}

	app, stop, err := startImmutableStreamApp(t, dir, &immutableStreamModule{name: "billing", regs: []immutableStreamReg{oldReg, newReg}})
	defer stop()
	if err != nil {
		t.Fatalf("Start with both event versions: %v", err)
	}
	bus := app.EventBus("billing")
	for i := 0; i < 3; i++ {
		if _, err := immutableStreamEventDef.EventStreamPublish(ctx, bus, OrderEventPayload{OrderID: "old"}, nil); err != nil {
			t.Fatalf("publish v1: %v", err)
		}
	}
	for i := 0; i < 4; i++ {
		if _, err := immutableStreamEventDefV2.EventStreamPublish(ctx, bus, OrderEventPayload{OrderID: "new"}, nil); err != nil {
			t.Fatalf("publish v2: %v", err)
		}
	}
	waitHandled(t, "old event version", &oldHandled, 3)
	waitHandled(t, "new event version", &newHandled, 4)
}
