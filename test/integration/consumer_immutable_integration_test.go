//go:build integration
// +build integration

package integration_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/go-monolith/mono"
	"github.com/go-monolith/mono/pkg/helper"
	"github.com/go-monolith/mono/pkg/types"
)

// keptSettingEvent is the event the event stream consumers of these tests
// register for.
var keptSettingEvent = helper.EventDefinition[OrderEventPayload]("kept-orders", "OrderPlaced", "v1")

// keptSettingModule registers one stream consumer service ("ingest" on stream
// ORDERS) or, when event is set, one event stream consumer (stream
// ORDER_EVENTS), with the consumer configuration it is given.
type keptSettingModule struct {
	consumer mono.ConsumerConfig
	event    bool
	ack      bool // acknowledge handled messages
	handled  *atomic.Int64
}

func (m *keptSettingModule) Name() string                { return "orders" }
func (m *keptSettingModule) Start(context.Context) error { return nil }
func (m *keptSettingModule) Stop(context.Context) error  { return nil }

func (m *keptSettingModule) handle(_ context.Context, msgs []*mono.Msg) error {
	for _, msg := range msgs {
		if m.handled != nil {
			m.handled.Add(1)
		}
		if m.ack {
			_ = msg.Ack()
		}
	}
	return nil
}

func (m *keptSettingModule) config(stream string) mono.StreamConsumerConfig {
	return mono.StreamConsumerConfig{
		Stream:   mono.StreamConfig{Name: stream},
		Consumer: m.consumer,
		Fetch:    mono.FetchConfig{BatchSize: 1, Timeout: time.Second},
	}
}

func (m *keptSettingModule) RegisterServices(c mono.ServiceContainer) error {
	if m.event {
		return nil
	}
	return c.RegisterStreamConsumerService("ingest", m.config("ORDERS"), m.handle)
}

func (m *keptSettingModule) RegisterEventConsumers(registry mono.EventRegistry) error {
	if !m.event {
		return nil
	}
	return registry.RegisterEventStreamConsumer(keptSettingEvent.ToBase(), m.config("ORDER_EVENTS"), m.handle, m)
}

// keptWarnings returns the recorded warnings about kept consumer settings.
func keptWarnings(log *sharedLog) []string {
	log.mu.Lock()
	defer log.mu.Unlock()
	var found []string
	for _, w := range log.warns {
		if strings.Contains(w, "cannot be changed on an existing consumer") {
			found = append(found, w)
		}
	}
	return found
}

// startKept starts an application with the module on the JetStream store in
// dir and returns it, its warnings and the error of Start. The application
// is stopped by the returned function, and when the test ends.
func startKept(t *testing.T, dir string, m *keptSettingModule) (mono.MonoApplication, *sharedLog, func(), error) {
	t.Helper()
	log := &sharedLog{}
	app, err := mono.NewMonoApplication(
		mono.WithCustomLogger(log),
		mono.WithJetStreamStorageDir(dir),
		mono.WithNATSDontListen(),
		mono.WithNATSInProcessConn(),
	)
	if err != nil {
		t.Fatalf("NewMonoApplication: %v", err)
	}
	if err := app.Register(m); err != nil {
		t.Fatalf("Register: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	startErr := app.Start(ctx)
	var once sync.Once
	stop := func() { once.Do(func() { _ = app.Stop(context.Background()) }) }
	t.Cleanup(stop)
	return app, log, stop, startErr
}

// keptConsumer returns the consumer of the module as the server holds it.
func keptConsumer(t *testing.T, app mono.MonoApplication, stream, consumer string) *jetstream.ConsumerInfo {
	t.Helper()
	es, err := app.EventBus("orders").EventStream()
	if err != nil {
		t.Fatalf("EventStream: %v", err)
	}
	s, err := es.Stream(context.Background(), stream)
	if err != nil {
		t.Fatalf("Stream %s: %v", stream, err)
	}
	c, err := s.Consumer(context.Background(), consumer)
	if err != nil {
		t.Fatalf("Consumer %s: %v", consumer, err)
	}
	info, err := c.Info(context.Background())
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	return info
}

// TestIntegration_StreamConsumerService_ImmutableSettingsKept: each consumer
// setting NATS cannot change on an existing consumer. A deployment that
// changes it starts, keeps the existing value and the consumer's position,
// and logs a warning with the command that applies the change, at every
// start while the configuration differs.
func TestIntegration_StreamConsumerService_ImmutableSettingsKept(t *testing.T) {
	ctx := context.Background()
	earlier := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	later := earlier.Add(time.Hour)

	tests := []struct {
		name          string
		before, after mono.ConsumerConfig
		setting       string
		configured    string
		kept          string
		check         func(t *testing.T, cfg jetstream.ConsumerConfig)
	}{
		{name: "DeliverPolicy all to new", after: mono.ConsumerConfig{DeliverPolicy: mono.DeliverNewPolicy},
			setting: "DeliverPolicy", configured: "new", kept: "all",
			check: func(t *testing.T, cfg jetstream.ConsumerConfig) {
				if cfg.DeliverPolicy != jetstream.DeliverAllPolicy {
					t.Errorf("DeliverPolicy = %v, want all", cfg.DeliverPolicy)
				}
			}},
		{name: "DeliverPolicy new to all", before: mono.ConsumerConfig{DeliverPolicy: mono.DeliverNewPolicy},
			setting: "DeliverPolicy", configured: "all", kept: "new",
			check: func(t *testing.T, cfg jetstream.ConsumerConfig) {
				if cfg.DeliverPolicy != jetstream.DeliverNewPolicy {
					t.Errorf("DeliverPolicy = %v, want new", cfg.DeliverPolicy)
				}
			}},
		{name: "OptStartSeq",
			before:  mono.ConsumerConfig{DeliverPolicy: mono.DeliverByStartSequencePolicy, OptStartSeq: 1},
			after:   mono.ConsumerConfig{DeliverPolicy: mono.DeliverByStartSequencePolicy, OptStartSeq: 2},
			setting: "OptStartSeq", configured: "2", kept: "1",
			check: func(t *testing.T, cfg jetstream.ConsumerConfig) {
				if cfg.OptStartSeq != 1 {
					t.Errorf("OptStartSeq = %d, want 1", cfg.OptStartSeq)
				}
			}},
		{name: "OptStartTime",
			before:  mono.ConsumerConfig{DeliverPolicy: mono.DeliverByStartTimePolicy, OptStartTime: &earlier},
			after:   mono.ConsumerConfig{DeliverPolicy: mono.DeliverByStartTimePolicy, OptStartTime: &later},
			setting: "OptStartTime", configured: "2026-01-01T01:00:00Z", kept: "2026-01-01T00:00:00Z",
			check: func(t *testing.T, cfg jetstream.ConsumerConfig) {
				if cfg.OptStartTime == nil || !cfg.OptStartTime.Equal(earlier) {
					t.Errorf("OptStartTime = %v, want %v", cfg.OptStartTime, earlier)
				}
			}},
		{name: "AckPolicy explicit to all", after: mono.ConsumerConfig{AckPolicy: mono.AckAllPolicy},
			setting: "AckPolicy", configured: "all", kept: "explicit",
			check: func(t *testing.T, cfg jetstream.ConsumerConfig) {
				if cfg.AckPolicy != jetstream.AckExplicitPolicy {
					t.Errorf("AckPolicy = %v, want explicit", cfg.AckPolicy)
				}
			}},
		{name: "ReplayPolicy", after: mono.ConsumerConfig{ReplayPolicy: mono.ReplayOriginalPolicy},
			setting: "ReplayPolicy", configured: "original", kept: "instant",
			check: func(t *testing.T, cfg jetstream.ConsumerConfig) {
				if cfg.ReplayPolicy != jetstream.ReplayInstantPolicy {
					t.Errorf("ReplayPolicy = %v, want instant", cfg.ReplayPolicy)
				}
			}},
		{name: "MaxWaiting default to 100", after: mono.ConsumerConfig{MaxWaiting: 100},
			setting: "MaxWaiting", configured: "100", kept: "512",
			check: func(t *testing.T, cfg jetstream.ConsumerConfig) {
				if cfg.MaxWaiting != 512 {
					t.Errorf("MaxWaiting = %d, want 512", cfg.MaxWaiting)
				}
			}},
		{name: "MaxWaiting 100 to 200", before: mono.ConsumerConfig{MaxWaiting: 100}, after: mono.ConsumerConfig{MaxWaiting: 200},
			setting: "MaxWaiting", configured: "200", kept: "100",
			check: func(t *testing.T, cfg jetstream.ConsumerConfig) {
				if cfg.MaxWaiting != 100 {
					t.Errorf("MaxWaiting = %d, want 100", cfg.MaxWaiting)
				}
			}},
		{name: "MaxWaiting 100 to default", before: mono.ConsumerConfig{MaxWaiting: 100},
			setting: "MaxWaiting", configured: "512", kept: "100",
			check: func(t *testing.T, cfg jetstream.ConsumerConfig) {
				if cfg.MaxWaiting != 100 {
					t.Errorf("MaxWaiting = %d, want 100", cfg.MaxWaiting)
				}
			}},
		{name: "MemoryStorage", after: mono.ConsumerConfig{MemoryStorage: true},
			setting: "MemoryStorage", configured: "true", kept: "false",
			check: func(t *testing.T, cfg jetstream.ConsumerConfig) {
				if cfg.MemoryStorage {
					t.Error("MemoryStorage = true, want false")
				}
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			var handled atomic.Int64
			app, log, stop, err := startKept(t, dir, &keptSettingModule{consumer: tt.before, ack: true, handled: &handled})
			if err != nil {
				t.Fatalf("first Start: %v", err)
			}
			for i := 0; i < 3; i++ {
				if _, err := helper.PublishStreamConsumerService(ctx, app.Services("orders"), "ingest", json.Marshal, i); err != nil {
					t.Fatalf("publish: %v", err)
				}
			}
			deadline := time.Now().Add(10 * time.Second)
			for handled.Load() < 3 && time.Now().Before(deadline) {
				time.Sleep(20 * time.Millisecond)
			}
			time.Sleep(300 * time.Millisecond)
			floor := keptConsumer(t, app, "ORDERS", "orders-ingest").AckFloor.Stream
			if got := keptWarnings(log); len(got) != 0 {
				t.Fatalf("first start warned: %v", got)
			}
			stop()

			for start := 2; start <= 3; start++ {
				app, log, stop, err := startKept(t, dir, &keptSettingModule{consumer: tt.after, ack: true})
				if err != nil {
					t.Fatalf("start %d with the changed setting: %v", start, err)
				}
				warnings := keptWarnings(log)
				if len(warnings) != 1 {
					t.Fatalf("start %d: warnings = %v, want one", start, warnings)
				}
				for _, part := range []string{
					"stream consumer service orders.ingest", "streamORDERS", "consumerorders-ingest",
					"setting" + tt.setting, "configured" + tt.configured, "kept" + tt.kept,
					"nats consumer rm ORDERS orders-ingest",
				} {
					if !strings.Contains(strings.ReplaceAll(warnings[0], " ", ""), strings.ReplaceAll(part, " ", "")) {
						t.Errorf("start %d: warning %q does not contain %q", start, warnings[0], part)
					}
				}
				info := keptConsumer(t, app, "ORDERS", "orders-ingest")
				tt.check(t, info.Config)
				if info.AckFloor.Stream != floor {
					t.Errorf("start %d: ack floor = %d, want %d: the consumer lost its position", start, info.AckFloor.Stream, floor)
				}
				stop()
			}
		})
	}
}

// TestIntegration_StreamConsumerService_UnchangedConsumerNotWarned: a restart
// with the same configuration logs no warning, also when MaxWaiting is left
// at zero, and the server's default is the one the framework compares with.
func TestIntegration_StreamConsumerService_UnchangedConsumerNotWarned(t *testing.T) {
	dir := t.TempDir()
	for start := 1; start <= 2; start++ {
		app, log, stop, err := startKept(t, dir, &keptSettingModule{ack: true})
		if err != nil {
			t.Fatalf("start %d: %v", start, err)
		}
		if got := keptWarnings(log); len(got) != 0 {
			t.Errorf("start %d warned: %v", start, got)
		}
		if got := keptConsumer(t, app, "ORDERS", "orders-ingest").Config.MaxWaiting; got != 512 {
			t.Errorf("start %d: the server's default MaxWaiting is %d, the framework assumes 512", start, got)
		}
		stop()
	}
}

// TestIntegration_StreamConsumerService_StartTimeComputedAtStartup: a start
// time computed when the application starts differs on every start. The
// second start succeeds, keeps the first time and says so.
func TestIntegration_StreamConsumerService_StartTimeComputedAtStartup(t *testing.T) {
	dir := t.TempDir()
	start := func() (*sharedLog, error) {
		since := time.Now().Add(-time.Hour)
		_, log, stop, err := startKept(t, dir, &keptSettingModule{
			consumer: mono.ConsumerConfig{DeliverPolicy: mono.DeliverByStartTimePolicy, OptStartTime: &since}, ack: true,
		})
		stop()
		return log, err
	}
	if _, err := start(); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	log, err := start()
	if err != nil {
		t.Fatalf("second Start: %v", err)
	}
	warnings := keptWarnings(log)
	if len(warnings) != 1 || !strings.Contains(warnings[0], "OptStartTime") || !strings.Contains(warnings[0], "use a fixed time") {
		t.Errorf("warnings = %v, want one about OptStartTime that says to use a fixed time", warnings)
	}
}

// TestIntegration_StreamConsumerService_DeletedConsumerGetsNewSettings: after
// the consumer is deleted, the configured values are applied and nothing is
// warned about.
func TestIntegration_StreamConsumerService_DeletedConsumerGetsNewSettings(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	_, _, stop, err := startKept(t, dir, &keptSettingModule{ack: true})
	stop()
	if err != nil {
		t.Fatalf("first Start: %v", err)
	}

	// The equivalent of `nats consumer rm ORDERS orders-ingest`, through an
	// application that does not register the service.
	helperApp, _, stopHelper, err := startKept(t, dir, &keptSettingModule{event: true, ack: true})
	if err != nil {
		t.Fatalf("helper Start: %v", err)
	}
	hes, err := helperApp.EventBus("orders").EventStream()
	if err != nil {
		t.Fatalf("EventStream: %v", err)
	}
	hstream, err := hes.Stream(ctx, "ORDERS")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if err := hstream.DeleteConsumer(ctx, "orders-ingest"); err != nil {
		t.Fatalf("delete the consumer: %v", err)
	}
	stopHelper()

	app, log, _, err := startKept(t, dir, &keptSettingModule{
		consumer: mono.ConsumerConfig{DeliverPolicy: mono.DeliverNewPolicy, MaxWaiting: 100, AckPolicy: mono.AckAllPolicy}, ack: true,
	})
	if err != nil {
		t.Fatalf("Start after the consumer was deleted: %v", err)
	}
	if got := keptWarnings(log); len(got) != 0 {
		t.Errorf("warnings = %v, want none", got)
	}
	cfg := keptConsumer(t, app, "ORDERS", "orders-ingest").Config
	if cfg.DeliverPolicy != jetstream.DeliverNewPolicy || cfg.MaxWaiting != 100 || cfg.AckPolicy != jetstream.AckAllPolicy {
		t.Errorf("consumer = deliver %v, max waiting %d, ack %v; want the configured values", cfg.DeliverPolicy, cfg.MaxWaiting, cfg.AckPolicy)
	}
}

// TestIntegration_EventStreamConsumer_ImmutableSettingsKept: the same for an
// event stream consumer.
func TestIntegration_EventStreamConsumer_ImmutableSettingsKept(t *testing.T) {
	durable := types.EventStreamConsumerName("orders", keptSettingEvent.ToBase())
	tests := []struct {
		name    string
		after   mono.ConsumerConfig
		setting string
	}{
		{"DeliverPolicy", mono.ConsumerConfig{DeliverPolicy: mono.DeliverNewPolicy}, "DeliverPolicy"},
		{"AckPolicy", mono.ConsumerConfig{AckPolicy: mono.AckAllPolicy}, "AckPolicy"},
		{"ReplayPolicy", mono.ConsumerConfig{ReplayPolicy: mono.ReplayOriginalPolicy}, "ReplayPolicy"},
		{"MaxWaiting", mono.ConsumerConfig{MaxWaiting: 100}, "MaxWaiting"},
		{"MemoryStorage", mono.ConsumerConfig{MemoryStorage: true}, "MemoryStorage"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			_, _, stop, err := startKept(t, dir, &keptSettingModule{event: true, ack: true})
			stop()
			if err != nil {
				t.Fatalf("first Start: %v", err)
			}
			app, log, _, err := startKept(t, dir, &keptSettingModule{event: true, ack: true, consumer: tt.after})
			if err != nil {
				t.Fatalf("Start with the changed setting: %v", err)
			}
			warnings := keptWarnings(log)
			if len(warnings) != 1 || !strings.Contains(warnings[0], tt.setting) ||
				!strings.Contains(warnings[0], "event stream consumer of module orders for event kept-orders.OrderPlaced") ||
				!strings.Contains(warnings[0], "nats consumer rm ORDER_EVENTS "+durable) {
				t.Errorf("warnings = %v, want one about %s naming the registration and the command", warnings, tt.setting)
			}
			cfg := keptConsumer(t, app, "ORDER_EVENTS", durable).Config
			if cfg.DeliverPolicy != jetstream.DeliverAllPolicy || cfg.AckPolicy != jetstream.AckExplicitPolicy ||
				cfg.ReplayPolicy != jetstream.ReplayInstantPolicy || cfg.MaxWaiting != 512 || cfg.MemoryStorage {
				t.Errorf("consumer changed: %+v", cfg)
			}
		})
	}
}

// TestIntegration_StreamConsumerService_AckPolicyKept records what keeping
// the ack policy means for the handler.
func TestIntegration_StreamConsumerService_AckPolicyKept(t *testing.T) {
	ctx := context.Background()

	t.Run("explicit kept, handler stops acknowledging", func(t *testing.T) {
		dir := t.TempDir()
		_, _, stop, err := startKept(t, dir, &keptSettingModule{ack: true})
		stop()
		if err != nil {
			t.Fatalf("first Start: %v", err)
		}
		var handled atomic.Int64
		app, log, _, err := startKept(t, dir, &keptSettingModule{
			consumer: mono.ConsumerConfig{AckPolicy: mono.AckNonePolicy, AckWait: time.Second, MaxDeliver: 3}, handled: &handled,
		})
		if err != nil {
			t.Fatalf("Start with AckNone configured: %v", err)
		}
		if got := keptWarnings(log); len(got) != 1 || !strings.Contains(got[0], "AckPolicy") || !strings.Contains(got[0], "keep acknowledging") {
			t.Errorf("warnings = %v, want one about AckPolicy", got)
		}
		if _, err := helper.PublishStreamConsumerService(ctx, app.Services("orders"), "ingest", json.Marshal, 1); err != nil {
			t.Fatalf("publish: %v", err)
		}
		// Not acknowledged under the kept explicit policy: redelivered
		// after AckWait, up to MaxDeliver.
		deadline := time.Now().Add(10 * time.Second)
		for handled.Load() < 3 && time.Now().Before(deadline) {
			time.Sleep(50 * time.Millisecond)
		}
		time.Sleep(1500 * time.Millisecond)
		t.Logf("one message, handler without Ack, explicit policy kept: handled %d times", handled.Load())
		if handled.Load() != 3 {
			t.Errorf("handled %d times, want 3 (MaxDeliver)", handled.Load())
		}
	})

	t.Run("none kept, explicit configured", func(t *testing.T) {
		dir := t.TempDir()
		_, _, stop, err := startKept(t, dir, &keptSettingModule{consumer: mono.ConsumerConfig{AckPolicy: mono.AckNonePolicy}, ack: true})
		stop()
		if err != nil {
			t.Fatalf("first Start with AckNone: %v", err)
		}
		// The explicit configuration carries the framework's AckWait and
		// MaxDeliver defaults; the kept AckNone accepts them.
		app, log, _, err := startKept(t, dir, &keptSettingModule{ack: true})
		if err != nil {
			t.Fatalf("Start with explicit configured: %v", err)
		}
		if got := keptWarnings(log); len(got) != 1 || !strings.Contains(got[0], "AckPolicy") {
			t.Errorf("warnings = %v, want one about AckPolicy", got)
		}
		if got := keptConsumer(t, app, "ORDERS", "orders-ingest").Config.AckPolicy; got != jetstream.AckNonePolicy {
			t.Errorf("AckPolicy = %v, want none kept", got)
		}
	})
}

// TestIntegration_StreamConsumerService_ImmutableSettingKeptDuringConversion:
// a non-durable consumer of a version up to v0.1.x is converted to a durable
// while its configuration changes an immutable setting. Both updates of the
// conversion keep the existing value.
func TestIntegration_StreamConsumerService_ImmutableSettingKeptDuringConversion(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	helperApp, _, stopHelper, err := startKept(t, dir, &keptSettingModule{event: true, ack: true})
	if err != nil {
		t.Fatalf("helper Start: %v", err)
	}
	es, err := helperApp.EventBus("orders").EventStream()
	if err != nil {
		t.Fatalf("EventStream: %v", err)
	}
	if _, err := es.CreateOrUpdateStream(ctx, mono.StreamConfig{
		Name: "ORDERS", Subjects: []string{"services.orders.ingest", "services.orders.ingest.>"},
	}); err != nil {
		t.Fatalf("create the stream: %v", err)
	}
	if _, err := es.CreateOrUpdateConsumer(ctx, "ORDERS", mono.ConsumerConfig{
		Name: "orders-ingest", AckPolicy: mono.AckExplicitPolicy, InactiveThreshold: time.Hour,
	}); err != nil {
		t.Fatalf("create the non-durable consumer: %v", err)
	}
	stopHelper()

	app, log, _, err := startKept(t, dir, &keptSettingModule{consumer: mono.ConsumerConfig{DeliverPolicy: mono.DeliverNewPolicy}, ack: true})
	if err != nil {
		t.Fatalf("Start with the changed deliver policy: %v", err)
	}
	if got := keptWarnings(log); len(got) != 1 || !strings.Contains(got[0], "DeliverPolicy") {
		t.Errorf("warnings = %v, want one about DeliverPolicy", got)
	}
	cfg := keptConsumer(t, app, "ORDERS", "orders-ingest").Config
	if cfg.Durable != "orders-ingest" || cfg.DeliverPolicy != jetstream.DeliverAllPolicy {
		t.Errorf("consumer = durable %q, deliver %v; want it converted with deliver policy all kept", cfg.Durable, cfg.DeliverPolicy)
	}
}
