//go:build integration
// +build integration

package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-monolith/mono"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// These tests cover the stable durable names of event stream consumers and the
// migration from the durables that mono v0.0.11 and earlier named
// "<event-module>-<event>-<version>-<sequence>", where the sequence followed
// the (non-deterministic) module start order.

var (
	namingPaymentProcessedV1 = mono.BaseEventDefinition{
		ModuleName: "billing", Name: "PaymentProcessed", Version: "v1",
		Subject: "events.billing.v1.payment-processed",
	}
	namingPaymentNotificationV1 = mono.BaseEventDefinition{
		ModuleName: "billing", Name: "PaymentProcessedNotification", Version: "v1",
		Subject: "events.billing.v1.payment-processed",
	}
	namingSubscriptionChangedV1 = mono.BaseEventDefinition{
		ModuleName: "billing", Name: "SubscriptionChanged", Version: "v1",
		Subject: "events.billing.v1.subscription-changed",
	}
	namingOrderCreatedV1 = mono.BaseEventDefinition{
		ModuleName: "orders", Name: "OrderCreated", Version: "v1",
		Subject: "events.orders.v1.created",
	}
)

// namingEmitterModule declares the events and exposes raw JetStream access to
// the test through the framework's own NATS connection.
type namingEmitterModule struct {
	eventBus mono.EventBus
}

func (m *namingEmitterModule) Name() string                  { return "billing" }
func (m *namingEmitterModule) Start(_ context.Context) error { return nil }
func (m *namingEmitterModule) Stop(_ context.Context) error  { return nil }
func (m *namingEmitterModule) SetEventBus(bus mono.EventBus) { m.eventBus = bus }

func (m *namingEmitterModule) EmitEvents() []mono.BaseEventDefinition {
	return []mono.BaseEventDefinition{
		namingPaymentProcessedV1, namingPaymentNotificationV1,
		namingSubscriptionChangedV1, namingOrderCreatedV1,
	}
}

func (m *namingEmitterModule) jetStream(t *testing.T) jetstream.JetStream {
	t.Helper()
	provider, ok := m.eventBus.(interface{ Conn() *nats.Conn })
	if !ok {
		t.Fatal("event bus does not expose its NATS connection")
	}
	js, err := jetstream.New(provider.Conn())
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	return js
}

// namingConsumerModule registers one event stream consumer and records the
// integer payload of every message it receives.
type namingConsumerModule struct {
	name      string
	event     mono.BaseEventDefinition
	stream    string
	retention mono.RetentionPolicy
	deliver   mono.DeliverPolicy

	mu       sync.Mutex
	received []int
}

func (m *namingConsumerModule) Name() string                  { return m.name }
func (m *namingConsumerModule) Start(_ context.Context) error { return nil }
func (m *namingConsumerModule) Stop(_ context.Context) error  { return nil }

func (m *namingConsumerModule) RegisterEventConsumers(registry mono.EventRegistry) error {
	return registry.RegisterEventStreamConsumer(m.event, mono.StreamConsumerConfig{
		Stream:   mono.StreamConfig{Name: m.stream, Retention: m.retention},
		Consumer: mono.ConsumerConfig{MaxDeliver: 5, DeliverPolicy: m.deliver},
		Fetch:    mono.FetchConfig{BatchSize: 10, Timeout: 200 * time.Millisecond},
	}, m.handle, m)
}

func (m *namingConsumerModule) handle(_ context.Context, msgs []*mono.Msg) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, msg := range msgs {
		n, err := strconv.Atoi(string(msg.Data))
		if err != nil {
			return fmt.Errorf("unexpected payload %q: %w", msg.Data, err)
		}
		m.received = append(m.received, n)
		if err := msg.Ack(); err != nil {
			return err
		}
	}
	return nil
}

func (m *namingConsumerModule) takeReceived() []int {
	m.mu.Lock()
	defer m.mu.Unlock()
	received := m.received
	m.received = nil
	return received
}

// syncBuffer is a goroutine-safe log sink.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// slowStartModule delays startup, like a module running migrations, so the
// framework sets up consumers well after the embedded server recovered them.
type slowStartModule struct{ delay time.Duration }

func (m *slowStartModule) Name() string { return "slow-start" }
func (m *slowStartModule) Start(_ context.Context) error {
	time.Sleep(m.delay)
	return nil
}
func (m *slowStartModule) Stop(_ context.Context) error { return nil }

// assertDurable fails unless the named consumer is a real durable.
func assertDurable(ctx context.Context, t *testing.T, js jetstream.JetStream, stream, name string) {
	t.Helper()
	consumer, err := js.Consumer(ctx, stream, name)
	if err != nil {
		t.Fatalf("consumer %s: %v", name, err)
	}
	if cfg := consumer.CachedInfo().Config; cfg.Durable != name || cfg.InactiveThreshold != 0 {
		t.Errorf("consumer %s: durable=%q inactive_threshold=%v, want durable %q with no threshold", name, cfg.Durable, cfg.InactiveThreshold, name)
	}
}

// runNamingApp starts an application on storeDir with the given modules and
// returns a function that stops it.
func runNamingApp(t *testing.T, storeDir string, logs *syncBuffer, modules ...mono.Module) func() {
	t.Helper()
	opts := []mono.MonoFrameworkOption{
		mono.WithNATSDontListen(),
		mono.WithNATSInProcessConn(),
		mono.WithJetStreamDomain("naming"),
		mono.WithJetStreamStorageDir(storeDir),
	}
	if logs != nil {
		opts = append(opts, mono.WithLogOutput(logs))
	}
	app, err := mono.NewMonoApplication(opts...)
	if err != nil {
		t.Fatalf("NewMonoApplication: %v", err)
	}
	for _, m := range modules {
		if err := app.Register(m); err != nil {
			t.Fatalf("Register %s: %v", m.Name(), err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := app.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer stopCancel()
		if err := app.Stop(stopCtx); err != nil {
			t.Errorf("Stop: %v", err)
		}
	}
	t.Cleanup(stop)
	return stop
}

// createLegacyDurable creates a pull consumer the way mono v0.0.11 did: with a
// Name but no Durable, which the server treats as a named non-durable
// consumer. It survives restarts but is deleted after 5s without pull
// requests within one server run, so fixtures consume from it right away.
func createLegacyDurable(ctx context.Context, t *testing.T, js jetstream.JetStream, stream, name string, deliver jetstream.DeliverPolicy) {
	t.Helper()
	_, err := js.CreateOrUpdateConsumer(ctx, stream, jetstream.ConsumerConfig{
		Name:          name,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: deliver,
		MaxDeliver:    5,
	})
	if err != nil {
		t.Fatalf("create legacy durable %s: %v", name, err)
	}
}

// consumeAndAck fetches n messages through a durable and acknowledges them.
func consumeAndAck(ctx context.Context, t *testing.T, js jetstream.JetStream, stream, name string, n int) {
	t.Helper()
	consumer, err := js.Consumer(ctx, stream, name)
	if err != nil {
		t.Fatalf("consumer %s: %v", name, err)
	}
	batch, err := consumer.Fetch(n, jetstream.FetchMaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("fetch from %s: %v", name, err)
	}
	got := 0
	for msg := range batch.Messages() {
		if err := msg.DoubleAck(ctx); err != nil {
			t.Fatalf("ack on %s: %v", name, err)
		}
		got++
	}
	if got != n {
		t.Fatalf("fetched %d messages from %s, want %d", got, name, n)
	}
}

func publishSequence(ctx context.Context, t *testing.T, js jetstream.JetStream, subject string, from, to int) {
	t.Helper()
	for i := from; i <= to; i++ {
		if _, err := js.Publish(ctx, subject, []byte(strconv.Itoa(i))); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
}

func consumerNames(ctx context.Context, t *testing.T, js jetstream.JetStream, stream string) []string {
	t.Helper()
	s, err := js.Stream(ctx, stream)
	if err != nil {
		t.Fatalf("stream %s: %v", stream, err)
	}
	lister := s.ConsumerNames(ctx)
	var names []string
	for name := range lister.Name() {
		names = append(names, name)
	}
	if err := lister.Err(); err != nil {
		t.Fatalf("list consumers on %s: %v", stream, err)
	}
	slices.Sort(names)
	return names
}

// waitReceived waits until the module has received want and then a quiet
// period passes without further deliveries.
func waitReceived(t *testing.T, m *namingConsumerModule, want []int) {
	t.Helper()
	var got []int
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && len(got) < len(want) {
		got = append(got, m.takeReceived()...)
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(700 * time.Millisecond) // several fetch timeouts: catch redeliveries
	got = append(got, m.takeReceived()...)
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Fatalf("%s received %v, want %v", m.name, got, want)
	}
}

func seq(from, to int) []int {
	var s []int
	for i := from; i <= to; i++ {
		s = append(s, i)
	}
	return s
}

// TestEventStreamConsumerNaming_MigratesLegacyDurables reproduces
// myspecs/myspec-monorepo#1616: analytics' durable flip-flopped between "-1"
// and "-2" across restarts on an interest-retention stream that a DeliverNew
// notification consumer shares. After upgrading, each consumer gets a stable
// durable that resumes where its legacy durables left off, and restarts
// neither rename nor replay.
func TestEventStreamConsumerNaming_MigratesLegacyDurables(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	storeDir := t.TempDir()
	const stream = "analytics-payment-processed"
	subject := namingPaymentProcessedV1.Subject

	// v0.0.11 state: two flip-flopped analytics durables and a caught-up
	// DeliverNew notification durable.
	emitter := &namingEmitterModule{}
	stop := runNamingApp(t, storeDir, nil, emitter)
	js := emitter.jetStream(t)
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: stream, Subjects: []string{subject},
		Retention: jetstream.InterestPolicy, Storage: jetstream.FileStorage,
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	createLegacyDurable(ctx, t, js, stream, "billing-PaymentProcessed-v1-1", jetstream.DeliverAllPolicy)
	createLegacyDurable(ctx, t, js, stream, "billing-PaymentProcessed-v1-2", jetstream.DeliverAllPolicy)
	createLegacyDurable(ctx, t, js, stream, "billing-PaymentProcessedNotification-v1-7", jetstream.DeliverNewPolicy)
	publishSequence(ctx, t, js, subject, 1, 15)
	consumeAndAck(ctx, t, js, stream, "billing-PaymentProcessed-v1-1", 10)
	consumeAndAck(ctx, t, js, stream, "billing-PaymentProcessed-v1-2", 5)
	consumeAndAck(ctx, t, js, stream, "billing-PaymentProcessedNotification-v1-7", 15)
	stop()

	legacy := []string{
		"billing-PaymentProcessed-v1-1",
		"billing-PaymentProcessed-v1-2",
		"billing-PaymentProcessedNotification-v1-7",
	}
	wantConsumers := slices.Sorted(slices.Values(append(slices.Clone(legacy),
		"analytics-billing-PaymentProcessed-v1",
		"notification-billing-PaymentProcessedNotification-v1")))

	for boot := 1; boot <= 3; boot++ {
		logs := &syncBuffer{}
		emitter := &namingEmitterModule{}
		analytics := &namingConsumerModule{
			name: "analytics", event: namingPaymentProcessedV1,
			stream: stream, retention: mono.InterestPolicy,
		}
		notification := &namingConsumerModule{
			name: "notification", event: namingPaymentNotificationV1,
			stream: stream, retention: mono.InterestPolicy, deliver: mono.DeliverNewPolicy,
		}
		modules := []mono.Module{emitter, analytics, notification}
		if boot == 1 {
			// Consumers are set up long after the server recovered the
			// legacy consumers; they must still be found and carried over.
			modules = append(modules, &slowStartModule{delay: 7 * time.Second})
		}
		stop := runNamingApp(t, storeDir, logs, modules...)
		js := emitter.jetStream(t)
		assertDurable(ctx, t, js, stream, "analytics-billing-PaymentProcessed-v1")
		assertDurable(ctx, t, js, stream, "notification-billing-PaymentProcessedNotification-v1")

		if boot == 1 {
			// Resumes after the highest legacy ack floor (10), not from 1.
			waitReceived(t, analytics, seq(11, 15))
		} else {
			waitReceived(t, analytics, nil)
		}
		waitReceived(t, notification, nil)

		next := 15 + boot
		publishSequence(ctx, t, js, subject, next, next)
		waitReceived(t, analytics, []int{next})
		waitReceived(t, notification, []int{next})

		if got := consumerNames(ctx, t, js, stream); !slices.Equal(got, wantConsumers) {
			t.Fatalf("boot %d: consumers = %v, want %v", boot, got, wantConsumers)
		}
		for _, name := range legacy {
			if want := "nats consumer rm " + stream + " " + name; !bytes.Contains([]byte(logs.String()), []byte(want)) {
				t.Errorf("boot %d: missing warning %q", boot, want)
			}
		}
		stop()
	}
}

// TestEventStreamConsumerNaming_WorkQueueLegacyDurable covers a work-queue
// stream, where a second unfiltered consumer is rejected: the legacy durable
// is replaced and its unacknowledged messages are delivered exactly once.
func TestEventStreamConsumerNaming_WorkQueueLegacyDurable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	storeDir := t.TempDir()
	const stream = "orders-work"
	subject := namingOrderCreatedV1.Subject

	emitter := &namingEmitterModule{}
	stop := runNamingApp(t, storeDir, nil, emitter)
	js := emitter.jetStream(t)
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: stream, Subjects: []string{subject},
		Retention: jetstream.WorkQueuePolicy, Storage: jetstream.FileStorage,
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	createLegacyDurable(ctx, t, js, stream, "orders-OrderCreated-v1-3", jetstream.DeliverAllPolicy)
	publishSequence(ctx, t, js, subject, 1, 6)
	consumeAndAck(ctx, t, js, stream, "orders-OrderCreated-v1-3", 2)
	stop()

	for boot := 1; boot <= 2; boot++ {
		emitter := &namingEmitterModule{}
		fulfilment := &namingConsumerModule{
			name: "fulfilment", event: namingOrderCreatedV1,
			stream: stream, retention: mono.WorkQueuePolicy,
		}
		stop := runNamingApp(t, storeDir, nil, emitter, fulfilment)
		js := emitter.jetStream(t)
		if boot == 1 {
			waitReceived(t, fulfilment, seq(3, 6))
		} else {
			waitReceived(t, fulfilment, nil)
		}
		if got := consumerNames(ctx, t, js, stream); !slices.Equal(got, []string{"fulfilment-orders-OrderCreated-v1"}) {
			t.Fatalf("boot %d: consumers = %v", boot, got)
		}
		stop()
	}
}

// TestEventStreamConsumerNaming_SharedLegacyDurables covers two modules
// consuming the same event on one stream. Their legacy durables cannot be
// told apart, so both resume after the lowest ack floor and lose nothing. The
// stream uses limits retention so that a replay from the start would show.
func TestEventStreamConsumerNaming_SharedLegacyDurables(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	storeDir := t.TempDir()
	const stream = "payments-shared"
	subject := namingPaymentProcessedV1.Subject

	emitter := &namingEmitterModule{}
	stop := runNamingApp(t, storeDir, nil, emitter)
	js := emitter.jetStream(t)
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: stream, Subjects: []string{subject},
		Retention: jetstream.LimitsPolicy, Storage: jetstream.FileStorage,
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	createLegacyDurable(ctx, t, js, stream, "billing-PaymentProcessed-v1-1", jetstream.DeliverAllPolicy)
	createLegacyDurable(ctx, t, js, stream, "billing-PaymentProcessed-v1-2", jetstream.DeliverAllPolicy)
	publishSequence(ctx, t, js, subject, 1, 12)
	consumeAndAck(ctx, t, js, stream, "billing-PaymentProcessed-v1-1", 10)
	consumeAndAck(ctx, t, js, stream, "billing-PaymentProcessed-v1-2", 4)
	stop()

	emitter = &namingEmitterModule{}
	analytics := &namingConsumerModule{name: "analytics", event: namingPaymentProcessedV1, stream: stream, retention: mono.LimitsPolicy}
	ledger := &namingConsumerModule{name: "ledger", event: namingPaymentProcessedV1, stream: stream, retention: mono.LimitsPolicy}
	runNamingApp(t, storeDir, nil, emitter, analytics, ledger)
	waitReceived(t, analytics, seq(5, 12))
	waitReceived(t, ledger, seq(5, 12))
}

// TestEventStreamConsumerNaming_StableAcrossRestarts covers the shape of the
// reported bug without any legacy state: two independent modules, each with a
// stream consumer, keep the same durables across restarts, and a message is
// delivered once in total.
func TestEventStreamConsumerNaming_StableAcrossRestarts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	storeDir := t.TempDir()

	for boot := 1; boot <= 5; boot++ {
		emitter := &namingEmitterModule{}
		analytics := &namingConsumerModule{name: "analytics", event: namingPaymentProcessedV1, stream: "analytics-stream", retention: mono.InterestPolicy}
		usage := &namingConsumerModule{name: "usage", event: namingSubscriptionChangedV1, stream: "usage-stream", retention: mono.InterestPolicy}
		// Alternate registration order: names must not depend on it.
		modules := []mono.Module{emitter, analytics, usage}
		if boot%2 == 0 {
			modules = []mono.Module{usage, analytics, emitter}
		}
		stop := runNamingApp(t, storeDir, nil, modules...)
		js := emitter.jetStream(t)

		if boot == 1 {
			publishSequence(ctx, t, js, namingPaymentProcessedV1.Subject, 1, 1)
			publishSequence(ctx, t, js, namingSubscriptionChangedV1.Subject, 1, 1)
			waitReceived(t, analytics, []int{1})
			waitReceived(t, usage, []int{1})
		} else {
			waitReceived(t, analytics, nil)
			waitReceived(t, usage, nil)
		}

		for stream, want := range map[string]string{
			"analytics-stream": "analytics-billing-PaymentProcessed-v1",
			"usage-stream":     "usage-billing-SubscriptionChanged-v1",
		} {
			if got := consumerNames(ctx, t, js, stream); !slices.Equal(got, []string{want}) {
				t.Fatalf("boot %d: consumers on %s = %v, want [%s]", boot, stream, got, want)
			}
		}
		stop()
	}
}

// TestEventStreamConsumerNaming_ConfiguredStartLaterThanLegacyFloor covers a
// consumer configured to start after the legacy ack floor: the configured
// start wins and startup succeeds.
func TestEventStreamConsumerNaming_ConfiguredStartLaterThanLegacyFloor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	storeDir := t.TempDir()
	const stream = "payments-start-seq"
	subject := namingPaymentProcessedV1.Subject

	emitter := &namingEmitterModule{}
	stop := runNamingApp(t, storeDir, nil, emitter)
	js := emitter.jetStream(t)
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: stream, Subjects: []string{subject}, Storage: jetstream.FileStorage,
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	createLegacyDurable(ctx, t, js, stream, "billing-PaymentProcessed-v1-1", jetstream.DeliverAllPolicy)
	publishSequence(ctx, t, js, subject, 1, 10)
	consumeAndAck(ctx, t, js, stream, "billing-PaymentProcessed-v1-1", 3)
	stop()

	emitter = &namingEmitterModule{}
	analytics := &startSeqConsumerModule{namingConsumerModule: namingConsumerModule{
		name: "analytics", event: namingPaymentProcessedV1, stream: stream,
	}, startSeq: 8}
	runNamingApp(t, storeDir, nil, emitter, analytics)
	waitReceived(t, &analytics.namingConsumerModule, seq(8, 10))
}

// startSeqConsumerModule is a namingConsumerModule that starts at a fixed
// stream sequence.
type startSeqConsumerModule struct {
	namingConsumerModule
	startSeq uint64
}

func (m *startSeqConsumerModule) RegisterEventConsumers(registry mono.EventRegistry) error {
	return registry.RegisterEventStreamConsumer(m.event, mono.StreamConsumerConfig{
		Stream: mono.StreamConfig{Name: m.stream},
		Consumer: mono.ConsumerConfig{
			MaxDeliver:    5,
			DeliverPolicy: mono.DeliverByStartSequencePolicy,
			OptStartSeq:   m.startSeq,
		},
		Fetch: mono.FetchConfig{BatchSize: 10, Timeout: 200 * time.Millisecond},
	}, m.handle, m)
}
