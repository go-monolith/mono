//go:build integration
// +build integration

package integration_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-monolith/mono"
	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/helper"
	"github.com/nats-io/nats.go/jetstream"
)

// These tests cover issue #22: a panic in a handler is recovered, logged, and
// the process, the subscription and the next message survive it.

// TestHandlerPanic_RequestReplyCallerGetsErrorAndServiceSurvives checks that a
// request-reply caller gets ErrHandlerPanic at once (not a timeout) and that
// the service keeps answering afterwards.
func TestHandlerPanic_RequestReplyCallerGetsErrorAndServiceSurvives(t *testing.T) {
	fw := newWorkerPoolApp(t)

	responder := &testModule{name: "crashy"}
	responder.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterRequestReplyService("divide", func(_ context.Context, req *mono.Msg) ([]byte, error) {
			if string(req.Data) == "panic" {
				panic("division by zero in handler")
			}
			return req.Data, nil
		})
	}
	if err := fw.Register(responder); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}
	startWorkerPoolApp(t, fw)

	client, err := responder.container.GetRequestReplyService("divide")
	if err != nil {
		t.Fatalf("Failed to get client: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	start := time.Now()
	_, err = client.Call(ctx, []byte("panic"))
	if !errors.Is(err, monoerrors.ErrHandlerPanic) {
		t.Fatalf("expected ErrHandlerPanic, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("panic reply took %s; the caller should not wait for a timeout", elapsed)
	}
	var svcErr *monoerrors.ServiceError
	if !errors.As(err, &svcErr) || svcErr.ServiceName != "divide" || svcErr.ModuleName != "crashy" {
		t.Fatalf("expected ServiceError for divide/crashy, got %#v", err)
	}

	resp, err := client.Call(ctx, []byte("fine"))
	if err != nil {
		t.Fatalf("call after the panic failed: %v", err)
	}
	if string(resp.Data) != "fine" {
		t.Fatalf("got %q, want fine", resp.Data)
	}
	if health := fw.Health(ctx); !health.Healthy {
		t.Fatalf("application unhealthy after a handler panic: %s", health.Message)
	}
}

// TestHandlerPanic_QueueGroupAndEventConsumerSurvive checks that a panicking
// queue group or event consumer handler does not stop later messages.
func TestHandlerPanic_QueueGroupAndEventConsumerSurvive(t *testing.T) {
	fw := newWorkerPoolApp(t)

	var qgHandled atomic.Int32
	worker := &testModule{name: "pool-worker"}
	worker.registerServicesFunc = func(container mono.ServiceContainer) error {
		return container.RegisterQueueGroupService("jobs", mono.QGHP{
			QueueGroup: "job-workers",
			Handler: func(_ context.Context, msg *mono.Msg) error {
				if string(msg.Data) == "panic" {
					panic("job handler bug")
				}
				qgHandled.Add(1)
				return nil
			},
		})
	}
	emitter := &poolEmitterModule{}
	consumer := &panickingEventConsumer{}
	for _, m := range []mono.Module{worker, emitter, consumer} {
		if err := fw.Register(m); err != nil {
			t.Fatalf("Failed to register %s: %v", m.Name(), err)
		}
	}
	startWorkerPoolApp(t, fw)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := worker.container.GetQueueGroupService("jobs")
	if err != nil {
		t.Fatalf("Failed to get client: %v", err)
	}
	if err := client.Send(ctx, []byte("panic")); err != nil {
		t.Fatalf("send panic: %v", err)
	}
	if err := client.Send(ctx, []byte("ok")); err != nil {
		t.Fatalf("send ok: %v", err)
	}
	waitForCount(t, func() int { return int(qgHandled.Load()) }, 1, 5*time.Second)

	// Event 0 panics in the consumer, event 1 must still be handled.
	for i := 0; i < 2; i++ {
		if err := poolTickEventDef.Publish(emitter.eventBus, poolTickEvent{ID: i}, nil); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	waitForCount(t, func() int { return int(consumer.handled.Load()) }, 1, 5*time.Second)
}

// panickingEventConsumer panics on the event with ID 0 and counts the others.
type panickingEventConsumer struct {
	handled atomic.Int32
}

func (m *panickingEventConsumer) Name() string                  { return "panicking-consumer" }
func (m *panickingEventConsumer) Start(_ context.Context) error { return nil }
func (m *panickingEventConsumer) Stop(_ context.Context) error  { return nil }
func (m *panickingEventConsumer) RegisterEventConsumers(registry mono.EventRegistry) error {
	return registry.RegisterEventConsumer(poolTickEventDef.ToBase(), func(_ context.Context, msg *mono.Msg) error {
		event, err := poolTickEventDef.Unmarshal(msg)
		if err != nil {
			return err
		}
		if event.ID == 0 {
			panic("event handler bug")
		}
		m.handled.Add(1)
		return nil
	}, m)
}

// panickingStreamModule owns a stream consumer whose handler panics on the
// first delivery of a message and acks it on the second.
type panickingStreamModule struct {
	container  mono.ServiceContainer
	deliveries atomic.Int32
	acked      atomic.Int32
}

func (m *panickingStreamModule) Name() string                  { return "panicking-stream" }
func (m *panickingStreamModule) Start(_ context.Context) error { return nil }
func (m *panickingStreamModule) Stop(_ context.Context) error  { return nil }
func (m *panickingStreamModule) RegisterServices(container mono.ServiceContainer) error {
	m.container = container
	return container.RegisterStreamConsumerService("ingest", mono.StreamConsumerConfig{
		Stream: mono.StreamConfig{
			Name:     "PANIC_TEST_STREAM",
			Subjects: []string{"panic-test.>"},
		},
		Consumer: mono.ConsumerConfig{
			AckWait:    time.Second, // short, so the redelivery after the panic is quick
			MaxDeliver: 5,
		},
		Fetch: mono.FetchConfig{BatchSize: 1, Timeout: 500 * time.Millisecond},
	}, func(_ context.Context, msgs []*mono.Msg) error {
		for _, msg := range msgs {
			if m.deliveries.Add(1) == 1 {
				panic("stream handler bug on first delivery")
			}
			if err := msg.Ack(); err != nil {
				return err
			}
			m.acked.Add(1)
		}
		return nil
	})
}

// TestHandlerPanic_StreamConsumerMessageRedelivered checks that a panic in a
// stream consumer handler neither kills the fetch loop nor loses the message:
// it is left unacknowledged and redelivered after AckWait, then handled.
func TestHandlerPanic_StreamConsumerMessageRedelivered(t *testing.T) {
	fw := newWorkerPoolApp(t, mono.WithJetStreamStorageDir(t.TempDir()))

	module := &panickingStreamModule{}
	if err := fw.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}
	startWorkerPoolApp(t, fw)

	client, err := module.container.GetStreamConsumerService("ingest")
	if err != nil {
		t.Fatalf("Failed to get client: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := client.Publish(ctx, []byte("order-1")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	waitForCount(t, func() int { return int(module.acked.Load()) }, 1, 15*time.Second)
	if module.deliveries.Load() < 2 {
		t.Fatalf("deliveries = %d, want at least 2 (panic, then redelivery)", module.deliveries.Load())
	}
}

// These tests cover the remaining handler kinds of issue #22: cron handlers
// (framework-owned ack, so a panic is Nak'd and the occurrence redelivered),
// event stream consumers (handler-owned ack, so the batch comes back after
// AckWait) and callbacks a module subscribes itself with EventBus.Subscribe.

type panickingCronModule struct {
	container  mono.ServiceContainer
	deliveries atomic.Int32
	successes  atomic.Int32
	mu         sync.Mutex
	seen       []cronDelivery // stream sequence and delivery count of every occurrence handled
}

type cronDelivery struct {
	streamSeq    uint64
	numDelivered uint64
	ok           bool
}

func (m *panickingCronModule) record(msg *mono.Msg, ok bool) {
	d := cronDelivery{ok: ok}
	if js, isJS := msg.NatsMsg.(jetstream.Msg); isJS {
		if md, err := js.Metadata(); err == nil {
			d.streamSeq, d.numDelivered = md.Sequence.Stream, md.NumDelivered
		}
	}
	m.mu.Lock()
	m.seen = append(m.seen, d)
	m.mu.Unlock()
}

func (m *panickingCronModule) deliveriesSeen() []cronDelivery {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]cronDelivery(nil), m.seen...)
}

func (m *panickingCronModule) Name() string                  { return "panicking-cron" }
func (m *panickingCronModule) Start(_ context.Context) error { return nil }
func (m *panickingCronModule) Stop(_ context.Context) error  { return nil }
func (m *panickingCronModule) RegisterServices(container mono.ServiceContainer) error {
	m.container = container
	return container.RegisterCronService("tick", mono.CronServiceConfig{Schedule: "@every 1s"}, func(_ context.Context, msg *mono.Msg) error {
		if m.deliveries.Add(1) == 1 {
			m.record(msg, false)
			panic("first occurrence explodes")
		}
		m.record(msg, true)
		m.successes.Add(1)
		return nil
	})
}

// TestHandlerPanic_CronOccurrenceIsNakedAndRedelivered checks that a panic in
// a cron handler is recovered, the occurrence is Nak'd and delivered again,
// and later occurrences keep running.
func TestHandlerPanic_CronOccurrenceIsNakedAndRedelivered(t *testing.T) {
	fw := newWorkerPoolApp(t, mono.WithJetStreamStorageDir(t.TempDir()))
	module := &panickingCronModule{}
	if err := fw.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}
	startWorkerPoolApp(t, fw)

	waitForCount(t, func() int { return int(module.successes.Load()) }, 1, 20*time.Second)
	seen := module.deliveriesSeen()
	if len(seen) < 2 || seen[0].ok {
		t.Fatalf("deliveries = %+v, want the panicking one followed by at least one more", seen)
	}
	// The occurrence that panicked (not merely the next scheduled one) must be
	// the one delivered again: same stream sequence, second delivery.
	if seen[1].streamSeq != seen[0].streamSeq || seen[1].numDelivered != 2 || !seen[1].ok {
		t.Fatalf("second delivery = %+v, want a redelivery (NumDelivered 2) of occurrence %d", seen[1], seen[0].streamSeq)
	}
	if health := fw.Health(context.Background()); !health.Healthy {
		t.Fatalf("application unhealthy after a cron handler panic: %s", health.Message)
	}
}

type boomPayload struct {
	ID int `json:"id"`
}

var boomStreamEventDef = helper.EventDefinition[boomPayload]("boom-stream", "Boom", "v1")

type boomStreamEmitter struct {
	eventBus mono.EventBus
}

func (m *boomStreamEmitter) Name() string                  { return "boom-stream" }
func (m *boomStreamEmitter) Start(_ context.Context) error { return nil }
func (m *boomStreamEmitter) Stop(_ context.Context) error  { return nil }
func (m *boomStreamEmitter) SetEventBus(bus mono.EventBus) { m.eventBus = bus }
func (m *boomStreamEmitter) EmitEvents() []mono.BaseEventDefinition {
	return []mono.BaseEventDefinition{boomStreamEventDef.ToBase()}
}

type boomStreamConsumer struct {
	deliveries atomic.Int32
	acked      atomic.Int32
}

func (m *boomStreamConsumer) Name() string                  { return "boom-stream-consumer" }
func (m *boomStreamConsumer) Start(_ context.Context) error { return nil }
func (m *boomStreamConsumer) Stop(_ context.Context) error  { return nil }
func (m *boomStreamConsumer) RegisterEventConsumers(registry mono.EventRegistry) error {
	config := mono.StreamConsumerConfig{
		Stream:   mono.StreamConfig{Name: "boom-stream", Retention: mono.WorkQueuePolicy},
		Consumer: mono.ConsumerConfig{AckWait: time.Second},
		Fetch:    mono.FetchConfig{BatchSize: 1, Timeout: 2 * time.Second},
	}
	handler := func(_ context.Context, msgs []*mono.Msg) error {
		if m.deliveries.Add(1) == 1 {
			panic("first batch explodes")
		}
		for _, msg := range msgs {
			if err := msg.Ack(); err != nil {
				return err
			}
			m.acked.Add(1)
		}
		return nil
	}
	return registry.RegisterEventStreamConsumer(boomStreamEventDef.ToBase(), config, handler, m)
}

// TestHandlerPanic_EventStreamConsumerBatchRedelivered checks that a panic in
// an event stream consumer handler leaves the batch unacknowledged, so it is
// redelivered after AckWait and handled by the surviving loop.
func TestHandlerPanic_EventStreamConsumerBatchRedelivered(t *testing.T) {
	fw := newWorkerPoolApp(t, mono.WithJetStreamStorageDir(t.TempDir()))
	emitter := &boomStreamEmitter{}
	consumer := &boomStreamConsumer{}
	for _, m := range []mono.Module{emitter, consumer} {
		if err := fw.Register(m); err != nil {
			t.Fatalf("Failed to register %s: %v", m.Name(), err)
		}
	}
	startWorkerPoolApp(t, fw)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	published := time.Now()
	if _, err := boomStreamEventDef.EventStreamPublish(ctx, emitter.eventBus, boomPayload{ID: 1}, nil); err != nil {
		t.Fatalf("publish: %v", err)
	}
	waitForCount(t, func() int { return int(consumer.acked.Load()) }, 1, 15*time.Second)
	if consumer.deliveries.Load() < 2 {
		t.Fatalf("deliveries = %d, want at least 2 (panic, then redelivery after AckWait)", consumer.deliveries.Load())
	}
	// Handler-owned ack: the framework must not Nak after a panic, so the
	// redelivery comes from AckWait, not at fetch speed.
	if elapsed := time.Since(published); elapsed < time.Second {
		t.Fatalf("batch was redelivered after %s, before AckWait (1s) passed: the framework must not Nak a panicking stream handler's batch", elapsed)
	}
}

// TestHandlerPanic_EventBusSubscribeCallbackRecovered checks that a panic in
// a callback a module subscribed itself is recovered by the event bus and the
// subscription keeps delivering.
func TestHandlerPanic_EventBusSubscribeCallbackRecovered(t *testing.T) {
	fw := newWorkerPoolApp(t)
	module := &testModule{name: "self-subscriber"}
	if err := fw.Register(module); err != nil {
		t.Fatalf("Failed to register module: %v", err)
	}
	startWorkerPoolApp(t, fw)

	received := make(chan string, 2)
	sub, err := module.eventBus.Subscribe("selfsub.events", func(_ context.Context, msg *mono.Msg) {
		if string(msg.Data) == "boom" {
			panic("callback bug")
		}
		received <- string(msg.Data)
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	for _, data := range []string{"boom", "fine"} {
		if err := module.eventBus.Publish("selfsub.events", []byte(data)); err != nil {
			t.Fatalf("publish %s: %v", data, err)
		}
	}
	select {
	case got := <-received:
		if got != "fine" {
			t.Fatalf("received %q, want fine", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the subscription stopped delivering after a callback panic")
	}
	if health := fw.Health(context.Background()); !health.Healthy {
		t.Fatalf("application unhealthy after a callback panic: %s", health.Message)
	}
}
