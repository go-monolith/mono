//go:build integration
// +build integration

package integration_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-monolith/mono"
	monoerrors "github.com/go-monolith/mono/pkg/errors"
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
