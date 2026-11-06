package lifecycle

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-monolith/mono/internal/registry"
	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
	"github.com/nats-io/nats.go/jetstream"
)

func TestRecoverHandlerPanic(t *testing.T) {
	t.Run("panic becomes an error with the stack", func(t *testing.T) {
		_, err := invokeRequestReplyHandler(context.Background(), func(context.Context, *types.Msg) ([]byte, error) {
			panic("boom")
		}, &types.Msg{})
		if !errors.Is(err, monoerrors.ErrHandlerPanic) {
			t.Fatalf("got %v, want ErrHandlerPanic", err)
		}
		stack, isPanic := panicStack(err)
		if !isPanic || stack == "" {
			t.Fatalf("expected a recovered panic with a stack, got isPanic=%v stack=%q", isPanic, stack)
		}
		if err.Error() != "handler panicked: boom" {
			t.Fatalf("Error() = %q", err.Error())
		}
	})

	t.Run("normal errors pass through untouched", func(t *testing.T) {
		want := errors.New("plain")
		err := invokeMsgHandler(context.Background(), func(context.Context, *types.Msg) error { return want }, &types.Msg{})
		if err != want {
			t.Fatalf("got %v, want the handler's own error", err)
		}
		if _, isPanic := panicStack(err); isPanic {
			t.Fatal("a plain error must not be reported as a panic")
		}
		fields := handlerErrorFields(err)
		if len(fields) != 2 {
			t.Fatalf("fields = %v, want only the error", fields)
		}
	})

	t.Run("batch handler", func(t *testing.T) {
		err := invokeBatchHandler(context.Background(), func(context.Context, []*types.Msg) error { panic(errors.New("batch boom")) }, nil)
		if !errors.Is(err, monoerrors.ErrHandlerPanic) {
			t.Fatalf("got %v, want ErrHandlerPanic", err)
		}
	})
}

// TestHandlerPanic_RequestReply checks that a panicking request-reply handler
// answers the caller with a panic error reply, the panic is logged with its
// stack, and the next request is handled normally.
func TestHandlerPanic_RequestReply(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()
	eventRegistry := registry.NewEventRegistry(logger)
	lm := NewLifecycleManager(reg, pluginReg, eventBus, eventRegistry, logger, 0)

	serviceModule := &mockModuleWithServices{
		mockModule: mockModule{name: "service-module"},
		serviceEntries: []*types.ServiceEntry{{
			Name: "svc", Type: types.ServiceTypeRequestReply,
			Subject:    "services.service-module.svc",
			QueueGroup: "service-module",
			RequestHandler: func(_ context.Context, msg *types.Msg) ([]byte, error) {
				if string(msg.Data) == "panic" {
					panic("nil map write")
				}
				return msg.Data, nil
			},
		}},
	}
	if err := reg.Register(serviceModule); err != nil {
		t.Fatal(err)
	}
	if err := lm.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lm.Stop(context.Background()) }()

	handler := eventBus.getHandlers("services.service-module.svc")[0]
	handler(context.Background(), &types.Msg{Subject: "services.service-module.svc", Reply: "reply-panic", Data: []byte("panic")})
	handler(context.Background(), &types.Msg{Subject: "services.service-module.svc", Reply: "reply-ok", Data: []byte("ok")})

	// The two requests run on different pool workers, so wait for both replies.
	eventually(t, func() bool { return len(eventBus.getPublishedMessages("reply-ok")) == 1 },
		"the request after the panic should be answered")
	eventually(t, func() bool { return len(eventBus.getPublishedMsgs("reply-panic")) == 1 },
		"the panicking request should get an error reply")
	requireErrorReply(t, eventBus, "reply-panic", types.ErrorTypeHandlerPanic)
	if !logger.hasErrorContaining("panic true") || !logger.hasErrorContaining("goroutine") ||
		!logger.hasErrorContaining("subject services.service-module.svc") {
		t.Error("expected the panic to be logged with panic=true, the subject and a stack trace")
	}
}

// TestHandlerPanic_QueueGroupAndEventConsumer checks that a panicking queue
// group or event consumer handler is logged and the next message is handled.
func TestHandlerPanic_QueueGroupAndEventConsumer(t *testing.T) {
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	pluginReg := registry.NewPluginRegistry(logger)
	eventBus := newMockEventBus()
	eventReg := newMockEventRegistry()

	var qgHandled, evHandled atomic.Int32
	panicOn := func(data []byte) {
		if string(data) == "panic" {
			panic("handler bug")
		}
	}
	eventReg.consumerEntries = []types.EventConsumerEntry{{
		EventDef: types.BaseEventDefinition{
			Name: "TestEvent", Version: "v1", ModuleName: "test-producer",
			Subject: "events.test-producer.v1.test-event",
		},
		Handler: func(_ context.Context, msg *types.Msg) error {
			panicOn(msg.Data)
			evHandled.Add(1)
			return nil
		},
		Module:     &mockModule{name: "consumer-module"},
		QueueGroup: "consumers",
	}}
	serviceModule := &mockModuleWithServices{
		mockModule: mockModule{name: "qg-module"},
		serviceEntries: []*types.ServiceEntry{{
			Name: "qg", Type: types.ServiceTypeQueueGroup,
			Subject: "services.qg-module.qg",
			QueueHandlers: []types.QGHP{{
				QueueGroup: "workers",
				Handler: func(_ context.Context, msg *types.Msg) error {
					panicOn(msg.Data)
					qgHandled.Add(1)
					return nil
				},
			}},
		}},
	}
	if err := reg.Register(serviceModule); err != nil {
		t.Fatal(err)
	}
	lm := NewLifecycleManager(reg, pluginReg, eventBus, eventReg, logger, 0)
	if err := lm.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lm.Stop(context.Background()) }()

	qg := eventBus.getHandlers("services.qg-module.qg")[0]
	qg(context.Background(), &types.Msg{Subject: "services.qg-module.qg", Reply: "ack-1", Data: []byte("panic")})
	qg(context.Background(), &types.Msg{Subject: "services.qg-module.qg", Reply: "ack-2", Data: []byte("ok")})
	eventually(t, func() bool { return qgHandled.Load() == 1 }, "the queue group message after the panic should be handled")
	if !logger.hasErrorContaining("QueueGroup handler error") || !logger.hasErrorContaining("panic true") {
		t.Error("expected the queue group panic to be logged with panic=true")
	}

	ev := eventBus.getHandlers("events.test-producer.v1.test-event")[0]
	ev(context.Background(), &types.Msg{Subject: "events.test-producer.v1.test-event", Data: []byte("panic")})
	ev(context.Background(), &types.Msg{Subject: "events.test-producer.v1.test-event", Data: []byte("ok")})
	eventually(t, func() bool { return evHandled.Load() == 1 }, "the event after the panic should be handled")
	if !logger.hasErrorContaining("Event consumer handler error") {
		t.Error("expected the event consumer panic to be logged")
	}
}

// ackTrackingJetStreamMsg records Ack/Nak calls so tests can assert the
// framework leaves acknowledgement to the handler after a panic.
type ackTrackingJetStreamMsg struct {
	mockJetStreamMsg
	acks, naks atomic.Int32
}

func (m *ackTrackingJetStreamMsg) Ack() error { m.acks.Add(1); return nil }
func (m *ackTrackingJetStreamMsg) Nak() error { m.naks.Add(1); return nil }

// TestHandlerPanic_StreamConsumerLoops checks that a panicking batch handler
// does not stop the fetch loop, that the next batch is handled, and that the
// framework neither Acks nor Naks the batch (the handler owns acknowledgement).
func TestHandlerPanic_StreamConsumerLoops(t *testing.T) {
	run := func(t *testing.T, start func(lm *lifecycleManager, ctx context.Context, consumer jetstream.Consumer, handler func(context.Context, []*types.Msg) error), logText string) {
		t.Helper()
		logger := &mockLogger{}
		msg := &ackTrackingJetStreamMsg{mockJetStreamMsg: mockJetStreamMsg{data: []byte("m")}}
		consumer := &mockJetStreamConsumer{fetchMessages: []jetstream.Msg{msg}}

		var calls atomic.Int32
		var mu sync.Mutex
		handled := 0
		handler := func(_ context.Context, batch []*types.Msg) error {
			if calls.Add(1) == 1 {
				panic("first batch panics")
			}
			mu.Lock()
			handled++
			mu.Unlock()
			return nil
		}

		lm := &lifecycleManager{
			logger:          logger,
			streamConsumers: make(map[string]context.CancelFunc),
			runtimeCtx:      context.Background(),
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			start(lm, ctx, consumer, handler)
			close(done)
		}()

		eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return handled >= 1
		}, "the loop should keep fetching and handle the batch after the panic")
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("loop did not stop")
		}

		if !logger.hasErrorContaining(logText) || !logger.hasErrorContaining("panic true") {
			t.Errorf("expected %q with panic=true in the log", logText)
		}
		if msg.acks.Load() != 0 || msg.naks.Load() != 0 {
			t.Errorf("framework acked %d / naked %d messages after the panic; acknowledgement belongs to the handler",
				msg.acks.Load(), msg.naks.Load())
		}
	}

	t.Run("stream consumer", func(t *testing.T) {
		run(t, func(lm *lifecycleManager, ctx context.Context, consumer jetstream.Consumer, handler func(context.Context, []*types.Msg) error) {
			entry := &types.ServiceEntry{
				Name: "s", ModuleName: "m",
				StreamConsumerConfig:  &types.StreamConsumerConfig{Fetch: types.FetchConfig{BatchSize: 1, Timeout: 50 * time.Millisecond}},
				StreamConsumerHandler: handler,
			}
			lm.runStreamConsumerLoop(ctx, consumer, entry)
		}, "Handler error")
	})

	t.Run("event stream consumer", func(t *testing.T) {
		run(t, func(lm *lifecycleManager, ctx context.Context, consumer jetstream.Consumer, handler func(context.Context, []*types.Msg) error) {
			entry := types.EventStreamConsumerEntry{
				EventDef: types.BaseEventDefinition{Name: "E", ModuleName: "p", Subject: "events.p.v1.e"},
				Config:   types.StreamConsumerConfig{Fetch: types.FetchConfig{BatchSize: 1, Timeout: 50 * time.Millisecond}},
				Handler:  handler,
				Module:   &mockModule{name: "consumer"},
			}
			lm.runEventStreamConsumerLoop(ctx, consumer, entry)
		}, "Event stream consumer handler error")
	})
}

// TestHandlerPanic_Cron checks that a panicking cron handler is Nak'd and the
// panic is logged with its stack.
func TestHandlerPanic_Cron(t *testing.T) {
	logger := &mockLogger{}
	lm := &lifecycleManager{logger: logger}
	entry := &types.ServiceEntry{
		Name:        "nightly",
		CronHandler: func(context.Context, *types.Msg) error { panic("cron bug") },
	}
	msg := &ackTrackingJetStreamMsg{}
	lm.dispatchCronTick(context.Background(), entry, &types.Msg{NatsMsg: msg})

	if msg.naks.Load() != 1 || msg.acks.Load() != 0 {
		t.Fatalf("acks=%d naks=%d, want the occurrence Nak'd once", msg.acks.Load(), msg.naks.Load())
	}
	if !logger.hasErrorContaining("Cron handler error") || !logger.hasErrorContaining("panic true") {
		t.Error("expected the cron panic to be logged with panic=true")
	}
}
