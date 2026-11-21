package lifecycle

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-monolith/mono/internal/registry"
	"github.com/go-monolith/mono/pkg/types"
)

// OverloadedError is a handler's own error whose type name, lower-cased and
// without its Error suffix, is the framework's "overloaded".
type OverloadedError struct{}

func (OverloadedError) Error() string { return "the warehouse is overloaded" }

// ValidationError is an ordinary handler error type.
type ValidationError struct{}

func (*ValidationError) Error() string { return "invalid" }

// endpointLastErrors returns the last error text of every request-reply
// endpoint, as the services protocol statistics report it.
func (lm *lifecycleManager) endpointLastErrors() []string {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	var texts []string
	for _, r := range lm.subRecords {
		if r.metrics != nil {
			_, _, lastError, _, _, _, _, _ := r.metrics.snapshot()
			texts = append(texts, lastError)
		}
	}
	return texts
}

// requestReplyFixture starts a lifecycle manager with one request-reply
// service and returns the subscription callback, the bus and the logger.
func requestReplyFixture(t *testing.T, handler types.RequestReplyHandler) (LifecycleManager, types.MsgHandler, *mockEventBus, *mockLogger) {
	t.Helper()
	logger := &mockLogger{}
	reg := registry.NewModuleRegistry(logger)
	eventBus := newMockEventBus()
	lm := NewLifecycleManager(reg, registry.NewPluginRegistry(logger), eventBus, registry.NewEventRegistry(logger), logger, 0)
	lm.SetServiceDiscovery(&types.ServiceDiscoveryConfig{})
	module := &mockModuleWithServices{
		mockModule: mockModule{name: "orders"},
		serviceEntries: []*types.ServiceEntry{{
			Name: "place", Type: types.ServiceTypeRequestReply,
			Subject: "services.orders.place", QueueGroup: "orders", RequestHandler: handler,
		}},
	}
	if err := reg.Register(module); err != nil {
		t.Fatal(err)
	}
	if err := lm.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lm.Stop(context.Background()) })
	return lm, eventBus.getHandlers("services.orders.place")[0], eventBus, logger
}

func request(reply, data string) *types.Msg {
	return &types.Msg{Subject: "services.orders.place", Reply: reply, Data: []byte(data)}
}

// TestRequestReply_PanicValueStaysInTheLog: what a handler panicked with (an
// internal value, perhaps with data of another caller) goes to the log, not
// to the caller and not into the endpoint statistics any client can read.
func TestRequestReply_PanicValueStaysInTheLog(t *testing.T) {
	lm, handler, bus, logger := requestReplyFixture(t, func(context.Context, *types.Msg) ([]byte, error) {
		panic("card 4111-1111-1111-1111 of customer 42")
	})
	handler(context.Background(), request("reply-1", "x"))
	eventually(t, func() bool { return len(bus.getPublishedMsgs("reply-1")) == 1 }, "no error reply")
	requireErrorReply(t, bus, "reply-1", types.ErrorTypeHandlerPanic)
	if got := bus.getPublishedMsgs("reply-1")[0].Header.Get(types.HeaderErrorMessage); strings.Contains(got, "4111") || got != "handler panicked" {
		t.Errorf("error message sent to the caller = %q, want a fixed text", got)
	}
	if !logger.hasErrorContaining("4111-1111-1111-1111") {
		t.Error("the panic value is not in the log")
	}
	for _, stats := range lm.(*lifecycleManager).endpointLastErrors() {
		if strings.Contains(stats, "4111") {
			t.Errorf("the panic value is in the endpoint statistics: %q", stats)
		}
	}
}

// TestRequestReply_HandlerErrorTypeNeverAFrameworkType: a handler's own error
// must not arrive as one of the types the framework answers with, which mean
// that the handler did not run.
func TestRequestReply_HandlerErrorTypeNeverAFrameworkType(t *testing.T) {
	_, handler, bus, _ := requestReplyFixture(t, func(context.Context, *types.Msg) ([]byte, error) {
		return nil, OverloadedError{}
	})
	handler(context.Background(), request("reply-1", "x"))
	eventually(t, func() bool { return len(bus.getPublishedMsgs("reply-1")) == 1 }, "no error reply")
	got := bus.getPublishedMsgs("reply-1")[0].Header.Get(types.HeaderErrorType)
	if got == types.ErrorTypeOverloaded || got == types.ErrorTypeShuttingDown || got == types.ErrorTypeHandlerPanic {
		t.Fatalf("the handler's OverloadedError is sent as the framework's %q", got)
	}
	if got != "overloaded-error" {
		t.Errorf("error type = %q, want overloaded-error", got)
	}
	for err, want := range map[error]string{
		errors.New("plain"):          "errorstring",
		&ValidationError{}:           "validation",
		OverloadedError{}:            "overloaded-error",
		&handlerPanicError{Value: 1}: types.ErrorTypeHandlerPanic,
	} {
		if got := HandlerErrorType(err); got != want {
			t.Errorf("HandlerErrorType(%T) = %q, want %q", err, got, want)
		}
	}
}

// TestRequestReply_AlwaysAnswers: a caller that asked for a reply gets one
// whatever the handler returned, instead of waiting for its timeout.
func TestRequestReply_AlwaysAnswers(t *testing.T) {
	t.Run("a nil response is an empty reply", func(t *testing.T) {
		_, handler, bus, logger := requestReplyFixture(t, func(context.Context, *types.Msg) ([]byte, error) { return nil, nil })
		handler(context.Background(), request("reply-1", "x"))
		eventually(t, func() bool { return len(bus.getPublishedMessages("reply-1")) == 1 }, "a handler returning (nil, nil) left the caller without a reply")
		if got := bus.getPublishedMessages("reply-1")[0]; len(got) != 0 {
			t.Errorf("reply = %q, want empty", got)
		}
		if logger.hasWarnContaining("No reply subject") {
			t.Error("a nil response was logged as a missing reply subject")
		}
	})

	t.Run("a response that cannot be published is an error reply", func(t *testing.T) {
		lm, handler, bus, logger := requestReplyFixture(t, func(context.Context, *types.Msg) ([]byte, error) { return []byte("too large"), nil })
		bus.setPublishDataError(errors.New("nats: maximum payload exceeded"))
		handler(context.Background(), request("reply-1", "x"))
		eventually(t, func() bool { return len(bus.getPublishedMsgs("reply-1")) == 1 }, "a response that could not be published left the caller without a reply")
		requireErrorReply(t, bus, "reply-1", types.ErrorTypeReplyFailed)
		if got := bus.getPublishedMsgs("reply-1")[0].Header.Get(types.HeaderErrorMessage); !strings.Contains(got, "maximum payload exceeded") {
			t.Errorf("error message = %q, want the reason", got)
		}
		if !logger.hasErrorContaining("Failed to publish response") {
			t.Error("the failed publish was not logged")
		}
		// The caller got an error, so the statistics count one.
		lastErrors := lm.(*lifecycleManager).endpointLastErrors()
		if len(lastErrors) != 1 || !strings.Contains(lastErrors[0], "could not be sent") {
			t.Errorf("last errors of the endpoints = %q, want the failed response", lastErrors)
		}
	})
}

// TestRequestReply_MessagesWithoutAReplySubject: a message published to a
// request-reply subject without a reply subject is handled, but it never
// holds the subscription callback (which would hold up the calls behind it),
// and it is logged at most once a second.
func TestRequestReply_MessagesWithoutAReplySubject(t *testing.T) {
	t.Run("handled, and logged once", func(t *testing.T) {
		handled := make(chan struct{}, 100)
		_, handler, _, logger := requestReplyFixture(t, func(context.Context, *types.Msg) ([]byte, error) {
			handled <- struct{}{}
			return []byte("ok"), nil
		})
		for i := 0; i < 50; i++ {
			handler(context.Background(), request("", "x"))
		}
		eventually(t, func() bool { return len(handled) == 50 }, "the messages were not handled")
		eventually(t, func() bool { return logger.countWarnsContaining("without a reply subject") >= 1 }, "nothing was logged")
		if n := logger.countWarnsContaining("without a reply subject") + logger.countWarnsContaining("No reply subject"); n > 2 {
			t.Errorf("%d warnings for 50 messages within a moment", n)
		}
	})

	t.Run("never blocks the callback when the pool is full", func(t *testing.T) {
		f := newBlockingRequestReplyFixture(t, types.ServiceOptions{MaxConcurrency: 1, MaxPending: 1}, 0)
		defer func() { _ = f.lm.Stop(context.Background()) }()
		defer close(f.release)
		f.send("reply-1") // runs
		f.waitStarted(t)
		f.send("reply-2") // queued

		returned := make(chan struct{})
		go func() {
			f.send("") // the pool and its queue are full
			close(returned)
		}()
		select {
		case <-returned:
		case <-time.After(2 * time.Second):
			t.Fatal("a message without a reply subject blocked the subscription callback")
		}
		// A call that arrives behind it is still answered at once.
		f.send("reply-3")
		requireErrorReply(t, f.eventBus, "reply-3", types.ErrorTypeOverloaded)
		if !f.logger.hasWarnContaining("rejected") {
			t.Error("the dropped message was not reported")
		}
	})
}
