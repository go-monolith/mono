package container

import (
	"context"
	"errors"
	"testing"
	"time"

	monoerrors "github.com/go-monolith/mono/pkg/errors"
	"github.com/go-monolith/mono/pkg/types"
)

func newBoundContainer(t *testing.T) *serviceContainer {
	t.Helper()
	container := NewServiceContainer(&mockLogger{}).(*serviceContainer)
	if err := container.BindModule(&mockModule{name: "orders"}); err != nil {
		t.Fatalf("BindModule failed: %v", err)
	}
	container.SetEventBus(&mockEventBus{})
	return container
}

func TestRegisterRequestReplyService_Options(t *testing.T) {
	handler := func(_ context.Context, req *types.Msg) ([]byte, error) { return req.Data, nil }

	t.Run("options are stored on the entry", func(t *testing.T) {
		container := newBoundContainer(t)
		err := container.RegisterRequestReplyService("check-stock", handler,
			types.WithMaxConcurrency(8), types.WithMaxPending(32))
		if err != nil {
			t.Fatalf("RegisterRequestReplyService failed: %v", err)
		}
		entry := container.services["check-stock"]
		if entry.Options.MaxConcurrency != 8 || entry.Options.MaxPending != 32 {
			t.Fatalf("Options = %+v, want {8 32}", entry.Options)
		}
	})

	t.Run("no options leave zero values for the framework defaults", func(t *testing.T) {
		container := newBoundContainer(t)
		if err := container.RegisterRequestReplyService("check-stock", handler); err != nil {
			t.Fatalf("RegisterRequestReplyService failed: %v", err)
		}
		if got := container.services["check-stock"].Options; got != (types.ServiceOptions{}) {
			t.Fatalf("Options = %+v, want zero value", got)
		}
	})

	t.Run("negative options are rejected", func(t *testing.T) {
		container := newBoundContainer(t)
		err := container.RegisterRequestReplyService("check-stock", handler, types.WithMaxConcurrency(-1))
		if err == nil {
			t.Fatal("expected an error for MaxConcurrency -1")
		}
		if container.Has("check-stock") {
			t.Fatal("service must not be registered when its options are invalid")
		}
		err = container.RegisterRequestReplyService("check-stock", handler, types.WithMaxPending(-5))
		if err == nil {
			t.Fatal("expected an error for MaxPending -5")
		}
	})

	t.Run("options survive the middleware round-trip", func(t *testing.T) {
		container := newBoundContainer(t)
		container.SetMiddlewareChain(&passThroughChain{})
		err := container.RegisterRequestReplyService("check-stock", handler, types.WithMaxConcurrency(3))
		if err != nil {
			t.Fatalf("RegisterRequestReplyService failed: %v", err)
		}
		if got := container.services["check-stock"].Options.MaxConcurrency; got != 3 {
			t.Fatalf("MaxConcurrency after middleware = %d, want 3", got)
		}
	})
}

// passThroughChain is a middleware chain that returns registrations unchanged.
type passThroughChain struct{}

func (passThroughChain) RunServiceRegistration(_ context.Context, reg types.ServiceRegistration) types.ServiceRegistration {
	return reg
}

func (passThroughChain) RunOutgoingMessage(octx types.OutgoingMessageContext) types.OutgoingMessageContext {
	return octx
}

func (passThroughChain) RunEventConsumerRegistration(_ context.Context, entry types.EventConsumerEntry) types.EventConsumerEntry {
	return entry
}

func (passThroughChain) RunEventStreamConsumerRegistration(_ context.Context, entry types.EventStreamConsumerEntry) types.EventStreamConsumerEntry {
	return entry
}

// rebuildingChain models middleware written before ServiceOptions existed: it
// rebuilds the registration and each QGHP pair with only the fields it knows.
type rebuildingChain struct{ passThroughChain }

func (rebuildingChain) RunServiceRegistration(_ context.Context, reg types.ServiceRegistration) types.ServiceRegistration {
	out := types.ServiceRegistration{
		Type: reg.Type, Name: reg.Name, ModuleName: reg.ModuleName, Subject: reg.Subject,
		RequestHandler: reg.RequestHandler,
	}
	for _, pair := range reg.QueueHandlers {
		out.QueueHandlers = append(out.QueueHandlers, types.QGHP{QueueGroup: pair.QueueGroup, Handler: pair.Handler})
	}
	return out
}

func TestApplyServiceRegistration_KeepsOptionsDroppedByMiddleware(t *testing.T) {
	handler := func(_ context.Context, _ *types.Msg) error { return nil }

	t.Run("queue group pair options survive a rebuilt pair", func(t *testing.T) {
		container := newBoundContainer(t)
		container.SetMiddlewareChain(rebuildingChain{})
		err := container.RegisterQueueGroupService("process-order",
			types.QGHP{QueueGroup: "ordered", Handler: handler, Options: types.ServiceOptions{MaxConcurrency: 1, MaxPending: 5}},
			types.QGHP{QueueGroup: "parallel", Handler: handler, Options: types.ServiceOptions{MaxConcurrency: 8}},
		)
		if err != nil {
			t.Fatalf("RegisterQueueGroupService failed: %v", err)
		}
		pairs := container.services["process-order"].QueueHandlers
		if pairs[0].Options != (types.ServiceOptions{MaxConcurrency: 1, MaxPending: 5}) {
			t.Fatalf("pair 0 Options = %+v, want {1 5}", pairs[0].Options)
		}
		if pairs[1].Options != (types.ServiceOptions{MaxConcurrency: 8}) {
			t.Fatalf("pair 1 Options = %+v, want {8 0}", pairs[1].Options)
		}
	})

	t.Run("request-reply options survive a rebuilt registration", func(t *testing.T) {
		container := newBoundContainer(t)
		container.SetMiddlewareChain(rebuildingChain{})
		rr := func(_ context.Context, req *types.Msg) ([]byte, error) { return req.Data, nil }
		if err := container.RegisterRequestReplyService("check-stock", rr, types.WithMaxConcurrency(1)); err != nil {
			t.Fatalf("RegisterRequestReplyService failed: %v", err)
		}
		if got := container.services["check-stock"].Options.MaxConcurrency; got != 1 {
			t.Fatalf("MaxConcurrency after rebuilding middleware = %d, want 1", got)
		}
	})

	t.Run("a middleware that sets options wins", func(t *testing.T) {
		entry := &types.ServiceEntry{
			Options:       types.ServiceOptions{MaxConcurrency: 1},
			QueueHandlers: []types.QGHP{{QueueGroup: "g", Handler: handler, Options: types.ServiceOptions{MaxConcurrency: 1}}},
		}
		reg := buildServiceRegistration(entry)
		reg.Options = types.ServiceOptions{MaxConcurrency: 4}
		reg.QueueHandlers = []types.QGHP{{QueueGroup: "g", Handler: handler, Options: types.ServiceOptions{MaxConcurrency: 6}}}
		applyServiceRegistration(entry, reg)
		if entry.Options.MaxConcurrency != 4 || entry.QueueHandlers[0].Options.MaxConcurrency != 6 {
			t.Fatalf("explicit middleware options not applied: %+v / %+v", entry.Options, entry.QueueHandlers[0].Options)
		}
	})

	t.Run("a renamed or added pair keeps its own (zero) options", func(t *testing.T) {
		entry := &types.ServiceEntry{
			QueueHandlers: []types.QGHP{{QueueGroup: "g", Handler: handler, Options: types.ServiceOptions{MaxConcurrency: 1}}},
		}
		reg := buildServiceRegistration(entry)
		reg.QueueHandlers = []types.QGHP{
			{QueueGroup: "renamed", Handler: handler},
			{QueueGroup: "added", Handler: handler},
		}
		applyServiceRegistration(entry, reg)
		for i, pair := range entry.QueueHandlers {
			if pair.Options != (types.ServiceOptions{}) {
				t.Fatalf("pair %d (%s) Options = %+v, want zero value", i, pair.QueueGroup, pair.Options)
			}
		}
	})
}

func TestRegisterQueueGroupService_PairOptions(t *testing.T) {
	handler := func(_ context.Context, _ *types.Msg) error { return nil }

	t.Run("pair options are kept", func(t *testing.T) {
		container := newBoundContainer(t)
		err := container.RegisterQueueGroupService("process-order",
			types.QGHP{QueueGroup: "ordered", Handler: handler, Options: types.ServiceOptions{MaxConcurrency: 1, MaxPending: 100}},
			types.QGHP{QueueGroup: "parallel", Handler: handler},
		)
		if err != nil {
			t.Fatalf("RegisterQueueGroupService failed: %v", err)
		}
		pairs := container.services["process-order"].QueueHandlers
		if pairs[0].Options.MaxConcurrency != 1 || pairs[0].Options.MaxPending != 100 {
			t.Fatalf("pair 0 Options = %+v", pairs[0].Options)
		}
		if pairs[1].Options != (types.ServiceOptions{}) {
			t.Fatalf("pair 1 Options = %+v, want zero value", pairs[1].Options)
		}
	})

	t.Run("negative pair options are rejected", func(t *testing.T) {
		container := newBoundContainer(t)
		err := container.RegisterQueueGroupService("process-order",
			types.QGHP{QueueGroup: "ordered", Handler: handler, Options: types.ServiceOptions{MaxPending: -1}},
		)
		if err == nil {
			t.Fatal("expected an error for MaxPending -1")
		}
	})
}

func TestFrameworkErrorResponse(t *testing.T) {
	t.Run("overloaded maps to ErrServiceOverloaded", func(t *testing.T) {
		err := frameworkErrorResponse("svc", "mod", types.ServiceTypeRequestReply, types.ErrorTypeOverloaded)
		if !errors.Is(err, monoerrors.ErrServiceOverloaded) {
			t.Fatalf("got %v, want ErrServiceOverloaded", err)
		}
		var svcErr *monoerrors.ServiceError
		if !errors.As(err, &svcErr) || svcErr.ServiceName != "svc" || svcErr.ModuleName != "mod" || svcErr.ServiceType != types.ServiceTypeRequestReply {
			t.Fatalf("unexpected ServiceError: %#v", err)
		}
	})

	t.Run("shutting-down maps to ErrServiceUnavailable", func(t *testing.T) {
		err := frameworkErrorResponse("svc", "mod", types.ServiceTypeQueueGroup, types.ErrorTypeShuttingDown)
		if !errors.Is(err, monoerrors.ErrServiceUnavailable) {
			t.Fatalf("got %v, want ErrServiceUnavailable", err)
		}
	})

	t.Run("other types are not framework errors", func(t *testing.T) {
		for _, errorType := range []string{"", "service", "timeout", "validation"} {
			if err := frameworkErrorResponse("svc", "mod", types.ServiceTypeRequestReply, errorType); err != nil {
				t.Fatalf("errorType %q: got %v, want nil", errorType, err)
			}
		}
	})
}

func TestRequestReplyClient_OverloadedResponse(t *testing.T) {
	overloaded := &types.Msg{
		Header: types.Header{
			types.HeaderError:        []string{"true"},
			types.HeaderErrorMessage: []string{"service overloaded"},
			types.HeaderErrorType:    []string{types.ErrorTypeOverloaded},
		},
	}
	client := &requestReplyClient{
		subject:     "services.orders.check-stock",
		serviceName: "check-stock",
		moduleName:  "orders",
		timeout:     5 * time.Second,
		eventBus: &mockEventBus{
			requestMsgWithCtxHandler: func(_ context.Context, _ *types.Msg) (*types.Msg, error) {
				return overloaded, nil
			},
		},
	}

	_, err := client.Call(context.Background(), []byte("sku"))
	if !errors.Is(err, monoerrors.ErrServiceOverloaded) {
		t.Fatalf("Call: got %v, want ErrServiceOverloaded", err)
	}
	if monoerrors.IsRemoteError(err) {
		t.Fatal("an overloaded response is a framework error, not a RemoteError")
	}

	_, err = client.CallMsg(context.Background(), &types.Msg{Data: []byte("sku")})
	if !errors.Is(err, monoerrors.ErrServiceOverloaded) {
		t.Fatalf("CallMsg: got %v, want ErrServiceOverloaded", err)
	}
}

func TestQueueGroupClient_CheckACK(t *testing.T) {
	client := &queueGroupClient{serviceName: "process-order", moduleName: "orders"}

	t.Run("plain ACK is accepted", func(t *testing.T) {
		for _, ack := range []*types.Msg{nil, {}, {Header: types.Header{}}, {Header: types.Header{types.HeaderError: []string{"false"}}}} {
			if err := client.checkACK(ack); err != nil {
				t.Fatalf("checkACK(%v) = %v, want nil", ack, err)
			}
		}
	})

	t.Run("overloaded ACK maps to ErrServiceOverloaded", func(t *testing.T) {
		err := client.checkACK(&types.Msg{Header: types.Header{
			types.HeaderError:     []string{"true"},
			types.HeaderErrorType: []string{types.ErrorTypeOverloaded},
		}})
		if !errors.Is(err, monoerrors.ErrServiceOverloaded) {
			t.Fatalf("got %v, want ErrServiceOverloaded", err)
		}
	})

	t.Run("shutting-down ACK maps to ErrServiceUnavailable", func(t *testing.T) {
		err := client.checkACK(&types.Msg{Header: types.Header{
			types.HeaderError:     []string{"true"},
			types.HeaderErrorType: []string{types.ErrorTypeShuttingDown},
		}})
		if !errors.Is(err, monoerrors.ErrServiceUnavailable) {
			t.Fatalf("got %v, want ErrServiceUnavailable", err)
		}
	})

	t.Run("other error ACK is a RemoteError", func(t *testing.T) {
		err := client.checkACK(&types.Msg{Header: types.Header{
			types.HeaderError:        []string{"true"},
			types.HeaderErrorMessage: []string{"boom"},
		}})
		remoteErr, ok := monoerrors.GetRemoteError(err)
		if !ok || remoteErr.Message != "boom" {
			t.Fatalf("got %v, want RemoteError with message boom", err)
		}
	})
}

func TestQueueGroupClient_SendOverloaded(t *testing.T) {
	overloaded := &types.Msg{
		Header: types.Header{
			types.HeaderError:     []string{"true"},
			types.HeaderErrorType: []string{types.ErrorTypeOverloaded},
		},
	}
	client := &queueGroupClient{
		subject:     "services.orders.process-order",
		serviceName: "process-order",
		moduleName:  "orders",
		timeout:     5 * time.Second,
		eventBus: &mockEventBus{
			requestMsgWithCtxHandler: func(_ context.Context, _ *types.Msg) (*types.Msg, error) {
				return overloaded, nil
			},
		},
	}

	if err := client.Send(context.Background(), []byte("order")); !errors.Is(err, monoerrors.ErrServiceOverloaded) {
		t.Fatalf("Send: got %v, want ErrServiceOverloaded", err)
	}
	if err := client.SendMsg(context.Background(), &types.Msg{Data: []byte("order")}); !errors.Is(err, monoerrors.ErrServiceOverloaded) {
		t.Fatalf("SendMsg: got %v, want ErrServiceOverloaded", err)
	}
	if client.lastSuccessfulACK.Load() != 0 {
		t.Fatal("a rejected send must not count as a successful ACK")
	}
}
