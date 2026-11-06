package container

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/go-monolith/mono/pkg/types"
)

func TestSetDeadlineHeaders(t *testing.T) {
	t.Run("no deadline sets nothing", func(t *testing.T) {
		msg := &types.Msg{}
		setDeadlineHeaders(context.Background(), msg)
		if len(msg.Header) != 0 {
			t.Fatalf("headers = %v, want none", msg.Header)
		}
	})

	t.Run("deadline and remaining time are written", func(t *testing.T) {
		deadline := time.Now().Add(2 * time.Second)
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		defer cancel()
		msg := &types.Msg{} // nil Header: must be created
		setDeadlineHeaders(ctx, msg)

		if got := msg.Header.Get(types.HeaderDeadline); got != strconv.FormatInt(deadline.UnixMilli(), 10) {
			t.Fatalf("%s = %q", types.HeaderDeadline, got)
		}
		ms, err := strconv.Atoi(msg.Header.Get(types.HeaderTimeout))
		if err != nil || ms <= 1500 || ms > 2000 {
			t.Fatalf("%s = %q, want roughly 2000", types.HeaderTimeout, msg.Header.Get(types.HeaderTimeout))
		}
	})

	t.Run("past deadline reports zero remaining", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()
		msg := &types.Msg{Header: types.Header{}}
		setDeadlineHeaders(ctx, msg)
		if got := msg.Header.Get(types.HeaderTimeout); got != "0" {
			t.Fatalf("%s = %q, want 0", types.HeaderTimeout, got)
		}
	})
}

// TestRequestReplyClient_SendsDeadlineHeaders checks that Call and CallMsg put
// the caller's deadline (or the client's default timeout) on the wire, before
// middleware runs, without touching the caller's own header map.
func TestRequestReplyClient_SendsDeadlineHeaders(t *testing.T) {
	var sent *types.Msg
	client := &requestReplyClient{
		subject: "services.orders.check", serviceName: "check", moduleName: "orders",
		timeout: 30 * time.Second,
		eventBus: &mockEventBus{
			requestMsgWithCtxHandler: func(_ context.Context, msg *types.Msg) (*types.Msg, error) {
				sent = msg
				return &types.Msg{Data: []byte("ok")}, nil
			},
		},
	}

	t.Run("Call uses the caller's deadline", func(t *testing.T) {
		deadline := time.Now().Add(5 * time.Second)
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		defer cancel()
		if _, err := client.Call(ctx, []byte("sku")); err != nil {
			t.Fatalf("Call: %v", err)
		}
		if got := sent.Header.Get(types.HeaderDeadline); got != strconv.FormatInt(deadline.UnixMilli(), 10) {
			t.Fatalf("%s = %q, want the caller's deadline", types.HeaderDeadline, got)
		}
	})

	t.Run("Call without a deadline sends the default timeout", func(t *testing.T) {
		if _, err := client.Call(context.Background(), []byte("sku")); err != nil {
			t.Fatalf("Call: %v", err)
		}
		ms, err := strconv.Atoi(sent.Header.Get(types.HeaderTimeout))
		if err != nil || ms < 29000 || ms > 30000 {
			t.Fatalf("%s = %q, want about 30000 (the client default)", types.HeaderTimeout, sent.Header.Get(types.HeaderTimeout))
		}
	})

	t.Run("CallMsg copies the caller's headers and does not mutate them", func(t *testing.T) {
		callerHeader := types.Header{"X-Tenant": {"acme"}}
		if _, err := client.CallMsg(context.Background(), &types.Msg{Data: []byte("sku"), Header: callerHeader}); err != nil {
			t.Fatalf("CallMsg: %v", err)
		}
		if sent.Header.Get("X-Tenant") != "acme" || sent.Header.Get(types.HeaderDeadline) == "" {
			t.Fatalf("sent headers = %v, want tenant and deadline", sent.Header)
		}
		if _, leaked := callerHeader[types.HeaderDeadline]; leaked {
			t.Fatal("the caller's header map must not be modified")
		}
	})
}

// TestQueueGroupClient_SendsDeadlineHeaders checks that queue group sends carry
// the deadline too (ACK mode gets the default; optimistic publish mode only
// when the caller's context has a deadline).
func TestQueueGroupClient_SendsDeadlineHeaders(t *testing.T) {
	var sent *types.Msg
	bus := &mockEventBus{
		requestMsgWithCtxHandler: func(_ context.Context, msg *types.Msg) (*types.Msg, error) {
			sent = msg
			return &types.Msg{}, nil
		},
		publishMsgFunc: func(msg *types.Msg) error { sent = msg; return nil },
	}
	client := &queueGroupClient{
		subject: "services.orders.process", serviceName: "process", moduleName: "orders",
		timeout: 5 * time.Second, eventBus: bus,
	}

	if err := client.Send(context.Background(), []byte("job")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if sent.Header.Get(types.HeaderDeadline) == "" {
		t.Fatal("ACK-mode send should carry the default deadline")
	}

	// Optimistic publish mode: no default deadline is applied, so no header
	// unless the caller's context has one.
	client.optimisticWindow = time.Minute
	client.lastSuccessfulACK.Store(time.Now().UnixNano())
	if err := client.SendMsg(context.Background(), &types.Msg{Data: []byte("job")}); err != nil {
		t.Fatalf("SendMsg: %v", err)
	}
	if sent.Header.Get(types.HeaderDeadline) != "" {
		t.Fatal("optimistic publish without a caller deadline must not invent one")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.SendMsg(ctx, &types.Msg{Data: []byte("job")}); err != nil {
		t.Fatalf("SendMsg: %v", err)
	}
	if sent.Header.Get(types.HeaderDeadline) == "" {
		t.Fatal("optimistic publish with a caller deadline should carry it")
	}
}
