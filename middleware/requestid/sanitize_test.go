package requestid

import (
	"context"
	"strings"
	"testing"

	"github.com/go-monolith/mono/pkg/types"
)

// TestForeignRequestIDIsSanitised: the header comes from the sender, so every
// wrapper puts a sanitised ID into the handler's context, and that is what a
// nested call sends on.
func TestForeignRequestIDIsSanitised(t *testing.T) {
	m, err := New()
	if err != nil {
		t.Fatal(err)
	}
	forged := "x module=billing status=success \x1b[2J" + strings.Repeat("A", 900_000)
	msg := &types.Msg{Header: types.Header{"X-Request-ID": {forged}}}
	check := func(t *testing.T, ctx context.Context) {
		t.Helper()
		id := GetRequestID(ctx)
		if len(id) != 128 || strings.ContainsAny(id, " \x1b[") || !strings.HasPrefix(id, "x_module=billing_status=success_") {
			t.Fatalf("request ID in the context = %q (%d bytes)", id, len(id))
		}
		out := m.OnOutgoingMessage(types.OutgoingMessageContext{Ctx: ctx, Msg: &types.Msg{}, ServiceType: types.ServiceTypeRequestReply})
		if got := out.Msg.Header.Get("X-Request-ID"); got != id {
			t.Fatalf("nested call sends %q, want %q", got, id)
		}
	}

	t.Run("request-reply", func(t *testing.T) {
		h := m.wrapRequestReplyHandler(func(ctx context.Context, _ *types.Msg) ([]byte, error) {
			check(t, ctx)
			return nil, nil
		})
		if _, err := h(context.Background(), msg); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("queue group", func(t *testing.T) {
		h := m.wrapQueueGroupHandler(func(ctx context.Context, _ *types.Msg) error {
			check(t, ctx)
			return nil
		})
		if err := h(context.Background(), msg); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("event consumer", func(t *testing.T) {
		h := m.wrapEventConsumerHandler(func(ctx context.Context, _ *types.Msg) error {
			check(t, ctx)
			return nil
		})
		if err := h(context.Background(), msg); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("stream consumer batch", func(t *testing.T) {
		h := m.wrapStreamConsumerHandler(func(ctx context.Context, _ []*types.Msg) error {
			check(t, ctx)
			return nil
		})
		if err := h(context.Background(), []*types.Msg{msg}); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("event stream consumer batch", func(t *testing.T) {
		h := m.wrapEventStreamConsumerHandler(func(ctx context.Context, _ []*types.Msg) error {
			check(t, ctx)
			return nil
		})
		if err := h(context.Background(), []*types.Msg{msg}); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("a clean ID is kept", func(t *testing.T) {
		clean := &types.Msg{Header: types.Header{"X-Request-ID": {"3f2c8a1e-9b7d-4c1a-8e2f-0a1b2c3d4e5f"}}}
		h := m.wrapRequestReplyHandler(func(ctx context.Context, _ *types.Msg) ([]byte, error) {
			if id := GetRequestID(ctx); id != "3f2c8a1e-9b7d-4c1a-8e2f-0a1b2c3d4e5f" {
				t.Fatalf("request ID = %q", id)
			}
			return nil, nil
		})
		if _, err := h(context.Background(), clean); err != nil {
			t.Fatal(err)
		}
	})
}
