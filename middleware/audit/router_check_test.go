package audit

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/go-monolith/mono/internal/container"
	"github.com/go-monolith/mono/internal/logger"
	"github.com/go-monolith/mono/pkg/types"
)

// RouterlessService is a container with an audit-trail channel service whose
// in channel nobody reads. It starts the router when start is true; the
// returned cancel stops it.
func RouterlessService(t *testing.T, start bool) (types.ServiceContainer, chan *types.Msg, context.CancelFunc) {
	t.Helper()
	m, err := New(WithOutput(&LockedBuffer{}))
	if err != nil {
		t.Fatal(err)
	}
	c := container.New(logger.NewDefaultLogger())
	if err := c.BindModule(m); err != nil {
		t.Fatal(err)
	}
	in, out := make(chan *types.Msg, 16), make(chan *types.Msg, 16)
	if err := c.RegisterChannelService(AuditTrailService, in, out); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if start {
		c.StartChannelRouters(ctx)
	}
	return c, in, cancel
}

// Before the channel routers start (a module's Start) SaveAuditTrail sends
// nothing and fails at once, instead of writing the entry and waiting for a
// confirmation that cannot arrive (issue #89).
func TestSaveAuditTrail_WithoutRouterFailsAtOnce(t *testing.T) {
	c, in, _ := RouterlessService(t, false)
	adapter, err := NewAuditAdapter(c, "orders")
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	n, err := adapter.SaveAuditTrail(context.Background(), customEntries(2))
	if n != 0 || !errors.Is(err, ErrConfirmationUnavailable) {
		t.Fatalf("SaveAuditTrail = %d, %v, want 0, ErrConfirmationUnavailable", n, err)
	}
	if waited := time.Since(start); waited > time.Second {
		t.Fatalf("SaveAuditTrail waited %v, want an immediate error", waited)
	}
	if len(in) != 0 {
		t.Fatalf("%d entries were sent, want none", len(in))
	}
	// AsyncSaveAuditTrail does not wait and still works.
	if err := adapter.AsyncSaveAuditTrail(context.Background(), customEntries(1)); err != nil || len(in) != 1 {
		t.Fatalf("AsyncSaveAuditTrail = %v with %d sent, want nil and 1", err, len(in))
	}
}

// When the router stops while SaveAuditTrail waits (shutdown began), the call
// returns ErrConfirmationUnavailable instead of waiting for its context.
func TestSaveAuditTrail_RouterStopsWhileWaiting(t *testing.T) {
	c, in, stopRouter := RouterlessService(t, true)
	adapter, err := NewAuditAdapter(c, "orders")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		<-time.After(100 * time.Millisecond)
		stopRouter()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	n, err := adapter.SaveAuditTrail(ctx, customEntries(1))
	if n != 0 || !errors.Is(err, ErrConfirmationUnavailable) {
		t.Fatalf("SaveAuditTrail = %d, %v, want 0, ErrConfirmationUnavailable", n, err)
	}
	if waited := time.Since(start); waited > 2*time.Second {
		t.Fatalf("SaveAuditTrail waited %v after the router stopped", waited)
	}
	if len(in) != 1 {
		t.Fatalf("%d entries sent, want the one sent before the router stopped", len(in))
	}
}

// When the router stops after confirming part of a call, the confirmed
// entries are counted and the rest is reported as not confirmed.
func TestSaveAuditTrail_RouterStopsAfterPartialConfirmation(t *testing.T) {
	c, in, stopRouter := RouterlessService(t, true)
	entry := c.Entries()[0]
	adapter, err := NewAuditAdapter(c, "orders")
	if err != nil {
		t.Fatal(err)
	}
	// A provider that confirms the first entry, then shutdown begins.
	go func() {
		msg := <-in
		confirmation, _ := json.Marshal(SaveEntryResponse{Success: true})
		entry.OutChannel <- &types.Msg{Reply: msg.Reply, Data: confirmation, Header: types.Header{CallHeader: msg.Header[CallHeader]}}
		<-in
		time.Sleep(100 * time.Millisecond)
		stopRouter()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	n, err := adapter.SaveAuditTrail(ctx, customEntries(2))
	if n != 1 || !errors.Is(err, ErrConfirmationUnavailable) {
		t.Fatalf("SaveAuditTrail = %d, %v, want 1, ErrConfirmationUnavailable", n, err)
	}
}
