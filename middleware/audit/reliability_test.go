package audit

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-monolith/mono/internal/container"
	"github.com/go-monolith/mono/internal/logger"
	"github.com/go-monolith/mono/pkg/types"
)

// LockedBuffer is a bytes.Buffer safe for the handler goroutine and the test.
type LockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

// Write appends p.
func (b *LockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// Entries parses the lines written so far.
func (b *LockedBuffer) Entries(t *testing.T) []Entry {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	var entries []Entry
	sc := bufio.NewScanner(bytes.NewReader(b.buf.Bytes()))
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for sc.Scan() {
		var e Entry
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			t.Fatalf("line %q is not an entry: %v", sc.Text(), err)
		}
		entries = append(entries, e)
	}
	return entries
}

// FlakyWriter fails the writes numbered in Fail (0-based) and records the rest.
type FlakyWriter struct {
	LockedBuffer
	mu    sync.Mutex
	n     int
	Fail  map[int]bool
	Fails bool // every write fails
}

// Write fails or records p.
func (w *FlakyWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	n := w.n
	w.n++
	fail := w.Fails || w.Fail[n]
	w.mu.Unlock()
	if fail {
		return 0, errors.New("disk full")
	}
	return w.LockedBuffer.Write(p)
}

// StartedAudit returns a started audit module on w, bound to a real service
// container with its channel routers running, and an adapter for a consumer.
func StartedAudit(t *testing.T, w interface{ Write([]byte) (int, error) }, opts ...Option) (*AuditModule, AuditAdapterPort) {
	t.Helper()
	m, err := New(append([]Option{WithOutput(w)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	c := container.New(logger.NewDefaultLogger())
	if err := c.BindModule(m); err != nil {
		t.Fatal(err)
	}
	if err := m.RegisterServices(c); err != nil {
		t.Fatal(err)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	routerCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c.StartChannelRouters(routerCtx)
	adapter, err := NewAuditAdapter(c, "orders")
	if err != nil {
		t.Fatal(err)
	}
	return m, adapter
}

func customEntries(n int) []Entry {
	entries := make([]Entry, n)
	for i := range entries {
		entries[i] = Entry{EventType: EventCustomAuditTrail, ModuleName: "orders", Details: map[string]any{"i": float64(i)}}
	}
	return entries
}

// TestSaveAuditTrail_ManyEntries: a synchronous save of more entries than the
// channels hold used to send them all before it read a confirmation, lose the
// confirmations the router could not deliver, and wait for them until its
// context ended.
func TestSaveAuditTrail_ManyEntries(t *testing.T) {
	out := &LockedBuffer{}
	_, adapter := StartedAudit(t, out, WithHashChaining(""))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	const n = 1000
	saved, err := adapter.SaveAuditTrail(ctx, customEntries(n))
	if err != nil || saved != n {
		t.Fatalf("SaveAuditTrail = %d, %v, want %d, nil", saved, err, n)
	}
	entries := out.Entries(t)
	if len(entries) != n {
		t.Fatalf("%d entries written, want %d", len(entries), n)
	}
	if err := VerifyChain(entries); err != nil {
		t.Fatalf("chain: %v", err)
	}
	// The adapter is usable again at once: nothing of the first call is left.
	if saved, err := adapter.SaveAuditTrail(ctx, customEntries(3)); err != nil || saved != 3 {
		t.Fatalf("second SaveAuditTrail = %d, %v", saved, err)
	}
}

// TestSaveAuditTrail_FailedWrite: an entry that could not be written is not
// reported as saved, and the chain does not advance over it, so the entries
// that are in the log still verify.
func TestSaveAuditTrail_FailedWrite(t *testing.T) {
	t.Run("every write fails", func(t *testing.T) {
		w := &FlakyWriter{Fails: true}
		_, adapter := StartedAudit(t, w, WithHashChaining(""))
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		saved, err := adapter.SaveAuditTrail(ctx, customEntries(1))
		if saved != 0 || err == nil || !strings.Contains(err.Error(), "disk full") {
			t.Fatalf("SaveAuditTrail = %d, %v, want 0 and the write error", saved, err)
		}
	})
	t.Run("the chain skips the entry that was not written", func(t *testing.T) {
		w := &FlakyWriter{Fail: map[int]bool{1: true}}
		_, adapter := StartedAudit(t, w, WithHashChaining(""))
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		saved, err := adapter.SaveAuditTrail(ctx, customEntries(4))
		if saved != 3 || err == nil {
			t.Fatalf("SaveAuditTrail = %d, %v, want 3 and an error", saved, err)
		}
		entries := w.Entries(t)
		if len(entries) != 3 {
			t.Fatalf("%d entries written, want 3", len(entries))
		}
		if err := VerifyChain(entries); err != nil {
			t.Fatalf("the log no longer verifies after a failed write: %v", err)
		}
	})
}

// TestStop_WritesAcceptedEntries: entries the module accepted before Stop are
// written by it, and a save after Stop neither panics nor reports success.
func TestStop_WritesAcceptedEntries(t *testing.T) {
	for run := 0; run < 20; run++ {
		out := &LockedBuffer{}
		m, adapter := StartedAudit(t, out)
		const n = 100
		if err := adapter.AsyncSaveAuditTrail(context.Background(), customEntries(n)); err != nil {
			t.Fatal(err)
		}
		if err := m.Stop(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got := len(out.Entries(t)); got != n {
			t.Fatalf("run %d: %d of %d accepted entries written by Stop", run, got, n)
		}
	}

	out := &LockedBuffer{}
	m, adapter := StartedAudit(t, out)
	if err := m.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := m.Stop(context.Background()); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
	// Fire-and-forget after Stop: accepted into the buffer or refused, never a panic.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = adapter.AsyncSaveAuditTrail(ctx, customEntries(300))
	saved, err := adapter.SaveAuditTrail(ctx, customEntries(1))
	if saved != 0 || err == nil {
		t.Fatalf("SaveAuditTrail after Stop = %d, %v, want 0 and an error", saved, err)
	}
}

// TestHookAfterFailedWrite: the chain of the framework's own entries does not
// advance over one that was not written either.
func TestHookAfterFailedWrite(t *testing.T) {
	w := &FlakyWriter{Fail: map[int]bool{1: true}}
	m, err := New(WithOutput(w), WithHashChaining(""))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		m.OnModuleLifecycle(context.Background(), types.ModuleLifecycleEvent{Type: types.ModuleStartedEvent, ModuleName: "orders"})
	}
	entries := w.Entries(t)
	if len(entries) != 3 {
		t.Fatalf("%d entries written, want 3", len(entries))
	}
	if err := VerifyChain(entries); err != nil {
		t.Fatalf("the log no longer verifies after a failed write: %v", err)
	}
}
