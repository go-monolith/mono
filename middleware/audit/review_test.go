package audit

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-monolith/mono/pkg/types"
)

// TestStop_ContextAlreadyEnded: by the time a middleware is stopped the Stop
// context has often ended (a module before it used it up). The accepted
// entries are written all the same, and the log is closed after them.
func TestStop_ContextAlreadyEnded(t *testing.T) {
	ended, cancel := context.WithCancel(context.Background())
	cancel()
	for name, stop := range map[string]func(*AuditModule) error{
		"Stop":          func(m *AuditModule) error { return m.Stop(ended) },
		"RollbackStart": func(m *AuditModule) error { return m.RollbackStart(ended) },
	} {
		t.Run(name, func(t *testing.T) {
			for run := 0; run < 20; run++ {
				out := &LockedBuffer{}
				m, adapter := StartedAudit(t, out)
				const n = 100
				if err := adapter.AsyncSaveAuditTrail(context.Background(), customEntries(n)); err != nil {
					t.Fatal(err)
				}
				if err := stop(m); err != nil {
					t.Fatalf("run %d: %v", run, err)
				}
				if got := len(out.Entries(t)); got != n {
					t.Fatalf("run %d: %d of %d accepted entries written", run, got, n)
				}
			}
		})
	}
}

// BlockingWriter blocks every write until Release is closed.
type BlockingWriter struct {
	LockedBuffer
	Release chan struct{}
}

// Write waits for the release.
func (w *BlockingWriter) Write(p []byte) (int, error) {
	<-w.Release
	return w.LockedBuffer.Write(p)
}

// TestRollbackStart_HandlerStillWriting: a handler that has not finished
// when the rollback gives up would run beside the handler of the next Start,
// so the module stays started and Start says so.
func TestRollbackStart_HandlerStillWriting(t *testing.T) {
	w := &BlockingWriter{Release: make(chan struct{})}
	m, adapter := StartedAudit(t, w)
	if err := adapter.AsyncSaveAuditTrail(context.Background(), customEntries(1)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // the handler is in the write
	ended, cancel := context.WithCancel(context.Background())
	cancel()
	if err := m.RollbackStart(ended); err == nil || !strings.Contains(err.Error(), "still writing") {
		t.Fatalf("RollbackStart = %v, want the handler reported as still writing", err)
	}
	if err := m.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "already started") {
		t.Fatalf("Start beside the running handler = %v, want already started", err)
	}
	close(w.Release)
	if err := m.RollbackStart(context.Background()); err != nil {
		t.Fatalf("RollbackStart once the handler can finish: %v", err)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("Start after the rollback: %v", err)
	}
	if err := m.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// CloseRecorder is a writer whose Close is counted.
type CloseRecorder struct {
	bytes.Buffer
	Closes int
}

// Close counts.
func (c *CloseRecorder) Close() error { c.Closes++; return nil }

// TestStop_NeverStarted: the hooks write entries before Start, so the final
// Stop closes the log of a module that was never started, or that a rollback
// stopped, once.
func TestStop_NeverStarted(t *testing.T) {
	w := &CloseRecorder{}
	m, err := New(WithOutput(w))
	if err != nil {
		t.Fatal(err)
	}
	m.OnServiceRegistration(context.Background(), types.ServiceRegistration{Name: "ping", ModuleName: "orders"})
	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := m.RollbackStart(ctx); err != nil {
		t.Fatal(err)
	}
	if w.Closes != 0 {
		t.Fatalf("the rollback closed the log %d time(s)", w.Closes)
	}
	for i := 0; i < 2; i++ {
		if err := m.Stop(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if w.Closes != 1 {
		t.Fatalf("the log was closed %d time(s) by two Stops, want once", w.Closes)
	}
	if err := m.Start(ctx); !errors.Is(err, ErrOutputClosed) {
		t.Fatalf("Start after Stop = %v, want ErrOutputClosed", err)
	}
}

// GatedWriter blocks the writes while Gate is locked and fails every write
// from the FailFrom-th on.
type GatedWriter struct {
	LockedBuffer
	Gate     sync.Mutex
	mu       sync.Mutex
	n        int
	FailFrom int
}

// Write waits for the gate, then records or fails.
func (w *GatedWriter) Write(p []byte) (int, error) {
	w.Gate.Lock()
	w.Gate.Unlock() //nolint:staticcheck // the gate only holds writers back
	w.mu.Lock()
	n := w.n
	w.n++
	w.mu.Unlock()
	if n >= w.FailFrom {
		return 0, errors.New("disk full")
	}
	return w.LockedBuffer.Write(p)
}

// TestSaveAuditTrail_StaleConfirmations: the confirmations of a call that
// stopped waiting arrive later. They must not be counted by the next call,
// which would then report entries as saved that were not written.
func TestSaveAuditTrail_StaleConfirmations(t *testing.T) {
	w := &GatedWriter{FailFrom: 3}
	_, adapter := StartedAudit(t, w)

	// The first call gives up while the writer is held back.
	w.Gate.Lock()
	short, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	saved, err := adapter.SaveAuditTrail(short, customEntries(3))
	cancel()
	if saved != 0 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first SaveAuditTrail = %d, %v, want 0 and the deadline", saved, err)
	}
	// Its three entries are written now, and confirmed, while nobody waits.
	w.Gate.Unlock()
	deadline := time.Now().Add(5 * time.Second)
	for len(w.Entries(t)) != 3 {
		if time.Now().After(deadline) {
			t.Fatalf("%d entries of the first call written", len(w.Entries(t)))
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond) // the confirmations reach the consumer channel

	// The second call's writes all fail: none of its entries is saved,
	// whatever confirmations of the first call are lying around.
	ctx, cancelLong := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelLong()
	saved, err = adapter.SaveAuditTrail(ctx, customEntries(2))
	if saved != 0 || err == nil || !strings.Contains(err.Error(), "disk full") {
		t.Fatalf("second SaveAuditTrail = %d, %v, want 0 and the write error", saved, err)
	}
}

// TestSaveAuditTrail_ContextEndsAfterAFailedWrite: the write error already
// confirmed is not hidden behind the context's.
func TestSaveAuditTrail_ContextEndsAfterAFailedWrite(t *testing.T) {
	container := newMockContainer()
	in, out := make(chan *types.Msg, 10), make(chan *types.Msg, 10)
	if err := container.RegisterChannelService("audit-trail", in, out); err != nil {
		t.Fatal(err)
	}
	adapter, err := NewAuditAdapter(container, "orders")
	if err != nil {
		t.Fatal(err)
	}
	// The first entry is refused, the second is never confirmed.
	go func() {
		req := <-in
		out <- &types.Msg{Data: []byte(`{"success":false,"error":"disk full"}`), Header: ConfirmationHeader(req)}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	saved, err := adapter.SaveAuditTrail(ctx, customEntries(2))
	if saved != 0 || !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "disk full") {
		t.Fatalf("SaveAuditTrail = %d, %v, want 0, the write error and the deadline", saved, err)
	}
}

// TornWriter writes half of the Torn-th write and fails it.
type TornWriter struct {
	bytes.Buffer
	n, Torn int
}

// Write tears one write.
func (w *TornWriter) Write(p []byte) (int, error) {
	n := w.n
	w.n++
	if n == w.Torn {
		half := len(p) / 2
		w.Buffer.Write(p[:half])
		return half, errors.New("disk full")
	}
	return w.Buffer.Write(p)
}

// TestWriteEntry_TornLine: a write that fails after part of its line is in
// the log leaves a torn line. The next entry starts on a line of its own,
// and the chain continues from the last complete entry.
func TestWriteEntry_TornLine(t *testing.T) {
	w := &TornWriter{Torn: 1}
	m, err := New(WithOutput(w), WithHashChaining(""))
	if err != nil {
		t.Fatal(err)
	}
	var failed int
	for i := 0; i < 4; i++ {
		if err := m.writeEntry(context.Background(), Entry{EventType: EventCustomAuditTrail, ModuleName: "orders"}); err != nil {
			failed++
		}
	}
	if failed != 1 {
		t.Fatalf("%d writes failed, want 1", failed)
	}
	lines := strings.Split(strings.TrimRight(w.String(), "\n"), "\n")
	if len(lines) != 4 {
		t.Fatalf("%d lines, want three entries and the torn line:\n%s", len(lines), w.String())
	}
	whole := &LockedBuffer{}
	for i, line := range lines {
		if i == 1 {
			continue // the torn line
		}
		if _, err := whole.Write([]byte(line + "\n")); err != nil {
			t.Fatal(err)
		}
	}
	entries := whole.Entries(t)
	if len(entries) != 3 {
		t.Fatalf("%d complete entries, want 3", len(entries))
	}
	if err := VerifyChain(entries); err != nil {
		t.Fatalf("chain of the complete entries: %v", err)
	}
}

func TestWaitForHandler(t *testing.T) {
	closed := make(chan struct{})
	close(closed)
	ended, cancel := context.WithCancel(context.Background())
	cancel()
	if err := WaitForHandler(ended, closed); err != nil {
		t.Fatalf("a finished handler: %v", err)
	}
	late := make(chan struct{})
	time.AfterFunc(handlerDrainGrace/4, func() { close(late) })
	if err := WaitForHandler(ended, late); err != nil {
		t.Fatalf("a handler that finishes within the grace: %v", err)
	}
	if err := WaitForHandler(ended, make(chan struct{})); !errors.Is(err, context.Canceled) {
		t.Fatalf("a handler that does not finish = %v", err)
	}
}

func TestIsConfirmationOf(t *testing.T) {
	msg := &types.Msg{Header: types.Header{CallHeader: {"a1"}}}
	if !IsConfirmationOf(msg, "a1") || IsConfirmationOf(msg, "a2") || IsConfirmationOf(&types.Msg{}, "a1") || IsConfirmationOf(nil, "a1") {
		t.Fatal("IsConfirmationOf does not match on the call header alone")
	}
	if ConfirmationHeader(nil) != nil || ConfirmationHeader(&types.Msg{}) != nil {
		t.Fatal("a request without a call ID gets a confirmation header")
	}
	if got := ConfirmationHeader(msg); len(got[CallHeader]) != 1 || got[CallHeader][0] != "a1" {
		t.Fatalf("ConfirmationHeader = %v", got)
	}
	if err := ConfirmationError(nil); err == nil {
		t.Fatal("an empty confirmation is not an error")
	}
}
