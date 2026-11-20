package audit

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-monolith/mono/pkg/types"
)

// TestRestartAfterRollback: the rollback of a failed application Start leaves
// the log file open, and the module starts again and keeps its chain. Before,
// every later Start failed with "audit module already started".
func TestRestartAfterRollback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.log")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	m, adapter := StartedAudit(t, f, WithHashChaining(""))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if n, err := adapter.SaveAuditTrail(ctx, customEntries(2)); n != 2 || err != nil {
		t.Fatalf("SaveAuditTrail = %d, %v", n, err)
	}
	if err := m.Start(ctx); err == nil || !strings.Contains(err.Error(), "already started") {
		t.Fatalf("second Start = %v, want already started", err)
	}
	if err := m.RollbackStart(ctx); err != nil {
		t.Fatalf("RollbackStart: %v", err)
	}
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start after a rollback: %v", err)
	}
	if n, err := adapter.SaveAuditTrail(ctx, customEntries(2)); n != 2 || err != nil {
		t.Fatalf("SaveAuditTrail after the restart = %d, %v", n, err)
	}
	m.OnModuleLifecycle(ctx, types.ModuleLifecycleEvent{Type: types.ModuleStartedEvent, ModuleName: "orders"})
	if err := m.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	w := &LockedBuffer{}
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	entries := w.Entries(t)
	if len(entries) != 5 {
		t.Fatalf("%d entries in the log, want 5", len(entries))
	}
	if err := VerifyChain(entries); err != nil {
		t.Fatalf("chain across the restart: %v", err)
	}

	// Stop closed the file: the module cannot be started again, and an entry
	// that still arrives is refused rather than written nowhere.
	if err := m.Start(ctx); !errors.Is(err, ErrOutputClosed) {
		t.Fatalf("Start after Stop = %v, want ErrOutputClosed", err)
	}
	if err := m.writeEntry(ctx, Entry{EventType: EventCustomAuditTrail}); !errors.Is(err, ErrOutputClosed) {
		t.Fatalf("writeEntry after Stop = %v, want ErrOutputClosed", err)
	}
}

// TestRestartAfterStop_WriterNotClosed: a writer Stop does not close keeps
// the module restartable after Stop as well.
func TestRestartAfterStop_WriterNotClosed(t *testing.T) {
	out := &LockedBuffer{}
	m, adapter := StartedAudit(t, out)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := m.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start after Stop: %v", err)
	}
	if n, err := adapter.SaveAuditTrail(ctx, customEntries(3)); n != 3 || err != nil {
		t.Fatalf("SaveAuditTrail after the restart = %d, %v", n, err)
	}
	if got := len(out.Entries(t)); got != 3 {
		t.Fatalf("%d entries written, want 3", got)
	}
}
