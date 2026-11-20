package accesslog

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-monolith/mono/pkg/types"
)

// TestRestartAfterRollback: the rollback of a failed application Start leaves
// the log file open, and the module logs again after the next Start. Before,
// the shutdown flag stayed set and the file was closed, so nothing was
// logged, without an error.
func TestRestartAfterRollback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "access.log")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	m, err := New(WithOutput(f))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	call := func(service string) {
		t.Helper()
		h := m.wrapRequestReplyHandler(func(context.Context, *types.Msg) ([]byte, error) { return nil, nil }, "orders", service)
		if _, err := h(ctx, &types.Msg{}); err != nil {
			t.Fatal(err)
		}
	}

	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	call("first")
	if err := m.RollbackStart(ctx); err != nil {
		t.Fatalf("RollbackStart: %v", err)
	}
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start after a rollback: %v", err)
	}
	call("second")
	if err := m.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"service=first", "service=second"} {
		if !strings.Contains(string(data), want) {
			t.Errorf("log lacks %q:\n%s", want, data)
		}
	}

	// Stop closed the file: the module cannot be started again, and says so.
	if err := m.Start(ctx); !errors.Is(err, ErrOutputClosed) {
		t.Fatalf("Start after Stop = %v, want ErrOutputClosed", err)
	}
	if err := m.Stop(ctx); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
}

// TestRestartAfterStop_WriterNotClosed: a writer Stop does not close keeps
// the module restartable after Stop as well.
func TestRestartAfterStop_WriterNotClosed(t *testing.T) {
	buf := &syncWriter{}
	m, err := New(WithOutput(buf))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := m.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start after Stop: %v", err)
	}
	h := m.wrapRequestReplyHandler(func(context.Context, *types.Msg) ([]byte, error) { return nil, nil }, "orders", "place")
	if _, err := h(ctx, &types.Msg{}); err != nil {
		t.Fatal(err)
	}
	if lines := buf.Lines(); len(lines) != 1 {
		t.Fatalf("%d lines logged after the restart, want 1", len(lines))
	}
}
